package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/s3/s3response"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// errSideMissing says one cluster does not hold the bucket: during a migration the target may not
// have been created yet, and a listing must still answer from the side that does hold it.
var errSideMissing = errors.New("listing: the bucket is not on this cluster")

// maxListKeys is S3's own page limit, and the bound on how much of a listing shunt holds at once.
const maxListKeys = 1000

// mergeToken is shunt's continuation token while a bucket spans two clusters. It carries each
// side's own token plus the last name handed to the client, so a resumed page re-reads at most one
// page per side and skips what was already sent. Clients treat it as opaque, which it is.
type mergeToken struct {
	P    string `json:"p,omitempty"`  // primary's continuation token
	PD   bool   `json:"pd,omitempty"` // primary exhausted
	S    string `json:"s,omitempty"`  // source's continuation token
	SD   bool   `json:"sd,omitempty"` // source exhausted
	Last string `json:"last,omitempty"`
}

func (t mergeToken) encode() string {
	b, _ := json.Marshal(t) //nolint:errcheck // a struct of strings and bools always marshals
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeMergeToken(s string) mergeToken {
	var t mergeToken
	if s == "" {
		return t
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || json.Unmarshal(b, &t) != nil {
		return mergeToken{}
	}
	return t
}

// listItem is one entry of a listing: an object, or a common prefix produced by a delimiter.
type listItem struct {
	name   string
	prefix bool
	obj    s3response.Object
}

// listSide is one cluster's paginated stream. At most one page is held at a time, so memory is a
// function of the page size, not of the bucket (ADR-0004).
type listSide struct {
	cl      *upstream.Cluster
	backend string
	used    string // the continuation token that produced the buffered page
	next    string // the page's own next token, "" when the page was the last
	done    bool
	started bool
	missing bool // the cluster answered NoSuchBucket
	items   []listItem
}

// mergeListing answers ListObjectsV2 from both of a migrating bucket's clusters: a sorted merge of
// two paginated streams, the primary winning when both hold the same name (docs/DESIGN.md §2.5).
// ListObjects v1 and ListMultipartUploads are not merged in POC-4 and go to the primary alone.
func (h *Handler) mergeListing(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome,
	primary *upstream.Cluster, pBackend string, source *upstream.Cluster, sBackend string) {
	start := time.Now()
	bucketKey := directory.Key(o.tenant, o.info.Bucket)
	q := r.URL.Query()
	maxKeys := maxListKeys
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n < maxListKeys {
			maxKeys = n
		}
	}
	tok := decodeMergeToken(q.Get("continuation-token"))
	ps := &listSide{cl: primary, backend: pBackend, used: tok.P, done: tok.PD}
	ss := &listSide{cl: source, backend: sBackend, used: tok.S, done: tok.SD}

	head := func(s *listSide) (*listItem, error) {
		for len(s.items) == 0 {
			switch {
			case s.done:
				return nil, nil
			case s.started && s.next == "":
				s.done = true
				return nil, nil
			case s.started:
				s.used = s.next
			}
			if err := h.fetchListPage(ctx, o, s, q, tok.Last); err != nil {
				if errors.Is(err, errSideMissing) {
					return nil, nil
				}
				return nil, err
			}
		}
		return &s.items[0], nil
	}

	emitted := make([]listItem, 0, min(maxKeys, 256))
	for len(emitted) < maxKeys {
		ph, err := head(ps)
		if err != nil {
			h.upstreamError(w, r, o, err)
			return
		}
		sh, err := head(ss)
		if err != nil {
			h.upstreamError(w, r, o, err)
			return
		}
		var take listItem
		switch {
		case ph == nil && sh == nil:
			maxKeys = len(emitted) // both streams are finished
			continue
		case sh == nil || (ph != nil && ph.name < sh.name):
			take, ps.items = *ph, ps.items[1:]
		case ph == nil || sh.name < ph.name:
			take, ss.items = *sh, ss.items[1:]
		default: // the same name on both: the primary's copy is the newer one
			take, ps.items, ss.items = *ph, ps.items[1:], ss.items[1:]
		}
		emitted = append(emitted, take)
	}

	// Truncated only if one side really has more; peeking costs at most one page per side.
	ph, _ := head(ps)
	sh, _ := head(ss)
	if ps.missing && ss.missing {
		h.answer(w, r, o, s3.NoSuchBucket, "")
		return
	}
	truncated := ph != nil || sh != nil
	next := ""
	if truncated && len(emitted) > 0 {
		next = mergeToken{P: ps.used, PD: ps.done, S: ss.used, SD: ss.done, Last: emitted[len(emitted)-1].name}.encode()
	}
	h.Metrics.ListingMerge.WithLabelValues(bucketKey).Observe(time.Since(start).Seconds())
	h.writeListing(w, o, q, emitted, truncated, next, maxKeys)
}

// fetchListPage reads one page from one side, skipping anything the client has already been sent.
func (h *Handler) fetchListPage(ctx context.Context, o *outcome, s *listSide, q url.Values, after string) error {
	// Always ask both sides for encoding-type=url and decode the keys here, whatever the client
	// asked for. Backends disagree about what that encoding means — Garage 2.3.0 percent-encodes
	// "/" in a key, MinIO leaves it — and two spellings of one key would merge as two objects
	// (docs/backend-compat.md). Decoding normalizes them; writeListing re-encodes on the way out.
	v := url.Values{"list-type": {"2"}, "max-keys": {strconv.Itoa(maxListKeys)}, "encoding-type": {"url"}}
	for _, k := range []string{"prefix", "delimiter", "fetch-owner"} {
		if x := q.Get(k); x != "" {
			v.Set(k, x)
		}
	}
	switch {
	case s.used != "":
		v.Set("continuation-token", s.used)
	case after != "":
		v.Set("start-after", after)
	case q.Get("start-after") != "":
		v.Set("start-after", q.Get("start-after"))
	}
	resp, err := h.clusterDo(ctx, s.cl, http.MethodGet, "/"+s.backend+"?"+v.Encode(), nil, o)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound && errorCode(body) == string(s3.NoSuchBucket) {
		s.done, s.missing, s.items = true, true, nil
		return errSideMissing
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("listing %s on %s: HTTP %d %s", s.backend, s.cl.Name, resp.StatusCode, errorCode(body))
	}
	var page s3response.ListObjectsV2Result
	if err := xml.Unmarshal(body, &page); err != nil {
		return fmt.Errorf("listing %s on %s: %w", s.backend, s.cl.Name, err)
	}
	s.items = s.items[:0]
	for _, c := range page.Contents {
		if c.Key != nil {
			s.items = append(s.items, listItem{name: decodeKey(*c.Key), obj: c})
		}
	}
	for _, cp := range page.CommonPrefixes {
		s.items = append(s.items, listItem{name: decodeKey(cp.Prefix), prefix: true})
	}
	sort.Slice(s.items, func(i, j int) bool { return s.items[i].name < s.items[j].name })
	for len(s.items) > 0 && after != "" && s.items[0].name <= after {
		s.items = s.items[1:]
	}
	s.started, s.next = true, ""
	if page.IsTruncated != nil && *page.IsTruncated && page.NextContinuationToken != nil {
		s.next = *page.NextContinuationToken
	}
	if len(s.items) == 0 && s.next == "" {
		s.done = true
	}
	return nil
}

// decodeKey undoes the percent-encoding of an encoding-type=url listing. A backend that ignores
// encoding-type returns the key verbatim, which decodes to itself unless the key really contains a
// percent escape; that is the one key shape this normalization cannot tell apart, and it is
// recorded in docs/backend-compat.md rather than guessed at.
func decodeKey(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	if d, err := url.PathUnescape(s); err == nil {
		return d
	}
	return s
}

// writeListing renders the merged page as a ListBucketResult naming the client's bucket.
func (h *Handler) writeListing(w http.ResponseWriter, o *outcome, q url.Values, items []listItem, truncated bool, next string, maxKeys int) {
	// The client's own encoding-type governs what it gets back, exactly as a single backend would.
	name := func(s string) string { return s }
	if strings.EqualFold(q.Get("encoding-type"), "url") {
		name = sigv4.Escape
	}
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>`)
	_ = xml.EscapeText(&b, []byte(o.info.Bucket))
	b.WriteString(`</Name><Prefix>`)
	_ = xml.EscapeText(&b, []byte(name(q.Get("prefix"))))
	b.WriteString(`</Prefix>`)
	if d := q.Get("delimiter"); d != "" {
		b.WriteString(`<Delimiter>`)
		_ = xml.EscapeText(&b, []byte(name(d)))
		b.WriteString(`</Delimiter>`)
	}
	if e := q.Get("encoding-type"); e != "" {
		b.WriteString(`<EncodingType>`)
		_ = xml.EscapeText(&b, []byte(e))
		b.WriteString(`</EncodingType>`)
	}
	fmt.Fprintf(&b, `<KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>`, len(items), maxKeys, truncated)
	if t := q.Get("continuation-token"); t != "" {
		b.WriteString(`<ContinuationToken>`)
		_ = xml.EscapeText(&b, []byte(t))
		b.WriteString(`</ContinuationToken>`)
	}
	if next != "" {
		b.WriteString(`<NextContinuationToken>`)
		_ = xml.EscapeText(&b, []byte(next))
		b.WriteString(`</NextContinuationToken>`)
	}
	for i := range items {
		it := &items[i]
		if it.prefix {
			continue
		}
		b.WriteString(`<Contents><Key>`)
		_ = xml.EscapeText(&b, []byte(name(it.name)))
		b.WriteString(`</Key>`)
		if lm := it.obj.LastModified; lm != nil {
			fmt.Fprintf(&b, `<LastModified>%s</LastModified>`, lm.UTC().Format("2006-01-02T15:04:05.000Z"))
		}
		if et := it.obj.ETag; et != nil {
			b.WriteString(`<ETag>`)
			_ = xml.EscapeText(&b, []byte(*et))
			b.WriteString(`</ETag>`)
		}
		if sz := it.obj.Size; sz != nil {
			fmt.Fprintf(&b, `<Size>%d</Size>`, *sz)
		}
		if sc := string(it.obj.StorageClass); sc != "" {
			b.WriteString(`<StorageClass>` + sc + `</StorageClass>`)
		}
		b.WriteString(`</Contents>`)
	}
	for i := range items {
		it := &items[i]
		if !it.prefix {
			continue
		}
		b.WriteString(`<CommonPrefixes><Prefix>`)
		_ = xml.EscapeText(&b, []byte(name(it.name)))
		b.WriteString(`</Prefix></CommonPrefixes>`)
	}
	b.WriteString(`</ListBucketResult>`)
	hd := w.Header()
	hd.Set("Content-Type", "application/xml")
	hd.Set("Content-Length", strconv.Itoa(b.Len()))
	w.WriteHeader(http.StatusOK)
	o.status = http.StatusOK
	n, _ := w.Write(b.Bytes())
	o.bytesOut = int64(n)
}
