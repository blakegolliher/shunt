package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// afterPrefix follows a common prefix in a resumed listing: every key under the prefix sorts before
// it. Garage lists the prefix itself again after start-after=<prefix> and MinIO does not
// (docs/reference/backend-compat.md); with this suffix neither does, and the merge drops anything
// at or before the last name it emitted whatever a backend answers.
const afterPrefix = "\U0010FFFF"

// spreadToken is shunt's continuation token for a bucket spread over legs (ADR-0018 N2). It does
// not grow with the legs: every leg resumes after Last, and Done names the legs already exhausted.
type spreadToken struct {
	Last   string   `json:"l,omitempty"`
	Prefix bool     `json:"p,omitempty"` // Last was a common prefix
	Done   []string `json:"d,omitempty"`
}

func (t spreadToken) encode() string {
	b, _ := json.Marshal(t) //nolint:errcheck // strings and a bool always marshal
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeSpreadToken(s string) (spreadToken, error) {
	var t spreadToken
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return t, err
	}
	if err := json.Unmarshal(b, &t); err != nil {
		return t, err
	}
	if t.Last == "" && (t.Prefix || len(t.Done) > 0) {
		return t, errors.New("a token that resumes nowhere")
	}
	return t, nil
}

// spreadLeg is one leg's paginated stream, read like one side of mergeListing and filtered to the
// keys the leg owns.
type spreadLeg struct {
	id string
	*listSide
}

// spreadListing answers ListObjectsV2 and ListObjects (v1) for a bucket spread over legs: a sorted
// merge of every leg's listing, each keeping only the keys it owns, common prefixes once. It holds
// at most one page per leg (maxListKeys entries each), MaxLegs pages in all (ADR-0019). A leg whose
// bucket is missing fails the listing: an answer without it would be silently incomplete.
func (h *Handler) spreadListing(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome, p *directory.Placement, clusters *upstream.Set) {
	start := time.Now()
	q := r.URL.Query()
	v1 := o.info.Op == s3.OpListObjects
	maxKeys := maxListKeys
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n < maxListKeys {
			maxKeys = n
		}
	}
	var tok spreadToken
	switch {
	case v1 && q.Get("marker") != "":
		m := q.Get("marker")
		d := q.Get("delimiter")
		tok = spreadToken{Last: m, Prefix: d != "" && strings.HasSuffix(m, d)}
	case !v1 && q.Get("continuation-token") != "":
		t, err := decodeSpreadToken(q.Get("continuation-token"))
		if err != nil {
			h.answer(w, r, o, s3.InvalidArgument, "The continuation token is not one this bucket issued.")
			return
		}
		tok = t
	case !v1 && q.Get("start-after") != "":
		tok = spreadToken{Last: q.Get("start-after")}
	}
	after := tok.Last
	if tok.Prefix {
		after += afterPrefix
	}

	// Keys interleave across legs by hash, so each leg holds about maxKeys/n of a page; a quarter
	// more plus a little covers the variance, and a leg that runs short reads its next page. Memory
	// and backend reads then scale with the client's page, not with maxKeys times the legs.
	live := len(p.Legs) - len(tok.Done)
	pageSize := max(maxKeys, 1)
	if live > 1 {
		pageSize = max(min(maxKeys, maxKeys/live+maxKeys/(4*live)+16), 1)
	}
	legs := make([]*spreadLeg, 0, len(p.Legs))
	routes := make([]string, 0, len(p.Legs))
	for _, id := range slices.Sorted(maps.Keys(p.Legs)) {
		l := p.Legs[id]
		cl, ok := clusters.Get(l.Cluster)
		if !ok {
			h.answer(w, r, o, s3.ServiceUnavailable, fmt.Sprintf("Cluster %s of this bucket is not available to this proxy.", l.Cluster))
			return
		}
		routes = append(routes, l.Cluster)
		legs = append(legs, &spreadLeg{id: id, listSide: &listSide{cl: cl, backend: l.Bucket, done: slices.Contains(tok.Done, id), page: pageSize}})
	}
	if h.wantsRoute(r) {
		w.Header().Set(headerRoute, "spread "+strings.Join(routes, "+"))
	}

	// A leg lists the keys it owns; while a move is under way the keys in its range may be on either
	// of its two legs, and the destination's copy wins, as in a migration's merge (ADR-0018 N3).
	own := func(l *spreadLeg) {
		l.items = slices.DeleteFunc(l.items, func(it listItem) bool {
			if it.prefix {
				return false
			}
			if m := p.Move; m != nil && (l.id == m.From || l.id == m.To) && migrate.InRangeHash(m.Range, it.name) {
				return false
			}
			owner, err := migrate.OwnerOf(p, it.name)
			return err != nil || owner != l.id
		})
	}
	fetch := func(l *spreadLeg, o *outcome) error {
		if err := h.fetchListPage(ctx, o, l.listSide, q, after); err != nil {
			if errors.Is(err, errSideMissing) {
				err = fmt.Errorf("leg %s: bucket %s is missing on %s", l.id, l.backend, l.cl.Name)
			}
			return err
		}
		own(l)
		return nil
	}
	// Every leg's first page at once: the listing waits for the slowest leg, not for all of them in
	// turn. Each read gets its own copy of the outcome, which a read writes to.
	errs := make([]error, len(legs))
	var wg sync.WaitGroup
	for i, l := range legs {
		if l.done {
			continue
		}
		wg.Go(func() {
			oc := *o
			errs[i] = fetch(l, &oc)
		})
	}
	wg.Wait()
	failed := errors.Join(errs...)
	head := func(l *spreadLeg) *listItem {
		for failed == nil && len(l.items) == 0 {
			switch {
			case l.done:
				return nil
			case l.started && l.next == "":
				l.done = true
				return nil
			case l.started:
				l.used = l.next
			}
			if err := fetch(l, o); err != nil {
				failed = err
				return nil
			}
		}
		if failed != nil || len(l.items) == 0 {
			return nil
		}
		return &l.items[0]
	}

	emitted := make([]listItem, 0, min(maxKeys, 256))
	for len(emitted) < maxKeys {
		var least *listItem
		for _, l := range legs {
			if it := head(l); it != nil && (least == nil || it.name < least.name) {
				least = it
			}
		}
		if failed != nil {
			h.upstreamError(w, r, o, failed)
			return
		}
		if least == nil {
			break
		}
		take := *least
		for _, l := range legs { // a common prefix several legs hold is one entry; a moving key, the destination's
			if len(l.items) > 0 && l.items[0].name == take.name {
				if p.Move != nil && l.id == p.Move.To {
					take = l.items[0]
				}
				l.items = l.items[1:]
			}
		}
		emitted = append(emitted, take)
	}

	truncated := false
	var done []string
	for _, l := range legs {
		if head(l) != nil {
			truncated = true
		} else {
			done = append(done, l.id)
		}
	}
	if failed != nil {
		h.upstreamError(w, r, o, failed)
		return
	}
	next := ""
	if truncated && len(emitted) > 0 {
		last := emitted[len(emitted)-1]
		next = spreadToken{Last: last.name, Prefix: last.prefix, Done: done}.encode()
	}
	h.Metrics.ListingMerge.WithLabelValues(directory.Key(o.tenant, o.info.Bucket)).Observe(time.Since(start).Seconds())
	if v1 {
		nextMarker := ""
		if truncated && len(emitted) > 0 {
			nextMarker = emitted[len(emitted)-1].name
		}
		h.writeListingV1(w, o, q, emitted, truncated, nextMarker, maxKeys)
		return
	}
	h.writeListing(w, o, q, emitted, truncated, next, maxKeys)
}

// writeListingV1 renders a merged page as ListObjects (v1) answers: Marker for where it started,
// NextMarker for where the next page starts.
func (h *Handler) writeListingV1(w http.ResponseWriter, o *outcome, q url.Values, items []listItem, truncated bool, nextMarker string, maxKeys int) {
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
	b.WriteString(`</Prefix><Marker>`)
	_ = xml.EscapeText(&b, []byte(name(q.Get("marker"))))
	b.WriteString(`</Marker>`)
	if nextMarker != "" {
		b.WriteString(`<NextMarker>`)
		_ = xml.EscapeText(&b, []byte(name(nextMarker)))
		b.WriteString(`</NextMarker>`)
	}
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
	fmt.Fprintf(&b, `<MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>`, maxKeys, truncated)
	writeListEntries(&b, items, name)
	b.WriteString(`</ListBucketResult>`)
	hd := w.Header()
	hd.Set("Content-Type", "application/xml")
	hd.Set("Content-Length", strconv.Itoa(b.Len()))
	w.WriteHeader(http.StatusOK)
	o.status = http.StatusOK
	n, _ := w.Write(b.Bytes())
	o.bytesOut = int64(n)
}
