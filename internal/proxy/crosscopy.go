package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// maxStreamedCopy is the largest object shunt copies between clusters as one PUT: S3's own limit
// for a single PUT. A bigger object is copied part by part when its source is multipart, and
// refused otherwise, with the client told to use a multipart copy (ADR-0014).
const maxStreamedCopy = 5 << 30

// copySourceHeaders are the client's conditions on the source object. They are evaluated by the
// cluster shunt reads from, as S3 evaluates them on the source.
var copySourceHeaders = map[string]string{
	"X-Amz-Copy-Source-If-Match":            "If-Match",
	"X-Amz-Copy-Source-If-None-Match":       "If-None-Match",
	"X-Amz-Copy-Source-If-Modified-Since":   "If-Modified-Since",
	"X-Amz-Copy-Source-If-Unmodified-Since": "If-Unmodified-Since",
}

// metadataHeaders travel with a copy whose directive is COPY: what the source object says about
// its own bytes. x-amz-meta-* is carried too, by prefix.
var metadataHeaders = []string{"Content-Type", "Content-Encoding", "Content-Language", "Content-Disposition", "Cache-Control", "Expires"}

// copyPlan is a resolved cross-cluster copy: which cluster holds the source object, under which
// bucket name, and what the client asked for.
type copyPlan struct {
	src          *upstream.Cluster
	srcBackend   string
	srcFallback  *upstream.Cluster // the other side of a migrating source bucket, tried on 404
	fallbackName string
	rawKey       string // the source key, escaped for a request path
	versionID    string
}

// resolveCopySource reads x-amz-copy-source and decides how the copy can be made: rewritten for the
// backend to do itself (same cluster), or streamed by shunt (returned plan), or refused.
func (h *Handler) resolveCopySource(v, tenant string, cred sigv4.Credential, snap *directory.Snapshot,
	clusters *upstream.Set, dst *upstream.Cluster,
) (rewritten string, plan *copyPlan, code s3.Code, message string) {
	lead := ""
	s := v
	if strings.HasPrefix(s, "/") {
		lead, s = "/", s[1:]
	}
	seg, rest, sep := s, "", "/"
	switch i := strings.IndexByte(s, '/'); {
	case i >= 0:
		seg, rest = s[:i], s[i+1:]
	default:
		j := strings.Index(strings.ToUpper(s), "%2F")
		if j < 0 {
			return "", nil, s3.InvalidArgument, "x-amz-copy-source must name a bucket and a key."
		}
		seg, rest, sep = s[:j], s[j+3:], s[j:j+3]
	}
	bucket, err := url.PathUnescape(seg)
	if err != nil {
		return "", nil, s3.InvalidArgument, "x-amz-copy-source is not valid."
	}
	rawKey, versionID := rest, ""
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		rawKey = rest[:i]
		if q, qerr := url.ParseQuery(rest[i+1:]); qerr == nil {
			versionID = q.Get("versionId")
		}
	}
	sp, ok := snap.Lookup(tenant, bucket)
	if !ok || !allowed(cred, bucket) {
		return "", nil, s3.NoSuchBucket, "The source bucket does not exist."
	}
	// The backend can copy for itself only when the object is certainly on the cluster doing the
	// copy: one cluster, and that cluster is the destination's.
	settled := sp.State == directory.StateActive || sp.State == directory.StateCutover
	if settled && sp.Primary == dst.Name && sp.Names[dst.Name] != "" {
		return lead + sp.Names[dst.Name] + sep + rest, nil, "", ""
	}

	key, err := url.PathUnescape(rawKey)
	if err != nil {
		return "", nil, s3.InvalidArgument, "x-amz-copy-source is not valid."
	}
	route, rerr := migrate.Decide(sp, migrate.ClassRead, key)
	if rerr != nil {
		return "", nil, s3.ServiceUnavailable, "The source bucket's migration ramp cannot be routed by this proxy version."
	}
	readFrom := sp.Primary
	if route.Cluster == migrate.Source {
		readFrom = sp.Source
	}
	cl, found := clusters.Get(readFrom)
	if !found || sp.Names[readFrom] == "" {
		return "", nil, s3.NoSuchKey, "The source object's cluster is not available."
	}
	p := &copyPlan{src: cl, srcBackend: sp.Names[readFrom], rawKey: rawKey, versionID: versionID}
	if route.Fallback && sp.Source != "" {
		other := sp.Source
		if readFrom == sp.Source {
			other = sp.Primary
		}
		if oc, ok2 := clusters.Get(other); ok2 && sp.Names[other] != "" {
			p.srcFallback, p.fallbackName = oc, sp.Names[other]
		}
	}
	return "", p, "", ""
}

// streamCopy answers a CopyObject or UploadPartCopy whose source is on another cluster, by reading
// it from there and writing it here (ADR-0014). A multipart source keeps its part layout, so its
// ETag survives the copy; a single object is one streamed PUT.
func (h *Handler) streamCopy(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome,
	plan *copyPlan, dst *upstream.Cluster, dstBackend string, cond condAction,
) {
	start := time.Now()
	if h.wantsRoute(r) {
		w.Header().Set(headerRoute, "copy "+plan.src.Name+"→"+dst.Name)
	}
	head, code, msg := h.copySourceHead(ctx, o, r, plan)
	if code != "" {
		h.answer(w, r, o, code, msg)
		return
	}
	size, _ := strconv.ParseInt(head.Header.Get("Content-Length"), 10, 64)
	etag := head.Header.Get("ETag")

	if o.info.Op == s3.OpUploadPartCopy {
		h.copyPart(ctx, w, r, o, plan, dst, dstBackend, size, start)
		return
	}
	parts := multipartCount(etag)
	switch {
	case parts > 1:
		h.copyByParts(ctx, w, r, o, plan, dst, dstBackend, head, parts, start)
	case size > maxStreamedCopy:
		h.answer(w, r, o, s3.EntityTooLarge, fmt.Sprintf("This copy crosses clusters, so shunt streams it, and %d bytes is over the %d-byte limit of a single PUT. Copy it with a multipart upload (UploadPartCopy).", size, maxStreamedCopy))
	default:
		h.copyWhole(ctx, w, r, o, plan, dst, dstBackend, head, size, cond, start)
	}
}

// copySourceHead reads the source object's metadata, applying the client's copy-source conditions,
// and falls back to the other cluster of a migrating source bucket on 404.
// On a fallback it moves the plan to the cluster that answered, so every later read of the source
// uses that cluster **and its bucket name**.
func (h *Handler) copySourceHead(ctx context.Context, o *outcome, r *http.Request, plan *copyPlan) (*http.Response, s3.Code, string) {
	try := func(cl *upstream.Cluster, backend string) (*http.Response, s3.Code, string) {
		req, err := h.copyRequest(ctx, o, http.MethodHead, cl, backend, plan.rawKey, plan.versionQuery(), nil, 0, emptyPayloadHash)
		if err != nil {
			return nil, s3.InternalError, ""
		}
		for from, to := range copySourceHeaders {
			if v := r.Header.Get(from); v != "" {
				req.Header.Set(to, v)
			}
		}
		resp, err := cl.Transport.RoundTrip(req)
		if err != nil {
			return nil, s3.ServiceUnavailable, "The source cluster could not be reached for this copy."
		}
		drain(resp)
		switch resp.StatusCode {
		case http.StatusOK:
			return resp, "", ""
		case http.StatusNotFound:
			return nil, s3.NoSuchKey, ""
		case http.StatusPreconditionFailed, http.StatusNotModified:
			return nil, s3.PreconditionFailed, "The condition on the copy source did not hold."
		}
		return nil, s3.InternalError, "The source object could not be read for this copy."
	}
	resp, code, msg := try(plan.src, plan.srcBackend)
	if code == s3.NoSuchKey && plan.srcFallback != nil {
		alt, c2, m2 := try(plan.srcFallback, plan.fallbackName)
		switch {
		case c2 == "":
			plan.src, plan.srcBackend = plan.srcFallback, plan.fallbackName
			return alt, "", ""
		case c2 != s3.NoSuchKey:
			return nil, c2, m2
		}
	}
	if code != "" {
		return nil, code, msg
	}
	return resp, "", ""
}

// copyWhole streams one object from the source cluster into one PUT on the destination.
func (h *Handler) copyWhole(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome,
	plan *copyPlan, dst *upstream.Cluster, dstBackend string, head *http.Response, size int64, cond condAction, start time.Time,
) {
	body, code, msg := h.copySourceBody(ctx, o, r, plan, "")
	if code != "" {
		h.answer(w, r, o, code, msg)
		return
	}
	defer body.Close() //nolint:errcheck // the source response body

	req, err := h.copyRequest(ctx, o, http.MethodPut, dst, dstBackend, rawKeyOf(r, o.info), nil, body, size, sigv4.UnsignedPayload)
	if err != nil {
		h.answer(w, r, o, s3.InternalError, "")
		return
	}
	applyCopyMetadata(req.Header, r.Header, head.Header)
	applyCondAction(req.Header, r.Header, cond)
	resp, err := dst.Transport.RoundTrip(req)
	if err != nil {
		h.answer(w, r, o, s3.ServiceUnavailable, "The destination cluster could not be reached for this copy.")
		return
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		h.copyBackendError(w, r, o, dst, resp)
		return
	}
	h.writeCopyResult(w, o, "CopyObjectResult", resp.Header.Get("ETag"), start)
}

// copyByParts reproduces a multipart source part for part, so the copy keeps the source's ETag
// shape (docs/DESIGN.md §2.5, the mover's part-layout rule).
func (h *Handler) copyByParts(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome,
	plan *copyPlan, dst *upstream.Cluster, dstBackend string, head *http.Response, parts int, start time.Time,
) {
	uploadID, code, msg := h.createUpload(ctx, o, r, dst, dstBackend, head)
	if code != "" {
		h.answer(w, r, o, code, msg)
		return
	}
	etags := make([]string, 0, parts)
	for n := 1; n <= parts; n++ {
		etag, c, m := h.copyOnePart(ctx, o, r, plan, dst, dstBackend, uploadID, n)
		if c != "" {
			h.abortUpload(ctx, o, dst, dstBackend, rawKeyOf(r, o.info), uploadID)
			h.answer(w, r, o, c, m)
			return
		}
		etags = append(etags, etag)
	}
	etag, code, msg := h.completeUpload(ctx, o, dst, dstBackend, rawKeyOf(r, o.info), uploadID, etags)
	if code != "" {
		h.abortUpload(ctx, o, dst, dstBackend, rawKeyOf(r, o.info), uploadID)
		h.answer(w, r, o, code, msg)
		return
	}
	h.writeCopyResult(w, o, "CopyObjectResult", etag, start)
}

// copyPart answers UploadPartCopy: the requested byte range of the source object, written as one
// part of an upload the client already started on this cluster.
func (h *Handler) copyPart(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome,
	plan *copyPlan, dst *upstream.Cluster, dstBackend string, size int64, start time.Time,
) {
	rng := r.Header.Get("X-Amz-Copy-Source-Range")
	body, code, msg := h.copySourceBody(ctx, o, r, plan, rng)
	if code != "" {
		h.answer(w, r, o, code, msg)
		return
	}
	defer body.Close() //nolint:errcheck // the source response body
	length := size
	if rng != "" {
		length = rangeLength(rng, size)
	}
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	if _, backendID, prefixed := migrate.DecodeUploadID(uploadID); prefixed {
		uploadID = backendID
	}
	req, err := h.copyRequest(ctx, o, http.MethodPut, dst, dstBackend, rawKeyOf(r, o.info),
		url.Values{"uploadId": {uploadID}, "partNumber": {q.Get("partNumber")}}, body, length, sigv4.UnsignedPayload)
	if err != nil {
		h.answer(w, r, o, s3.InternalError, "")
		return
	}
	resp, err := dst.Transport.RoundTrip(req)
	if err != nil {
		h.answer(w, r, o, s3.ServiceUnavailable, "The destination cluster could not be reached for this copy.")
		return
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		h.copyBackendError(w, r, o, dst, resp)
		return
	}
	h.writeCopyResult(w, o, "CopyPartResult", resp.Header.Get("ETag"), start)
}

// copyOnePart copies part n of a multipart source into the destination's upload, by asking the
// source for exactly that part.
func (h *Handler) copyOnePart(ctx context.Context, o *outcome, r *http.Request, plan *copyPlan,
	dst *upstream.Cluster, dstBackend, uploadID string, n int,
) (etag string, code s3.Code, message string) {
	q := plan.versionQuery()
	if q == nil {
		q = url.Values{}
	}
	q.Set("partNumber", strconv.Itoa(n))
	req, err := h.copyRequest(ctx, o, http.MethodGet, plan.src, plan.srcBackend, plan.rawKey, q, nil, 0, emptyPayloadHash)
	if err != nil {
		return "", s3.InternalError, ""
	}
	resp, err := plan.src.Transport.RoundTrip(req)
	if err != nil {
		return "", s3.ServiceUnavailable, "The source cluster could not be reached for this copy."
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		drain(resp)
		return "", s3.InternalError, "A part of the source object could not be read for this copy."
	}
	defer resp.Body.Close() //nolint:errcheck // relayed below
	length, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	put, err := h.copyRequest(ctx, o, http.MethodPut, dst, dstBackend, rawKeyOf(r, o.info),
		url.Values{"uploadId": {uploadID}, "partNumber": {strconv.Itoa(n)}}, resp.Body, length, sigv4.UnsignedPayload)
	if err != nil {
		return "", s3.InternalError, ""
	}
	pr, err := dst.Transport.RoundTrip(put)
	if err != nil {
		return "", s3.ServiceUnavailable, "The destination cluster could not be reached for this copy."
	}
	defer drain(pr)
	if pr.StatusCode != http.StatusOK {
		return "", s3.InternalError, "A part of this copy could not be written."
	}
	return pr.Header.Get("ETag"), "", ""
}

// copySourceBody starts the read of the source object, with the client's copy-source conditions and
// an optional byte range.
func (h *Handler) copySourceBody(ctx context.Context, o *outcome, r *http.Request, plan *copyPlan, rng string) (io.ReadCloser, s3.Code, string) {
	req, err := h.copyRequest(ctx, o, http.MethodGet, plan.src, plan.srcBackend, plan.rawKey, plan.versionQuery(), nil, 0, emptyPayloadHash)
	if err != nil {
		return nil, s3.InternalError, ""
	}
	for from, to := range copySourceHeaders {
		if v := r.Header.Get(from); v != "" {
			req.Header.Set(to, v)
		}
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := plan.src.Transport.RoundTrip(req)
	if err != nil {
		return nil, s3.ServiceUnavailable, "The source cluster could not be reached for this copy."
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, "", ""
	case http.StatusNotFound:
		drain(resp)
		return nil, s3.NoSuchKey, ""
	case http.StatusPreconditionFailed, http.StatusNotModified:
		drain(resp)
		return nil, s3.PreconditionFailed, "The condition on the copy source did not hold."
	}
	drain(resp)
	return nil, s3.InternalError, "The source object could not be read for this copy."
}

func (h *Handler) createUpload(ctx context.Context, o *outcome, r *http.Request, dst *upstream.Cluster, dstBackend string, head *http.Response) (uploadID string, code s3.Code, message string) {
	req, err := h.copyRequest(ctx, o, http.MethodPost, dst, dstBackend, rawKeyOf(r, o.info), url.Values{"uploads": {""}}, nil, 0, emptyPayloadHash)
	if err != nil {
		return "", s3.InternalError, ""
	}
	applyCopyMetadata(req.Header, r.Header, head.Header)
	resp, err := dst.Transport.RoundTrip(req)
	if err != nil {
		return "", s3.ServiceUnavailable, "The destination cluster could not be reached for this copy."
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return "", s3.InternalError, "This copy could not be started on the destination cluster."
	}
	var out struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(body, &out); err != nil || out.UploadID == "" {
		return "", s3.InternalError, "This copy could not be started on the destination cluster."
	}
	return out.UploadID, "", ""
}

func (h *Handler) completeUpload(ctx context.Context, o *outcome, dst *upstream.Cluster, dstBackend, rawKey, uploadID string, etags []string) (etag string, code s3.Code, message string) {
	var b bytes.Buffer
	b.WriteString(`<CompleteMultipartUpload>`)
	for i, etag := range etags {
		fmt.Fprintf(&b, `<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>`, i+1, etag)
	}
	b.WriteString(`</CompleteMultipartUpload>`)
	payload := b.Bytes()
	req, err := h.copyRequest(ctx, o, http.MethodPost, dst, dstBackend, rawKey, url.Values{"uploadId": {uploadID}}, bytes.NewReader(payload), int64(len(payload)), hashBytes(payload))
	if err != nil {
		return "", s3.InternalError, ""
	}
	resp, err := dst.Transport.RoundTrip(req)
	if err != nil {
		return "", s3.ServiceUnavailable, "The destination cluster could not be reached for this copy."
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return "", s3.InternalError, "This copy could not be completed on the destination cluster."
	}
	var out struct {
		ETag string `xml:"ETag"`
	}
	_ = xml.Unmarshal(body, &out) //nolint:errcheck // an ETag-less answer is still a completed copy
	return out.ETag, "", ""
}

func (h *Handler) abortUpload(ctx context.Context, o *outcome, dst *upstream.Cluster, dstBackend, rawKey, uploadID string) {
	req, err := h.copyRequest(context.WithoutCancel(ctx), o, http.MethodDelete, dst, dstBackend, rawKey, url.Values{"uploadId": {uploadID}}, nil, 0, emptyPayloadHash)
	if err != nil {
		return
	}
	if resp, rerr := dst.Transport.RoundTrip(req); rerr == nil {
		drain(resp)
	}
}

// copyRequest builds and signs one request shunt makes on its own behalf during a copy.
func (h *Handler) copyRequest(ctx context.Context, o *outcome, method string, cl *upstream.Cluster, backend, rawKey string,
	q url.Values, body io.Reader, length int64, payloadHash string,
) (*http.Request, error) {
	ep := cl.Next()
	target := "/" + backend
	if rawKey != "" {
		target += "/" + rawKey
	}
	if len(q) > 0 {
		target += "?" + strings.ReplaceAll(q.Encode(), "+", "%20")
	}
	req, err := http.NewRequestWithContext(ctx, method, cl.Scheme+"://"+ep+target, body) //nolint:gosec // G704: endpoint from the directory
	if err != nil {
		return nil, err
	}
	req.Host = ep
	req.ContentLength = length
	if body == nil {
		req.Body = http.NoBody
	}
	appendVia(req.Header, h.Via)
	req.Header.Set(telemetry.HeaderRequestID, o.rid)
	sigv4.Sign(req, cl.Creds, cl.Region, payloadHash, time.Now())
	return req, nil
}

// applyCopyMetadata puts the source object's own metadata on the write, or the client's when it
// asked for REPLACE, as S3's x-amz-metadata-directive says.
func applyCopyMetadata(dst, client, source http.Header) {
	from := source
	if strings.EqualFold(client.Get("X-Amz-Metadata-Directive"), "REPLACE") {
		from = client
	}
	for _, name := range metadataHeaders {
		if v := from.Get(name); v != "" {
			dst.Set(name, v)
		}
	}
	for name, vs := range from {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-meta-") {
			for _, v := range vs {
				dst.Add(name, v)
			}
		}
	}
	if v := client.Get("X-Amz-Storage-Class"); v != "" {
		dst.Set("X-Amz-Storage-Class", v)
	}
}

// applyCondAction carries the client's own write conditions, as ADR-0013 resolved them, onto the
// destination write of a copy.
func applyCondAction(dst, client http.Header, cond condAction) {
	if cond.dropIfMatch {
		if cond.createOnly {
			dst.Set("If-None-Match", "*")
		}
		return
	}
	for _, name := range []string{"If-None-Match", "If-Match"} {
		if v := client.Get(name); v != "" {
			dst.Set(name, v)
		}
	}
}

func (h *Handler) copyBackendError(w http.ResponseWriter, r *http.Request, o *outcome, dst *upstream.Cluster, resp *http.Response) {
	code := s3.InternalError
	if resp.StatusCode == http.StatusPreconditionFailed {
		code = s3.PreconditionFailed
	}
	if h.Log != nil {
		h.Log.Warn("a cross-cluster copy could not be written", "request_id", o.rid, "cluster", dst.Name, "status", resp.StatusCode)
	}
	h.answer(w, r, o, code, "The destination cluster refused this copy.")
}

// writeCopyResult answers the client with the XML a backend would have sent.
func (h *Handler) writeCopyResult(w http.ResponseWriter, o *outcome, element, etag string, start time.Time) {
	var b bytes.Buffer
	b.WriteString(xml.Header)
	fmt.Fprintf(&b, `<%s><LastModified>%s</LastModified><ETag>`, element, start.UTC().Format("2006-01-02T15:04:05.000Z"))
	_ = xml.EscapeText(&b, []byte(etag))
	fmt.Fprintf(&b, `</ETag></%s>`, element)
	hd := w.Header()
	hd.Set("Content-Type", "application/xml")
	hd.Set("Content-Length", strconv.Itoa(b.Len()))
	o.status = http.StatusOK
	w.WriteHeader(http.StatusOK)
	n, _ := w.Write(b.Bytes())
	o.bytesOut = int64(n)
}

// versionQuery is the source object's versionId, when the client named one.
func (p *copyPlan) versionQuery() url.Values {
	if p.versionID == "" {
		return nil
	}
	return url.Values{"versionId": {p.versionID}}
}

// multipartCount reads the part count out of a multipart ETag ("<hex>-<n>"); 1 for a plain object.
func multipartCount(etag string) int {
	etag = strings.Trim(etag, `"`)
	i := strings.LastIndexByte(etag, '-')
	if i < 0 {
		return 1
	}
	n, err := strconv.Atoi(etag[i+1:])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// rangeLength is the number of bytes a "bytes=a-b" range covers of an object of this size.
func rangeLength(rng string, size int64) int64 {
	spec, ok := strings.CutPrefix(strings.TrimSpace(rng), "bytes=")
	if !ok {
		return size
	}
	first, last, found := strings.Cut(spec, "-")
	a, err := strconv.ParseInt(strings.TrimSpace(first), 10, 64)
	if err != nil {
		return size
	}
	if !found || strings.TrimSpace(last) == "" {
		return size - a
	}
	b, err := strconv.ParseInt(strings.TrimSpace(last), 10, 64)
	if err != nil || b < a {
		return size
	}
	return b - a + 1
}

// rawKeyOf is the request's own object key, escaped as the client sent it.
func rawKeyOf(r *http.Request, info s3.RequestInfo) string {
	raw := sigv4.RawPath(r)
	if info.Style == s3.StyleVirtualHost {
		return strings.TrimPrefix(raw, "/")
	}
	rest := strings.TrimPrefix(raw, "/")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[i+1:]
	}
	return ""
}

// hashBytes is the payload hash of a small body shunt composes itself.
func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}
