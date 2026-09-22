package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/s3/xmlrw"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// createAttempts bounds the backend names tried for one CreateBucket.
const createAttempts = 3

// answer is shunt answering a request itself with an S3 error.
func (h *Handler) answer(w http.ResponseWriter, r *http.Request, o *outcome, code s3.Code, msg string) {
	e := s3.Lookup(code)
	o.status = e.Status
	o.err = "shunt: " + string(code)
	writeErrorMessage(w, code, e.Status, msg, r.URL.Path, o.rid)
}

// staleFor reports whether this proxy has lost its lease and p is moving (ADR-0016).
func (h *Handler) staleFor(p *directory.Placement) bool {
	return p.State != directory.StateActive && h.Stale != nil && h.Stale()
}

// staleRead widens a read a stale proxy would send to the source only. The proxy may have missed
// ramp steps, and another proxy may already have written the key to the target: target first,
// then source, is right whatever step it missed, since a stale proxy writes nothing to a moving
// bucket and the target only ever holds the newer copy.
func staleRead(stale bool, class migrate.OpClass, route migrate.Route) migrate.Route {
	if stale && class == migrate.ClassRead && route.Cluster == migrate.Source && !route.Fallback {
		return migrate.Route{Cluster: migrate.Primary, Fallback: true}
	}
	return route
}

// refuseWrite answers a write shunt will not route yet with 503 and Retry-After, which every SDK
// retries (ADR-0016): a held ramp step, or a stale proxy.
func (h *Handler) refuseWrite(w http.ResponseWriter, r *http.Request, o *outcome, bucket, reason, msg string) {
	h.Metrics.RefusedWrites.WithLabelValues(directory.Key(o.tenant, bucket), reason).Inc()
	w.Header().Set("Retry-After", "1")
	h.answer(w, r, o, s3.ServiceUnavailable, msg)
}

// listBuckets synthesizes ListBuckets from the directory: every bucket of the tenant, on every
// cluster, and nothing else. No upstream request is made. prefix is honored; the response is one
// final page.
func (h *Handler) listBuckets(w http.ResponseWriter, r *http.Request, o *outcome, cred sigv4.Credential, snap *directory.Snapshot) {
	prefix := r.URL.Query().Get("prefix")
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString(`<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>`)
	b.WriteString(ownerID(o.tenant))
	b.WriteString(`</ID><DisplayName>`)
	_ = xml.EscapeText(&b, []byte(o.tenant))
	b.WriteString(`</DisplayName></Owner><Buckets>`)
	for _, name := range snap.Buckets(o.tenant) {
		if !strings.HasPrefix(name, prefix) || !allowed(cred, name) {
			continue
		}
		p, _ := snap.Lookup(o.tenant, name)
		b.WriteString(`<Bucket><Name>`)
		_ = xml.EscapeText(&b, []byte(name))
		b.WriteString(`</Name><CreationDate>`)
		b.WriteString(p.Created.UTC().Format("2006-01-02T15:04:05.000Z"))
		b.WriteString(`</CreationDate></Bucket>`)
	}
	b.WriteString(`</Buckets>`)
	if prefix != "" {
		b.WriteString(`<Prefix>`)
		_ = xml.EscapeText(&b, []byte(prefix))
		b.WriteString(`</Prefix>`)
	}
	b.WriteString(`</ListAllMyBucketsResult>`)
	hd := w.Header()
	hd.Set("Content-Type", "application/xml")
	hd.Set("Content-Length", strconv.Itoa(b.Len()))
	w.WriteHeader(http.StatusOK)
	o.status = http.StatusOK
	n, _ := w.Write(b.Bytes())
	o.bytesOut = int64(n)
}

// createBucket creates a bucket on the tenant's default cluster under a generated backend name.
// The placement row is claimed first and the backend bucket created second, so a lost race or a
// failed backend call only ever removes shunt's own row, never a bucket another request created.
// The client's CreateBucketConfiguration is discarded: the directory, not the client, decides
// where the bucket lives.
func (h *Handler) createBucket(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome, cred sigv4.Credential, snap *directory.Snapshot, clusters *upstream.Set) {
	bucket := o.info.Bucket
	switch {
	case !s3.ValidBucketName(bucket):
		h.answer(w, r, o, s3.InvalidBucketName, "")
		return
	case !allowed(cred, bucket):
		h.answer(w, r, o, s3.AccessDenied, "")
		return
	}
	if _, exists := snap.Lookup(o.tenant, bucket); exists {
		h.answer(w, r, o, s3.BucketAlreadyOwnedByYou, "")
		return
	}
	t, ok := snap.Tenant(o.tenant)
	if !ok {
		h.answer(w, r, o, s3.AccessDenied, "The tenant has no default cluster in the bucket directory.")
		return
	}
	cl, ok := clusters.Get(t.DefaultCluster)
	if !ok {
		h.answer(w, r, o, s3.InternalError, "")
		return
	}
	o.cluster, o.clusterType = cl.Name, cl.Type
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
	}
	actor := "proxy:" + o.accessKey
	dctx := context.WithoutCancel(ctx) // a claimed row must be released even if the client leaves

	for attempt := 0; attempt < createAttempts; attempt++ {
		name := directory.BackendName(o.tenant, bucket, attempt)
		err := h.Dir.Create(dctx, o.tenant, bucket, cl.Name, name, actor)
		var verr *config.Error
		switch {
		case err == nil:
		case errors.Is(err, directory.ErrExists):
			h.answer(w, r, o, s3.BucketAlreadyOwnedByYou, "")
			return
		case errors.Is(err, directory.ErrNotFound):
			h.answer(w, r, o, s3.AccessDenied, "The tenant has no entry in the bucket directory.")
			return
		case errors.As(err, &verr):
			continue // the generated name is already used by another placement on this cluster
		case errors.Is(err, directory.ErrReadOnly), errors.Is(err, directory.ErrLockTimeout):
			h.logDirectoryError("CreateBucket could not write the directory", o, bucket, err)
			h.answer(w, r, o, s3.ServiceUnavailable, "The bucket directory is not writable right now. Try again later.")
			return
		default:
			h.logDirectoryError("CreateBucket could not write the directory", o, bucket, err)
			h.answer(w, r, o, s3.InternalError, "")
			return
		}
		o.backend = name
		resp, err := h.clusterDo(ctx, cl, http.MethodPut, "/"+name, createBody(cl), o)
		if err != nil {
			h.releaseRow(dctx, o, bucket, actor, "backend request failed: "+err.Error())
			h.upstreamError(w, r, o, err)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		o.upstreamID = resp.Header.Get("X-Amz-Request-Id")
		switch code := errorCode(body); {
		case resp.StatusCode/100 == 2:
		case resp.StatusCode == http.StatusConflict && code == string(s3.BucketAlreadyOwnedByYou):
			// This cluster's credentials already own the name, and the directory (just written
			// without a conflict) says no placement uses it: a bucket left by an earlier create
			// that failed half way. It is adopted.
			if h.Log != nil {
				h.Log.Warn("CreateBucket adopted an existing backend bucket that no placement used", "request_id", o.rid,
					"tenant", o.tenant, "bucket", bucket, "cluster", cl.Name, "backend_bucket", name)
			}
		case resp.StatusCode == http.StatusConflict && code == string(s3.BucketAlreadyExists):
			h.releaseRow(dctx, o, bucket, actor, "backend name taken")
			continue
		default:
			h.releaseRow(dctx, o, bucket, actor, "backend refused: HTTP "+strconv.Itoa(resp.StatusCode)+" "+code)
			h.relayBackendError(w, r, o, cl, name, resp, body)
			return
		}
		w.Header().Set("Location", "/"+bucket)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		o.status = http.StatusOK
		return
	}
	if h.Log != nil {
		h.Log.Error("CreateBucket found no free backend bucket name", "request_id", o.rid, "tenant", o.tenant, "bucket", bucket, "cluster", cl.Name, "attempts", createAttempts)
	}
	h.answer(w, r, o, s3.InternalError, "No free backend bucket name was found.")
}

func (h *Handler) releaseRow(ctx context.Context, o *outcome, bucket, actor, why string) {
	if err := h.Dir.Delete(ctx, o.tenant, bucket, actor); err != nil && h.Log != nil {
		h.Log.Error("CreateBucket failed after claiming its placement, and removing the placement failed too; the placement names a backend bucket that may not exist",
			"request_id", o.rid, "tenant", o.tenant, "bucket", bucket, "cluster", o.cluster, "backend_bucket", o.backend, "cause", why, "err", err.Error())
	}
}

func (h *Handler) logDirectoryError(msg string, o *outcome, bucket string, err error) {
	if h.Log != nil {
		h.Log.Error(msg, "request_id", o.rid, "tenant", o.tenant, "bucket", bucket, "err", err.Error())
	}
}

// createBody is the CreateBucket body sent upstream: AWS needs a LocationConstraint outside
// us-east-1; every other backend gets none.
func createBody(cl *upstream.Cluster) []byte {
	if cl.Type != "aws" || cl.Region == "" || cl.Region == "us-east-1" {
		return nil
	}
	return []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + cl.Region + `</LocationConstraint></CreateBucketConfiguration>`)
}

// clusterDo sends a small request shunt originates itself (CreateBucket), signed with the
// cluster's credentials, skipping endpoints that refuse the connection.
func (h *Handler) clusterDo(ctx context.Context, cl *upstream.Cluster, method, path string, body []byte, o *outcome) (*http.Response, error) {
	sum := sha256.Sum256(body)
	var lastErr error
	for range cl.Endpoints {
		ep := cl.Next()
		o.upstream = ep
		req, err := http.NewRequestWithContext(ctx, method, cl.Scheme+"://"+ep+path, bytes.NewReader(body)) //nolint:gosec // G704: configured cluster endpoint
		if err != nil {
			return nil, err
		}
		req.Host = ep
		req.Header.Set("User-Agent", "")
		req.Header.Set(telemetry.HeaderRequestID, o.rid)
		appendVia(req.Header, h.Via)
		sigv4.Sign(req, cl.Creds, cl.Region, hex.EncodeToString(sum[:]), time.Now())
		resp, err := cl.Transport.RoundTrip(req)
		if err != nil && upstream.IsConnectError(err) {
			lastErr = err
			continue
		}
		return resp, err
	}
	return nil, lastErr
}

// relayBackendError answers with a backend's error for a request shunt originated, with backend
// names rewritten.
func (h *Handler) relayBackendError(w http.ResponseWriter, r *http.Request, o *outcome, cl *upstream.Cluster, backend string, resp *http.Response, body []byte) {
	out := body
	if h.Rewrite {
		var b bytes.Buffer
		xw := xmlrw.New(&b, newEditor(r, o.info, cl, backend), xmlrw.MaxText)
		_, _ = xw.Write(body)
		_ = xw.Flush()
		out = b.Bytes()
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(out)))
	w.WriteHeader(resp.StatusCode)
	o.status = resp.StatusCode
	o.err = "backend: " + errorCode(body)
	n, _ := w.Write(out)
	o.bytesOut = int64(n)
}

// afterDeleteBucket removes the placement once the backend bucket is gone (204), or was already
// gone (404: a row whose bucket vanished is removed with a warning). It returns false when it
// answered the client itself because the directory could not be written.
func (h *Handler) afterDeleteBucket(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome, p *prepared, resp *http.Response) bool {
	if o.info.Op != s3.OpDeleteBucket || p.placement == nil {
		return true
	}
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return true
	}
	err := h.Dir.Delete(context.WithoutCancel(ctx), o.tenant, o.info.Bucket, "proxy:"+o.accessKey)
	if err == nil || errors.Is(err, directory.ErrNotFound) {
		if resp.StatusCode == http.StatusNotFound && h.Log != nil {
			h.Log.Warn("DeleteBucket: the backend bucket was already gone; placement removed", "request_id", o.rid,
				"tenant", o.tenant, "bucket", o.info.Bucket, "cluster", o.cluster, "backend_bucket", o.backend)
		}
		return true
	}
	h.logDirectoryError("DeleteBucket removed the backend bucket but could not remove its placement; a retry removes it", o, o.info.Bucket, err)
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	h.answer(w, r, o, s3.ServiceUnavailable, "The bucket directory is not writable right now. Try again later.")
	return false
}

// failRedirect answers an upstream redirect with a 500: shunt never follows or relays one, since
// a relayed redirect would send the client to the backend (POC-3: region redirects fail loudly).
func (h *Handler) failRedirect(w http.ResponseWriter, r *http.Request, o *outcome, p *prepared, resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if h.Log != nil {
		h.Log.Error("upstream answered with a redirect; shunt does not relay or follow redirects. Check the cluster's region and endpoints",
			"request_id", o.rid, "cluster", p.cl.Name, "region", p.cl.Region, "status", resp.StatusCode,
			"location", resp.Header.Get("Location"), "bucket_region", resp.Header.Get("X-Amz-Bucket-Region"), "backend_bucket", o.backend)
	}
	h.answer(w, r, o, s3.InternalError, "The backend redirected the request.")
	o.err = "upstream redirect " + strconv.Itoa(resp.StatusCode)
}

func isRedirect(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}

// errorCode extracts <Code> from a small S3 error body.
func errorCode(body []byte) string {
	i := bytes.Index(body, []byte("<Code>"))
	if i < 0 {
		return ""
	}
	rest := body[i+len("<Code>"):]
	j := bytes.Index(rest, []byte("</Code>"))
	if j < 0 {
		return ""
	}
	return string(rest[:j])
}

// fallbackRead answers from the source when the primary has not been backfilled yet. It returns
// nil when the source cannot answer either, leaving the primary's 404 to be relayed untouched.
func (h *Handler) fallbackRead(ctx context.Context, r *http.Request, o *outcome, p *prepared, primary *http.Response) *http.Response {
	side := &outcome{rid: o.rid, info: o.info, tm: &timings{}}
	alt, err := h.roundTrip(ctx, side, p, p.other, p.otherBackend, nil)
	if err != nil || alt.StatusCode >= 400 {
		if alt != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(alt.Body, 64<<10))
			_ = alt.Body.Close()
		}
		if err != nil && h.Log != nil {
			h.Log.Warn("fallback read to the migration source failed; relaying the primary's 404",
				"request_id", o.rid, "bucket", p.bucketKey, "source", p.other.Name, "err", err.Error())
		}
		return nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(primary.Body, 64<<10))
	_ = primary.Body.Close()
	h.Metrics.FallbackReads.WithLabelValues(p.bucketKey).Inc()
	o.cluster, o.clusterType, o.backend, o.upstream = p.other.Name, p.other.Type, p.otherBackend, side.upstream
	if h.Rewrite {
		p.ed = newEditor(r, o.info, p.other, p.otherBackend) // echoes now come from the source
	}
	return alt
}

// deleteOnSource sends a migration delete to the placement's source cluster, before the primary
// gets it, and reports the outcome of that leg: both (the source accepted it), source_missing, or
// source_failed. A failed source leg is logged but does not stop the primary leg: the client asked
// for a delete, and the primary is where its reads go first (docs/DESIGN.md §2.5, ADR-0004).
func (h *Handler) deleteOnSource(ctx context.Context, o *outcome, p *prepared) string {
	side := &outcome{rid: o.rid, info: o.info, tm: &timings{}}
	resp, err := h.roundTrip(context.WithoutCancel(ctx), side, p, p.other, p.otherBackend, nil)
	if err != nil {
		if h.Log != nil {
			h.Log.Error("delete did not reach the migration source; the object can come back when the mover copies it (ADR-0004)",
				"request_id", o.rid, "bucket", p.bucketKey, "source", p.other.Name, "backend_bucket", p.otherBackend, "err", err.Error())
		}
		return "source_failed"
	}
	defer resp.Body.Close() //nolint:errcheck // drained below
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return "source_missing"
	case resp.StatusCode < 300:
		return "both"
	}
	if h.Log != nil {
		h.Log.Error("the migration source refused a delete; the object can come back when the mover copies it (ADR-0004)",
			"request_id", o.rid, "bucket", p.bucketKey, "source", p.other.Name, "status", resp.StatusCode)
	}
	return "source_failed"
}

// countDualDelete meters a migration delete once both legs have run. A primary leg that fails
// after the source leg succeeded is the partial failure docs/migrating.md tells operators about:
// the object is gone from the source but still on the primary, and the client saw an error.
func (h *Handler) countDualDelete(o *outcome, p *prepared, source string, primary *http.Response, err error) {
	outcome := source
	if err != nil || primary.StatusCode >= 300 {
		outcome = "primary_failed"
		if source != "source_failed" && h.Log != nil {
			status := 0
			if primary != nil {
				status = primary.StatusCode
			}
			h.Log.Error("a migration delete removed the object from the source but the primary refused it; the object is still on the primary and the client was told the delete failed (docs/migrating.md)",
				"request_id", o.rid, "bucket", p.bucketKey, "primary", p.cl.Name, "status", status, "source_leg", source)
		}
	}
	h.Metrics.DualDelete.WithLabelValues(p.bucketKey, outcome).Inc()
}
