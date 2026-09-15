package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// refusedOps answer 501 in resign mode: their configuration bodies name buckets (as ARNs or
// destinations) and accounts that differ per backend, and relaying them would either leak a
// backend name or point the backend at a bucket name the client does not know (ADR-0006).
// Bucket policy is not here: it passes through unrewritten (docs/reference/backend-compat.md).
var refusedOps = map[s3.Op]bool{
	s3.OpGetBucketLogging: true, s3.OpPutBucketLogging: true,
	s3.OpGetBucketReplication: true, s3.OpPutBucketReplication: true, s3.OpDeleteBucketReplication: true,
	s3.OpGetBucketInventoryConfiguration: true, s3.OpPutBucketInventoryConfiguration: true,
	s3.OpDeleteBucketInventoryConfiguration: true, s3.OpListBucketInventoryConfigurations: true,
	s3.OpGetBucketAnalyticsConfiguration: true, s3.OpPutBucketAnalyticsConfiguration: true,
	s3.OpDeleteBucketAnalyticsConfiguration: true, s3.OpListBucketAnalyticsConfigurations: true,
	s3.OpGetBucketNotificationConfiguration: true, s3.OpPutBucketNotificationConfiguration: true,
}

// prepareResign verifies the client, resolves the placement, and prepares the upstream request
// for the cluster the placement routes to. It returns false when shunt has already answered.
func (h *Handler) prepareResign(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome, inBody *progressReader) (*prepared, bool) {
	t0 := time.Now()
	id, aerr := sigv4.Verify(ctx, r, h.Store, time.Now(), sigv4.Options{ClockSkew: h.ClockSkew, RequireHash: true})
	if aerr != nil {
		h.Metrics.AuthFailures.WithLabelValues(string(aerr.Reason)).Inc()
		o.status = aerr.Err.Status
		o.err = "auth: " + string(aerr.Reason)
		writeAuthError(w, aerr, r.URL.Path, o.rid)
		return nil, false
	}
	mode := "header"
	if id.Presigned {
		mode = "presigned"
	}
	h.Metrics.AuthDuration.WithLabelValues(mode).Observe(time.Since(t0).Seconds())
	o.tenant, o.accessKey = id.Credential.Tenant, id.Credential.AccessKey
	info := o.info
	snap := h.Dir.Snapshot()

	switch {
	case info.Level == s3.LevelService && info.Op == s3.OpListBuckets:
		h.listBuckets(w, r, o, id.Credential, snap)
		return nil, false
	case info.Level == s3.LevelService:
		h.answer(w, r, o, s3.NotImplemented, "shunt answers only ListBuckets at the service level.")
		return nil, false
	case refusedOps[info.Op]:
		h.answer(w, r, o, s3.NotImplemented, info.Op.String()+" is not supported through shunt: its configuration names buckets and accounts that differ per backend.")
		return nil, false
	case info.Op == s3.OpCreateBucket:
		h.createBucket(ctx, w, r, o, id.Credential, snap)
		return nil, false
	}

	p, ok := snap.Lookup(o.tenant, info.Bucket)
	if !ok || !allowed(id.Credential, info.Bucket) {
		h.answer(w, r, o, s3.NoSuchBucket, "")
		return nil, false
	}
	if !ownerMatches(r.Header, "X-Amz-Expected-Bucket-Owner", o.tenant) || !ownerMatches(r.Header, "X-Amz-Source-Expected-Bucket-Owner", o.tenant) {
		h.answer(w, r, o, s3.AccessDenied, "")
		return nil, false
	}
	if p.State != directory.StateActive && (info.Op == s3.OpPutBucketVersioning || info.Op == s3.OpDeleteBucket) {
		h.answer(w, r, o, s3.InvalidBucketState, fmt.Sprintf("The bucket is %s; %s is refused until its migration completes.", p.State, info.Op))
		return nil, false
	}

	rawQuery := r.URL.RawQuery
	if id.Presigned {
		rawQuery = stripPresign(rawQuery)
	}
	clusterName := p.Route()
	rawQuery, prefix, conflict := rewriteUploadIDs(rawQuery)
	if conflict {
		h.answer(w, r, o, s3.NoSuchUpload, "")
		return nil, false
	}
	if prefix != "" {
		pcl, found := h.Clusters.ByID(prefix)
		if !found || p.Names[pcl.Name] == "" {
			h.answer(w, r, o, s3.NoSuchUpload, "")
			return nil, false
		}
		clusterName = pcl.Name
	}
	cl, ok := h.Clusters.Get(clusterName)
	if !ok {
		if h.Log != nil {
			h.Log.Error("placement routes to a cluster that is not configured", "request_id", o.rid, "tenant", o.tenant, "bucket", info.Bucket, "cluster", clusterName)
		}
		h.answer(w, r, o, s3.InternalError, "")
		return nil, false
	}
	backend := p.Names[cl.Name]
	o.cluster, o.clusterType, o.backend = cl.Name, cl.Type, backend

	copySource := ""
	if v := r.Header.Get("X-Amz-Copy-Source"); v != "" {
		cs, code, msg := rewriteCopySource(v, o.tenant, id.Credential, snap, cl.Name)
		if code != "" {
			h.answer(w, r, o, code, msg)
			return nil, false
		}
		copySource = cs
	}

	plan, perr := h.planBody(r, id, inBody, cl)
	if perr != nil {
		h.Metrics.AuthFailures.WithLabelValues(string(perr.Reason)).Inc()
		o.status = perr.Err.Status
		o.err = "auth: " + string(perr.Reason)
		writeAuthError(w, perr, r.URL.Path, o.rid)
		return nil, false
	}

	target := upstreamPath(r, info, backend)
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	var ed *editor
	if h.Rewrite {
		ed = newEditor(r, info, cl, backend)
	}
	build := func(ctx context.Context, endpoint string) (*http.Request, error) {
		out, err := http.NewRequestWithContext(ctx, r.Method, cl.Scheme+"://"+endpoint+target, plan.body) //nolint:gosec // G704: scheme and host come from config
		if err != nil {
			return nil, err
		}
		out.Host = endpoint // upstream is always path-style (ADR-0001 amendment)
		out.ContentLength = plan.contentLength
		copyHeaders(out.Header, r.Header)
		for _, name := range clientAuthHeaders {
			out.Header.Del(name)
		}
		out.Header.Del("X-Amz-Expected-Bucket-Owner")
		out.Header.Del("X-Amz-Source-Expected-Bucket-Owner")
		if copySource != "" {
			out.Header.Set("X-Amz-Copy-Source", copySource)
		}
		if info.Op != s3.OpGetObject && info.Op != s3.OpHeadObject {
			out.Header.Del("Accept-Encoding") // bodies shunt may rewrite must arrive uncompressed
		}
		if plan.stripEncoding {
			stripAWSChunked(out.Header)
			out.Header.Del("X-Amz-Decoded-Content-Length")
			out.Header.Del("X-Amz-Trailer")
		}
		if plan.keepTrailer {
			out.Header.Set("Content-Encoding", "aws-chunked")
			out.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(plan.decodedLen, 10))
		}
		if _, ok := r.Header["User-Agent"]; !ok {
			out.Header.Set("User-Agent", "")
		}
		appendVia(out.Header, h.Via)
		out.Header.Set(telemetry.HeaderRequestID, o.rid)
		sigv4.Sign(out, cl.Creds, cl.Region, plan.payloadHash, time.Now())
		return out, nil
	}
	return &prepared{cl: cl, build: build, plan: plan, ed: ed, placement: p}, true
}

// allowed applies the credential's bucket allowlist. A credential without one may use every
// bucket of its tenant.
func allowed(c sigv4.Credential, bucket string) bool {
	return len(c.Buckets) == 0 || slices.Contains(c.Buckets, bucket)
}

// ownerID is the canonical owner id shunt shows a tenant (ListBuckets <Owner>) and checks
// x-amz-expected-bucket-owner against. Backend account ids never reach the check.
func ownerID(tenant string) string {
	sum := sha256.Sum256([]byte("shunt-tenant:" + tenant))
	return hex.EncodeToString(sum[:])
}

func ownerMatches(h http.Header, name, tenant string) bool {
	v := h.Get(name)
	return v == "" || v == ownerID(tenant)
}

// upstreamPath replaces the client's bucket with the backend bucket in the raw request path.
// A virtual-host request becomes path-style. Key bytes are kept exactly as the client sent them.
func upstreamPath(r *http.Request, info s3.RequestInfo, backend string) string {
	raw := sigv4.RawPath(r)
	if info.Style == s3.StyleVirtualHost {
		return "/" + backend + raw
	}
	rest := strings.TrimPrefix(raw, "/")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return "/" + backend + rest[i:]
	}
	return "/" + backend
}

// rewriteUploadIDs strips the cluster prefix from uploadId and upload-id-marker in a raw query
// (docs/DESIGN.md §2.4). It returns the rewritten query, the cluster id found, and conflict when
// two values name different clusters. Unprefixed values are left alone (the tolerance rule).
func rewriteUploadIDs(raw string) (query, cluster string, conflict bool) {
	if !strings.Contains(raw, "pload") { // uploadId, upload-id-marker, and their escaped forms
		return raw, "", false
	}
	parts := strings.Split(raw, "&")
	prefix, changed := "", false
	for i, part := range parts {
		k, v, _ := strings.Cut(part, "=")
		dk, err := url.QueryUnescape(k)
		if err != nil || (dk != "uploadId" && dk != "upload-id-marker") {
			continue
		}
		dv, err := url.QueryUnescape(v)
		if err != nil {
			continue
		}
		id, backendID, ok := migrate.DecodeUploadID(dv)
		if !ok {
			continue
		}
		if prefix != "" && prefix != id {
			return raw, "", true
		}
		prefix = id
		parts[i] = k + "=" + sigv4.Escape(backendID)
		changed = true
	}
	if !changed {
		return raw, "", false
	}
	return strings.Join(parts, "&"), prefix, false
}

// rewriteCopySource resolves x-amz-copy-source ("[/]bucket/key[?versionId=…]") through the
// directory. A source on another cluster cannot be copied by the backend: 501 until a later phase
// decides whether shunt streams the copy (docs/STATUS.md carried gap).
func rewriteCopySource(v, tenant string, cred sigv4.Credential, snap *directory.Snapshot, cluster string) (source string, code s3.Code, message string) {
	lead, s := "", v
	if strings.HasPrefix(s, "/") {
		lead, s = "/", s[1:]
	}
	seg, rest, sep := s, "", "/"
	if i := strings.IndexByte(s, '/'); i >= 0 {
		seg, rest = s[:i], s[i+1:]
	} else if i := strings.Index(strings.ToUpper(s), "%2F"); i >= 0 {
		seg, rest, sep = s[:i], s[i+3:], s[i:i+3]
	} else {
		return "", s3.InvalidArgument, "x-amz-copy-source must name a bucket and a key."
	}
	bucket, err := url.PathUnescape(seg)
	if err != nil {
		return "", s3.InvalidArgument, "x-amz-copy-source is not valid."
	}
	sp, ok := snap.Lookup(tenant, bucket)
	if !ok || !allowed(cred, bucket) {
		return "", s3.NoSuchBucket, "The source bucket does not exist."
	}
	if sp.Route() != cluster || sp.Names[cluster] == "" {
		return "", s3.NotImplemented, "Copying between buckets on different clusters is not supported yet."
	}
	return lead + sp.Names[cluster] + sep + rest, "", ""
}
