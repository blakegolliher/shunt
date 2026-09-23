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
	"github.com/blakegolliher/shunt/internal/upstream"
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
	clusters := h.Clusters.Load() // live since POC-5; the pointers this request takes stay valid to its end

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
		h.createBucket(ctx, w, r, o, id.Credential, snap, clusters)
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
	if p.Spread() {
		// A bucket spread over legs (ADR-0018 N2): a key's requests go to the leg that owns it, as a
		// plain one-cluster bucket; listings merge every leg; a bucket-level request every leg answers
		// alike goes to the first; any other would have to reach every leg, and is not supported yet.
		switch {
		case info.Level == s3.LevelObject:
			np, err := migrate.Narrow(p, info.Key)
			if err != nil {
				h.answer(w, r, o, s3.ServiceUnavailable, "This bucket's key split cannot be routed by this proxy version. Try again later.")
				return nil, false
			}
			p = &np
		case info.Op == s3.OpListObjectsV2 || info.Op == s3.OpListObjects:
			h.spreadListing(ctx, w, r, o, p, clusters)
			return nil, false
		case info.Op == s3.OpHeadBucket || info.Op == s3.OpGetBucketLocation:
			np := migrate.FirstLeg(p)
			p = &np
		default:
			h.answer(w, r, o, s3.NotImplemented, fmt.Sprintf("%s is not supported on a bucket spread over %d backend buckets yet (ADR-0018).", info.Op, len(p.Legs)))
			return nil, false
		}
	}

	rawQuery := r.URL.RawQuery
	if id.Presigned {
		rawQuery = stripPresign(rawQuery)
	}
	// Where this request goes (docs/DESIGN.md §2.5, ADR-0004). An uploadId, resolved below, wins:
	// a multipart upload only exists on the cluster that issued its id.
	class := migrate.Class(info.Op)
	mutating := r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions
	if p.ReadOnly && mutating {
		h.refuseReadOnly(w, r, o, info.Bucket, "placement-read-only", "This bucket is read-only for maintenance.", p.RejectWrites)
		return nil, false
	}
	route, err := migrate.Decide(p, class, info.Key)
	if err != nil {
		if h.Log != nil {
			h.Log.Error("refusing a ramped request: the placement's ramp names a hash this build does not implement; routing it would re-split keys (ADR-0004)",
				"request_id", o.rid, "tenant", o.tenant, "bucket", info.Bucket, "err", err.Error())
		}
		h.answer(w, r, o, s3.ServiceUnavailable, "This bucket's migration ramp cannot be routed by this proxy version. Try again later.")
		return nil, false
	}
	stale := h.staleFor(p)
	route = staleRead(stale, class, route)
	switch {
	case route.Held:
		h.refuseWrite(w, r, o, info.Bucket, "hold", "This key is moving to another cluster and its writes pause until every proxy has the change. Retry shortly.")
		return nil, false
	case stale && (class == migrate.ClassWrite || class == migrate.ClassDelete):
		h.refuseWrite(w, r, o, info.Bucket, "stale", "This proxy has lost contact with its control node and does not write to a bucket that is moving. Retry shortly.")
		return nil, false
	}
	clusterName := p.Primary
	if route.Cluster == migrate.Source {
		clusterName = p.Source
	}
	rawQuery, prefix, conflict := rewriteUploadIDs(rawQuery)
	if conflict {
		h.answer(w, r, o, s3.NoSuchUpload, "")
		return nil, false
	}
	if prefix != "" {
		pcl, found := clusters.ByID(prefix)
		if !found || p.Names[pcl.Name] == "" {
			h.answer(w, r, o, s3.NoSuchUpload, "")
			return nil, false
		}
		clusterName = pcl.Name
		route = migrate.Route{Cluster: migrate.Primary} // pinned: no fallback, no dual, no merge
	}
	if mutating {
		check := []string{clusterName}
		if route.Both {
			check = append(check, p.Source)
		}
		for _, name := range check {
			if c, found := snap.Cluster(name); found && c.ReadOnly {
				h.refuseReadOnly(w, r, o, info.Bucket, "cluster-read-only", "Backend cluster "+name+" is read-only for maintenance.", c.RejectWrites)
				return nil, false
			}
		}
	}
	cl, ok := clusters.Get(clusterName)
	if !ok {
		if h.Log != nil {
			h.Log.Error("placement routes to a cluster that is not configured", "request_id", o.rid, "tenant", o.tenant, "bucket", info.Bucket, "cluster", clusterName)
		}
		h.answer(w, r, o, s3.InternalError, "")
		return nil, false
	}
	backend := p.Names[cl.Name]
	o.cluster, o.clusterType, o.backend = cl.Name, cl.Type, backend
	if p.State == directory.StateRamping && class == migrate.ClassWrite {
		bucketKey := directory.Key(o.tenant, info.Bucket)
		h.Metrics.RampWrites.WithLabelValues(bucketKey, route.Cluster.String()).Inc()
		if h.Telemetry != nil {
			h.Telemetry.ObserveMigration(time.Now(), bucketKey, "writes", route.Cluster.String())
		}
	}

	// A conditional write while the bucket's objects are split across two clusters is judged
	// against both, not only the cluster this write lands on (ADR-0013).
	act, code, msg := h.conditionalWrite(ctx, r, o, info, p, clusters, cl, backend, class)
	if code != "" {
		h.answer(w, r, o, code, msg)
		return nil, false
	}
	cond := act

	// The placement's other cluster, for a read that falls back, a delete that goes to both, or a
	// listing that merges.
	var other *upstream.Cluster
	otherBackend := ""
	if p.Source != "" && (route.Fallback || route.Both || route.Merge) {
		name := p.Source
		if route.Cluster == migrate.Source {
			name = p.Primary
		}
		if oc, found := clusters.Get(name); found && p.Names[name] != "" {
			other, otherBackend = oc, p.Names[name]
		}
	}
	if route.Merge && other != nil && info.Op == s3.OpListObjectsV2 {
		h.mergeListing(ctx, w, r, o, cl, backend, other, otherBackend)
		return nil, false
	}

	copySource := ""
	if v := r.Header.Get("X-Amz-Copy-Source"); v != "" {
		cs, plan, code, msg := h.resolveCopySource(v, o.tenant, id.Credential, snap, clusters, cl)
		switch {
		case code != "":
			h.answer(w, r, o, code, msg)
			return nil, false
		case plan != nil:
			// The source is on another cluster, or on a bucket whose objects are split across two:
			// no backend can do this copy, so shunt streams it (ADR-0014).
			h.streamCopy(ctx, w, r, o, plan, cl, backend, cond)
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

	// A delete that goes to both clusters needs its body twice, so it is read once and replayed.
	// Bounded by S3's own limit on DeleteObjects (1000 keys); see ADR-0004.
	if route.Both && plan.body != nil && r.ContentLength != 0 {
		if aerr := plan.buffer(maxDeleteBody); aerr != nil {
			h.Metrics.AuthFailures.WithLabelValues(string(aerr.Reason)).Inc()
			o.status, o.err = aerr.Err.Status, "auth: "+string(aerr.Reason)
			writeAuthError(w, aerr, r.URL.Path, o.rid)
			return nil, false
		}
	}
	var ed *editor
	if h.Rewrite {
		ed = newEditor(r, info, cl, backend)
	}
	build := func(ctx context.Context, cl *upstream.Cluster, backend, endpoint string) (*http.Request, error) {
		target := upstreamPath(r, info, backend)
		if rawQuery != "" {
			target += "?" + rawQuery
		}
		out, err := http.NewRequestWithContext(ctx, r.Method, cl.Scheme+"://"+endpoint+target, plan.reader()) //nolint:gosec // G704: scheme and host come from config
		if err != nil {
			return nil, err
		}
		out.Host = endpoint // upstream is always path-style (ADR-0001 amendment)
		out.ContentLength = plan.contentLength
		copyHeaders(out.Header, r.Header)
		for _, name := range clientAuthHeaders {
			out.Header.Del(name)
		}
		out.Header.Del(headerDebug) // shunt's own request header; the backend never sees it
		if cond.dropIfMatch {
			out.Header.Del("If-Match") // shunt checked it against the other cluster (ADR-0013)
			if cond.createOnly {
				out.Header.Set("If-None-Match", "*")
			} else {
				out.Header.Del("If-None-Match")
			}
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
	return &prepared{cl: cl, backend: backend, other: other, otherBackend: otherBackend, route: route,
		build: build, plan: plan, ed: ed, placement: p, bucketKey: directory.Key(o.tenant, info.Bucket)}, true
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
