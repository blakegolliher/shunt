package proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// condUnavailable is the answer when a precondition cannot be checked: shunt refuses rather than
// answer from one side of a split bucket, or from a backend that does not judge the header at all.
const condUnavailable = "The condition on this write could not be checked. Try again."

// emptyPayloadHash is the SHA-256 of no bytes, the payload hash of a bodyless signed request.
const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// condAction is what the cross-cluster precondition check leaves for the upstream request to do.
// The zero value forwards the client's headers untouched.
type condAction struct {
	dropIfMatch bool // shunt has checked If-Match itself, against the other cluster
	createOnly  bool // ... so the write must still refuse to overwrite: If-None-Match: * instead
}

// hasWritePrecondition reports whether a write carries a condition shunt has to reason about.
func hasWritePrecondition(h http.Header) bool {
	return h.Get("If-None-Match") != "" || h.Get("If-Match") != ""
}

// conditionalWrite evaluates a conditional write against **both** clusters of a bucket that is
// mid-migration (docs/DESIGN.md §2.5, ADR-0013).
//
// While a bucket is RAMPING or MIGRATING its objects are split across two clusters, and a write
// goes to one of them. A backend can only judge a precondition against the copy it holds, so
// `If-None-Match: *` on a key that lives on the other cluster would create a second copy and
// answer 200 where S3 promises 412, and `If-Match` on such a key would answer 412 where the
// client's version is current. Both are answered here instead:
//
//   - **If-None-Match** (create-once): the other cluster is asked first. If it holds the key, the
//     write is refused with 412 and never sent.
//   - **If-Match** (update-if-current): if the destination holds the key, its own backend decides.
//     If it does not and the other cluster holds a matching version, the header is dropped and the
//     write goes out as `If-None-Match: *` instead, so a racing create still loses. If neither side
//     has a matching version, it is 412.
//
// A check that cannot be made fails closed with 503: guessing either way breaks the promise.
func (h *Handler) conditionalWrite(ctx context.Context, r *http.Request, o *outcome, info s3.RequestInfo, p *directory.Placement,
	clusters *upstream.Set, role string, cl *upstream.Cluster, backend string, class migrate.OpClass,
) (condAction, s3.Code, string) {
	if class != migrate.ClassWrite || !hasWritePrecondition(r.Header) {
		return condAction{}, "", ""
	}
	if info.Op != s3.OpPutObject && info.Op != s3.OpCompleteMultipartUpload && info.Op != s3.OpCopyObject {
		return condAction{}, "", "" // no other op carries these preconditions on the object
	}
	// The placement's other cluster, when the bucket is mid-migration: the key may be there instead.
	// ACTIVE and CUTOVER have one cluster to consider (after cutover the source is a subset of it).
	var other *upstream.Cluster
	otherBackend := ""
	if p.State == directory.StateRamping || p.State == directory.StateMigrating {
		otherRole := p.Source
		if role == p.Source {
			otherRole = p.Primary
		}
		if oc, ok := clusters.Get(p.ClusterOf(otherRole)); ok && otherRole != role && p.Names[otherRole] != "" {
			other, otherBackend = oc, p.Names[otherRole]
		}
	}
	// Nothing to add when the bucket sits on one cluster that judges the condition itself.
	if other == nil && cl.ConditionalWrite {
		return condAction{}, "", ""
	}

	inm, im := r.Header.Get("If-None-Match"), r.Header.Get("If-Match")
	onOther, otherETag := false, ""
	if other != nil {
		status, etag, err := h.headKey(ctx, o, r, info, other, otherBackend)
		if err != nil {
			if h.Log != nil {
				h.Log.Warn("a conditional write could not be checked against the migration's other cluster; refusing rather than answering it on one side (ADR-0013)",
					"request_id", o.rid, "bucket", backend, "other", other.Name, "err", err.Error())
			}
			return condAction{}, s3.ServiceUnavailable, condUnavailable
		}
		onOther, otherETag = status == http.StatusOK, etag
	}

	if inm != "" {
		if onOther && etagMatches(inm, otherETag) {
			return condAction{}, s3.PreconditionFailed, ""
		}
		// A cluster whose profile says it ignores the header cannot refuse the overwrite itself, so
		// shunt makes the check: HEAD-then-commit, the same guard and the same window as the mover's
		// on such a backend (ADR-0004, ADR-0013). Answering "created" for a write that overwrote is
		// what the create-once idiom cannot survive.
		if !cl.ConditionalWrite {
			destStatus, destETag, derr := h.headKey(ctx, o, r, info, cl, backend)
			if derr != nil {
				return condAction{}, s3.ServiceUnavailable, condUnavailable
			}
			if destStatus == http.StatusOK && etagMatches(inm, destETag) {
				return condAction{}, s3.PreconditionFailed, ""
			}
		}
	}
	if im == "" || other == nil {
		return condAction{}, "", "" // If-Match on one cluster is the backend's own to judge
	}
	destStatus, _, err := h.headKey(ctx, o, r, info, cl, backend)
	if err != nil {
		return condAction{}, s3.ServiceUnavailable, condUnavailable
	}
	switch {
	case destStatus == http.StatusOK:
		return condAction{}, "", "" // the destination holds a copy: its backend judges If-Match
	case !onOther || !etagMatches(im, otherETag):
		return condAction{}, s3.PreconditionFailed, ""
	}
	// The current version is on the other cluster and matches. shunt has made the client's check,
	// so the header goes away; the write must still not overwrite a copy created meanwhile.
	act := condAction{dropIfMatch: true, createOnly: cl.ConditionalWrite}
	if !act.createOnly && h.Log != nil {
		h.Log.Warn("a conditional update is being written to a cluster that ignores If-None-Match: *; a write racing it on that cluster would be overwritten (ADR-0013, capabilities.conditional_write)",
			"request_id", o.rid, "cluster", cl.Name, "bucket", backend)
	}
	return act, "", ""
}

// headKey asks one cluster whether it holds this request's object, and with which ETag. It signs
// a HEAD of the same key path the client used, against that cluster's backend bucket name.
func (h *Handler) headKey(ctx context.Context, o *outcome, r *http.Request, info s3.RequestInfo, cl *upstream.Cluster, backend string) (status int, etag string, err error) {
	ep := cl.Next()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, cl.Scheme+"://"+ep+upstreamPath(r, info, backend), http.NoBody) //nolint:gosec // G704: endpoint from the directory
	if err != nil {
		return 0, "", err
	}
	req.Host = ep
	appendVia(req.Header, h.Via)
	req.Header.Set(telemetry.HeaderRequestID, o.rid)
	sigv4.Sign(req, cl.Creds, cl.Region, emptyPayloadHash, time.Now())
	resp, err := cl.Transport.RoundTrip(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close() //nolint:errcheck // drained below
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return resp.StatusCode, "", &backendStatusError{status: resp.StatusCode, cluster: cl.Name}
	}
	return resp.StatusCode, resp.Header.Get("ETag"), nil
}

// backendStatusError is a HEAD that answered neither 200 nor 404, so nothing can be concluded.
type backendStatusError struct {
	status  int
	cluster string
}

func (e *backendStatusError) Error() string {
	return e.cluster + " answered HTTP " + http.StatusText(e.status)
}

// etagMatches applies an If-Match or If-None-Match header value to one ETag: "*" matches any
// existing object, and a list matches when any entry equals the ETag, ignoring a weak prefix.
func etagMatches(header, etag string) bool {
	header = strings.TrimSpace(header)
	if header == "*" {
		return true
	}
	for _, want := range strings.Split(header, ",") {
		if normalizeETag(want) == normalizeETag(etag) && etag != "" {
			return true
		}
	}
	return false
}

func normalizeETag(v string) string {
	return strings.Trim(strings.TrimPrefix(strings.TrimSpace(v), "W/"), `"`)
}
