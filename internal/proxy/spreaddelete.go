package proxy

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // G501: Content-MD5 is the integrity header S3 defines for DeleteObjects, not a security use
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// DeleteObjects on a bucket spread over legs (ADR-0018, amended 2026-09-25). The request's body,
// verified and read once (at most maxDeleteBody, as for a migration's dual delete, ADR-0004), goes
// unchanged to every leg, so the client's Content-MD5 or checksum still holds. A leg deletes the
// keys it holds; a key it does not hold is reported deleted, as S3 does. While a move is under
// way its source leg is sent the request first and every other leg after, the order a single
// DELETE of a moving key takes (ADR-0004 race 1). The answer takes each key's entry from the leg
// that owns it: for a key in the move, the destination. A leg other than the move's source that
// does not answer fails the request, since its keys' outcomes are unknown; a source leg that does
// not answer is logged, as a dual delete's is, and the owners' answers stand.

// deleteResult is DeleteObjects' answer, as far as shunt reads and writes it.
type deleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr,omitempty"`
	Deleted []deletedObject `xml:"Deleted"`
	Errors  []deleteError   `xml:"Error"`
}

type deletedObject struct {
	Key                   string `xml:"Key"`
	VersionID             string `xml:"VersionId,omitempty"`
	DeleteMarker          *bool  `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
}

type deleteError struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}

// maxDeleteResult bounds a leg's answer: 1000 keys of at most 1024 bytes each, with their tags.
const maxDeleteResult = 4 << 20

// parseDeleteResult reads a leg's DeleteObjects answer.
func parseDeleteResult(body []byte) (deleteResult, error) {
	var res deleteResult
	if err := xml.Unmarshal(body, &res); err != nil {
		return deleteResult{}, err
	}
	if res.XMLName.Local != "DeleteResult" {
		return deleteResult{}, fmt.Errorf("not a DeleteResult: <%s>", res.XMLName.Local)
	}
	return res, nil
}

// spreadDeleteLeg is one leg's part of a spread DeleteObjects.
type spreadDeleteLeg struct {
	id      string
	cl      *upstream.Cluster
	backend string
	status  int
	body    []byte
	err     error
}

// spreadDeleteObjects answers DeleteObjects for a bucket spread over legs.
func (h *Handler) spreadDeleteObjects(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome, id sigv4.Identity, inBody *progressReader, p *directory.Placement, rt spreadRuntime) {
	bucket := o.info.Bucket
	if p.ReadOnly {
		h.refuseReadOnly(w, r, o, bucket, "placement-read-only", "This bucket is read-only for maintenance.", p.RejectWrites)
		return
	}
	if p.Barrier != nil && p.Barrier.Kind == config.BarrierMutations {
		h.refuseWrite(w, r, o, bucket, "barrier", "This bucket's writes pause while a change to it reaches every proxy. Retry shortly.")
		return
	}
	if m := p.Move; m != nil {
		if p.KeyHash != directory.RampHash {
			h.answer(w, r, o, s3.ServiceUnavailable, "This bucket's key split cannot be routed by this proxy version. Try again later.")
			return
		}
		if mv := p.MoveView(); mv.Held() {
			// A step of the move is being written everywhere: the keys it moves pause their writes,
			// and a request naming many keys cannot tell which, so all of it waits (it is short).
			h.refuseWrite(w, r, o, bucket, "hold", "Keys of this bucket are moving to another cluster and their deletes pause until every proxy has the change. Retry shortly.")
			return
		}
		if h.staleFor(p) {
			h.refuseWrite(w, r, o, bucket, "stale", "This proxy has lost contact with its control node and does not write to a bucket that is moving. Retry shortly.")
			return
		}
	}

	// Every leg, the move's source first.
	legs := make([]*spreadDeleteLeg, 0, len(p.Legs))
	for _, lid := range directory.ListingLegs(p, "") {
		l := p.Legs[lid]
		cl, ok := rt.clusters.Get(l.Cluster)
		if !ok {
			h.answer(w, r, o, s3.ServiceUnavailable, fmt.Sprintf("Cluster %s of this bucket is not available to this proxy.", l.Cluster))
			return
		}
		if c, found := rt.snap.Cluster(l.Cluster); found && c.ReadOnly {
			h.refuseReadOnly(w, r, o, bucket, "cluster-read-only", "Backend cluster "+l.Cluster+" is read-only for maintenance.", c.RejectWrites)
			return
		} else if found && c.Barrier != nil {
			h.refuseWrite(w, r, o, bucket, "barrier", "Backend cluster "+l.Cluster+"'s writes pause while a change to it reaches every proxy. Retry shortly.")
			return
		}
		leg := &spreadDeleteLeg{id: lid, cl: cl, backend: l.Bucket}
		if p.Move != nil && lid == p.Move.From {
			legs = append([]*spreadDeleteLeg{leg}, legs...)
			continue
		}
		legs = append(legs, leg)
	}

	// The bucket's gates, as for any delete (ADR-0021 D2): a mutation token, and, while a move is
	// under way, a source token; a closed source gate pauses the delete (the DELETE rule).
	bucketKey := directory.Key(o.tenant, bucket)
	tok, _, admitted := h.Gates.Enter(bucketKey, admission.Mutations)
	if !admitted {
		h.refuseWrite(w, r, o, bucket, "barrier", "This bucket's writes pause while a change to it reaches every proxy. Retry shortly.")
		return
	}
	o.tok = tok
	if p.Move != nil {
		src, _, ok := h.Gates.Enter(bucketKey, admission.Source)
		if !ok || (p.Barrier != nil && p.Barrier.Kind == config.BarrierSource) {
			src.Release(h.Gates, admission.Definitive)
			h.refuseWrite(w, r, o, bucket, "source_closed", "This bucket's deletes pause while its migration source is removed. Retry shortly.")
			return
		}
		o.src = src
	}

	body, code := h.readDeleteBody(r, id, inBody)
	if code != "" {
		h.answer(w, r, o, code, "")
		return
	}
	if inBody != nil {
		o.bytesIn = inBody.count()
	}
	hdr := http.Header{}
	for _, k := range []string{"Content-Md5", "X-Amz-Sdk-Checksum-Algorithm", "X-Amz-Checksum-Crc32", "X-Amz-Checksum-Crc32c", "X-Amz-Checksum-Crc64nvme", "X-Amz-Checksum-Sha1", "X-Amz-Checksum-Sha256", "X-Amz-Bypass-Governance-Retention", "X-Amz-Mfa"} {
		if v := r.Header.Get(k); v != "" {
			hdr.Set(k, v)
		}
	}
	if hdr.Get("Content-Md5") == "" && !hasChecksum(hdr) {
		sum := md5.Sum(body) //nolint:gosec // G401: see the import
		hdr.Set("Content-Md5", base64.StdEncoding.EncodeToString(sum[:]))
	}

	send := func(l *spreadDeleteLeg) {
		oc := *o
		resp, err := h.clusterDoHeader(ctx, l.cl, http.MethodPost, "/"+l.backend+"?delete", body, hdr, &oc)
		if err != nil {
			l.err = err
			return
		}
		defer resp.Body.Close() //nolint:errcheck // read below
		l.status = resp.StatusCode
		l.body, l.err = io.ReadAll(io.LimitReader(resp.Body, maxDeleteResult))
	}
	rest := legs
	if p.Move != nil {
		send(legs[0]) // the move's source first; its answer is not waited on for the result
		rest = legs[1:]
	}
	var wg sync.WaitGroup
	for _, l := range rest {
		wg.Go(func() { send(l) })
	}
	wg.Wait()

	// Every leg's answer, read; a leg that did not answer, or answered an error for the whole
	// request, fails it unless it is the move's source.
	results := map[string]deleteResult{}
	sourceFailed := false
	for _, l := range legs {
		source := p.Move != nil && l.id == p.Move.From
		var res deleteResult
		var err error
		switch {
		case l.err != nil:
			err = l.err
			if dispatched(l.err, nil, 0) {
				if source {
					o.srcUncertain = true
				} else {
					o.uncertain = true
				}
			}
		case l.status != http.StatusOK:
			err = fmt.Errorf("HTTP %d %s", l.status, errorCode(l.body))
		default:
			if res, err = parseDeleteResult(l.body); err != nil {
				err = fmt.Errorf("unreadable answer: %w", err)
			}
		}
		if err != nil && source {
			if h.Log != nil {
				h.Log.Error("delete did not reach the migration source; the objects can come back when the mover copies them (ADR-0004)",
					"request_id", o.rid, "bucket", bucketKey, "leg", l.id, "source", l.cl.Name, "err", err.Error())
			}
			sourceFailed = true
			continue
		}
		if err != nil {
			if l.err == nil && l.status >= 400 && l.status < 500 {
				// The same body goes to every leg, so a leg that refuses it (MalformedXML, BadDigest)
				// speaks for all of them: its error is the answer.
				h.relayBackendError(w, r, o, l.cl, l.backend, &http.Response{StatusCode: l.status, Header: http.Header{"Content-Type": {"application/xml"}}}, l.body)
				return
			}
			h.upstreamError(w, r, o, fmt.Errorf("leg %s on %s: %w", l.id, l.cl.Name, err))
			return
		}
		results[l.id] = res
	}

	// Each key's entry from the leg that owns it.
	owner := func(key string) string {
		if p.Move != nil && migrate.InMove(p, key) {
			return p.Move.To
		}
		lid, err := migrate.OwnerOf(p, key)
		if err != nil {
			return ""
		}
		return lid
	}
	out := deleteResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for _, l := range legs {
		res, ok := results[l.id]
		if !ok {
			continue
		}
		for _, d := range res.Deleted {
			if owner(d.Key) == l.id {
				out.Deleted = append(out.Deleted, d)
			}
		}
		for _, e := range res.Errors {
			if owner(e.Key) == l.id {
				out.Errors = append(out.Errors, e)
			}
		}
	}
	switch {
	case sourceFailed:
		h.Metrics.DualDelete.WithLabelValues(bucketKey, "source_failed").Inc()
	case p.Move != nil:
		h.Metrics.DualDelete.WithLabelValues(bucketKey, "both").Inc()
	}
	var b bytes.Buffer
	b.WriteString(xml.Header)
	if err := xml.NewEncoder(&b).Encode(out); err != nil {
		h.answer(w, r, o, s3.InternalError, "")
		return
	}
	if h.wantsRoute(r) {
		ids := make([]string, 0, len(legs))
		for _, l := range legs {
			ids = append(ids, l.cl.Name)
		}
		w.Header().Set(headerRoute, "spread "+strings.Join(ids, "+"))
	}
	o.cluster, o.clusterType = "spread", "spread"
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(b.Len()))
	w.WriteHeader(http.StatusOK)
	o.status = http.StatusOK
	n, _ := w.Write(b.Bytes())
	o.bytesOut = int64(n)
}

// spreadRuntime is what spreadDeleteObjects reads of the request's bundle.
type spreadRuntime struct {
	snap     *directory.Snapshot
	clusters *upstream.Set
}

// readDeleteBody reads a DeleteObjects body once, verified as the client signed it: a hex
// x-amz-content-sha256 is checked here, since shunt signs the body again for each leg; signed
// chunks are verified by the decoder as they are read.
func (h *Handler) readDeleteBody(r *http.Request, id sigv4.Identity, inBody *progressReader) ([]byte, s3.Code) {
	if inBody == nil {
		return nil, s3.MalformedXML
	}
	plan, perr := h.planBody(r, id, inBody, &upstream.Cluster{EnforcesSHA256: true})
	if perr != nil {
		h.Metrics.AuthFailures.WithLabelValues(string(perr.Reason)).Inc()
		return nil, perr.Err.Code
	}
	if aerr := plan.buffer(maxDeleteBody); aerr != nil {
		h.Metrics.AuthFailures.WithLabelValues(string(aerr.Reason)).Inc()
		return nil, aerr.Err.Code
	}
	if id.Payload == sigv4.PayloadSHA256 && !id.Presigned {
		sum := sha256.Sum256(plan.buffered)
		if hex.EncodeToString(sum[:]) != id.PayloadHash {
			return nil, s3.XAmzContentSHA256Mismatch
		}
	}
	if len(plan.buffered) == 0 {
		return nil, s3.MalformedXML
	}
	return plan.buffered, ""
}

// hasChecksum reports whether the headers carry an x-amz-checksum-* value.
func hasChecksum(hdr http.Header) bool {
	for k := range hdr {
		if strings.HasPrefix(k, "X-Amz-Checksum-") {
			return true
		}
	}
	return false
}
