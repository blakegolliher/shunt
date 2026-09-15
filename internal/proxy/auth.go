package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/sigv4/chunked"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// Mode is the auth mode (ADR-0001).
type Mode uint8

// Auth modes.
const (
	ModePassthrough Mode = iota
	ModeResign
)

// Capabilities is the subset of the cluster capability profile the resign path consults.
type Capabilities struct {
	EnforcesSHA256  bool // backend rejects a wrong hex x-amz-content-sha256 itself
	UnsignedTrailer bool // backend accepts STREAMING-UNSIGNED-PAYLOAD-TRAILER
}

// presignParams are stripped from the upstream URL after a presigned request is verified.
var presignParams = []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature", "X-Amz-Security-Token"}

// clientAuthHeaders are always removed before re-signing (amendment 4).
var clientAuthHeaders = []string{"Authorization", "X-Amz-Date", "X-Amz-Content-Sha256", "X-Amz-Security-Token"}

// bodyPlan is the resign decision for one request: what body goes upstream, with which
// x-amz-content-sha256, Content-Length, and headers (docs/DESIGN.md §2.2 table).
type bodyPlan struct {
	body          io.Reader
	contentLength int64
	payloadHash   string
	decoder       *chunked.ChunkReader // set when shunt decodes signed chunks
	sha           hash.Hash            // set when shunt hashes a hex-sha256 body for log-and-alert
	expectHex     string
	stripEncoding bool // drop aws-chunked framing headers
	keepTrailer   bool // forward as unsigned trailer (headers kept)
	decodedLen    int64
}

// resign verifies the client request and builds the upstream request per the §2.2 table.
// It returns an *sigv4.AuthError (already counted) for the caller to render.
func (h *Handler) resign(ctx context.Context, r *http.Request, o *outcome, inBody *progressReader, endpoint string) (*http.Request, *bodyPlan, *sigv4.AuthError) {
	t0 := time.Now()
	id, aerr := sigv4.Verify(ctx, r, h.Store, time.Now(), sigv4.Options{ClockSkew: h.ClockSkew, RequireHash: true})
	if aerr != nil {
		h.Metrics.AuthFailures.WithLabelValues(string(aerr.Reason)).Inc()
		return nil, nil, aerr
	}
	mode := "header"
	if id.Presigned {
		mode = "presigned"
	}
	h.Metrics.AuthDuration.WithLabelValues(mode).Observe(time.Since(t0).Seconds())
	o.tenant = id.Credential.Tenant

	// Upstream target: always path-style with Host = endpoint (decision 1).
	rawPath := sigv4.RawPath(r)
	if o.info.Style == s3.StyleVirtualHost {
		rawPath = "/" + o.info.Bucket + rawPath
	}
	rawQuery := r.URL.RawQuery
	if id.Presigned {
		rawQuery = stripPresign(rawQuery)
	}
	target := h.Cluster.Scheme + "://" + endpoint + rawPath
	if rawQuery != "" {
		target += "?" + rawQuery
	}

	plan, perr := h.planBody(r, id, inBody)
	if perr != nil {
		h.Metrics.AuthFailures.WithLabelValues(string(perr.Reason)).Inc()
		return nil, nil, perr
	}
	out, err := http.NewRequestWithContext(ctx, r.Method, target, plan.body) //nolint:gosec // G704: scheme and host come from config
	if err != nil {
		return nil, nil, &sigv4.AuthError{Reason: sigv4.ReasonMalformed, Err: s3.Lookup(s3.InvalidURI)}
	}
	out.Host = endpoint
	out.ContentLength = plan.contentLength
	copyHeaders(out.Header, r.Header)
	for _, hname := range clientAuthHeaders {
		out.Header.Del(hname)
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
	sigv4.Sign(out, h.ClusterCreds, h.Cluster.Region, plan.payloadHash, time.Now())
	return out, plan, nil
}

// planBody applies the §2.2 payload table.
func (h *Handler) planBody(r *http.Request, id sigv4.Identity, inBody *progressReader) (*bodyPlan, *sigv4.AuthError) {
	var src io.Reader
	if inBody != nil {
		src = inBody
	}
	plan := &bodyPlan{body: src, contentLength: r.ContentLength, payloadHash: id.PayloadHash}
	if src == nil {
		plan.body = nil
		if id.Presigned || id.Payload == sigv4.PayloadMissing {
			plan.payloadHash = sigv4.UnsignedPayload
		}
		return plan, nil
	}
	switch id.Payload {
	case sigv4.PayloadUnsigned, sigv4.PayloadStreamingUnsignedTrailer, sigv4.PayloadMissing:
		if id.Payload == sigv4.PayloadMissing {
			plan.payloadHash = sigv4.UnsignedPayload
		}
		return plan, nil // body untouched, header verbatim
	case sigv4.PayloadSHA256:
		if !h.Capabilities.EnforcesSHA256 {
			// The backend will not check; hash in the copy loop and log a mismatch (amendment 5).
			plan.sha = sha256.New()
			plan.expectHex = id.PayloadHash
			plan.body = io.TeeReader(src, plan.sha)
		}
		return plan, nil
	}
	if id.Presigned {
		plan.payloadHash = sigv4.UnsignedPayload
		return plan, nil
	}

	// Signed chunk variants: decode and verify while streaming.
	decodedLen, err := chunked.ParseDecodedContentLength(r.Header)
	if err != nil {
		return nil, &sigv4.AuthError{Reason: sigv4.ReasonMalformed, Err: asS3(err)}
	}
	trailerType, err := chunked.ExtractChecksumType(r.Header)
	if err != nil {
		return nil, &sigv4.AuthError{Reason: sigv4.ReasonTrailer, Err: asS3(err)}
	}
	withTrailer := id.Payload == sigv4.PayloadStreamingSignedTrailer
	if withTrailer && trailerType == "" {
		return nil, &sigv4.AuthError{Reason: sigv4.ReasonTrailer, Err: s3.Error{Code: s3.InvalidRequest, Status: 400, Message: "x-amz-trailer is required with STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER."}}
	}
	if !withTrailer {
		trailerType = ""
	}
	rd, err := chunked.NewSignedChunkReader(src, chunked.AuthData{Access: id.Credential.AccessKey, Region: id.Scope.Region, Signature: id.Signature},
		"", id.SigningKey, id.Time, trailerType, withTrailer, decodedLen)
	if err != nil {
		return nil, &sigv4.AuthError{Reason: sigv4.ReasonMalformed, Err: asS3(err)}
	}
	dec := rd.(*chunked.ChunkReader) //nolint:errcheck // NewSignedChunkReader returns *ChunkReader
	plan.decoder = dec
	plan.decodedLen = decodedLen
	plan.stripEncoding = true
	switch {
	case withTrailer && h.Capabilities.UnsignedTrailer:
		// Re-frame as an unsigned trailer so the backend validates the checksum itself.
		algo := chunked.Algorithm(trailerType)
		plan.body = chunked.NewEncoder(dec, decodedLen, string(trailerType), dec, chunked.DefaultChunkSize)
		plan.contentLength = chunked.EncodedLength(decodedLen, chunked.DefaultChunkSize, string(trailerType), chunked.Base64Len(algo))
		plan.payloadHash = sigv4.StreamingUnsignedPayloadTrailer
		plan.stripEncoding = false
		plan.keepTrailer = true
	default:
		// Decoded plain body; the decoder verified chunk signatures (and the trailer, if any).
		plan.body = dec
		plan.contentLength = decodedLen
		plan.payloadHash = sigv4.UnsignedPayload
	}
	plan.body = &guardReader{r: plan.body, log: h.Log}
	return plan, nil
}

// guardReader turns a panic inside a body decoder into a read error. Decoders run in net/http's
// transport write loop, which does not recover panics, so without this guard one malformed body
// from an authenticated client terminates the whole process (a negative chunk size did, found by
// fuzzing at the POC-2 gate). The request fails with 400 IncompleteBody and the panic is logged.
type guardReader struct {
	r   io.Reader
	log *slog.Logger
}

func (g *guardReader) Read(p []byte) (n int, err error) {
	defer func() {
		if v := recover(); v != nil {
			if g.log != nil {
				g.log.Error("request body decoder panicked; request aborted instead of crashing the process",
					"panic", fmt.Sprint(v), "stack", string(debug.Stack()))
			}
			n, err = 0, s3.Lookup(s3.IncompleteBody)
		}
	}()
	return g.r.Read(p)
}

// asS3 converts a chunked-package error into an s3.Error for rendering.
func asS3(err error) s3.Error {
	if e, ok := err.(s3.Error); ok { //nolint:errorlint // chunked returns s3.Error values directly
		return e
	}
	return s3.Lookup(s3.InvalidRequest)
}

// stripPresign removes the X-Amz-* auth parameters from a raw query string, preserving the
// encoding of everything else.
func stripPresign(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "&")
	out := parts[:0]
	for _, p := range parts {
		k := p
		if i := strings.IndexByte(p, '='); i >= 0 {
			k = p[:i]
		}
		if dk, err := url.QueryUnescape(k); err == nil {
			k = dk
		}
		skip := false
		for _, name := range presignParams {
			if k == name {
				skip = true
				break
			}
		}
		if !skip {
			out = append(out, p)
		}
	}
	return strings.Join(out, "&")
}

// stripAWSChunked removes only the aws-chunked token from Content-Encoding (amendment 4).
func stripAWSChunked(h http.Header) {
	v := h.Get("Content-Encoding")
	if v == "" {
		return
	}
	var keep []string
	for _, tok := range strings.Split(v, ",") {
		if t := strings.TrimSpace(tok); t != "" && !strings.EqualFold(t, "aws-chunked") {
			keep = append(keep, t)
		}
	}
	if len(keep) == 0 {
		h.Del("Content-Encoding")
		return
	}
	h.Set("Content-Encoding", strings.Join(keep, ", "))
}

// writeAuthError renders a verification failure with its status.
func writeAuthError(w http.ResponseWriter, aerr *sigv4.AuthError, resource, rid string) {
	body := s3.Render(aerr.Err.Code, aerr.Err.Message, resource, rid, "")
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(aerr.Err.Status)
	_, _ = w.Write(body) //nolint:gosec // XML from s3.Render
}

func hexSum(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }
