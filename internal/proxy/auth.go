package proxy

import (
	"bytes"
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

	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/sigv4/chunked"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// Mode is the auth mode (ADR-0001).
type Mode uint8

// Auth modes.
const (
	ModePassthrough Mode = iota
	ModeResign
)

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
	buffered      []byte // set when the body must be sent to both clusters (ADR-0004)
}

// planBody applies the §2.2 payload table against the target cluster's capability profile.
func (h *Handler) planBody(r *http.Request, id sigv4.Identity, inBody *progressReader, cl *upstream.Cluster) (*bodyPlan, *sigv4.AuthError) {
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
		if !cl.EnforcesSHA256 {
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
	case withTrailer && cl.UnsignedTrailer:
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

// maxDeleteBody caps the DeleteObjects body shunt replays to the second cluster. S3 allows 1000
// keys per request; 1 MiB is comfortably above the largest legal body.
const maxDeleteBody = 1 << 20

// buffer reads the planned body once so it can be sent to both clusters (ADR-0004). It is used
// only for deletes, whose bodies are small and bounded.
func (p *bodyPlan) buffer(limit int64) *sigv4.AuthError {
	if p.body == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(p.body, limit+1))
	if err != nil {
		return &sigv4.AuthError{Reason: sigv4.ReasonChunkSignature, Err: asS3(err)}
	}
	if int64(len(data)) > limit {
		return &sigv4.AuthError{Reason: sigv4.ReasonMalformed, Err: s3.Lookup(s3.MaxMessageLengthExceeded)}
	}
	p.buffered, p.body, p.contentLength = data, nil, int64(len(data))
	return nil
}

// reader returns the body for one upstream attempt: the replayable copy when there is one.
func (p *bodyPlan) reader() io.Reader {
	if p.buffered != nil {
		return bytes.NewReader(p.buffered)
	}
	return p.body
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
