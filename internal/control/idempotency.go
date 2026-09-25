package control

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

// Idempotency (ADR-0021, contracts §1). Every request that creates an operation record carries an
// Idempotency-Key. The record keeps the key and a keyed digest of the request's canonical intent;
// a retry with the same key and intent gets the same record, never a second run, and the same key
// with another intent is refused. A request may also say which generation of its scope it was
// made against (If-Generation); a scope that has moved since is refused before anything runs.

// Request headers of a mutation.
const (
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderIfGeneration   = "If-Generation"
	// HeaderIdempotencyRetention answers how long the key keeps naming its record, in seconds:
	// a client must not retry with it after that.
	HeaderIdempotencyRetention = "Idempotency-Retention-Seconds"
)

// Codes of the idempotency contract.
const (
	CodeIdempotencyKeyRequired = "idempotency_key_required"
	CodeIdempotencyConflict    = "idempotency_conflict"
)

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// codedError is a refusal with its own HTTP status and code.
type codedError struct {
	status int
	code   string
	msg    string
}

func (e *codedError) Error() string { return e.msg }

// requestMeta is what a mutation request says about itself beyond its body.
type requestMeta struct {
	key        string // Idempotency-Key
	generation *int64 // If-Generation, when given
}

// readMeta reads a mutation's Idempotency-Key, required, and If-Generation, optional.
func readMeta(w http.ResponseWriter, r *http.Request) (requestMeta, error) {
	var m requestMeta
	m.key = r.Header.Get(HeaderIdempotencyKey)
	switch {
	case m.key == "":
		return m, &codedError{status: http.StatusBadRequest, code: CodeIdempotencyKeyRequired,
			msg: "this request creates an operation and needs an Idempotency-Key header: a new random value for each new request, the same one to retry it"}
	case !idempotencyKeyPattern.MatchString(m.key):
		return m, bad("Idempotency-Key: want 1-128 letters, digits, '.', '_', ':' or '-'")
	}
	if g := r.Header.Get(HeaderIfGeneration); g != "" {
		n, err := strconv.ParseInt(g, 10, 64)
		if err != nil || n < 0 {
			return m, bad("If-Generation %q: want a generation, a non-negative integer", g)
		}
		m.generation = &n
	}
	w.Header().Set(HeaderIdempotencyRetention, strconv.Itoa(int(IdempotencyRetention/time.Second)))
	return m, nil
}

// apply puts the request's key, intent digest and expected generation on a new record.
func (m requestMeta) apply(s *Server, op *Operation, body []byte) {
	op.RequestID = m.key
	op.IntentDigest = s.intentDigest(op.Kind, op.Placement, op.Cluster, body)
	if m.generation != nil && op.Scope != nil {
		op.Scope.Generation = *m.generation
	}
}

// intentDigest is a keyed digest of a request's canonical intent: its kind, its scope and its
// body with object keys sorted. Keyed with the confirmation key every control node shares, so it
// matches across nodes while a body carrying a secret (cluster add, adopt) cannot be recovered or
// guessed from a stored record.
func (s *Server) intentDigest(kind, placement, cluster string, body []byte) string {
	canonical := bytes.TrimSpace(body)
	var v any
	dec := json.NewDecoder(bytes.NewReader(canonical))
	dec.UseNumber()
	if dec.Decode(&v) == nil {
		if c, err := json.Marshal(v); err == nil {
			canonical = c
		}
	}
	mac := hmac.New(sha256.New, s.confirmKey())
	_, _ = fmt.Fprintf(mac, "shunt-intent-v1\x00%s\x00%s\x00%s\x00", kind, placement, cluster)
	_, _ = mac.Write(canonical)
	return hex.EncodeToString(mac.Sum(nil))
}

// peekBody reads a control request's body and puts it back for the handler.
func peekBody(r *http.Request) []byte {
	data, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20+1)) //nolint:errcheck // the handler reports a short body
	r.Body = io.NopCloser(bytes.NewReader(data))
	return data
}

// Public is the record as the API answers it: the intent digest stays in the store.
func (op Operation) Public() Operation {
	op.IntentDigest = ""
	return op
}

// replayed answers a route's request that an earlier one with the same key and intent already made:
// with that operation's outcome, once it has one, as the route would have answered it.
func (s *Server) replayed(w http.ResponseWriter, r *http.Request, err error) bool {
	var rp *IdempotentReplay
	if !errors.As(err, &rp) {
		return false
	}
	op := rp.Existing
	for !op.Terminal() {
		if serr := s.sleep(r.Context(), s.fencePoll()); serr != nil {
			return true // the client left; the record has the outcome
		}
		next, gerr := s.ops().Get(r.Context(), op.ID)
		if gerr != nil {
			fail(w, gerr)
			return true
		}
		if next == nil {
			fail(w, notFound("operation %s is no longer retained", op.ID))
			return true
		}
		op = next
	}
	if op.Status == StatusSucceeded {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if len(op.Result) > 0 {
			_, _ = w.Write(op.Result)
		} else {
			_, _ = w.Write([]byte("{}"))
		}
		return true
	}
	e := Error{Code: "failed", Message: "operation " + op.ID + " " + op.Status}
	if op.Error != nil {
		e = *op.Error
	}
	e.Retryable = retryable(e.Code)
	writeJSON(w, statusOf(e.Code), e)
	return true
}

// statusOf is the HTTP status an error code answers with (errorOf's mapping, by code).
func statusOf(code string) int {
	switch code {
	case "bad_request", "invalid", CodeIdempotencyKeyRequired, CodeProtocol:
		return http.StatusBadRequest
	case "not_found":
		return http.StatusNotFound
	case "refused", "conflict", CodeOperationConflict, CodeGenerationConflict, CodeIdempotencyConflict,
		CodeClusterMismatch, CodeEpochMismatch, CodeResyncRequired, CodeNotCancellable, CodeRetirementUnproven:
		return http.StatusConflict
	case CodeOperationCapacity:
		return http.StatusTooManyRequests
	case "unavailable":
		return http.StatusServiceUnavailable
	case "backend":
		return http.StatusBadGateway
	}
	return http.StatusInternalServerError
}
