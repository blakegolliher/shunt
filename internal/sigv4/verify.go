package sigv4

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/s3"
)

// Credential is what a CredentialStore returns. Secret is redacted by every formatting verb.
type Credential struct {
	AccessKey string
	Secret    string
	Tenant    string
	Buckets   []string // optional allowlist; empty means all
}

// String redacts the secret.
func (c Credential) String() string {
	return fmt.Sprintf("Credential{AccessKey:%s Tenant:%s Secret:[redacted]}", c.AccessKey, c.Tenant)
}

// GoString redacts the secret for %#v.
func (c Credential) GoString() string { return c.String() }

// Format redacts the secret for every verb, including %+v.
func (c Credential) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(c.String())) }

// CredentialStore is one of the two named interface seams (CLAUDE.md). Lookup returns
// ErrUnknownAccessKey for a key that does not exist.
type CredentialStore interface {
	Lookup(ctx context.Context, accessKey string) (Credential, error)
}

// ErrUnknownAccessKey is returned by CredentialStore implementations for an unknown key.
var ErrUnknownAccessKey = errors.New("sigv4: unknown access key")

// Reason labels a verification failure for shunt_auth_failures_total{reason}.
type Reason string

// Failure reasons (docs/telemetry-catalog.md).
const (
	ReasonMissing         Reason = "missing"
	ReasonMalformed       Reason = "malformed"
	ReasonUnknownKey      Reason = "unknown_key"
	ReasonSignature       Reason = "signature"
	ReasonSkew            Reason = "skew"
	ReasonExpired         Reason = "expired"
	ReasonSigV4A          Reason = "sigv4a"
	ReasonToken           Reason = "token"
	ReasonMissingSHA256   Reason = "missing_sha256"
	ReasonUnsignedHeaders Reason = "unsigned_headers"
	ReasonChunkSignature  Reason = "chunk_signature"
	ReasonTrailer         Reason = "trailer"
)

// AuthError is a verification failure with its S3 error and metric reason.
type AuthError struct {
	Reason Reason
	Err    s3.Error
}

func (e *AuthError) Error() string { return string(e.Reason) + ": " + e.Err.Error() }

// S3 returns the error to render to the client.
func (e *AuthError) S3() s3.Error { return e.Err }

func fail(reason Reason, code s3.Code, msg string) *AuthError {
	e := s3.Lookup(code)
	if msg != "" {
		e.Message = msg
	}
	return &AuthError{Reason: reason, Err: e}
}

// Identity is the result of a successful verification.
type Identity struct {
	Credential    Credential
	Scope         Scope
	SigningKey    []byte // kSigning for the client's scope; seeds chunk verification
	Signature     string // the seed signature
	SignedHeaders []string
	Time          time.Time // x-amz-date as signed
	Presigned     bool
	Payload       PayloadMode
	PayloadHash   string // the x-amz-content-sha256 value (or UNSIGNED-PAYLOAD for presigned)
}

// Options tune Verify.
type Options struct {
	ClockSkew   time.Duration // default 15m
	MaxExpires  int64         // presigned X-Amz-Expires cap, default 604800
	RequireHash bool          // require x-amz-content-sha256 on header-signed requests (S3 does, except HEAD)
}

// Auth is a parsed Authorization header or presigned query.
type Auth struct {
	AccessKey     string
	Scope         Scope
	SignedHeaders []string
	Signature     string
	Presigned     bool
	Date          string // X-Amz-Date for presigned
	Expires       int64  // X-Amz-Expires for presigned
}

// parseAuthorization parses "AWS4-HMAC-SHA256 Credential=…, SignedHeaders=…, Signature=…".
func parseAuthorization(h string) (Auth, *AuthError) {
	h = strings.TrimSpace(h)
	if h == "" {
		return Auth{}, fail(ReasonMissing, s3.AccessDenied, "Missing Authorization header.")
	}
	algo, rest, _ := strings.Cut(h, " ")
	switch algo {
	case Algorithm:
	case AlgorithmA:
		return Auth{}, fail(ReasonSigV4A, s3.NotImplemented, "AWS4-ECDSA-P256-SHA256 (SigV4A) is not supported; use AWS4-HMAC-SHA256.")
	case "AWS":
		return Auth{}, fail(ReasonMalformed, s3.InvalidRequest, "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.")
	default:
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, "Unsupported authorization algorithm.")
	}
	parts := strings.Split(rest, ",")
	if len(parts) != 3 {
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, "The authorization header requires three components: Credential, SignedHeaders, and Signature.")
	}
	var a Auth
	for i, want := range []string{"Credential", "SignedHeaders", "Signature"} {
		k, v, ok := strings.Cut(strings.TrimSpace(parts[i]), "=")
		if !ok || k != want {
			return Auth{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, "The authorization header is malformed; expected "+want+" at position "+strconv.Itoa(i+1)+".")
		}
		switch i {
		case 0:
			var err *AuthError
			a.AccessKey, a.Scope, err = parseCredential(v)
			if err != nil {
				return Auth{}, err
			}
		case 1:
			a.SignedHeaders = strings.Split(v, ";")
		case 2:
			a.Signature = v
		}
	}
	if !isHex(a.Signature, 64) {
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, "The Signature must be 64 hex characters.")
	}
	return a, nil
}

func parseCredential(v string) (string, Scope, *AuthError) {
	p := strings.Split(v, "/")
	if len(p) != 5 {
		return "", Scope{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, `The authorization header is malformed; the Credential is mal-formed; expecting "<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request".`)
	}
	if p[3] != Service {
		return "", Scope{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, `The authorization header is malformed; incorrect service "`+p[3]+`". This endpoint belongs to "s3".`)
	}
	if p[4] != terminator {
		return "", Scope{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, `The authorization header is malformed; incorrect terminal "`+p[4]+`". This endpoint uses "aws4_request".`)
	}
	if _, err := time.Parse(DateFormat, p[1]); err != nil {
		return "", Scope{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, `The authorization header is malformed; incorrect date format "`+p[1]+`". This date in the credential must be in the format "yyyyMMdd".`)
	}
	if p[0] == "" || p[2] == "" {
		return "", Scope{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, "The authorization header is malformed; empty access key or region.")
	}
	return p[0], Scope{Date: p[1], Region: p[2], Service: Service}, nil
}

// parsePresigned parses the X-Amz-* query parameters of a presigned URL.
func parsePresigned(r *http.Request, maxExpires int64) (Auth, *AuthError) {
	q := RawQuery(r)
	if q.Get("AWSAccessKeyId") != "" || (q.Get("Signature") != "" && q.Get("Expires") != "") {
		return Auth{}, fail(ReasonMalformed, s3.InvalidRequest, "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.")
	}
	missing := func() *AuthError {
		return fail(ReasonMalformed, s3.AuthorizationQueryParametersError, "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.")
	}
	algo := q.Get("X-Amz-Algorithm")
	switch algo {
	case "":
		return Auth{}, missing()
	case Algorithm:
	case AlgorithmA:
		return Auth{}, fail(ReasonSigV4A, s3.NotImplemented, `X-Amz-Algorithm only supports "AWS4-HMAC-SHA256".`)
	default:
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationQueryParametersError, `X-Amz-Algorithm only supports "AWS4-HMAC-SHA256".`)
	}
	cred, date, sig, sh, exp := q.Get("X-Amz-Credential"), q.Get("X-Amz-Date"), q.Get("X-Amz-Signature"), q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Expires")
	if cred == "" || date == "" || sig == "" || sh == "" || exp == "" {
		return Auth{}, missing()
	}
	ak, scope, err := parseCredential(cred)
	if err != nil {
		err.Err.Code = s3.AuthorizationQueryParametersError
		return Auth{}, err
	}
	if _, perr := time.Parse(TimeFormat, date); perr != nil {
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationQueryParametersError, "X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\".")
	}
	if date[:8] != scope.Date {
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationQueryParametersError, "Invalid credential date. Date is not the same as X-Amz-Date.")
	}
	n, perr := strconv.ParseInt(exp, 10, 64)
	switch {
	case perr != nil:
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationQueryParametersError, "X-Amz-Expires should be a number.")
	case n < 1:
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationQueryParametersError, "X-Amz-Expires must be at least 1 second.")
	case n > maxExpires:
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationQueryParametersError, "X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds.")
	}
	if !isHex(sig, 64) {
		return Auth{}, fail(ReasonMalformed, s3.AuthorizationQueryParametersError, "X-Amz-Signature must be 64 hex characters.")
	}
	return Auth{AccessKey: ak, Scope: scope, SignedHeaders: strings.Split(sh, ";"), Signature: sig, Presigned: true, Date: date, Expires: n}, nil
}

// ignoredHeaders need not appear in SignedHeaders even when present.
var ignoredHeaders = map[string]bool{"authorization": true, "user-agent": true, "expect": true, "transfer-encoding": true, "x-amzn-trace-id": true}

// Verify checks the request's signature against the store. now is injectable for tests.
func Verify(ctx context.Context, r *http.Request, store CredentialStore, now time.Time, o Options) (Identity, *AuthError) {
	if o.ClockSkew == 0 {
		o.ClockSkew = 15 * time.Minute
	}
	if o.MaxExpires == 0 {
		o.MaxExpires = 604800
	}
	if r.Header.Get("X-Amz-Security-Token") != "" || RawQuery(r).Get("X-Amz-Security-Token") != "" {
		return Identity{}, fail(ReasonToken, s3.InvalidToken, "Session tokens are not supported by this endpoint.")
	}

	var a Auth
	var aerr *AuthError
	var signTime time.Time
	authHeader := r.Header.Get("Authorization")
	q := RawQuery(r)
	switch {
	case authHeader != "":
		if a, aerr = parseAuthorization(authHeader); aerr != nil {
			return Identity{}, aerr
		}
		var terr *AuthError
		if signTime, terr = requestTime(r, now, o.ClockSkew); terr != nil {
			return Identity{}, terr
		}
		if signTime.UTC().Format(DateFormat) != a.Scope.Date {
			return Identity{}, fail(ReasonMalformed, s3.AuthorizationHeaderMalformed, "Invalid credential date. Date is not the same as X-Amz-Date.")
		}
	case q.Get("X-Amz-Algorithm") != "" || q.Get("X-Amz-Credential") != "" || q.Get("X-Amz-Signature") != "":
		if a, aerr = parsePresigned(r, o.MaxExpires); aerr != nil {
			return Identity{}, aerr
		}
		signTime, _ = time.Parse(TimeFormat, a.Date)
		if signTime.Add(time.Duration(a.Expires) * time.Second).Before(now) {
			return Identity{}, fail(ReasonExpired, s3.AccessDenied, "Request has expired")
		}
	default:
		return Identity{}, fail(ReasonMissing, s3.AccessDenied, "Anonymous requests are not supported by this endpoint. Provide an AWS4-HMAC-SHA256 Authorization header or a presigned URL.")
	}

	// Every x-amz-* header present must be signed (all modes); non-ignored others may be omitted.
	signedSet := make(map[string]bool, len(a.SignedHeaders))
	for _, h := range a.SignedHeaders {
		signedSet[strings.ToLower(h)] = true
	}
	for name := range r.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") && !signedSet[lower] && !ignoredHeaders[lower] {
			return Identity{}, fail(ReasonUnsignedHeaders, s3.AccessDenied, "There were headers present in the request which were not signed: "+lower)
		}
	}

	payloadHash := UnsignedPayload
	mode := PayloadUnsigned
	if !a.Presigned {
		v := r.Header.Get("X-Amz-Content-Sha256")
		mode = parsePayloadMode(v)
		switch mode {
		case PayloadMissing:
			if o.RequireHash && r.Method != http.MethodHead {
				return Identity{}, fail(ReasonMissingSHA256, s3.InvalidRequest, "Missing required header for this request: x-amz-content-sha256")
			}
			v = ""
		case PayloadInvalid:
			return Identity{}, fail(ReasonMalformed, s3.InvalidArgument, "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, a STREAMING-* value, or a 64-character hex SHA-256.")
		case PayloadStreamingECDSA:
			return Identity{}, fail(ReasonSigV4A, s3.NotImplemented, "The chunk encoding algorithm "+v+" is not supported.")
		}
		payloadHash = v
	}

	cred, err := store.Lookup(ctx, a.AccessKey)
	if err != nil {
		if errors.Is(err, ErrUnknownAccessKey) {
			return Identity{}, fail(ReasonUnknownKey, s3.InvalidAccessKeyId, "")
		}
		return Identity{}, fail(ReasonUnknownKey, s3.InternalError, "credential store error")
	}

	canon := CanonicalRequest(r.Method, RawPath(r), canonicalQueryValues(r, a.Presigned), r.Header, r.Host, r.ContentLength, a.SignedHeaders, payloadHash)
	key := SigningKey(cred.Secret, a.Scope)
	want := Signature(key, StringToSign(signTime, a.Scope, canon))
	if !constantTimeEqual(want, a.Signature) {
		return Identity{}, fail(ReasonSignature, s3.SignatureDoesNotMatch, "")
	}
	return Identity{
		Credential: cred, Scope: a.Scope, SigningKey: key, Signature: a.Signature, SignedHeaders: normalizeSignedHeaders(a.SignedHeaders),
		Time: signTime, Presigned: a.Presigned, Payload: mode, PayloadHash: payloadHash,
	}, nil
}

// canonicalQueryValues returns the query for canonicalization; presigned requests exclude
// X-Amz-Signature.
func canonicalQueryValues(r *http.Request, presigned bool) map[string][]string {
	q := RawQuery(r)
	if presigned {
		delete(q, "X-Amz-Signature")
	}
	return q
}

// requestTime reads x-amz-date (ISO 8601 basic) or the Date header (RFC 1123) and enforces skew.
func requestTime(r *http.Request, now time.Time, skew time.Duration) (time.Time, *AuthError) {
	var t time.Time
	var err error
	switch {
	case r.Header.Get("X-Amz-Date") != "":
		t, err = time.Parse(TimeFormat, r.Header.Get("X-Amz-Date"))
	case r.Header.Get("Date") != "":
		t, err = http.ParseTime(r.Header.Get("Date"))
	default:
		return t, fail(ReasonMissing, s3.AccessDenied, "AWS authentication requires a valid Date or x-amz-date header")
	}
	if err != nil {
		return t, fail(ReasonMalformed, s3.AccessDenied, "AWS authentication requires a valid Date or x-amz-date header")
	}
	if d := now.Sub(t); d > skew || d < -skew {
		return t, fail(ReasonSkew, s3.RequestTimeTooSkewed, "The difference between the request time and the server's time is too large.")
	}
	return t, nil
}

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || (c > '9' && c < 'A') || (c > 'F' && c < 'a') || c > 'f' {
			return false
		}
	}
	return true
}
