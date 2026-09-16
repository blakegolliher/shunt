package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Algorithm strings.
const (
	Algorithm  = "AWS4-HMAC-SHA256"
	AlgorithmA = "AWS4-ECDSA-P256-SHA256"
	Service    = "s3"
	terminator = "aws4_request"
	TimeFormat = "20060102T150405Z"
	DateFormat = "20060102"

	// Payload hash sentinels (x-amz-content-sha256).
	UnsignedPayload                 = "UNSIGNED-PAYLOAD"
	StreamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	StreamingSignedPayload          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingSignedPayloadTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// Scope is the credential scope: date/region/service/aws4_request.
type Scope struct {
	Date    string // YYYYMMDD
	Region  string
	Service string
}

func (s Scope) String() string {
	return s.Date + "/" + s.Region + "/" + s.Service + "/" + terminator
}

// CanonicalRequest builds the SigV4 canonical request with the S3 rules:
//   - rawPath is used verbatim (single-encoded as sent; no normalization). Empty → "/".
//   - query keys and values are sorted (key, then value) and RFC 3986-encoded; empty values keep "=".
//   - only signedHeaders are included, lowercased and sorted, values trimmed with interior
//     whitespace runs folded to one space, multi-values joined by ",".
//   - contentLength is signed from the numeric length when "content-length" is listed.
func CanonicalRequest(method, rawPath string, query url.Values, headers http.Header, host string, contentLength int64, signedHeaders []string, payloadHash string) string {
	if rawPath == "" {
		rawPath = "/"
	}
	var b strings.Builder
	b.Grow(512)
	b.WriteString(method)
	b.WriteByte('\n')
	b.WriteString(rawPath)
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(query))
	b.WriteByte('\n')
	names := normalizeSignedHeaders(signedHeaders)
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(canonicalHeaderValue(name, headers, host, contentLength))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(strings.Join(names, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String()
}

// normalizeSignedHeaders lowercases, de-duplicates, and sorts the SignedHeaders list.
func normalizeSignedHeaders(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, h := range in {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" || seen[h] {
			continue
		}
		seen[h] = true
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func canonicalHeaderValue(name string, headers http.Header, host string, contentLength int64) string {
	switch name {
	case "host":
		return strings.TrimSpace(host)
	case "content-length":
		if contentLength >= 0 {
			return strconv.FormatInt(contentLength, 10)
		}
	}
	vals := headers[http.CanonicalHeaderKey(name)]
	if len(vals) == 0 {
		return ""
	}
	if len(vals) == 1 {
		return foldSpace(vals[0])
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = foldSpace(v)
	}
	return strings.Join(parts, ",")
}

// foldSpace trims and collapses interior runs of spaces and tabs to a single space.
func foldSpace(v string) string {
	v = strings.TrimSpace(v)
	if !strings.ContainsAny(v, "  \t") {
		return v
	}
	var b strings.Builder
	b.Grow(len(v))
	space := false
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == ' ' || c == '\t' {
			space = true
			continue
		}
		if space {
			b.WriteByte(' ')
			space = false
		}
		b.WriteByte(c)
	}
	return b.String()
}

// canonicalQuery sorts by key then value and encodes per RFC 3986 (space → %20, "+" → %2B,
// unreserved A-Za-z0-9-_.~ kept, uppercase hex). Empty values render as "key=".
func canonicalQuery(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	first := true
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		ek := Escape(k)
		for _, v := range vals {
			if !first {
				b.WriteByte('&')
			}
			first = false
			b.WriteString(ek)
			b.WriteByte('=')
			b.WriteString(Escape(v))
		}
	}
	return b.String()
}

const upperHex = "0123456789ABCDEF"

// Escape percent-encodes everything except RFC 3986 unreserved characters.
func Escape(s string) string {
	n := 0
	for i := 0; i < len(s); i++ {
		if !unreserved(s[i]) {
			n++
		}
	}
	if n == 0 {
		return s
	}
	out := make([]byte, 0, len(s)+2*n)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if unreserved(c) {
			out = append(out, c)
			continue
		}
		out = append(out, '%', upperHex[c>>4], upperHex[c&15])
	}
	return string(out)
}

func unreserved(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~'
}

// StringToSign for a header- or query-signed request.
func StringToSign(t time.Time, scope Scope, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	return Algorithm + "\n" + t.UTC().Format(TimeFormat) + "\n" + scope.String() + "\n" + hex.EncodeToString(sum[:])
}

// SigningKey derives kSigning = HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request").
func SigningKey(secret string, scope Scope) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), []byte(scope.Date))
	k = hmacSHA256(k, []byte(scope.Region))
	k = hmacSHA256(k, []byte(scope.Service))
	return hmacSHA256(k, []byte(terminator))
}

// Signature is hex(HMAC(kSigning, stringToSign)).
func Signature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// RawPath returns the request-target path exactly as sent on the wire, up to the first '?'.
// Never EscapedPath(): S3 signs the bytes the client sent.
func RawPath(r *http.Request) string {
	uri := r.RequestURI
	if uri == "" { // client-side requests (tests, upstream signing) carry the path in the URL
		uri = r.URL.EscapedPath()
		if r.URL.RawQuery != "" {
			uri += "?" + r.URL.RawQuery
		}
	}
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		uri = uri[:i]
	}
	if uri == "" || uri[0] != '/' {
		if u, err := url.ParseRequestURI(uri); err == nil && u.Path != "" {
			return u.EscapedPath()
		}
		return "/"
	}
	return uri
}

// RawQuery returns the decoded query for canonicalization ("decode then re-encode").
func RawQuery(r *http.Request) url.Values {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return url.Values{}
	}
	return q
}
