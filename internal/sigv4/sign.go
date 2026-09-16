package sigv4

import (
	"net/http"
	"strings"
	"time"
)

// Credentials are an access key and secret for signing upstream requests.
type Credentials struct {
	AccessKey string
	Secret    string
}

// Sign header-signs req in place with creds for the given region (docs/DESIGN.md §2.2 "sign the
// upstream request with the target cluster's credentials"). It sets x-amz-date and
// x-amz-content-sha256 (payloadHash, verbatim), and signs host, content-length (when known),
// content-md5 and content-type when present, and every x-amz-* header present. Callers strip
// the client's Authorization/x-amz-date/x-amz-content-sha256/x-amz-security-token first.
func Sign(req *http.Request, creds Credentials, region, payloadHash string, now time.Time) {
	now = now.UTC()
	req.Header.Set("X-Amz-Date", now.Format(TimeFormat))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signed := make([]string, 0, 8)
	signed = append(signed, "host")
	if sendsContentLength(req) {
		signed = append(signed, "content-length")
	}
	for name := range req.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") || lower == "content-md5" || lower == "content-type" {
			signed = append(signed, lower)
		}
	}
	scope := Scope{Date: now.Format(DateFormat), Region: region, Service: Service}
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	cl := req.ContentLength
	if req.Body == nil && cl == 0 {
		cl = 0
	}
	canon := CanonicalRequest(req.Method, RawPath(req), RawQuery(req), req.Header, host, cl, signed, payloadHash)
	sts := StringToSign(now, scope, canon)
	sig := Signature(SigningKey(creds.Secret, scope), sts)
	names := normalizeSignedHeaders(signed)
	req.Header.Set("Authorization", Algorithm+" Credential="+creds.AccessKey+"/"+scope.String()+
		", SignedHeaders="+strings.Join(names, ";")+", Signature="+sig)
}

// sendsContentLength reports whether content-length should be signed: only when net/http's client
// will put the header on req. A positive length always goes out; a zero length only with a body on
// the methods that carry one (PUT, POST, PATCH). A DELETE with http.NoBody sends none, and Garage
// refuses a signature that lists a header the request does not carry. A request with no Body yet
// (signed before its body is built, as aws-chunked seeds are) is not signed for it either: signing
// fewer headers than are sent is valid, signing one that is not sent is not.
func sendsContentLength(req *http.Request) bool {
	switch {
	case req.ContentLength > 0:
		return true
	case req.ContentLength < 0, req.Body == nil:
		return false
	}
	switch req.Method {
	case http.MethodPut, http.MethodPost, http.MethodPatch:
		return true
	}
	return false
}
