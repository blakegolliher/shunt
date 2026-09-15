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
	if req.ContentLength >= 0 && req.Method != http.MethodGet && req.Method != http.MethodHead && (req.Body != nil || req.ContentLength > 0) {
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
