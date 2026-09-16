package sigv4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// The AWS SDK signer is the oracle (docs/DESIGN.md P2 item 6). Test-only import.
var sdk = v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })

type mapStore map[string]Credential

func (m mapStore) Lookup(_ context.Context, ak string) (Credential, error) {
	c, ok := m[ak]
	if !ok {
		return Credential{}, ErrUnknownAccessKey
	}
	return c, nil
}

const (
	testAK     = "AKIAIOSFODNN7EXAMPLE"
	testSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testRegion = "us-east-1"
)

var store = mapStore{testAK: {AccessKey: testAK, Secret: testSecret, Tenant: "t"}}
var now = time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

// serverSide turns a client-side request (as the SDK signed it) into what net/http's server
// would hand a handler: RequestURI set, Host set, body length known.
func serverSide(t *testing.T, r *http.Request) *http.Request {
	t.Helper()
	uri := r.URL.RequestURI()
	sr := httptest.NewRequest(r.Method, uri, r.Body)
	sr.RequestURI = uri
	sr.Host = r.Host
	if sr.Host == "" {
		sr.Host = r.URL.Host
	}
	sr.Header = r.Header.Clone()
	sr.ContentLength = r.ContentLength
	return sr
}

// trickyKeys mirrors s3diff's matrix.
var trickyKeys = []string{
	"/b/dir/plain.bin", "/b/dir%20with%20space/file%20name.bin", "/b/dir/%C3%BCn%C3%AFc%C3%B6d%C3%A9-%E6%97%A5%E6%9C%AC%E8%AA%9E.bin",
	"/b/dir/a+b.bin", "/b/dir/100%25.bin", "/b/dir//double.bin", "/b/dir/trailing/", "/b/" + strings.Repeat("k", 1024),
	"/b/a%2Fb", "/b/~tilde", "/b/star*.bin", "/", "/b", "/b/",
}

var queries = []string{"", "list-type=2&prefix=dir%2F&max-keys=3", "uploads", "acl", "partNumber=1&uploadId=a%2Bb%3Dc", "versionId=x&tagging", "a=z&a=b&A=1", "sp%20ace=v%20al&plus=a%2Bb"}

func TestSDKSignsShuntVerifies(t *testing.T) {
	for _, path := range trickyKeys {
		for _, q := range queries {
			for _, method := range []string{"GET", "PUT", "HEAD", "DELETE", "POST"} {
				name := method + " " + path + "?" + q
				t.Run(name, func(t *testing.T) {
					body := "payload"
					r, _ := http.NewRequest(method, "https://b.shunt.example.com:8443"+path+"?"+q, strings.NewReader(body)) //nolint:noctx // test
					r.ContentLength = int64(len(body))
					r.Header.Set("X-Amz-Meta-Foo", "  bar   baz ")
					r.Header.Add("X-Amz-Meta-Multi", "one")
					r.Header.Add("X-Amz-Meta-Multi", "two")
					r.Header.Set("Content-Type", "application/octet-stream")
					r.Header.Set("Content-Md5", "1B2M2Y8AsgTpgAmY7PhCfg==")
					r.Header.Set("Expect", "100-continue")
					hash := UnsignedPayload
					if method == "PUT" {
						hash = "315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3"
					}
					r.Header.Set("X-Amz-Content-Sha256", hash) // the S3 client middleware sets this; the raw signer does not
					if err := sdk.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSecret}, r, hash, Service, testRegion, now); err != nil {
						t.Fatal(err)
					}
					id, aerr := Verify(context.Background(), serverSide(t, r), store, now.Add(3*time.Minute), Options{RequireHash: true})
					if aerr != nil {
						t.Fatalf("verify failed: %v\nAuthorization: %s", aerr, r.Header.Get("Authorization"))
					}
					if id.Credential.Tenant != "t" || id.Scope.Region != testRegion || id.PayloadHash != hash {
						t.Fatalf("identity: %+v", id)
					}
				})
			}
		}
	}
}

func TestShuntSignsMatchesSDK(t *testing.T) {
	for _, path := range trickyKeys {
		for _, q := range queries {
			t.Run(path+"?"+q, func(t *testing.T) {
				mk := func() *http.Request {
					r, _ := http.NewRequest("PUT", "http://127.0.0.1:3900"+path+"?"+q, strings.NewReader("xyz")) //nolint:noctx // test
					r.ContentLength = 3
					r.Header.Set("X-Amz-Meta-A", "1")
					r.Header.Set("X-Amz-Decoded-Content-Length", "3")
					r.Header.Set("Content-Type", "text/plain")
					r.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
					return r
				}
				ours, theirs := mk(), mk()
				Sign(ours, Credentials{AccessKey: testAK, Secret: testSecret}, testRegion, UnsignedPayload, now)
				theirs.Header.Set("X-Amz-Date", now.Format(TimeFormat))
				theirs.Header.Set("X-Amz-Content-Sha256", UnsignedPayload)
				if err := sdk.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSecret}, theirs, UnsignedPayload, Service, testRegion, now); err != nil {
					t.Fatal(err)
				}
				if a, b := ours.Header.Get("Authorization"), theirs.Header.Get("Authorization"); a != b {
					t.Fatalf("authorization differs\nours:   %s\ntheirs: %s", a, b)
				}
			})
		}
	}
}

func TestShuntSignsShuntVerifies(t *testing.T) {
	r, _ := http.NewRequest("GET", "http://h/b/k?x=1", nil) //nolint:noctx // test
	Sign(r, Credentials{AccessKey: testAK, Secret: testSecret}, "garage", UnsignedPayload, now)
	id, aerr := Verify(context.Background(), serverSide(t, r), store, now, Options{})
	if aerr != nil || id.Scope.Region != "garage" {
		t.Fatalf("%v %+v", aerr, id)
	}
}

func TestPresignedRoundTrip(t *testing.T) {
	r, _ := http.NewRequest("GET", "https://b.shunt.example.com/dir/k%20ey?versionId=1", nil) //nolint:noctx // test
	signedURL, _, err := sdk.PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSecret}, r, UnsignedPayload, Service, testRegion, now)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(signedURL)
	u.RawQuery += "&X-Amz-Expires=300"
	// The SDK's PresignHTTP does not add X-Amz-Expires itself (the S3 client does); sign again with it present.
	r2, _ := http.NewRequest("GET", "https://b.shunt.example.com/dir/k%20ey?versionId=1&X-Amz-Expires=300", nil) //nolint:noctx // test
	signedURL, _, err = sdk.PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSecret}, r2, UnsignedPayload, Service, testRegion, now)
	if err != nil {
		t.Fatal(err)
	}
	get, _ := http.NewRequest("GET", signedURL, nil) //nolint:noctx // test
	sr := serverSide(t, get)
	id, aerr := Verify(context.Background(), sr, store, now.Add(4*time.Minute), Options{})
	if aerr != nil {
		t.Fatalf("presigned verify: %v\n%s", aerr, signedURL)
	}
	if !id.Presigned || id.PayloadHash != UnsignedPayload {
		t.Fatalf("identity %+v", id)
	}
	if _, aerr = Verify(context.Background(), sr, store, now.Add(6*time.Minute), Options{}); aerr == nil || aerr.Reason != ReasonExpired {
		t.Fatalf("expected expired, got %v", aerr)
	}
	// Tampered signature.
	fu, _ := url.Parse(signedURL)
	bad := strings.Replace(signedURL, "X-Amz-Signature="+fu.Query().Get("X-Amz-Signature")[:8], "X-Amz-Signature=00000000", 1)
	if bad == signedURL {
		t.Fatal("tamper did not apply")
	}
	badReq, _ := http.NewRequest("GET", bad, nil) //nolint:noctx // test
	if _, aerr = Verify(context.Background(), serverSide(t, badReq), store, now, Options{}); aerr == nil || aerr.Reason != ReasonSignature {
		t.Fatalf("expected signature failure, got %v", aerr)
	}
}

func TestRejections(t *testing.T) {
	good := func() *http.Request {
		r, _ := http.NewRequest("PUT", "http://h/b/k", strings.NewReader("x")) //nolint:noctx // test
		r.ContentLength = 1
		Sign(r, Credentials{AccessKey: testAK, Secret: testSecret}, testRegion, UnsignedPayload, now)
		return r
	}
	cases := []struct {
		name   string
		mutate func(r *http.Request)
		reason Reason
		status int
	}{
		{"missing auth", func(r *http.Request) { r.Header.Del("Authorization") }, ReasonMissing, 403},
		{"sigv2", func(r *http.Request) { r.Header.Set("Authorization", "AWS AKID:sig") }, ReasonMalformed, 400},
		{"sigv4a", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), Algorithm, AlgorithmA, 1))
		}, ReasonSigV4A, 501},
		{"two components", func(r *http.Request) {
			r.Header.Set("Authorization", Algorithm+" Credential=a/b/c/s3/aws4_request, Signature=x")
		}, ReasonMalformed, 400},
		{"bad scope", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "/s3/aws4_request", "/sqs/aws4_request", 1))
		}, ReasonMalformed, 400},
		{"unknown key", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), testAK, "AKIANOPE", 1))
		}, ReasonUnknownKey, 403},
		{"tampered signature", func(r *http.Request) {
			r.Header.Set("Authorization", r.Header.Get("Authorization")[:len(r.Header.Get("Authorization"))-4]+"0000")
		}, ReasonSignature, 403},
		{"unsigned x-amz header", func(r *http.Request) { r.Header.Set("X-Amz-Meta-Extra", "1") }, ReasonUnsignedHeaders, 403},
		{"security token", func(r *http.Request) { r.Header.Set("X-Amz-Security-Token", "tok") }, ReasonToken, 400},
		{"missing sha256", func(r *http.Request) { r.Header.Del("X-Amz-Content-Sha256") }, ReasonMissingSHA256, 400},
		{"invalid sha256", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", "nothex") }, ReasonMalformed, 400},
		{"ecdsa streaming", func(r *http.Request) {
			r.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD")
		}, ReasonSigV4A, 501},
		{"no date", func(r *http.Request) { r.Header.Del("X-Amz-Date") }, ReasonMissing, 403},
		{"scope date mismatch", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "/20260915/", "/20260914/", 1))
		}, ReasonMalformed, 400},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := good()
			c.mutate(r)
			_, aerr := Verify(context.Background(), serverSide(t, r), store, now, Options{RequireHash: true})
			if aerr == nil {
				t.Fatal("expected rejection")
			}
			if aerr.Reason != c.reason || aerr.Err.Status != c.status {
				t.Fatalf("got reason=%s status=%d (%s), want %s/%d", aerr.Reason, aerr.Err.Status, aerr.Err.Message, c.reason, c.status)
			}
		})
	}
}

func TestSkewAndDateHeader(t *testing.T) {
	r, _ := http.NewRequest("GET", "http://h/b/k", nil) //nolint:noctx // test
	Sign(r, Credentials{AccessKey: testAK, Secret: testSecret}, testRegion, UnsignedPayload, now)
	for _, d := range []time.Duration{14 * time.Minute, -14 * time.Minute} {
		if _, aerr := Verify(context.Background(), serverSide(t, r), store, now.Add(d), Options{}); aerr != nil {
			t.Errorf("skew %v rejected: %v", d, aerr)
		}
	}
	for _, d := range []time.Duration{16 * time.Minute, -16 * time.Minute} {
		if _, aerr := Verify(context.Background(), serverSide(t, r), store, now.Add(d), Options{}); aerr == nil || aerr.Reason != ReasonSkew {
			t.Errorf("skew %v accepted: %v", d, aerr)
		}
	}
	// Date header fallback (RFC 1123): sign by hand with only host;date in SignedHeaders.
	r2, _ := http.NewRequest("GET", "http://h/b/k", nil) //nolint:noctx // test
	r2.Header.Set("Date", now.Format(http.TimeFormat))
	r2.Header.Set("X-Amz-Content-Sha256", UnsignedPayload)
	scope := Scope{Date: now.Format(DateFormat), Region: testRegion, Service: Service}
	canon := CanonicalRequest("GET", "/b/k", nil, r2.Header, "h", 0, []string{"host", "date", "x-amz-content-sha256"}, UnsignedPayload)
	sig := Signature(SigningKey(testSecret, scope), StringToSign(now, scope, canon))
	r2.Header.Set("Authorization", Algorithm+" Credential="+testAK+"/"+scope.String()+", SignedHeaders=date;host;x-amz-content-sha256, Signature="+sig)
	if _, aerr := Verify(context.Background(), serverSide(t, r2), store, now, Options{}); aerr != nil {
		t.Fatalf("Date header not honored: %v", aerr)
	}
}

func TestCredentialRedaction(t *testing.T) {
	c := Credential{AccessKey: "AK", Secret: "TOPSECRET", Tenant: "t"}
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), fmt.Sprintf("%s", c), c.String()} {
		if strings.Contains(s, "TOPSECRET") {
			t.Fatalf("secret leaked: %s", s)
		}
	}
	if !errors.Is(fmt.Errorf("wrap: %w", ErrUnknownAccessKey), ErrUnknownAccessKey) {
		t.Fatal("sentinel")
	}
}

func TestCanonicalPieces(t *testing.T) {
	if got := canonicalQuery(url.Values{"b": {"2", "1"}, "a": {""}, "s p": {"v+w"}}); got != "a=&b=1&b=2&s%20p=v%2Bw" {
		t.Errorf("query: %s", got)
	}
	if got := foldSpace("  a  \t b   c "); got != "a b c" {
		t.Errorf("fold: %q", got)
	}
	if Escape("A-_.~z") != "A-_.~z" || Escape("a b/€") != "a%20b%2F%E2%82%AC" {
		t.Errorf("escape")
	}
	sr := httptest.NewRequest("GET", "/b/k%2Fx?a=1", nil)
	sr.RequestURI = "/b/k%2Fx?a=1"
	if RawPath(sr) != "/b/k%2Fx" {
		t.Errorf("raw path: %s", RawPath(sr))
	}
	if parsePayloadMode("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER") != PayloadStreamingSignedTrailer || parsePayloadMode(strings.Repeat("a", 64)) != PayloadSHA256 || parsePayloadMode("zz") != PayloadInvalid {
		t.Error("payload mode")
	}
	if StringToSign(now, Scope{"20260915", "r", "s3"}, "x")[:len(Algorithm)] != Algorithm {
		t.Error("sts")
	}
}

func FuzzParseAuthorization(f *testing.F) {
	f.Add(Algorithm + " Credential=AK/20260915/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=" + strings.Repeat("a", 64))
	f.Add("AWS AK:sig")
	f.Add(Algorithm + " Credential=,SignedHeaders=,Signature=")
	f.Add("")
	f.Fuzz(func(_ *testing.T, in string) {
		_, _ = parseAuthorization(in)
	})
}

func FuzzCanonicalRequest(f *testing.F) {
	f.Add("GET", "/b/k%20x", "a=1&b=", "host;x-amz-date", "x-amz-date", "20260915T120000Z")
	f.Add("PUT", "", "", "", "", "")
	f.Fuzz(func(t *testing.T, method, path, query, signed, hname, hval string) {
		q, err := url.ParseQuery(query)
		if err != nil {
			q = url.Values{}
		}
		h := http.Header{}
		if hname != "" && !strings.ContainsAny(hname, " :\r\n") {
			h.Set(hname, hval)
		}
		c := CanonicalRequest(method, path, q, h, "host", 3, strings.Split(signed, ";"), UnsignedPayload)
		if strings.Count(c, "\n") < 5 {
			t.Fatalf("canonical request has too few lines: %q", c)
		}
	})
}

func BenchmarkVerifyHeader(b *testing.B) {
	r, _ := http.NewRequest("PUT", "http://h/b/dir/key.bin?partNumber=3&uploadId=abc", strings.NewReader("x")) //nolint:noctx // test
	r.ContentLength = 1
	r.Header.Set("X-Amz-Meta-A", "1")
	r.Header.Set("Content-Type", "application/octet-stream")
	Sign(r, Credentials{AccessKey: testAK, Secret: testSecret}, testRegion, UnsignedPayload, now)
	sr := serverSide(&testing.T{}, r)
	b.ReportAllocs()
	for b.Loop() {
		if _, aerr := Verify(context.Background(), sr, store, now, Options{}); aerr != nil {
			b.Fatal(aerr)
		}
	}
}

func BenchmarkSign(b *testing.B) {
	r, _ := http.NewRequest("PUT", "http://127.0.0.1:3900/b/dir/key.bin?partNumber=3&uploadId=abc", strings.NewReader("x")) //nolint:noctx // test
	r.ContentLength = 1
	r.Header.Set("X-Amz-Meta-A", "1")
	b.ReportAllocs()
	for b.Loop() {
		Sign(r, Credentials{AccessKey: testAK, Secret: testSecret}, testRegion, UnsignedPayload, now)
	}
}

func BenchmarkSigningKey(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = SigningKey(testSecret, Scope{"20260915", "us-east-1", "s3"})
	}
}

var _ io.Reader = strings.NewReader("")
