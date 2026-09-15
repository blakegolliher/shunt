package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/sigv4/chunked"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

const (
	clientAK, clientSecret   = "SHUNTCLIENTKEY", "client-secret-DO-NOT-LEAK-1"
	clusterAK, clusterSecret = "GARAGEBACKENDKEY", "cluster-secret-DO-NOT-LEAK-2"
	clusterRegion            = "garage"
)

type mapStore map[string]sigv4.Credential

func (m mapStore) Lookup(_ context.Context, ak string) (sigv4.Credential, error) {
	c, ok := m[ak]
	if !ok {
		return sigv4.Credential{}, sigv4.ErrUnknownAccessKey
	}
	return c, nil
}

// seen is what the backend recorded about the upstream request.
type seen struct {
	mu            sync.Mutex
	method, uri   string
	host          string
	header        http.Header
	contentLength int64
	bodyLen       int64
	bodySHA       string
	verified      *sigv4.AuthError // result of verifying the upstream signature with the cluster creds
}

// backend records the request, verifies its signature with the cluster credentials, and answers.
func backend(t *testing.T, rec *seen, status int, respBody string) http.Handler {
	t.Helper()
	store := mapStore{clusterAK: {AccessKey: clusterAK, Secret: clusterSecret, Tenant: "cluster"}}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.method, rec.uri, rec.host, rec.header, rec.contentLength = r.Method, r.RequestURI, r.Host, r.Header.Clone(), r.ContentLength
		_, aerr := sigv4.Verify(r.Context(), r, store, time.Now(), sigv4.Options{RequireHash: true})
		rec.verified = aerr
		h := sha256.New()
		n, _ := io.Copy(h, r.Body)
		rec.bodyLen, rec.bodySHA = n, hex.EncodeToString(h.Sum(nil))
		if aerr != nil {
			w.WriteHeader(403)
			_, _ = w.Write([]byte("<Error><Code>SignatureDoesNotMatch</Code></Error>"))
			return
		}
		w.Header().Set("X-Amz-Request-Id", "UP")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	})
}

type rrig struct {
	*rig
	rec    *seen
	errLog *syncBuf
}

func newResignRig(t *testing.T, be http.Handler, caps Capabilities) *rrig {
	t.Helper()
	r := newRig(t, be, time.Second)
	errLog := &syncBuf{}
	r.h.Mode = ModeResign
	r.h.Store = mapStore{clientAK: {AccessKey: clientAK, Secret: clientSecret, Tenant: "acme"}}
	r.h.ClusterCreds = sigv4.Credentials{AccessKey: clusterAK, Secret: clusterSecret}
	r.h.Capabilities = caps
	r.h.Log = slog.New(slog.NewJSONHandler(errLog, nil))
	r.h.Cluster.Region = clusterRegion
	return &rrig{rig: r, errLog: errLog}
}

// clientSign signs a client request with the shunt-issued key, as an SDK would.
func clientSign(r *http.Request, payloadHash string) {
	sigv4.Sign(r, sigv4.Credentials{AccessKey: clientAK, Secret: clientSecret}, "us-east-1", payloadHash, time.Now())
}

func do(t *testing.T, r *http.Request) (*http.Response, []byte) {
	t.Helper()
	resp, err := fresh().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func TestResignUnsignedPayloadRow(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, "ok"), Capabilities{true, true})
	body := []byte("hello resign")
	req, _ := http.NewRequest(http.MethodPut, r.front.URL+"/b/dir/k%20ey?x=1", bytes.NewReader(body))
	req.Host = "bkt.shunt.example.com:8443" // virtual-host client
	req.ContentLength = int64(len(body))
	req.Header.Set("X-Amz-Meta-Keep", "yes")
	req.Header.Set("Content-Type", "text/plain")
	clientSign(req, sigv4.UnsignedPayload)
	resp, b := do(t, req)
	if resp.StatusCode != 200 || string(b) != "ok" {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	if rec.verified != nil {
		t.Fatalf("upstream signature invalid: %v\n%v", rec.verified, rec.header)
	}
	if rec.uri != "/bkt/b/dir/k%20ey?x=1" || rec.host != strings.TrimPrefix(r.backend.URL, "http://") {
		t.Errorf("upstream target: %s %s", rec.uri, rec.host)
	}
	if rec.header.Get("X-Amz-Content-Sha256") != sigv4.UnsignedPayload || rec.header.Get("X-Amz-Meta-Keep") != "yes" || rec.header.Get("Content-Type") != "text/plain" {
		t.Errorf("headers: %v", rec.header)
	}
	if rec.header.Get("X-Amz-Security-Token") != "" || strings.Contains(rec.header.Get("Authorization"), clientAK) {
		t.Errorf("client auth leaked upstream: %v", rec.header)
	}
	if !strings.Contains(rec.header.Get("Authorization"), clusterAK+"/") || !strings.Contains(rec.header.Get("Authorization"), "/"+clusterRegion+"/s3/") {
		t.Errorf("upstream not signed with cluster creds and region: %s", rec.header.Get("Authorization"))
	}
	if rec.bodyLen != int64(len(body)) || rec.contentLength != int64(len(body)) {
		t.Errorf("body %d content-length %d", rec.bodyLen, rec.contentLength)
	}
}

func TestResignSHA256RowPassesHeaderVerbatim(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{true, true})
	body := []byte("payload")
	sum := sha256.Sum256(body)
	req, _ := http.NewRequest(http.MethodPut, r.front.URL+"/b/k", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	clientSign(req, hex.EncodeToString(sum[:]))
	resp, _ := do(t, req)
	if resp.StatusCode != 200 || rec.header.Get("X-Amz-Content-Sha256") != hex.EncodeToString(sum[:]) || rec.verified != nil {
		t.Fatalf("%d %v %v", resp.StatusCode, rec.header.Get("X-Amz-Content-Sha256"), rec.verified)
	}
}

func TestResignSHA256CompensationWhenBackendDoesNotEnforce(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{EnforcesSHA256: false, UnsignedTrailer: true})
	body := []byte("payload")
	wrong := strings.Repeat("0", 64)
	req, _ := http.NewRequest(http.MethodPut, r.front.URL+"/b/k", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	clientSign(req, wrong)
	resp, _ := do(t, req)
	if resp.StatusCode != 200 { // log-and-alert only: the backend's answer is relayed
		t.Fatalf("status %d", resp.StatusCode)
	}
	if !strings.Contains(r.errLog.String(), `"reason":"sha256"`) {
		t.Fatalf("no compensation alert: %s", r.errLog.String())
	}
	if v := metric(t, r.h.Metrics, "shunt_compensation_total", `outcome="logged",reason="sha256"`); v != 1 {
		t.Fatalf("compensation metric %v", v)
	}
}

func signedChunkRequest(t *testing.T, url string, payload []byte, trailer string) (*http.Request, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(payload)))
	req.Header.Set("Content-Encoding", "aws-chunked")
	hash := sigv4.StreamingSignedPayload
	if trailer != "" {
		hash = sigv4.StreamingSignedPayloadTrailer
		req.Header.Set("X-Amz-Trailer", trailer)
	}
	req.Header.Set("X-Amz-Meta-Keep", "1")
	// Sign the headers first (the seed), then build the body from the seed signature.
	now := time.Now()
	sigv4.Sign(req, sigv4.Credentials{AccessKey: clientAK, Secret: clientSecret}, "us-east-1", hash, now)
	auth := req.Header.Get("Authorization")
	seed := auth[strings.LastIndex(auth, "Signature=")+len("Signature="):]
	date, _ := time.Parse(sigv4.TimeFormat, req.Header.Get("X-Amz-Date"))
	scope := sigv4.Scope{Date: date.Format(sigv4.DateFormat), Region: "us-east-1", Service: "s3"}
	var th = crc32.NewIEEE()
	if trailer == "" {
		th = nil
	}
	enc := chunked.NewSignedEncoder(bytes.NewReader(payload), sigv4.SigningKey(clientSecret, scope), seed, date, "us-east-1", trailer, th, 8192)
	wire, err := io.ReadAll(enc)
	if err != nil {
		t.Fatal(err)
	}
	req.Body = io.NopCloser(bytes.NewReader(wire))
	req.ContentLength = int64(len(wire))
	return req, wire
}

func TestResignStreamingSignedRowDecodes(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{true, true})
	payload := bytes.Repeat([]byte("q"), 20000)
	req, _ := signedChunkRequest(t, r.front.URL+"/b/k", payload, "")
	resp, b := do(t, req)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s log=%s", resp.StatusCode, b, r.log.String())
	}
	sum := sha256.Sum256(payload)
	if rec.verified != nil || rec.bodySHA != hex.EncodeToString(sum[:]) || rec.contentLength != int64(len(payload)) {
		t.Fatalf("upstream got verified=%v len=%d sha ok=%v", rec.verified, rec.contentLength, rec.bodySHA == hex.EncodeToString(sum[:]))
	}
	h := rec.header
	if h.Get("X-Amz-Content-Sha256") != sigv4.UnsignedPayload || h.Get("Content-Encoding") != "" || h.Get("X-Amz-Decoded-Content-Length") != "" || h.Get("X-Amz-Trailer") != "" || h.Get("X-Amz-Meta-Keep") != "1" {
		t.Errorf("header rewrite (amendment 4): %v", h)
	}
}

func TestResignStreamingSignedTrailerRowReencodes(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{true, true})
	payload := bytes.Repeat([]byte("t"), 150000)
	req, _ := signedChunkRequest(t, r.front.URL+"/b/k", payload, "x-amz-checksum-crc32")
	resp, b := do(t, req)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s\nlog=%s", resp.StatusCode, b, r.waitLog(t, "GetObject"))
	}
	h := rec.header
	if h.Get("X-Amz-Content-Sha256") != sigv4.StreamingUnsignedPayloadTrailer || h.Get("Content-Encoding") != "aws-chunked" ||
		h.Get("X-Amz-Trailer") != "x-amz-checksum-crc32" || h.Get("X-Amz-Decoded-Content-Length") != strconv.Itoa(len(payload)) {
		t.Errorf("unsigned-trailer headers: %v", h)
	}
	if rec.verified != nil {
		t.Errorf("upstream signature: %v", rec.verified)
	}
	// Amendment 2: the upstream received exactly the declared Content-Length, never chunked TE.
	if rec.contentLength <= 0 || rec.bodyLen != rec.contentLength || h.Get("Transfer-Encoding") != "" {
		t.Errorf("content-length %d received %d te=%q", rec.contentLength, rec.bodyLen, h.Get("Transfer-Encoding"))
	}
	// And the frame decodes to the payload with a valid crc32 trailer.
	want := chunked.EncodedLength(int64(len(payload)), chunked.DefaultChunkSize, "x-amz-checksum-crc32", 8)
	if rec.contentLength != want {
		t.Errorf("content-length %d, precomputed %d", rec.contentLength, want)
	}
}

func TestResignStreamingSignedTrailerRowWithoutBackendSupport(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{EnforcesSHA256: true, UnsignedTrailer: false})
	payload := bytes.Repeat([]byte("u"), 5000)
	req, _ := signedChunkRequest(t, r.front.URL+"/b/k", payload, "x-amz-checksum-crc32")
	resp, _ := do(t, req)
	if resp.StatusCode != 200 || rec.header.Get("X-Amz-Content-Sha256") != sigv4.UnsignedPayload || rec.contentLength != int64(len(payload)) {
		t.Fatalf("%d %v len=%d", resp.StatusCode, rec.header.Get("X-Amz-Content-Sha256"), rec.contentLength)
	}
}

func TestResignChunkSignatureTamperMidStream(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{true, true})
	payload := bytes.Repeat([]byte("z"), 30000)
	req, wire := signedChunkRequest(t, r.front.URL+"/b/k", payload, "")
	// Flip a payload byte in the second chunk.
	i := bytes.Index(wire[9000:], []byte("\r\n")) + 9000 + 2 + 100
	wire[i] ^= 0xff
	req.Body = io.NopCloser(bytes.NewReader(wire))
	resp, b := do(t, req)
	if resp.StatusCode != 403 || !strings.Contains(string(b), "SignatureDoesNotMatch") {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	if rec.bodyLen >= int64(len(payload)) {
		t.Errorf("upstream received the full body (%d) despite the tamper", rec.bodyLen)
	}
	if v := metric(t, r.h.Metrics, "shunt_auth_failures_total", `reason="chunk_signature"`); v != 1 {
		t.Errorf("metric %v", v)
	}
}

func TestResignTrailerChecksumMismatch(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{EnforcesSHA256: true, UnsignedTrailer: false})
	payload := bytes.Repeat([]byte("c"), 4000)
	req, wire := signedChunkRequest(t, r.front.URL+"/b/k", payload, "x-amz-checksum-crc32")
	// Replace the checksum value with a valid-looking but wrong one; the trailer signature then
	// also fails, so the decoder rejects before the trailer signature check.
	s := string(wire)
	i := strings.Index(s, "x-amz-checksum-crc32:")
	j := strings.Index(s[i:], "\r\n") + i
	s = s[:i] + "x-amz-checksum-crc32:AAAAAA==" + s[j:]
	req.Body = io.NopCloser(strings.NewReader(s))
	req.ContentLength = int64(len(s))
	resp, b := do(t, req)
	if resp.StatusCode/100 != 4 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
}

func TestResignPresigned(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, "data"), Capabilities{true, true})
	// Build a presigned GET by hand with the client key.
	now := time.Now().UTC()
	scope := sigv4.Scope{Date: now.Format(sigv4.DateFormat), Region: "us-east-1", Service: "s3"}
	q := "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" + clientAK + "%2F" + scope.String() + "&X-Amz-Date=" + now.Format(sigv4.TimeFormat) + "&X-Amz-Expires=60&X-Amz-SignedHeaders=host&versionId=7"
	q = strings.ReplaceAll(q, "/", "%2F")
	q = strings.Replace(q, "X-Amz-Credential="+clientAK+"%2F", "X-Amz-Credential="+clientAK+"%2F", 1)
	target := r.front.URL + "/b/k%20ey?" + q
	req, _ := http.NewRequest(http.MethodGet, target, nil)
	host := strings.TrimPrefix(r.front.URL, "http://")
	canon := sigv4.CanonicalRequest("GET", "/b/k%20ey", sigv4.RawQuery(req), req.Header, host, 0, []string{"host"}, sigv4.UnsignedPayload)
	sig := sigv4.Signature(sigv4.SigningKey(clientSecret, scope), sigv4.StringToSign(now, scope, canon))
	req, _ = http.NewRequest(http.MethodGet, target+"&X-Amz-Signature="+sig, nil)
	resp, b := do(t, req)
	if resp.StatusCode != 200 || string(b) != "data" {
		t.Fatalf("%d %s\nlog=%s", resp.StatusCode, b, r.log.String())
	}
	if rec.verified != nil || strings.Contains(rec.uri, "X-Amz-") || !strings.Contains(rec.uri, "versionId=7") || rec.header.Get("Authorization") == "" {
		t.Fatalf("upstream: verified=%v uri=%s auth=%q", rec.verified, rec.uri, rec.header.Get("Authorization"))
	}
	// Expired.
	old := strings.Replace(target, "X-Amz-Expires=60", "X-Amz-Expires=1", 1)
	req2, _ := http.NewRequest(http.MethodGet, old+"&X-Amz-Signature="+sig, nil)
	time.Sleep(1100 * time.Millisecond)
	resp2, b2 := do(t, req2)
	if resp2.StatusCode != 403 || !strings.Contains(string(b2), "expired") {
		t.Fatalf("expired presign: %d %s", resp2.StatusCode, b2)
	}
}

func TestResignRejectsBeforeBodyOn100Continue(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{true, true})
	var reads int
	inner := r.h
	r.front.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.Body = countingBody2{req.Body, &reads}
		inner.ServeHTTP(w, req)
	})
	c := rawDial(t, r.front)
	req, _ := http.NewRequest(http.MethodPut, "http://h/b/huge", nil)
	req.Host = "h"
	req.ContentLength = 5 << 30
	req.Body = http.NoBody
	sigv4.Sign(req, sigv4.Credentials{AccessKey: clientAK, Secret: "WRONG"}, "us-east-1", sigv4.UnsignedPayload, time.Now())
	fmt.Fprintf(c, "PUT /b/huge HTTP/1.1\r\nHost: h\r\nContent-Length: 5368709120\r\nExpect: 100-continue\r\nAuthorization: %s\r\nX-Amz-Date: %s\r\nX-Amz-Content-Sha256: UNSIGNED-PAYLOAD\r\n\r\n",
		req.Header.Get("Authorization"), req.Header.Get("X-Amz-Date"))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 403 || reads != 0 || rec.method != "" {
		t.Fatalf("status %d reads %d upstream touched=%v", resp.StatusCode, reads, rec.method != "")
	}
}

type countingBody2 struct {
	io.ReadCloser
	n *int
}

func (c countingBody2) Read(p []byte) (int, error) { *c.n++; return c.ReadCloser.Read(p) }

func TestResignErrorsCountedAndSecretsNeverLeak(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, "ok"), Capabilities{EnforcesSHA256: false, UnsignedTrailer: false})

	// Exercise success and every failure path, including a recovered panic (amendment 6).
	panicked := false
	inner := r.h
	r.front.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/b/panic" && !panicked {
			panicked = true
			defer func() {
				if p := recover(); p != nil {
					http.Error(w, fmt.Sprint("recovered: ", p), 500)
				}
			}()
			cred, _ := r.h.Store.Lookup(req.Context(), clientAK)
			panic(fmt.Sprintf("boom %v %+v %#v", cred, cred, cred))
		}
		inner.ServeHTTP(w, req)
	})
	send := func(mutate func(*http.Request)) {
		body := []byte("x")
		req, _ := http.NewRequest(http.MethodPut, r.front.URL+"/b/k", bytes.NewReader(body))
		req.ContentLength = 1
		clientSign(req, strings.Repeat("0", 64)) // wrong sha256 → compensation path logs
		mutate(req)
		resp, _ := do(t, req)
		resp.Body.Close() //nolint:errcheck // test
	}
	send(func(*http.Request) {})
	send(func(q *http.Request) {
		a := q.Header.Get("Authorization")
		q.Header.Set("Authorization", a[:len(a)-4]+"0000") // wrong but well-formed signature
	})
	send(func(q *http.Request) { q.Header.Del("Authorization") })
	send(func(q *http.Request) { q.Header.Set("X-Amz-Security-Token", clientSecret) })
	send(func(q *http.Request) { q.URL.Path = "/b/panic" })
	req, _ := signedChunkRequest(t, r.front.URL+"/b/k", bytes.Repeat([]byte("p"), 3000), "x-amz-checksum-crc32")
	resp, _ := do(t, req)
	resp.Body.Close() //nolint:errcheck // test
	r.backend.Close() // upstream down path
	send(func(*http.Request) {})

	outputs := map[string]string{"access log": r.log.String(), "alert log": r.errLog.String()}
	snap, _ := json.Marshal(r.h.Slow.Snapshot())
	outputs["slow ring"] = string(snap)
	mrec := httptest.NewRecorder()
	fams, _ := r.h.Metrics.Registry.Gather()
	mb, _ := json.Marshal(fams)
	outputs["metrics"] = string(mb)
	_ = mrec
	for name, out := range outputs {
		for _, secret := range []string{clientSecret, clusterSecret} {
			if strings.Contains(out, secret) {
				t.Errorf("secret leaked into %s", name)
			}
		}
	}
	if !panicked {
		t.Fatal("panic path not exercised")
	}
	for _, reason := range []string{"signature", "missing", "token"} {
		if metric(t, r.h.Metrics, "shunt_auth_failures_total", `reason="`+reason+`"`) < 1 {
			t.Errorf("reason %s not counted; access log:\n%s", reason, r.log.String())
		}
	}
}

// metric reads a counter value from the registry by name and label pair string.
func metric(t *testing.T, m *telemetry.Metrics, name, labels string) float64 {
	t.Helper()
	fams, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, mt := range f.GetMetric() {
			var parts []string
			for _, l := range mt.GetLabel() {
				parts = append(parts, l.GetName()+`="`+l.GetValue()+`"`)
			}
			if strings.Join(parts, ",") == labels {
				return mt.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func BenchmarkResignSmallGET(b *testing.B) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "4096")
		_, _ = w.Write(make([]byte, 4096))
	}))
	defer be.Close()
	cl, _ := upstream.New("b", config.Cluster{Scheme: "http", Region: "r", Endpoints: []string{strings.TrimPrefix(be.URL, "http://")}}, upstream.Options{})
	h := New(Handler{Cluster: cl, Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil), Slow: telemetry.NewSlowRing(100, time.Second),
		IdleTimeout: time.Second, MetadataTimeout: time.Second, Mode: ModeResign,
		Store: mapStore{clientAK: {AccessKey: clientAK, Secret: clientSecret, Tenant: "t"}}, ClusterCreds: sigv4.Credentials{AccessKey: clusterAK, Secret: clusterSecret},
		Capabilities: Capabilities{true, true}}, 256<<10)
	req := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	req.RequestURI = "/b/k"
	clientSign(req, sigv4.UnsignedPayload)
	b.ReportAllocs()
	for b.Loop() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			b.Fatal(rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkResignSignedChunkPUT1MiB(b *testing.B) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
	}))
	defer be.Close()
	cl, _ := upstream.New("b", config.Cluster{Scheme: "http", Region: "r", Endpoints: []string{strings.TrimPrefix(be.URL, "http://")}}, upstream.Options{})
	h := New(Handler{Cluster: cl, Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil), Slow: telemetry.NewSlowRing(100, time.Second),
		IdleTimeout: time.Second, MetadataTimeout: time.Second, Mode: ModeResign,
		Store: mapStore{clientAK: {AccessKey: clientAK, Secret: clientSecret, Tenant: "t"}}, ClusterCreds: sigv4.Credentials{AccessKey: clusterAK, Secret: clusterSecret},
		Capabilities: Capabilities{true, true}}, 256<<10)
	payload := make([]byte, 1<<20)
	tt := &testing.T{}
	req, wire := signedChunkRequest(tt, "http://h/b/k", payload, "")
	req.RequestURI = "/b/k"
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		req.Body = io.NopCloser(bytes.NewReader(wire))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			b.Fatal(rec.Code, rec.Body.String())
		}
	}
}
