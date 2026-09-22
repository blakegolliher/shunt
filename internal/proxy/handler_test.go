package proxy

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// syncBuf is a bytes.Buffer safe for the handler goroutine to write while a test reads it.
type syncBuf struct {
	mu   sync.Mutex
	b    bytes.Buffer
	drop bool
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drop {
		return len(p), nil
	}
	return s.b.Write(p)
}

// discard makes the buffer drop everything written to it from now on.
func (s *syncBuf) discard()       { s.mu.Lock(); defer s.mu.Unlock(); s.drop = true }
func (s *syncBuf) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// rig wires a handler in front of backend and serves it on a plain httptest server.
type rig struct {
	backend *httptest.Server
	front   *httptest.Server
	h       *Handler
	log     *syncBuf
}

// waitLog waits for the access log to contain substr: the handler finishes its record after the
// client has already seen the response (or the abort).
func (r *rig) waitLog(t *testing.T, substr string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if s := r.log.String(); strings.Contains(s, substr) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("access log never contained %q:\n%s", substr, r.log.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newRig(t *testing.T, backend http.Handler, idle time.Duration) *rig {
	t.Helper()
	be := httptest.NewServer(backend)
	t.Cleanup(be.Close)
	cl, err := upstream.New("test", config.Cluster{Scheme: "http", Endpoints: []string{strings.TrimPrefix(be.URL, "http://")}}, upstream.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	log := &syncBuf{}
	h := New(Handler{
		Cluster: cl, Domains: s3.NewDomains([]string{"*.shunt.example.com"}),
		Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(log),
		Slow: telemetry.NewSlowRing(10, 0), IdleTimeout: idle, MetadataTimeout: 5 * time.Second,
		Via: "1.1 shunt/test",
	}, 64<<10)
	fr := httptest.NewServer(h)
	fr.Config.ErrorLog = nil
	t.Cleanup(fr.Close)
	return &rig{backend: be, front: fr, h: h, log: log}
}

func TestRoundTripPreservesEverything(t *testing.T) {
	var seen http.Header
	var seenHost, seenURI string
	var seenBody []byte
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, seenHost, seenURI = r.Header.Clone(), r.Host, r.RequestURI
		seenBody, _ = io.ReadAll(r.Body)
		w.Header().Set("X-Amz-Request-Id", "UP123")
		w.Header().Set("ETag", `"abc"`)
		w.Header().Set("Connection", "close")
		w.WriteHeader(201)
		_, _ = w.Write([]byte("created"))
	}), time.Second)

	body := strings.NewReader("hello body")
	req, _ := http.NewRequest(http.MethodPut, r.front.URL+"/b/dir/k%20ey?partNumber=1&uploadId=u", body)
	req.Host = "shunt.example.com:8443"
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=x")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("User-Agent", "") // a client that sends no UA; the proxy must not invent one
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 201 || string(out) != "created" {
		t.Fatalf("status %d body %q", resp.StatusCode, out)
	}
	if seenHost != "shunt.example.com:8443" {
		t.Errorf("host not preserved: %q", seenHost)
	}
	if seenURI != "/b/dir/k%20ey?partNumber=1&uploadId=u" {
		t.Errorf("raw path/query not preserved: %q", seenURI)
	}
	if string(seenBody) != "hello body" {
		t.Errorf("body: %q", seenBody)
	}
	if seen.Get("Authorization") == "" || seen.Get("X-Amz-Content-Sha256") != "UNSIGNED-PAYLOAD" {
		t.Errorf("signed headers not forwarded: %v", seen)
	}
	if seen.Get("Keep-Alive") != "" || seen.Get("Accept-Encoding") != "" {
		t.Errorf("hop-by-hop or transport headers leaked: %v", seen)
	}
	if seen.Get("Via") != "1.1 shunt/test" || seen.Get(telemetry.HeaderRequestID) == "" {
		t.Errorf("Via/request id missing upstream: %v", seen)
	}
	if _, ok := seen["User-Agent"]; ok && seen.Get("User-Agent") != "" {
		t.Errorf("unexpected upstream User-Agent %q", seen.Get("User-Agent"))
	}
	if resp.Header.Get("X-Amz-Request-Id") != "UP123" || resp.Header.Get("ETag") != `"abc"` {
		t.Errorf("response headers not passed back: %v", resp.Header)
	}
	if resp.Header.Get(telemetry.HeaderRequestID) == "" || resp.Header.Get("Via") != "1.1 shunt/test" {
		t.Errorf("response ids missing: %v", resp.Header)
	}
	if l := r.waitLog(t, `"op":"UploadPart"`); !strings.Contains(l, `"upstream_request_id":"UP123"`) || strings.Contains(l, "k ey") {
		t.Errorf("access log: %s", l)
	}
}

func TestLargeBodyBothWays(t *testing.T) {
	const n = 3<<20 + 12345
	payload := bytes.Repeat([]byte("0123456789abcdef"), n/16+1)[:n]
	sum := sha256.Sum256(payload)
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		if sha256.Sum256(got) != sum {
			http.Error(w, "corrupt upload", 400)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		_, _ = w.Write(payload)
	}), time.Second)
	resp, err := fresh().Post(r.front.URL+"/b/k", "application/octet-stream", bytes.NewReader(payload)) //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || sha256.Sum256(got) != sum {
		t.Fatalf("status %d len %d", resp.StatusCode, len(got))
	}
}

// A backend response without Content-Type must reach the client without one: net/http would
// otherwise sniff "text/xml; charset=utf-8" onto XML bodies (found by s3diff against Garage).
func TestNoContentTypeIsNotInvented(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header()["Content-Type"] = nil // the backend sends none
		_, _ = w.Write([]byte(`<?xml version="1.0"?><InitiateMultipartUploadResult/>`))
	}), time.Second)
	req, _ := http.NewRequest(http.MethodPost, r.front.URL+"/b/k?uploads", nil)
	resp, err := fresh().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	if v, ok := resp.Header["Content-Type"]; ok {
		t.Fatalf("proxy invented Content-Type %v", v)
	}
	r2 := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte("<xml/>"))
	}), time.Second)
	resp2, err := fresh().Get(r2.front.URL + "/b/k") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close() //nolint:errcheck // test
	if ct := resp2.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("backend Content-Type not preserved: %q", ct)
	}
}

func TestUnknownOpIsProxied(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(418) }), time.Second)
	req, _ := http.NewRequest("PATCH", r.front.URL+"/b?frobnicate", nil)
	resp, err := fresh().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != 418 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	r.waitLog(t, `"op":"Unknown"`)
}

// fresh returns a client with its own connection pool so tests never share a keep-alive
// connection that a previous test's proxy aborted.
func fresh() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DisableCompression: true}}
}

// rawDial opens a raw TCP connection to srv for hand-written HTTP.
func rawDial(t *testing.T, srv *httptest.Server) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// The gate: an unauthorized 5 GiB PUT with Expect: 100-continue must fail before the client
// sends a body byte, and fast.
func TestExpectContinueRejectedBeforeBody(t *testing.T) {
	var bodyReads atomic.Int64
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A backend verifying the signature answers from headers alone; it never reads the body.
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<Error><Code>SignatureDoesNotMatch</Code></Error>"))
	}), time.Second)
	// Count reads of the client body by wrapping the handler.
	inner := r.h
	r.front.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.Body = countingBody{req.Body, &bodyReads}
		inner.ServeHTTP(w, req)
	})

	c := rawDial(t, r.front)
	start := time.Now()
	fmt.Fprintf(c, "PUT /b/huge HTTP/1.1\r\nHost: shunt.example.com\r\nContent-Length: 5368709120\r\nExpect: 100-continue\r\nAuthorization: AWS4-HMAC-SHA256 bad\r\n\r\n")
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "SignatureDoesNotMatch") {
		t.Fatalf("body %q", body)
	}
	if bodyReads.Load() != 0 {
		t.Fatalf("client body was read %d times before the 403", bodyReads.Load())
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("403 took %v", elapsed)
	}
}

type countingBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (c countingBody) Read(p []byte) (int, error) { c.n.Add(1); return c.ReadCloser.Read(p) }

// The happy path: backend sends 100, client sends the body, gets 200.
func TestExpectContinueAccepted(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Len", fmt.Sprint(len(b)))
		w.WriteHeader(200)
	}), time.Second)
	c := rawDial(t, r.front)
	fmt.Fprintf(c, "PUT /b/k HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "HTTP/1.1 100") {
		t.Fatalf("expected 100 Continue, got %q %v", line, err)
	}
	for { // skip the empty line(s) after the 100
		l, _ := br.ReadString('\n')
		if l == "\r\n" {
			break
		}
	}
	fmt.Fprint(c, "hello")
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || resp.Header.Get("X-Len") != "5" {
		t.Fatalf("status %d x-len %s", resp.StatusCode, resp.Header.Get("X-Len"))
	}
}

// hijackAfter writes k bytes with the given framing, then kills the connection.
func hijackAfter(k int, contentLength bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if contentLength {
			w.Header().Set("Content-Length", "1000000")
		}
		w.WriteHeader(200)
		_, _ = w.Write(bytes.Repeat([]byte("x"), k))
		http.NewResponseController(w).Flush() //nolint:errcheck // test
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			conn.Close()
		}
	})
}

// The gate: a backend killed mid-GET never yields a silent short read.
func TestBackendDiesMidGetContentLength(t *testing.T) {
	r := newRig(t, hijackAfter(70000, true), time.Second)
	resp, err := fresh().Get(r.front.URL + "/b/k") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	got, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("silent short read: got %d bytes and clean EOF", len(got))
	}
	r.waitLog(t, `"error":"body:`)
}

func TestBackendDiesMidGetChunked(t *testing.T) {
	r := newRig(t, hijackAfter(70000, false), time.Second)
	c := rawDial(t, r.front)
	fmt.Fprintf(c, "GET /b/k HTTP/1.1\r\nHost: h\r\n\r\n")
	raw, _ := io.ReadAll(c) // until the proxy closes the connection
	if !bytes.Contains(raw, []byte("Transfer-Encoding: chunked")) {
		t.Fatalf("expected a chunked response, got:\n%s", raw[:min(400, len(raw))])
	}
	if bytes.HasSuffix(raw, []byte("0\r\n\r\n")) {
		t.Fatal("silent short read: chunked body was terminated cleanly")
	}
}

func TestIdleTimeoutStalledBackend(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000000")
		w.WriteHeader(200)
		_, _ = w.Write(bytes.Repeat([]byte("p"), 70000)) // beyond net/http's buffers, so headers reach the client
		http.NewResponseController(w).Flush()            //nolint:errcheck // test
		<-r.Context().Done()                             // stall until the proxy gives up
	}), 300*time.Millisecond)
	start := time.Now()
	resp, err := fresh().Get(r.front.URL + "/b/k") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	_, err = io.ReadAll(resp.Body)
	if err == nil {
		t.Fatal("expected an error from the stalled backend")
	}
	if d := time.Since(start); d < 250*time.Millisecond || d > 2*time.Second {
		t.Fatalf("idle timeout fired after %v", d)
	}
}

func TestIdleTimeoutNotFiredWhileProgressing(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(200)
		for i := 0; i < 6; i++ { // 6 bytes over ~1.2 s with a 300 ms idle limit
			_, _ = w.Write([]byte("x"))
			http.NewResponseController(w).Flush() //nolint:errcheck // test
			time.Sleep(200 * time.Millisecond)
		}
	}), 300*time.Millisecond)
	resp, err := fresh().Get(r.front.URL + "/b/k") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "xxxxxx" {
		t.Fatalf("slow-but-progressing transfer failed: %q %v", got, err)
	}
}

func TestMetadataDeadline(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}), time.Second)
	r.h.MetadataTimeout = 200 * time.Millisecond
	resp, err := fresh().Head(r.front.URL + "/b/k") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestUpstreamDown(t *testing.T) {
	r := newRig(t, http.NotFoundHandler(), time.Second)
	r.backend.Close()
	resp, err := fresh().Get(r.front.URL + "/b/k") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(string(body), "<Code>InternalError</Code>") {
		t.Fatalf("status %d body %s", resp.StatusCode, body)
	}
}

func TestMetricsAndSlowRing(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }), time.Second)
	windows := telemetry.NewCollector()
	r.h.Telemetry = windows
	resp, _ := fresh().Get(r.front.URL + "/b/k") //nolint:noctx // test
	resp.Body.Close()                            //nolint:errcheck // test
	fams, _ := r.h.Metrics.Registry.Gather()
	names := map[string]bool{}
	for _, f := range fams {
		names[f.GetName()] = true
	}
	for _, n := range []string{"shunt_requests_total", "shunt_request_duration_seconds", "shunt_upstream_ttfb_seconds", "shunt_bytes_out_total", "shunt_inflight"} {
		if !names[n] {
			t.Errorf("metric %s not observed", n)
		}
	}
	if snap := r.h.Slow.Snapshot(); len(snap) != 1 || snap[0].Op != "GetObject" || snap[0].BytesOut != 2 {
		t.Fatalf("slow ring: %+v", snap)
	}
	if window := windows.Completed(time.Now().Add(telemetry.WindowDuration)); window == nil || len(window.Sketches) != 4 || window.Counters[0].Requests != 1 {
		t.Fatalf("telemetry window: %+v", window)
	}
}

func BenchmarkSmallGET(b *testing.B) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "4096")
		_, _ = w.Write(make([]byte, 4096))
	}))
	defer be.Close()
	cl, _ := upstream.New("b", config.Cluster{Scheme: "http", Endpoints: []string{strings.TrimPrefix(be.URL, "http://")}}, upstream.Options{})
	h := New(Handler{Cluster: cl, Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil), Slow: telemetry.NewSlowRing(100, time.Second), IdleTimeout: time.Second, MetadataTimeout: time.Second}, 256<<10)
	req := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	b.ReportAllocs()
	for b.Loop() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			b.Fatal(rec.Code)
		}
	}
}

func BenchmarkPUT1MiB(b *testing.B) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
	}))
	defer be.Close()
	cl, _ := upstream.New("b", config.Cluster{Scheme: "http", Endpoints: []string{strings.TrimPrefix(be.URL, "http://")}}, upstream.Options{})
	h := New(Handler{Cluster: cl, Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil), Slow: telemetry.NewSlowRing(100, time.Second), IdleTimeout: time.Second, MetadataTimeout: time.Second}, 256<<10)
	payload := make([]byte, 1<<20)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPut, "/b/k", bytes.NewReader(payload))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			b.Fatal(rec.Code)
		}
	}
}
