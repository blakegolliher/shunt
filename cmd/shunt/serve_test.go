package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
)

func selfSigned(t *testing.T, dir string) (cert, key string) {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "*.shunt.example.com"},
		DNSNames: []string{"*.shunt.example.com", "shunt.example.com"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	kb, _ := x509.MarshalECPrivateKey(k)
	cert, key = filepath.Join(dir, "c.crt"), filepath.Join(dir, "c.key")
	_ = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
	return cert, key
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// TestServeEndToEnd starts serve against an httptest backend, proxies one request over TLS,
// checks the admin endpoints, then cancels the context and expects a clean drain.
func TestServeEndToEnd(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Request-Id", "UP1")
		_, _ = io.WriteString(w, "host="+r.Host)
	}))
	defer backend.Close()
	dir := t.TempDir()
	cert, key := selfSigned(t, dir)
	listen, adminAddr := freePort(t), freePort(t)
	cfg, err := config.Parse([]byte(`
listener: { address: "` + listen + `", domains: ["*.shunt.example.com"], tls: { cert: ` + cert + `, key: ` + key + ` } }
admin: { address: "` + adminAddr + `" }
auth: { mode: passthrough }
proxy: { cluster: be, drain_timeout: 2s }
clusters:
  be: { type: s3, scheme: http, region: r, endpoints: ["` + strings.TrimPrefix(backend.URL, "http://") + `"], credentials: { access_key: a, secret_ref: env:S } }
telemetry: { access_log: { enabled: false } }
`))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, io.Discard) }()

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed test cert
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, listen) // any *.shunt.example.com → the proxy
		},
	}}
	var resp *http.Response
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err = client.Get("https://bkt.shunt.example.com/key") //nolint:noctx // test
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != 200 || string(body) != "host=bkt.shunt.example.com" || resp.Header.Get("X-Amz-Request-Id") != "UP1" || resp.Header.Get("X-Shunt-Request-Id") == "" {
		t.Fatalf("proxied response: %d %q %v", resp.StatusCode, body, resp.Header)
	}
	if resp.TLS == nil || resp.TLS.Version < tls.VersionTLS12 { // ALPN is covered in internal/listener; this client offers none
		t.Fatalf("tls: %+v", resp.TLS)
	}

	h, err := http.Get("http://" + adminAddr + "/-/healthz") //nolint:noctx // test
	if err != nil || h.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", err, h)
	}
	h.Body.Close()                                           //nolint:errcheck // test
	m, err := http.Get("http://" + adminAddr + "/-/metrics") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	mb, _ := io.ReadAll(m.Body)
	m.Body.Close() //nolint:errcheck // test
	if !strings.Contains(string(mb), `shunt_requests_total{cluster="be",cluster_type="s3",op="GetObject",status_class="2xx"} 1`) {
		t.Fatalf("metrics missing the proxied request:\n%s", mb[:min(800, len(mb))])
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not drain")
	}
	if h, err := http.Get("http://" + adminAddr + "/-/healthz"); err == nil { //nolint:noctx // test
		h.Body.Close() //nolint:errcheck // test
		t.Fatal("admin still up after drain")
	}
}

// A lab listener on plaintext http: requests are served without TLS, and every line serve logs
// carries the plaintext marker so it cannot be missed in a startup banner.
// runPlaintextServe serves one plaintext request with the given log format and returns the log.
func runPlaintextServe(t *testing.T, format string) string {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()
	listen, adminAddr := freePort(t), freePort(t)
	cfg, err := config.Parse([]byte(`
listener: { address: "` + listen + `", plaintext: true }
admin: { address: "` + adminAddr + `" }
auth: { mode: passthrough }
proxy: { cluster: be, drain_timeout: 2s }
clusters:
  be: { type: s3, scheme: http, region: r, endpoints: ["` + strings.TrimPrefix(backend.URL, "http://") + `"], credentials: { access_key: a, secret_ref: env:S } }
telemetry: { log_format: ` + format + `, access_log: { enabled: false } }
`))
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, logs) }()
	var resp *http.Response
	for deadline := time.Now().Add(5 * time.Second); ; {
		resp, err = http.Get("http://" + listen + "/bkt/key") //nolint:noctx // test
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != 200 || resp.TLS != nil {
		t.Fatalf("plaintext request: %d tls=%v", resp.StatusCode, resp.TLS)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return logs.String()
}

func TestServePlaintextListenerMarksEveryLine(t *testing.T) {
	out := runPlaintextServe(t, "json")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 3 {
		t.Fatalf("too few log lines:\n%s", out)
	}
	for _, line := range lines {
		if !strings.Contains(line, `"client_listener":"PLAINTEXT http`) {
			t.Errorf("startup line without the plaintext marker: %s", line)
		}
	}
	if !strings.Contains(out, `"level":"WARN","msg":"the client listener is PLAINTEXT http`) {
		t.Errorf("no plaintext warning:\n%s", out)
	}
}

// The console format is one short line per event, and a plaintext listener tags every one.
func TestServeConsoleLogFormat(t *testing.T) {
	out := runPlaintextServe(t, "console")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 3 {
		t.Fatalf("too few log lines:\n%s", out)
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "{") || !strings.Contains(line, " [PLAINTEXT] ") {
			t.Errorf("not a tagged console line: %s", line)
		}
	}
	if !strings.Contains(out, "WARN  [PLAINTEXT] the client listener is PLAINTEXT http") || !strings.Contains(out, "INFO  [PLAINTEXT] shunt serving  version=") {
		t.Errorf("console lines missing the warning or the banner:\n%s", out)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
