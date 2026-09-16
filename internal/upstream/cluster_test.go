package upstream

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
)

func TestRoundRobin(t *testing.T) {
	c, err := New("x", config.Cluster{Scheme: "http", Endpoints: []string{"a:1", "b:1", "c:1"}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for i := 0; i < 7; i++ {
		got = append(got, c.Next())
	}
	if want := "a:1 b:1 c:1 a:1 b:1 c:1 a:1"; strings.Join(got, " ") != want {
		t.Fatalf("got %v", got)
	}
}

func TestNoEndpoints(t *testing.T) {
	if _, err := New("x", config.Cluster{Scheme: "http"}, Options{}); err != errNoEndpoints {
		t.Fatalf("want ErrNoEndpoints, got %v", err)
	}
}

func TestDNSModeIsOneEntry(t *testing.T) {
	c, err := New("aws", config.Cluster{Scheme: "https", EndpointMode: "dns", Endpoint: "s3.amazonaws.com"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Next() != "s3.amazonaws.com" || c.Transport.TLSClientConfig == nil {
		t.Fatalf("dns cluster: %+v", c)
	}
}

func TestTransportIsHTTP1AndUncompressed(t *testing.T) {
	var proto, enc string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto, enc = r.Proto, r.Header.Get("Accept-Encoding")
	}))
	defer srv.Close()
	c, err := New("t", config.Cluster{Scheme: "http", Endpoints: []string{strings.TrimPrefix(srv.URL, "http://")}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+c.Next()+"/", nil)
	resp, err := c.Transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close() //nolint:errcheck // test
	if proto != "HTTP/1.1" || enc != "" {
		t.Fatalf("proto=%s accept-encoding=%q", proto, enc)
	}
	c.Close()
}

func TestBadCA(t *testing.T) {
	if _, err := New("x", config.Cluster{Scheme: "https", Endpoints: []string{"a:1"}, TLS: config.ClusterTLS{CA: "/nonexistent.pem"}}, Options{}); err == nil {
		t.Fatal("expected error for missing CA file")
	}
}

func TestClusterIDIsOpaqueAndStable(t *testing.T) {
	a, b := ClusterID("garage"), ClusterID("minio")
	if len(a) != 6 || a == b || ClusterID("garage") != a || strings.Contains(a, "garage") {
		t.Fatalf("ids %q %q", a, b)
	}
	for _, c := range a {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("id %q is not lowercase hex", a)
		}
	}
}

func TestIsConnectError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	c, _ := New("x", config.Cluster{Scheme: "http", Endpoints: []string{addr}}, Options{})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/", nil)
	_, err = c.Transport.RoundTrip(req)
	if !IsConnectError(err) {
		t.Fatalf("refused connection not a connect error: %v", err)
	}
	if IsConnectError(errors.New("other")) || IsConnectError(context.DeadlineExceeded) {
		t.Fatal("non-dial errors reported as connect errors")
	}
}

func BenchmarkNext(b *testing.B) {
	c, _ := New("x", config.Cluster{Scheme: "http", Endpoints: []string{"a:1", "b:1", "c:1"}}, Options{})
	b.ReportAllocs()
	for b.Loop() {
		_ = c.Next()
	}
}
