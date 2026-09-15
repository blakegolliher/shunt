package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/telemetry"
)

func newSrv(t *testing.T) (*Server, *httptest.Server, *telemetry.Metrics) {
	t.Helper()
	m := telemetry.NewMetrics()
	slow := telemetry.NewSlowRing(5, 0)
	slow.Record(&telemetry.SlowEntry{RequestID: "r1", Op: "GetObject", Total: time.Second})
	s := New(m.Registry, slow)
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	return s, ts, m
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx,gosec // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestEndpoints(t *testing.T) {
	s, ts, m := newSrv(t)
	m.RequestsTotal.WithLabelValues("GetObject", "2xx", "garage", "s3").Inc()
	if code, body := get(t, ts.URL+"/-/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("healthz %d %q", code, body)
	}
	if code, body := get(t, ts.URL+"/-/metrics"); code != 200 || !strings.Contains(body, `shunt_requests_total{cluster="garage",cluster_type="s3",op="GetObject",status_class="2xx"} 1`) || !strings.Contains(body, "process_cpu_seconds_total") {
		t.Fatalf("metrics %d\n%s", code, body[:min(600, len(body))])
	}
	code, body := get(t, ts.URL+"/-/slow")
	var out struct {
		Threshold string
		Entries   []telemetry.SlowEntry
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil || code != 200 || len(out.Entries) != 1 || out.Entries[0].RequestID != "r1" {
		t.Fatalf("slow %d %s %v", code, body, err)
	}
	if code, _ := get(t, ts.URL+"/debug/pprof/"); code != 200 {
		t.Fatalf("pprof %d", code)
	}
	resp, err := http.Post(ts.URL+"/-/drain", "", nil) //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != http.StatusAccepted || !s.Draining() {
		t.Fatalf("drain %d draining=%v", resp.StatusCode, s.Draining())
	}
	if code, body := get(t, ts.URL+"/-/healthz"); code != 503 || body != "draining\n" {
		t.Fatalf("healthz after drain %d %q", code, body)
	}
	if code, _ := get(t, ts.URL+"/-/drain"); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /-/drain should be 405, got %d", code)
	}
}

func BenchmarkHealthz(b *testing.B) {
	s := New(telemetry.NewMetrics().Registry, telemetry.NewSlowRing(1, 0))
	req := httptest.NewRequest(http.MethodGet, "/-/healthz", nil)
	b.ReportAllocs()
	for b.Loop() {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
	}
}
