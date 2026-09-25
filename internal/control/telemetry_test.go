package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/telemetry"
)

func telemetryWindow(t *testing.T, at time.Time, cluster string, latency time.Duration) *telemetry.Window {
	t.Helper()
	c := telemetry.NewCollector()
	c.Observe(telemetry.Observation{At: at, Operation: "GetObject", Cluster: cluster, Status: 200,
		BytesOut: 19, ClientTotal: latency, UpstreamTTFB: latency / 4, UpstreamTotal: latency * 3 / 4})
	w := c.Completed(at.Add(telemetry.WindowDuration))
	if w == nil {
		t.Fatal("no completed window")
	}
	return w
}

func TestTelemetryReadModelsAndEvent(t *testing.T) {
	base := time.Now().UTC().Truncate(telemetry.WindowDuration).Add(time.Second)
	events := NewEvents(16)
	_, ch, _, ok, cancel := events.Subscribe("")
	if !ok {
		t.Fatal("subscribe")
	}
	defer cancel()
	s := &Server{Metrics: telemetry.NewMetrics(), Telemetry: telemetry.NewStore(10 * time.Second), Events: events, Token: "test"}
	if err := s.publishTelemetry([]Member{
		{ID: "p1", Live: true, Telemetry: telemetryWindow(t, base, "source", 10*time.Millisecond)},
		{ID: "p2", Live: true, Telemetry: telemetryWindow(t, base, "target", 20*time.Millisecond)},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.Type != eventTypeTelemetry {
			t.Fatalf("event type = %s", ev.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("no telemetry event")
	}

	h := s.Handler()
	req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/latest", nil)
	req.Header.Set("Authorization", "Bearer test")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("latest: %d %s", rec.Code, rec.Body.String())
	}
	var latest LatestTelemetry
	if err := json.Unmarshal(rec.Body.Bytes(), &latest); err != nil {
		t.Fatal(err)
	}
	wantScopes := map[string]bool{"fleet": true, "cluster:source": true, "cluster:target": true, "proxy:p1": true, "proxy:p2": true}
	for _, w := range latest.Windows {
		delete(wantScopes, w.Scope)
	}
	if len(wantScopes) != 0 {
		t.Fatalf("missing latest scopes: %v", wantScopes)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/telemetry/series?scope=fleet&series=client_total&op=all", nil)
	req.Header.Set("Authorization", "Bearer test")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("series: %d %s", rec.Code, rec.Body.String())
	}
	var series TelemetrySeries
	if err := json.Unmarshal(rec.Body.Bytes(), &series); err != nil {
		t.Fatal(err)
	}
	if len(series.Points) != 1 || series.Points[0].Count != 2 || series.Points[0].P99 < 19_000 {
		t.Fatalf("series = %#v", series)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/telemetry/series?scope=fleet&series=requests_per_second&op=all", nil)
	req.Header.Set("Authorization", "Bearer test")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("request rate: %d %s", rec.Code, rec.Body.String())
	}
	series = TelemetrySeries{}
	if err := json.Unmarshal(rec.Body.Bytes(), &series); err != nil {
		t.Fatal(err)
	}
	if len(series.Points) != 1 || series.Points[0].Value == nil || *series.Points[0].Value != 0.2 {
		t.Fatalf("request rate = %#v", series)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/telemetry/series?scope=nope&series=client_total", nil)
	req.Header.Set("Authorization", "Bearer test")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad scope: %d %s", rec.Code, rec.Body.String())
	}
}

// The API answers a status point with its code and a bucket scope's point with its backend
// cluster: without them the UI cannot tell one line from another.
func TestTelemetrySeriesCarryCodeAndCluster(t *testing.T) {
	base := time.Now().UTC().Truncate(telemetry.WindowDuration).Add(time.Second)
	c := telemetry.NewCollector()
	for _, o := range []telemetry.Observation{
		{At: base, Operation: "PutObject", Cluster: "minio-a", Status: 503, ClientTotal: time.Millisecond},
		{At: base, Operation: "PutObject", Cluster: "minio-b", Status: 200, ClientTotal: time.Millisecond},
	} {
		c.Observe(o)
		c.ObserveBucket(o, "acme/data")
	}
	s := &Server{Metrics: telemetry.NewMetrics(), Telemetry: telemetry.NewStore(10 * time.Second), Token: "test"}
	if err := s.publishTelemetry([]Member{{ID: "p1", Live: true, Telemetry: c.Completed(base.Add(telemetry.WindowDuration))}}); err != nil {
		t.Fatal(err)
	}
	get := func(query string) TelemetrySeries {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/series?"+query, nil)
		req.Header.Set("Authorization", "Bearer test")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", query, rec.Code, rec.Body.String())
		}
		var out TelemetrySeries
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	status := get("scope=fleet&series=status_per_second&op=all")
	if len(status.Points) != 1 || status.Points[0].Code != "503" {
		t.Fatalf("status points: %+v", status.Points)
	}
	byCluster := map[string]bool{}
	for _, p := range get("scope=bucket:acme/data&series=requests_per_second&op=all").Points {
		byCluster[p.Cluster] = true
	}
	if !byCluster["minio-a"] || !byCluster["minio-b"] || len(byCluster) != 2 {
		t.Fatalf("bucket points by cluster: %v", byCluster)
	}
}
