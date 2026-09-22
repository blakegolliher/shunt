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

	req = httptest.NewRequest(http.MethodGet, "/v1/telemetry/series?scope=nope&series=client_total", nil)
	req.Header.Set("Authorization", "Bearer test")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad scope: %d %s", rec.Code, rec.Body.String())
	}
}
