package verify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/telemetry"
)

func TestCompareTelemetryUsesTheMatchingWindow(t *testing.T) {
	base := time.Now().UTC().Truncate(telemetry.WindowDuration).Add(time.Second)
	c := telemetry.NewCollector()
	for i := 1; i <= 100; i++ {
		c.Observe(telemetry.Observation{At: base, Operation: "GetObject", Cluster: "client", Status: 200,
			ClientTotal: time.Duration(i) * time.Millisecond})
	}
	w := c.Completed(base.Add(telemetry.WindowDuration))
	p99, err := clientWindowP99(w)
	if err != nil {
		t.Fatal(err)
	}
	serverP99 := p99 + p99/20 // 5%, inside the default 10% tolerance
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(rw).Encode(latestTelemetry{Windows: []telemetry.ScopeWindow{
			{Scope: "fleet", Start: w.Start.Add(-telemetry.WindowDuration), Series: []telemetry.Summary{{Series: telemetry.SeriesClientTotal, Op: telemetry.OpAll, P99: 1}}},
			{Scope: "fleet", Start: w.Start, End: w.End, Series: []telemetry.Summary{{Series: telemetry.SeriesClientTotal, Op: telemetry.OpAll, P99: serverP99}}},
		}})
	}))
	defer srv.Close()
	got, err := CompareTelemetry(context.Background(), srv.URL, "token", w, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !got.WithinTolerance || got.ClientP99US != p99 || got.FleetP99US != serverP99 || got.DifferencePct < 4.9 || got.DifferencePct > 5.1 {
		t.Fatalf("comparison: %+v", got)
	}
}
