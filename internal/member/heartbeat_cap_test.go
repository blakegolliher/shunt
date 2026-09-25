package member

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// Found by the H2 benchmarks (2026-09-25): the heartbeat's size check ran before the moving
// buckets' fallback-read counts were added, so a heartbeat under the cap by that check could reach
// the control plane over its decoder's limit and be refused whole, lease and drain proof with it.
// The counts are cutover's evidence and are never dropped; the check now includes them, and
// telemetry is what goes.
func TestHeartbeatCapCountsFallbackReads(t *testing.T) {
	var mu sync.Mutex
	var sizes []int
	var got []control.Heartbeat
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var hb control.Heartbeat
		if err := json.Unmarshal(body, &hb); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		sizes, got = append(sizes, len(body)), append(got, hb)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(control.HeartbeatAnswer{Seq: hb.Seq, Identity: testIdentity, Version: hb.Applied, LeaseTTL: 10 * time.Second})
	}))
	defer srv.Close()
	c := New(Config{Endpoints: []string{srv.URL}, ProxyID: "p1", CacheDir: t.TempDir(), Interval: 5 * time.Second, LeaseTTL: 10 * time.Second},
		slog.New(slog.DiscardHandler))
	c.Gates = admission.New()
	c.Metrics = telemetry.NewMetrics()
	start := time.Now()
	c.Telemetry = telemetry.NewCollector()
	for i := range 40 {
		c.Telemetry.Observe(telemetry.Observation{At: start, Operation: "GetObject", Cluster: fmt.Sprintf("vast%02d", i%4), Status: 200,
			ClientTotal: time.Duration(i+1) * time.Millisecond, UpstreamTotal: time.Duration(i+1) * time.Millisecond / 2, UpstreamTTFB: time.Millisecond})
	}
	c.Now = func() time.Time { return start.Add(3 * telemetry.WindowDuration) }
	cluster := func() config.Cluster {
		return config.Cluster{Type: "s3", Scheme: "http", Region: "r", EndpointMode: "static", Endpoints: []string{"127.0.0.1:1"},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:x"}}
	}
	d := control.Directory{File: directory.File{Version: 1, Identity: testIdentity,
		Clusters: map[string]config.Cluster{"vast01": cluster(), "vast02": cluster()},
		Tenants:  map[string]directory.Tenant{"acme": {DefaultCluster: "vast01"}}, Placements: map[string]directory.Placement{}},
		Secrets: map[string]string{"control:x": "s1"}}
	for i := range 60 {
		name := fmt.Sprintf("moving-bucket-%03d", i)
		d.Placements["acme/"+name] = directory.Placement{State: directory.StateMigrating, Primary: "vast02", Source: "vast01",
			Names: map[string]string{"vast01": name, "vast02": name}}
	}
	if _, err := c.install(&d, false); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.beat(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	full, first := sizes[0], got[0]
	mu.Unlock()
	if first.Telemetry == nil || len(first.FallbackReads) != 60 {
		t.Fatalf("the uncapped heartbeat: telemetry %v, %d fallback counts", first.Telemetry != nil, len(first.FallbackReads))
	}
	counts, _ := json.Marshal(first.FallbackReads)
	window, _ := json.Marshal(first.Telemetry)
	if len(window) <= len(counts) {
		t.Fatalf("the test needs a window (%d bytes) larger than the counts (%d bytes)", len(window), len(counts))
	}
	// A cap the heartbeat passes only with its fallback counts: a check that leaves them out keeps
	// the window and sends too much.
	c.heartbeatCap = full - len(counts)/2
	if err := c.beat(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	size, last := sizes[len(sizes)-1], got[len(got)-1]
	mu.Unlock()
	if size > c.heartbeatCap || last.Telemetry != nil || len(last.FallbackReads) != 60 {
		t.Fatalf("under a cap of %d bytes: sent %d bytes, telemetry kept %v, %d fallback counts", c.heartbeatCap, size, last.Telemetry != nil, len(last.FallbackReads))
	}
}
