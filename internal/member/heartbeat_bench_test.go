package member

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// A member's heartbeat at T19's scale (ADR-0021 D2): its size, safety fields against telemetry,
// its encode and decode, the size cap, and one whole heartbeat against a large directory.

// heartbeatWindow is a telemetry window as a busy proxy sends it: clusters × six operation
// classes × four latency series of compressed sketches (10,000 samples spread from 50 µs to about
// 2 s), a counter per class with a few status keys, and the full per-bucket block.
func heartbeatWindow(clusters int) *telemetry.Window {
	h := hdrhistogram.New(1, int64(120*time.Second/time.Microsecond), 3)
	rnd := rand.New(rand.NewPCG(420032, 1)) //nolint:gosec // deterministic value spread
	for range 10_000 {
		_ = h.RecordValue(int64(50 + math.Exp(rnd.Float64()*math.Log(40_000))))
	}
	data, err := h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		panic(err)
	}
	start := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	w := &telemetry.Window{Start: start, End: start.Add(telemetry.WindowDuration)}
	ops := []telemetry.OpClass{telemetry.OpRead, telemetry.OpWrite, telemetry.OpList, telemetry.OpDelete, telemetry.OpMultipart, telemetry.OpOther}
	series := []string{telemetry.SeriesClientTotal, telemetry.SeriesUpstreamTTFB, telemetry.SeriesUpstreamTotal, telemetry.SeriesProxyOverhead}
	errs := map[string]int64{"404": 3, "not_found": 120, "503": 1}
	for c := range clusters {
		name := fmt.Sprintf("cluster-%02d", c)
		for _, op := range ops {
			w.Counters = append(w.Counters, telemetry.WindowCounter{Op: op, Cluster: name, Requests: 10_000, BytesIn: 1 << 30, BytesOut: 2 << 30, Errors: errs})
			for _, s := range series {
				w.Sketches = append(w.Sketches, telemetry.Sketch{Series: s, Op: op, Cluster: name, Data: data})
			}
		}
	}
	for i := range telemetry.MaxBucketsPerWindow {
		w.Buckets = append(w.Buckets, telemetry.BucketCounter{Bucket: fmt.Sprintf("tenant-%02d/bucket-%02d", i, i), Cluster: "cluster-00",
			Requests: 1000, BytesIn: 1 << 20, BytesOut: 1 << 20})
	}
	return w
}

// heartbeatAcks is a drain proof for n barriers, as Gates.Acks reports them: operation ids,
// placement scopes, a few requests still out.
func heartbeatAcks(n int) []admission.Ack {
	acks := make([]admission.Ack, n)
	for i := range acks {
		acks[i] = admission.Ack{ID: fmt.Sprintf("1758800000000-%06x", i), Scope: directory.PlacementResource(fmt.Sprintf("tenant-%03d/bucket-%06d", i%1000, i)),
			Generation: int64(1000 + i), Kind: config.BarrierMutations, Closed: true, Inflight: int64(i % 3)}
	}
	return acks
}

// benchHeartbeat is one member heartbeat with acks acknowledgements and window (nil: none).
func benchHeartbeat(acks int, window *telemetry.Window) control.Heartbeat {
	return control.Heartbeat{Protocol: control.Protocol, Identity: testIdentity, Started: time.Date(2026, 9, 25, 11, 0, 0, 0, time.UTC),
		Incarnation: "0123456789abcdef0123456789abcdef", Seq: 4242, Applied: 1234, Durable: 1234, Host: "proxy-0001.example", Version: "v0.0.0-bench",
		Secrets: map[string]string{"cluster-00": "3", "cluster-01": "5"}, SecretsHeld: map[string]int64{"cluster-00": 0},
		FallbackReads: map[string]float64{"tenant-001/moving": 12}, Barriers: heartbeatAcks(acks), Telemetry: window}
}

func encoded(b *testing.B, hb *control.Heartbeat) []byte {
	b.Helper()
	raw, err := json.Marshal(hb)
	if err != nil {
		b.Fatal(err)
	}
	return raw
}

// reportSplit records a heartbeat's size and what of it is safety metadata (everything but the
// telemetry window), and bytes per acknowledgement: the heartbeat with its acks against one
// without.
func reportSplit(b *testing.B, hb control.Heartbeat) {
	b.Helper()
	full := len(encoded(b, &hb))
	safetyHB := hb
	safetyHB.Telemetry = nil
	safety := len(encoded(b, &safetyHB))
	noAcks := safetyHB
	noAcks.Barriers = nil
	b.ReportMetric(float64(full), "bytes/heartbeat")
	b.ReportMetric(float64(safety), "safety-bytes")
	b.ReportMetric(float64(full-safety), "telemetry-bytes")
	if n := len(hb.Barriers); n > 0 {
		b.ReportMetric(float64(safety-len(encoded(b, &noAcks)))/float64(n), "bytes/ack")
	}
}

// BenchmarkHeartbeatMarshal is a member encoding one heartbeat with 100 barrier
// acknowledgements and two clusters' telemetry: the encode in Client.call. The metrics say how
// large it is and how it splits between safety metadata and telemetry.
func BenchmarkHeartbeatMarshal(b *testing.B) {
	hb := benchHeartbeat(100, heartbeatWindow(2))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(&hb); err != nil {
			b.Fatal(err)
		}
	}
	reportSplit(b, hb)
}

// BenchmarkHeartbeatUnmarshal is the control node parsing the same heartbeat as its handler
// does (unknown fields refused): the parse cost of every heartbeat, 1,000 a second at 1,000
// proxies on a 1-second interval.
func BenchmarkHeartbeatUnmarshal(b *testing.B) {
	hb := benchHeartbeat(100, heartbeatWindow(2))
	raw := encoded(b, &hb)
	b.ReportAllocs()
	for b.Loop() {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		var out control.Heartbeat
		if err := dec.Decode(&out); err != nil || len(out.Barriers) != 100 {
			b.Fatalf("decode: %v", err)
		}
	}
	reportSplit(b, hb)
}

// BenchmarkHeartbeatSafetyAtMaxAcks encodes a heartbeat carrying control.MaxBarrierAcks
// acknowledgements and no telemetry: the largest safety payload a member may send. It must fit
// under maxHeartbeatBytes with room to spare, since telemetry is all the cap can drop.
func BenchmarkHeartbeatSafetyAtMaxAcks(b *testing.B) {
	hb := benchHeartbeat(control.MaxBarrierAcks, nil)
	if n := len(encoded(b, &hb)); n >= maxHeartbeatBytes {
		b.Fatalf("a heartbeat of %d acknowledgements is %d bytes, over the %d cap", control.MaxBarrierAcks, n, maxHeartbeatBytes)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(&hb); err != nil {
			b.Fatal(err)
		}
	}
	reportSplit(b, hb)
}

// atBound is a heartbeat with 100 acknowledgements and the densest telemetry window that stays
// under maxHeartbeatBytes (kept), and the next denser one (over).
func atBound(b *testing.B) (kept, over control.Heartbeat) {
	b.Helper()
	for clusters := 1; clusters < 64; clusters++ {
		hb := benchHeartbeat(100, heartbeatWindow(clusters))
		if len(encoded(b, &hb)) > maxHeartbeatBytes {
			if clusters == 1 {
				b.Fatal("one cluster's window is over the cap")
			}
			return benchHeartbeat(100, heartbeatWindow(clusters-1)), hb
		}
	}
	b.Fatal("no window reaches the cap")
	return
}

// BenchmarkCapTelemetryKept is the size check each heartbeat makes before it is sent
// (capTelemetry: one full encode) on the densest heartbeat that fits: the window is kept, and
// Client.call then encodes it a second time to send it.
func BenchmarkCapTelemetryKept(b *testing.B) {
	hb, _ := atBound(b)
	b.ReportAllocs()
	for b.Loop() {
		if dropped, _ := capTelemetry(&hb, maxHeartbeatBytes); dropped {
			b.Fatal("dropped a window under the cap")
		}
	}
	reportSplit(b, hb)
	b.ReportMetric(float64(len(hb.Telemetry.Sketches)), "sketches")
}

// BenchmarkCapTelemetryDropped is the same check on a heartbeat just over the cap: the window is
// dropped so that the lease and the drain proof get through. Bytes/heartbeat is the size before
// the drop.
func BenchmarkCapTelemetryDropped(b *testing.B) {
	_, hb := atBound(b)
	w := hb.Telemetry
	b.ReportAllocs()
	for b.Loop() {
		hb.Telemetry = w
		if dropped, _ := capTelemetry(&hb, maxHeartbeatBytes); !dropped || len(hb.Barriers) != 100 {
			b.Fatal("kept a window over the cap, or lost acknowledgements")
		}
	}
	hb.Telemetry = w
	reportSplit(b, hb)
}

// BenchmarkBeat is one whole member heartbeat (Client.beat) against a stub control node that
// decodes it and grants a lease: building the heartbeat from the installed snapshot, the drain
// proof for 100 barriers, the size check, the encode, the round trip over loopback, and the
// lease renewal. No telemetry window. The placements sub-benchmarks vary only the size of the
// installed directory, so a difference between them is per-heartbeat work proportional to it.
func BenchmarkBeat(b *testing.B) {
	for _, placements := range []int{1000, 100_000} {
		b.Run(fmt.Sprintf("placements=%d", placements), func(b *testing.B) {
			var bytesIn atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var hb control.Heartbeat
				dec := json.NewDecoder(r.Body)
				if err := dec.Decode(&hb); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				bytesIn.Store(dec.InputOffset())
				_ = json.NewEncoder(w).Encode(control.HeartbeatAnswer{Seq: hb.Seq, Identity: testIdentity, Version: hb.Applied, LeaseTTL: 10 * time.Second})
			}))
			defer srv.Close()
			c := New(Config{Endpoints: []string{srv.URL}, ProxyID: "p1", CacheDir: b.TempDir(), Interval: 5 * time.Second, LeaseTTL: 10 * time.Second},
				slog.New(slog.DiscardHandler))
			c.Gates = admission.New()
			d := control.Directory{File: directory.File{Version: 1, Identity: testIdentity,
				Clusters: map[string]config.Cluster{"vast01": {Type: "s3", Scheme: "http", Region: "r", EndpointMode: "static", Endpoints: []string{"127.0.0.1:1"},
					Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:vast01"}}},
				Tenants: map[string]directory.Tenant{"acme": {DefaultCluster: "vast01"}}, Placements: make(map[string]directory.Placement, placements)},
				Secrets: map[string]string{"control:vast01": "s1"}}
			for i := range placements {
				name := fmt.Sprintf("bucket-%06d", i)
				p := directory.Placement{State: directory.StateActive, Primary: "vast01", Names: map[string]string{"vast01": name}}
				if i < 100 {
					p.Barrier = &config.Barrier{ID: fmt.Sprintf("1758800000000-%06x", i), Kind: config.BarrierMutations}
				}
				d.Placements["acme/"+name] = p
			}
			if _, err := c.install(&d, false); err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			if err := c.beat(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := c.beat(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(bytesIn.Load()), "bytes/heartbeat")
		})
	}
}
