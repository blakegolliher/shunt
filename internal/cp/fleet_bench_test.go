package cp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// The etcd fleet table at T19's scale (ADR-0021 D2): 1,000 registered proxies, each reporting a
// drain proof for 100 barriers, on single-node embedded etcd with fsync.

// fleetWindow is a telemetry window as a busy proxy sends it: clusters × six operation classes
// × four latency series of compressed sketches, a counter per class, the full per-bucket block.
func fleetWindow(clusters int) *telemetry.Window {
	if clusters == 0 {
		return nil
	}
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
	for c := range clusters {
		name := fmt.Sprintf("cluster-%02d", c)
		for _, op := range ops {
			w.Counters = append(w.Counters, telemetry.WindowCounter{Op: op, Cluster: name, Requests: 10_000, BytesIn: 1 << 30, BytesOut: 2 << 30,
				Errors: map[string]int64{"404": 3, "not_found": 120, "503": 1}})
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

// fleetHeartbeat is member i's heartbeat: 100 drained barrier acknowledgements and window.
func fleetHeartbeat(i int, seq int64, window *telemetry.Window) control.Heartbeat {
	acks := make([]admission.Ack, 100)
	for j := range acks {
		acks[j] = admission.Ack{ID: fmt.Sprintf("1758800000000-%06x", j), Scope: directory.PlacementResource(fmt.Sprintf("tenant-%03d/bucket-%06d", j, j)),
			Generation: int64(1000 + j), Kind: config.BarrierMutations, Closed: true}
	}
	return control.Heartbeat{Protocol: control.Protocol, Identity: directory.Identity{ClusterID: "c0ffee00c0ffee00c0ffee00c0ffee00", Epoch: "e0000000000000000000000000000001"},
		Started: time.Date(2026, 9, 25, 11, 0, 0, 0, time.UTC), Incarnation: fmt.Sprintf("%032x", i+1), Seq: seq, Applied: 1234, Durable: 1234,
		Host: fmt.Sprintf("proxy-%04d.example", i), Version: "v0.0.0-bench", Secrets: map[string]string{"cluster-00": "3"}, Barriers: acks, Telemetry: window}
}

// fleetOf starts single-node etcd and registers members proxies through their first heartbeat,
// each with a window of telemetry clusters (0: none).
func fleetOf(b *testing.B, members, clusters int) (*Fleet, []control.Heartbeat) {
	b.Helper()
	tc := startCluster(b, 1)
	// A long lease: no member may fall silent while a slow read is measured.
	f := NewFleet(tc.nodes[0].Client(), 10*time.Minute)
	window := fleetWindow(clusters)
	hbs := make([]control.Heartbeat, members)
	for i := range hbs {
		hbs[i] = fleetHeartbeat(i, 1, window)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, members)
	sem := make(chan struct{}, 32)
	for i := range hbs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := f.Heartbeat(ctx, fmt.Sprintf("proxy-%04d", i), hbs[i]); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		b.Fatal(err)
	}
	return f, hbs
}

// benchFleetHeartbeat is one steady-state heartbeat (a known incarnation: a lease keep-alive and
// one transaction compared on the member record's revision) from each member in turn.
func benchFleetHeartbeat(b *testing.B, clusters int) {
	f, hbs := fleetOf(b, 1000, clusters)
	ctx := context.Background()
	raw, err := json.Marshal(proxyRecord{Heartbeat: hbs[0]})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		hb := hbs[i%len(hbs)]
		hb.Seq = int64(2 + i)
		if _, err := f.Heartbeat(ctx, fmt.Sprintf("proxy-%04d", i%len(hbs)), hb); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(raw)), "bytes/record")
}

// BenchmarkFleetHeartbeat1k is the etcd fleet table applying one member's heartbeat with 1,000
// members registered, each reporting 100 barriers, no telemetry: what shunt-control pays per
// heartbeat, 1,000 times a second at 1,000 proxies on a 1-second interval. Bytes/record is the
// proxy key written.
func BenchmarkFleetHeartbeat1k(b *testing.B) { benchFleetHeartbeat(b, 0) }

// BenchmarkFleetHeartbeat1kTelemetry is BenchmarkFleetHeartbeat1k with two clusters' telemetry
// in every heartbeat: the proxy key then carries the window too.
func BenchmarkFleetHeartbeat1kTelemetry(b *testing.B) { benchFleetHeartbeat(b, 2) }

// BenchmarkFleetHeartbeat1kParallel is BenchmarkFleetHeartbeat1k with heartbeats arriving
// concurrently (GOMAXPROCS × 4 senders, each member from one sender at a time), as 1,000
// proxies send them: etcd batches their commits, so ns/op here is the per-heartbeat share of a
// control node's throughput, not one heartbeat's latency.
func BenchmarkFleetHeartbeat1kParallel(b *testing.B) {
	f, hbs := fleetOf(b, 1000, 0)
	ctx := context.Background()
	var next atomic.Int64
	b.SetParallelism(4)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := int(next.Add(1))
			hb := hbs[n%len(hbs)]
			hb.Seq = int64(2 + n)
			if _, err := f.Heartbeat(ctx, fmt.Sprintf("proxy-%04d", n%len(hbs)), hb); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// benchFleetMembers is one read of the whole fleet table.
func benchFleetMembers(b *testing.B, clusters int) {
	f, _ := fleetOf(b, 1000, clusters)
	ctx := context.Background()
	resp, err := f.cli.Get(ctx, fleetPrefix, clientv3.WithPrefix())
	if err != nil {
		b.Fatal(err)
	}
	var read int
	for _, kv := range resp.Kvs {
		read += len(kv.Key) + len(kv.Value)
	}
	b.ReportAllocs()
	for b.Loop() {
		ms, err := f.Members(ctx)
		live := 0
		for i := range ms {
			if ms[i].Live {
				live++
			}
		}
		if err != nil || live != 1000 {
			b.Fatalf("live members: %d of %d, %v", live, len(ms), err)
		}
	}
	b.ReportMetric(float64(read), "bytes/read")
	b.ReportMetric(1000, "members/op")
}

// BenchmarkFleetMembers1k is one read of the etcd fleet table with 1,000 members each reporting
// 100 barriers, no telemetry. Barrier evaluation (control.drainBlockers) reads the whole table on
// every poll of every waiting operation, so this, not the evaluation itself
// (control.BenchmarkDrainBlockers1kDrained), is the poll's floor on shunt-control. Bytes/read is
// what one read returns.
func BenchmarkFleetMembers1k(b *testing.B) { benchFleetMembers(b, 0) }

// BenchmarkFleetMembers1kTelemetry is BenchmarkFleetMembers1k when every member's last
// heartbeat carried two clusters' telemetry: the table read decodes every window too, though a
// barrier's evaluation reads none of it.
func BenchmarkFleetMembers1kTelemetry(b *testing.B) { benchFleetMembers(b, 2) }
