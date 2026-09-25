package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// The control node's side of ADR-0021 D2 at T19's scale: barrier evaluation against 1,000
// members each acknowledging 100 barriers, and the heartbeat handler against a large directory.

// benchIdentity is the lineage of the benchmarks' directory.
var benchIdentity = directory.Identity{ClusterID: "c0ffee00c0ffee00c0ffee00c0ffee00", Epoch: "e0000000000000000000000000000001"}

// snapStore is a directory.Store that serves one immutable snapshot: all that barrier evaluation
// and the heartbeat handler read. Any other method panics (nil embedded interface).
type snapStore struct {
	directory.Store
	snap *directory.Snapshot
}

func (s snapStore) Snapshot() *directory.Snapshot { return s.snap }

// benchSnapshot is a directory of placements (at least one: acme/held, carrying the barrier
// under evaluation) on one cluster, at version 10.
func benchSnapshot(placements int, barrier string) *directory.Snapshot {
	f := &directory.File{Version: 10, Identity: benchIdentity, Clusters: map[string]config.Cluster{"c0": {}},
		Placements: make(map[string]directory.Placement, placements)}
	f.Placements["acme/held"] = directory.Placement{State: directory.StateActive, Primary: "c0", Names: map[string]string{"c0": "held"},
		Barrier: &config.Barrier{ID: barrier, Kind: config.BarrierMutations}}
	for i := 1; i < placements; i++ {
		name := fmt.Sprintf("bucket-%06d", i)
		f.Placements["acme/"+name] = directory.Placement{State: directory.StateActive, Primary: "c0", Names: map[string]string{"c0": name}}
	}
	return directory.NewSnapshot(f)
}

// benchAcks is a member's drain proof for n barriers, all drained, the barrier st names last:
// memberBlockers finds it by a linear search, so last is its worst case.
func benchAcks(n int, st *BarrierState) []admission.Ack {
	acks := make([]admission.Ack, 0, n)
	for i := range n - 1 {
		acks = append(acks, admission.Ack{ID: fmt.Sprintf("1758800000000-%06x", i), Scope: directory.PlacementResource(fmt.Sprintf("acme/other-%03d", i)),
			Generation: 7, Kind: config.BarrierMutations, Closed: true})
	}
	return append(acks, admission.Ack{ID: st.ID, Scope: st.Scope, Generation: st.Generation, Kind: st.Kind, Closed: true})
}

// benchMembers is n live members on the directory's lineage, every one drained for st through
// acks acknowledgements, with durable caches at the hold's version. silent of them (spread
// through the list) are silent instead, and unresolved carry an incarnation that never retired.
func benchMembers(n, acks, silent, unresolved int, st *BarrierState) []Member {
	ms := make([]Member, n)
	proof := benchAcks(acks, st)
	for i := range ms {
		ms[i] = Member{ID: fmt.Sprintf("proxy-%04d", i), Live: true, Identity: benchIdentity, Applied: st.HoldVersion, Durable: st.HoldVersion,
			Incarnation: &Incarnation{ID: fmt.Sprintf("%032x", i+1), State: IncarnationActive}, Barriers: proof, SinceSeen: time.Second}
	}
	for j := range silent {
		m := &ms[(j*n/max(silent, 1))%n]
		m.Live, m.Barriers, m.SinceSeen = false, nil, 30*time.Second
	}
	for j := range unresolved {
		m := &ms[(j*n/max(unresolved, 1)+1)%n]
		m.Unresolved = []Incarnation{{ID: fmt.Sprintf("%032x", 1<<40+j), State: IncarnationUnclean, Uncertain: 1}}
	}
	return ms
}

// benchDrain builds a server whose fleet is ms over a directory of placements, and a barrier
// state as the hold wrote it.
func benchDrain(placements int, members func(st *BarrierState) []Member) (*Server, *BarrierState) {
	st := &BarrierState{ID: "1758800000000-abcdef", Scope: directory.PlacementResource("acme/held"), Kind: config.BarrierMutations, HoldVersion: 10, Generation: 9}
	fleet := &barrierFleet{}
	fleet.set(members(st)...)
	s := &Server{Dir: snapStore{snap: benchSnapshot(placements, st.ID)}, Fleet: fleet, Metrics: telemetry.NewMetrics()}
	return s, st
}

func runDrain(b *testing.B, s *Server, st *BarrierState, want func(int) bool, members int) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		out, err := s.drainBlockers(ctx, st, nil)
		if err != nil || !want(len(out)) {
			b.Fatalf("blockers: %d, %v", len(out), err)
		}
	}
	b.ReportMetric(float64(members), "members/op")
}

// BenchmarkDrainBlockers1kDrained is one evaluation of a barrier (one poll of blockUntil, every
// FencePoll while an operation waits) against 1,000 live members, each acknowledging 100
// barriers with the one under evaluation among them, all drained. It is the members × acks
// cost of the drain check on the control node, excluding the fleet store's read (the fake
// fleet hands back its slice; cp.BenchmarkFleetMembers1k measures etcd's).
func BenchmarkDrainBlockers1kDrained(b *testing.B) {
	s, st := benchDrain(10, func(st *BarrierState) []Member { return benchMembers(1000, 100, 0, 0, st) })
	runDrain(b, s, st, func(n int) bool { return n == 0 }, 1000)
}

// BenchmarkDrainBlockers1kMixed is BenchmarkDrainBlockers1kDrained with 100 members standing in
// the way: 50 silent and 50 carrying an incarnation that never retired. Each blocker is a
// formatted message, so this is the evaluation's cost when it has something to report.
func BenchmarkDrainBlockers1kMixed(b *testing.B) {
	s, st := benchDrain(10, func(st *BarrierState) []Member { return benchMembers(1000, 100, 50, 50, st) })
	runDrain(b, s, st, func(n int) bool { return n == 100 }, 1000)
}

// BenchmarkDrainBlockers1kDir100k is BenchmarkDrainBlockers1kDrained with a 100,000-placement
// directory on the control node. The members and their acks are the same, so any difference
// from the 10-placement run is a cost of the directory's size on each poll.
func BenchmarkDrainBlockers1kDir100k(b *testing.B) {
	s, st := benchDrain(100_000, func(st *BarrierState) []Member { return benchMembers(1000, 100, 0, 0, st) })
	runDrain(b, s, st, func(n int) bool { return n == 0 }, 1000)
}

// benchWindow is a telemetry window as a busy proxy sends it: clusters × six operation classes
// × four latency series of compressed sketches, a counter per class with a few status keys, and
// the full per-bucket block (MaxBucketsPerWindow placements on one cluster).
func benchWindow(clusters int) *telemetry.Window {
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

// BenchmarkHeartbeatHandler is the control node's handling of one member heartbeat, fleet store
// excluded (the fake fleet grants at once): decode with the 1 MiB cap and unknown fields refused,
// validation of 100 barrier acknowledgements, the lineage check and the answer. The heartbeat
// carries two clusters' telemetry. At 1,000 proxies on a 1-second interval a control node takes
// 1,000 of these a second. The placements sub-benchmarks vary only the control node's directory.
func BenchmarkHeartbeatHandler(b *testing.B) {
	for _, placements := range []int{10, 100_000} {
		b.Run(fmt.Sprintf("placements=%d", placements), func(b *testing.B) {
			s, st := benchDrain(placements, func(*BarrierState) []Member { return nil })
			hb := Heartbeat{Protocol: Protocol, Identity: benchIdentity, Started: time.Date(2026, 9, 25, 11, 0, 0, 0, time.UTC),
				Incarnation: fmt.Sprintf("%032x", 1), Seq: 42, Applied: 10, Durable: 10, Host: "proxy-0001.example", Version: "v0.0.0-bench",
				Secrets: map[string]string{"c0": "3"}, Barriers: benchAcks(100, st), Telemetry: benchWindow(2)}
			body, err := json.Marshal(hb)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				req := httptest.NewRequest(http.MethodPost, "/v1/fleet/proxy-0001/heartbeat", bytes.NewReader(body))
				req.SetPathValue("id", "proxy-0001")
				rec := httptest.NewRecorder()
				s.heartbeat(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("heartbeat: %d %s", rec.Code, rec.Body.String())
				}
			}
			b.ReportMetric(float64(len(body)), "bytes/heartbeat")
		})
	}
}

// BenchmarkFenceStatus1k is the fence block of GET /v1/placements/{tenant}/{bucket}/view (the
// Buckets screen) with 1,000 live members over a 1,000-placement directory: one read of the fleet
// and a check of each member's installed version. It makes the same kind of pass as
// BenchmarkDrainBlockers1kDrained, so a large gap between the two is per-member work that is not
// the check itself (at the time of writing, a copy of the directory per member).
func BenchmarkFenceStatus1k(b *testing.B) {
	s, _ := benchDrain(1000, func(st *BarrierState) []Member { return benchMembers(1000, 100, 0, 0, st) })
	p := directory.Placement{State: directory.StateActive, Primary: "c0"}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if fs := s.fenceStatus(ctx, p); fs.Proxies != 1000 || len(fs.WaitingOn) != 0 {
			b.Fatalf("fence: %+v", fs)
		}
	}
	b.ReportMetric(1000, "members/op")
}
