package admission

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// scaleDirectory is design §8's large directory for the admission benchmarks (ADR-0021 T19):
// placements over four clusters, the first barriers of them held, alternating the two kinds (a
// source barrier on a placement in CUTOVER, as validation requires). clusterBarrier, if set, puts
// a read-only barrier on cluster c0 as well, which closes mutations on every placement with a
// bucket there: a quarter of the directory.
func scaleDirectory(placements, barriers int, clusterBarrier bool) *directory.File {
	f := &directory.File{Placements: make(map[string]directory.Placement, placements), Clusters: map[string]config.Cluster{}}
	for c := range 4 {
		f.Clusters[fmt.Sprintf("c%d", c)] = config.Cluster{}
	}
	if clusterBarrier {
		f.Clusters["c0"] = config.Cluster{ReadOnly: true, Barrier: &config.Barrier{ID: "1758800000000-c0c0c0", Kind: config.BarrierMutations}}
	}
	for i := range placements {
		key := fmt.Sprintf("tenant-%03d/bucket-%06d", i%1000, i)
		p := directory.Placement{State: directory.StateActive, Primary: fmt.Sprintf("c%d", i%4)}
		if i < barriers {
			kind := config.BarrierMutations
			if i%2 == 1 {
				kind = config.BarrierSource
				p.State, p.Source, p.Target = directory.StateCutover, fmt.Sprintf("c%d", (i+1)%4), p.Primary
			}
			p.Barrier = &config.Barrier{ID: fmt.Sprintf("1758800000000-%06x", i), Kind: kind}
		}
		f.Placements[key] = p
	}
	return f
}

// BenchmarkEnterReleaseSpread is the request path's admission cost when the traffic is spread
// over many placements, as a proxy serving many buckets sees it: a token taken and released,
// GOMAXPROCS goroutines, each walking its own run of 10,000 placements. It is the counterpart
// of BenchmarkEnterRelease (every goroutine on one placement, the contended case): the gates
// share no lock across placements, so this one should scale with cores and the other should not.
func BenchmarkEnterReleaseSpread(b *testing.B) {
	const n = 10_000
	keys := make([]string, n)
	g := New()
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant-%03d/bucket-%06d", i%1000, i)
		tok, _, _ := g.Enter(keys[i], Mutations) // gates are made on first use; this is the steady state
		tok.Release(g, Definitive)
	}
	var worker atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := int(worker.Add(1)) * 7919
		for pb.Next() {
			tok, _, ok := g.Enter(keys[i%n], Mutations)
			if !ok {
				b.Error("an open gate refused")
				return
			}
			tok.Release(g, Definitive)
			i++
		}
	})
}

// BenchmarkEnterClosedParallel is the refusal of a held placement's mutations, GOMAXPROCS
// goroutines on the one closed gate: what a client retrying writes against a bucket under a
// barrier costs the proxy before the 503. It should cost no more than an admission.
func BenchmarkEnterClosedParallel(b *testing.B) {
	g := New()
	g.Close("acme/held", Mutations, "1758800000000-abcdef")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, barrier, ok := g.Enter("acme/held", Mutations); ok || barrier == "" {
				b.Error("a closed gate admitted")
				return
			}
		}
	})
}

// BenchmarkBarriers100kFirst is the first derivation of a 100,000-placement snapshot's barriers
// (100 held placements): one scan of every placement, which a proxy pays once per installed
// directory version, when its gate keeper applies the version's closures. Placements/op is the
// directory's size.
func BenchmarkBarriers100kFirst(b *testing.B) {
	snap := directory.NewSnapshot(scaleDirectory(100_000, 100, false))
	b.ReportAllocs()
	for b.Loop() {
		g := New() // a fresh cache: every iteration derives
		if c, _ := g.Barriers(snap); len(c) != 100 {
			b.Fatalf("closures: %d", len(c))
		}
	}
	b.ReportMetric(100_000, "placements/op")
}

// BenchmarkBarriers100kCached is BenchmarkBarriers100kFirst once the snapshot's derivation is
// cached: what every later reader of the same installed version (the next heartbeat, the served
// bundle's publish) pays. It must not depend on the directory's size.
func BenchmarkBarriers100kCached(b *testing.B) {
	snap := directory.NewSnapshot(scaleDirectory(100_000, 100, false))
	g := New()
	g.Barriers(snap)
	b.ReportAllocs()
	for b.Loop() {
		if c, _ := g.Barriers(snap); len(c) != 100 {
			b.Fatalf("closures: %d", len(c))
		}
	}
}

// BenchmarkAcks100kFirst is a heartbeat's drain proof over a 100,000-placement snapshot with
// 100 barriers, the first time the snapshot is read: the derivation's scan plus one ack per
// barrier. Acks/op is what the heartbeat carries.
func BenchmarkAcks100kFirst(b *testing.B) {
	snap := directory.NewSnapshot(scaleDirectory(100_000, 100, false))
	b.ReportAllocs()
	for b.Loop() {
		g := New()
		if len(g.Acks(snap)) != 100 {
			b.Fatal("acks")
		}
	}
	b.ReportMetric(100, "acks/op")
}

// BenchmarkAcks100kCached is the same drain proof on every later heartbeat of the same installed
// version: 100 gate reads and a sort, bounded by barriers, not by placements.
func BenchmarkAcks100kCached(b *testing.B) {
	snap := directory.NewSnapshot(scaleDirectory(100_000, 100, false))
	g := New()
	g.Acks(snap)
	b.ReportAllocs()
	for b.Loop() {
		if len(g.Acks(snap)) != 100 {
			b.Fatal("acks")
		}
	}
	b.ReportMetric(100, "acks/op")
}

// BenchmarkAcks100kClusterBarrierCached is the drain proof, on a cached snapshot, while a
// cluster barrier (read-only on one of four clusters) is in flight next to 100 placement
// barriers. A cluster's ack sums the gate of every placement with a bucket on it, so this one is
// bounded by the cluster's placements (25,000 here), not by barriers: the cost every heartbeat
// pays for as long as the cluster is held. Placements/op is the gates read per heartbeat.
func BenchmarkAcks100kClusterBarrierCached(b *testing.B) {
	snap := directory.NewSnapshot(scaleDirectory(100_000, 100, true))
	g := New()
	acks := g.Acks(snap)
	if len(acks) != 101 {
		b.Fatalf("acks: %d", len(acks))
	}
	_, placements := g.Barriers(snap)
	held := len(placements["1758800000000-c0c0c0"])
	b.ReportAllocs()
	for b.Loop() {
		if len(g.Acks(snap)) != 101 {
			b.Fatal("acks")
		}
	}
	b.ReportMetric(float64(held), "placements/op")
}

// BenchmarkApply100k is the gate keeper's install step on a proxy that has gates for 100,000
// placements (every one has taken a mutation) when a version with 100 barriers is installed:
// close the 100, open every other gate. Once per installed version, never per request.
func BenchmarkApply100k(b *testing.B) {
	snap := directory.NewSnapshot(scaleDirectory(100_000, 100, false))
	g := New()
	snap.EachPlacement(func(key string, _ *directory.Placement) bool {
		tok, _, _ := g.Enter(key, Mutations)
		tok.Release(g, Definitive)
		return true
	})
	closures, _ := g.Barriers(snap)
	b.ReportAllocs()
	for b.Loop() {
		g.Apply(closures, false)
	}
	b.ReportMetric(100_000, "gates/op")
}
