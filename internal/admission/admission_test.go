package admission

import (
	"sync"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// A closed gate refuses a new token and names its barrier; a token taken before the close is
// counted until it is released; an uncertain release stays counted for good.
func TestGateCloseAndDrain(t *testing.T) {
	g := New()
	tok, _, ok := g.Enter("acme/data", Mutations)
	if !ok {
		t.Fatal("an open gate refused")
	}
	g.Close("acme/data", Mutations, "op-1")
	if _, barrier, ok := g.Enter("acme/data", Mutations); ok || barrier != "op-1" {
		t.Fatalf("a closed gate admitted: ok=%v barrier=%q", ok, barrier)
	}
	if _, _, ok := g.Enter("acme/data", Source); !ok {
		t.Fatal("closing mutations closed the source gate too")
	}
	if _, _, ok := g.Enter("acme/other", Mutations); !ok {
		t.Fatal("closing one placement closed another")
	}
	st := g.State("acme/data")
	if st.Inflight[Mutations] != 1 || st.Closed[Mutations] != "op-1" {
		t.Fatalf("state with one token out: %+v", st)
	}
	tok.Release(g, Definitive)
	if st := g.State("acme/data"); st.Inflight[Mutations] != 0 || st.Uncertain[Mutations] != 0 {
		t.Fatalf("state after a definitive release: %+v", st)
	}
	g.Open("acme/data", Mutations)
	tok, _, _ = g.Enter("acme/data", Mutations)
	tok.Release(g, Uncertain)
	if st := g.State("acme/data"); st.Inflight[Mutations] != 0 || st.Uncertain[Mutations] != 1 || g.Uncertain() != 1 {
		t.Fatalf("state after an uncertain release: %+v total %d", st, g.Uncertain())
	}
	// The zero token and nil gates are no-ops.
	Token{}.Release(g, Uncertain)
	var none *Gates
	if _, _, ok := none.Enter("x", Mutations); !ok || none.Uncertain() != 0 {
		t.Fatal("nil gates must admit and count nothing")
	}
	none.Apply(Closures{"x": {}}, false)
}

// Apply closes what the closures list and opens the rest, except while sticky, when a closed gate
// stays closed (install backpressure: no request routes by the version that opened it yet).
func TestApply(t *testing.T) {
	g := New()
	g.Close("a", Mutations, "op-1")
	g.Close("b", Source, "op-2")
	g.Apply(Closures{"a": {Mutations: "op-1"}, "c": {Mutations: "op-3"}}, false)
	if st := g.State("b"); st.Closed[Source] != "" {
		t.Fatal("a gate the closures do not list stayed closed")
	}
	if st := g.State("c"); st.Closed[Mutations] != "op-3" {
		t.Fatal("a listed gate was not closed")
	}
	g.Apply(Closures{}, true)
	if st := g.State("a"); st.Closed[Mutations] != "op-1" {
		t.Fatal("a sticky apply opened a closed gate")
	}
	g.Apply(Closures{}, false)
	if st := g.State("a"); st.Closed[Mutations] != "" {
		t.Fatal("a plain apply left a gate closed")
	}
}

func file() *directory.File {
	return &directory.File{Version: 9, Generations: map[string]int64{"placement:acme/held": 8, "placement:acme/purge": 7, "cluster:vast01": 9},
		Clusters: map[string]config.Cluster{
			"vast01": {Barrier: &config.Barrier{ID: "op-c", Kind: config.BarrierMutations}},
			"vast02": {},
		},
		Placements: map[string]directory.Placement{
			"acme/held":   {State: directory.StateRamping, Primary: "vast02", Source: "vast01", Barrier: &config.Barrier{ID: "op-h", Kind: config.BarrierMutations}},
			"acme/purge":  {State: directory.StateCutover, Primary: "vast02", Source: "vast02", Barrier: &config.Barrier{ID: "op-p", Kind: config.BarrierSource}},
			"acme/on-c":   {State: directory.StateActive, Primary: "vast01"},
			"acme/spread": {State: directory.StateActive, Legs: map[string]directory.Leg{"a": {Cluster: "vast01"}, "b": {Cluster: "vast02"}}},
			"acme/quiet":  {State: directory.StateActive, Primary: "vast02"},
		}}
}

// A placement barrier closes its kind on its placement; a cluster barrier closes mutations on every
// placement with a bucket on the cluster; the acks name each barrier with its scope's generation
// and the counts through the gates it closes.
func TestBarriersAndAcks(t *testing.T) {
	f := file()
	closures, placements := Barriers(f)
	want := Closures{
		"acme/held":   {Mutations: "op-h"}, // its own hold closes mutations; vast01 is its source, closed by op-c too, but the hold came first
		"acme/purge":  {Source: "op-p"},
		"acme/on-c":   {Mutations: "op-c"},
		"acme/spread": {Mutations: "op-c"},
	}
	// acme/held is on vast01 as well: the cluster barrier lists it, the placement's own barrier keeps
	// the mutations gate's id.
	for key, c := range want {
		if closures[key] != c {
			t.Errorf("closure of %s = %v, want %v", key, closures[key], c)
		}
	}
	if _, ok := closures["acme/quiet"]; ok {
		t.Error("a placement on no barrier is listed")
	}
	if len(placements["op-c"]) != 3 {
		t.Errorf("the cluster barrier closes %v, want three placements", placements["op-c"])
	}
	g := New()
	g.Apply(closures, false)
	tok, _, _ := g.Enter("acme/on-c", Mutations) // taken before the close would be counted; here the gate is closed, so refused
	if tok.g != nil {
		t.Fatal("a closed gate handed out a token")
	}
	g.Open("acme/spread", Mutations)
	tok, _, _ = g.Enter("acme/spread", Mutations)
	g.Close("acme/spread", Mutations, "op-c")
	g.Open("acme/purge", Source)
	src, _, ok := g.Enter("acme/purge", Source)
	if !ok {
		t.Fatal("the source gate refused after being opened")
	}
	g.Close("acme/purge", Source, "op-p")
	src.Release(g, Uncertain)
	acks := g.Acks(f)
	byScope := map[string]Ack{}
	for _, a := range acks {
		byScope[a.Scope] = a
	}
	if a := byScope["cluster:vast01"]; a.ID != "op-c" || a.Generation != 9 || !a.Closed || a.Inflight != 1 || a.Uncertain != 0 {
		t.Errorf("cluster ack: %+v", a)
	}
	if a := byScope["placement:acme/held"]; a.ID != "op-h" || a.Generation != 8 || !a.Closed || a.Kind != config.BarrierMutations {
		t.Errorf("held ack: %+v", a)
	}
	if a := byScope["placement:acme/purge"]; a.ID != "op-p" || a.Generation != 7 || !a.Closed || a.Uncertain != 1 || a.Kind != config.BarrierSource {
		t.Errorf("purge ack: %+v", a)
	}
	if len(acks) != 3 || acks[0].Scope != "cluster:vast01" {
		t.Errorf("acks: %+v", acks)
	}
	tok.Release(g, Definitive)
	if a := New().Acks(f); len(a) != 3 || a[0].Closed || a[1].Closed {
		t.Errorf("acks with no gates closed yet: %+v", a)
	}
}

// Closing and entering interlock: however they interleave, a token is either refused or counted;
// none is lost.
func TestCloseEnterRace(t *testing.T) {
	g := New()
	var wg sync.WaitGroup
	var mu sync.Mutex
	held := []Token{}
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if tok, _, ok := g.Enter("k", Mutations); ok {
					mu.Lock()
					held = append(held, tok)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 50 {
			if i%2 == 0 {
				g.Close("k", Mutations, "b")
			} else {
				g.Open("k", Mutations)
			}
		}
	}()
	wg.Wait()
	if st := g.State("k"); st.Inflight[Mutations] != int64(len(held)) {
		t.Fatalf("inflight %d, tokens out %d", st.Inflight[Mutations], len(held))
	}
	for _, tok := range held {
		tok.Release(g, Definitive)
	}
	if st := g.State("k"); st.Inflight[Mutations] != 0 {
		t.Fatalf("inflight %d after every release", st.Inflight[Mutations])
	}
}

// BenchmarkEnterRelease is the request path's cost: a token taken and released, sixteen goroutines
// on one placement.
func BenchmarkEnterRelease(b *testing.B) {
	g := New()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tok, _, _ := g.Enter("acme/data", Mutations)
			tok.Release(g, Definitive)
		}
	})
}

// BenchmarkAcks is a heartbeat's report over a directory of 10,000 placements with eight barriers.
func BenchmarkAcks(b *testing.B) {
	f := &directory.File{Placements: map[string]directory.Placement{}, Clusters: map[string]config.Cluster{"c": {}}}
	for i := range 10000 {
		key := "acme/b" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/676)%26))
		p := directory.Placement{State: directory.StateActive, Primary: "c"}
		if i < 8 {
			p.Barrier = &config.Barrier{ID: "op", Kind: config.BarrierMutations}
		}
		f.Placements[key] = p
	}
	g := New()
	b.ReportAllocs()
	for b.Loop() {
		if len(g.Acks(f)) != 8 {
			b.Fatal("acks")
		}
	}
}
