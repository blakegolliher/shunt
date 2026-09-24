// Package admission is a proxy's local admission accounting (ADR-0021 D2). Every request that can
// change a placement's backend buckets, and every request that depends on a moving placement's
// source, takes a token from that placement's gate before it touches a backend and gives it back
// when the backend's outcome is known. A drain barrier, written on a placement or a cluster with
// the change it guards, closes the gate: no new token is handed out, and the proxy reports, in
// each heartbeat, how many tokens are still out and how many backend outcomes it never learned.
// The control node commits the change only once every proxy reports zero of both.
//
// Gates are keyed by placement, made on first use and kept, so the request path's cost is one
// map lookup and one short critical section per mutation; reads of a placement that is not moving
// take nothing. Closing a gate and taking a token share the gate's lock, so a request that took
// its bundle before a barrier was installed is refused when it reaches the gate after, and one
// that entered before is counted until it ends. No lock is held across backend I/O.
package admission

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// Kind is one of a placement's gates.
type Kind uint8

const (
	// Mutations counts every request that can change the placement's buckets: writes, deletes,
	// multipart parts and completion, bucket configuration, bucket creation and deletion.
	Mutations Kind = iota
	// Source counts work that depends on a moving placement's source: a read that falls back to
	// it, a listing that merges it, a copy that reads it, the source leg of a dual delete.
	Source
	kinds
)

// String names the kind as a barrier does (config.BarrierMutations, config.BarrierSource).
func (k Kind) String() string {
	if k == Source {
		return config.BarrierSource
	}
	return config.BarrierMutations
}

// KindOf is the gate a barrier kind names.
func KindOf(kind string) (Kind, bool) {
	switch kind {
	case config.BarrierMutations:
		return Mutations, true
	case config.BarrierSource:
		return Source, true
	}
	return 0, false
}

// Outcome is how a token's backend work ended.
type Outcome uint8

const (
	// Definitive is a request the backend answered, or that provably never reached it whole (a
	// connection that was refused, a body cut short before it was all sent).
	Definitive Outcome = iota
	// Uncertain is a request sent whole and never answered. The backend may have committed it,
	// and nothing this proxy can do will tell; the count stays for the life of the process.
	Uncertain
)

// Closed is what closes each of a placement's gates: the barrier id, or "" for open.
type Closed [kinds]string

// Closures maps placement keys to what closes their gates.
type Closures map[string]Closed

type gate struct {
	mu        sync.Mutex
	closed    Closed
	inflight  [kinds]int64
	uncertain [kinds]int64
}

// Gates is a proxy's admission state: one gate per placement it has seen a mutation or
// source-dependent request for.
type Gates struct {
	m         sync.Map     // placement key → *gate
	uncertain atomic.Int64 // outcomes never learned, every gate, this process
}

// New returns empty gates.
func New() *Gates { return &Gates{} }

func (g *Gates) gate(key string) *gate {
	if v, ok := g.m.Load(key); ok {
		return v.(*gate) //nolint:errcheck // the map holds only *gate
	}
	v, _ := g.m.LoadOrStore(key, &gate{})
	return v.(*gate) //nolint:errcheck // the map holds only *gate
}

// Token is one admitted piece of work. The zero Token releases nothing, so a caller that took none
// (a read of a settled placement, a nil Gates) needs no branch at the end.
type Token struct {
	g    *gate
	kind Kind
}

// Enter takes a token for kind on the placement key. It refuses, naming the barrier, when the
// gate is closed. A nil Gates admits everything and counts nothing.
func (g *Gates) Enter(key string, kind Kind) (t Token, barrier string, ok bool) {
	if g == nil {
		return Token{}, "", true
	}
	gt := g.gate(key)
	gt.mu.Lock()
	if b := gt.closed[kind]; b != "" {
		gt.mu.Unlock()
		return Token{}, b, false
	}
	gt.inflight[kind]++
	gt.mu.Unlock()
	return Token{g: gt, kind: kind}, "", true
}

// Release gives the token back with how its work ended.
func (t Token) Release(g *Gates, o Outcome) {
	if t.g == nil {
		return
	}
	t.g.mu.Lock()
	t.g.inflight[t.kind]--
	if o == Uncertain {
		t.g.uncertain[t.kind]++
	}
	t.g.mu.Unlock()
	if o == Uncertain && g != nil {
		g.uncertain.Add(1)
	}
}

// Close closes kind on key for barrier; a request that reaches the gate after this is refused.
func (g *Gates) Close(key string, kind Kind, barrier string) {
	gt := g.gate(key)
	gt.mu.Lock()
	gt.closed[kind] = barrier
	gt.mu.Unlock()
}

// Open opens kind on key.
func (g *Gates) Open(key string, kind Kind) {
	if v, ok := g.m.Load(key); ok {
		gt := v.(*gate) //nolint:errcheck // the map holds only *gate
		gt.mu.Lock()
		gt.closed[kind] = ""
		gt.mu.Unlock()
	}
}

// Apply makes the gates match closures: every gate named is closed as listed, and every other
// gate is opened, unless sticky, when a gate that is closed stays closed. A proxy applies the
// closures of each installed directory version; sticky while its requests still use an older
// version (install backpressure), since a gate opened for a version no request routes by yet
// would let a request on the old version write by the old rule.
func (g *Gates) Apply(closures Closures, sticky bool) {
	if g == nil {
		return
	}
	for key, c := range closures {
		gt := g.gate(key)
		gt.mu.Lock()
		for k := range kinds {
			if c[k] != "" || !sticky {
				gt.closed[k] = c[k]
			}
		}
		gt.mu.Unlock()
	}
	if sticky {
		return
	}
	g.m.Range(func(k, v any) bool {
		if _, listed := closures[k.(string)]; listed { //nolint:errcheck // the map is keyed by string
			return true
		}
		gt := v.(*gate) //nolint:errcheck // the map holds only *gate
		gt.mu.Lock()
		gt.closed = Closed{}
		gt.mu.Unlock()
		return true
	})
}

// State is one placement's gates as they stand.
type State struct {
	Closed    Closed
	Inflight  [kinds]int64
	Uncertain [kinds]int64
}

// State reports one placement's gates.
func (g *Gates) State(key string) State {
	if g == nil {
		return State{}
	}
	v, ok := g.m.Load(key)
	if !ok {
		return State{}
	}
	gt := v.(*gate) //nolint:errcheck // the map holds only *gate
	gt.mu.Lock()
	defer gt.mu.Unlock()
	return State{Closed: gt.closed, Inflight: gt.inflight, Uncertain: gt.uncertain}
}

// Uncertain is how many backend outcomes this process never learned, every gate together. It
// never goes down: an incarnation that has any cannot prove a barrier drained (ADR-0021 D2).
func (g *Gates) Uncertain() int64 {
	if g == nil {
		return 0
	}
	return g.uncertain.Load()
}

// Inflight is how many tokens are out, every gate and kind together: what a process that stops
// before they are given back leaves unknown.
func (g *Gates) Inflight() int64 {
	if g == nil {
		return 0
	}
	var n int64
	g.m.Range(func(_, v any) bool {
		gt := v.(*gate) //nolint:errcheck // the map holds only *gate
		gt.mu.Lock()
		for k := range kinds {
			n += gt.inflight[k]
		}
		gt.mu.Unlock()
		return true
	})
	return n
}

// Ack is one barrier's proof from one proxy, as the heartbeat carries it (ADR-0021 D2).
type Ack struct {
	ID string `json:"id"`
	// Scope is directory.PlacementResource or directory.ClusterResource; Generation is its
	// generation in the directory the proxy installed, which the control node compares exactly.
	Scope      string `json:"scope"`
	Generation int64  `json:"generation"`
	Kind       string `json:"kind"`
	// Closed: every gate the barrier names is closed on this proxy. Inflight: tokens still out
	// through them; Uncertain: outcomes through them never learned. A cluster barrier sums the
	// placements on the cluster.
	Closed    bool  `json:"closed"`
	Inflight  int64 `json:"inflight"`
	Uncertain int64 `json:"uncertain"`
}

// Directory is what Barriers reads: a *directory.Snapshot, or a *directory.File in tests.
type Directory interface {
	EachPlacement(fn func(key string, p *directory.Placement) bool)
	EachCluster(fn func(name string, c *config.Cluster) bool)
	Generation(resource string) int64
}

// Barriers derives, from a directory, what each barrier in it closes: a placement's barrier
// closes the kind it names on that placement; a cluster's closes mutations on every placement with
// a bucket on the cluster. It returns the closures and, for each barrier, the placements it closes.
// Placements are scanned for their clusters only when some cluster carries a barrier.
func Barriers(d Directory) (closures Closures, placements map[string][]string) {
	closures, placements = Closures{}, map[string][]string{}
	clusterBarriers := map[string]string{}
	d.EachCluster(func(name string, c *config.Cluster) bool {
		if c.Barrier != nil {
			clusterBarriers[name] = c.Barrier.ID
		}
		return true
	})
	d.EachPlacement(func(key string, p *directory.Placement) bool {
		if b := p.Barrier; b != nil {
			if k, ok := KindOf(b.Kind); ok {
				c := closures[key]
				c[k] = b.ID
				closures[key] = c
				placements[b.ID] = append(placements[b.ID], key)
			}
		}
		if len(clusterBarriers) == 0 {
			return true
		}
		for _, name := range PlacementClusters(p) {
			id, ok := clusterBarriers[name]
			if !ok {
				continue
			}
			cl := closures[key]
			if cl[Mutations] == "" {
				cl[Mutations] = id
			}
			closures[key] = cl
			placements[id] = append(placements[id], key)
		}
		return true
	})
	return closures, placements
}

// Acks reports every barrier in d with this proxy's counts through the gates it closes, sorted
// by scope. The list is bounded by barriers in flight, never by placements.
func (g *Gates) Acks(d Directory) []Ack {
	_, placements := Barriers(d)
	var out []Ack
	d.EachPlacement(func(key string, p *directory.Placement) bool {
		b := p.Barrier
		if b == nil {
			return true
		}
		k, ok := KindOf(b.Kind)
		if !ok {
			return true
		}
		st := g.State(key)
		out = append(out, Ack{ID: b.ID, Scope: directory.PlacementResource(key), Generation: d.Generation(directory.PlacementResource(key)), Kind: b.Kind,
			Closed: st.Closed[k] == b.ID, Inflight: st.Inflight[k], Uncertain: st.Uncertain[k]})
		return true
	})
	d.EachCluster(func(name string, c *config.Cluster) bool {
		b := c.Barrier
		if b == nil {
			return true
		}
		// A placement whose own barrier closed its mutations first drains for both; closed by
		// either is closed.
		ack := Ack{ID: b.ID, Scope: directory.ClusterResource(name), Generation: d.Generation(directory.ClusterResource(name)), Kind: b.Kind, Closed: true}
		for _, key := range placements[b.ID] {
			st := g.State(key)
			ack.Closed = ack.Closed && st.Closed[Mutations] != ""
			ack.Inflight += st.Inflight[Mutations]
			ack.Uncertain += st.Uncertain[Mutations]
		}
		out = append(out, ack)
		return true
	})
	slices.SortFunc(out, func(a, b Ack) int { return strings.Compare(a.Scope, b.Scope) })
	return out
}

// PlacementClusters names every cluster a placement has a bucket on: its roles, and its legs.
func PlacementClusters(p *directory.Placement) []string {
	var out []string
	add := func(name string) {
		if name == "" {
			return
		}
		for _, have := range out {
			if have == name {
				return
			}
		}
		out = append(out, name)
	}
	for _, role := range []string{p.Primary, p.Source, p.Target, p.Cold} {
		add(p.ClusterOf(role))
	}
	for role := range p.Names {
		add(p.ClusterOf(role))
	}
	for _, l := range p.Legs {
		add(l.Cluster)
	}
	return out
}
