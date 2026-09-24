// Package runtimecfg is the proxy's runtime bundle (ADR-0021 D1): the directory snapshot, the client
// key table and the upstream cluster set built for that directory, published together. A request
// acquires one bundle before it authenticates and uses only it to authenticate, route and sign, so
// an install that lands mid-request can never pair one version's routes with another version's
// keys or signers. The package has no etcd dependency; the member, the lab proxy and tests build
// bundles from what they already hold.
package runtimecfg

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// Bundle is one immutable runtime. Nothing in it is written after it is published: Keys is an
// auth.Table in production (a map the key store never writes again), Clusters a registry Set.
//
// A published bundle is reference counted: the publisher holds it while it is current, and each
// request that acquired it holds it until the request ends. Once replaced it is retired, and when
// its last holder releases it, it releases its cluster set, which closes the idle connections of
// transports no newer set uses.
type Bundle struct {
	Snapshot *directory.Snapshot
	Keys     sigv4.CredentialStore
	Clusters *upstream.Set

	refs atomic.Int64
	pub  *Publisher
}

// Identity is the lineage of the bundle's directory.
func (b *Bundle) Identity() directory.Identity { return b.Snapshot.File().Identity }

// Version is the bundle's directory version.
func (b *Bundle) Version() int64 { return b.Snapshot.Version() }

// Release ends a use that Acquire began.
func (b *Bundle) Release() {
	if b.refs.Add(-1) == 0 {
		b.pub.drained(b)
	}
}

// MaxRetired is how many replaced bundles requests may still hold before installs back up (ADR-0021
// D1: "initial limit eight"). Each holds a cluster set, and so its transports, open.
const MaxRetired = 8

// Stats is the publisher's state, for metrics and diagnostics.
type Stats struct {
	Version int64 // published
	Retired int   // replaced bundles a request still holds
	// Pending is the version of the newest bundle waiting for a retired one to drain, 0 when none.
	// While it is set, installs are backpressured: the proxy serves Version.
	Pending int64
}

// Publisher holds the current bundle. Acquire is an atomic load and a compare-and-swap, for every
// request; Publish is serialized and never moves the directory backwards within a lineage.
type Publisher struct {
	// Observe, if set, is called with the state after every change, under the publisher's lock:
	// it must not call back into the publisher. Set it before the publisher is shared.
	Observe func(Stats)
	// Published, if set, is called with each bundle as it becomes current, outside the lock: at
	// Publish, or later for a bundle that waited on the retired-bundle bound. The proxy's
	// admission gates follow it (ADR-0021 D2).
	Published func(*Bundle)

	mu      sync.Mutex // serializes Publish and retirement
	cur     atomic.Pointer[Bundle]
	retired int
	pending *Bundle
	limit   int
}

// NewPublisher returns a publisher holding b.
func NewPublisher(b *Bundle) *Publisher {
	p := &Publisher{limit: MaxRetired}
	if b.Clusters != nil && !b.Clusters.Retain() {
		panic("runtimecfg: the first bundle's cluster set is already released")
	}
	b.pub = p
	b.refs.Store(1)
	p.cur.Store(b)
	return p
}

// Load returns the current bundle without holding it, for reads that end before the next publish
// could matter (status, diagnostics); never nil. A request uses Acquire.
func (p *Publisher) Load() *Bundle { return p.cur.Load() }

// Acquire returns the current bundle, held until Release. Acquire and retirement interlock through
// the count: the publisher lets go of a bundle only after the next is current, so a bundle whose
// count already reached zero is never handed out; the load is simply retried.
func (p *Publisher) Acquire() *Bundle {
	for {
		b := p.cur.Load()
		n := b.refs.Load()
		if n > 0 && b.refs.CompareAndSwap(n, n+1) {
			return b
		}
	}
}

// Stats returns the publisher's state.
func (p *Publisher) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statsLocked()
}

func (p *Publisher) statsLocked() Stats {
	st := Stats{Version: p.cur.Load().Version(), Retired: p.retired}
	if p.pending != nil {
		st.Pending = p.pending.Version()
	}
	return st
}

// Refresh publishes a bundle of the three parts as they are now. The lab proxy calls it after its
// directory installs a version (the registry has committed that version's clusters by then) and
// after its key file changes; a member after each install.
func (p *Publisher) Refresh(snap *directory.Snapshot, keys sigv4.CredentialStore, clusters *upstream.Set) error {
	return p.Publish(&Bundle{Snapshot: snap, Keys: keys, Clusters: clusters})
}

// Publish makes b current. Within one lineage a bundle for an older directory version is refused:
// a slower install must not replace a newer one (T01). An equal version is allowed, since the lab's
// client keys change without a directory write. A bundle on another lineage is refused too, except
// the first identity a schema-1 directory takes; a restore's new epoch is re-enrollment (H4), not
// an install.
//
// When MaxRetired replaced bundles are still held, b is not published yet: it becomes the pending
// bundle, replacing an older pending one (installs coalesce), and is published when a retired
// bundle drains. Publish returns nil then; Stats reports the backpressure. In-flight requests are
// never cut short to make room.
func (p *Publisher) Publish(b *Bundle) error {
	p.mu.Lock()
	latest := p.cur.Load()
	if p.pending != nil {
		latest = p.pending
	}
	curID, nextID := latest.Identity(), b.Identity()
	switch {
	case !curID.IsZero() && curID != nextID:
		p.mu.Unlock()
		return fmt.Errorf("runtimecfg: bundle for cluster %s epoch %s refused: this proxy runs cluster %s epoch %s", nextID.ClusterID, nextID.Epoch, curID.ClusterID, curID.Epoch)
	case b.Version() < latest.Version():
		p.mu.Unlock()
		return fmt.Errorf("runtimecfg: bundle for directory version %d refused: version %d is published", b.Version(), latest.Version())
	case b.Clusters != nil && !b.Clusters.Retain():
		p.mu.Unlock()
		return fmt.Errorf("runtimecfg: bundle for directory version %d refused: its cluster set was replaced and released", b.Version())
	}
	b.pub = p
	b.refs.Store(1)
	var dropped *Bundle
	if p.pending != nil {
		dropped, p.pending = p.pending, nil
	}
	old := p.swapLocked(b)
	p.mu.Unlock()
	if dropped != nil {
		dropped.Clusters.Release()
	}
	if old != nil {
		old.Release()
		if p.Published != nil {
			p.Published(b)
		}
	}
	return nil
}

// swapLocked makes b current and returns the bundle it replaced, whose publisher reference the
// caller releases after unlocking, or parks b as pending at the bound and returns nil.
func (p *Publisher) swapLocked(b *Bundle) *Bundle {
	if p.retired >= p.limit {
		p.pending = b
		p.observeLocked()
		return nil
	}
	old := p.cur.Swap(b)
	p.retired++
	p.observeLocked()
	return old
}

// drained is a retired bundle's last release: it lets go of its cluster set and, if an install is
// waiting on the bound, publishes the pending bundle.
func (p *Publisher) drained(b *Bundle) {
	if b.Clusters != nil {
		b.Clusters.Release()
	}
	p.mu.Lock()
	p.retired--
	var old, next *Bundle
	if next = p.pending; next != nil {
		p.pending = nil
		old = p.swapLocked(next)
	} else {
		p.observeLocked()
	}
	p.mu.Unlock()
	if old != nil {
		old.Release()
		if p.Published != nil {
			p.Published(next)
		}
	}
}

func (p *Publisher) observeLocked() {
	if p.Observe != nil {
		p.Observe(p.statsLocked())
	}
}
