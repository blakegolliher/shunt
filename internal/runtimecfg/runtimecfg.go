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
type Bundle struct {
	Snapshot *directory.Snapshot
	Keys     sigv4.CredentialStore
	Clusters *upstream.Set
}

// Identity is the lineage of the bundle's directory.
func (b *Bundle) Identity() directory.Identity { return b.Snapshot.File().Identity }

// Version is the bundle's directory version.
func (b *Bundle) Version() int64 { return b.Snapshot.Version() }

// Publisher holds the current bundle. Load is one atomic pointer read, for every request; Publish
// is serialized and never moves the directory backwards within a lineage.
type Publisher struct {
	mu  sync.Mutex // serializes Publish
	cur atomic.Pointer[Bundle]
}

// NewPublisher returns a publisher holding b.
func NewPublisher(b *Bundle) *Publisher {
	p := &Publisher{}
	p.cur.Store(b)
	return p
}

// Load returns the current bundle; never nil once the publisher was made with one.
func (p *Publisher) Load() *Bundle { return p.cur.Load() }

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
func (p *Publisher) Publish(b *Bundle) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur := p.cur.Load(); cur != nil {
		curID, nextID := cur.Identity(), b.Identity()
		switch {
		case !curID.IsZero() && curID != nextID:
			return fmt.Errorf("runtimecfg: bundle for cluster %s epoch %s refused: this proxy runs cluster %s epoch %s", nextID.ClusterID, nextID.Epoch, curID.ClusterID, curID.Epoch)
		case b.Version() < cur.Version():
			return fmt.Errorf("runtimecfg: bundle for directory version %d refused: version %d is published", b.Version(), cur.Version())
		}
	}
	p.cur.Store(b)
	return nil
}
