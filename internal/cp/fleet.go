package cp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/control"
)

// The fleet table in etcd (ADR-0016 on ADR-0015's substrate):
//
//	/shunt/fleet/members/<id>   the member, persistent: written by its first heartbeat, deleted by forget
//	/shunt/fleet/proxies/<id>   its last heartbeat, on a lease: gone when the lease expires, so a
//	                            member whose key is absent is silent
//
// Membership outlives liveness on purpose: a bucket's first step waits for every member, and a
// partitioned member must not vanish from that check just because its lease expired.
const (
	fleetPrefix = "/shunt/fleet/"
	kMembers    = fleetPrefix + "members/"
	kProxies    = fleetPrefix + "proxies/"
)

// defaultDropMargin is how far past its lease a silent member's key stays (Fleet.DropMargin). A
// member stops routing writes on moving buckets when its lease lapses, measured from when it sent
// its last acknowledged heartbeat; the key it left behind lasts this much longer, so the control
// plane never drops a member before the member has stopped itself.
const defaultDropMargin = 5 * time.Second

type memberRecord struct {
	Joined time.Time `json:"joined"`
}

type proxyRecord struct {
	control.Heartbeat
	Seen  time.Time `json:"seen"`
	Lease int64     `json:"lease"`
}

// Fleet implements control.Fleet on etcd.
type Fleet struct {
	cli      *clientv3.Client
	leaseTTL time.Duration
	now      func() time.Time
	// DropMargin: how far past its lease a member's key stays; default 5s. Tests shorten it.
	DropMargin time.Duration

	mu     sync.Mutex
	leases map[string]clientv3.LeaseID // proxy id → the lease this node last renewed for it
	known  map[string]bool             // ids whose membership this node has written or seen
}

var _ control.Fleet = (*Fleet)(nil)

// NewFleet returns the fleet table. leaseTTL is what members are told to measure their staleness
// by; their keys live leaseTTL + dropMargin.
func NewFleet(cli *clientv3.Client, leaseTTL time.Duration) *Fleet {
	return &Fleet{cli: cli, leaseTTL: leaseTTL, now: time.Now, leases: map[string]clientv3.LeaseID{}, known: map[string]bool{}}
}

// Heartbeat implements control.Fleet.
func (f *Fleet) Heartbeat(ctx context.Context, id string, hb control.Heartbeat) (time.Duration, error) {
	f.mu.Lock()
	lease, held := f.leases[id]
	known := f.known[id]
	f.mu.Unlock()
	if held {
		if _, err := f.cli.KeepAliveOnce(ctx, lease); err != nil {
			held = false // expired, or granted by another node: a fresh one below
		}
	}
	if !held {
		margin := f.DropMargin
		if margin <= 0 {
			margin = defaultDropMargin
		}
		ttl := int64(math.Ceil((f.leaseTTL + margin).Seconds()))
		g, err := f.cli.Grant(ctx, ttl)
		if err != nil {
			return 0, fmt.Errorf("%w: lease: %w", control.ErrUnavailable, err)
		}
		lease = g.ID
	}
	rec, _ := json.Marshal(proxyRecord{Heartbeat: hb, Seen: f.now().UTC(), Lease: int64(lease)})
	ops := []clientv3.Op{clientv3.OpPut(kProxies+id, string(rec), clientv3.WithLease(lease))}
	if !known {
		// The first heartbeat makes it a member; the record is written once and left alone.
		joined, _ := json.Marshal(memberRecord{Joined: f.now().UTC()})
		txn := f.cli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(kMembers+id), "=", 0)).
			Then(clientv3.OpPut(kMembers+id, string(joined)), ops[0]).Else(ops[0])
		if _, err := txn.Commit(); err != nil {
			return 0, fmt.Errorf("%w: heartbeat: %w", control.ErrUnavailable, err)
		}
	} else if _, err := f.cli.Do(ctx, ops[0]); err != nil {
		return 0, fmt.Errorf("%w: heartbeat: %w", control.ErrUnavailable, err)
	}
	f.mu.Lock()
	f.leases[id], f.known[id] = lease, true
	f.mu.Unlock()
	return f.leaseTTL, nil
}

// Members implements control.Fleet.
func (f *Fleet) Members(ctx context.Context) ([]control.Member, error) {
	resp, err := f.cli.Get(ctx, fleetPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("%w: reading the fleet: %w", control.ErrUnavailable, err)
	}
	byID := map[string]*control.Member{}
	member := func(id string) *control.Member {
		m, ok := byID[id]
		if !ok {
			m = &control.Member{ID: id}
			byID[id] = m
		}
		return m
	}
	now := f.now()
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		switch {
		case strings.HasPrefix(key, kMembers):
			member(strings.TrimPrefix(key, kMembers))
		case strings.HasPrefix(key, kProxies):
			var rec proxyRecord
			if json.Unmarshal(kv.Value, &rec) != nil {
				continue
			}
			m := member(strings.TrimPrefix(key, kProxies))
			m.Live, m.Applied, m.Seq, m.Started, m.Seen, m.FallbackReads = true, rec.Applied, rec.Seq, rec.Started, rec.Seen, rec.FallbackReads
			m.Host, m.Version, m.Telemetry, m.Identity, m.Secrets, m.Durable = rec.Host, rec.Version, rec.Telemetry, rec.Identity, rec.Secrets, rec.Durable
			m.Installed, m.CacheError = rec.Installed, rec.CacheError
			if !rec.Seen.IsZero() {
				m.SinceSeen = now.Sub(rec.Seen)
			}
		}
	}
	out := make([]control.Member, 0, len(byID))
	for _, m := range byID {
		out = append(out, *m)
	}
	return out, nil
}

// Forget implements control.Fleet: refused while the member's lease is live.
func (f *Fleet) Forget(ctx context.Context, id string) error {
	resp, err := f.cli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(kProxies+id), "=", 0)).
		Then(clientv3.OpDelete(kMembers + id)).Commit()
	if err != nil {
		return fmt.Errorf("%w: forget: %w", control.ErrUnavailable, err)
	}
	if !resp.Succeeded {
		return control.Refuse("proxy %s is live (its lease has not expired); stop it first, then forget it. A running proxy re-joins on its next heartbeat", id)
	}
	if resp.Responses[0].GetResponseDeleteRange().Deleted == 0 {
		return control.NotFound("no proxy %s in the fleet", id)
	}
	f.mu.Lock()
	delete(f.known, id)
	delete(f.leases, id)
	f.mu.Unlock()
	return nil
}
