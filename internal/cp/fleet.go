package cp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/control"
)

// The fleet table in etcd (ADR-0016 on ADR-0015's substrate; incarnations ADR-0021 D2):
//
//	/shunt/fleet/members/<id>   the member, persistent: written by its first heartbeat, deleted by
//	                            forget. It carries the member's current incarnation and the ones
//	                            that ended without retiring (memberRecord).
//	/shunt/fleet/proxies/<id>   its last heartbeat, on a lease: gone when the lease expires, so a
//	                            member whose key is absent is silent
//
// Membership outlives liveness on purpose: a bucket's first step waits for every member, and a
// partitioned member must not vanish from that check just because its lease expired. An
// incarnation outlives its process for the same reason: the work it dispatched may still land.
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

// maxResolvedKept bounds the resolved incarnations a member record keeps, for the diagnostics.
const maxResolvedKept = 4

type memberRecord struct {
	Joined time.Time `json:"joined"`
	// Current is the member's current incarnation: active, or retired until a new one registers.
	// Unresolved ended without retiring cleanly, newest last; Resolved were resolved by an
	// operator, the last few kept with their attestation.
	Current         *control.Incarnation  `json:"current,omitempty"`
	Unresolved      []control.Incarnation `json:"unresolved,omitempty"`
	Resolved        []control.Incarnation `json:"resolved,omitempty"`
	RetireRequested bool                  `json:"retire_requested,omitempty"`
}

type proxyRecord struct {
	control.Heartbeat
	Seen  time.Time `json:"seen"`
	Lease int64     `json:"lease"`
}

// cached is what this node last read of a member record: its revision, for the heartbeat's
// compare, and the incarnation it recorded, so the common heartbeat is one transaction.
type cached struct {
	rev    int64
	rec    memberRecord
	prevID string // the previous incarnation a heartbeat carried and this record holds
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
	known  map[string]cached           // proxy id → the member record as this node last read it
}

var _ control.Fleet = (*Fleet)(nil)

// NewFleet returns the fleet table. leaseTTL is what members are told to measure their staleness
// by; their keys live leaseTTL + dropMargin.
func NewFleet(cli *clientv3.Client, leaseTTL time.Duration) *Fleet {
	return &Fleet{cli: cli, leaseTTL: leaseTTL, now: time.Now, leases: map[string]clientv3.LeaseID{}, known: map[string]cached{}}
}

// Heartbeat implements control.Fleet. The common heartbeat (a known incarnation, nothing new to
// record) is one transaction compared on the member record's revision; a heartbeat that changes
// the record (a new incarnation, a previous one to record) reads and rewrites it.
func (f *Fleet) Heartbeat(ctx context.Context, id string, hb control.Heartbeat) (control.Grant, error) {
	f.mu.Lock()
	lease, held := f.leases[id]
	c, known := f.known[id]
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
			return control.Grant{}, fmt.Errorf("%w: lease: %w", control.ErrUnavailable, err)
		}
		lease = g.ID
	}
	rec, _ := json.Marshal(proxyRecord{Heartbeat: hb, Seen: f.now().UTC(), Lease: int64(lease)})
	put := clientv3.OpPut(kProxies+id, string(rec), clientv3.WithLease(lease))
	unchanged := known && c.rec.Current != nil && c.rec.Current.ID == hb.Incarnation && c.rec.Current.State == control.IncarnationActive &&
		(hb.Previous == nil || c.prevID == hb.Previous.ID)
	if unchanged {
		resp, err := f.cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(kMembers+id), "=", c.rev)).Then(put).Commit()
		if err != nil {
			return control.Grant{}, fmt.Errorf("%w: heartbeat: %w", control.ErrUnavailable, err)
		}
		if resp.Succeeded {
			f.mu.Lock()
			f.leases[id] = lease
			f.mu.Unlock()
			return control.Grant{LeaseTTL: f.leaseTTL, Retire: c.rec.RetireRequested, PreviousRecorded: hb.Previous != nil}, nil
		}
	}
	// The record is new, or changed since this node read it, or the heartbeat changes it.
	var grant control.Grant
	err := f.change(ctx, id, true, func(rec *memberRecord) error {
		now := f.now().UTC()
		if rec.Joined.IsZero() {
			rec.Joined = now
		}
		if cur := rec.Current; cur != nil && cur.ID != hb.Incarnation {
			if cur.State == control.IncarnationActive {
				// Replaced without retiring: a crash, or a stop whose retirement never got through.
				cur.State, cur.Ended = control.IncarnationUnclean, now
				if err := rec.addUnresolved(*cur); err != nil {
					return &control.RetirementError{Proxy: id, Incarnations: rec.Unresolved, What: "registering incarnation " + hb.Incarnation + " refused"}
				}
			}
			rec.Current = nil
		}
		if rec.Current == nil {
			rec.Current = &control.Incarnation{ID: hb.Incarnation, Started: hb.Started, State: control.IncarnationActive}
			rec.RetireRequested = false
		}
		if p := hb.Previous; p != nil && !rec.knows(p.ID) {
			// Evidence only the proxy held: a process that ended while the control plane was
			// unreachable. A clean retirement needs no record; an unclean one is unresolved.
			if p.State == control.IncarnationUnclean || p.Uncertain > 0 {
				inc := control.Incarnation{ID: p.ID, Started: p.Started, Ended: p.Ended, State: control.IncarnationUnclean, Uncertain: p.Uncertain}
				if inc.Ended.IsZero() {
					inc.Ended = now
				}
				if err := rec.addUnresolved(inc); err != nil {
					return &control.RetirementError{Proxy: id, Incarnations: rec.Unresolved, What: "recording incarnation " + p.ID + " refused"}
				}
			}
		}
		grant = control.Grant{LeaseTTL: f.leaseTTL, Retire: rec.RetireRequested, PreviousRecorded: hb.Previous != nil}
		return nil
	}, put)
	if err != nil {
		return control.Grant{}, err
	}
	f.mu.Lock()
	f.leases[id] = lease
	if hb.Previous != nil {
		c := f.known[id]
		c.prevID = hb.Previous.ID
		f.known[id] = c
	}
	f.mu.Unlock()
	return grant, nil
}

// knows reports whether the record holds incarnation id anywhere.
func (r *memberRecord) knows(id string) bool {
	if r.Current != nil && r.Current.ID == id {
		return true
	}
	for _, inc := range r.Unresolved {
		if inc.ID == id {
			return true
		}
	}
	for _, inc := range r.Resolved {
		if inc.ID == id {
			return true
		}
	}
	return false
}

// addUnresolved appends an incarnation that did not retire cleanly, refusing at the bound.
func (r *memberRecord) addUnresolved(inc control.Incarnation) error {
	if len(r.Unresolved) >= control.MaxUnresolvedIncarnations {
		return fmt.Errorf("%d unresolved incarnations", len(r.Unresolved))
	}
	r.Unresolved = append(r.Unresolved, inc)
	return nil
}

// change reads a member's record, applies fn, and writes it back compared on the revision read,
// with extra writes in the same transaction; create says a missing record is made. It retries a
// few times when another node wrote first.
func (f *Fleet) change(ctx context.Context, id string, create bool, fn func(rec *memberRecord) error, extra ...clientv3.Op) error {
	key := kMembers + id
	for attempt := 0; attempt < 8; attempt++ {
		resp, err := f.cli.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("%w: reading the fleet: %w", control.ErrUnavailable, err)
		}
		var rec memberRecord
		var rev int64
		if len(resp.Kvs) > 0 {
			if uerr := json.Unmarshal(resp.Kvs[0].Value, &rec); uerr != nil {
				return fmt.Errorf("member record %s: %w", id, uerr)
			}
			rev = resp.Kvs[0].ModRevision
		} else if !create {
			return control.NotFound("no proxy %s in the fleet", id)
		}
		if ferr := fn(&rec); ferr != nil {
			return ferr
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		cmp := clientv3.Compare(clientv3.ModRevision(key), "=", rev)
		if rev == 0 {
			cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		}
		then := append([]clientv3.Op{clientv3.OpPut(key, string(raw))}, extra...)
		tresp, err := f.cli.Txn(ctx).If(cmp).Then(then...).Commit()
		if err != nil {
			return fmt.Errorf("%w: writing the fleet: %w", control.ErrUnavailable, err)
		}
		if tresp.Succeeded {
			f.mu.Lock()
			c := f.known[id]
			c.rev, c.rec = tresp.Header.Revision, rec
			f.known[id] = c
			f.mu.Unlock()
			return nil
		}
	}
	return fmt.Errorf("%w: the fleet record of %s kept changing; retry", control.ErrUnavailable, id)
}

// Retire implements control.Fleet.
func (f *Fleet) Retire(ctx context.Context, id, incarnation string, uncertain int64) error {
	var extra []clientv3.Op
	err := f.change(ctx, id, false, func(rec *memberRecord) error {
		now := f.now().UTC()
		switch cur := rec.Current; {
		case cur != nil && cur.ID == incarnation:
			if cur.State == control.IncarnationRetired {
				return nil // a retried retirement
			}
			cur.Ended, cur.Uncertain = now, uncertain
			if uncertain > 0 {
				cur.State = control.IncarnationUnclean
				if err := rec.addUnresolved(*cur); err != nil {
					return &control.RetirementError{Proxy: id, Incarnations: rec.Unresolved, What: "recording the retirement refused"}
				}
				rec.Current = nil
			} else {
				cur.State = control.IncarnationRetired
			}
			rec.RetireRequested = false
			extra = []clientv3.Op{clientv3.OpDelete(kProxies + id)} // no longer live: its lease is done with
			return nil
		default:
			// A late retirement of an incarnation a newer one already replaced: a lost reply,
			// retried. Clean, it discharges the unresolved record; unclean, it stays.
			for i := range rec.Unresolved {
				if rec.Unresolved[i].ID != incarnation {
					continue
				}
				if uncertain == 0 {
					rec.Unresolved = slices.Delete(rec.Unresolved, i, i+1)
				} else {
					rec.Unresolved[i].Uncertain, rec.Unresolved[i].Ended = uncertain, now
				}
				return nil
			}
			if rec.knows(incarnation) {
				return nil
			}
			return control.Refuse("proxy %s has no incarnation %s to retire", id, incarnation)
		}
	})
	if err != nil {
		return err
	}
	if len(extra) > 0 {
		_, _ = f.cli.Do(ctx, extra[0]) //nolint:errcheck // the lease key expires by itself
		f.mu.Lock()
		delete(f.leases, id)
		f.mu.Unlock()
	}
	return nil
}

// RequestRetire implements control.Fleet.
func (f *Fleet) RequestRetire(ctx context.Context, id string) error {
	return f.change(ctx, id, false, func(rec *memberRecord) error {
		if rec.Current == nil || rec.Current.State != control.IncarnationActive {
			return control.Refuse("proxy %s has no running process to retire", id)
		}
		rec.RetireRequested = true
		return nil
	})
}

// Resolve implements control.Fleet.
func (f *Fleet) Resolve(ctx context.Context, id, incarnation, attestation, actor string) error {
	return f.change(ctx, id, false, func(rec *memberRecord) error {
		now := f.now().UTC()
		resolved := func(inc control.Incarnation) {
			inc.State, inc.Attestation, inc.ResolvedBy, inc.ResolvedAt = control.IncarnationResolved, attestation, actor, now
			rec.Resolved = append(rec.Resolved, inc)
			if len(rec.Resolved) > maxResolvedKept {
				rec.Resolved = rec.Resolved[len(rec.Resolved)-maxResolvedKept:]
			}
		}
		for i := range rec.Unresolved {
			if rec.Unresolved[i].ID == incarnation {
				inc := rec.Unresolved[i]
				rec.Unresolved = slices.Delete(rec.Unresolved, i, i+1)
				resolved(inc)
				return nil
			}
		}
		if cur := rec.Current; cur != nil && cur.ID == incarnation && cur.State == control.IncarnationActive {
			// A process that crashed and was never replaced: resolving it means the operator has
			// established it is stopped. It must not be live: a heartbeating process is not stopped.
			resp, err := f.cli.Get(ctx, kProxies+id, clientv3.WithCountOnly())
			if err != nil {
				return fmt.Errorf("%w: reading the fleet: %w", control.ErrUnavailable, err)
			}
			if resp.Count > 0 {
				return control.Refuse("proxy %s's incarnation %s is live (its lease has not expired): a running process is retired, not resolved", id, incarnation)
			}
			inc := *cur
			inc.Ended = now
			rec.Current = nil
			resolved(inc)
			return nil
		}
		if rec.knows(incarnation) {
			return control.Refuse("proxy %s's incarnation %s is not unresolved", id, incarnation)
		}
		return control.NotFound("proxy %s has no incarnation %s", id, incarnation)
	})
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
			m := member(strings.TrimPrefix(key, kMembers))
			var rec memberRecord
			if json.Unmarshal(kv.Value, &rec) != nil {
				continue
			}
			m.Incarnation, m.Unresolved, m.RetireRequested = rec.Current, rec.Unresolved, rec.RetireRequested
		case strings.HasPrefix(key, kProxies):
			var rec proxyRecord
			if json.Unmarshal(kv.Value, &rec) != nil {
				continue
			}
			m := member(strings.TrimPrefix(key, kProxies))
			m.Live, m.Applied, m.Seq, m.Started, m.Seen, m.FallbackReads = true, rec.Applied, rec.Seq, rec.Started, rec.Seen, rec.FallbackReads
			m.Host, m.Version, m.Telemetry, m.Identity, m.Secrets, m.Durable = rec.Host, rec.Version, rec.Telemetry, rec.Identity, rec.Secrets, rec.Durable
			m.Installed, m.CacheError, m.Uncertain = rec.Installed, rec.CacheError, rec.Uncertain
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

// Forget implements control.Fleet: refused while the member's lease is live, and while it has an
// incarnation that did not retire cleanly, which forgetting would take for gone.
func (f *Fleet) Forget(ctx context.Context, id string) error {
	for attempt := 0; attempt < 8; attempt++ {
		resp, err := f.cli.Get(ctx, kMembers+id)
		if err != nil {
			return fmt.Errorf("%w: forget: %w", control.ErrUnavailable, err)
		}
		if len(resp.Kvs) == 0 {
			return control.NotFound("no proxy %s in the fleet", id)
		}
		var rec memberRecord
		if uerr := json.Unmarshal(resp.Kvs[0].Value, &rec); uerr != nil {
			return fmt.Errorf("member record %s: %w", id, uerr)
		}
		live, err := f.cli.Get(ctx, kProxies+id, clientv3.WithCountOnly())
		if err != nil {
			return fmt.Errorf("%w: forget: %w", control.ErrUnavailable, err)
		}
		if live.Count > 0 {
			return control.Refuse("proxy %s is live (its lease has not expired); stop it first, then forget it. A running proxy re-joins on its next heartbeat", id)
		}
		var unproven []control.Incarnation
		if cur := rec.Current; cur != nil && cur.State == control.IncarnationActive {
			unproven = append(unproven, *cur)
		}
		unproven = append(unproven, rec.Unresolved...)
		if len(unproven) > 0 {
			return &control.RetirementError{Proxy: id, Incarnations: unproven, What: "forgetting proxy " + id + " refused"}
		}
		tresp, err := f.cli.Txn(ctx).If(
			clientv3.Compare(clientv3.CreateRevision(kProxies+id), "=", 0),
			clientv3.Compare(clientv3.ModRevision(kMembers+id), "=", resp.Kvs[0].ModRevision),
		).Then(clientv3.OpDelete(kMembers + id)).Commit()
		if err != nil {
			return fmt.Errorf("%w: forget: %w", control.ErrUnavailable, err)
		}
		if !tresp.Succeeded {
			continue // the record changed under the read, or the proxy came back
		}
		f.mu.Lock()
		delete(f.known, id)
		delete(f.leases, id)
		f.mu.Unlock()
		return nil
	}
	return fmt.Errorf("%w: the fleet record of %s kept changing; retry", control.ErrUnavailable, id)
}
