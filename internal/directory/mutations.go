package directory

import (
	"fmt"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
)

// The operator mutations, as changes to a File in memory. FileDir applies them inside its
// lock-and-rewrite; the control plane's store (internal/cp) applies them to its watch cache and
// writes the difference as one etcd transaction. Both stores share these so that a rule about
// what a change may do lives in one place.

// Create adds an ACTIVE placement on cluster under the backend bucket name, for a tenant the
// directory already knows.
func (f *File) Create(tenant, bucket, cluster, backend string, now time.Time) error {
	k := Key(tenant, bucket)
	if _, ok := f.Placements[k]; ok {
		return ErrExists
	}
	if _, ok := f.Tenants[tenant]; !ok {
		return fmt.Errorf("%w: tenant %q is not in the directory", ErrNotFound, tenant)
	}
	f.ensureMaps()
	f.Placements[k] = Placement{
		State: StateActive, Primary: cluster, Names: map[string]string{cluster: backend},
		Created: now.UTC().Truncate(time.Millisecond),
	}
	return nil
}

// Delete removes an ACTIVE placement.
func (f *File) Delete(tenant, bucket string) error {
	k := Key(tenant, bucket)
	p, ok := f.Placements[k]
	if !ok {
		return ErrNotFound
	}
	if p.State != StateActive {
		return fmt.Errorf("%w: %s is %s; only an ACTIVE placement can be deleted", ErrConflict, k, p.State)
	}
	delete(f.Placements, k)
	return nil
}

// SetTenantDefault repoints a tenant's default cluster: the cluster its next CreateBucket lands on.
func (f *File) SetTenantDefault(tenant, cluster string) error {
	t, ok := f.Tenants[tenant]
	if !ok {
		return fmt.Errorf("%w: tenant %q is not in the directory", ErrNotFound, tenant)
	}
	if _, ok := f.Clusters[cluster]; !ok {
		return fmt.Errorf("%w: cluster %q is not in the directory", ErrNotFound, cluster)
	}
	if t.DefaultCluster == cluster {
		return fmt.Errorf("%w: %s already defaults to %s", ErrConflict, tenant, cluster)
	}
	t.DefaultCluster = cluster
	f.Tenants[tenant] = t
	return nil
}

// SetState applies a transition to a placement that is still in state from.
func (f *File) SetState(tenant, bucket, from string, t Transition) error {
	k := Key(tenant, bucket)
	p, ok := f.Placements[k]
	if !ok {
		return ErrNotFound
	}
	if p.State != from {
		return fmt.Errorf("%w: %s is %s in the directory, expected %s", ErrConflict, k, p.State, from)
	}
	np, err := Apply(p, t)
	if err != nil {
		return err
	}
	f.Placements[k] = np
	return nil
}

// SetPlacementReadOnly updates the maintenance switch without changing migration state.
func (f *File) SetPlacementReadOnly(tenant, bucket string, readOnly, reject bool) error {
	k := Key(tenant, bucket)
	p, ok := f.Placements[k]
	if !ok {
		return fmt.Errorf("%w: no bucket %s in the directory", ErrNotFound, k)
	}
	if p.ReadOnly == readOnly && p.RejectWrites == (readOnly && reject) {
		return fmt.Errorf("%w: %s read-only is already %t", ErrConflict, k, readOnly)
	}
	p.ReadOnly, p.RejectWrites = readOnly, readOnly && reject
	f.Placements[k] = p
	return nil
}

// SetClusterReadOnly updates the maintenance switch on one backend.
func (f *File) SetClusterReadOnly(name string, readOnly, reject bool) error {
	c, ok := f.Clusters[name]
	if !ok {
		return fmt.Errorf("%w: cluster %q is not in the directory", ErrNotFound, name)
	}
	if c.ReadOnly == readOnly && c.RejectWrites == (readOnly && reject) {
		return fmt.Errorf("%w: cluster %s read-only is already %t", ErrConflict, name, readOnly)
	}
	c.ReadOnly, c.RejectWrites = readOnly, readOnly && reject
	f.Clusters[name] = c
	return nil
}

// PutCluster adds a cluster, or replaces its definition.
func (f *File) PutCluster(name string, c config.Cluster) {
	f.ensureMaps()
	f.Clusters[name] = c
	config.ApplyClusterDefaults(f.Clusters)
}

// RemoveCluster deletes a cluster no tenant or placement references.
func (f *File) RemoveCluster(name string) error {
	if _, ok := f.Clusters[name]; !ok {
		return fmt.Errorf("%w: cluster %q is not in the directory", ErrNotFound, name)
	}
	if refs := References(f, name); len(refs) > 0 {
		return fmt.Errorf("%w: cluster %q is still referenced by %s", ErrInUse, name, strings.Join(refs, ", "))
	}
	delete(f.Clusters, name)
	return nil
}

// Adopt writes an ACTIVE placement for a bucket that already exists on cluster under backend,
// creating the tenant (defaulting to cluster) if it is not in the directory yet (docs/DESIGN.md §11).
func (f *File) Adopt(tenant, bucket, cluster, backend string, now time.Time) error {
	k := Key(tenant, bucket)
	if _, ok := f.Placements[k]; ok {
		return ErrExists
	}
	f.ensureMaps()
	if _, ok := f.Tenants[tenant]; !ok {
		f.Tenants[tenant] = Tenant{DefaultCluster: cluster}
	}
	f.Placements[k] = Placement{
		State: StateActive, Primary: cluster, Names: map[string]string{cluster: backend},
		Created: now.UTC().Truncate(time.Millisecond),
	}
	return nil
}

// CreateSpread writes an ACTIVE placement spread over legs, one per cluster, owning equal ranges of
// the key hash space in the order given (ADR-0018 N2). Each leg is named after its cluster. A new
// tenant gets the first leg's cluster as its default.
func (f *File) CreateSpread(tenant, bucket string, legs []Leg, now time.Time) error {
	k := Key(tenant, bucket)
	if _, ok := f.Placements[k]; ok {
		return ErrExists
	}
	if len(legs) < 2 || len(legs) > MaxLegs {
		return fmt.Errorf("%w: a spread bucket has 2 to %d legs, not %d", ErrConflict, MaxLegs, len(legs))
	}
	byID := make(map[string]Leg, len(legs))
	ids := make([]string, 0, len(legs))
	for _, l := range legs {
		if _, dup := byID[l.Cluster]; dup {
			return fmt.Errorf("%w: cluster %s is named twice; a new bucket spreads over different clusters, since splitting one cluster's keys over two of its buckets gains nothing", ErrConflict, l.Cluster)
		}
		byID[l.Cluster] = l
		ids = append(ids, l.Cluster)
	}
	f.ensureMaps()
	if _, ok := f.Tenants[tenant]; !ok {
		f.Tenants[tenant] = Tenant{DefaultCluster: legs[0].Cluster}
	}
	f.Placements[k] = Placement{State: StateActive, Legs: byID, Owners: EvenOwners(ids), KeyHash: RampHash,
		Created: now.UTC().Truncate(time.Millisecond)}
	return nil
}

// SetTarget records the cluster and backend bucket an ACTIVE placement will move to (shunt expand).
func (f *File) SetTarget(tenant, bucket, cluster, backend string) error {
	k := Key(tenant, bucket)
	p, ok := f.Placements[k]
	if !ok {
		return ErrNotFound
	}
	if p.State != StateActive {
		return fmt.Errorf("%w: %s is %s; expand prepares an ACTIVE placement", ErrConflict, k, p.State)
	}
	if p.Spread() {
		return fmt.Errorf("%w: %s is spread over %d legs; expanding it is ADR-0018 N3", ErrConflict, k, len(p.Legs))
	}
	if p.Target != "" && p.Target != cluster {
		return fmt.Errorf("%w: %s is already expanded to %s", ErrConflict, k, p.Target)
	}
	np := p.clone()
	np.Target = cluster
	if np.Names == nil {
		np.Names = map[string]string{}
	}
	np.Names[cluster] = backend
	f.Placements[k] = np
	return nil
}

// ClearTarget forgets the cluster `shunt expand` prepared, while no step has used it: the placement
// stays ACTIVE where it is. For a spread bucket it retires the legs that own nothing. The backend
// bucket is left as it is; shunt may not have created it.
func (f *File) ClearTarget(tenant, bucket string) error {
	k := Key(tenant, bucket)
	p, ok := f.Placements[k]
	if !ok {
		return ErrNotFound
	}
	if p.State != StateActive {
		return fmt.Errorf("%w: %s is %s; a target can only be cleared before the first step", ErrConflict, k, p.State)
	}
	if p.Spread() {
		// A spread bucket's prepared destinations are its legs that own nothing (a move released
		// before it routed anything leaves one): retiring them is directory-only (ADR-0018 N3c).
		idle := IdleLegs(p)
		if len(idle) == 0 {
			return fmt.Errorf("%w: every leg of %s owns keys; there is no idle leg to retire", ErrConflict, k)
		}
		np := p.clone()
		for _, id := range idle {
			delete(np.Legs, id)
		}
		f.Placements[k] = settle(np)
		return nil
	}
	if p.Target == "" {
		return fmt.Errorf("%w: %s has no target to clear", ErrConflict, k)
	}
	np := p.clone()
	if np.Target != np.Cold {
		delete(np.Names, np.Target)
	}
	np.Target = ""
	f.Placements[k] = np
	return nil
}

func (f *File) ensureMaps() {
	if f.Clusters == nil {
		f.Clusters = map[string]config.Cluster{}
	}
	if f.Tenants == nil {
		f.Tenants = map[string]Tenant{}
	}
	if f.Placements == nil {
		f.Placements = map[string]Placement{}
	}
}

// Clone returns a deep copy.
func (f *File) Clone() *File { return f.clone() }
