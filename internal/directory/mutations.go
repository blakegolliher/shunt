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
