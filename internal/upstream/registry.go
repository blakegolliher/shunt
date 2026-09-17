package upstream

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/blakegolliher/shunt/internal/config"
)

// Registry is the live set of clusters in resign mode. Clusters are directory state (ADR-0008), so
// they change while shunt serves: Apply builds the next Set and swaps it in atomically. A request
// loads the Set once and keeps its *Cluster pointers to the end, so a swap never disturbs a
// request in flight.
type Registry struct {
	opts    Options
	resolve func(ref string) (string, error)

	cur  atomic.Pointer[Set]
	mu   sync.Mutex // serializes Apply
	defs map[string]config.Cluster
}

// NewRegistry returns a Registry holding no clusters. resolve turns a secret_ref into the secret.
func NewRegistry(o Options, resolve func(ref string) (string, error)) *Registry {
	r := &Registry{opts: o, resolve: resolve, defs: map[string]config.Cluster{}}
	r.cur.Store(&Set{byName: map[string]*Cluster{}, byID: map[string]*Cluster{}, names: []string{}})
	return r
}

// Close releases the idle connections of every live cluster.
func (r *Registry) Close() { r.cur.Load().Close() }

// Load returns the current Set. It is never nil and must not be modified.
func (r *Registry) Load() *Set { return r.cur.Load() }

// Build makes a cluster from its definition exactly as Apply would, secret resolved in this process,
// without making it live. The caller closes it.
func (r *Registry) Build(name string, def config.Cluster) (*Cluster, error) {
	cl, err := New(name, def, r.opts)
	if err != nil {
		return nil, fmt.Errorf("cluster %s: %w", name, err)
	}
	if r.resolve != nil {
		secret, rerr := r.resolve(def.Credentials.SecretRef)
		if rerr != nil {
			cl.Close()
			return nil, fmt.Errorf("cluster %s: %w", name, rerr)
		}
		cl.Creds.Secret = secret
	}
	return cl, nil
}

// Apply makes clusters the live set. A cluster whose definition is unchanged keeps its *Cluster,
// transport and pooled connections; a new or changed one is built and its secret resolved. Nothing
// is swapped if any cluster fails, so a definition the proxy cannot sign for never goes live.
// Clusters that dropped out, or were replaced, have their idle connections closed after the swap.
// It returns the names added (new or changed) and removed.
func (r *Registry) Apply(clusters map[string]config.Cluster) (added, removed []string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.cur.Load()
	next := &Set{byName: map[string]*Cluster{}, byID: map[string]*Cluster{}, names: make([]string, 0, len(clusters))}
	var built []*Cluster
	fail := func(e error) ([]string, []string, error) {
		for _, c := range built {
			c.Close()
		}
		return nil, nil, e
	}
	for _, name := range slices.Sorted(maps.Keys(clusters)) {
		def := clusters[name]
		cl, reuse := old.byName[name]
		if !reuse || !reflect.DeepEqual(r.defs[name], def) {
			cl, err = r.Build(name, def)
			if err != nil {
				return fail(err)
			}
			built = append(built, cl)
			added = append(added, name)
		}
		if other, dup := next.byID[cl.ID]; dup {
			return fail(fmt.Errorf("clusters %s and %s derive the same id %s; rename one", other.Name, name, cl.ID))
		}
		next.byName[name], next.byID[cl.ID] = cl, cl
		next.names = append(next.names, name)
	}
	r.cur.Store(next)
	r.defs = maps.Clone(clusters)
	for name, cl := range old.byName {
		if now, ok := next.byName[name]; !ok || now != cl {
			if !ok {
				removed = append(removed, name)
			}
			cl.Close()
		}
	}
	slices.Sort(removed)
	return added, removed, nil
}
