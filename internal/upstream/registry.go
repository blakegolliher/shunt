package upstream

import (
	"fmt"
	"maps"
	"net/http"
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
	return r.BuildWith(name, def, "")
}

// BuildWith is Build with the secret given rather than resolved: `cluster add` checking a secret
// the operator just typed, before any store holds it.
func (r *Registry) BuildWith(name string, def config.Cluster, secret string) (*Cluster, error) {
	cl, err := New(name, def, r.opts)
	if err != nil {
		return nil, fmt.Errorf("cluster %s: %w", name, err)
	}
	if secret != "" {
		cl.Creds.Secret = secret
		return cl, nil
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

// Apply makes clusters the live set: Prepare with the registry's own resolver, then Commit.
func (r *Registry) Apply(clusters map[string]config.Cluster) (added, removed []string, err error) {
	c, err := r.Prepare(clusters, nil)
	if err != nil {
		return nil, nil, err
	}
	added, removed = c.Commit()
	return added, removed, nil
}

// Candidate is a Set that Prepare built and nothing serves from yet. Commit makes it live; a
// candidate that is never committed is dropped, and the live set is as it was.
type Candidate struct {
	r     *Registry
	next  *Set
	defs  map[string]config.Cluster
	added []string
}

// Prepare builds the Set clusters describe without making it live. A cluster whose definition
// and secret are unchanged keeps its *Cluster, transport and pooled connections; one whose secret
// alone changed gets a new *Cluster that signs with the new secret and shares the old transport;
// a new or otherwise changed one is built. resolve turns a secret_ref into the secret for this
// candidate only (nil: the registry's own resolver), so a candidate's secrets never reach the live
// set before Commit. Any cluster failing fails the whole candidate.
func (r *Registry) Prepare(clusters map[string]config.Cluster, resolve func(ref string) (string, error)) (*Candidate, error) {
	return r.PrepareWith(clusters, resolve, nil)
}

// PrepareWith is Prepare with each cluster's secret generation (Cluster.SecretGeneration) from
// generation; nil leaves them 0. A cluster whose secret generation moved is a rotation even when the
// secret string did not change: its signer is rebuilt over the same transport.
func (r *Registry) PrepareWith(clusters map[string]config.Cluster, resolve func(ref string) (string, error), generation func(name string) int64) (*Candidate, error) {
	if resolve == nil {
		resolve = r.resolve
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.cur.Load()
	c := &Candidate{r: r, next: &Set{byName: map[string]*Cluster{}, byID: map[string]*Cluster{}, names: make([]string, 0, len(clusters))}, defs: maps.Clone(clusters)}
	for _, name := range slices.Sorted(maps.Keys(clusters)) {
		def := clusters[name]
		var secret string
		if resolve != nil {
			s, err := resolve(def.Credentials.SecretRef)
			if err != nil {
				return nil, fmt.Errorf("cluster %s: %w", name, err)
			}
			secret = s
		}
		var gen int64
		if generation != nil {
			gen = generation(name)
		}
		cl, ok := old.byName[name]
		switch {
		case ok && reflect.DeepEqual(r.defs[name], def) && cl.Creds.Secret == secret && cl.SecretGeneration == gen:
		case ok && reflect.DeepEqual(r.defs[name], def):
			cl = cl.withSecret(secret, gen)
			c.added = append(c.added, name)
		default:
			var err error
			if cl, err = New(name, def, r.opts); err != nil {
				return nil, fmt.Errorf("cluster %s: %w", name, err)
			}
			cl.Creds.Secret, cl.SecretGeneration = secret, gen
			c.added = append(c.added, name)
		}
		if other, dup := c.next.byID[cl.ID]; dup {
			return nil, fmt.Errorf("clusters %s and %s derive the same id %s; rename one", other.Name, name, cl.ID)
		}
		c.next.byName[name], c.next.byID[cl.ID] = cl, cl
		c.next.names = append(c.next.names, name)
	}
	return c, nil
}

// Commit makes the candidate the live set and returns the names added (new or changed) and
// removed. Clusters that dropped out or were replaced have their idle connections closed, unless
// the live set still uses their transport. A request that loaded the old set keeps its *Cluster
// pointers, and so its secrets, to the end.
func (c *Candidate) Commit() (added, removed []string) {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.cur.Load()
	r.cur.Store(c.next)
	r.defs = c.defs
	inUse := make(map[*http.Transport]bool, len(c.next.byName))
	for _, cl := range c.next.byName {
		inUse[cl.Transport] = true
	}
	for name, cl := range old.byName {
		if now, ok := c.next.byName[name]; !ok || now != cl {
			if !ok {
				removed = append(removed, name)
			}
			if !inUse[cl.Transport] {
				cl.Close()
			}
		}
	}
	slices.Sort(removed)
	return c.added, removed
}
