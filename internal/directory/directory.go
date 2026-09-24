package directory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/config"
)

// Placement states (docs/DESIGN.md §2.5).
const (
	StateActive    = "ACTIVE"
	StateRamping   = "RAMPING"
	StateMigrating = "MIGRATING"
	StateCutover   = "CUTOVER"
)

// States lists every state in lifecycle order.
var States = []string{StateActive, StateRamping, StateMigrating, StateCutover}

// Tenant is a customer namespace.
type Tenant struct {
	DefaultCluster string `yaml:"default_cluster" json:"default_cluster"`
}

// Ramp is the deterministic write shift during RAMPING (decision 15).
type Ramp struct {
	// Hash names the function that splits keys by Ratio. It is written when the ramp starts and
	// never changes for the life of the ramp: a proxy that does not implement the named hash
	// refuses to route the ramp rather than split keys differently (ADR-0004, POC-5 amendment).
	Hash     string   `yaml:"hash" json:"hash"`
	Ratio    float64  `yaml:"ratio,omitempty" json:"ratio,omitempty"`
	Prefixes []string `yaml:"prefixes,omitempty" json:"prefixes,omitempty"`
	// Range limits the ramp to the keys whose hash it holds, and makes Ratio a share of it: the
	// ramp of a move of part of a bucket (ADR-0018 N3). Nil is every key, as before N3.
	Range *HashRange `yaml:"range,omitempty" json:"range,omitempty"`
	// Hold is a ramp step that has been written but is not in force yet (ADR-0016). A key inside
	// Hold but outside the ramp is held: its writes are refused with 503 until the step reaches
	// every proxy, so no proxy writes it to the target while another still writes it to the source.
	// Split by the same Hash.
	Hold *RampHold `yaml:"hold,omitempty" json:"hold,omitempty"`
}

// RampHold is the ramp a held step moves to: a ratio, prefixes, or both.
type RampHold struct {
	Ratio    float64  `yaml:"ratio,omitempty" json:"ratio,omitempty"`
	Prefixes []string `yaml:"prefixes,omitempty" json:"prefixes,omitempty"`
}

// RampHash is the ramp hash this build writes and routes by: FNV-1a 64 over the key, finished with
// murmur3's fmix64 (internal/migrate.InRange). Any change to that function takes a new name.
const RampHash = "fnv1a-fmix64-v1"

// CutoverEvidence records what `shunt cutover` checked before it let go of the source: the mover
// had converged, and this proxy served no fallback read over Window (ADR-0004). purge-source
// refuses a placement without it.
type CutoverEvidence struct {
	At            time.Time     `yaml:"at" json:"at"`
	Window        time.Duration `yaml:"window" json:"window"`
	FallbackReads float64       `yaml:"fallback_reads" json:"fallback_reads"` // the counter, unchanged across the window
}

// Placement maps one (tenant, bucket) to clusters and a state.
type Placement struct {
	State   string `yaml:"state" json:"state"`
	Primary string `yaml:"primary,omitempty" json:"primary,omitempty"` // always set in v1; empty in v2
	Source  string `yaml:"source,omitempty" json:"source,omitempty"`
	// Target is the cluster `shunt expand` prepared for the move, recorded while ACTIVE so the
	// first ramp or migrate step needs no --to. Its backend name is in Names.
	Target       string            `yaml:"target,omitempty" json:"target,omitempty"`
	Cutover      *CutoverEvidence  `yaml:"cutover,omitempty" json:"cutover,omitempty"`
	Ramp         *Ramp             `yaml:"ramp,omitempty" json:"ramp,omitempty"`
	Names        map[string]string `yaml:"names,omitempty" json:"names,omitempty"` // cluster → backend bucket name; v1 only
	Cold         string            `yaml:"cold,omitempty" json:"cold,omitempty"`
	Tier         string            `yaml:"tier,omitempty" json:"tier,omitempty"` // native | emulated
	Lifecycle    string            `yaml:"lifecycle,omitempty" json:"lifecycle,omitempty"`
	Created      time.Time         `yaml:"created,omitempty" json:"created,omitzero"`
	ReadOnly     bool              `yaml:"read_only,omitempty" json:"read_only,omitempty"`
	RejectWrites bool              `yaml:"reject_writes,omitempty" json:"reject_writes,omitempty"`
	// Watch asks proxies for this bucket's traffic by backend cluster in telemetry, as they give it
	// for every bucket spread over legs or moving; bounded per window (telemetry.MaxBucketsPerWindow).
	Watch bool `yaml:"watch,omitempty" json:"watch,omitempty"`
	// Barrier is the drain barrier of a change in progress on this placement (ADR-0021 D2): a held
	// ramp step or a read-only change closes mutations, purge-source closes source-dependent work.
	// Proxies close the gate it names on install and report the drain in their heartbeats; the
	// control node clears it with the change's commit. At most one per placement.
	Barrier *Barrier `yaml:"barrier,omitempty" json:"barrier,omitempty"`

	// Schema v2 (ADR-0018, legs.go). Only a decoder sets these, and it converts them into the v1
	// fields above before anything else sees the placement, so outside legs.go they are always
	// empty. In v2, Target and Cold name legs instead of clusters.
	Legs    map[string]Leg `yaml:"legs,omitempty" json:"legs,omitempty"`
	Owners  []Owner        `yaml:"owners,omitempty" json:"owners,omitempty"`
	KeyHash string         `yaml:"hash,omitempty" json:"hash,omitempty"` // splits the key space among Owners
	Move    *Move          `yaml:"move,omitempty" json:"move,omitempty"`
	// Prefixes are prefix rules (ADR-0020, scopes.go): scopes of keys owned by their own table.
	Prefixes []PrefixRule `yaml:"prefixes,omitempty" json:"prefixes,omitempty"`

	// LegClusters is set only on a move's view (MoveView), never stored: its roles (Source, Primary
	// and the keys of Names) are leg ids there, since two legs may share a cluster (ADR-0018 N3b),
	// and this maps each to its cluster. ClusterOf reads it.
	LegClusters map[string]string `yaml:"-" json:"-"`
}

// ClusterOf is the cluster a role of p is on: a move's view names its roles by leg, and every other
// placement names them by cluster.
func (p *Placement) ClusterOf(role string) string {
	if c, ok := p.LegClusters[role]; ok {
		return c
	}
	return role
}

// Barrier is config.Barrier: a drain barrier on a placement or a cluster (ADR-0021 D2).
type Barrier = config.Barrier

func (p Placement) clone() Placement {
	c := p
	c.Names = maps.Clone(p.Names)
	c.Ramp = v2ramp(p.Ramp)
	if p.Barrier != nil {
		b := *p.Barrier
		c.Barrier = &b
	}
	if p.Cutover != nil {
		ev := *p.Cutover
		c.Cutover = &ev
	}
	c.Legs = maps.Clone(p.Legs)
	c.LegClusters = maps.Clone(p.LegClusters)
	c.Owners = slices.Clone(p.Owners)
	c.Prefixes = clonePrefixes(p.Prefixes)
	if p.Move != nil {
		m := *p.Move
		m.Ramp, m.Cutover = v2ramp(p.Move.Ramp), v2cutover(p.Move.Cutover)
		c.Move = &m
	}
	return c
}

// File is the directory file schema. Version increments on every write; a reload that does not
// carry a higher version is rejected.
type File struct {
	Version int64 `yaml:"version" json:"version"`
	// Schema is the schema the directory was written with (SchemaVersion); 0 is schema 1.
	Schema int `yaml:"schema,omitempty" json:"schema,omitempty"`
	// Identity is the directory's lineage; zero until the first write under schema 2.
	Identity Identity `yaml:"identity,omitempty" json:"identity,omitzero"`
	// Generations maps each resource (PlacementResource, ClusterResource, TenantResource) to the
	// version of the write that last changed it. Stamp keeps it.
	Generations map[string]int64 `yaml:"generations,omitempty" json:"generations,omitempty"`
	// Clusters are the backends placements route to (docs/DESIGN.md §1.5: control-plane state).
	// Credentials are secret_refs only; a secret never lives in this file.
	Clusters   map[string]config.Cluster `yaml:"clusters,omitempty" json:"clusters"`
	Tenants    map[string]Tenant         `yaml:"tenants,omitempty" json:"tenants"`
	Placements map[string]Placement      `yaml:"placements,omitempty" json:"placements"`
}

func (f *File) clone() *File {
	c := &File{Version: f.Version, Schema: f.Schema, Identity: f.Identity, Generations: maps.Clone(f.Generations),
		Clusters: make(map[string]config.Cluster, len(f.Clusters)), Tenants: maps.Clone(f.Tenants),
		Placements: make(map[string]Placement, len(f.Placements))}
	for name := range f.Clusters {
		cl := f.Clusters[name]
		cl.Endpoints = slices.Clone(cl.Endpoints)
		c.Clusters[name] = cl // capability pointers are shared: nothing writes through them
	}
	for k := range f.Placements {
		c.Placements[k] = f.Placements[k].clone()
	}
	return c
}

// References lists what in f still names cluster: tenants defaulting to it and placements using
// it in any role. A cluster with references cannot be removed.
func References(f *File, cluster string) []string {
	var refs []string
	for _, name := range sortedKeys(f.Tenants) {
		if f.Tenants[name].DefaultCluster == cluster {
			refs = append(refs, "tenants."+name+".default_cluster")
		}
	}
	for _, k := range sortedKeys(f.Placements) {
		p := f.Placements[k]
		_, named := p.Names[cluster]
		for _, l := range p.Legs {
			named = named || l.Cluster == cluster
		}
		if p.Primary == cluster || p.Source == cluster || p.Target == cluster || p.Cold == cluster || named {
			refs = append(refs, "placements."+k)
		}
	}
	return refs
}

// Key joins a tenant and bucket into the placement key.
func Key(tenant, bucket string) string { return tenant + "/" + bucket }

// DefaultTenant is the tenant of a client key that names none. A deployment with one team never
// needs to mention tenants: its buckets are addressed by bare name (ADR-0010).
const DefaultTenant = "default"

// SplitKey splits a placement key.
func SplitKey(k string) (tenant, bucket string, ok bool) {
	tenant, bucket, ok = strings.Cut(k, "/")
	return tenant, bucket, ok && tenant != "" && bucket != ""
}

// parse decodes a directory file. Unknown keys are errors naming their key path; an empty
// document is an empty directory at version 0.
func parse(data []byte) (*File, error) {
	f := &File{}
	if len(bytes.TrimSpace(data)) == 0 {
		return f, nil
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(f); err != nil {
		if errors.Is(err, io.EOF) {
			return &File{}, nil
		}
		return nil, config.KeyedDecodeError("directory", data, err)
	}
	for _, k := range sortedKeys(f.Placements) {
		p := f.Placements[k]
		if err := p.fromV2(); err != nil {
			return nil, &config.Error{Key: "placements." + k, Msg: err.Error()}
		}
		f.Placements[k] = p
	}
	config.ApplyClusterDefaults(f.Clusters)
	return f, nil
}

// marshal encodes a directory file. Map keys come out sorted, so the output is deterministic.
func marshal(f *File) ([]byte, error) {
	body, err := yaml.Dump(f, yaml.WithIndent(2))
	if err != nil {
		return nil, err
	}
	return append([]byte("# shunt directory (ADR-0005). Written by shunt; hand edits must increment version.\n"), body...), nil
}

// Directory is one of the two named interface seams (CLAUDE.md): what the proxy's request path
// needs of the directory. Snapshot is lock-free and never nil; writes are serialized and visible
// in the next Snapshot of the instance that made them. Three implementations: FileDir (a file on
// this host, ADR-0005), the control plane's store on etcd (internal/cp, ADR-0015), and a member
// proxy's client of that store (internal/member), which forwards writes to the control plane.
type Directory interface {
	Snapshot() *Snapshot
	// Create writes an ACTIVE placement on cluster under the backend bucket name.
	Create(ctx context.Context, tenant, bucket, cluster, backend, actor string) error
	// Delete removes a placement. Only ACTIVE placements can be deleted.
	Delete(ctx context.Context, tenant, bucket, actor string) error
}

// Store is a Directory the control API can change: the operator mutations behind every verb
// (ADR-0008). FileDir for a single-node lab; internal/cp for a fleet.
type Store interface {
	Directory
	// Sync makes this instance's Snapshot current with the store before a decision is taken on
	// it: a reload of the file, or a linearizable read of the version in etcd and a wait for it.
	Sync(ctx context.Context) error
	// SetState applies a transition if the placement is still in state from.
	SetState(ctx context.Context, tenant, bucket, from string, t Transition, actor string) error
	// SetPlacementReadOnly changes a placement's maintenance switch; barrier, switching it on, is
	// the drain barrier written with it (ADR-0021 D2).
	SetPlacementReadOnly(ctx context.Context, tenant, bucket string, readOnly, reject bool, barrier, actor string) error
	// SetPlacementWatch asks for, or stops, the placement's per-backend traffic telemetry.
	SetPlacementWatch(ctx context.Context, tenant, bucket string, watch bool, actor string) error
	// SetClusterReadOnly changes a backend's maintenance switch, with its barrier when switching on.
	SetClusterReadOnly(ctx context.Context, name string, readOnly, reject bool, barrier, actor string) error
	// SetBarrier writes a drain barrier on a placement carrying none; ClearBarrier removes the one
	// named from a placement (PlacementResource) or a cluster (ClusterResource), and only that one.
	SetBarrier(ctx context.Context, tenant, bucket string, b Barrier, actor string) error
	ClearBarrier(ctx context.Context, resource, id, actor string) error
	// Adopt takes over an existing backend bucket as an ACTIVE placement.
	Adopt(ctx context.Context, tenant, bucket, cluster, backend, actor string) error
	// SetTarget records the cluster and backend bucket `shunt expand` prepared, on an ACTIVE placement.
	SetTarget(ctx context.Context, tenant, bucket, cluster, backend, actor string) error
	// CreateSpread writes an ACTIVE placement spread over one leg per cluster (ADR-0018 N2).
	CreateSpread(ctx context.Context, tenant, bucket string, legs []Leg, actor string) error
	// ClearTarget forgets an ACTIVE placement's prepared target before any step has used it.
	ClearTarget(ctx context.Context, tenant, bucket, actor string) error
	// Carve adds a prefix rule owned as its keys are now, and Merge removes one owned as its
	// parent scope is (ADR-0020): neither changes any key's owner.
	Carve(ctx context.Context, tenant, bucket, prefix, actor string) error
	Merge(ctx context.Context, tenant, bucket, prefix, actor string) error
	// SetTenantDefault changes where a tenant's new buckets are created.
	SetTenantDefault(ctx context.Context, tenant, cluster, actor string) error
	// PutCluster adds or replaces a cluster. secret, when not empty, is the cluster's secret key for
	// a store that keeps secrets itself (encrypted, internal/cp); FileDir refuses one, since the
	// directory file carries only secret_refs (ADR-0008).
	PutCluster(ctx context.Context, name string, c config.Cluster, secret, actor string) error
	// RemoveCluster drops a cluster nothing references.
	RemoveCluster(ctx context.Context, name, actor string) error
	// Changes returns the last limit change records at or before version before, newest first;
	// before 0 means the current version. Records past the store's retention are gone.
	Changes(ctx context.Context, before int64, limit int) ([]Change, error)
}

// Errors returned by Directory implementations.
var (
	ErrExists   = errors.New("directory: placement already exists")
	ErrInUse    = errors.New("directory: cluster is still in use")
	ErrNotFound = errors.New("directory: not found") // wrapped with what was not found: a placement, tenant or cluster
	ErrConflict = errors.New("directory: placement changed concurrently")
	// ErrRefused is a change refused on its merits, whatever else changes concurrently.
	ErrRefused      = errors.New("directory: refused")
	ErrReadOnly     = errors.New("directory: directory file is not writable")
	ErrSecretInline = errors.New("directory: the directory file carries only secret_refs, never a secret")
	ErrLockTimeout  = errors.New("directory: timed out waiting for the directory lock")
	errStaleVersion = errors.New("directory: file version did not increase")
)

type key struct{ tenant, bucket string }

// Snapshot is an immutable view of the directory. Callers must not modify what it returns.
type Snapshot struct {
	file    *File
	byKey   map[key]*Placement
	buckets map[string][]string // tenant → sorted client bucket names
}

// NewSnapshot builds a snapshot from a validated file: the control plane's watch cache and a
// member proxy build theirs from records that never came from a file (ADR-0015).
func NewSnapshot(f *File) *Snapshot { return newSnapshot(f) }

func newSnapshot(f *File) *Snapshot {
	s := &Snapshot{file: f, byKey: make(map[key]*Placement, len(f.Placements)), buckets: map[string][]string{}}
	for k := range f.Placements {
		tenant, bucket, ok := SplitKey(k)
		if !ok {
			continue // Validate rejects these; a snapshot is only built from validated files
		}
		p := f.Placements[k]
		s.byKey[key{tenant, bucket}] = &p
		s.buckets[tenant] = append(s.buckets[tenant], bucket)
	}
	for _, b := range s.buckets {
		slices.Sort(b)
	}
	return s
}

// Version is the directory file version this snapshot was built from.
func (s *Snapshot) Version() int64 { return s.file.Version }

// Lookup returns the placement for (tenant, bucket).
func (s *Snapshot) Lookup(tenant, bucket string) (*Placement, bool) {
	p, ok := s.byKey[key{tenant, bucket}]
	return p, ok
}

// Cluster returns a cluster definition.
func (s *Snapshot) Cluster(name string) (config.Cluster, bool) {
	c, ok := s.file.Clusters[name]
	return c, ok
}

// Tenant returns a tenant row.
func (s *Snapshot) Tenant(name string) (Tenant, bool) {
	t, ok := s.file.Tenants[name]
	return t, ok
}

// Buckets returns the tenant's client bucket names, sorted.
func (s *Snapshot) Buckets(tenant string) []string { return s.buckets[tenant] }

// File returns a deep copy of the directory contents.
func (s *Snapshot) File() *File { return s.file.clone() }

// Generation is the generation of a resource in the snapshot's directory (File.Generation).
func (s *Snapshot) Generation(resource string) int64 { return s.file.Generation(resource) }

// EachPlacement calls fn for every placement until it returns false. The placement must not be
// modified. It is what a caller that would otherwise clone the whole directory to read it uses.
func (s *Snapshot) EachPlacement(fn func(key string, p *Placement) bool) {
	for k, p := range s.byKey {
		if !fn(Key(k.tenant, k.bucket), p) {
			return
		}
	}
}

// EachCluster calls fn for every cluster until it returns false.
func (s *Snapshot) EachCluster(fn func(name string, c *config.Cluster) bool) {
	for name := range s.file.Clusters {
		c := s.file.Clusters[name]
		if !fn(name, &c) {
			return
		}
	}
}

// EachPlacement calls fn for every placement until it returns false, as Snapshot.EachPlacement.
func (f *File) EachPlacement(fn func(key string, p *Placement) bool) {
	for k := range f.Placements {
		p := f.Placements[k]
		if !fn(k, &p) {
			return
		}
	}
}

// EachCluster calls fn for every cluster until it returns false, as Snapshot.EachCluster.
func (f *File) EachCluster(fn func(name string, c *config.Cluster) bool) {
	for name := range f.Clusters {
		c := f.Clusters[name]
		if !fn(name, &c) {
			return
		}
	}
}
