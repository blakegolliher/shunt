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
	Primary string `yaml:"primary" json:"primary"`
	Source  string `yaml:"source,omitempty" json:"source,omitempty"`
	// Target is the cluster `shunt expand` prepared for the move, recorded while ACTIVE so the
	// first ramp or migrate step needs no --to. Its backend name is in Names.
	Target    string            `yaml:"target,omitempty" json:"target,omitempty"`
	Cutover   *CutoverEvidence  `yaml:"cutover,omitempty" json:"cutover,omitempty"`
	Ramp      *Ramp             `yaml:"ramp,omitempty" json:"ramp,omitempty"`
	Names     map[string]string `yaml:"names" json:"names"` // cluster → backend bucket name
	Cold      string            `yaml:"cold,omitempty" json:"cold,omitempty"`
	Tier      string            `yaml:"tier,omitempty" json:"tier,omitempty"` // native | emulated
	Lifecycle string            `yaml:"lifecycle,omitempty" json:"lifecycle,omitempty"`
	Created   time.Time         `yaml:"created,omitempty" json:"created,omitzero"`
}

func (p Placement) clone() Placement {
	c := p
	c.Names = maps.Clone(p.Names)
	if p.Ramp != nil {
		r := *p.Ramp
		r.Prefixes = slices.Clone(p.Ramp.Prefixes)
		if p.Ramp.Hold != nil {
			h := *p.Ramp.Hold
			h.Prefixes = slices.Clone(p.Ramp.Hold.Prefixes)
			r.Hold = &h
		}
		c.Ramp = &r
	}
	if p.Cutover != nil {
		ev := *p.Cutover
		c.Cutover = &ev
	}
	return c
}

// File is the directory file schema. Version increments on every write; a reload that does not
// carry a higher version is rejected.
type File struct {
	Version int64 `yaml:"version" json:"version"`
	// Clusters are the backends placements route to (docs/DESIGN.md §1.5: control-plane state).
	// Credentials are secret_refs only; a secret never lives in this file.
	Clusters   map[string]config.Cluster `yaml:"clusters,omitempty" json:"clusters"`
	Tenants    map[string]Tenant         `yaml:"tenants,omitempty" json:"tenants"`
	Placements map[string]Placement      `yaml:"placements,omitempty" json:"placements"`
}

func (f *File) clone() *File {
	c := &File{Version: f.Version, Clusters: make(map[string]config.Cluster, len(f.Clusters)), Tenants: maps.Clone(f.Tenants),
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

// Directory is one of the two named interface seams (CLAUDE.md): the file backend now, Postgres
// behind shunt-control in P3c. Snapshot is lock-free and never nil; writes are serialized and
// visible in the next Snapshot of the instance that made them.
type Directory interface {
	Snapshot() *Snapshot
	// Create writes an ACTIVE placement on cluster under the backend bucket name.
	Create(ctx context.Context, tenant, bucket, cluster, backend, actor string) error
	// Delete removes a placement. Only ACTIVE placements can be deleted.
	Delete(ctx context.Context, tenant, bucket, actor string) error
	// SetState applies a transition if the placement is still in state from.
	SetState(ctx context.Context, tenant, bucket, from string, t Transition, actor string) error
}

// Errors returned by Directory implementations.
var (
	ErrExists       = errors.New("directory: placement already exists")
	ErrInUse        = errors.New("directory: cluster is still in use")
	ErrNotFound     = errors.New("directory: not found") // wrapped with what was not found: a placement, tenant or cluster
	ErrConflict     = errors.New("directory: placement changed concurrently")
	ErrReadOnly     = errors.New("directory: directory file is not writable")
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
