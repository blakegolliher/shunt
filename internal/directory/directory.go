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
	Ratio    float64  `yaml:"ratio,omitempty" json:"ratio,omitempty"`
	Prefixes []string `yaml:"prefixes,omitempty" json:"prefixes,omitempty"`
}

// Placement maps one (tenant, bucket) to clusters and a state.
type Placement struct {
	State     string            `yaml:"state" json:"state"`
	Primary   string            `yaml:"primary" json:"primary"`
	Source    string            `yaml:"source,omitempty" json:"source,omitempty"`
	Ramp      *Ramp             `yaml:"ramp,omitempty" json:"ramp,omitempty"`
	Names     map[string]string `yaml:"names" json:"names"` // cluster → backend bucket name
	Cold      string            `yaml:"cold,omitempty" json:"cold,omitempty"`
	Tier      string            `yaml:"tier,omitempty" json:"tier,omitempty"` // native | emulated
	Lifecycle string            `yaml:"lifecycle,omitempty" json:"lifecycle,omitempty"`
	Created   time.Time         `yaml:"created,omitempty" json:"created,omitzero"`
}

// Route returns the cluster POC-3 sends a request to. RAMPING and MIGRATING route to the source
// until POC-4 adds ramp and fallback routing, so no request silently 404s against an empty target.
func (p *Placement) Route() string {
	if p.State == StateRamping || p.State == StateMigrating {
		return p.Source
	}
	return p.Primary
}

func (p Placement) clone() Placement {
	c := p
	c.Names = maps.Clone(p.Names)
	if p.Ramp != nil {
		r := *p.Ramp
		r.Prefixes = slices.Clone(p.Ramp.Prefixes)
		c.Ramp = &r
	}
	return c
}

// File is the directory file schema. Version increments on every write; a reload that does not
// carry a higher version is rejected.
type File struct {
	Version    int64                `yaml:"version" json:"version"`
	Tenants    map[string]Tenant    `yaml:"tenants,omitempty" json:"tenants"`
	Placements map[string]Placement `yaml:"placements,omitempty" json:"placements"`
}

func (f *File) clone() *File {
	c := &File{Version: f.Version, Tenants: maps.Clone(f.Tenants), Placements: make(map[string]Placement, len(f.Placements))}
	for k := range f.Placements {
		c.Placements[k] = f.Placements[k].clone()
	}
	return c
}

// Key joins a tenant and bucket into the placement key.
func Key(tenant, bucket string) string { return tenant + "/" + bucket }

// SplitKey splits a placement key.
func SplitKey(k string) (tenant, bucket string, ok bool) {
	tenant, bucket, ok = strings.Cut(k, "/")
	return tenant, bucket, ok && tenant != "" && bucket != ""
}

// Parse decodes a directory file. Unknown keys are errors naming their key path; an empty
// document is an empty directory at version 0.
func Parse(data []byte) (*File, error) {
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
	return f, nil
}

// Marshal encodes a directory file. Map keys come out sorted, so the output is deterministic.
func Marshal(f *File) ([]byte, error) {
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
	ErrNotFound     = errors.New("directory: no such placement")
	ErrConflict     = errors.New("directory: placement changed concurrently")
	ErrReadOnly     = errors.New("directory: directory file is not writable")
	ErrLockTimeout  = errors.New("directory: timed out waiting for the directory lock")
	ErrStaleVersion = errors.New("directory: file version did not increase")
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

// Tenant returns a tenant row.
func (s *Snapshot) Tenant(name string) (Tenant, bool) {
	t, ok := s.file.Tenants[name]
	return t, ok
}

// Buckets returns the tenant's client bucket names, sorted.
func (s *Snapshot) Buckets(tenant string) []string { return s.buckets[tenant] }

// File returns a deep copy of the directory contents.
func (s *Snapshot) File() *File { return s.file.clone() }
