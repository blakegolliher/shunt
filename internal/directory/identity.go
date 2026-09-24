package directory

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// SchemaVersion is the directory schema this build reads and writes (ADR-0021). A directory
// written with a newer schema is refused: a binary must not act on state it cannot interpret.
// Schema 1 carried no identity and no generations; the first write under this build upgrades it.
const SchemaVersion = 2

// Identity names a directory's lineage (ADR-0021). ClusterID is drawn once, when the directory is
// first written, and never changes. Epoch is drawn for each recovery lineage: a restore starts a
// new one. Versions compare only within one identity; across two, a larger version proves nothing.
type Identity struct {
	ClusterID string `yaml:"cluster_id" json:"cluster_id"`
	Epoch     string `yaml:"epoch" json:"epoch"`
}

// idLen is the length of a cluster id or epoch: 128 random bits, hex.
const idLen = 32

// NewIdentity draws a new cluster id and epoch.
func NewIdentity() (Identity, error) {
	c, err := randomID()
	if err != nil {
		return Identity{}, err
	}
	e, err := randomID()
	if err != nil {
		return Identity{}, err
	}
	return Identity{ClusterID: c, Epoch: e}, nil
}

func randomID() (string, error) {
	var b [idLen / 2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("directory: drawing an identity: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// IsZero reports whether i is unset: a schema-1 directory not yet written by this build.
func (i Identity) IsZero() bool { return i == Identity{} }

// Validate checks that both halves are 32 lowercase hex characters.
func (i Identity) Validate() error {
	if !validID(i.ClusterID) {
		return fmt.Errorf("cluster_id %q: want %d lowercase hex characters", i.ClusterID, idLen)
	}
	if !validID(i.Epoch) {
		return fmt.Errorf("epoch %q: want %d lowercase hex characters", i.Epoch, idLen)
	}
	return nil
}

func validID(s string) bool {
	if len(s) != idLen {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Resource keys name what a generation counts: one placement, cluster or tenant.

// PlacementResource is the resource key of placement key (tenant/bucket).
func PlacementResource(key string) string { return "placement:" + key }

// ClusterResource is the resource key of a cluster.
func ClusterResource(name string) string { return "cluster:" + name }

// TenantResource is the resource key of a tenant.
func TenantResource(name string) string { return "tenant:" + name }

// Generation is the generation of a resource: the directory version of the write that last
// changed it. 0 is a resource that has not changed since the schema upgrade, or is not there; the
// upgrade stamps nothing, so it fits in one bounded transaction however large the directory.
// Versions only grow within an epoch, so a resource deleted and recreated under the same name
// never reuses a generation an old confirmation or operation could match.
func (f *File) Generation(resource string) int64 { return f.Generations[resource] }

// ErrNewerSchema is returned for a directory written by a newer shunt.
var ErrNewerSchema = errors.New("directory: written by a newer shunt")

// CheckSchema refuses a schema newer than this build's.
func CheckSchema(schema int) error {
	if schema > SchemaVersion {
		return fmt.Errorf("%w (schema %d; this build reads schema %d): upgrade this binary", ErrNewerSchema, schema, SchemaVersion)
	}
	return nil
}

// Stamp prepares next, the file a write produced from prev, before it is committed: the current
// schema; the identity, drawn on the first write under this build and otherwise prev's, whatever
// the write did; and the generation of every placement, cluster and tenant the write created or
// changed, set to next.Version; an unchanged one keeps its generation. A removed resource's
// generation goes with it. changed names more resources to stamp, for changes the file does not
// show (a cluster's secret, on the control plane); each must exist in next.
func Stamp(prev, next *File, changed ...string) error {
	next.Schema = SchemaVersion
	next.Identity = prev.Identity
	if next.Identity.IsZero() {
		id, err := NewIdentity()
		if err != nil {
			return err
		}
		next.Identity = id
	}
	gens := make(map[string]int64, len(next.Placements)+len(next.Clusters)+len(next.Tenants))
	stamp := func(res string, same bool) {
		if !same {
			gens[res] = next.Version
		} else if g := prev.Generations[res]; g > 0 {
			gens[res] = g
		}
	}
	for k := range next.Placements {
		old, had := prev.Placements[k]
		stamp(PlacementResource(k), had && sameJSON(old, next.Placements[k]))
	}
	for name := range next.Clusters {
		old, had := prev.Clusters[name]
		stamp(ClusterResource(name), had && sameJSON(old, next.Clusters[name]))
	}
	for name := range next.Tenants {
		old, had := prev.Tenants[name]
		stamp(TenantResource(name), had && reflect.DeepEqual(old, next.Tenants[name]))
	}
	for _, res := range changed {
		gens[res] = next.Version
	}
	next.Generations = gens
	return nil
}

// sameJSON compares two records as they are stored, so nil and empty collections are equal. Deep
// equality answers most comparisons without encoding either.
func sameJSON(a, b any) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(x, y)
}

// validateLineage checks the schema, the identity and the generations.
func validateLineage(e *errs, f *File) {
	if err := CheckSchema(f.Schema); err != nil {
		e.add("schema", "%v", err)
	}
	if !f.Identity.IsZero() {
		if err := f.Identity.Validate(); err != nil {
			e.add("identity", "%v", err)
		}
	}
	for _, res := range sortedKeys(f.Generations) {
		if g := f.Generations[res]; g <= 0 || g > f.Version {
			e.add("generations."+res, "generation %d outside 1..version %d", g, f.Version)
		}
	}
}
