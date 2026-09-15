package directory

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/s3"
)

var tierModes = []string{"native", "emulated"}

type errs []error

func (e *errs) add(key, format string, args ...any) {
	*e = append(*e, &config.Error{Key: key, Msg: fmt.Sprintf(format, args...)})
}

func (e *errs) oneOf(key, val string, allowed []string) bool {
	if slices.Contains(allowed, val) {
		return true
	}
	if val == "" {
		e.add(key, "required; one of %s", strings.Join(allowed, "|"))
	} else {
		e.add(key, "%q is not one of %s", val, strings.Join(allowed, "|"))
	}
	return false
}

// ref reports an error unless name is a configured cluster. Returns true when it resolves.
func (e *errs) ref(clusters map[string]config.Cluster, key, name string) bool {
	if name == "" {
		e.add(key, "required")
		return false
	}
	if _, ok := clusters[name]; !ok {
		e.add(key, "unknown cluster %q", name)
		return false
	}
	return true
}

func sortedKeys[V any](m map[string]V) []string { return slices.Sorted(maps.Keys(m)) }

// ValidTenant reports whether name is a legal tenant name: 1–32 lowercase letters, digits, and
// hyphens, starting and ending with a letter or digit. Tenant names prefix generated backend
// bucket names, so they must be legal there.
func ValidTenant(name string) bool {
	if name == "" || len(name) > 32 || name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// Validate checks a directory file's structure and its references to the configured clusters. It
// returns every failure joined, each naming its key. Transition legality is checked by Apply at
// write time, not here: the validator only sees one state.
func Validate(f *File, clusters map[string]config.Cluster) error {
	var e errs
	if f.Version < 0 {
		e.add("version", "must not be negative")
	}
	for _, name := range sortedKeys(f.Tenants) {
		k := "tenants." + name
		if !ValidTenant(name) {
			e.add(k, "tenant names are 1-32 lowercase letters, digits, and hyphens, starting and ending with a letter or digit")
		}
		if t := f.Tenants[name]; t.DefaultCluster == "" {
			e.add(k+".default_cluster", "required")
		} else if _, ok := clusters[t.DefaultCluster]; !ok {
			e.add(k+".default_cluster", "unknown cluster %q", t.DefaultCluster)
		}
	}
	used := map[[2]string]string{} // (cluster, backend bucket) → placement key
	for _, pk := range sortedKeys(f.Placements) {
		validatePlacement(&e, f, clusters, pk, used)
	}
	if len(e) == 0 {
		return nil
	}
	return errors.Join(e...)
}

func validatePlacement(e *errs, f *File, clusters map[string]config.Cluster, pk string, used map[[2]string]string) {
	k := "placements." + pk
	p := f.Placements[pk]
	if tenant, bucket, ok := SplitKey(pk); !ok || strings.Contains(bucket, "/") {
		e.add(k, "key must be <tenant>/<bucket>")
	} else {
		if _, found := f.Tenants[tenant]; !found {
			e.add(k, "unknown tenant %q", tenant)
		}
		if !s3.ValidBucketName(bucket) {
			e.add(k, "%q is not a valid S3 bucket name", bucket)
		}
	}
	if !e.oneOf(k+".state", p.State, States) {
		return
	}
	e.ref(clusters, k+".primary", p.Primary)
	migrating := p.State != StateActive
	switch {
	case migrating && p.Source == "":
		e.add(k+".source", "required in state %s", p.State)
	case !migrating && p.Source != "":
		e.add(k+".source", "not allowed in state ACTIVE")
	case p.Source != "":
		e.ref(clusters, k+".source", p.Source)
		if p.Source == p.Primary {
			e.add(k+".source", "must differ from primary")
		}
	}
	if p.Ramp != nil {
		if p.State != StateRamping {
			e.add(k+".ramp", "only allowed in state RAMPING")
		}
		if p.Ramp.Ratio < 0 || p.Ramp.Ratio > 1 {
			e.add(k+".ramp.ratio", "must be within [0, 1], got %v", p.Ramp.Ratio)
		}
		if p.Ramp.Ratio == 0 && len(p.Ramp.Prefixes) == 0 {
			e.add(k+".ramp", "needs a ratio or at least one prefix")
		}
	} else if p.State == StateRamping {
		e.add(k+".ramp", "required in state RAMPING")
	}
	for _, cl := range sortedKeys(p.Names) {
		nk := k + ".names." + cl
		e.ref(clusters, nk, cl)
		name := p.Names[cl]
		switch {
		case name == "":
			e.add(nk, "backend bucket name is empty")
		case !s3.ValidBucketName(name):
			e.add(nk, "%q is not a valid S3 bucket name", name)
		default:
			if other, dup := used[[2]string{cl, name}]; dup {
				e.add(nk, "backend bucket %q on cluster %q is already used by placement %q; placements must never share a backend bucket", name, cl, other)
			} else {
				used[[2]string{cl, name}] = pk
			}
		}
	}
	for _, cl := range []string{p.Primary, p.Source, p.Cold} {
		if cl == "" {
			continue
		}
		if _, ok := clusters[cl]; !ok {
			continue // already reported
		}
		if _, ok := p.Names[cl]; !ok {
			e.add(k+".names", "missing backend bucket name for cluster %q", cl)
		}
	}
	validateTier(e, clusters, k, p)
}

func validateTier(e *errs, clusters map[string]config.Cluster, k string, p Placement) {
	if p.Tier != "" && !e.oneOf(k+".tier", p.Tier, tierModes) {
		return
	}
	switch p.Tier {
	case "":
		if p.Cold != "" {
			e.add(k+".tier", "required when cold is set; one of %s", strings.Join(tierModes, "|"))
		}
		if p.Lifecycle != "" {
			e.add(k+".tier", "required when lifecycle is set")
		}
	case "native":
		if p.Cold != "" {
			e.add(k+".cold", "not allowed with tier: native; the backend owns its classes")
		}
		if cl, ok := clusters[p.Primary]; ok && cl.StorageClasses != "native" {
			e.add(k+".tier", "native requires clusters.%s.storage_classes: native", p.Primary)
		}
	case "emulated":
		if p.Cold == "" {
			e.add(k+".cold", "required with tier: emulated")
		} else if e.ref(clusters, k+".cold", p.Cold) {
			if cl := clusters[p.Cold]; cl.StorageClass == "" {
				e.add(k+".cold", "cluster %q has no storage_class; an emulated cold cluster must state one", p.Cold)
			}
			if p.Cold == p.Primary {
				e.add(k+".cold", "must differ from primary")
			}
		}
	}
}
