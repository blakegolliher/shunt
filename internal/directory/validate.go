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

// validTenant reports whether name is a legal tenant name: 1–32 lowercase letters, digits, and
// hyphens, starting and ending with a letter or digit. Tenant names prefix generated backend
// bucket names, so they must be legal there.
func validTenant(name string) bool {
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

// Validate checks a directory's structure and references: what Load checks of a file, for a
// directory assembled from records (internal/cp, internal/member).
func Validate(f *File) error { return validate(f) }

// validate checks a directory file's structure: its clusters, and every reference to them. It
// returns every failure joined, each naming its key. Transition legality is checked by Apply at
// write time, not here: the validator only sees one state.
func validate(f *File) error {
	clusters := f.Clusters
	var e errs
	if f.Version < 0 {
		e.add("version", "must not be negative")
	}
	if err := config.ValidateClusters("clusters", clusters); err != nil {
		e = append(e, err)
	}
	for _, name := range sortedKeys(f.Tenants) {
		k := "tenants." + name
		if !validTenant(name) {
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
	if p.Spread() {
		validateSpread(e, clusters, k, pk, p, used)
		return
	}
	e.ref(clusters, k+".primary", p.Primary)
	migrating := p.State != StateActive
	if p.Target != "" {
		switch {
		case migrating:
			e.add(k+".target", "only allowed in state ACTIVE; a move in progress names its clusters in primary and source")
		case p.Target == p.Primary:
			e.add(k+".target", "must differ from primary")
		case e.ref(clusters, k+".target", p.Target):
			if _, ok := p.Names[p.Target]; !ok {
				e.add(k+".names", "missing backend bucket name for target cluster %q", p.Target)
			}
		}
	}
	if p.Cutover != nil && p.State != StateCutover {
		e.add(k+".cutover", "only allowed in state CUTOVER")
	}
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
	validateRamp(e, k, p.State, p.Ramp)
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

// validateRamp checks a ramp where it sits: a placement's, or a move's (ADR-0018 N3).
func validateRamp(e *errs, k, state string, r *Ramp) {
	if r != nil {
		if state != StateRamping {
			e.add(k+".ramp", "only allowed in state RAMPING")
		}
		if !validRampHashName(r.Hash) {
			e.add(k+".ramp.hash", "required: the name of the hash that splits the ramp's keys, e.g. %s; got %q", RampHash, r.Hash)
		}
		if r.Ratio < 0 || r.Ratio > 1 {
			e.add(k+".ramp.ratio", "must be within [0, 1], got %v", r.Ratio)
		}
		if r.Ratio == 0 && len(r.Prefixes) == 0 && r.Hold == nil {
			e.add(k+".ramp", "needs a ratio or at least one prefix")
		}
		if h := r.Hold; h != nil {
			if h.Ratio < 0 || h.Ratio > 1 {
				e.add(k+".ramp.hold.ratio", "must be within [0, 1], got %v", h.Ratio)
			}
			if h.Ratio == 0 && len(h.Prefixes) == 0 {
				e.add(k+".ramp.hold", "needs a ratio or at least one prefix")
			}
		}
	} else if state == StateRamping {
		e.add(k+".ramp", "required in state RAMPING")
	}
	if r != nil && r.Range != nil && r.Range.From > r.Range.To {
		e.add(k+".ramp.range", "from is after to")
	}
}

// validateSpread checks a placement spread over legs (ADR-0018 N2): its v2 shape, what N2 routes,
// and each leg as a backend bucket, never shared with another placement.
func validateSpread(e *errs, clusters map[string]config.Cluster, k, pk string, p Placement, used map[[2]string]string) {
	if err := checkV2(&p); err != nil {
		e.add(k, "%s", err.Error())
		return
	}
	if err := checkSpread(&p); err != nil {
		e.add(k, "%s", err.Error())
	}
	if p.Move != nil {
		validateRamp(e, k+".move", p.State, p.Move.Ramp)
	}
	if p.Primary != "" || p.Source != "" || p.Names != nil || p.Ramp != nil || p.Cutover != nil {
		e.add(k, "a placement spread over legs names its buckets in legs, not in primary, source, names, ramp or cutover")
	}
	onCluster := map[string]string{}
	for _, id := range sortedKeys(p.Legs) {
		l, lk := p.Legs[id], k+".legs."+id
		if !e.ref(clusters, lk+".cluster", l.Cluster) {
			continue
		}
		if other, dup := onCluster[l.Cluster]; dup {
			e.add(lk+".cluster", "legs %s and %s are both on cluster %q; this build routes one leg per cluster (ADR-0018 N3)", other, id, l.Cluster)
		}
		onCluster[l.Cluster] = id
		switch {
		case !s3.ValidBucketName(l.Bucket):
			e.add(lk+".bucket", "%q is not a valid S3 bucket name", l.Bucket)
		default:
			if other, dup := used[[2]string{l.Cluster, l.Bucket}]; dup {
				e.add(lk+".bucket", "backend bucket %q on cluster %q is already used by placement %q; placements must never share a backend bucket", l.Bucket, l.Cluster, other)
			} else {
				used[[2]string{l.Cluster, l.Bucket}] = pk
			}
		}
	}
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

// validRampHashName accepts a lowercase name of letters, digits and dashes. Whether this build
// implements the named hash is a routing question, answered per placement (migrate.InRange), so a
// directory written by a newer build still loads and only its ramps are refused.
func validRampHashName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-' && i > 0:
		default:
			return false
		}
	}
	return true
}
