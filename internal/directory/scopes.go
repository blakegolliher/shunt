package directory

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
)

// Prefix ownership (ADR-0020). A spread placement may carry prefix rules, each a scope with its own
// owners table. A key belongs to the rule with the longest prefix it starts with, or to the
// placement's own Owners (the scope of the empty prefix); its owner is the leg its hash falls to in
// that scope's table. Carving a rule copies its parent's table and merging one requires it to equal
// its parent's, so neither changes any key's owner.

// MaxPrefixRules caps a placement's prefix rules, and MaxPrefixLen a rule's prefix (the S3 key
// limit). Removal criterion: changed only by an amendment to ADR-0020.
const (
	MaxPrefixRules = 64
	MaxPrefixLen   = 1024
)

// PrefixRule is one scope: the keys under Prefix that no longer rule claims, split among legs by
// Owners exactly as a placement's own Owners split the rest.
type PrefixRule struct {
	Prefix string  `yaml:"prefix" json:"prefix"`
	Owners []Owner `yaml:"owners" json:"owners"`
}

// Scope is the scope key belongs to: the prefix of the longest rule it starts with ("" for none)
// and that scope's owners table.
func (p *Placement) Scope(key string) (prefix string, owners []Owner) {
	best := -1
	for i := range p.Prefixes {
		r := &p.Prefixes[i]
		if (best < 0 || len(r.Prefix) > len(p.Prefixes[best].Prefix)) && strings.HasPrefix(key, r.Prefix) {
			best = i
		}
	}
	if best < 0 {
		return "", p.Owners
	}
	return p.Prefixes[best].Prefix, p.Prefixes[best].Owners
}

// OwnerIn is the leg of owners whose range holds h. owners partitions the hash space (checkOwners),
// so there always is one.
func OwnerIn(owners []Owner, h Hash) string {
	i := sort.Search(len(owners), func(i int) bool { return owners[i].To >= h })
	if i == len(owners) {
		return ""
	}
	return owners[i].Leg
}

// parentTable is the table the keys under prefix fall to without a rule for prefix itself: the
// longest other rule that prefix starts with, or the placement's own Owners.
func (p *Placement) parentTable(prefix string) []Owner {
	best := -1
	for i := range p.Prefixes {
		r := &p.Prefixes[i]
		if r.Prefix != prefix && strings.HasPrefix(prefix, r.Prefix) && (best < 0 || len(r.Prefix) > len(p.Prefixes[best].Prefix)) {
			best = i
		}
	}
	if best < 0 {
		return p.Owners
	}
	return p.Prefixes[best].Owners
}

// ownsKeys reports whether leg owns a range in any scope of p.
func (p *Placement) ownsKeys(leg string) bool {
	owns := func(o Owner) bool { return o.Leg == leg }
	if slices.ContainsFunc(p.Owners, owns) {
		return true
	}
	for i := range p.Prefixes {
		if slices.ContainsFunc(p.Prefixes[i].Owners, owns) {
			return true
		}
	}
	return false
}

// ListingLegs are the legs, sorted, that can hold keys under a listing's prefix: those of the scope
// the prefix itself falls in, of every rule nested under it, and a move's two legs. The rest hold
// no key under the prefix, so a listing need not read them (ADR-0020).
func ListingLegs(p *Placement, prefix string) []string {
	set := map[string]bool{}
	add := func(owners []Owner) {
		for _, o := range owners {
			set[o.Leg] = true
		}
	}
	_, owners := p.Scope(prefix)
	add(owners)
	for i := range p.Prefixes {
		if strings.HasPrefix(p.Prefixes[i].Prefix, prefix) {
			add(p.Prefixes[i].Owners)
		}
	}
	if m := p.Move; m != nil {
		set[m.From], set[m.To] = true, true
	}
	return slices.Sorted(maps.Keys(set))
}

// checkPrefixes checks a placement's prefix rules on their own: the caps, distinct non-empty
// prefixes in order, and every table a partition of the key space naming the placement's legs.
func checkPrefixes(p *Placement) error {
	if len(p.Prefixes) == 0 {
		return nil
	}
	if len(p.Prefixes) > MaxPrefixRules {
		return fmt.Errorf("prefixes: %d rules; a placement has at most %d (ADR-0020)", len(p.Prefixes), MaxPrefixRules)
	}
	if p.KeyHash == "" {
		return fmt.Errorf("hash: required when prefix rules split the key space, e.g. %s", RampHash)
	}
	for i := range p.Prefixes {
		r := &p.Prefixes[i]
		switch {
		case r.Prefix == "":
			return fmt.Errorf("prefixes[%d].prefix: required; the empty prefix is the placement's own owners", i)
		case len(r.Prefix) > MaxPrefixLen:
			return fmt.Errorf("prefixes[%d].prefix: %d bytes; a prefix has at most %d", i, len(r.Prefix), MaxPrefixLen)
		case i > 0 && r.Prefix <= p.Prefixes[i-1].Prefix:
			return fmt.Errorf("prefixes[%d].prefix: %q; rules are sorted by prefix, each prefix once", i, r.Prefix)
		}
		if err := checkOwners(r.Owners, p.Legs); err != nil {
			return fmt.Errorf("prefixes[%d] (%q): %w", i, r.Prefix, err)
		}
	}
	return nil
}

// Carve adds a prefix rule to a placement: a scope for the keys under prefix, owned exactly as they
// are now. No key changes owner, so no data moves and no proxy needs a fence (ADR-0020). A plain
// placement becomes a v2 one with its single leg.
func (f *File) Carve(tenant, bucket, prefix string) error {
	k := Key(tenant, bucket)
	p, ok := f.Placements[k]
	if !ok {
		return ErrNotFound
	}
	switch {
	case p.State != StateActive || p.Move != nil:
		return fmt.Errorf("%w: %s is %s; prefix rules change only at rest, with no move in progress", ErrRefused, k, p.State)
	case prefix == "":
		return fmt.Errorf("%w: a prefix is required; the empty prefix is every key", ErrRefused)
	case len(prefix) > MaxPrefixLen:
		return fmt.Errorf("%w: a prefix has at most %d bytes", ErrRefused, MaxPrefixLen)
	case len(p.Prefixes) >= MaxPrefixRules:
		return fmt.Errorf("%w: %s has %d prefix rules, the most a bucket may have (ADR-0020)", ErrRefused, k, MaxPrefixRules)
	case slices.ContainsFunc(p.Prefixes, func(r PrefixRule) bool { return r.Prefix == prefix }):
		return fmt.Errorf("%w: %s already has a rule for %q", ErrRefused, k, prefix)
	case !p.Spread() && (p.Target != "" || p.Cold != "" || p.Tier != "" || p.Lifecycle != ""):
		return fmt.Errorf("%w: %s has a target, cold tier or lifecycle recorded; prefix rules need a bucket with none (clear the target first)", ErrRefused, k)
	}
	np := p.clone()
	if !np.Spread() {
		np.Legs = map[string]Leg{p.Primary: {Cluster: p.Primary, Bucket: p.Names[p.Primary]}}
		np.Owners = []Owner{{From: FullRange.From, To: FullRange.To, Leg: p.Primary}}
		np.Primary, np.Names = "", nil
	}
	np.KeyHash = RampHash
	np.Prefixes = append(np.Prefixes, PrefixRule{Prefix: prefix, Owners: slices.Clone(np.parentTable(prefix))})
	slices.SortFunc(np.Prefixes, func(a, b PrefixRule) int { return strings.Compare(a.Prefix, b.Prefix) })
	f.Placements[k] = np
	return nil
}

// Merge removes a prefix rule whose table equals its parent's: its keys fall back to the parent
// scope with the same owners, so again no key changes owner. A bucket left with one leg owning
// every key is plain again.
func (f *File) Merge(tenant, bucket, prefix string) error {
	k := Key(tenant, bucket)
	p, ok := f.Placements[k]
	if !ok {
		return ErrNotFound
	}
	i := slices.IndexFunc(p.Prefixes, func(r PrefixRule) bool { return r.Prefix == prefix })
	switch {
	case i < 0:
		return fmt.Errorf("%w: %s has no rule for %q", ErrRefused, k, prefix)
	case p.State != StateActive || p.Move != nil:
		return fmt.Errorf("%w: %s is %s; prefix rules change only at rest, with no move in progress", ErrRefused, k, p.State)
	case !slices.Equal(p.Prefixes[i].Owners, p.parentTable(prefix)):
		return fmt.Errorf("%w: the keys under %q are owned differently from the rest of their scope; move them to match before merging the rule", ErrRefused, prefix)
	}
	np := p.clone()
	np.Prefixes = slices.Delete(np.Prefixes, i, i+1)
	if len(np.Prefixes) == 0 {
		np.Prefixes = nil
	}
	f.Placements[k] = settle(np)
	return nil
}

func clonePrefixes(rules []PrefixRule) []PrefixRule {
	if rules == nil {
		return nil
	}
	out := make([]PrefixRule, len(rules))
	for i, r := range rules {
		out[i] = PrefixRule{Prefix: r.Prefix, Owners: slices.Clone(r.Owners)}
	}
	return out
}
