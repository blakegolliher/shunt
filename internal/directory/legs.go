package directory

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
)

// Schema v2 of a placement (ADR-0018): legs, a table of hash ranges that says which leg owns each
// key, and at most one move. A placement whose one leg owns every key is converted on decode into
// the v1 fields (Primary, Source, Target, Names, Ramp, Cutover), so the rest of shunt sees one form
// for it (N1). A placement whose keys several legs own is spread (N2): it stays in v2 form, ACTIVE,
// with Primary and Names empty, and the proxy narrows it to one leg per key (migrate.Narrow). What
// this build cannot route is refused, never guessed at: two legs on one cluster, a moving or tiered
// spread placement, or a move of part of the key space need ADR-0018 N3.

// MaxLegs caps a placement's legs (ADR-0018). Removal criterion: raised only by an amendment to
// ADR-0018, once the N-way listing benchmark meets its target at the new cap.
const MaxLegs = 32

// Leg is one backend bucket a placement spans.
type Leg struct {
	Cluster string `yaml:"cluster" json:"cluster"`
	Bucket  string `yaml:"bucket" json:"bucket"`
}

// Hash is a point of the 64-bit key hash space, written as 16 lowercase hex digits so that JSON
// readers without 64-bit integers (the browser) see it exactly.
type Hash uint64

// MarshalText writes h as 16 hex digits.
func (h Hash) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "%016x", uint64(h)), nil }

// UnmarshalText reads exactly 16 hex digits.
func (h *Hash) UnmarshalText(b []byte) error {
	if len(b) != 16 {
		return fmt.Errorf("hash %q: want 16 hex digits", b)
	}
	v, err := strconv.ParseUint(string(b), 16, 64)
	if err != nil {
		return fmt.Errorf("hash %q: want 16 hex digits", b)
	}
	*h = Hash(v)
	return nil
}

// HashRange is an inclusive range of the key hash space.
type HashRange struct {
	From Hash `yaml:"from" json:"from"`
	To   Hash `yaml:"to" json:"to"`
}

// FullRange is the whole key hash space.
var FullRange = HashRange{From: 0, To: math.MaxUint64}

// Owner says which leg holds the keys whose hash falls in [From, To].
type Owner struct {
	From Hash   `yaml:"from" json:"from"`
	To   Hash   `yaml:"to" json:"to"`
	Leg  string `yaml:"leg" json:"leg"`
}

// Move transfers the keys of one range from one leg to another, through the placement's state
// (RAMPING, MIGRATING, CUTOVER). Ramp and cutover evidence belong to the move.
type Move struct {
	Range   HashRange        `yaml:"range" json:"range"`
	From    string           `yaml:"from" json:"from"`
	To      string           `yaml:"to" json:"to"`
	Ramp    *Ramp            `yaml:"ramp,omitempty" json:"ramp,omitempty"`
	Cutover *CutoverEvidence `yaml:"cutover,omitempty" json:"cutover,omitempty"`
}

// legID is what a leg may be called. A leg converted from v1 is named after its cluster, so the
// rule is the cluster name rule (internal/control).
var legID = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// isV2 reports whether p was decoded from schema v2.
func (p *Placement) isV2() bool { return p.Legs != nil || p.Owners != nil || p.Move != nil }

// fromV2 converts a placement decoded from schema v2 into the v1 fields this build routes by, and
// clears the v2 ones. A v1 placement is left as it is.
func (p *Placement) fromV2() error {
	if !p.isV2() {
		return nil
	}
	if p.Primary != "" || p.Source != "" || p.Names != nil || p.Ramp != nil || p.Cutover != nil {
		return fmt.Errorf("a placement is written in one schema: legs, owners and move (v2), or primary, source, names, ramp and cutover (v1), not both")
	}
	if err := checkV2(p); err != nil {
		return err
	}
	if len(p.Owners) > 1 || (p.Move != nil && (p.Move.Range != FullRange || sameClusterMove(p))) {
		return checkSpread(p)
	}
	cluster := func(leg string) string { return p.Legs[leg].Cluster }
	names := make(map[string]string, len(p.Legs))
	for _, id := range sortedKeys(p.Legs) {
		l := p.Legs[id]
		if _, dup := names[l.Cluster]; dup {
			return fmt.Errorf("legs: two legs on cluster %q, but one leg owns every key and nothing moves: a plain placement names one bucket per cluster", l.Cluster)
		}
		names[l.Cluster] = l.Bucket
	}
	owner := p.Owners[0].Leg
	v1 := *p
	v1.Legs, v1.Owners, v1.Move, v1.KeyHash = nil, nil, nil, ""
	v1.Names = names
	switch m := p.Move; {
	case p.State == StateActive && m != nil:
		return fmt.Errorf("move: not allowed in state ACTIVE")
	case p.State == StateActive:
		v1.Primary = cluster(owner)
	case m == nil:
		return fmt.Errorf("move: required in state %s", p.State)
	case m.Range != FullRange:
		return fmt.Errorf("move.range: a move of part of the key space needs ADR-0018 N3; this build moves every key")
	case m.From != owner:
		return fmt.Errorf("move.from: %q does not own the range it moves; %q does", m.From, owner)
	default:
		v1.Source, v1.Primary = cluster(m.From), cluster(m.To)
		v1.Ramp, v1.Cutover = m.Ramp, m.Cutover
	}
	if p.Target != "" {
		v1.Target = cluster(p.Target)
	}
	if p.Cold != "" {
		v1.Cold = cluster(p.Cold)
	}
	*p = v1
	return nil
}

// Spread reports whether p keeps its v2 fields in memory (ADR-0018): several legs own its keys
// (N2), or part of its keys is moving to another leg (N3). Its Primary and Names are empty then.
func (p *Placement) Spread() bool { return len(p.Owners) > 1 || p.Move != nil }

// checkSpread is what this build routes of a spread placement: untiered, one leg per cluster, and
// at rest when ACTIVE, or moving one range that a single leg owns from that leg to another.
func checkSpread(p *Placement) error {
	switch {
	case p.Target != "" || p.Cold != "" || p.Tier != "" || p.Lifecycle != "":
		return fmt.Errorf("target, cold, tier and lifecycle are not set on a placement spread over legs in this build")
	case p.State == StateActive && p.Move != nil:
		return fmt.Errorf("move: not allowed in state ACTIVE")
	case p.State != StateActive && p.Move == nil:
		return fmt.Errorf("move: required in state %s", p.State)
	}
	m := p.Move
	if m == nil {
		return nil
	}
	if !slices.ContainsFunc(p.Owners, func(o Owner) bool { return o.Leg == m.From && o.From <= m.Range.From && m.Range.To <= o.To }) {
		return fmt.Errorf("move.range: leg %q does not own all of %s", m.From, rangeText(m.Range))
	}
	switch {
	case p.State == StateRamping && m.Ramp == nil:
		return fmt.Errorf("move.ramp: required in state RAMPING")
	case p.State != StateRamping && m.Ramp != nil:
		return fmt.Errorf("move.ramp: only allowed in state RAMPING")
	case p.State != StateCutover && m.Cutover != nil:
		return fmt.Errorf("move.cutover: only allowed in state CUTOVER")
	}
	return nil
}

// sameClusterMove reports whether a move's two legs share a cluster: such a move stays in v2 form,
// since a plain placement names one bucket per cluster.
func sameClusterMove(p *Placement) bool {
	from, okF := p.Legs[p.Move.From]
	to, okT := p.Legs[p.Move.To]
	return okF && okT && from.Cluster == to.Cluster
}

// EvenOwners splits the key hash space into len(legs) ranges of equal width, in the order given.
func EvenOwners(legs []string) []Owner {
	n := uint64(len(legs))
	out := make([]Owner, 0, n)
	width := math.MaxUint64 / n
	for i, leg := range legs {
		from := Hash(uint64(i) * width)
		to := Hash(uint64(i+1)*width - 1)
		if i == len(legs)-1 {
			to = math.MaxUint64
		}
		out = append(out, Owner{From: from, To: to, Leg: leg})
	}
	return out
}

// checkV2 checks what schema v2 says on its own, before any conversion: the legs, that the owners
// partition the key space, and that the move and the leg references name legs.
func checkV2(p *Placement) error {
	if len(p.Legs) == 0 {
		return fmt.Errorf("legs: at least one leg is required")
	}
	if len(p.Legs) > MaxLegs {
		return fmt.Errorf("legs: %d legs; a placement has at most %d (ADR-0018)", len(p.Legs), MaxLegs)
	}
	for _, id := range sortedKeys(p.Legs) {
		l := p.Legs[id]
		switch {
		case !legID.MatchString(id):
			return fmt.Errorf("legs.%s: a leg id is lowercase letters, digits, - and _, starting with a letter or digit", id)
		case l.Cluster == "":
			return fmt.Errorf("legs.%s.cluster: required", id)
		case l.Bucket == "":
			return fmt.Errorf("legs.%s.bucket: required", id)
		}
	}
	if p.KeyHash != "" && !validRampHashName(p.KeyHash) {
		return fmt.Errorf("hash: %q is not a hash name", p.KeyHash)
	}
	if len(p.Owners) > 1 && p.KeyHash == "" {
		return fmt.Errorf("hash: required when owners split the key space, e.g. %s", RampHash)
	}
	if err := checkOwners(p.Owners, p.Legs); err != nil {
		return err
	}
	if m := p.Move; m != nil {
		switch {
		case m.Range.From > m.Range.To:
			return fmt.Errorf("move.range: from is after to")
		case !hasLeg(p.Legs, m.From):
			return fmt.Errorf("move.from: unknown leg %q", m.From)
		case !hasLeg(p.Legs, m.To):
			return fmt.Errorf("move.to: unknown leg %q", m.To)
		case m.From == m.To:
			return fmt.Errorf("move.to: must differ from move.from")
		}
	}
	if p.Target != "" && !hasLeg(p.Legs, p.Target) {
		return fmt.Errorf("target: unknown leg %q", p.Target)
	}
	if p.Cold != "" && !hasLeg(p.Legs, p.Cold) {
		return fmt.Errorf("cold: unknown leg %q", p.Cold)
	}
	return nil
}

func hasLeg(legs map[string]Leg, id string) bool {
	_, ok := legs[id]
	return ok
}

// checkOwners requires the owners to cover the key hash space exactly once, in order, each range
// naming a leg.
func checkOwners(owners []Owner, legs map[string]Leg) error {
	if len(owners) == 0 {
		return fmt.Errorf("owners: at least one range is required")
	}
	next := Hash(0)
	for i, o := range owners {
		switch {
		case !hasLeg(legs, o.Leg):
			return fmt.Errorf("owners[%d].leg: unknown leg %q", i, o.Leg)
		case i == 0 && o.From != 0:
			return fmt.Errorf("owners[0]: starts at %016x; the first range starts at 0000000000000000", uint64(o.From))
		case o.From != next:
			return fmt.Errorf("owners[%d]: starts at %016x; the previous range ends at %016x, and the ranges must follow on without a gap or an overlap", i, uint64(o.From), uint64(next)-1)
		case o.To < o.From:
			return fmt.Errorf("owners[%d]: from is after to", i)
		}
		if o.To == math.MaxUint64 {
			if i != len(owners)-1 {
				return fmt.Errorf("owners[%d]: ends the key space, but more ranges follow", i)
			}
			return nil
		}
		next = o.To + 1
	}
	return fmt.Errorf("owners: the ranges stop at %016x; they must cover the key space up to ffffffffffffffff", uint64(next)-1)
}

// ToV2 returns p in schema v2: every backend bucket a leg named after its cluster, the leg that
// holds the keys at rest the only owner (the source while a move is in progress), and the move
// from it to the primary. It is what N2 will write; N1 uses it to prove every reader round-trips.
func (p Placement) ToV2() Placement {
	if p.Spread() {
		return p.clone() // already v2
	}
	v2 := p.clone()
	v2.Primary, v2.Source, v2.Names, v2.Ramp, v2.Cutover = "", "", nil, nil, nil
	v2.Legs = make(map[string]Leg, len(p.Names))
	for cl, bucket := range p.Names {
		v2.Legs[cl] = Leg{Cluster: cl, Bucket: bucket}
	}
	owner := p.Primary
	if p.State != StateActive {
		owner = p.Source
		v2.Move = &Move{Range: FullRange, From: p.Source, To: p.Primary, Ramp: v2ramp(p.Ramp), Cutover: v2cutover(p.Cutover)}
	}
	v2.Owners = []Owner{{From: FullRange.From, To: FullRange.To, Leg: owner}}
	return v2
}

func v2ramp(r *Ramp) *Ramp {
	if r == nil {
		return nil
	}
	c := *r
	c.Prefixes = slices.Clone(r.Prefixes)
	if r.Range != nil {
		rg := *r.Range
		c.Range = &rg
	}
	if r.Hold != nil {
		h := *r.Hold
		h.Prefixes = slices.Clone(r.Hold.Prefixes)
		c.Hold = &h
	}
	return &c
}

func v2cutover(ev *CutoverEvidence) *CutoverEvidence {
	if ev == nil {
		return nil
	}
	c := *ev
	return &c
}

// placementJSON is Placement without its methods, so UnmarshalJSON can decode into it.
type placementJSON Placement

// UnmarshalJSON decodes either schema and converts v2 into v1 (fromV2). It covers every JSON
// reader at once: the control plane's store, GET /v1/directory, a member's cache and the change
// records.
func (p *Placement) UnmarshalJSON(b []byte) error {
	var w placementJSON
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	pl := Placement(w)
	if err := pl.fromV2(); err != nil {
		return err
	}
	*p = pl
	return nil
}
