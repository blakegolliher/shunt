package directory

import (
	"fmt"
	"maps"
	"slices"
)

// Moves of part of a bucket (ADR-0018 N3). A move takes one range of the key hash space from the
// leg that owns it to another leg, through the states a whole-bucket migration takes. Every rule of
// a migration applies to it unchanged, because a move is applied as the two-cluster migration it is
// (MoveView): the source leg is the source, the destination leg the primary, and the ramp is limited
// to the range. At most one move is in flight per placement.

// MoveView is the two-cluster placement a move is, for the keys in its range: its state, the source
// leg as Source, the destination leg as Primary, and its ramp limited to the range. Every rule
// written for a migration (routing, the fence, the mover, cutover) applies to it as it is.
func (p *Placement) MoveView() Placement {
	m := p.Move
	from, to := p.Legs[m.From], p.Legs[m.To]
	v := Placement{State: p.State, Source: m.From, Primary: m.To,
		Names:       map[string]string{m.From: from.Bucket, m.To: to.Bucket},
		LegClusters: map[string]string{m.From: from.Cluster, m.To: to.Cluster},
		Ramp:        v2ramp(m.Ramp), Cutover: v2cutover(m.Cutover), Created: p.Created, ReadOnly: p.ReadOnly, RejectWrites: p.RejectWrites}
	if v.Ramp != nil {
		rg := m.Range
		v.Ramp.Range = &rg
	}
	return v
}

// applyMove is Apply for a transition that starts, steps or ends a move of part of a bucket.
func applyMove(p Placement, t Transition) (Placement, error) {
	fail := func(format string, args ...any) (Placement, error) {
		return p, &TransitionError{From: p.State, To: t.To, Reason: fmt.Sprintf(format, args...)}
	}
	if p.Move == nil {
		return startMove(p, t)
	}
	m := *p.Move
	if t.Range != nil && *t.Range != m.Range {
		return fail("a move of range %s is in progress; finish it before moving another", rangeText(m.Range))
	}
	if t.Leg != "" && t.Leg != m.From {
		return fail("a move of leg %s's keys is in progress; finish it before moving another leg's", m.From)
	}
	v := p.MoveView()
	if t.Target != "" && t.Target != v.ClusterOf(v.Primary) {
		return fail("the move goes to %s; a later step may only repeat that target", v.ClusterOf(v.Primary))
	}
	t.Range, t.Leg, t.Target, t.Name = nil, "", "", ""
	nv, err := Apply(v, t)
	if err != nil {
		return p, err
	}
	np := p.clone()
	switch {
	case nv.State == StateActive && t.Release:
		// The held first step never reached every proxy: nothing was routed to the destination, so
		// the placement is as it was, with the destination leg kept as a leg that owns nothing.
		np.State, np.Move = StateActive, nil
		return settle(np), nil
	case nv.State == StateActive:
		// The move is over: the destination owns the range, and a source left owning nothing goes.
		np.State, np.Move = StateActive, nil
		np.Owners = reassign(p.Owners, m.Range, m.To)
		if !slices.ContainsFunc(np.Owners, func(o Owner) bool { return o.Leg == m.From }) {
			delete(np.Legs, m.From)
		}
		return settle(np), nil
	}
	np.State = nv.State
	np.Move.Ramp, np.Move.Cutover = nv.Ramp, nv.Cutover
	if np.Move.Ramp != nil {
		rg := m.Range
		np.Move.Ramp.Range = &rg
	}
	return np, nil
}

// startMove is the first step of a move: from a plain bucket, whose primary becomes the leg owning
// every key, or from a spread one. The range must lie inside what one leg owns; the destination is
// the leg on the target cluster, or a new leg made from the target and its bucket name.
func startMove(p Placement, t Transition) (Placement, error) {
	fail := func(format string, args ...any) (Placement, error) {
		return p, &TransitionError{From: p.State, To: t.To, Reason: fmt.Sprintf(format, args...)}
	}
	if p.State != StateActive {
		return fail("a move of part of a bucket starts from ACTIVE")
	}
	if t.Range == nil && t.Leg == "" {
		return fail("name the range of keys to move, or the leg whose keys move: the bucket is spread over legs")
	}
	if t.Range != nil && t.Range.From > t.Range.To {
		return fail("the range's start is after its end")
	}
	np := p.clone()
	if !p.Spread() {
		if p.Cold != "" || p.Tier != "" {
			return fail("a tiered bucket moves whole; moving part of it is not supported")
		}
		if p.Target != "" { // expand recorded the destination
			switch {
			case t.Target == "":
				t.Target = p.Target
			case t.Target != p.Target:
				return fail("%s was expanded to %s; a different target needs expand first", p.Primary, p.Target)
			}
			if t.Name == "" {
				t.Name = p.Names[p.Target]
			}
		}
		np.Legs = map[string]Leg{p.Primary: {Cluster: p.Primary, Bucket: p.Names[p.Primary]}}
		np.Owners = []Owner{{From: FullRange.From, To: FullRange.To, Leg: p.Primary}}
		np.KeyHash = RampHash
		np.Primary, np.Names, np.Target = "", nil, ""
	}
	if t.Target == "" {
		return fail("the target cluster is required")
	}
	if t.Range == nil { // the leg's first range; a leg that owns several moves them one at a time
		if _, ok := np.Legs[t.Leg]; !ok {
			return fail("the bucket has no leg %s; its legs are %v", t.Leg, slices.Sorted(maps.Keys(np.Legs)))
		}
		for _, o := range np.Owners {
			if o.Leg == t.Leg {
				t.Range = &HashRange{From: o.From, To: o.To}
				break
			}
		}
		if t.Range == nil {
			return fail("leg %s owns no keys; there is nothing of it to move", t.Leg)
		}
	}
	rg := *t.Range
	src := ""
	for _, o := range np.Owners {
		if o.From <= rg.From && rg.To <= o.To {
			src = o.Leg
		}
	}
	switch {
	case src == "":
		return fail("the range %s spans more than one leg; move one leg's keys at a time", rangeText(rg))
	case t.Leg != "" && src != t.Leg:
		return fail("the range %s is leg %s's, not leg %s's", rangeText(rg), src, t.Leg)
	}
	// The destination: the leg holding the named bucket, or the one leg on the target cluster when
	// no bucket is named, or a new leg (ADR-0018 N3b: a cluster may hold several).
	dst, onTarget := "", 0
	for _, id := range slices.Sorted(maps.Keys(np.Legs)) {
		l := np.Legs[id]
		if l.Cluster != t.Target {
			continue
		}
		onTarget++
		if t.Name == "" || t.Name == l.Bucket {
			dst = id
		}
	}
	switch {
	case t.Name == "" && onTarget > 1:
		return fail("cluster %s holds %d legs of the bucket; name the bucket the range moves to", t.Target, onTarget)
	case dst == src:
		return fail("the range is on %s/%s already", t.Target, np.Legs[src].Bucket)
	case dst == "" && t.Name == "":
		return fail("name the bucket on %s that the range moves to", t.Target)
	case dst == "":
		dst = newLegID(np.Legs, t.Target)
		np.Legs[dst] = Leg{Cluster: t.Target, Bucket: t.Name}
	}
	if len(np.Legs) > MaxLegs {
		return fail("a placement has at most %d legs", MaxLegs)
	}
	// The move is the two-bucket migration of its range: Apply decides its first step, on roles
	// named by leg as in MoveView.
	view := Placement{State: StateActive, Primary: src, Names: map[string]string{src: np.Legs[src].Bucket}}
	tv := t
	tv.Range, tv.Leg, tv.Target, tv.Name = nil, "", dst, np.Legs[dst].Bucket
	nv, err := Apply(view, tv)
	if err != nil {
		return p, err
	}
	np.State = nv.State
	np.Move = &Move{Range: rg, From: src, To: dst, Ramp: nv.Ramp, Cutover: nv.Cutover}
	if np.Move.Ramp != nil {
		r := rg
		np.Move.Ramp.Range = &r
	}
	return np, nil
}

// newLegID names a new leg on cluster: the cluster's name, or with -2, -3, ... when a leg has it.
func newLegID(legs map[string]Leg, cluster string) string {
	id := cluster
	for n := 2; ; n++ {
		if _, taken := legs[id]; !taken && legID.MatchString(id) {
			return id
		}
		id = fmt.Sprintf("%s-%d", cluster, n)
		if len(id) > 63 {
			id = fmt.Sprintf("%s-%d", cluster[:63-len(fmt.Sprint(n))-1], n)
		}
	}
}

// reassign gives rg to leg: the owner ranges it cuts are split around it, and neighbors owned by
// the same leg are joined.
func reassign(owners []Owner, rg HashRange, leg string) []Owner {
	var out []Owner
	add := func(o Owner) {
		if n := len(out); n > 0 && out[n-1].Leg == o.Leg && out[n-1].To+1 == o.From {
			out[n-1].To = o.To
			return
		}
		out = append(out, o)
	}
	for _, o := range owners {
		switch {
		case o.To < rg.From || o.From > rg.To:
			add(o)
		default:
			if o.From < rg.From {
				add(Owner{From: o.From, To: rg.From - 1, Leg: o.Leg})
			}
			if o.From <= rg.From {
				add(Owner{From: rg.From, To: rg.To, Leg: leg})
			}
			if o.To > rg.To {
				add(Owner{From: rg.To + 1, To: o.To, Leg: o.Leg})
			}
		}
	}
	return out
}

// settle turns a placement back into its plain form when one leg owns every key and nothing moves:
// that leg is the primary, and another leg that owns nothing is the recorded target, as expand
// leaves it.
func settle(p Placement) Placement {
	if p.Move != nil || len(p.Owners) != 1 {
		return p
	}
	owner := p.Owners[0].Leg
	v := p
	v.Legs, v.Owners, v.KeyHash = nil, nil, ""
	v.Primary = p.Legs[owner].Cluster
	v.Names = map[string]string{v.Primary: p.Legs[owner].Bucket}
	for _, id := range slices.Sorted(maps.Keys(p.Legs)) {
		l := p.Legs[id]
		if _, taken := v.Names[l.Cluster]; taken {
			continue // a leg that owns nothing on the owner's cluster: not a target a plain bucket can name
		}
		v.Names[l.Cluster] = l.Bucket
		if v.Target == "" {
			v.Target = l.Cluster
		}
	}
	return v
}

// IdleLegs are the legs of p, sorted, that own no keys and take part in no move.
func IdleLegs(p Placement) []string {
	var out []string
	for _, id := range slices.Sorted(maps.Keys(p.Legs)) {
		if p.Move != nil && (id == p.Move.From || id == p.Move.To) {
			continue
		}
		if !slices.ContainsFunc(p.Owners, func(o Owner) bool { return o.Leg == id }) {
			out = append(out, id)
		}
	}
	return out
}

func rangeText(rg HashRange) string {
	return fmt.Sprintf("%016x-%016x", uint64(rg.From), uint64(rg.To))
}
