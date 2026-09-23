package directory

import (
	"fmt"
	"slices"
	"strings"
)

// Transition is a requested state change for one placement.
type Transition struct {
	To       string
	Target   string   // leaving ACTIVE: the cluster that becomes primary
	Name     string   // leaving ACTIVE: the backend bucket name on Target
	Ratio    float64  // RAMPING: hash ratio of keys whose writes go to the new primary
	Prefixes []string // RAMPING: key prefixes whose writes go to the new primary
	// Cutover is the evidence `shunt cutover` checked, recorded on MIGRATING → CUTOVER.
	Cutover *CutoverEvidence
	// Hold writes a step that moves writes to the new primary as a hold first (ADR-0016): the ramp in
	// force is kept and the step is recorded as ramp.hold. A held MIGRATING step is RAMPING with
	// hold.ratio 1 until the control node writes MIGRATING itself.
	Hold bool
	// Release undoes a hold that did not reach every proxy: the step is dropped, and a placement held
	// from ACTIVE goes back to ACTIVE with its target recorded, as it was before the step.
	Release bool
	// Complete writes the step a hold was for, and only if the placement is still held when the
	// write happens: a hold released concurrently must not be completed, or the step would move
	// writes that no proxy held.
	Complete bool
	// Range, leaving ACTIVE, moves only the keys whose hash it holds (ADR-0018 N3): from the leg that
	// owns them to the target, which becomes a new leg unless one is on that cluster already. Later
	// steps of the move name it again or not at all.
	Range *HashRange
}

// TransitionError is an illegal or malformed state change. It names both states.
type TransitionError struct {
	From, To string
	Reason   string
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("directory: illegal transition %s -> %s: %s", e.From, e.To, e.Reason)
}

// legal is the transition table (docs/DESIGN.md §2.3, §2.5): ACTIVE → RAMPING → MIGRATING →
// CUTOVER → ACTIVE with RAMPING skippable, plus RAMPING → RAMPING for a ramp step. Rolling back is
// a reconcile, never a state jump, so every other pair is refused (including RAMPING → ACTIVE).
var legal = map[[2]string]bool{
	{StateActive, StateRamping}:    true,
	{StateActive, StateMigrating}:  true,
	{StateRamping, StateRamping}:   true,
	{StateRamping, StateMigrating}: true,
	{StateMigrating, StateCutover}: true,
	{StateCutover, StateActive}:    true,
}

// next lists the states reachable from state.
func next(state string) []string {
	var out []string
	for _, to := range States {
		if legal[[2]string{state, to}] {
			out = append(out, to)
		}
	}
	return out
}

// Apply returns p after transition t, or a *TransitionError. It never modifies p.
func Apply(p Placement, t Transition) (Placement, error) {
	fail := func(format string, args ...any) (Placement, error) {
		return p, &TransitionError{From: p.State, To: t.To, Reason: fmt.Sprintf(format, args...)}
	}
	if t.Range != nil && *t.Range == FullRange && !p.Spread() {
		t.Range = nil // the whole bucket: a migration as before
	}
	if p.Spread() || t.Range != nil {
		return applyMove(p, t)
	}
	if t.Release {
		return release(p)
	}
	if !slices.Contains(States, t.To) {
		return fail("unknown state %q; states are %s", t.To, strings.Join(States, ", "))
	}
	if t.Hold && t.To != StateRamping && t.To != StateMigrating {
		return fail("only a step to RAMPING or MIGRATING is held")
	}
	if p.Ramp != nil && p.Ramp.Hold != nil && t.Hold {
		return fail("a held step is already in progress; repeat that step to complete it first")
	}
	if t.Complete && !p.Held() {
		return fail("the held step was released or completed meanwhile; nothing is held to complete")
	}
	if !legal[[2]string{p.State, t.To}] {
		next := next(p.State)
		return fail("allowed from %s: %s", p.State, strings.Join(next, ", "))
	}
	leaving := p.State == StateActive
	if !leaving && (t.Target != "" || t.Name != "") {
		return fail("a target cluster and backend name are only given when leaving ACTIVE")
	}
	if leaving && p.Target != "" {
		// shunt expand recorded the target: the first step inherits it, and may only repeat it.
		switch {
		case t.Target == "":
			t.Target = p.Target
		case t.Target != p.Target:
			return fail("%s was expanded to %s; a different target needs expand first", p.Primary, p.Target)
		}
		if t.Name == "" {
			t.Name = p.Names[p.Target]
		} else if t.Name != p.Names[p.Target] {
			return fail("the target backend bucket is %s/%s (recorded by expand), not %s", p.Target, p.Names[p.Target], t.Name)
		}
	}
	if t.Cutover != nil && t.To != StateCutover {
		return fail("cutover evidence only applies to CUTOVER")
	}
	if t.To != StateRamping && (t.Ratio != 0 || len(t.Prefixes) > 0) {
		return fail("a ramp ratio or prefix only applies to RAMPING")
	}
	np := p.clone()
	np.State = t.To
	switch {
	case leaving:
		if t.Target == "" || t.Name == "" {
			return fail("the target cluster and its backend bucket name are required")
		}
		if t.Target == p.Primary {
			return fail("target %q is already the primary", t.Target)
		}
		if np.Names == nil {
			np.Names = map[string]string{}
		}
		np.Source, np.Primary, np.Target = p.Primary, t.Target, ""
		np.Names[t.Target] = t.Name
		if t.To == StateRamping {
			if t.Ratio == 0 && len(t.Prefixes) == 0 {
				return fail("RAMPING needs a ratio or at least one prefix")
			}
			np.Ramp = &Ramp{Hash: RampHash, Ratio: t.Ratio, Prefixes: slices.Clone(t.Prefixes)}
		}
	case p.State == StateRamping && t.To == StateRamping:
		old := Ramp{}
		if p.Ramp != nil {
			old = *p.Ramp
		}
		if old.Hash != RampHash {
			return fail("the ramp splits keys by hash %q, which this build does not implement (it implements %s); extending it would re-split keys: finish it with the build that started it", old.Hash, RampHash)
		}
		r := Ramp{Hash: old.Hash, Ratio: old.Ratio, Prefixes: slices.Clone(old.Prefixes)}
		if t.Ratio != 0 {
			if t.Ratio < old.Ratio {
				return fail("the ramp only grows: ratio %v is below the current %v (shrinking needs a reconcile first)", t.Ratio, old.Ratio)
			}
			r.Ratio = t.Ratio
		}
		for _, pre := range t.Prefixes {
			if !slices.Contains(r.Prefixes, pre) {
				r.Prefixes = append(r.Prefixes, pre)
			}
		}
		if r.Ratio == old.Ratio && len(r.Prefixes) == len(old.Prefixes) {
			return fail("the ramp step changes nothing; give a higher ratio or a new prefix")
		}
		if h := old.Hold; h != nil && !t.Hold {
			// Completing a held step: it may move only the keys that were held, never more.
			for _, pre := range r.Prefixes {
				if !slices.Contains(old.Prefixes, pre) && !slices.Contains(h.Prefixes, pre) {
					return fail("prefix %q was not part of the held step; repeat the held step to complete it first", pre)
				}
			}
			if r.Ratio > h.Ratio && r.Ratio > old.Ratio {
				return fail("ratio %v is beyond the held step's %v; repeat the held step to complete it first", r.Ratio, h.Ratio)
			}
		}
		np.Ramp = &r
	case t.To == StateMigrating:
		if p.Held() && p.Ramp.Hold.Ratio < 1 && !t.Hold {
			return fail("a held ramp step is in progress; repeat that ramp step to complete it before MIGRATING")
		}
		np.Ramp = nil
	case t.To == StateCutover:
		np.Cutover = t.Cutover
	case p.State == StateCutover && t.To == StateActive:
		if p.Source != p.Primary && p.Source != p.Cold {
			delete(np.Names, p.Source)
		}
		np.Source, np.Cutover = "", nil
	}
	if t.Hold {
		// Keep the ramp in force (none, leaving ACTIVE) and record the step as its hold.
		base := Ramp{Hash: RampHash}
		if p.State == StateRamping && p.Ramp != nil {
			base = *p.Ramp
			base.Prefixes = slices.Clone(p.Ramp.Prefixes)
		}
		h := &RampHold{Ratio: 1}
		if np.Ramp != nil {
			h = &RampHold{Ratio: np.Ramp.Ratio, Prefixes: slices.Clone(np.Ramp.Prefixes)}
		}
		base.Hold = h
		np.State, np.Ramp = StateRamping, &base
	}
	return np, nil
}

// release undoes a held step (Transition.Release, ADR-0016).
func release(p Placement) (Placement, error) {
	if p.State != StateRamping || p.Ramp == nil || p.Ramp.Hold == nil {
		return p, &TransitionError{From: p.State, To: p.State, Reason: "there is no held step to release"}
	}
	np := p.clone()
	if p.Ramp.Ratio == 0 && len(p.Ramp.Prefixes) == 0 {
		// Held from ACTIVE: nothing was ever routed to the target. Back to ACTIVE, target recorded.
		np.State, np.Primary, np.Source, np.Target, np.Ramp = StateActive, p.Source, "", p.Primary, nil
		return np, nil
	}
	np.Ramp.Hold = nil
	return np, nil
}

// Held reports whether p has a ramp step written but not yet in force: its own, or its move's.
func (p Placement) Held() bool {
	if p.Move != nil {
		return p.Move.Ramp != nil && p.Move.Ramp.Hold != nil
	}
	return p.Ramp != nil && p.Ramp.Hold != nil
}
