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
	if !slices.Contains(States, t.To) {
		return fail("unknown state %q; states are %s", t.To, strings.Join(States, ", "))
	}
	if !legal[[2]string{p.State, t.To}] {
		next := next(p.State)
		return fail("allowed from %s: %s", p.State, strings.Join(next, ", "))
	}
	leaving := p.State == StateActive
	if !leaving && (t.Target != "" || t.Name != "") {
		return fail("a target cluster and backend name are only given when leaving ACTIVE")
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
		np.Source, np.Primary = p.Primary, t.Target
		np.Names[t.Target] = t.Name
		if t.To == StateRamping {
			if t.Ratio == 0 && len(t.Prefixes) == 0 {
				return fail("RAMPING needs a ratio or at least one prefix")
			}
			np.Ramp = &Ramp{Ratio: t.Ratio, Prefixes: slices.Clone(t.Prefixes)}
		}
	case p.State == StateRamping && t.To == StateRamping:
		old := Ramp{}
		if p.Ramp != nil {
			old = *p.Ramp
		}
		r := Ramp{Ratio: old.Ratio, Prefixes: slices.Clone(old.Prefixes)}
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
		np.Ramp = &r
	case t.To == StateMigrating:
		np.Ramp = nil
	case p.State == StateCutover && t.To == StateActive:
		if p.Source != p.Primary && p.Source != p.Cold {
			delete(np.Names, p.Source)
		}
		np.Source = ""
	}
	return np, nil
}
