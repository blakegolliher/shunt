package migrate

import (
	"fmt"
	"testing"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/s3"
)

// Every operation the classifier can produce must have a routing class. An operation added to
// internal/s3 without a decision here fails this test rather than silently routing to the primary.
func TestEveryOpHasAClass(t *testing.T) {
	ops := s3.Ops()
	if len(ops) < 90 {
		t.Fatalf("only %d ops: the table shrank unexpectedly", len(ops))
	}
	for _, op := range ops {
		if Class(op) == 0 {
			t.Errorf("%s has no routing class", op)
		}
	}
}

func ramping(ratio float64, prefixes ...string) *directory.Placement {
	return &directory.Placement{State: directory.StateRamping, Primary: "target", Source: "src",
		Ramp: &directory.Ramp{Ratio: ratio, Prefixes: prefixes}}
}

// The table in docs/DESIGN.md §2.5, asserted row by row.
func TestRoutingTable(t *testing.T) {
	active := &directory.Placement{State: directory.StateActive, Primary: "target"}
	migrating := &directory.Placement{State: directory.StateMigrating, Primary: "target", Source: "src"}
	cutover := &directory.Placement{State: directory.StateCutover, Primary: "target", Source: "src"}
	all, none := ramping(1, ""), ramping(0)

	cases := []struct {
		name  string
		p     *directory.Placement
		class OpClass
		want  Route
	}{
		{"active write", active, ClassWrite, Route{Cluster: Primary}},
		{"active read", active, ClassRead, Route{Cluster: Primary}},
		{"active delete", active, ClassDelete, Route{Cluster: Primary}},
		{"active list", active, ClassList, Route{Cluster: Primary}},

		{"ramping write in range", all, ClassWrite, Route{Cluster: Primary}},
		{"ramping write out of range", none, ClassWrite, Route{Cluster: Source}},
		{"ramping read in range", all, ClassRead, Route{Cluster: Primary, Fallback: true}},
		{"ramping read out of range", none, ClassRead, Route{Cluster: Source}},
		{"ramping delete", none, ClassDelete, Route{Cluster: Primary, Both: true}},
		{"ramping list", none, ClassList, Route{Cluster: Primary, Merge: true}},
		{"ramping bucket config", none, ClassBucket, Route{Cluster: Primary}},
		{"ramping upload is pinned by its id", none, ClassUpload, Route{Cluster: Primary}},

		{"migrating write", migrating, ClassWrite, Route{Cluster: Primary}},
		{"migrating read falls back", migrating, ClassRead, Route{Cluster: Primary, Fallback: true}},
		{"migrating delete hits both", migrating, ClassDelete, Route{Cluster: Primary, Both: true}},
		{"migrating list merges", migrating, ClassList, Route{Cluster: Primary, Merge: true}},

		{"cutover write", cutover, ClassWrite, Route{Cluster: Primary}},
		{"cutover read does not fall back", cutover, ClassRead, Route{Cluster: Primary}},
		{"cutover delete is single", cutover, ClassDelete, Route{Cluster: Primary}},
	}
	for _, c := range cases {
		if got := Decide(c.p, c.class, "some/key"); got != c.want {
			t.Errorf("%s: got %+v want %+v", c.name, got, c.want)
		}
	}
}

// A placement that never had a second cluster routes everything to its primary, whatever it says.
func TestNoSourceIsAlwaysPrimary(t *testing.T) {
	p := &directory.Placement{State: directory.StateMigrating, Primary: "target"}
	for _, class := range []OpClass{ClassRead, ClassWrite, ClassDelete, ClassList, ClassBucket, ClassUpload} {
		if got := Decide(p, class, "k"); got != (Route{Cluster: Primary}) {
			t.Errorf("class %d: %+v", class, got)
		}
	}
}

func TestInRangePrefixes(t *testing.T) {
	r := &directory.Ramp{Prefixes: []string{"runs/2026-09/", "logs/"}}
	for _, k := range []string{"runs/2026-09/a", "logs/x"} {
		if !InRange(r, k) {
			t.Errorf("%q should be in range", k)
		}
	}
	for _, k := range []string{"runs/2026-08/a", "run", ""} {
		if InRange(r, k) {
			t.Errorf("%q should not be in range", k)
		}
	}
	if InRange(nil, "k") {
		t.Error("a nil ramp puts nothing in range")
	}
}

// The ramp is a function of the key: the same key always lands on the same side, and raising the
// ratio only ever moves keys toward the target, never back (docs/DESIGN.md §2.5, ADR-0004).
func TestInRangeIsStableAndMonotonic(t *testing.T) {
	keys := make([]string, 2000)
	for i := range keys {
		keys[i] = fmt.Sprintf("dir/%04d/object-%d.bin", i%37, i)
	}
	ratios := []float64{0, 0.01, 0.05, 0.25, 0.5, 0.9, 1}
	var prev map[string]bool
	for _, ratio := range ratios {
		r := &directory.Ramp{Ratio: ratio}
		cur, in := map[string]bool{}, 0
		for _, k := range keys {
			v := InRange(r, k)
			if v != InRange(r, k) {
				t.Fatalf("%q: not stable at ratio %v", k, ratio)
			}
			cur[k] = v
			if v {
				in++
			}
		}
		for k, was := range prev {
			if was && !cur[k] {
				t.Fatalf("%q left the target range when the ratio rose to %v", k, ratio)
			}
		}
		if want := float64(len(keys)) * ratio; ratio > 0 && ratio < 1 {
			if diff := float64(in) - want; diff < -0.08*float64(len(keys)) || diff > 0.08*float64(len(keys)) {
				t.Errorf("ratio %v put %d/%d keys on the target, want about %.0f", ratio, in, len(keys), want)
			}
		}
		if ratio == 0 && in != 0 {
			t.Errorf("ratio 0 put %d keys on the target", in)
		}
		if ratio == 1 && in != len(keys) {
			t.Errorf("ratio 1 left %d keys on the source", len(keys)-in)
		}
		prev = cur
	}
}

func BenchmarkDecide(b *testing.B) {
	p := ramping(0.25, "runs/")
	b.ReportAllocs()
	for b.Loop() {
		if Decide(p, ClassWrite, "dir/object-12345.bin").Cluster == 0 {
			b.Fatal("no decision")
		}
	}
}
