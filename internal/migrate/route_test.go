package migrate

import (
	"errors"
	"fmt"
	"strings"
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
		Ramp: &directory.Ramp{Hash: directory.RampHash, Ratio: ratio, Prefixes: prefixes}}
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
		{"cutover delete still hits both, so the source stays a subset of the primary", cutover, ClassDelete, Route{Cluster: Primary, Both: true}},
		{"cutover list does not merge", cutover, ClassList, Route{Cluster: Primary}},
	}
	for _, c := range cases {
		if got, err := Decide(c.p, c.class, "some/key"); err != nil || got != c.want {
			t.Errorf("%s: got %+v want %+v", c.name, got, c.want)
		}
	}
}

// A placement that never had a second cluster routes everything to its primary, whatever it says.
func TestNoSourceIsAlwaysPrimary(t *testing.T) {
	p := &directory.Placement{State: directory.StateMigrating, Primary: "target"}
	for _, class := range []OpClass{ClassRead, ClassWrite, ClassDelete, ClassList, ClassBucket, ClassUpload} {
		if got, err := Decide(p, class, "k"); err != nil || got != (Route{Cluster: Primary}) {
			t.Errorf("class %d: %+v", class, got)
		}
	}
}

func TestInRangePrefixes(t *testing.T) {
	r := &directory.Ramp{Hash: directory.RampHash, Prefixes: []string{"runs/2026-09/", "logs/"}}
	for _, k := range []string{"runs/2026-09/a", "logs/x"} {
		if !inRange(t, r, k) {
			t.Errorf("%q should be in range", k)
		}
	}
	for _, k := range []string{"runs/2026-08/a", "run", ""} {
		if inRange(t, r, k) {
			t.Errorf("%q should not be in range", k)
		}
	}
	if inRange(t, nil, "k") {
		t.Error("a nil ramp puts nothing in range")
	}
}

// Keys that differ only in their last bytes, as sequential names do, split by the ratio too. Plain
// FNV-1a put all of a/0000 … a/3999 on the target at 0.5, and 80% of the walkthrough's keys.
func TestInRangeSplitsSequentialKeys(t *testing.T) {
	for _, format := range []string{"a/%04d", "verify/1789589767464489064/%04d", "seed/obj-%d", "logs/2026/09/16/%d.json", "%d"} {
		for _, ratio := range []float64{0.1, 0.5, 0.9} {
			r := &directory.Ramp{Hash: directory.RampHash, Ratio: ratio}
			const n = 4000
			in := 0
			for i := 0; i < n; i++ {
				if inRange(t, r, fmt.Sprintf(format, i)) {
					in++
				}
			}
			if got := float64(in) / n; got < ratio-0.04 || got > ratio+0.04 {
				t.Errorf("%q at ratio %v: %.3f of keys on the target", format, ratio, got)
			}
		}
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
		r := &directory.Ramp{Hash: directory.RampHash, Ratio: ratio}
		cur, in := map[string]bool{}, 0
		for _, k := range keys {
			v := inRange(t, r, k)
			if v != inRange(t, r, k) {
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
		if r, err := Decide(p, ClassWrite, "dir/object-12345.bin"); err != nil || r.Cluster == 0 {
			b.Fatal("no decision")
		}
	}
}

// inRange is InRange for ramps the test built with this build's hash: an error is a test failure.
func inRange(t testing.TB, r *directory.Ramp, key string) bool {
	t.Helper()
	in, err := InRange(r, key)
	if err != nil {
		t.Fatalf("InRange(%q): %v", key, err)
	}
	return in
}

// A ramp that names a hash this build does not implement is refused wherever the hash would decide,
// and routed normally wherever it would not (ADR-0004, POC-5 amendment): a proxy never re-splits keys.
func TestUnknownRampHashIsRefused(t *testing.T) {
	foreign := &directory.Placement{State: directory.StateRamping, Primary: "target", Source: "src",
		Ramp: &directory.Ramp{Hash: "fnv1a-v0", Ratio: 0.5, Prefixes: []string{"runs/"}}}
	for _, class := range []OpClass{ClassWrite, ClassRead} {
		if _, err := Decide(foreign, class, "data/k"); !errors.Is(err, ErrUnknownRampHash) || !strings.Contains(err.Error(), `"fnv1a-v0"`) {
			t.Errorf("class %d by hash: want ErrUnknownRampHash naming the hash, got %v", class, err)
		}
		if got, err := Decide(foreign, class, "runs/k"); err != nil || got.Cluster != Primary {
			t.Errorf("class %d by prefix: %+v %v", class, got, err)
		}
	}
	for _, class := range []OpClass{ClassDelete, ClassList, ClassBucket, ClassUpload} {
		if _, err := Decide(foreign, class, "data/k"); err != nil {
			t.Errorf("class %d does not depend on the hash: %v", class, err)
		}
	}
	prefixOnly := &directory.Ramp{Hash: "fnv1a-v0", Prefixes: []string{"runs/"}}
	if in, err := InRange(prefixOnly, "data/k"); err != nil || in {
		t.Errorf("a prefix-only ramp needs no hash: %v %v", in, err)
	}
	if _, err := InRange(&directory.Ramp{Ratio: 0.5}, "k"); !errors.Is(err, ErrUnknownRampHash) {
		t.Errorf("a ramp with no hash name is refused, got %v", err)
	}
	if in, err := InRange(&directory.Ramp{Hash: "fnv1a-v0", Ratio: 1}, "k"); err != nil || !in {
		t.Errorf("at ratio 1 every key is in range whatever the hash: %v %v", in, err)
	}
}

// fnv1a-fmix64-v1 is a name for these exact values. A change that moves any of them must take a new
// name in directory.RampHash, or proxies of two builds would split the same ramp differently.
func TestRampHashIsPinned(t *testing.T) {
	if directory.RampHash != "fnv1a-fmix64-v1" {
		t.Fatalf("RampHash is %q: update the pinned values below only together with a new name", directory.RampHash)
	}
	for key, want := range map[string]uint64{
		"":                                0xefd01f60ba992926,
		"a/0000":                          0x1bef7c837bb43864,
		"a/0001":                          0x887b798489aca7c0,
		"verify/1789589767464489064/0012": 0x818f437aa9bfa4c6,
		"runs/2026-09/ckpt-000017.pt":     0xc30d8369350a63cb,
	} {
		if got := rampHash(key); got != want {
			t.Errorf("rampHash(%q) = %#x, want %#x", key, got, want)
		}
	}
}

// TestHeldKeys is ADR-0016's routing: a key inside a held step but outside the ramp in force is
// refused for writes and read target first; a key outside both still goes to the source only, and
// a key inside the ramp is untouched by the hold.
func TestHeldKeys(t *testing.T) {
	p := &directory.Placement{State: directory.StateRamping, Primary: "minio", Source: "garage",
		Ramp: &directory.Ramp{Hash: directory.RampHash, Prefixes: []string{"old/"}, Hold: &directory.RampHold{Prefixes: []string{"new/"}}}}
	cases := []struct {
		key   string
		class OpClass
		want  Route
	}{
		{"new/a", ClassWrite, Route{Cluster: Primary, Held: true}},
		{"new/a", ClassRead, Route{Cluster: Primary, Fallback: true}},
		{"new/a", ClassDelete, Route{Cluster: Primary, Both: true}},
		{"new/a", ClassList, Route{Cluster: Primary, Merge: true}},
		{"old/a", ClassWrite, Route{Cluster: Primary}},
		{"old/a", ClassRead, Route{Cluster: Primary, Fallback: true}},
		{"other/a", ClassWrite, Route{Cluster: Source}},
		{"other/a", ClassRead, Route{Cluster: Source}},
	}
	for _, c := range cases {
		got, err := Decide(p, c.class, c.key)
		if err != nil || got != c.want {
			t.Errorf("%s class %d: got %+v %v, want %+v", c.key, c.class, got, err, c.want)
		}
	}

	// Held from ACTIVE with hold.ratio 1 (a held migrate start): every write is held.
	all := &directory.Placement{State: directory.StateRamping, Primary: "minio", Source: "garage",
		Ramp: &directory.Ramp{Hash: directory.RampHash, Hold: &directory.RampHold{Ratio: 1}}}
	for _, k := range []string{"a", "b/c", "zzz"} {
		if got, _ := Decide(all, ClassWrite, k); !got.Held {
			t.Errorf("%s: not held under hold.ratio 1: %+v", k, got)
		}
	}

	// A hash-split hold holds exactly the keys between the two ratios.
	hp := &directory.Placement{State: directory.StateRamping, Primary: "minio", Source: "garage",
		Ramp: &directory.Ramp{Hash: directory.RampHash, Ratio: 0.3, Hold: &directory.RampHold{Ratio: 0.6}}}
	for i := range 2000 {
		k := fmt.Sprintf("k/%04d", i)
		in03, _ := InRange(&directory.Ramp{Hash: directory.RampHash, Ratio: 0.3}, k)
		in06, _ := InRange(&directory.Ramp{Hash: directory.RampHash, Ratio: 0.6}, k)
		got, _ := Decide(hp, ClassWrite, k)
		switch {
		case in03 && (got.Held || got.Cluster != Primary):
			t.Fatalf("%s is in force and was routed %+v", k, got)
		case !in03 && in06 && !got.Held:
			t.Fatalf("%s is between the ratios and was not held: %+v", k, got)
		case !in06 && (got.Held || got.Cluster != Source):
			t.Fatalf("%s is outside the hold and was routed %+v", k, got)
		}
	}
}

// A ramp limited to a hash range moves only keys inside it: its ratio is a share of the range,
// counted from the range's start, and a prefix moves only the prefixed keys inside it (ADR-0018 N3).
func TestRampLimitedToARange(t *testing.T) {
	lower := directory.HashRange{From: 0, To: 1<<63 - 1}
	var inside, outside []string
	for i := 0; len(inside) < 400 || len(outside) < 50; i++ {
		k := fmt.Sprintf("k/%05d", i)
		if InRangeHash(lower, k) {
			inside = append(inside, k)
		} else {
			outside = append(outside, k)
		}
	}
	all := &directory.Ramp{Hash: directory.RampHash, Ratio: 1, Range: &lower}
	half := &directory.Ramp{Hash: directory.RampHash, Ratio: 0.5, Range: &lower, Prefixes: []string{"p/"}}
	moved := 0
	for _, k := range inside {
		if in, err := InRange(all, k); err != nil || !in {
			t.Fatalf("%s is inside the range: %v %v", k, in, err)
		}
		if in, _ := InRange(half, k); in {
			moved++
		}
	}
	if moved < 160 || moved > 240 { // about half of 400
		t.Fatalf("ratio 0.5 of the range moved %d of %d keys", moved, len(inside))
	}
	for _, k := range outside {
		for _, r := range []*directory.Ramp{all, half, {Hash: directory.RampHash, Ratio: 1, Range: &lower, Hold: &directory.RampHold{Ratio: 1}}} {
			if in, _ := InRange(r, k); in {
				t.Fatalf("%s is outside the range but moved", k)
			}
			if held, _ := InHold(r, k); held {
				t.Fatalf("%s is outside the range but held", k)
			}
		}
		if in, _ := InRange(half, "p/"+k); in && !InRangeHash(lower, "p/"+k) {
			t.Fatalf("a prefix moved p/%s from outside the range", k)
		}
	}
	if _, err := InRange(&directory.Ramp{Hash: "other-v9", Ratio: 1, Range: &lower}, "x"); err == nil {
		t.Fatal("a ranged ramp by an unknown hash must be refused: the range needs the hash")
	}
}
