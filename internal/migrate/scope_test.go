package migrate

import (
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/directory"
)

// scoped is a spread placement with nested prefix rules whose tables differ (ADR-0020), and a move
// of half of the scope of the empty prefix.
func scoped() *directory.Placement {
	half := directory.HashRange{From: 0, To: 1<<63 - 1}
	return &directory.Placement{State: directory.StateRamping, KeyHash: directory.RampHash,
		Legs: map[string]directory.Leg{
			"hot": {Cluster: "vast01", Bucket: "d"}, "warm": {Cluster: "vast02", Bucket: "d"}, "cold": {Cluster: "vast03", Bucket: "d"},
		},
		Owners: directory.EvenOwners([]string{"hot", "warm"}),
		Prefixes: []directory.PrefixRule{
			{Prefix: "archive/", Owners: directory.EvenOwners([]string{"cold"})},
			{Prefix: "archive/hot/", Owners: directory.EvenOwners([]string{"hot"})},
			{Prefix: "logs/", Owners: directory.EvenOwners([]string{"warm", "cold"})},
		},
		Move: &directory.Move{Range: half, From: "hot", To: "cold", Ramp: &directory.Ramp{Hash: directory.RampHash, Ratio: 0.5, Range: &half}},
	}
}

// bruteOwner is the owner by the definition: the longest rule the key starts with, else the
// placement's own table; then the range holding the key's hash.
func bruteOwner(p *directory.Placement, key string) (scope, leg string) {
	owners := p.Owners
	for _, r := range p.Prefixes {
		if strings.HasPrefix(key, r.Prefix) && len(r.Prefix) >= len(scope) {
			scope, owners = r.Prefix, r.Owners
		}
	}
	h := directory.Hash(rampHash(key))
	for _, o := range owners {
		if o.From <= h && h <= o.To {
			return scope, o.Leg
		}
	}
	return scope, ""
}

func TestOwnerOfWithPrefixRules(t *testing.T) {
	p := scoped()
	for key, want := range map[string]string{"archive/x": "cold", "archive/hot/x": "hot", "archive/ho": "cold"} {
		if got, err := OwnerOf(p, key); err != nil || got != want {
			t.Errorf("owner of %q: %s %v, want %s", key, got, err, want)
		}
	}
	// A key of a rule's scope is never in a move of the scope of the empty prefix, whatever its hash.
	for i := range 2000 {
		k := "archive/k" + string(rune('a'+i%26)) + strings.Repeat("z", i/26)
		if InMove(p, k) {
			t.Fatalf("%q is in the move, but its scope is archive/", k)
		}
		if n, err := Narrow(p, k); err != nil || n.Primary != "vast03" {
			t.Fatalf("%q narrowed to %+v %v", k, n, err)
		}
	}
}

// FuzzOwnerOf holds the owner function and move membership to their definitions for any key.
func FuzzOwnerOf(f *testing.F) {
	for _, k := range []string{"", "a", "archive/", "archive/hot/1", "archive/ho", "logs/x", "logs", "data/2026/09/x"} {
		f.Add(k)
	}
	p := scoped()
	f.Fuzz(func(t *testing.T, key string) {
		scope, want := bruteOwner(p, key)
		got, err := OwnerOf(p, key)
		if err != nil || got != want {
			t.Fatalf("owner of %q: %q %v, want %q", key, got, err, want)
		}
		if s, _ := p.Scope(key); s != scope {
			t.Fatalf("scope of %q: %q, want %q", key, s, scope)
		}
		if in := InMove(p, key); in != (scope == "" && InRangeHash(p.Move.Range, key)) {
			t.Fatalf("%q in the move: %v", key, in)
		}
		if InMove(p, key) && want != p.Move.From {
			t.Fatalf("%q is in the move from %s but owned by %s", key, p.Move.From, want)
		}
	})
}

func BenchmarkOwnerOfWithRules(b *testing.B) {
	p := scoped()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := OwnerOf(p, "logs/2026/09/23/part-00017.json"); err != nil {
			b.Fatal(err)
		}
	}
}
