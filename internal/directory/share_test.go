package directory

import (
	"strings"
	"testing"
	"time"
)

func mustHash(t *testing.T, s string) Hash {
	t.Helper()
	var h Hash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		t.Fatal(err)
	}
	return h
}

// LeadingShare is the UI's leadingShare, bit for bit: these are the web client's answers
// (web/src/hashRange.ts, before the share moved to the server), so a share asked for in either
// place names the same keys. A share that rounds to all of the range is the whole range.
func TestLeadingShareMatchesTheUI(t *testing.T) {
	for _, tc := range []struct {
		from, to string
		share    float64
		want     string
	}{
		{"0000000000000000", "ffffffffffffffff", 0.5, "7fffffffffffffff"},
		{"0000000000000000", "ffffffffffffffff", 0.25, "3fffffffffffffff"},
		{"0000000000000000", "7ffffffffffffffe", 0.5, "3ffffffffffffffe"},
		{"7fffffffffffffff", "ffffffffffffffff", 0.33, "aa3d70a3d70a3d6e"},
		{"0000000000000000", "ffffffffffffffff", 0.00001, "00068db8bac710ca"},
		{"0000000000000010", "0000000000000013", 0.1, "0000000000000010"},
		{"0000000000000000", "ffffffffffffffff", 0.9999, "fff972474538ef33"},
		{"8000000000000000", "ffffffffffffffff", 0.25, "9fffffffffffffff"},
		{"0000000000000010", "0000000000000013", 0.0001, "0000000000000010"},
		{"0000000000000000", "ffffffffffffffff", 0.99996, "ffffffffffffffff"},
		{"0000000000000000", "ffffffffffffffff", 1, "ffffffffffffffff"},
	} {
		r := HashRange{From: mustHash(t, tc.from), To: mustHash(t, tc.to)}
		if got := LeadingShare(r, tc.share); got.From != r.From || got.To != mustHash(t, tc.want) {
			t.Errorf("LeadingShare(%s-%s, %v) = %s, want %s-%s", tc.from, tc.to, tc.share, rangeText(got), tc.from, tc.want)
		}
	}
}

// Half of one leg to a cluster the bucket has no leg on (ADR-0018 N3): the share is of the leg's
// range, the new leg is the named bucket there, and a later step may repeat the same leg and share,
// as a retyped command line would, but not another share. A share is refused with a range, outside
// 0..1, and without a leg on a spread bucket; on a plain bucket it is of the whole key space.
func TestMoveAShareOfALeg(t *testing.T) {
	f := &File{Clusters: sampleClusters(t), Tenants: map[string]Tenant{}, Placements: map[string]Placement{}}
	if err := f.CreateSpread("default", "data01", []Leg{{Cluster: "garage", Bucket: "data01"}, {Cluster: "minio", Bucket: "data01"}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	p := f.Placements["default/data01"]
	var garage HashRange
	for _, o := range p.Owners {
		if o.Leg == "garage" {
			garage = HashRange{From: o.From, To: o.To}
		}
	}
	moved := step(t, p, Transition{To: StateRamping, Target: "cold", Name: "data01", Leg: "garage", Share: 0.5, Ratio: 0.25})
	if m := moved.Move; m.From != "garage" || m.Range != LeadingShare(garage, 0.5) || moved.Legs[m.To].Cluster != "cold" || moved.Legs[m.To].Bucket != "data01" {
		t.Fatalf("half of leg garage to cold: move %+v, legs %+v", moved.Move, moved.Legs)
	}
	if m := moved.Move.Range; m.To-m.From+1 != (garage.To-garage.From+1)/2 {
		t.Fatalf("the move's range %s is not half of garage's %s", rangeText(m), rangeText(garage))
	}
	moved = step(t, moved, Transition{To: StateRamping, Leg: "garage", Share: 0.5, Ratio: 0.5})
	if _, err := Apply(moved, Transition{To: StateRamping, Leg: "garage", Share: 0.4, Ratio: 1}); err == nil || !strings.Contains(err.Error(), "may repeat only the share") {
		t.Fatalf("another share on a later step: %v", err)
	}

	rg := LeadingShare(garage, 0.5)
	for name, tc := range map[string]struct {
		tr   Transition
		want string
	}{
		"over one":     {Transition{To: StateRamping, Target: "cold", Name: "d", Leg: "garage", Share: 1.5, Ratio: 1}, "outside 0..1"},
		"negative":     {Transition{To: StateRamping, Target: "cold", Name: "d", Leg: "garage", Share: -0.5, Ratio: 1}, "outside 0..1"},
		"with a range": {Transition{To: StateRamping, Target: "cold", Name: "d", Range: &rg, Share: 0.5, Ratio: 1}, "names the keys to move already"},
		"no leg":       {Transition{To: StateRamping, Target: "cold", Name: "d", Share: 0.5, Ratio: 1}, "or the leg whose keys move"},
	} {
		if _, err := Apply(p, tc.tr); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}

	plain := Placement{State: StateActive, Primary: "garage", Names: map[string]string{"garage": "data"}}
	half := step(t, plain, Transition{To: StateRamping, Target: "minio", Name: "data-b", Share: 0.5, Ratio: 0.25})
	if !half.Spread() || half.Move.Range != LeadingShare(FullRange, 0.5) {
		t.Fatalf("half of a plain bucket: %+v", half.Move)
	}
	whole := step(t, plain, Transition{To: StateRamping, Target: "minio", Name: "data-b", Share: 1, Ratio: 0.25})
	if whole.Spread() || whole.Primary != "minio" || whole.Source != "garage" {
		t.Fatalf("all of a plain bucket is a migration: %+v", whole)
	}
	if _, err := Apply(whole, Transition{To: StateRamping, Share: 0.5, Ratio: 1}); err == nil || !strings.Contains(err.Error(), "on the move's first step") {
		t.Fatalf("a share on a migration's later step: %v", err)
	}
}
