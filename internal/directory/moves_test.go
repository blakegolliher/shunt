package directory

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

var lowerHalf = HashRange{From: 0, To: 1<<63 - 1}

// step applies a transition and fails the test on a refusal.
func step(t *testing.T, p Placement, tr Transition) Placement {
	t.Helper()
	np, err := Apply(p, tr)
	if err != nil {
		t.Fatalf("%s -> %s: %v", p.State, tr.To, err)
	}
	return np
}

// A plain bucket moves half its keys to a new leg, through every state a migration takes, and ends
// spread over two legs; moving the other half ends plain again on the new cluster (ADR-0018 N3).
func TestMoveHalfABucketThenTheRest(t *testing.T) {
	p := Placement{State: StateActive, Primary: "garage", Names: map[string]string{"garage": "data"}}
	p = step(t, p, Transition{To: StateRamping, Target: "minio", Name: "data-b", Range: &lowerHalf, Ratio: 0.25})
	if !p.Spread() || p.State != StateRamping || p.Primary != "" || len(p.Legs) != 2 || len(p.Owners) != 1 || p.Owners[0].Leg != "garage" {
		t.Fatalf("a plain bucket starting a partial move: %+v", p)
	}
	if m := p.Move; m.From != "garage" || m.To != "minio" || m.Range != lowerHalf || m.Ramp == nil || m.Ramp.Ratio != 0.25 || *m.Ramp.Range != lowerHalf {
		t.Fatalf("the move: %+v ramp %+v", m, m.Ramp)
	}
	v := p.MoveView()
	if v.Source != "garage" || v.Primary != "minio" || v.Names["minio"] != "data-b" || v.State != StateRamping || *v.Ramp.Range != lowerHalf {
		t.Fatalf("move view: %+v", v)
	}
	// Every rule of a migration holds for the move: the ramp only grows.
	if _, err := Apply(p, Transition{To: StateRamping, Ratio: 0.1}); err == nil || !strings.Contains(err.Error(), "only grows") {
		t.Fatalf("shrinking the move's ramp: %v", err)
	}
	p = step(t, p, Transition{To: StateRamping, Ratio: 1})
	p = step(t, p, Transition{To: StateMigrating})
	if p.Move.Ramp != nil {
		t.Fatalf("MIGRATING kept a ramp: %+v", p.Move)
	}
	ev := &CutoverEvidence{At: time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), Window: time.Minute}
	p = step(t, p, Transition{To: StateCutover, Cutover: ev})
	if p.Move.Cutover == nil || p.Move.Cutover.Window != time.Minute {
		t.Fatalf("cutover evidence: %+v", p.Move)
	}
	p = step(t, p, Transition{To: StateActive})
	want := []Owner{{From: 0, To: 1<<63 - 1, Leg: "minio"}, {From: 1 << 63, To: FullRange.To, Leg: "garage"}}
	if p.State != StateActive || p.Move != nil || !reflect.DeepEqual(p.Owners, want) || len(p.Legs) != 2 {
		t.Fatalf("after the first move: %+v", p)
	}
	upper := HashRange{From: 1 << 63, To: FullRange.To}
	p = step(t, p, Transition{To: StateMigrating, Target: "minio", Range: &upper})
	p = step(t, p, Transition{To: StateCutover, Cutover: ev})
	p = step(t, p, Transition{To: StateActive})
	if p.Spread() || p.Primary != "minio" || len(p.Names) != 1 || p.Names["minio"] != "data-b" || p.Target != "" {
		t.Fatalf("after moving every key: %+v", p)
	}
}

// A held first step that never reached every proxy is released to where it started: a plain bucket
// with the destination recorded as its target, as expand leaves it.
func TestMoveReleaseAndRefusals(t *testing.T) {
	plain := Placement{State: StateActive, Primary: "garage", Names: map[string]string{"garage": "data"}}
	held := step(t, plain, Transition{To: StateRamping, Target: "minio", Name: "data-b", Range: &lowerHalf, Ratio: 0.5, Hold: true})
	if !held.Held() || held.Move.Ramp.Ratio != 0 || held.Move.Ramp.Hold.Ratio != 0.5 {
		t.Fatalf("held first step: %+v", held.Move.Ramp)
	}
	back := step(t, held, Transition{Release: true})
	if back.Spread() || back.State != StateActive || back.Primary != "garage" || back.Target != "minio" || back.Names["minio"] != "data-b" {
		t.Fatalf("released: %+v", back)
	}

	spread := Placement{State: StateActive, KeyHash: RampHash,
		Legs:   map[string]Leg{"g": {Cluster: "garage", Bucket: "a"}, "m": {Cluster: "minio", Bucket: "b"}},
		Owners: []Owner{{From: 0, To: 1<<63 - 1, Leg: "g"}, {From: 1 << 63, To: FullRange.To, Leg: "m"}}}
	across := HashRange{From: 1 << 62, To: 1<<63 + 5}
	for name, tr := range map[string]Transition{
		"no range":                     {To: StateMigrating, Target: "cold", Name: "c"},
		"two legs' keys":               {To: StateMigrating, Target: "cold", Name: "c", Range: &across},
		"onto its own leg":             {To: StateMigrating, Target: "garage", Range: &lowerHalf},
		"a second bucket on a cluster": {To: StateMigrating, Target: "minio", Name: "other", Range: &lowerHalf},
	} {
		if _, err := Apply(spread, tr); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	moving := step(t, spread, Transition{To: StateMigrating, Target: "minio", Range: &lowerHalf})
	other := HashRange{From: 1 << 63, To: FullRange.To}
	if _, err := Apply(moving, Transition{To: StateCutover, Range: &other}); err == nil || !strings.Contains(err.Error(), "in progress") {
		t.Errorf("a second move while one is in flight: %v", err)
	}
}

// A moving placement is written and read back through YAML and JSON unchanged, and validates.
func TestMovingPlacementRoundTrips(t *testing.T) {
	p := Placement{State: StateActive, Primary: "garage", Names: map[string]string{"garage": "data"}}
	p = step(t, p, Transition{To: StateRamping, Target: "minio", Name: "data-b", Range: &lowerHalf, Ratio: 0.5})
	f := &File{Version: 1, Clusters: sampleClusters(t), Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}},
		Placements: map[string]Placement{"acme/data": p}}
	if err := validate(f); err != nil {
		t.Fatalf("a moving placement is invalid: %v", err)
	}
	body, err := marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	back, err := parse(body)
	if err != nil || !reflect.DeepEqual(back.Placements["acme/data"], p) {
		t.Fatalf("YAML round trip: %v\n got  %+v\n want %+v\n%s", err, back.Placements["acme/data"], p, body)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var j Placement
	if err := json.Unmarshal(raw, &j); err != nil || !reflect.DeepEqual(j, p) {
		t.Fatalf("JSON round trip: %v\n%s", err, raw)
	}
}

func TestReassign(t *testing.T) {
	owners := []Owner{{From: 0, To: 99, Leg: "a"}, {From: 100, To: FullRange.To, Leg: "b"}}
	got := reassign(owners, HashRange{From: 20, To: 29}, "c")
	want := []Owner{{From: 0, To: 19, Leg: "a"}, {From: 20, To: 29, Leg: "c"}, {From: 30, To: 99, Leg: "a"}, {From: 100, To: FullRange.To, Leg: "b"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("middle: %+v", got)
	}
	got = reassign(owners, HashRange{From: 0, To: 99}, "b")
	if want := []Owner{{From: 0, To: FullRange.To, Leg: "b"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("a whole range joins its neighbor: %+v", got)
	}
}
