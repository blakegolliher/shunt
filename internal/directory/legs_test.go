package directory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The same directory written in v1 and in v2 loads to the same placements: N1 reads v2 without
// changing what any placement means (ADR-0018).
func TestV2SampleMatchesV1(t *testing.T) {
	v1, err := Load("testdata/valid/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	v2, err := Load("testdata/valid/mixed-v2.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range sortedKeys(v1.Placements) {
		want := v1.Placements[k]
		if got := v2.Placements[k]; !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n v2 %+v\n v1 %+v", k, got, want)
		}
	}
	if len(v2.Placements) != len(v1.Placements) {
		t.Fatalf("%d placements in v2, %d in v1", len(v2.Placements), len(v1.Placements))
	}
}

// v2Shapes are the placement shapes the sample does not have: an expanded target, a held ramp, and
// cutover evidence.
func v2Shapes() map[string]Placement {
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	return map[string]Placement{
		"acme/expanded": {State: StateActive, Primary: "garage", Target: "minio", Names: map[string]string{"garage": "data", "minio": "data-001"}},
		"acme/held": {State: StateRamping, Primary: "minio", Source: "garage", Names: map[string]string{"garage": "held", "minio": "held-001"},
			Ramp: &Ramp{Hash: RampHash, Ratio: 0.25, Prefixes: []string{"a/"}, Hold: &RampHold{Ratio: 0.5, Prefixes: []string{"a/", "b/"}}}},
		"acme/cut": {State: StateCutover, Primary: "minio", Source: "garage", Names: map[string]string{"garage": "cut", "minio": "cut-001"},
			Cutover: &CutoverEvidence{At: at, Window: time.Minute, FallbackReads: 37}},
		"acme/cold": {State: StateActive, Primary: "garage", Cold: "cold", Tier: "emulated", Names: map[string]string{"garage": "arch", "cold": "arch"}},
	}
}

// Every placement survives v1 → v2 → v1 through both encodings every reader uses: the directory
// file's YAML, and the JSON of the control plane's store, GET /v1/directory and the member cache.
func TestV2RoundTrip(t *testing.T) {
	f, err := Load("testdata/valid/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	f.Clusters = sampleClusters(t)
	f.Tenants = map[string]Tenant{"acme": {DefaultCluster: "garage"}, "zed": {DefaultCluster: "minio"}}
	f.Placements = v2Shapes()
	if err := validate(f); err != nil {
		t.Fatalf("the shapes must be valid v1: %v", err)
	}
	withMixed, err := Load("testdata/valid/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []*File{f, withMixed} {
		v2 := &File{Version: file.Version, Clusters: file.Clusters, Tenants: file.Tenants, Placements: map[string]Placement{}}
		for k, p := range file.Placements {
			v2.Placements[k] = p.ToV2()
		}
		body, err := marshal(v2)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "legs:") || strings.Contains(string(body), "primary:") {
			t.Fatalf("ToV2 did not write v2:\n%s", body)
		}
		back, err := parse(body)
		if err != nil {
			t.Fatalf("v2 YAML does not parse: %v\n%s", err, body)
		}
		for k, want := range file.Placements {
			if got := back.Placements[k]; !reflect.DeepEqual(got, want) {
				t.Errorf("YAML %s:\n got  %+v\n want %+v", k, got, want)
			}
			raw, err := json.Marshal(want.ToV2())
			if err != nil {
				t.Fatal(err)
			}
			var got Placement
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("JSON %s: %v\n%s", k, err, raw)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("JSON %s:\n got  %+v\n want %+v\n%s", k, got, want, raw)
			}
		}
	}
}

// The v1 writer is unchanged: a placement written today has no v2 key, byte for byte as before.
func TestV1WriterUnchanged(t *testing.T) {
	raw, err := json.Marshal(v2Shapes()["acme/held"])
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"legs", "owners", "move", "hash"} {
		if _, ok := top[key]; ok {
			t.Fatalf("v1 JSON carries %s: %s", key, raw)
		}
	}
}

// A JSON reader refuses what a YAML reader refuses: the store and the member cache hold the same
// placements the file does.
func TestV2JSONRefusals(t *testing.T) {
	for name, body := range map[string]string{
		"spread, moving": `{"state":"RAMPING","hash":"fnv1a-fmix64-v1","legs":{"a":{"cluster":"garage","bucket":"x"},"b":{"cluster":"minio","bucket":"x"}},"owners":[{"from":"0000000000000000","to":"7fffffffffffffff","leg":"a"},{"from":"8000000000000000","to":"ffffffffffffffff","leg":"b"}],"move":{"range":{"from":"0000000000000000","to":"7fffffffffffffff"},"from":"a","to":"b"}}`,
		"both schemas":   `{"state":"ACTIVE","primary":"garage","names":{"garage":"x"},"legs":{"a":{"cluster":"garage","bucket":"x"}},"owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"a"}]}`,
		"short hash":     `{"state":"ACTIVE","legs":{"a":{"cluster":"garage","bucket":"x"}},"owners":[{"from":"0","to":"ffffffffffffffff","leg":"a"}]}`,
	} {
		var p Placement
		if err := json.Unmarshal([]byte(body), &p); err == nil {
			t.Errorf("%s: accepted as %+v", name, p)
		}
	}
}

// FuzzParseV2 holds every valid directory to the v2 round trip. FuzzParse already covers parsing
// itself, and its seeds include the v2 samples.
func FuzzParseV2(f *testing.F) {
	files, _ := filepath.Glob("testdata/*/*.yaml")
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	clusters := sampleClusters(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		d, err := parse(data)
		if err != nil {
			return
		}
		if d.Clusters == nil {
			d.Clusters = clusters
		}
		if validate(d) != nil {
			return
		}
		for k, p := range d.Placements {
			v2 := p.ToV2()
			raw, err := json.Marshal(v2)
			if err != nil {
				t.Fatal(err)
			}
			var back Placement
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatalf("%s: a valid placement's v2 form is refused: %v\n%s", k, err, raw)
			}
			if !reflect.DeepEqual(back, p) {
				t.Fatalf("%s: v2 round trip changed it:\n got  %+v\n want %+v", k, back, p)
			}
		}
	})
}

func BenchmarkParseV2(b *testing.B) {
	data, err := os.ReadFile("testdata/valid/mixed-v2.yaml")
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := parse(data); err != nil {
			b.Fatal(err)
		}
	}
}

// A spread placement stays in v2 form in memory, validates against the sample clusters, and
// survives the directory file's YAML and the JSON of every other reader unchanged (ADR-0018 N2).
func TestSpreadPlacementRoundTrips(t *testing.T) {
	f, err := loadSample(t, "testdata/valid/spread-v2.yaml", sampleClusters(t))
	if err != nil {
		t.Fatal(err)
	}
	p := f.Placements["acme/spread"]
	if !p.Spread() || p.Primary != "" || p.Names != nil || len(p.Legs) != 2 || p.Owners[1].Leg != "minio" {
		t.Fatalf("spread placement in memory: %+v", p)
	}
	if plain := f.Placements["acme/plain"]; plain.Spread() || plain.Primary != "garage" {
		t.Fatalf("a v1 placement next to it: %+v", plain)
	}
	body, err := marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	back, err := parse(body)
	if err != nil {
		t.Fatalf("written spread directory does not parse: %v\n%s", err, body)
	}
	if !reflect.DeepEqual(back.Placements["acme/spread"], p) {
		t.Fatalf("YAML round trip:\n got  %+v\n want %+v", back.Placements["acme/spread"], p)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var j Placement
	if err := json.Unmarshal(raw, &j); err != nil || !reflect.DeepEqual(j, p) {
		t.Fatalf("JSON round trip: %v\n got  %+v\n want %+v\n%s", err, j, p, raw)
	}
	if refs := References(f, "minio"); len(refs) != 1 || refs[0] != "placements.acme/spread" {
		t.Fatalf("a leg's cluster is referenced: %v", refs)
	}
}

// CreateSpread splits the key space evenly, one leg per cluster, and nothing moves a spread
// placement in this build: every transition and expand refuses it.
func TestCreateSpreadAndItsGuards(t *testing.T) {
	f := &File{Clusters: sampleClusters(t), Tenants: map[string]Tenant{}, Placements: map[string]Placement{}}
	legs := []Leg{{Cluster: "garage", Bucket: "s-g"}, {Cluster: "minio", Bucket: "s-m"}, {Cluster: "cold", Bucket: "s-c"}}
	if err := f.CreateSpread("acme", "spread", legs, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := validate(f); err != nil {
		t.Fatalf("a created spread placement is invalid: %v", err)
	}
	p := f.Placements["acme/spread"]
	if len(p.Owners) != 3 || p.Owners[0].Leg != "garage" || p.Owners[2].Leg != "cold" || p.KeyHash != RampHash || f.Tenants["acme"].DefaultCluster != "garage" {
		t.Fatalf("created: %+v, tenant %+v", p, f.Tenants["acme"])
	}
	if err := checkOwners(p.Owners, p.Legs); err != nil {
		t.Fatalf("owners do not partition the key space: %v", err)
	}
	for name, bad := range map[string][]Leg{
		"one leg":      legs[:1],
		"same cluster": {{Cluster: "garage", Bucket: "a"}, {Cluster: "garage", Bucket: "b"}},
	} {
		if err := f.CreateSpread("acme", "x-"+strings.ReplaceAll(name, " ", "-"), bad, time.Now()); !errors.Is(err, ErrConflict) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := f.CreateSpread("acme", "spread", legs, time.Now()); !errors.Is(err, ErrExists) {
		t.Errorf("a second create: %v", err)
	}
	if err := f.SetTarget("acme", "spread", "minio", "x"); !errors.Is(err, ErrConflict) {
		t.Errorf("expand of a spread bucket: %v", err)
	}
	if _, err := Apply(p, Transition{To: StateRamping, Target: "minio", Name: "x"}); err == nil || !strings.Contains(err.Error(), "spread over 3 legs") {
		t.Errorf("a step on a spread bucket: %v", err)
	}
}

func TestEvenOwners(t *testing.T) {
	for n := 1; n <= MaxLegs; n++ {
		ids := make([]string, n)
		legs := map[string]Leg{}
		for i := range ids {
			ids[i] = fmt.Sprintf("l%d", i)
			legs[ids[i]] = Leg{Cluster: ids[i], Bucket: "b"}
		}
		if err := checkOwners(EvenOwners(ids), legs); err != nil {
			t.Fatalf("%d legs: %v", n, err)
		}
	}
}
