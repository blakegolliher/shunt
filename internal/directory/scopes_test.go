package directory

import (
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

// ownerAt is a key's owner in p for a hash h: its scope's table at h. The key's own hash is
// migrate's business; carve and merge change only which table a key reads, so any h will do.
func ownerAt(p *Placement, key string, h Hash) string {
	_, owners := p.Scope(key)
	return OwnerIn(owners, h)
}

func threeLegs() map[string]Leg {
	return map[string]Leg{"garage": {Cluster: "garage", Bucket: "s-g"}, "minio": {Cluster: "minio", Bucket: "s-m"}, "cold": {Cluster: "cold", Bucket: "s-c"}}
}

// A key's scope is the longest rule it starts with, or the empty prefix.
func TestScopeIsTheLongestMatch(t *testing.T) {
	legs := threeLegs()
	p := Placement{State: StateActive, Legs: legs, KeyHash: RampHash, Owners: EvenOwners([]string{"garage", "minio"}),
		Prefixes: []PrefixRule{
			{Prefix: "archive/", Owners: EvenOwners([]string{"cold"})},
			{Prefix: "archive/2025/", Owners: EvenOwners([]string{"minio"})},
			{Prefix: "logs", Owners: EvenOwners([]string{"garage"})},
		}}
	for key, want := range map[string]string{
		"archive/x": "archive/", "archive/2025/a": "archive/2025/", "archive/2025": "archive/", "logs/1": "logs",
		"logsx": "logs", "data/1": "", "": "", "arch": "",
	} {
		if got, _ := p.Scope(key); got != want {
			t.Errorf("scope of %q: %q, want %q", key, got, want)
		}
	}
	if got := ownerAt(&p, "archive/2025/a", 0); got != "minio" {
		t.Fatalf("a nested rule's key: %s", got)
	}
	for prefix, want := range map[string][]string{
		"archive/":      {"cold", "minio"}, // archive/ itself and the nested archive/2025/
		"archive/2025/": {"minio"},
		"archive/20":    {"cold", "minio"},
		"logs/":         {"garage"},
		"data/":         {"garage", "minio"},
		"":              {"cold", "garage", "minio"},
	} {
		if got := ListingLegs(&p, prefix); !reflect.DeepEqual(got, want) {
			t.Errorf("legs listed under %q: %v, want %v", prefix, got, want)
		}
	}
}

// randomTable is a partition of the key space among legs, cut at up to three random points.
func randomTable(rnd *rand.Rand, legs []string) []Owner {
	cuts := map[Hash]bool{}
	for range rnd.Intn(4) {
		cuts[Hash(rnd.Uint64()|1)] = true
	}
	var out []Owner
	from := Hash(0)
	for _, c := range sortedHashes(cuts) {
		out = append(out, Owner{From: from, To: c - 1, Leg: legs[rnd.Intn(len(legs))]})
		from = c
	}
	return append(out, Owner{From: from, To: FullRange.To, Leg: legs[rnd.Intn(len(legs))]})
}

func sortedHashes(m map[Hash]bool) []Hash {
	out := make([]Hash, 0, len(m))
	for h := range m {
		out = append(out, h)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Carve and merge change no key's owner, whatever rules exist and however their tables differ (as
// moves within scopes will leave them): so neither needs a fence (ADR-0020).
func TestCarveAndMergeKeepEveryOwner(t *testing.T) {
	rnd := rand.New(rand.NewSource(20)) //nolint:gosec // G404: a test's reproducible choices
	ids := []string{"garage", "minio", "cold"}
	alphabet := []string{"a", "b", "/", "ab", "a/", "b/"}
	word := func(n int) string {
		var b strings.Builder
		for range n {
			b.WriteString(alphabet[rnd.Intn(len(alphabet))])
		}
		return b.String()
	}
	carves, merges := 0, 0
	for run := range 300 {
		f := &File{Clusters: sampleClusters(t), Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{}}
		p := Placement{State: StateActive, Legs: threeLegs(), KeyHash: RampHash, Owners: randomTable(rnd, ids)}
		for range rnd.Intn(5) { // rules as moves inside scopes would leave them: tables differing from their parents
			prefix := word(1 + rnd.Intn(3))
			if _, dup := indexRule(p.Prefixes, prefix); !dup {
				p.Prefixes = append(p.Prefixes, PrefixRule{Prefix: prefix, Owners: randomTable(rnd, ids)})
			}
		}
		sortRules(p.Prefixes)
		if !p.Spread() {
			continue // one owner and no rule: a decoder makes that a plain placement, never kept in v2
		}
		f.Placements["acme/spr"] = p
		if err := validate(f); err != nil {
			t.Fatalf("run %d: the generated placement is invalid: %v", run, err)
		}
		keys := make([]string, 200)
		hashes := make([]Hash, len(keys))
		for i := range keys {
			keys[i], hashes[i] = word(rnd.Intn(6)), Hash(rnd.Uint64())
		}
		check := func(what string, before *Placement) {
			t.Helper()
			after := f.Placements["acme/spr"]
			for i, k := range keys {
				if a, b := ownerAt(before, k, hashes[i]), ownerAt(&after, k, hashes[i]); a != b {
					t.Fatalf("run %d, %s: key %q at %016x was owned by %s and is owned by %s", run, what, k, uint64(hashes[i]), a, b)
				}
			}
			if err := validate(f); err != nil {
				t.Fatalf("run %d, %s: %v", run, what, err)
			}
		}
		for range 3 {
			before := f.Placements["acme/spr"]
			prefix := word(1 + rnd.Intn(3))
			if err := f.Carve("acme", "spr", prefix); err == nil {
				carves++
				check("carve "+prefix, &before)
			}
			before = f.Placements["acme/spr"]
			if r := before.Prefixes; len(r) > 0 {
				victim := r[rnd.Intn(len(r))].Prefix
				if err := f.Merge("acme", "spr", victim); err == nil {
					merges++
					check("merge "+victim, &before)
				} else if !errors.Is(err, ErrRefused) {
					t.Fatalf("merge %s: %v", victim, err)
				}
			}
		}
	}
	if carves < 100 || merges < 50 {
		t.Fatalf("the property ran thin: %d carves, %d merges", carves, merges)
	}
}

func indexRule(rules []PrefixRule, prefix string) (int, bool) {
	for i, r := range rules {
		if r.Prefix == prefix {
			return i, true
		}
	}
	return -1, false
}

func sortRules(rules []PrefixRule) {
	for i := 1; i < len(rules); i++ {
		for j := i; j > 0 && rules[j].Prefix < rules[j-1].Prefix; j-- {
			rules[j], rules[j-1] = rules[j-1], rules[j]
		}
	}
}

// Carving a plain bucket makes it v2 with its one leg; merging the rule back makes it plain again,
// as it was. A rule owned differently from its parent is not merged.
func TestCarveAPlainBucketAndMergeBack(t *testing.T) {
	f := &File{Clusters: sampleClusters(t), Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{}}
	created := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	plain := Placement{State: StateActive, Primary: "garage", Names: map[string]string{"garage": "data"}, Created: created}
	f.Placements["acme/data"] = plain
	if err := f.Carve("acme", "data", "archive/"); err != nil {
		t.Fatal(err)
	}
	p := f.Placements["acme/data"]
	if !p.Spread() || p.Primary != "" || len(p.Legs) != 1 || len(p.Prefixes) != 1 || p.Prefixes[0].Owners[0].Leg != "garage" || p.KeyHash != RampHash {
		t.Fatalf("carved: %+v", p)
	}
	if err := validate(f); err != nil {
		t.Fatalf("a carved plain bucket is invalid: %v", err)
	}
	// Both encodings keep the rule: the directory file and every JSON reader.
	body, err := marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	back, err := parse(body)
	if err != nil || !reflect.DeepEqual(back.Placements["acme/data"], p) {
		t.Fatalf("YAML round trip: %v\n got  %+v\n want %+v\n%s", err, back.Placements["acme/data"], p, body)
	}
	raw, _ := json.Marshal(p)
	var j Placement
	if err := json.Unmarshal(raw, &j); err != nil || !reflect.DeepEqual(j, p) {
		t.Fatalf("JSON round trip: %v\n got  %+v\n want %+v", err, j, p)
	}
	if err := f.Carve("acme", "data", "archive/"); !errors.Is(err, ErrRefused) {
		t.Fatalf("a second carve of one prefix: %v", err)
	}
	if err := f.Merge("acme", "data", "archive/"); err != nil {
		t.Fatal(err)
	}
	if got := f.Placements["acme/data"]; !reflect.DeepEqual(got, plain) {
		t.Fatalf("carve then merge did not give back the plain bucket:\n got  %+v\n want %+v", got, plain)
	}

	// A rule owned differently from its parent (as a move within it will leave it) stays.
	f.Placements["acme/data"] = Placement{State: StateActive, Legs: threeLegs(), KeyHash: RampHash, Owners: EvenOwners([]string{"garage", "minio"}),
		Prefixes: []PrefixRule{{Prefix: "archive/", Owners: EvenOwners([]string{"cold"})}}}
	if err := f.Merge("acme", "data", "archive/"); err == nil || !strings.Contains(err.Error(), "owned differently") {
		t.Fatalf("merging a rule owned differently: %v", err)
	}
	if err := f.Merge("acme", "data", "nope/"); err == nil || !strings.Contains(err.Error(), "no rule") {
		t.Fatalf("merging a rule that is not there: %v", err)
	}
}

// What carve refuses, the caps, and what this build refuses of a bucket with rules: a move (P2).
func TestPrefixRulesRefusalsAndCaps(t *testing.T) {
	f := &File{Clusters: sampleClusters(t), Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{}}
	f.Placements["acme/moving"] = Placement{State: StateRamping, Primary: "minio", Source: "garage", Names: map[string]string{"garage": "mov", "minio": "mov-b"},
		Ramp: &Ramp{Hash: RampHash, Ratio: 0.5}}
	f.Placements["acme/aimed"] = Placement{State: StateActive, Primary: "garage", Target: "minio", Names: map[string]string{"garage": "tgt", "minio": "tgt-b"}}
	f.Placements["acme/spr"] = Placement{State: StateActive, Legs: threeLegs(), KeyHash: RampHash, Owners: EvenOwners([]string{"garage", "minio"})}
	for name, tc := range map[string]struct{ bucket, prefix, want string }{
		"moving":   {"moving", "a/", "only at rest"},
		"target":   {"aimed", "a/", "clear the target first"},
		"empty":    {"spr", "", "a prefix is required"},
		"too long": {"spr", strings.Repeat("x", MaxPrefixLen+1), "at most 1024 bytes"},
	} {
		if err := f.Carve("acme", tc.bucket, tc.prefix); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for i := range MaxPrefixRules {
		if err := f.Carve("acme", "spr", string(rune('a'+i%26))+strings.Repeat("/", 1+i/26)); err != nil {
			t.Fatalf("rule %d: %v", i, err)
		}
	}
	if err := f.Carve("acme", "spr", "one-more/"); err == nil || !strings.Contains(err.Error(), "the most a bucket may have") {
		t.Fatalf("rule %d: %v", MaxPrefixRules+1, err)
	}
	if err := validate(f); err != nil {
		t.Fatalf("a bucket at the cap: %v", err)
	}
	p := f.Placements["acme/spr"]
	if _, err := Apply(p, Transition{To: StateRamping, Target: "cold", Name: "x", Leg: "garage", Ratio: 0.5}); err == nil || !strings.Contains(err.Error(), "ADR-0020 P2") {
		t.Fatalf("a move in a bucket with rules: %v", err)
	}
	// A leg owning keys only under a rule is not idle: expand --clear must not retire it.
	q := Placement{State: StateActive, Legs: threeLegs(), KeyHash: RampHash, Owners: EvenOwners([]string{"garage", "minio"}),
		Prefixes: []PrefixRule{{Prefix: "archive/", Owners: EvenOwners([]string{"cold"})}}}
	if idle := IdleLegs(q); len(idle) != 0 {
		t.Fatalf("idle legs: %v", idle)
	}
	// The decoder refuses rules out of order, repeated, empty, or not a partition.
	for name, rules := range map[string]string{
		"unsorted":    `[{"prefix":"b/","owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"a"}]},{"prefix":"a/","owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"a"}]}]`,
		"repeated":    `[{"prefix":"a/","owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"a"}]},{"prefix":"a/","owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"a"}]}]`,
		"empty":       `[{"prefix":"","owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"a"}]}]`,
		"gap":         `[{"prefix":"a/","owners":[{"from":"0000000000000000","to":"7fffffffffffffff","leg":"a"}]}]`,
		"unknown leg": `[{"prefix":"a/","owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"zz"}]}]`,
	} {
		body := `{"state":"ACTIVE","hash":"fnv1a-fmix64-v1","legs":{"a":{"cluster":"garage","bucket":"x"}},"owners":[{"from":"0000000000000000","to":"ffffffffffffffff","leg":"a"}],"prefixes":` + rules + `}`
		var got Placement
		if err := json.Unmarshal([]byte(body), &got); err == nil {
			t.Errorf("%s: accepted as %+v", name, got)
		}
	}
}

func BenchmarkScope64Rules(b *testing.B) {
	p := Placement{Owners: EvenOwners([]string{"a", "b"})}
	for i := range MaxPrefixRules {
		p.Prefixes = append(p.Prefixes, PrefixRule{Prefix: "runs/2026-" + string(rune('a'+i%26)) + strings.Repeat("x", i/26) + "/", Owners: EvenOwners([]string{"a"})})
	}
	sortRules(p.Prefixes)
	b.ReportAllocs()
	for b.Loop() {
		_, owners := p.Scope("runs/2026-q/part-00017.parquet")
		_ = OwnerIn(owners, 1<<63)
	}
}

// The sample with prefix rules loads, validates, and survives the directory file's YAML.
func TestPrefixesSampleRoundTrips(t *testing.T) {
	f, err := loadSample(t, "testdata/valid/prefixes-v2.yaml", sampleClusters(t))
	if err != nil {
		t.Fatal(err)
	}
	p := f.Placements["acme/scoped"]
	if !p.Spread() || len(p.Prefixes) != 2 || ownerAt(&p, "archive/x", 0) != "cold" || ownerAt(&p, "archive/hot/x", 0) != "garage" || ownerAt(&p, "x", FullRange.To) != "minio" {
		t.Fatalf("loaded: %+v", p)
	}
	body, err := marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	back, err := parse(body)
	if err != nil || !reflect.DeepEqual(back.Placements["acme/scoped"], p) {
		t.Fatalf("YAML round trip: %v\n%s", err, body)
	}
}
