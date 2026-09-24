package proxy

// ADR-0020 P1: a spread bucket with a prefix rule, driven through the handler. Keys under the rule
// go to the legs of its own table, and a listing under the prefix reads only those legs.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// ownScope carves prefix in acme/spread and then gives its scope to minio alone, as a finished
// move within the scope would (ADR-0020 P2), by writing the directory file.
func ownScope(t *testing.T, m *mixedRig, prefix string) *directory.Placement {
	t.Helper()
	if err := m.dir.Carve(context.Background(), "acme", "spread", prefix, "test"); err != nil {
		t.Fatal(err)
	}
	f := m.dir.Snapshot().File()
	p := f.Placements["acme/spread"]
	p.Prefixes[0].Owners = directory.EvenOwners([]string{"minio"})
	f.Placements["acme/spread"] = p
	f.Version++
	body, err := yaml.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.WriteAtomic(m.dirPath, body); err != nil {
		t.Fatal(err)
	}
	if _, err := m.dir.Reload(); err != nil {
		t.Fatal(err)
	}
	got, _ := m.dir.Snapshot().Lookup("acme", "spread")
	return got
}

func (f *fakeS3) holds(bucket, key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.buckets[bucket][key]
	return ok
}

func (f *fakeS3) listed(bucket string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.seen {
		if strings.HasPrefix(s, "GET /"+bucket+"?") && strings.Contains(s, "list-type=2") {
			n++
		}
	}
	return n
}

func TestPrefixRuleRoutesAndListsOnlyItsLegs(t *testing.T) {
	m := newMixedRig(t, nil)
	spread(t, m)
	p := ownScope(t, m, "archive/")
	if len(p.Prefixes) != 1 || p.Prefixes[0].Owners[0].Leg != "minio" {
		t.Fatalf("placement: %+v", p)
	}
	archive, rest := make([]string, 0, 30), make([]string, 0, 30)
	for i := range 30 {
		a, r := fmt.Sprintf("archive/%02d", i), fmt.Sprintf("data/%02d", i)
		for _, k := range []string{a, r} {
			if resp := m.acme(t, "PUT", "/spread/"+k, []byte("v-"+k)); resp.StatusCode != 200 {
				t.Fatalf("PUT %s: %d %s", k, resp.StatusCode, resp.body)
			}
		}
		archive, rest = append(archive, a), append(rest, r)
	}
	for _, k := range archive {
		if !m.minio.holds("spread-m", k) || m.garage.holds("spread-g", k) {
			t.Fatalf("%s is not on minio alone, the only leg of its scope", k)
		}
		if resp := m.acme(t, "GET", "/spread/"+k, nil); resp.StatusCode != 200 || string(resp.body) != "v-"+k {
			t.Fatalf("GET %s: %d %s", k, resp.StatusCode, resp.body)
		}
	}
	onGarage := 0
	for _, k := range rest {
		if m.garage.holds("spread-g", k) {
			onGarage++
		}
	}
	if onGarage == 0 || onGarage == len(rest) {
		t.Fatalf("keys outside the rule are not split by hash: %d of %d on garage", onGarage, len(rest))
	}
	// A stray under the prefix on the leg that does not own it is filtered from every listing.
	m.garage.put("spread-g", "archive/stray", []byte("not mine"))

	before := m.garage.listed("spread-g")
	r := m.acme(t, "GET", "/spread?list-type=2&prefix=archive/", nil)
	if r.StatusCode != 200 {
		t.Fatalf("listing archive/: %d %s", r.StatusCode, r.body)
	}
	if got := listKeys(t, r.body, "Key"); !slices.Equal(got, archive) {
		t.Fatalf("listing archive/:\n got  %v\n want %v", got, archive)
	}
	if n := m.garage.listed("spread-g") - before; n != 0 {
		t.Fatalf("a listing under a prefix only minio owns read garage %d times", n)
	}
	r = m.acme(t, "GET", "/spread?list-type=2", nil)
	want := append(slices.Clone(archive), rest...)
	slices.Sort(want)
	if got := listKeys(t, r.body, "Key"); !slices.Equal(got, want) {
		t.Fatalf("listing everything:\n got  %v\n want %v", got, want)
	}
	if m.garage.listed("spread-g") == before {
		t.Fatal("a listing of the whole bucket did not read garage")
	}
}

// During a move within a scope (ADR-0020 P2), keys of the scope in the moving range write to the
// destination and read back from either leg, and a listing under the prefix merges both; keys
// outside the scope in the same hash range stay where they are.
func TestScopedMoveThroughTheProxy(t *testing.T) {
	m := newMixedRig(t, nil)
	spread(t, m)
	if err := m.dir.Carve(context.Background(), "acme", "spread", "archive/", "test"); err != nil {
		t.Fatal(err)
	}
	p0, _ := m.dir.Snapshot().Lookup("acme", "spread")
	// Keys hashing into garage's range of archive/: one written before the move, one after; and a
	// key outside archive/ in the same range.
	var before, after, outside string
	for i := 0; before == "" || after == "" || outside == ""; i++ {
		for _, k := range []string{fmt.Sprintf("archive/%03d", i), fmt.Sprintf("data/%03d", i)} {
			if id, _ := migrate.OwnerOf(p0, k); id != "garage" {
				continue
			}
			switch {
			case strings.HasPrefix(k, "data/") && outside == "":
				outside = k
			case strings.HasPrefix(k, "archive/") && before == "":
				before = k
			case strings.HasPrefix(k, "archive/") && after == "":
				after = k
			}
		}
	}
	if r := m.acme(t, "PUT", "/spread/"+before, []byte("old")); r.StatusCode != 200 {
		t.Fatalf("PUT %s: %d", before, r.StatusCode)
	}
	f := m.dir.Snapshot().File()
	p := f.Placements["acme/spread"]
	var garageRange directory.HashRange
	for _, o := range p.Prefixes[0].Owners {
		if o.Leg == "garage" {
			garageRange = directory.HashRange{From: o.From, To: o.To}
		}
	}
	p.State, p.Move = directory.StateMigrating, &directory.Move{Scope: "archive/", Range: garageRange, From: "garage", To: "minio"}
	f.Placements["acme/spread"] = p
	f.Version++
	body, err := yaml.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.WriteAtomic(m.dirPath, body); err != nil {
		t.Fatal(err)
	}
	if _, err := m.dir.Reload(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{after, outside} {
		if r := m.acme(t, "PUT", "/spread/"+k, []byte("new")); r.StatusCode != 200 {
			t.Fatalf("PUT %s: %d %s", k, r.StatusCode, r.body)
		}
	}
	if !m.minio.holds("spread-m", after) || m.garage.holds("spread-g", after) {
		t.Fatalf("%s is in the moving scope and range: it belongs on minio", after)
	}
	if !m.garage.holds("spread-g", outside) || m.minio.holds("spread-m", outside) {
		t.Fatalf("%s is outside the moving scope: it stays on garage", outside)
	}
	if r := m.acme(t, "GET", "/spread/"+before, nil); r.StatusCode != 200 || string(r.body) != "old" {
		t.Fatalf("GET %s, written before the move: %d %s", before, r.StatusCode, r.body)
	}
	r := m.acme(t, "GET", "/spread?list-type=2&prefix=archive/", nil)
	if got := listKeys(t, r.body, "Key"); !slices.Contains(got, before) || !slices.Contains(got, after) || len(got) != 2 {
		t.Fatalf("listing archive/ during the move: %v", got)
	}
}
