package directory

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// The first write under schema 2 draws the identity; later writes keep it, and every changed
// resource takes the write's version as its generation while the others keep theirs.
func TestWritesStampIdentityAndGenerations(t *testing.T) {
	ctx := context.Background()
	d, err := Open(writeDir(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !d.Snapshot().File().Identity.IsZero() {
		t.Fatal("a schema-1 file has an identity before any write")
	}
	if err := d.Create(ctx, "acme", "data", "garage", "acme-0000-data", "t"); err != nil {
		t.Fatal(err)
	}
	f := d.Snapshot().File()
	if f.Schema != SchemaVersion || f.Identity.Validate() != nil {
		t.Fatalf("after the first write: schema %d identity %+v", f.Schema, f.Identity)
	}
	id := f.Identity
	if g := f.Generation(PlacementResource("acme/data")); g != 2 {
		t.Fatalf("new placement generation %d, want 2", g)
	}
	if g := f.Generation(ClusterResource("garage")); g != 0 {
		t.Fatalf("an unchanged cluster was stamped: generation %d", g)
	}

	if err := d.Create(ctx, "acme", "more", "garage", "acme-0000-more", "t"); err != nil {
		t.Fatal(err)
	}
	f = d.Snapshot().File()
	if f.Identity != id {
		t.Fatal("a write changed the identity")
	}
	if g := f.Generation(PlacementResource("acme/data")); g != 2 {
		t.Fatalf("an unchanged placement moved to generation %d", g)
	}
	if g := f.Generation(PlacementResource("acme/more")); g != 3 {
		t.Fatalf("second placement generation %d, want 3", g)
	}

	// Deleted and recreated under the same name: a new generation, never the old one.
	if err := d.Delete(ctx, "acme", "data", "t"); err != nil {
		t.Fatal(err)
	}
	if g := d.Snapshot().File().Generation(PlacementResource("acme/data")); g != 0 {
		t.Fatalf("a deleted placement kept generation %d", g)
	}
	if err := d.Create(ctx, "acme", "data", "garage", "acme-0000-data", "t"); err != nil {
		t.Fatal(err)
	}
	if g := d.Snapshot().File().Generation(PlacementResource("acme/data")); g != 5 {
		t.Fatalf("recreated placement generation %d, want 5", g)
	}

	// The identity and generations survive a reopen.
	d2, err := Open(d.Path())
	if err != nil {
		t.Fatal(err)
	}
	if f2 := d2.Snapshot().File(); f2.Identity != id || f2.Generation(PlacementResource("acme/data")) != 5 {
		t.Fatalf("after reopen: %+v %v", f2.Identity, f2.Generations)
	}
}

// Stamp stamps what the file does not show when told: a cluster whose secret changed.
func TestStampNamedResources(t *testing.T) {
	prev := &File{Version: 4, Schema: SchemaVersion, Identity: Identity{ClusterID: strings.Repeat("a", 32), Epoch: strings.Repeat("b", 32)},
		Clusters: sampleClusters(t), Generations: map[string]int64{ClusterResource("garage"): 3}}
	next := prev.clone()
	next.Version = 5
	if err := Stamp(prev, next, ClusterResource("garage")); err != nil {
		t.Fatal(err)
	}
	if g := next.Generation(ClusterResource("garage")); g != 5 {
		t.Fatalf("named cluster generation %d, want 5", g)
	}
	if g := next.Generation(ClusterResource("minio")); g != 0 {
		t.Fatalf("an unnamed unchanged cluster was stamped: %d", g)
	}
}

func TestLineageValidation(t *testing.T) {
	good := Identity{ClusterID: strings.Repeat("a", 32), Epoch: strings.Repeat("0", 32)}
	cases := []struct {
		name string
		f    File
		key  string
	}{
		{"newer schema", File{Version: 1, Schema: SchemaVersion + 1}, "schema"},
		{"short cluster id", File{Version: 1, Identity: Identity{ClusterID: "abc", Epoch: good.Epoch}}, "identity"},
		{"uppercase epoch", File{Version: 1, Identity: Identity{ClusterID: good.ClusterID, Epoch: strings.Repeat("A", 32)}}, "identity"},
		{"generation past the version", File{Version: 2, Identity: good, Generations: map[string]int64{"placement:a/b": 3}}, "generations.placement:a/b"},
		{"zero generation", File{Version: 2, Identity: good, Generations: map[string]int64{"cluster:x": 0}}, "generations.cluster:x"},
	}
	for _, c := range cases {
		err := validate(&c.f)
		if err == nil || !strings.Contains(err.Error(), c.key) {
			t.Errorf("%s: %v, want an error naming %s", c.name, err, c.key)
		}
	}
	if err := validate(&File{Version: 1, Schema: SchemaVersion, Identity: good}); err != nil {
		t.Errorf("a valid lineage: %v", err)
	}
}

// A file written by a newer shunt is refused at open: this build must not act on state it cannot
// interpret.
func TestOpenRefusesANewerSchema(t *testing.T) {
	path := writeDir(t, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte("schema: 3\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "newer shunt") {
		t.Fatalf("open of a schema-3 file: %v", err)
	}
	if !errors.Is(CheckSchema(3), ErrNewerSchema) {
		t.Fatal("CheckSchema(3) is not ErrNewerSchema")
	}
}

func BenchmarkStamp(b *testing.B) {
	prev := &File{Version: 1, Identity: Identity{ClusterID: strings.Repeat("a", 32), Epoch: strings.Repeat("b", 32)},
		Clusters: sampleClusters(b), Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{}}
	for i := range 10000 {
		k := "acme/b" + strings.Repeat("0", 5-len(itoa(i))) + itoa(i)
		prev.Placements[k] = Placement{State: StateActive, Primary: "garage", Names: map[string]string{"garage": k[5:]}}
	}
	next := prev.clone()
	next.Version = 2
	for b.Loop() {
		if err := Stamp(prev, next); err != nil {
			b.Fatal(err)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var s []byte
	for ; i > 0; i /= 10 {
		s = append([]byte{byte('0' + i%10)}, s...)
	}
	return string(s)
}
