package upstream

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
)

// Signer rotation on the install path (ADR-0021 D1): every installed directory version prepares
// the cluster set, and a secret rotation rebuilds one cluster's signer over its old transport.

// rotationClusters is sixteen clusters, each with its own secret_ref.
func rotationClusters() map[string]config.Cluster {
	defs := map[string]config.Cluster{}
	for i := range 16 {
		defs[fmt.Sprintf("c%02d", i)] = regCluster(fmt.Sprintf("10.0.0.%d:80", i+1), fmt.Sprintf("control:c%02d", i))
	}
	return defs
}

// BenchmarkRegistryRotateSecret is one secret rotation of one of sixteen clusters: Prepare with
// the cluster's next secret generation (its signer rebuilt, its transport and pool kept, the
// fifteen others reused as they are), then Commit. The same path runs for every install that
// carries a rotation, on every proxy.
func BenchmarkRegistryRotateSecret(b *testing.B) {
	defs := rotationClusters()
	var gen int64 = 1
	r := NewRegistry(Options{}, func(string) (string, error) { return "s", nil })
	b.Cleanup(r.Close)
	resolve := func(ref string) (string, error) {
		if ref == "control:c00" {
			return "s-" + strconv.FormatInt(gen, 10), nil
		}
		return "s", nil
	}
	generation := func(name string) int64 {
		if name == "c00" {
			return gen
		}
		return 1
	}
	cand, err := r.PrepareWith(defs, resolve, generation)
	if err != nil {
		b.Fatal(err)
	}
	cand.Commit()
	b.ReportAllocs()
	for b.Loop() {
		gen++
		cand, err := r.PrepareWith(defs, resolve, generation)
		if err != nil {
			b.Fatal(err)
		}
		if added, _ := cand.Commit(); len(added) != 1 {
			b.Fatalf("rotated %v, want only c00", added)
		}
	}
}

// BenchmarkRegistryPrepareUnchanged is the cluster set's preparation for an installed version that
// changes no cluster (a placement edit): sixteen definitions compared and reused, then Commit.
// It is the floor every install pays for the cluster set.
func BenchmarkRegistryPrepareUnchanged(b *testing.B) {
	defs := rotationClusters()
	r := NewRegistry(Options{}, func(string) (string, error) { return "s", nil })
	b.Cleanup(r.Close)
	if _, _, err := r.Apply(defs); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		cand, err := r.Prepare(defs, nil)
		if err != nil {
			b.Fatal(err)
		}
		if added, _ := cand.Commit(); len(added) != 0 {
			b.Fatalf("changed %v, want none", added)
		}
	}
}
