package upstream

import (
	"errors"
	"sync"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
)

func regCluster(endpoint, ref string) config.Cluster {
	return config.Cluster{Type: "s3", Scheme: "http", Region: "r", EndpointMode: "static", Endpoints: []string{endpoint},
		Credentials: config.Credentials{AccessKey: "AK", SecretRef: ref}}
}

func TestRegistryAddsReusesReplacesAndRemoves(t *testing.T) {
	secrets := map[string]string{"env:A": "a", "env:B": "b"}
	r := NewRegistry(Options{}, func(ref string) (string, error) {
		if s, ok := secrets[ref]; ok {
			return s, nil
		}
		return "", errors.New("unresolvable " + ref)
	})
	if len(r.Load().Names()) != 0 {
		t.Fatal("a new registry holds no clusters")
	}
	added, removed, err := r.Apply(map[string]config.Cluster{"vast01": regCluster("10.0.0.1:80", "env:A")})
	if err != nil || len(added) != 1 || len(removed) != 0 {
		t.Fatalf("add vast01: %v %v %v", added, removed, err)
	}
	v1, _ := r.Load().Get("vast01")
	if v1.Creds.Secret != "a" {
		t.Fatalf("secret not resolved: %q", v1.Creds.Secret)
	}

	added, _, err = r.Apply(map[string]config.Cluster{"vast01": regCluster("10.0.0.1:80", "env:A"), "vast02": regCluster("10.0.0.2:80", "env:B")})
	if err != nil || len(added) != 1 || added[0] != "vast02" {
		t.Fatalf("add vast02: %v %v", added, err)
	}
	if same, _ := r.Load().Get("vast01"); same != v1 {
		t.Error("an unchanged cluster must keep its *Cluster and its connection pool")
	}
	if _, ok := r.Load().ByID(ClusterID("vast02")); !ok {
		t.Error("vast02 not addressable by id")
	}

	// A definition whose secret cannot resolve is refused, and nothing is swapped.
	before := r.Load()
	if _, _, err := r.Apply(map[string]config.Cluster{"vast01": regCluster("10.0.0.1:80", "env:A"), "vast03": regCluster("10.0.0.3:80", "env:MISSING")}); err == nil {
		t.Fatal("an unresolvable secret_ref was accepted")
	}
	if r.Load() != before {
		t.Error("a failed Apply swapped the set")
	}

	// A changed definition is rebuilt; a dropped one is removed.
	added, removed, err = r.Apply(map[string]config.Cluster{"vast02": regCluster("10.0.0.22:80", "env:B")})
	if err != nil || len(added) != 1 || added[0] != "vast02" || len(removed) != 1 || removed[0] != "vast01" {
		t.Fatalf("replace vast02, remove vast01: %v %v %v", added, removed, err)
	}
	if v2, _ := r.Load().Get("vast02"); v2.Endpoints[0] != "10.0.0.22:80" {
		t.Errorf("vast02 not rebuilt: %v", v2.Endpoints)
	}
	if _, ok := r.Load().Get("vast01"); ok {
		t.Error("vast01 still present")
	}
}

// Requests load the set while Apply swaps it; -race proves the swap is safe.
func TestRegistrySwapUnderLoad(t *testing.T) {
	r := NewRegistry(Options{}, func(string) (string, error) { return "s", nil })
	one := map[string]config.Cluster{"a": regCluster("10.0.0.1:80", "env:S")}
	two := map[string]config.Cluster{"a": regCluster("10.0.0.1:80", "env:S"), "b": regCluster("10.0.0.2:80", "env:S")}
	if _, _, err := r.Apply(one); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if cl, ok := r.Load().Get("a"); !ok || cl.Next() == "" {
					t.Error("cluster a vanished mid-swap")
					return
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		defs := one
		if i%2 == 0 {
			defs = two
		}
		if _, _, err := r.Apply(defs); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func BenchmarkRegistryLoadGet(b *testing.B) {
	r := NewRegistry(Options{}, func(string) (string, error) { return "s", nil })
	if _, _, err := r.Apply(map[string]config.Cluster{"a": regCluster("10.0.0.1:80", "env:S")}); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		_, _ = r.Load().Get("a")
	}
}
