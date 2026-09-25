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

// T03: a secret-only rotation, the secret_ref unchanged, signs new requests with the new secret
// over the same connection pool, while a request that loaded the old set keeps the old secret.
func TestRegistrySecretOnlyRotation(t *testing.T) {
	var mu sync.Mutex
	secret := "old"
	r := NewRegistry(Options{}, func(string) (string, error) { mu.Lock(); defer mu.Unlock(); return secret, nil })
	defs := map[string]config.Cluster{"vast01": regCluster("10.0.0.1:80", "control:vast01")}
	if _, _, err := r.Apply(defs); err != nil {
		t.Fatal(err)
	}
	held, _ := r.Load().Get("vast01") // a request in flight
	mu.Lock()
	secret = "new"
	mu.Unlock()
	added, removed, err := r.Apply(defs)
	if err != nil || len(added) != 1 || added[0] != "vast01" || len(removed) != 0 {
		t.Fatalf("rotation: added %v removed %v err %v", added, removed, err)
	}
	now, _ := r.Load().Get("vast01")
	if now.Creds.Secret != "new" {
		t.Fatalf("new requests sign with %q, want the rotated secret", now.Creds.Secret)
	}
	if held.Creds.Secret != "old" {
		t.Errorf("the request in flight now signs with %q; it must keep the secret it started with", held.Creds.Secret)
	}
	if now.Transport != held.Transport {
		t.Error("a secret-only rotation must keep the connection pool")
	}
	if again, _, _ := r.Apply(defs); len(again) != 0 {
		t.Errorf("an unchanged definition and secret was rebuilt: %v", again)
	}
	if same, _ := r.Load().Get("vast01"); same != now {
		t.Error("an unchanged cluster must keep its *Cluster")
	}
}

// A secret generation that moved is a rotation even with the same secret string: a new signer,
// carrying the generation, over the same transport.
func TestRegistrySecretGeneration(t *testing.T) {
	r := NewRegistry(Options{}, func(string) (string, error) { return "same", nil })
	defs := map[string]config.Cluster{"vast01": regCluster("10.0.0.1:80", "control:vast01")}
	gen := int64(4)
	byGen := func(string) int64 { return gen }
	c, err := r.PrepareWith(defs, nil, byGen)
	if err != nil {
		t.Fatal(err)
	}
	c.Commit()
	first, _ := r.Load().Get("vast01")
	if first.SecretGeneration != 4 {
		t.Fatalf("generation %d, want 4", first.SecretGeneration)
	}
	gen = 9
	if c, err = r.PrepareWith(defs, nil, byGen); err != nil {
		t.Fatal(err)
	}
	if added, _ := c.Commit(); len(added) != 1 {
		t.Fatalf("a moved generation was not a change: %v", added)
	}
	second, _ := r.Load().Get("vast01")
	if second == first || second.SecretGeneration != 9 || second.Transport != first.Transport {
		t.Fatalf("after the rotation: new signer %v, generation %d, same transport %v", second != first, second.SecretGeneration, second.Transport == first.Transport)
	}
}

// T02 at the registry: a prepared candidate is not live, resolves its secrets with its own
// resolver, and changes nothing if it is never committed.
func TestRegistryCandidateIsNotLiveUntilCommitted(t *testing.T) {
	r := NewRegistry(Options{}, func(string) (string, error) { return "live", nil })
	if _, _, err := r.Apply(map[string]config.Cluster{"vast01": regCluster("10.0.0.1:80", "control:vast01")}); err != nil {
		t.Fatal(err)
	}
	before := r.Load()
	next := map[string]config.Cluster{"vast01": regCluster("10.0.0.1:80", "control:vast01"), "vast02": regCluster("10.0.0.2:80", "control:vast02")}
	candidate := func(string) (string, error) { return "candidate", nil }
	if _, err := r.Prepare(next, candidate); err != nil {
		t.Fatal(err)
	}
	if r.Load() != before {
		t.Fatal("a prepared candidate went live before Commit")
	}
	if cl, _ := r.Load().Get("vast01"); cl.Creds.Secret != "live" {
		t.Fatalf("the live cluster signs with %q after an uncommitted candidate", cl.Creds.Secret)
	}
	c, err := r.Prepare(next, candidate)
	if err != nil {
		t.Fatal(err)
	}
	added, removed := c.Commit()
	if len(added) != 2 || len(removed) != 0 {
		t.Fatalf("commit: added %v removed %v", added, removed)
	}
	if cl, _ := r.Load().Get("vast01"); cl.Creds.Secret != "candidate" {
		t.Fatalf("after Commit vast01 signs with %q", cl.Creds.Secret)
	}
	if _, ok := r.Load().Get("vast02"); !ok {
		t.Fatal("vast02 missing after Commit")
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
