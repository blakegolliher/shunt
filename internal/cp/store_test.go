package cp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
)

func openStore(t *testing.T, tc *testCluster, i int, key []byte) *Store {
	t.Helper()
	c, err := NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Open(ctx, tc.nodes[i].Client(), c, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("open on %s: %v", tc.nodes[i].Name(), err)
	}
	t.Cleanup(s.Close)
	return s
}

func cluster(secretRef string) config.Cluster {
	return config.Cluster{Type: "s3", Scheme: "http", Region: "us-east-1", Endpoints: []string{"127.0.0.1:1"},
		Credentials: config.Credentials{AccessKey: "AK", SecretRef: secretRef}}
}

// Writes on one node are seen on every node, secrets are sealed in etcd and readable only with
// the key, and a conflicting write is retried onto the newer version.
func TestStoreWritesConvergeAcrossNodes(t *testing.T) {
	tc := startCluster(t, 3)
	key := make([]byte, 32)
	key[0] = 1
	a, b := openStore(t, tc, 0, key), openStore(t, tc, 1, key)
	ctx := context.Background()

	if err := a.PutCluster(ctx, "vast01", cluster("control:vast01"), "s3cr3t", "test"); err != nil {
		t.Fatal(err)
	}
	if err := a.PutCluster(ctx, "vast01", cluster("env:X"), "s3cr3t", "test"); err == nil || !strings.Contains(err.Error(), "control:vast01") {
		t.Errorf("a stored secret with a foreign ref: %v", err)
	}
	if err := a.PutCluster(ctx, "vast02", cluster("control:vast02"), "", "test"); err == nil || !strings.Contains(err.Error(), "no secret is stored") {
		t.Errorf("a control: ref with no stored secret: %v", err)
	}
	if err := a.Adopt(ctx, "acme", "data", "vast01", "data", "test"); err != nil {
		t.Fatal(err)
	}
	if v := a.Version(); v != 2 {
		t.Fatalf("version after two writes: %d", v)
	}
	// The second node sees it through its watch.
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := b.WaitVersion(wctx, 2); err != nil {
		t.Fatal(err)
	}
	p, ok := b.Snapshot().Lookup("acme", "data")
	if !ok || p.Primary != "vast01" {
		t.Fatalf("node b: %+v %v", p, ok)
	}
	if got, err := b.Resolve("control:vast01"); err != nil || got != "s3cr3t" {
		t.Fatalf("node b resolves the cluster secret: %q %v", got, err)
	}
	if got := b.ClusterSecrets(); got["control:vast01"] != "s3cr3t" {
		t.Errorf("ClusterSecrets: %v", got)
	}

	// The secret is sealed in etcd: the raw value never contains it.
	raw, err := tc.nodes[0].Client().Get(ctx, kClusters+"vast01")
	if err != nil || len(raw.Kvs) != 1 || strings.Contains(string(raw.Kvs[0].Value), "s3cr3t") {
		t.Fatalf("the cluster secret is in the clear in etcd: %s %v", raw.Kvs, err)
	}
	// And a node with the wrong key cannot open the store.
	wrong := make([]byte, 32)
	c, _ := NewCipher(wrong)
	if _, err := Open(ctx, tc.nodes[2].Client(), c, nil); err == nil || !errors.Is(err, ErrCiphertext) {
		t.Errorf("opening with the wrong key: %v", err)
	}

	// Two nodes write "at once": both succeed, on consecutive versions.
	done := make(chan error, 2)
	go func() { done <- a.SetTenantDefault(ctx, "acme", "vast01", "a") }()
	go func() { done <- b.Adopt(ctx, "acme", "logs", "vast01", "logs", "b") }()
	e1, e2 := <-done, <-done
	// SetTenantDefault to the same cluster is a conflict by the rule, so accept that one refusal.
	for _, e := range []error{e1, e2} {
		if e != nil && !errors.Is(e, directory.ErrConflict) {
			t.Fatalf("concurrent writes: %v / %v", e1, e2)
		}
	}
	if err := a.WaitVersion(wctx, b.Version()); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Snapshot().Lookup("acme", "logs"); !ok {
		t.Fatal("node a did not see node b's write")
	}

	// The change log has one record per version, and they say who did what.
	changes, err := tc.nodes[0].Client().Get(ctx, kChanges, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(changes.Kvs)) != a.Version() {
		t.Errorf("%d change records for %d versions", len(changes.Kvs), a.Version())
	}
	if !strings.Contains(string(changes.Kvs[0].Value), `"op":"cluster-put"`) || strings.Contains(string(changes.Kvs[0].Value), "s3cr3t") {
		t.Errorf("first change record: %s", changes.Kvs[0].Value)
	}

	// Client keys: stored sealed, delivered to every node, removed everywhere.
	if err := a.Add(sigv4.Credential{AccessKey: "CLIENT", Secret: "cs", Tenant: "acme", Buckets: []string{"data"}}); err != nil {
		t.Fatal(err)
	}
	if err := a.Add(sigv4.Credential{AccessKey: "CLIENT", Secret: "cs"}); !errors.Is(err, auth.ErrDuplicateKey) {
		t.Errorf("duplicate key: %v", err)
	}
	// The same key again, for another bucket, widens its allowlist; for a bucket it reaches, it
	// writes nothing; with another secret it is refused.
	if err := a.Add(sigv4.Credential{AccessKey: "CLIENT", Secret: "cs", Tenant: "acme", Buckets: []string{"logs"}}); err != nil {
		t.Errorf("re-import for another bucket: %v", err)
	}
	if ks := a.Tenant("acme"); len(ks) != 1 || !slices.Equal(ks[0].Buckets, []string{"data", "logs"}) {
		t.Errorf("after re-import: %+v", ks)
	}
	before := a.Version()
	if err := a.Add(sigv4.Credential{AccessKey: "CLIENT", Secret: "cs", Tenant: "acme", Buckets: []string{"data"}}); err != nil || a.Version() != before {
		t.Errorf("re-import for a bucket it reaches: %v, version %d -> %d", err, before, a.Version())
	}
	if err := a.Add(sigv4.Credential{AccessKey: "CLIENT", Secret: "other", Tenant: "acme"}); !errors.Is(err, auth.ErrDuplicateKey) {
		t.Errorf("re-import with another secret: %v", err)
	}
	if err := b.WaitVersion(wctx, a.Version()); err != nil {
		t.Fatal(err)
	}
	if ks := b.Tenant("acme"); len(ks) != 1 || ks[0].Secret != "cs" || len(ks[0].Buckets) != 2 {
		t.Fatalf("node b keys: %+v", ks)
	}
	if all := b.All(); len(all) != 1 || all[0].AccessKey != "CLIENT" {
		t.Fatalf("All: %+v", all)
	}
	rawKey, _ := tc.nodes[0].Client().Get(ctx, kCredentials+"CLIENT")
	if len(rawKey.Kvs) != 1 || strings.Contains(string(rawKey.Kvs[0].Value), `"cs"`) {
		t.Errorf("the client secret is in the clear in etcd: %s", rawKey.Kvs)
	}
	if err := b.Remove("CLIENT"); err != nil {
		t.Fatal(err)
	}
	if err := b.Remove("CLIENT"); !errors.Is(err, auth.ErrUnknownKey) {
		t.Errorf("removing twice: %v", err)
	}
	if err := a.WaitVersion(wctx, b.Version()); err != nil {
		t.Fatal(err)
	}
	if len(a.All()) != 0 {
		t.Error("node a still holds the removed key")
	}

	// Removing a referenced cluster is refused; the secret goes with the cluster when it can go.
	if err := a.RemoveCluster(ctx, "vast01", "test"); !errors.Is(err, directory.ErrInUse) {
		t.Errorf("removing a referenced cluster: %v", err)
	}
	if err := a.PutCluster(ctx, "vast03", cluster("control:vast03"), "other", "test"); err != nil {
		t.Fatal(err)
	}
	if err := a.RemoveCluster(ctx, "vast03", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Resolve("control:vast03"); err == nil {
		t.Error("the removed cluster's secret is still resolvable")
	}
}

// A node that restarts reloads the directory from etcd; a store whose watch is cut reloads too.
func TestStoreSurvivesRestartAndReload(t *testing.T) {
	tc := startCluster(t, 3)
	key := make([]byte, 32)
	a := openStore(t, tc, 0, key)
	ctx := context.Background()
	if err := a.PutCluster(ctx, "vast01", cluster("control:vast01"), "s", "test"); err != nil {
		t.Fatal(err)
	}
	if err := a.Adopt(ctx, "acme", "data", "vast01", "data", "test"); err != nil {
		t.Fatal(err)
	}
	v := a.Version()
	a.Close()
	tc.restart(1)
	b := openStore(t, tc, 1, key)
	if b.Version() != v {
		t.Fatalf("node b after restart: version %d, want %d", b.Version(), v)
	}
	if _, ok := b.Snapshot().Lookup("acme", "data"); !ok {
		t.Fatal("node b lost the placement across a restart")
	}
	// Prepare sees every version, with the candidate's secrets resolvable.
	c := openStore(t, tc, 2, key)
	seen := 0
	c.Prepare = func(f *directory.File) error {
		if _, err := c.Resolve("control:vast01"); err != nil {
			return err
		}
		seen++
		return nil
	}
	if err := c.SetTenantDefault(ctx, "acme", "vast01", "t"); err == nil || !errors.Is(err, directory.ErrConflict) {
		t.Fatalf("setting the default to what it is: %v", err)
	}
	if err := c.PutCluster(ctx, "vast02", cluster("control:vast02"), "s2", "t"); err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Error("Prepare did not run on the write")
	}
	refused := errors.New("cannot build")
	c.Prepare = func(*directory.File) error { return refused }
	if err := c.PutCluster(ctx, "vast04", cluster("control:vast04"), "s4", "t"); !errors.Is(err, refused) {
		t.Errorf("Prepare's refusal did not refuse the write: %v", err)
	}
	if _, ok := c.Snapshot().Cluster("vast04"); ok {
		t.Error("a refused write was installed")
	}
}

// A placement stored in schema v2 (ADR-0018) loads as the same placement: build N1 reads what a
// later build writes, before any build writes it.
func TestStoreReadsPlacementSchemaV2(t *testing.T) {
	tc := startCluster(t, 1)
	key := make([]byte, 32)
	a := openStore(t, tc, 0, key)
	ctx := context.Background()
	if err := a.PutCluster(ctx, "vast01", cluster("control:vast01"), "s", "test"); err != nil {
		t.Fatal(err)
	}
	if err := a.Adopt(ctx, "acme", "data", "vast01", "data", "test"); err != nil {
		t.Fatal(err)
	}
	want, _ := a.Snapshot().Lookup("acme", "data")
	a.Close()
	raw, err := json.Marshal(want.ToV2())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tc.nodes[0].Client().Put(ctx, kPlacements+"acme/data", string(raw)); err != nil {
		t.Fatal(err)
	}
	b := openStore(t, tc, 0, key)
	if got, ok := b.Snapshot().Lookup("acme", "data"); !ok || !reflect.DeepEqual(*got, *want) {
		t.Fatalf("v2 placement loaded as %+v, want %+v (stored %s)", got, want, raw)
	}
}
