package cp

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Export is one installed version whole, at the production store (third review, R3-05): while node
// b rotates vast01's access key and secret together a hundred times, every export node a serves
// pairs the access key with its own secret, however the reads and the watch's installs interleave.
// Negative control: reading the snapshot and the secrets one after another (as GET /v1/directory
// did) pairs one version's key with another's secret within the same run.
func TestStoreExportIsOneVersion(t *testing.T) {
	tc := startCluster(t, 2)
	key := make([]byte, 32)
	key[0] = 5
	a, b := openStore(t, tc, 0, key), openStore(t, tc, 1, key)
	ctx := context.Background()
	def := cluster("control:vast01")
	def.Credentials.AccessKey = "ak-0"
	if err := b.PutCluster(ctx, "vast01", def, "sk-0", "test"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 1; i <= 100; i++ {
			def.Credentials.AccessKey = fmt.Sprintf("ak-%d", i)
			if err := b.PutCluster(ctx, "vast01", def, fmt.Sprintf("sk-%d", i), "test"); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	checked := 0
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			if checked == 0 {
				t.Fatal("no export was read while the rotations ran")
			}
			return
		default:
		}
		exp := a.Export()
		cl, ok := exp.Snapshot.File().Clusters["vast01"]
		if !ok {
			continue // before a has installed the first version
		}
		if want := "sk-" + strings.TrimPrefix(cl.Credentials.AccessKey, "ak-"); exp.Secrets["control:vast01"] != want {
			t.Fatalf("export v%d pairs access key %s with secret %q", exp.Snapshot.Version(), cl.Credentials.AccessKey, exp.Secrets["control:vast01"])
		}
		checked++
	}
}
