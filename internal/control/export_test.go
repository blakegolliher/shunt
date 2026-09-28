package control

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/blakegolliher/shunt/internal/sigv4"
)

// hookKeys runs hook on every All while it is set: a key store another writer changes the directory
// through, between the directory's read and the keys'.
type hookKeys struct {
	Keys
	mu   sync.Mutex
	hook func()
}

func (k *hookKeys) All() []sigv4.Credential {
	k.mu.Lock()
	hook := k.hook
	k.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

// GET /v1/directory is one version whole (third review, R3-05). A lab reads its directory, keys and
// secrets one after another: a rotation of vast01's access key and secret that lands between the
// directory's read and the others' makes the handler read again, and the answer is the rotated
// version with its own secret, never the old key with the new secret. A directory that never stops
// changing while it is read answers a retryable 503, never a mixed pair. Negative control: without
// the version check around the reads, the answer is version 7's key with version 8's secret.
func TestDirectoryExportIsOneVersion(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	ctx := context.Background()
	def := rg.dir.Snapshot().File().Clusters["vast01"]
	def.Credentials.SecretRef = "control:vast01"
	if err := rg.dir.PutCluster(ctx, "vast01", def, "", "test"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	secret := "old-secret"
	rg.ctl.ClusterSecrets = func() map[string]string {
		mu.Lock()
		defer mu.Unlock()
		return map[string]string{"control:vast01": secret}
	}
	rotations := 0
	rotate := func() {
		def := rg.dir.Snapshot().File().Clusters["vast01"]
		rotations++
		def.Credentials.AccessKey = "rotated-" + string(rune('a'+rotations))
		if err := rg.dir.PutCluster(ctx, "vast01", def, "", "test"); err != nil {
			t.Error(err)
		}
		mu.Lock()
		secret = def.Credentials.AccessKey + "-secret"
		mu.Unlock()
	}
	keys := &hookKeys{Keys: &stubKeys{}}
	rg.ctl.Keys = keys
	keys.hook = func() {
		keys.hook = nil // once: the rotation lands during the first read, not the retry
		rotate()
	}
	var got Directory
	rg.must("GET", "/v1/directory", nil, &got)
	ak := got.Clusters["vast01"].Credentials.AccessKey
	if want := ak + "-secret"; got.Secrets["control:vast01"] != want || got.Version != rg.dir.Snapshot().Version() {
		t.Fatalf("directory v%d carries access key %q with secret %q (want %q, at v%d)", got.Version, ak, got.Secrets["control:vast01"], want, rg.dir.Snapshot().Version())
	}

	keys.mu.Lock()
	keys.hook = rotate // every read: the directory never settles
	keys.mu.Unlock()
	if code, raw := rg.call("GET", "/v1/directory", nil, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("a directory that never settles: %d %s", code, raw)
	}
}
