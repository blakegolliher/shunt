package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/runtimecfg"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// hookStore is a key table that runs a hook the first time it is asked for a key: a new runtime
// bundle published while a request is authenticating.
type hookStore struct {
	mapStore
	once sync.Once
	hook func()
}

func (h *hookStore) Lookup(ctx context.Context, ak string) (sigv4.Credential, error) {
	h.once.Do(h.hook)
	return h.mapStore.Lookup(ctx, ak)
}

// A request authenticates, routes and signs with the one bundle it took (ADR-0021 D1): a bundle with
// a rotated cluster secret, published after the request has taken its own and before it signs,
// does not reach it. The next request signs with the new secret.
func TestRequestKeepsItsBundle(t *testing.T) {
	var mu sync.Mutex
	var signedWith []string
	oldKey := mapStore{clusterAK: {AccessKey: clusterAK, Secret: clusterSecret}}
	newKey := mapStore{clusterAK: {AccessKey: clusterAK, Secret: "rotated-cluster-secret"}}
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		which := "neither"
		if _, err := sigv4.Verify(r.Context(), r, oldKey, time.Now(), sigv4.Options{}); err == nil {
			which = "old"
		} else if _, err := sigv4.Verify(r.Context(), r, newKey, time.Now(), sigv4.Options{}); err == nil {
			which = "new"
		}
		mu.Lock()
		signedWith = append(signedWith, which)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(be.Close)
	set, dir := resignParts(t, strings.TrimPrefix(be.URL, "http://"), Capabilities{true, true}, "t")

	// The rotated set: the same cluster, its secret changed.
	rotated := upstream.NewRegistry(upstream.Options{}, func(string) (string, error) { return "rotated-cluster-secret", nil })
	t.Cleanup(rotated.Close)
	if _, _, err := rotated.Apply(dir.Snapshot().File().Clusters); err != nil {
		t.Fatal(err)
	}
	keys := mapStore{clientAK: {AccessKey: clientAK, Secret: clientSecret, Tenant: "t"}}
	var rt *runtimecfg.Publisher
	first := &hookStore{mapStore: keys, hook: func() {
		if err := rt.Refresh(dir.Snapshot(), keys, rotated.Load()); err != nil {
			t.Error(err)
		}
	}}
	rt = runtimecfg.NewPublisher(&runtimecfg.Bundle{Snapshot: dir.Snapshot(), Keys: first, Clusters: set.Load()})
	r := newRig(t, be.Config.Handler, time.Second)
	r.h.Mode, r.h.Runtime, r.h.Dir, r.h.Rewrite = ModeResign, rt, dir, true

	for range 2 {
		req, err := http.NewRequest(http.MethodGet, r.front.URL+"/bbb/k", nil)
		if err != nil {
			t.Fatal(err)
		}
		clientSign(req, sigv4.UnsignedPayload)
		resp, body := do(t, req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET: %d %s", resp.StatusCode, body)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(signedWith) != 2 || signedWith[0] != "old" || signedWith[1] != "new" {
		t.Fatalf("upstream requests signed with %v; want the first with the secret of the bundle it took, the second with the rotated one", signedWith)
	}
}
