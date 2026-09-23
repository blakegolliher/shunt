package member

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// fakeControl speaks the member's side of the control API: directory long-poll, heartbeat,
// create and delete. It is what a member sees of shunt-control.
type fakeControl struct {
	mu      sync.Mutex
	dir     control.Directory
	beats   []control.Heartbeat
	down    atomic.Bool
	srv     *httptest.Server
	answers int64 // version the heartbeat answers with; 0 = the directory's
}

func newFakeControl(t *testing.T) *fakeControl {
	t.Helper()
	f := &fakeControl{}
	f.dir = control.Directory{File: directory.File{Version: 1,
		Clusters:   map[string]config.Cluster{"vast01": {Type: "s3", Scheme: "http", Region: "r", Endpoints: []string{"127.0.0.1:1"}, Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:vast01"}}},
		Tenants:    map[string]directory.Tenant{"acme": {DefaultCluster: "vast01"}},
		Placements: map[string]directory.Placement{"acme/data": {State: directory.StateActive, Primary: "vast01", Names: map[string]string{"vast01": "data"}}}},
		Credentials: []control.Credential{{AccessKey: "CLIENT", Secret: "cs", Tenant: "acme"}},
		Secrets:     map[string]string{"control:vast01": "s1"}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/directory", func(w http.ResponseWriter, r *http.Request) {
		if f.down.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		var since int64
		_, _ = fmtSscan(r.URL.Query().Get("since"), &since)
		deadline := time.Now().Add(200 * time.Millisecond)
		for {
			f.mu.Lock()
			d := f.dir
			f.mu.Unlock()
			if d.Version > since {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(d)
				return
			}
			if r.URL.Query().Get("wait") == "0s" || time.Now().After(deadline) {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	mux.HandleFunc("POST /v1/fleet/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if f.down.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		var hb control.Heartbeat
		_ = json.NewDecoder(r.Body).Decode(&hb)
		f.mu.Lock()
		f.beats = append(f.beats, hb)
		v := f.dir.Version
		if f.answers != 0 {
			v = f.answers
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(control.HeartbeatAnswer{Version: v, LeaseTTL: time.Second})
	})
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/create", func(w http.ResponseWriter, r *http.Request) {
		var req control.CreateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		defer f.mu.Unlock()
		key := directory.Key(r.PathValue("tenant"), r.PathValue("bucket"))
		if _, ok := f.dir.Placements[key]; ok {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(control.Error{Code: "conflict", Message: "directory: placement already exists"})
			return
		}
		f.dir.Placements = cloneP(f.dir.Placements)
		f.dir.Placements[key] = directory.Placement{State: directory.StateActive, Primary: req.Cluster, Names: map[string]string{req.Cluster: req.Name}}
		f.dir.Version++
		_ = json.NewEncoder(w).Encode(control.VersionResult{Key: key, Version: f.dir.Version})
	})
	mux.HandleFunc("DELETE /v1/placements/{tenant}/{bucket}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		key := directory.Key(r.PathValue("tenant"), r.PathValue("bucket"))
		if _, ok := f.dir.Placements[key]; !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(control.Error{Code: "not_found", Message: "no such placement"})
			return
		}
		f.dir.Placements = cloneP(f.dir.Placements)
		delete(f.dir.Placements, key)
		f.dir.Version++
		_ = json.NewEncoder(w).Encode(control.VersionResult{Key: key, Version: f.dir.Version})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func cloneP(m map[string]directory.Placement) map[string]directory.Placement {
	out := make(map[string]directory.Placement, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func fmtSscan(s string, v *int64) (int, error) {
	if s == "" {
		return 0, nil
	}
	n, err := json.Number(s).Int64()
	*v = n
	return 1, err
}

func (f *fakeControl) bump() {
	f.mu.Lock()
	f.dir.Version++
	f.mu.Unlock()
}

func newClient(t *testing.T, f *fakeControl) *Client {
	t.Helper()
	c := New(Config{Endpoints: []string{f.srv.URL}, ProxyID: "p1", CacheDir: t.TempDir(), Interval: 50 * time.Millisecond, LeaseTTL: 300 * time.Millisecond, LongPoll: 200 * time.Millisecond},
		slog.New(slog.DiscardHandler))
	if err := c.Load(); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestMemberInstallsDirectoryAndKeys(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	c.Telemetry = telemetry.NewCollector()
	c.Telemetry.Observe(telemetry.Observation{At: time.Now().Add(-11 * time.Second), Operation: "GetObject", Cluster: "vast01", Status: 200, ClientTotal: time.Millisecond})
	prepared := 0
	c.Prepare = func(fl *directory.File) error {
		if _, err := c.Resolve("control:vast01"); err != nil {
			return err
		}
		prepared++
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Stale() {
		t.Fatal("stale after a registered heartbeat")
	}
	if v := c.Snapshot().Version(); v != 1 || prepared != 1 {
		t.Fatalf("version %d prepared %d", v, prepared)
	}
	if _, ok := c.Snapshot().Lookup("acme", "data"); !ok {
		t.Fatal("placement not installed")
	}
	if cred, err := c.Keys().Lookup(ctx, "CLIENT"); err != nil || cred.Secret != "cs" || cred.Tenant != "acme" {
		t.Fatalf("client key: %+v %v", cred, err)
	}
	if s, err := c.Resolve("control:vast01"); err != nil || s != "s1" {
		t.Fatalf("cluster secret: %q %v", s, err)
	}
	if _, err := c.Resolve("control:nope"); err == nil {
		t.Error("an undelivered secret resolved")
	}
	if st, _ := os.Stat(filepath.Join(c.cfg.CacheDir, "directory.json")); st == nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("cache file: %v", st)
	}

	// The poll picks up a newer version; a create is forwarded and visible before it returns.
	go c.Run(ctx)
	f.bump()
	wctx, wcancel := context.WithTimeout(ctx, 3*time.Second)
	defer wcancel()
	if err := c.WaitVersion(wctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, "acme", "new", "vast01", "acme-1234-new", "proxy:CLIENT"); err != nil {
		t.Fatal(err)
	}
	if p, ok := c.Snapshot().Lookup("acme", "new"); !ok || p.Names["vast01"] != "acme-1234-new" {
		t.Fatalf("created placement not visible: %+v %v", p, ok)
	}
	if err := c.Create(ctx, "acme", "new", "vast01", "x", "p"); err == nil || !errors.Is(err, directory.ErrExists) {
		t.Errorf("creating twice: %v", err)
	}
	if err := c.Delete(ctx, "acme", "new", "p"); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Snapshot().Lookup("acme", "new"); ok {
		t.Fatal("deleted placement still visible")
	}
	if err := c.Delete(ctx, "acme", "new", "p"); err == nil || !errors.Is(err, directory.ErrNotFound) {
		t.Errorf("deleting twice: %v", err)
	}
	f.mu.Lock()
	n := len(f.beats)
	var last control.Heartbeat
	if n > 0 {
		last = f.beats[n-1]
	}
	f.mu.Unlock()
	if n < 2 {
		t.Errorf("only %d heartbeats", n)
	}
	if last.Telemetry == nil || len(last.Telemetry.Sketches) == 0 {
		t.Error("heartbeat carried no completed telemetry window")
	}
}

// The lease lapses when the control plane is unreachable, and again when the member is behind
// the version the control plane answers with; it returns once the member has caught up. A
// restart with the control plane down serves the cached directory.
func TestMemberStaleModeAndCache(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	go c.Run(ctx)
	f.down.Store(true)
	waitFor(t, 2*time.Second, "stale with the control plane down", c.Stale)
	f.down.Store(false)
	waitFor(t, 2*time.Second, "fresh with the control plane back", func() bool { return !c.Stale() })

	// The control plane answers with a version this member cannot fetch: still stale.
	f.mu.Lock()
	f.answers = 99
	f.mu.Unlock()
	waitFor(t, 2*time.Second, "stale while behind the control plane", c.Stale)
	f.mu.Lock()
	f.answers = 0
	f.mu.Unlock()
	waitFor(t, 2*time.Second, "fresh once caught up", func() bool { return !c.Stale() })
	cancel()

	// A new client on the same cache dir, with the control plane down, serves the cached version.
	f.down.Store(true)
	c2 := New(c.cfg, slog.New(slog.DiscardHandler))
	if err := c2.Load(); err != nil {
		t.Fatal(err)
	}
	if v := c2.Snapshot().Version(); v != c.Snapshot().Version() {
		t.Fatalf("cached version %d, want %d", v, c.Snapshot().Version())
	}
	if _, ok := c2.Snapshot().Lookup("acme", "data"); !ok || !c2.Stale() {
		t.Fatal("cached placement missing, or not stale")
	}
	if cred, err := c2.Keys().Lookup(context.Background(), "CLIENT"); err != nil || cred.Secret != "cs" {
		t.Fatalf("cached client key: %+v %v", cred, err)
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer rcancel()
	if err := c2.Register(rctx); err == nil {
		t.Error("registered with the control plane down")
	}
}

// A directory the proxy cannot prepare (a cluster it cannot build) is refused, and the last good
// version stays, secrets included.
func TestMemberRefusesAVersionItCannotPrepare(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	c.Prepare = func(fl *directory.File) error {
		if _, ok := fl.Clusters["bad"]; ok {
			return errors.New("cannot build bad")
		}
		return nil
	}
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.dir.Clusters["bad"] = config.Cluster{Type: "s3", Scheme: "http", Region: "r", Endpoints: []string{"127.0.0.1:2"}, Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:bad"}}
	f.dir.Secrets = map[string]string{"control:vast01": "s1", "control:bad": "sb"}
	f.dir.Version++
	f.mu.Unlock()
	if _, err := c.fetch(ctx, 1, 0); err == nil {
		t.Fatal("a version the proxy cannot prepare was installed")
	}
	if v := c.Snapshot().Version(); v != 1 {
		t.Fatalf("version after a refused install: %d", v)
	}
	if s, err := c.Resolve("control:vast01"); err != nil || s != "s1" {
		t.Fatalf("secrets after a refused install: %q %v", s, err)
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: still not so after %s", what, d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A control plane that serves placements in schema v2 (ADR-0018) routes exactly as one serving v1,
// through the long poll and again from the member's cache.
func TestMemberReadsPlacementSchemaV2(t *testing.T) {
	f := newFakeControl(t)
	want := f.dir.Placements["acme/data"]
	f.dir.Placements = map[string]directory.Placement{"acme/data": want.ToV2()}
	c := newClient(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	go c.Run(ctx)
	waitFor(t, 2*time.Second, "the v2 placement installed", func() bool {
		p, ok := c.Snapshot().Lookup("acme", "data")
		return ok && reflect.DeepEqual(*p, want)
	})
	cancel()
	f.down.Store(true)
	c2 := New(c.cfg, slog.New(slog.DiscardHandler))
	if err := c2.Load(); err != nil {
		t.Fatal(err)
	}
	if p, ok := c2.Snapshot().Lookup("acme", "data"); !ok || !reflect.DeepEqual(*p, want) {
		t.Fatalf("from the cache: %+v, want %+v", p, want)
	}
}
