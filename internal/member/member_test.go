package member

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
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
	// answer, if set, edits each heartbeat answer before it is sent; set it with setAnswer.
	answer func(*control.HeartbeatAnswer)
}

func newFakeControl(t *testing.T) *fakeControl {
	t.Helper()
	f := &fakeControl{}
	f.dir = control.Directory{File: directory.File{Version: 1, Identity: testIdentity,
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
		f.mu.Lock()
		cur := f.dir.Identity
		f.mu.Unlock()
		if e := r.URL.Query().Get("epoch"); e != "" && e != cur.Epoch {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(control.Error{Code: control.CodeEpochMismatch, Message: "another epoch", CurrentIdentity: &cur})
			return
		}
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
		edit := f.answer
		f.mu.Unlock()
		if hb.Protocol != control.Protocol {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(control.Error{Code: control.CodeProtocol, Message: "protocol"})
			return
		}
		f.mu.Lock()
		cur := f.dir.Identity
		f.mu.Unlock()
		if !hb.Identity.IsZero() && hb.Identity != cur {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(control.Error{Code: control.CodeEpochMismatch, Message: "another epoch", CurrentIdentity: &cur})
			return
		}
		a := control.HeartbeatAnswer{Seq: hb.Seq, Identity: cur, Version: v, LeaseTTL: time.Second}
		if edit != nil {
			edit(&a)
		}
		_ = json.NewEncoder(w).Encode(a)
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

// testIdentity is the fake control plane's lineage.
var testIdentity = directory.Identity{ClusterID: "c0ffee00c0ffee00c0ffee00c0ffee00", Epoch: "e0000000000000000000000000000001"}

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

func (f *fakeControl) setAnswer(edit func(*control.HeartbeatAnswer)) {
	f.mu.Lock()
	f.answer = edit
	f.mu.Unlock()
}

// version returns a deep copy of the fake's directory at version v.
func (f *fakeControl) version(t *testing.T, v int64) *control.Directory {
	t.Helper()
	f.mu.Lock()
	data, err := json.Marshal(f.dir)
	f.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var d control.Directory
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	d.Version = v
	return &d
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
	c.Prepare = func(fl *directory.File, resolve func(string) (string, error)) (func(), error) {
		if _, err := resolve("control:vast01"); err != nil {
			return nil, err
		}
		prepared++
		return func() {}, nil
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
	c.Prepare = func(fl *directory.File, _ func(string) (string, error)) (func(), error) {
		if _, ok := fl.Clusters["bad"]; ok {
			return nil, errors.New("cannot build bad")
		}
		return func() {}, nil
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

// T01: installs are serialized. A version whose preparation is slow is not installed over a newer
// one that a concurrent fetch (a heartbeat's, a forwarded write's) brought in meanwhile; the
// installed version and the cache only move forward, and a late older version is dropped.
func TestMemberNeverInstallsAnOlderVersionOverANewerOne(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	preparing2 := make(chan struct{})
	installed3 := make(chan struct{})
	var mu sync.Mutex
	var order []int64
	c.OnInstall = func(s *directory.Snapshot) {
		mu.Lock()
		order = append(order, s.Version())
		mu.Unlock()
		if s.Version() == 3 {
			close(installed3)
		}
	}
	c.Prepare = func(fl *directory.File, _ func(string) (string, error)) (func(), error) {
		if fl.Version == 2 {
			close(preparing2)
			// Unserialized, version 3 installs while 2 is held here, and 2 then lands on top of it.
			// Serialized, 3 waits for 2, and the timer lets 2 go.
			select {
			case <-installed3:
			case <-time.After(200 * time.Millisecond):
			}
		}
		return func() {}, nil
	}
	if _, err := c.install(f.version(t, 1), true); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := c.install(f.version(t, 2), true); err != nil {
			t.Error(err)
		}
	}()
	<-preparing2
	go func() {
		defer wg.Done()
		if _, err := c.install(f.version(t, 3), true); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	if v := c.Snapshot().Version(); v != 3 {
		t.Fatalf("installed version %d after installing 2 and 3 concurrently, want 3", v)
	}
	mu.Lock()
	for i := 1; i < len(order); i++ {
		if order[i] < order[i-1] {
			t.Errorf("installed versions went back: %v", order)
		}
	}
	mu.Unlock()
	if installed, err := c.install(f.version(t, 2), true); installed || err != nil {
		t.Errorf("a late older version: installed %v, err %v", installed, err)
	}
	if v := c.Snapshot().Version(); v != 3 {
		t.Fatalf("a late older version was installed over 3: at %d", v)
	}
	var cached control.Directory
	data, err := os.ReadFile(filepath.Join(c.cfg.CacheDir, "directory.json"))
	if err == nil {
		err = json.Unmarshal(data, &cached)
	}
	if err != nil || cached.Version != 3 {
		t.Fatalf("cache at version %d (%v), want 3", cached.Version, err)
	}
}

// T02: a candidate refused after its new secrets resolved leaves the live resolver, the cluster
// registry, the client keys and the cache as they were. The refused secret never reaches any of
// them, not even while the candidate is being prepared.
func TestMemberRefusedCandidateLeavesTheLiveSecrets(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	reg := upstream.NewRegistry(upstream.Options{}, c.Resolve)
	t.Cleanup(reg.Close)
	c.Prepare = func(fl *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, err := reg.Prepare(fl.Clusters, resolve)
		if err != nil {
			return nil, err
		}
		if fl.Version == 2 {
			if s, _ := c.Resolve("control:vast01"); s != "s1" {
				t.Errorf("the live resolver answered the candidate's secret %q while it was prepared", s)
			}
			return nil, errors.New("refused after its secrets resolved")
		}
		return func() { cand.Commit() }, nil
	}
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.dir.Secrets = map[string]string{"control:vast01": "s2"}
	f.dir.Credentials = []control.Credential{{AccessKey: "CLIENT", Secret: "cs2", Tenant: "acme"}}
	f.dir.Version++
	f.mu.Unlock()
	if _, err := c.fetch(ctx, 1, 0); err == nil {
		t.Fatal("the refused version was installed")
	}
	if s, err := c.Resolve("control:vast01"); err != nil || s != "s1" {
		t.Errorf("live resolver after a refused version: %q %v, want s1", s, err)
	}
	if cl, ok := reg.Load().Get("vast01"); !ok || cl.Creds.Secret != "s1" {
		t.Errorf("the live registry signs with %q after a refused version, want s1", cl.Creds.Secret)
	}
	if cred, err := c.Keys().Lookup(ctx, "CLIENT"); err != nil || cred.Secret != "cs" {
		t.Errorf("client key after a refused version: %+v %v", cred, err)
	}
	data, err := os.ReadFile(filepath.Join(c.cfg.CacheDir, "directory.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "s2") || strings.Contains(string(data), "cs2") {
		t.Error("the refused version's secrets reached the cache")
	}
}

// The heartbeat reports the secret generation of each cluster's signer, from the installed version.
func TestHeartbeatReportsSecretGenerations(t *testing.T) {
	f := newFakeControl(t)
	f.mu.Lock()
	f.dir.Generations = map[string]int64{directory.SecretResource("vast01"): 1}
	f.mu.Unlock()
	c := newClient(t, f)
	if err := c.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	last := f.beats[len(f.beats)-1]
	if last.Secrets["vast01"] != "1" || len(last.Secrets) != 1 {
		t.Fatalf("heartbeat secrets %v, want vast01 at 1", last.Secrets)
	}
}

// fakeClock is a settable clock for the lease tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (k *fakeClock) Now() time.Time {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.now
}

func (k *fakeClock) Add(d time.Duration) {
	k.mu.Lock()
	k.now = k.now.Add(d)
	k.mu.Unlock()
}

// T07: the lease runs from the heartbeat's send time, for the shorter of the control plane's
// grant and the proxy's own lease_ttl, and only an answer to the heartbeat sent, on time, with a
// grant, renews it.
func TestMemberLeaseFollowsTheServerGrant(t *testing.T) {
	f := newFakeControl(t) // grants 1 s
	c := New(Config{Endpoints: []string{f.srv.URL}, ProxyID: "p1", CacheDir: t.TempDir(), Interval: time.Second, LeaseTTL: time.Minute, LongPoll: 200 * time.Millisecond},
		slog.New(slog.DiscardHandler))
	clock := &fakeClock{now: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)}
	c.Now = clock.Now
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Stale() {
		t.Fatal("stale right after a granted heartbeat")
	}
	clock.Add(time.Second)
	if c.Stale() {
		t.Fatal("stale at the end of a 1 s grant")
	}
	clock.Add(time.Millisecond)
	if !c.Stale() {
		t.Fatal("fresh past the server's 1 s grant: the lease followed the proxy's own 60 s lease_ttl")
	}

	refused := []struct {
		name string
		edit func(*control.HeartbeatAnswer)
	}{
		{"an answer delayed past the lease it grants", func(*control.HeartbeatAnswer) { clock.Add(2 * time.Second) }},
		{"a zero grant", func(a *control.HeartbeatAnswer) { a.LeaseTTL = 0 }},
		{"an answer to another heartbeat", func(a *control.HeartbeatAnswer) { a.Seq-- }},
		{"a version behind the proxy's", func(a *control.HeartbeatAnswer) { a.Version = 0 }},
		{"an answer for another lineage", func(a *control.HeartbeatAnswer) { a.Identity.Epoch = "e0000000000000000000000000000002" }},
	}
	for _, r := range refused {
		f.setAnswer(r.edit)
		if err := c.beat(ctx); err == nil {
			t.Errorf("%s: no error", r.name)
		}
		if !c.Stale() {
			t.Errorf("%s renewed the lease", r.name)
		}
	}

	f.setAnswer(nil)
	if err := c.beat(ctx); err != nil || c.Stale() {
		t.Fatalf("a good answer after the refused ones: %v, stale %v", err, c.Stale())
	}
	// An older answer never replaces a newer grant, even with a later deadline.
	if err := c.renew(ctx, c.seq.Load()-1, clock.Now().Add(time.Hour), c.Snapshot().Version(), control.HeartbeatAnswer{Seq: c.seq.Load() - 1, Identity: testIdentity, Version: c.Snapshot().Version(), LeaseTTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	clock.Add(time.Second + time.Millisecond)
	if !c.Stale() {
		t.Fatal("an older heartbeat's answer extended the lease")
	}
}

// A control plane that moves to another epoch (a restore) leaves this member on the old lineage:
// its versions compare with nothing there. The member refuses the new lineage's directory, takes
// no lease, and stays stale until it is re-enrolled; it never installs across lineages.
func TestMemberStaysStaleAcrossAnEpochChange(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Stale() {
		t.Fatal("stale after registering")
	}
	f.mu.Lock()
	f.dir.Identity.Epoch = "e0000000000000000000000000000002"
	f.dir.Version = 1 // a restore: a lower or equal version in the new epoch
	f.mu.Unlock()
	if err := c.beat(ctx); err == nil {
		t.Fatal("a heartbeat from the old epoch was answered with a lease")
	}
	if !c.Stale() {
		t.Fatal("fresh after the control plane moved to another epoch")
	}
	if _, err := c.fetch(ctx, c.Snapshot().Version(), 0); err == nil {
		t.Fatal("the new epoch's directory was fetched as if it continued the old one")
	}
	if _, err := c.install(f.version(t, 5), true); err == nil {
		t.Fatal("a directory from another epoch was installed")
	}
	if id := c.Snapshot().File().Identity; id != testIdentity {
		t.Fatalf("installed lineage changed to %+v", id)
	}
	f.setAnswer(nil)
	_ = c.beat(ctx)
	if !c.Stale() {
		t.Fatal("a lineage fault cleared by itself")
	}
}

// A cache written before schema 2 has no identity: it is ignored rather than served, since none of
// its versions can be compared with the control plane's.
func TestMemberIgnoresACacheWithoutIdentity(t *testing.T) {
	f := newFakeControl(t)
	dir := t.TempDir()
	d := f.version(t, 7)
	d.Identity = directory.Identity{}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "directory.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	c := New(Config{Endpoints: []string{f.srv.URL}, ProxyID: "p1", CacheDir: dir, Interval: time.Second, LeaseTTL: 3 * time.Second}, slog.New(slog.DiscardHandler))
	if err := c.Load(); err != nil {
		t.Fatal(err)
	}
	if v := c.Snapshot().Version(); v != 0 {
		t.Fatalf("a cache with no identity was installed at version %d", v)
	}
}

// Stale is on the request path for every moving bucket: a pointer load, read by many requests at
// once while heartbeats renew the lease.
func BenchmarkStaleParallel(b *testing.B) {
	c := New(Config{LeaseTTL: time.Minute}, slog.New(slog.DiscardHandler))
	c.lease.Store(&grant{seq: 1, until: time.Now().Add(time.Hour)})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if c.Stale() {
				b.Error("stale")
			}
		}
	})
}

// BenchmarkInstall is one directory version of 1,000 placements through validation, preparation,
// publication and the cache write.
func BenchmarkInstall(b *testing.B) {
	c := New(Config{CacheDir: b.TempDir(), LeaseTTL: time.Minute}, slog.New(slog.DiscardHandler))
	base := control.Directory{File: directory.File{Identity: testIdentity,
		Clusters: map[string]config.Cluster{"vast01": {Type: "s3", Scheme: "http", Region: "r", EndpointMode: "static", Endpoints: []string{"127.0.0.1:1"},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:vast01"}}},
		Tenants: map[string]directory.Tenant{"acme": {DefaultCluster: "vast01"}}, Placements: map[string]directory.Placement{}},
		Secrets: map[string]string{"control:vast01": "s1"}}
	for i := range 1000 {
		name := fmt.Sprintf("bucket-%04d", i)
		base.Placements["acme/"+name] = directory.Placement{State: directory.StateActive, Primary: "vast01", Names: map[string]string{"vast01": name}}
	}
	for i := 0; b.Loop(); i++ {
		d := base
		d.Version = int64(i + 1)
		if _, err := c.install(&d, true); err != nil {
			b.Fatal(err)
		}
	}
}
