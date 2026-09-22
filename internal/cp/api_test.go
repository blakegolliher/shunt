package cp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// The control API served from the etcd store, with member proxies that heartbeat over HTTP
// (ADR-0016 on ADR-0015): the walkthrough's steps, the hold, the strict first step, a released
// hold, a silent member, forget, and fleet-wide cutover evidence. Time is real; leases are short.

// fakeS3 is an in-process backend that answers versioning as never enabled.
func fakeS3(t *testing.T) *httptest.Server {
	t.Helper()
	be := s3mem.New()
	fake := gofakes3.New(be, gofakes3.WithTimeSkewLimit(0)).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.URL.Query()["versioning"]; ok && r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`<VersioningConfiguration/>`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/" {
			w.Header().Set("Server", "MinIO")
			_, _ = w.Write([]byte(`<ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>`))
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	for _, b := range []string{"data01", "logs"} {
		if err := be.CreateBucket(b); err != nil {
			t.Fatal(err)
		}
	}
	return srv
}

// node is one control node's API over the shared etcd cluster.
type node struct {
	t     *testing.T
	api   *httptest.Server
	ctl   *control.Server
	store *Store
	fleet *Fleet
}

func startNode(t *testing.T, tc *testCluster, i int, key []byte, leaseTTL time.Duration) *node {
	t.Helper()
	c, err := NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	store := New(tc.nodes[i].Client(), c, slog.New(slog.DiscardHandler))
	registry := upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, store.Resolve)
	t.Cleanup(registry.Close)
	store.Prepare = func(f *directory.File) error { _, _, err := registry.Apply(f.Clusters); return err }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	fleet := NewFleet(tc.nodes[i].Client(), leaseTTL)
	fleet.DropMargin = 500 * time.Millisecond
	ctl := &control.Server{Dir: store, Clusters: registry, Metrics: telemetry.NewMetrics(), Log: slog.New(slog.DiscardHandler),
		Keys: store, Fleet: fleet, ClusterSecrets: store.ClusterSecrets, FencePoll: 20 * time.Millisecond}
	api := httptest.NewServer(ctl.Handler())
	t.Cleanup(api.Close)
	return &node{t: t, api: api, ctl: ctl, store: store, fleet: fleet}
}

func (n *node) call(method, path string, body, out any) (int, string) {
	n.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, n.api.URL+path, &buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		n.t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw bytes.Buffer
	_, _ = raw.ReadFrom(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw.Bytes(), out); err != nil {
			n.t.Fatalf("%s %s: decoding %s: %v", method, path, raw.String(), err)
		}
	}
	return resp.StatusCode, raw.String()
}

func (n *node) must(method, path string, body, out any) {
	n.t.Helper()
	if code, raw := n.call(method, path, body, out); code != http.StatusOK {
		n.t.Fatalf("%s %s: HTTP %d %s", method, path, code, raw)
	}
}

func (n *node) refused(method, path string, body any, want string) {
	n.t.Helper()
	code, raw := n.call(method, path, body, nil)
	if code != http.StatusConflict || !strings.Contains(raw, want) {
		n.t.Fatalf("%s %s: want 409 mentioning %q, got HTTP %d %s", method, path, want, code, raw)
	}
}

// member is a simulated proxy: it heartbeats to a node with the version it says it has installed.
type member struct {
	id      string
	node    *node
	applied atomic.Int64
	follow  atomic.Bool // report the node's current version
	seq     atomic.Int64
	stop    chan struct{}
	reads   atomic.Int64 // fallback reads it reports for data01
}

func startMember(t *testing.T, n *node, id string, interval time.Duration) *member {
	t.Helper()
	m := &member{id: id, node: n, stop: make(chan struct{})}
	m.follow.Store(true)
	go func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			m.beat()
			select {
			case <-m.stop:
				return
			case <-tick.C:
			}
		}
	}()
	t.Cleanup(func() { m.pause() })
	m.beat()
	return m
}

func (m *member) beat() {
	if m.follow.Load() {
		m.applied.Store(m.node.store.Version())
	}
	hb := control.Heartbeat{Seq: m.seq.Add(1), Applied: m.applied.Load(), FallbackReads: map[string]float64{"acme/data01": float64(m.reads.Load())}}
	m.node.call(http.MethodPost, "/v1/fleet/"+m.id+"/heartbeat", hb, nil) //nolint:errcheck // a member that cannot reach the node just misses a beat
}

func (m *member) pause() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
}

func placement(t *testing.T, n *node) directory.Placement {
	t.Helper()
	p, ok := n.store.Snapshot().Lookup("acme", "data01")
	if !ok {
		t.Fatal("no acme/data01")
	}
	return *p
}

func TestControlAPIOnEtcdWithMembers(t *testing.T) {
	tc := startCluster(t, 2)
	key := make([]byte, 32)
	key[3] = 7
	a, b := startNode(t, tc, 0, key, time.Second), startNode(t, tc, 1, key, time.Second)
	src, dst := fakeS3(t), fakeS3(t)
	def := func(srv *httptest.Server, name string) config.Cluster {
		cw := true
		return config.Cluster{Type: "minio", Scheme: "http", Region: "us-east-1", Endpoints: []string{strings.TrimPrefix(srv.URL, "http://")},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:" + name}, Capabilities: config.Capabilities{ConditionalWrite: &cw}}
	}

	// Clusters added on node a, with secrets the control plane stores, are usable from node b.
	a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast01", Cluster: def(src, "vast01"), Secret: "s1"}, nil)
	b.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast02", Cluster: def(dst, "vast02"), Secret: "s2"}, nil)
	var st control.Status
	b.must("GET", "/v1/status", nil, &st)
	if len(st.Clusters) != 2 || st.Clusters[0].SecretRef != "control:vast01" {
		t.Fatalf("node b status: %+v", st.Clusters)
	}
	b.must("POST", "/v1/placements/acme/data01/adopt", control.AdoptRequest{Cluster: "vast01"}, nil)
	var ex control.ExpandResult
	a.must("POST", "/v1/placements/acme/data01/expand", control.ExpandRequest{To: "vast02", Create: true}, &ex)
	if !ex.CreatedBucket {
		t.Fatalf("expand: %+v", ex)
	}

	// GET /v1/directory delivers the directory with the cluster secrets and the client keys. Node
	// b's watch delivers node a's write a few milliseconds after a has it, as a member's long
	// poll would; the test waits for it rather than racing it.
	a.must("POST", "/v1/tenants/acme/client-keys", control.ClientKeyRequest{AccessKey: "CLIENT", Secret: "cs"}, nil)
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if err := b.store.WaitVersion(sctx, a.store.Version()); err != nil {
		t.Fatal(err)
	}
	var dir control.Directory
	b.must("GET", "/v1/directory?since=0", nil, &dir)
	if dir.Version != b.store.Version() || dir.Secrets["control:vast01"] != "s1" || len(dir.Credentials) != 1 || dir.Credentials[0].Secret != "cs" || len(dir.Placements) != 1 {
		t.Fatalf("directory payload: version %d secrets %v creds %+v placements %d", dir.Version, dir.Secrets, dir.Credentials, len(dir.Placements))
	}
	if code, _ := b.call("GET", fmt.Sprintf("/v1/directory?since=%d&wait=100ms", dir.Version), nil, nil); code != http.StatusNotModified {
		t.Fatalf("long-poll at the current version: %d", code)
	}

	// Two members, heartbeating to different nodes. A ramp step is held until both have it.
	p2, p3 := startMember(t, a, "p2", 50*time.Millisecond), startMember(t, b, "p3", 50*time.Millisecond)
	var tr control.TransitionResult
	tr = control.TransitionResult{}
	a.must("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 0.5}, &tr)
	if !tr.Held || tr.Proxies != 2 || len(tr.WaitingOn) != 0 || tr.Ratio != 0.5 {
		t.Fatalf("ramp with two members: %+v", tr)
	}
	if p := placement(t, b); p.Held() || p.Ramp.Ratio != 0.5 {
		t.Fatalf("node b after the step: %+v", p.Ramp)
	}

	// p3 stops installing: the next step's hold cannot reach it and is released, then the
	// precondition refuses until p3 catches up.
	p3.follow.Store(false)
	a.refused("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 0.8, Wait: "1s"}, "did not reach every proxy within 1s, waiting on p3")
	if p := placement(t, a); p.Held() || p.Ramp.Ratio != 0.5 {
		t.Fatalf("after a released hold: %+v", p.Ramp)
	}
	a.refused("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 0.8, Wait: "300ms"}, "is not on every proxy yet, waiting on p3")
	p3.follow.Store(true)
	tr = control.TransitionResult{}
	a.must("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 0.8}, &tr)
	if !tr.Held || tr.Proxies != 2 {
		t.Fatalf("after p3 caught up: %+v", tr)
	}

	// p3 goes silent: a later step goes ahead without it and says so.
	p3.pause()
	time.Sleep(2500 * time.Millisecond) // lease 1s + margin 500ms, and etcd's whole-second lease
	var fl control.FleetStatus
	a.must("GET", "/v1/fleet", nil, &fl)
	if len(fl.Members) != 2 || fl.Members[1].ID != "p3" || fl.Members[1].Live {
		t.Fatalf("fleet with p3 silent: %+v", fl.Members)
	}
	tr = control.TransitionResult{}
	b.must("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 1}, &tr)
	if len(tr.Silent) != 1 || tr.Silent[0] != "p3" || tr.Proxies != 1 {
		t.Fatalf("step with a silent member: %+v", tr)
	}

	// migrate start at ratio 1 moves no writes: no hold, even on a node whose copy of the directory
	// lags the step node b just wrote (fencedStep syncs first). Cutover counts the fleet's reads.
	tr = control.TransitionResult{}
	a.must("POST", "/v1/placements/acme/data01/migrate", control.MigrateRequest{}, &tr)
	if tr.Held || tr.To != directory.StateMigrating {
		t.Fatalf("migrate start: %+v", tr)
	}
	a.must("POST", "/v1/placements/acme/data01/mover-progress", control.Progress{Source: "vast01", Primary: "vast02", Pass: 1, Done: true, Converged: true}, nil)
	p2.reads.Store(3)
	time.Sleep(150 * time.Millisecond)
	go func() {
		time.Sleep(100 * time.Millisecond)
		p2.reads.Store(4) // a fallback read on p2 during the window
	}()
	a.refused("POST", "/v1/placements/acme/data01/cutover", control.CutoverRequest{Window: "400ms"}, "still fall back")
	time.Sleep(150 * time.Millisecond)
	tr = control.TransitionResult{}
	a.must("POST", "/v1/placements/acme/data01/cutover", control.CutoverRequest{Window: "300ms"}, &tr)
	if tr.To != directory.StateCutover || tr.Cutover == nil || tr.Cutover.FallbackReads != 4 {
		t.Fatalf("cutover: %+v %+v", tr, tr.Cutover)
	}

	// A bucket's first step waits for the silent member by name, until it is forgotten.
	b.must("POST", "/v1/placements/acme/logs/adopt", control.AdoptRequest{Cluster: "vast01"}, nil)
	b.must("POST", "/v1/placements/acme/logs/expand", control.ExpandRequest{To: "vast02", Name: "logs"}, nil)
	b.refused("POST", "/v1/placements/acme/logs/ramp", control.RampRequest{Ratio: 0.5, Wait: "300ms"}, "waiting on p3")
	b.refused("DELETE", "/v1/fleet/p2", nil, "is live")
	b.must("DELETE", "/v1/fleet/p3", nil, nil)
	tr = control.TransitionResult{}
	b.must("POST", "/v1/placements/acme/logs/ramp", control.RampRequest{Ratio: 0.5}, &tr)
	if !tr.Held || tr.Proxies != 1 {
		t.Fatalf("first step after forget: %+v", tr)
	}

	// The audit trail in etcd has one record per version.
	changes, err := tc.nodes[0].Client().Get(context.Background(), kChanges, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(changes.Kvs)) != a.store.Version() {
		t.Errorf("%d change records for version %d", len(changes.Kvs), a.store.Version())
	}
}
