package cp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"flag"
	"os"
	"path/filepath"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/blakegolliher/shunt/internal/admission"
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
	t      *testing.T
	api    *httptest.Server
	ctl    *control.Server
	store  *Store
	fleet  *Fleet
	ops    *Operations
	events *control.Events
	hookMu sync.Mutex
	hook   func(control.Operation) // a test's own OnChange, beside the events
}

// onChange sets a test's hook on the node's operation records, safely beside the running watch.
func (n *node) onChange(fn func(control.Operation)) {
	n.hookMu.Lock()
	n.hook = fn
	n.hookMu.Unlock()
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
	store.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, err := registry.Prepare(f.Clusters, resolve)
		if err != nil {
			return nil, err
		}
		return func() { cand.Commit() }, nil
	}
	events := control.NewEvents(256)
	store.OnInstall = events.Directory
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	n := &node{t: t, store: store, events: events}
	ops := NewOperations(tc.nodes[i].Client())
	ops.Node = fmt.Sprintf("c%d", i+1)
	ops.OnChange = func(op control.Operation) {
		events.Fence(op)
		n.hookMu.Lock()
		hook := n.hook
		n.hookMu.Unlock()
		if hook != nil {
			hook(op)
		}
	}
	if err := ops.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ops.Close)
	fleet := NewFleet(tc.nodes[i].Client(), leaseTTL)
	fleet.DropMargin = 500 * time.Millisecond
	ctl := &control.Server{Dir: store, Clusters: registry, Metrics: telemetry.NewMetrics(), Log: slog.New(slog.DiscardHandler),
		Keys: store, Fleet: fleet, ClusterSecrets: store.ClusterSecrets, FencePoll: 20 * time.Millisecond,
		Ops: ops, Node: ops.Node, Events: events, ConfirmKey: c.Derive("confirm")}
	api := httptest.NewServer(ctl.Handler())
	t.Cleanup(api.Close)
	n.api, n.ctl, n.fleet, n.ops = api, ctl, fleet, ops
	return n
}

func (n *node) call(method, path string, body, out any) (int, string) {
	n.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, n.api.URL+path, &buf)
	if method != http.MethodGet {
		req.Header.Set(control.HeaderIdempotencyKey, fmt.Sprintf("test-%d", nodeKeys.Add(1)))
	}
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

var nodeKeys atomic.Int64

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
	id          string
	incarnation string
	node        *node
	applied     atomic.Int64
	follow      atomic.Bool // report the node's current version
	seq         atomic.Int64
	stop        chan struct{}
	done        chan struct{}
	reads       atomic.Int64 // fallback reads it reports for data01
	noAck       atomic.Bool  // install versions but acknowledge no barrier: a drain that cannot finish
	gates       *admission.Gates
}

func startMember(t *testing.T, n *node, id string, interval time.Duration) *member {
	t.Helper()
	m := &member{id: id, node: n, stop: make(chan struct{}), done: make(chan struct{}), incarnation: fmt.Sprintf("%032x", time.Now().UnixNano()), gates: admission.New()}
	m.follow.Store(true)
	go func() {
		defer close(m.done)
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
	snap := m.node.store.Snapshot()
	if m.follow.Load() {
		closures, _ := admission.Barriers(snap)
		m.gates.Apply(closures, false)
		m.applied.Store(snap.Version())
	}
	acks := m.gates.Acks(snap)
	if m.noAck.Load() {
		acks = nil
	}
	hb := control.Heartbeat{Protocol: control.Protocol, Identity: m.node.store.Snapshot().File().Identity, Seq: m.seq.Add(1), Applied: m.applied.Load(),
		Durable: m.applied.Load(), Incarnation: m.incarnation, Barriers: acks, FallbackReads: map[string]float64{"acme/data01": float64(m.reads.Load())}}
	m.node.call(http.MethodPost, "/v1/fleet/"+m.id+"/heartbeat", hb, nil) //nolint:errcheck // a member that cannot reach the node just misses a beat
}

func (m *member) pause() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	<-m.done
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
	if dir.Identity.Validate() != nil || dir.Identity != a.store.Snapshot().File().Identity {
		t.Fatalf("directory payload identity %+v", dir.Identity)
	}
	lineage := "&cluster_id=" + dir.Identity.ClusterID + "&epoch=" + dir.Identity.Epoch
	if code, _ := b.call("GET", fmt.Sprintf("/v1/directory?since=%d&wait=100ms%s", dir.Version, lineage), nil, nil); code != http.StatusNotModified {
		t.Fatalf("long-poll at the current version: %d", code)
	}
	if code, _ := b.call("GET", fmt.Sprintf("/v1/directory?since=%d&wait=100ms", dir.Version), nil, nil); code != http.StatusBadRequest {
		t.Fatalf("long-poll with a version and no lineage: %d, want 400", code)
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

	// p3 stops installing: the next step stays blocked with its durable hold until it is
	// canceled. Cancellation releases only this operation's hold and restores the old ratio.
	p3.follow.Store(false)
	var blocked control.Operation
	if code, raw := a.call("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 0.8, Wait: "1s"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("blocked ramp: want 202, got %d %s", code, raw)
	}
	if blocked.Status != control.StatusBlocked || blocked.Phase != control.PhaseDrain || len(blocked.Blockers) != 1 || blocked.Blockers[0].Code != control.BlockerInstallPending || blocked.Blockers[0].ProxyID != "p3" {
		t.Fatalf("blocked ramp record: %+v", blocked)
	}
	if p := placement(t, a); !p.Held() || p.Ramp.Ratio != 0.5 || p.Barrier == nil || p.Barrier.ID != blocked.ID {
		t.Fatalf("durable hold: %+v", p)
	}
	var blockers control.BlockerPage
	a.must("GET", "/v1/operations/"+blocked.ID+"/blockers", nil, &blockers)
	if !blockers.Complete || len(blockers.Blockers) != 1 || blockers.Blockers[0].ProxyID != "p3" {
		t.Fatalf("blocker page: %+v", blockers)
	}
	var canceled control.Operation
	a.must("POST", "/v1/operations/"+blocked.ID+"/cancel", struct{}{}, &canceled)
	if canceled.Status != control.StatusCancelled || canceled.EffectState != control.EffectNone {
		t.Fatalf("canceled ramp: %+v", canceled)
	}
	if p := placement(t, a); p.Held() || p.Ramp.Ratio != 0.5 {
		t.Fatalf("after cancellation: %+v", p.Ramp)
	}
	p3.follow.Store(true)
	tr = control.TransitionResult{}
	a.must("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 0.8}, &tr)
	if !tr.Held || tr.Proxies != 2 {
		t.Fatalf("after p3 caught up: %+v", tr)
	}

	// p3 goes silent: a later step remains blocked because silence proves neither drain nor
	// retirement. Cancel it, resolve the stopped incarnation, then forget the member.
	p3.pause()
	time.Sleep(2500 * time.Millisecond) // lease 1s + margin 500ms, and etcd's whole-second lease
	var fl control.FleetStatus
	a.must("GET", "/v1/fleet", nil, &fl)
	if len(fl.Members) != 2 || fl.Members[1].ID != "p3" || fl.Members[1].Live {
		t.Fatalf("fleet with p3 silent: %+v", fl.Members)
	}
	blocked = control.Operation{}
	if code, raw := b.call("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 1, Wait: "300ms"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("ramp with a silent member: want 202, got %d %s", code, raw)
	}
	if blocked.Status != control.StatusBlocked || len(blocked.Blockers) != 1 || blocked.Blockers[0].Code != control.BlockerProxyMissing || blocked.Blockers[0].ProxyID != "p3" {
		t.Fatalf("silent member blockers: %+v", blocked)
	}
	// Blocked in its precondition, the step has written nothing: it can be canceled (defect 6 of
	// the H2 review), which frees the bucket for the next operation.
	if blocked.Barrier != nil || !slices.Equal(blocked.AllowedActions, []string{control.ActionCancel}) {
		t.Fatalf("a step blocked in its precondition: %+v", blocked)
	}
	var preCanceled control.Operation
	b.must("POST", "/v1/operations/"+blocked.ID+"/cancel", struct{}{}, &preCanceled)
	if preCanceled.Status != control.StatusCancelled || preCanceled.EffectState != control.EffectNone {
		t.Fatalf("cancel in the precondition: %+v", preCanceled)
	}
	if p := placement(t, b); p.Held() || p.Ramp == nil || p.Ramp.Ratio != 0.8 {
		t.Fatalf("a step canceled in its precondition changed the placement: %+v", p)
	}
	b.refused("DELETE", "/v1/fleet/p2", nil, "is live")
	b.refused("DELETE", "/v1/fleet/p3", nil, "did not retire cleanly")
	b.must("POST", "/v1/fleet/p3/resolve", control.ResolveRequest{Incarnation: p3.incarnation, Attestation: "test host stopped; no backend work in flight"}, nil)
	b.must("DELETE", "/v1/fleet/p3", nil, nil)
	var next control.Operation
	if code, raw := b.call("POST", "/v1/operations", control.OperationRequest{Kind: control.OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":1,"wait":"300ms"}`)}, &next); code != http.StatusAccepted {
		t.Fatalf("a step after the canceled one: %d %s", code, raw)
	}
	var completed *control.Operation
	waitFor(t, 5*time.Second, "the next step did not complete after p3 was forgotten", func() bool {
		completed, _ = b.ops.Get(context.Background(), next.ID)
		return completed != nil && completed.Terminal()
	})
	if completed.Status != control.StatusSucceeded || json.Unmarshal(completed.Result, &tr) != nil {
		t.Fatalf("step after resolving the silent member: %+v", completed)
	}
	if tr.Ratio != 1 || tr.Proxies != 1 {
		t.Fatalf("step after resolving the silent member: %+v", tr)
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
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			ops, err := a.ops.List(context.Background(), "acme/data01", "", 10)
			if err == nil {
				for _, op := range ops {
					if op.Kind == control.OpCutover && op.Phase == control.PhaseWindow {
						p2.reads.Store(4) // a fallback read on p2 during the incarnation-bound window
						return
					}
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	// The window runs once every proxy has drained the hold (the fleet's blockers come first), so
	// the record is followed until the window's verdict is on it rather than timed by the wait.
	var cutoverBlocked control.Operation
	code, raw := a.call("POST", "/v1/placements/acme/data01/cutover", control.CutoverRequest{Window: "400ms", Wait: "0s"}, &cutoverBlocked)
	if code != http.StatusAccepted {
		t.Fatalf("cutover: HTTP %d %s", code, raw)
	}
	waitFor(t, 5*time.Second, "the cutover window's fallback blocker", func() bool {
		op, _ := a.ops.Get(context.Background(), cutoverBlocked.ID)
		if op == nil || op.Terminal() {
			t.Fatalf("cutover ended without the fallback blocker: %+v", op)
		}
		cutoverBlocked = *op
		return op.Status == control.StatusBlocked && slices.ContainsFunc(op.Blockers, func(b control.Blocker) bool { return b.Code == control.BlockerOldRequests })
	})
	var cutoverCanceled control.Operation
	a.must("POST", "/v1/operations/"+cutoverBlocked.ID+"/cancel", struct{}{}, &cutoverCanceled)
	if cutoverCanceled.Status != control.StatusCancelled {
		t.Fatalf("cancel cutover: %+v", cutoverCanceled)
	}
	time.Sleep(150 * time.Millisecond)
	tr = control.TransitionResult{}
	a.must("POST", "/v1/placements/acme/data01/cutover", control.CutoverRequest{Window: "300ms"}, &tr)
	if tr.To != directory.StateCutover || tr.Cutover == nil || tr.Cutover.FallbackReads != 4 {
		t.Fatalf("cutover: %+v %+v", tr, tr.Cutover)
	}

	// A bucket's first step now counts only the remaining member.
	b.must("POST", "/v1/placements/acme/logs/adopt", control.AdoptRequest{Cluster: "vast01"}, nil)
	b.must("POST", "/v1/placements/acme/logs/expand", control.ExpandRequest{To: "vast02", Name: "logs"}, nil)
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

// An operation started on one node is followed from another: its record, its fence events, and
// its outcome (ADR-0017). Then the records a node leaves running when it stops, and the audit
// trail read back through the store.
func TestOperationsAcrossNodes(t *testing.T) {
	tc := startCluster(t, 2)
	key := make([]byte, 32)
	key[5] = 9
	a, b := startNode(t, tc, 0, key, time.Second), startNode(t, tc, 1, key, time.Second)
	src, dst := fakeS3(t), fakeS3(t)
	def := func(srv *httptest.Server, name string) config.Cluster {
		cw := true
		return config.Cluster{Type: "minio", Scheme: "http", Region: "us-east-1", Endpoints: []string{strings.TrimPrefix(srv.URL, "http://")},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:" + name}, Capabilities: config.Capabilities{ConditionalWrite: &cw}}
	}
	a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast01", Cluster: def(src, "vast01"), Secret: "s1"}, nil)
	a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast02", Cluster: def(dst, "vast02"), Secret: "s2"}, nil)
	a.must("POST", "/v1/placements/acme/data01/adopt", control.AdoptRequest{Cluster: "vast01"}, nil)
	a.must("POST", "/v1/placements/acme/data01/expand", control.ExpandRequest{To: "vast02", Create: true}, nil)
	p2 := startMember(t, a, "p2", 50*time.Millisecond)

	// A step through its own route runs under a record that the other node reads at once.
	var tr control.TransitionResult
	a.must("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 0.5}, &tr)
	if tr.Operation == "" || !tr.Held {
		t.Fatalf("ramp through its route: %+v", tr)
	}
	var rec control.Operation
	b.must("GET", "/v1/operations/"+tr.Operation, nil, &rec)
	if rec.Status != control.StatusSucceeded || rec.Kind != control.OpRamp || rec.Node != "c1" || rec.Version != tr.Version {
		t.Fatalf("the record on node b: %+v", rec)
	}

	// p2 stops installing: an operation started on a waits on it, which b sees on the record and
	// as fence events.
	_, fence, _, ok, cancel := b.events.Subscribe("")
	if !ok {
		t.Fatal("subscribe on b")
	}
	defer cancel()
	var (
		trailMu sync.Mutex
		trail   []string
	)
	a.onChange(func(op control.Operation) {
		trailMu.Lock()
		trail = append(trail, fmt.Sprintf("%s phase=%s status=%s waiting=%v updated=%s", op.ID, op.Phase, op.Status, op.WaitingOn, op.Updated.Format("05.000")))
		trailMu.Unlock()
	})
	p2.follow.Store(false)
	var started control.Operation
	if code, raw := a.call("POST", "/v1/operations", control.OperationRequest{Kind: control.OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":0.8,"wait":"8s"}`)}, &started); code != http.StatusAccepted {
		t.Fatalf("POST /v1/operations: %d %s", code, raw)
	}
	if started.ID == "" || started.Terminal() {
		t.Fatalf("started: %+v", started)
	}
	waitFor(t, 5*time.Second, "node b did not see the hold waiting on p2", func() bool {
		var op control.Operation
		if code, _ := b.call("GET", "/v1/operations/"+started.ID, nil, &op); code != http.StatusOK {
			return false
		}
		return op.Status == control.StatusBlocked && op.Phase == control.PhaseDrain && len(op.Blockers) == 1 && op.Blockers[0].ProxyID == "p2"
	})
	deadline := time.After(5 * time.Second)
	sawHold := false
	for !sawHold {
		select {
		case ev := <-fence:
			var fe control.FenceEvent
			if ev.Type == "fence" && json.Unmarshal(ev.Data, &fe) == nil && fe.Operation == started.ID && fe.Status == control.StatusBlocked && fe.Phase == control.PhaseDrain {
				sawHold = true
			}
		case <-deadline:
			t.Fatal("node b published no fence event for the operation node a runs")
		}
	}
	p2.follow.Store(true)
	var done control.Operation
	waitFor(t, 8*time.Second, "the operation did not succeed once p2 caught up", func() bool {
		done = control.Operation{} // a fresh decode each poll: omitted fields would otherwise keep an earlier poll's values
		if code, _ := a.call("GET", "/v1/operations/"+started.ID, nil, &done); code != http.StatusOK {
			return false
		}
		return done.Status == control.StatusSucceeded
	})
	var res control.TransitionResult
	if err := json.Unmarshal(done.Result, &res); err != nil || !res.Held || res.Ratio != 0.8 || done.Version == 0 || done.Version != res.Version || len(done.WaitingOn) != 0 {
		trailMu.Lock()
		defer trailMu.Unlock()
		t.Fatalf("the finished record: %+v result %+v (%v)\nrecord trail on a:\n%s", done, res, err, strings.Join(trail, "\n"))
	}
	var list control.OperationList
	b.must("GET", "/v1/operations?placement=acme/data01", nil, &list)
	if len(list.Operations) < 2 || list.Operations[0].ID != started.ID {
		t.Fatalf("listing on b: %+v", list.Operations)
	}

	// A durable barrier whose owner is gone stays blocked and resumable instead of being failed.
	orphan := &control.Operation{ID: "0000000000001-abcdef", Kind: control.OpRamp, Placement: "acme/data01", Node: "ghost", Status: control.StatusRunning, Phase: control.PhaseDrain, Sequence: 1,
		Barrier: &control.BarrierState{ID: "0000000000001-abcdef", Scope: directory.PlacementResource("acme/data01"), Kind: config.BarrierMutations, HoldVersion: a.store.Version()}}
	if err := a.ops.Create(context.Background(), orphan); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "node b did not index the orphan operation", func() bool {
		ops, err := b.ops.List(context.Background(), "", "", 100)
		return err == nil && slices.ContainsFunc(ops, func(op *control.Operation) bool { return op.ID == orphan.ID })
	})
	if err := b.ctl.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	var failed control.Operation
	a.must("GET", "/v1/operations/"+orphan.ID, nil, &failed)
	if failed.Status != control.StatusBlocked || len(failed.Blockers) != 1 || failed.Blockers[0].Code != control.BlockerOwnerLost || !slices.Contains(failed.AllowedActions, control.ActionResume) {
		t.Fatalf("the orphan: %+v", failed)
	}

	// The audit trail, newest first, from either node.
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if err := b.store.WaitVersion(sctx, a.store.Version()); err != nil {
		t.Fatal(err)
	}
	changes, err := b.store.Changes(context.Background(), 0, 3)
	if err != nil || len(changes) != 3 || changes[0].Version != a.store.Version() || changes[1].Version != changes[0].Version-1 || changes[0].Actor == "" {
		t.Fatalf("changes: %+v %v", changes, err)
	}
	var page control.AuditPage
	a.must("GET", "/v1/audit?limit=2", nil, &page)
	if len(page.Changes) != 2 || page.Changes[0].Version != a.store.Version() {
		t.Fatalf("audit page: %+v", page.Changes)
	}
	older, err := b.store.Changes(context.Background(), changes[2].Version, 2)
	if err != nil || len(older) != 2 || older[0].Version != changes[2].Version {
		t.Fatalf("paging the changes: %+v %v", older, err)
	}
}

// T09. A barrier record outlives its control-node owner. Another node marks the lost owner,
// resumes the same operation and commits it once the proxy drain proof arrives; a cancellation
// after that commit is refused.
func TestBarrierOwnerLossResumeOnEtcd(t *testing.T) {
	tc := startCluster(t, 2)
	key := make([]byte, 32)
	key[7] = 11
	a, b := startNode(t, tc, 0, key, time.Second), startNode(t, tc, 1, key, time.Second)
	ownerCtx, stopOwner := context.WithCancel(context.Background())
	a.ctl.Ctx = ownerCtx
	src := fakeS3(t)
	cw := true
	def := config.Cluster{Type: "minio", Scheme: "http", Region: "us-east-1", Endpoints: []string{strings.TrimPrefix(src.URL, "http://")},
		Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:vast01"}, Capabilities: config.Capabilities{ConditionalWrite: &cw}}
	a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast01", Cluster: def, Secret: "s1"}, nil)
	a.must("POST", "/v1/placements/acme/data01/adopt", control.AdoptRequest{Cluster: "vast01"}, nil)
	p2 := startMember(t, a, "p2", 20*time.Millisecond)
	p2.follow.Store(false)

	var started control.Operation
	req := control.OperationRequest{Kind: control.OpClusterReadOnly, Cluster: "vast01", Args: json.RawMessage(`{"read_only":true,"wait":"0s"}`)}
	if code, raw := a.call("POST", "/v1/operations", req, &started); code != http.StatusAccepted {
		t.Fatalf("starting read-only: %d %s", code, raw)
	}
	waitFor(t, 5*time.Second, "read-only did not block on p2", func() bool {
		op, _ := a.ops.Get(context.Background(), started.ID)
		return op != nil && op.Status == control.StatusBlocked && op.Barrier != nil && len(op.Blockers) == 1 && op.Blockers[0].ProxyID == "p2"
	})

	var ownerChanged bool
	for range 20 {
		owned, err := a.ops.Get(context.Background(), started.ID)
		if err != nil || owned == nil {
			t.Fatalf("reading owned operation: %+v %v", owned, err)
		}
		owned.Node = "ghost"
		owned.Sequence++
		err = a.ops.Update(context.Background(), owned)
		if err == nil {
			ownerChanged = true
			break
		}
		if !errors.Is(err, control.ErrStaleSequence) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !ownerChanged {
		t.Fatal("the running operation did not yield its record for the owner-loss simulation")
	}
	stopOwner()
	waitFor(t, 5*time.Second, "node b did not index the owner change", func() bool {
		ops, err := b.ops.List(context.Background(), "", "", 100)
		return err == nil && slices.ContainsFunc(ops, func(op *control.Operation) bool { return op.ID == started.ID && op.Node == "ghost" })
	})
	if err := b.ctl.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the lost owner was not recorded", func() bool {
		op, _ := b.ops.Get(context.Background(), started.ID)
		return op != nil && op.Status == control.StatusBlocked && len(op.Blockers) == 1 && op.Blockers[0].Code == control.BlockerOwnerLost && slices.Contains(op.AllowedActions, control.ActionResume)
	})

	p2.follow.Store(true)
	var resumed control.Operation
	if code, raw := b.call("POST", "/v1/operations/"+started.ID+"/resume", struct{}{}, &resumed); code != http.StatusAccepted {
		t.Fatalf("resuming on node b: %d %s", code, raw)
	}
	var done *control.Operation
	waitFor(t, 8*time.Second, "the resumed barrier did not finish", func() bool {
		done, _ = b.ops.Get(context.Background(), started.ID)
		return done != nil && done.Terminal()
	})
	if done.Status != control.StatusSucceeded || done.Node != "c2" || done.OwnerTerm != 1 || done.Barrier == nil || !done.Barrier.Committed {
		t.Fatalf("resumed barrier: %+v", done)
	}
	cl := b.store.Snapshot().File().Clusters["vast01"]
	if !cl.ReadOnly || cl.Barrier != nil {
		t.Fatalf("read-only commit: %+v", cl)
	}
	code, raw := b.call("POST", "/v1/operations/"+started.ID+"/cancel", struct{}{}, nil)
	var apiErr control.Error
	_ = json.Unmarshal([]byte(raw), &apiErr)
	if code != http.StatusConflict || apiErr.Code != control.CodeNotCancellable {
		t.Fatalf("cancel after commit: %d %s", code, raw)
	}
}

// Defect 1 of the H2 review on etcd, for every barrier kind whose commit clears its barrier: the
// owner's commit reached the directory, the owner was lost before its record said Committed, and
// a resume on another control node must find the commit rather than drain forever for an ack no
// proxy can send.
func TestResumeAfterLostCommitRecordOnEtcd(t *testing.T) {
	type kind struct {
		name   string
		req    control.OperationRequest
		commit func(t *testing.T, n *node, id string)
		check  func(t *testing.T, n *node)
	}
	step := func(tr directory.Transition) func(t *testing.T, n *node, id string) {
		return func(t *testing.T, n *node, id string) {
			tr.Complete, tr.Barrier = true, id
			if err := n.store.SetState(context.Background(), "acme", "data01", placement(t, n).State, tr, "owner"); err != nil {
				t.Fatalf("the owner's commit: %v", err)
			}
		}
	}
	clear := func(scope string) func(t *testing.T, n *node, id string) {
		return func(t *testing.T, n *node, id string) {
			if err := n.store.ClearBarrier(context.Background(), scope, id, "owner"); err != nil {
				t.Fatalf("the owner's commit: %v", err)
			}
		}
	}
	kinds := []kind{
		{"ramp", control.OperationRequest{Kind: control.OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":0.5,"wait":"0s"}`)},
			step(directory.Transition{To: directory.StateRamping, Ratio: 0.5}),
			func(t *testing.T, n *node) {
				if p := placement(t, n); p.Held() || p.Barrier != nil || p.Ramp == nil || p.Ramp.Ratio != 0.5 {
					t.Fatalf("ramp: %+v", p)
				}
			}},
		{"migrate", control.OperationRequest{Kind: control.OpMigrate, Placement: "acme/data01", Args: json.RawMessage(`{"wait":"0s"}`)},
			step(directory.Transition{To: directory.StateMigrating}),
			func(t *testing.T, n *node) {
				if p := placement(t, n); p.State != directory.StateMigrating || p.Barrier != nil {
					t.Fatalf("migrate: %+v", p)
				}
			}},
		{"placement-read-only", control.OperationRequest{Kind: control.OpPlacementReadOnly, Placement: "acme/data01", Args: json.RawMessage(`{"read_only":true,"wait":"0s"}`)},
			clear(directory.PlacementResource("acme/data01")),
			func(t *testing.T, n *node) {
				if p := placement(t, n); !p.ReadOnly || p.Barrier != nil {
					t.Fatalf("placement read-only: %+v", p)
				}
			}},
		{"cluster-read-only", control.OperationRequest{Kind: control.OpClusterReadOnly, Cluster: "vast01", Args: json.RawMessage(`{"read_only":true,"wait":"0s"}`)},
			clear(directory.ClusterResource("vast01")),
			func(t *testing.T, n *node) {
				if c := n.store.Snapshot().File().Clusters["vast01"]; !c.ReadOnly || c.Barrier != nil {
					t.Fatalf("cluster read-only: %+v", c)
				}
			}},
	}
	for _, k := range kinds {
		t.Run(k.name, func(t *testing.T) {
			tc := startCluster(t, 2)
			key := make([]byte, 32)
			key[5] = 13
			a, b := startNode(t, tc, 0, key, time.Second), startNode(t, tc, 1, key, time.Second)
			ownerCtx, stopOwner := context.WithCancel(context.Background())
			a.ctl.Ctx = ownerCtx
			src, dst := fakeS3(t), fakeS3(t)
			cw := true
			def := func(srv *httptest.Server, name string) config.Cluster {
				return config.Cluster{Type: "minio", Scheme: "http", Region: "us-east-1", Endpoints: []string{strings.TrimPrefix(srv.URL, "http://")},
					Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:" + name}, Capabilities: config.Capabilities{ConditionalWrite: &cw}}
			}
			a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast01", Cluster: def(src, "vast01"), Secret: "s1"}, nil)
			a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast02", Cluster: def(dst, "vast02"), Secret: "s2"}, nil)
			a.must("POST", "/v1/placements/acme/data01/adopt", control.AdoptRequest{Cluster: "vast01"}, nil)
			a.must("POST", "/v1/placements/acme/data01/expand", control.ExpandRequest{To: "vast02", Create: true}, nil)
			p2 := startMember(t, a, "p2", 20*time.Millisecond)
			p2.noAck.Store(true)

			var started control.Operation
			if code, raw := a.call("POST", "/v1/operations", k.req, &started); code != http.StatusAccepted {
				t.Fatalf("starting %s: %d %s", k.name, code, raw)
			}
			waitFor(t, 5*time.Second, k.name+" did not block in its drain", func() bool {
				op, _ := a.ops.Get(context.Background(), started.ID)
				return op != nil && op.Status == control.StatusBlocked && op.Barrier != nil && op.Barrier.HoldVersion > 0 && op.Phase == control.PhaseDrain
			})
			var lost bool
			for range 20 {
				owned, err := a.ops.Get(context.Background(), started.ID)
				if err != nil || owned == nil {
					t.Fatalf("reading owned operation: %+v %v", owned, err)
				}
				owned.Node = "ghost"
				owned.Sequence++
				if err = a.ops.Update(context.Background(), owned); err == nil {
					lost = true
					break
				}
				if !errors.Is(err, control.ErrStaleSequence) {
					t.Fatal(err)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !lost {
				t.Fatal("the running operation did not yield its record")
			}
			stopOwner()
			k.commit(t, a, started.ID)
			waitFor(t, 5*time.Second, "node b did not index the owner change", func() bool {
				op, err := b.ops.Get(context.Background(), started.ID)
				return err == nil && op != nil && op.Node == "ghost"
			})
			if err := b.ctl.Sweep(context.Background()); err != nil {
				t.Fatal(err)
			}
			var resumed control.Operation
			if code, raw := b.call("POST", "/v1/operations/"+started.ID+"/resume", struct{}{}, &resumed); code != http.StatusAccepted {
				t.Fatalf("resuming on node b: %d %s", code, raw)
			}
			var done *control.Operation
			waitFor(t, 8*time.Second, "the resumed "+k.name+" did not finish", func() bool {
				done, _ = b.ops.Get(context.Background(), started.ID)
				return done != nil && done.Terminal()
			})
			if done.Status != control.StatusSucceeded || done.Node != "c2" || done.Barrier == nil || !done.Barrier.Committed {
				t.Fatalf("resumed %s: %+v", k.name, done)
			}
			if err := b.store.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			k.check(t, b)
		})
	}
}

var updateRoutes = flag.Bool("update", false, "rewrite docs/reference/control-routes.json from the route tables")

// The route table the web UI's parity test reads is the code's (ADR-0017): control.Routes and
// this package's, kept equal to docs/reference/control-routes.json.
func TestRouteTable(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "reference", "control-routes.json")
	routes := append(control.Routes(), Routes()...)
	want, err := json.MarshalIndent(routes, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')
	if *updateRoutes {
		if err := os.WriteFile(path, want, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v; run go test ./internal/cp -run TestRouteTable -update", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s does not match the route tables; run go test ./internal/cp -run TestRouteTable -update and commit it", path)
	}
	seen := map[string]bool{}
	for _, r := range routes {
		k := r.Method + " " + r.Pattern
		if seen[k] {
			t.Errorf("route %s is listed twice", k)
		}
		seen[k] = true
		if (r.Method == "POST" || r.Method == "DELETE") != r.Mutation {
			t.Errorf("route %s: mutation %v does not match its method", k, r.Mutation)
		}
	}
}

// A credential rotation on the control plane starts a secret generation: the version it landed in,
// which the cluster view reports, and the control plane holds the new secret. The cluster's own
// generation moves with it (H0a: its credentials are part of the cluster an operation expects).
func TestRotateCredentialsOnEtcd(t *testing.T) {
	tc := startCluster(t, 1)
	a := startNode(t, tc, 0, make([]byte, 32), time.Second)
	src := fakeS3(t)
	def := config.Cluster{Type: "minio", Scheme: "http", Region: "us-east-1", Endpoints: []string{strings.TrimPrefix(src.URL, "http://")},
		Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:vast01"}}
	a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast01", Cluster: def, Secret: "s1"}, nil)

	var out control.CredentialsResult
	a.must("POST", "/v1/clusters/vast01/credentials", control.CredentialsRequest{Secret: "s2"}, &out)
	if out.Generation != strconv.FormatInt(out.Version, 10) || out.Cluster.SecretRef != "control:vast01" {
		t.Fatalf("rotation: %+v", out)
	}
	waitFor(t, 5*time.Second, "the rotation installed on the node", func() bool { return a.store.Snapshot().Version() >= out.Version })
	if s := a.store.ClusterSecrets()["control:vast01"]; s != "s2" {
		t.Fatalf("the control plane holds %q, want s2", s)
	}
	if g := a.store.Snapshot().File().Generation(directory.ClusterResource("vast01")); g != out.Version {
		t.Fatalf("cluster generation %d after the rotation, want %d", g, out.Version)
	}
	var view control.ClusterView
	a.must("GET", "/v1/clusters/vast01/view", nil, &view)
	if view.Secret == nil || view.Secret.Generation != out.Generation {
		t.Fatalf("cluster view secret: %+v, want generation %s", view.Secret, out.Generation)
	}
}
