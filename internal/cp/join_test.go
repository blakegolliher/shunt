package cp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
)

// A join on real etcd (ADR-0021 D4, H3a): one control node takes a second through the join
// operation. The join answers with its learner on the record; a retry under the same
// Idempotency-Key answers the same record and adds nothing; the bootstrap starts the second member,
// which the join promotes once it has caught up, and the record ends succeeded with two voters and
// the two-voter warning.
func TestJoinOnEtcdFromOneNodeToTwo(t *testing.T) {
	tc := startCluster(t, 1)
	key := bytes.Repeat([]byte{0x33}, 32)
	n, post := joinAPI(t, tc, key, tc.nodes[0].Promote)

	port := freePort(t)
	peer := fmt.Sprintf("http://127.0.0.1:%d", port)
	req := control.JoinRequest{Name: "c2", PeerURL: peer, APIURL: "http://127.0.0.1:9952"}
	var op, again control.Operation
	if code, raw := post("/v1/control/members", "join-etcd", req, &op); code != http.StatusAccepted || op.Member == nil {
		t.Fatalf("join: %d %s", code, raw)
	}
	if code, raw := post("/v1/control/members", "join-etcd", req, &again); code != http.StatusAccepted || again.ID != op.ID {
		t.Fatalf("a retried join: %d %s", code, raw)
	}
	ms, err := tc.nodes[0].ListMembers(context.Background())
	if err != nil || len(ms) != 2 {
		t.Fatalf("members after a join and its retry: %+v %v (want 2)", ms, err)
	}

	var b control.Bootstrap
	if code, raw := post("/v1/control/joins/"+op.ID+"/bootstrap", "", control.BootstrapRequest{PeerURL: peer}, &b); code != http.StatusOK {
		t.Fatalf("bootstrap: %d %s", code, raw)
	}
	if !bytes.Equal(b.EncryptionKey, key) || b.MemberID != op.Member.ID {
		t.Fatalf("bootstrap: %+v", b)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir := filepath.Join(t.TempDir(), "c2")
	second, err := Start(ctx, NodeConfig{Name: "c2", DataDir: dir, PeerURL: peer, InitialCluster: b.InitialCluster, Existing: true, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	tc.nodes, tc.dirs, tc.ports = append(tc.nodes, second), append(tc.dirs, dir), append(tc.ports, port)

	deadline := time.Now().Add(30 * time.Second)
	var done *control.Operation
	for {
		done, err = n.ops.Get(context.Background(), op.ID)
		if err != nil {
			t.Fatal(err)
		}
		if done != nil && done.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the join never ended: %+v", done)
		}
		time.Sleep(50 * time.Millisecond)
	}
	var res control.JoinResult
	if err := json.Unmarshal(done.Result, &res); err != nil || done.Status != control.StatusSucceeded || res.Voters != 2 || res.Warning == "" {
		t.Fatalf("the join's end: %+v %+v %v", done, res, err)
	}
	ms, err = tc.nodes[0].ListMembers(context.Background())
	if err != nil || len(ms) != 2 || ms[0].Learner || ms[1].Learner {
		t.Fatalf("members after the join: %+v %v", ms, err)
	}
}

// joinAPI serves node 0's control API with its membership, promoting through promote, and returns
// the node and a POST helper.
func joinAPI(t *testing.T, tc *testCluster, key []byte, promote func(context.Context, uint64) error) (*node, func(path, idem string, body, out any) (int, string)) {
	t.Helper()
	n := startNode(t, tc, 0, key, time.Second)
	n.ctl.Members = &control.Membership{List: tc.nodes[0].ListMembers, AddLearner: tc.nodes[0].AddLearner, Promote: promote, Remove: tc.nodes[0].RemoveMember, Key: func() []byte { return key }}
	api := &API{Node: tc.nodes[0], Store: n.store, Fleet: n.fleet, Control: n.ctl}
	mux := http.NewServeMux() // as shunt-control mounts them: its own routes, and the control API
	mux.Handle("/v1/control", api.Handler())
	mux.Handle("/v1/control/", api.Handler())
	mux.Handle("/v1/", n.ctl.Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return n, func(path, idem string, body, out any) (int, string) {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(raw))
		if idem != "" {
			req.Header.Set(control.HeaderIdempotencyKey, idem)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var b bytes.Buffer
		_, _ = b.ReadFrom(resp.Body)
		if out != nil && resp.StatusCode < 300 {
			if err := json.Unmarshal(b.Bytes(), out); err != nil {
				t.Fatalf("%s: %v %s", path, err, b.String())
			}
		}
		return resp.StatusCode, b.String()
	}
}

// A join canceled on real etcd (ADR-0021 D4, H3b, T12): the second node starts from its bootstrap
// and runs as a learner, which the join does not promote while it is held back (as a lagging learner
// is). The cancellation removes that learner by ID; the first node is the only member again and stays
// writable. A second join, canceled before its node starts, removes its unstarted learner the same
// way.
func TestJoinOnEtcdCanceledBeforePromotion(t *testing.T) {
	tc := startCluster(t, 1)
	key := bytes.Repeat([]byte{0x44}, 32)
	var hold atomic.Bool
	hold.Store(true)
	_, post := joinAPI(t, tc, key, func(ctx context.Context, id uint64) error {
		if hold.Load() {
			return control.ErrLearnerNotReady
		}
		return tc.nodes[0].Promote(ctx, id)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	port := freePort(t)
	peer := fmt.Sprintf("http://127.0.0.1:%d", port)
	var op control.Operation
	if code, raw := post("/v1/control/members", "join-cancel", control.JoinRequest{Name: "c2", PeerURL: peer}, &op); code != http.StatusAccepted || op.Member == nil {
		t.Fatalf("join: %d %s", code, raw)
	}
	var b control.Bootstrap
	if code, raw := post("/v1/control/joins/"+op.ID+"/bootstrap", "", control.BootstrapRequest{PeerURL: peer}, &b); code != http.StatusOK {
		t.Fatalf("bootstrap: %d %s", code, raw)
	}
	second, err := Start(ctx, NodeConfig{Name: "c2", DataDir: filepath.Join(t.TempDir(), "c2"), PeerURL: peer, InitialCluster: b.InitialCluster, Existing: true, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(second.Close)
	for {
		ms, err := tc.nodes[0].ListMembers(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(ms) == 2 && ms[0].Name != "" && ms[1].Name != "" {
			if !ms[0].Learner && !ms[1].Learner {
				t.Fatal("the joining node was promoted while its join held it back: it promoted itself")
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	var ended control.Operation
	code, raw := post("/v1/operations/"+op.ID+"/cancel", "", struct{}{}, &ended)
	if code != http.StatusOK || ended.Status != control.StatusCancelled || ended.EffectState != control.EffectNone {
		t.Fatalf("cancel of a started learner: %d %s", code, raw)
	}
	ms, err := tc.nodes[0].ListMembers(ctx)
	if err != nil || len(ms) != 1 || ms[0].Name != "c1" {
		t.Fatalf("members after the cancel: %+v %v", ms, err)
	}
	if _, err := tc.nodes[0].Client().Put(ctx, "/test/after-cancel", "ok"); err != nil {
		t.Fatalf("the first node after the cancel: %v", err)
	}

	peer3 := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	if code, raw := post("/v1/control/members", "join-cancel-2", control.JoinRequest{Name: "c3", PeerURL: peer3}, &op); code != http.StatusAccepted || op.Member == nil {
		t.Fatalf("second join: %d %s", code, raw)
	}
	if code, raw := post("/v1/operations/"+op.ID+"/cancel", "", struct{}{}, &ended); code != http.StatusOK || ended.Status != control.StatusCancelled {
		t.Fatalf("cancel of an unstarted learner: %d %s", code, raw)
	}
	if ms, err := tc.nodes[0].ListMembers(ctx); err != nil || len(ms) != 1 {
		t.Fatalf("members after the second cancel: %+v %v", ms, err)
	}
}
