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
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
)

// A join on real etcd (ADR-0021 D4, H3a): one control node takes a second through the join
// operation. The join answers with its learner on the record; a retry under the same
// Idempotency-Key answers the same record and adds nothing; the bootstrap starts the second member,
// which promotes itself, and the record ends succeeded with two voters and the two-voter warning.
func TestJoinOnEtcdFromOneNodeToTwo(t *testing.T) {
	tc := startCluster(t, 1)
	key := bytes.Repeat([]byte{0x33}, 32)
	n := startNode(t, tc, 0, key, time.Second)
	n.ctl.Members = &control.Membership{List: tc.nodes[0].ListMembers, AddLearner: tc.nodes[0].AddLearner, Key: func() []byte { return key }}
	api := &API{Node: tc.nodes[0], Store: n.store, Fleet: n.fleet, Control: n.ctl}
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	post := func(path, idem string, body, out any) (int, string) {
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
