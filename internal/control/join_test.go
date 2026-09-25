package control

// Joining a control node as an operation (ADR-0021 D4, H3a): T12's lost add response, resume after
// owner loss, and the bootstrap's handling of the data-encryption key, over a fake membership that
// answers as etcd does.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeMembers is an etcd membership: IDs from 0x100 up, a learner until promoted, and a peer URL at
// most once (etcd answers "Peer URLs already exists").
type fakeMembers struct {
	mu         sync.Mutex
	ms         []EtcdMember
	next       uint64
	adds       int
	loseAnswer bool // the add lands, and its answer is lost
}

func newFakeMembers() *fakeMembers {
	return &fakeMembers{ms: []EtcdMember{{ID: 0x1, Name: "c1", PeerURLs: []string{"http://127.0.0.1:9961"}}}, next: 0x100}
}

func (f *fakeMembers) list(context.Context) ([]EtcdMember, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]EtcdMember, len(f.ms))
	for i, m := range f.ms {
		m.PeerURLs = slices.Clone(m.PeerURLs)
		out[i] = m
	}
	return out, nil
}

func (f *fakeMembers) add(_ context.Context, peer string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adds++
	for _, m := range f.ms {
		if slices.Contains(m.PeerURLs, peer) {
			return 0, errors.New("etcdserver: Peer URLs already exists")
		}
	}
	id := f.next
	f.next++
	f.ms = append(f.ms, EtcdMember{ID: id, PeerURLs: []string{peer}, Learner: true})
	if f.loseAnswer {
		return 0, context.DeadlineExceeded
	}
	return id, nil
}

// set applies fn to member id: the node starting (a name) or etcd promoting it.
func (f *fakeMembers) set(id string, fn func(*EtcdMember)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.ms {
		if memberHex(f.ms[i].ID) == id {
			fn(&f.ms[i])
		}
	}
}

func (f *fakeMembers) addCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.adds
}

var joinKey = bytes.Repeat([]byte{0x5a}, 32)

// joinRig is a control rig whose server has a membership, with the join routes mounted beside the
// control API as shunt-control mounts them.
func joinRig(t *testing.T) (*rig, *fakeMembers, *httptest.Server) {
	t.Helper()
	rg := newRig(t)
	fm := newFakeMembers()
	rg.ctl.Members = &Membership{List: fm.list, AddLearner: fm.add, Key: func() []byte { return slices.Clone(joinKey) }}
	rg.ctl.Sleep = func(ctx context.Context, d time.Duration) error { // real, short: the join polls
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
			return nil
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/control/members", rg.ctl.ServeJoin)
	mux.HandleFunc("POST /v1/control/joins/{id}/bootstrap", rg.ctl.ServeJoinBootstrap)
	mux.Handle("/", rg.ctl.Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return rg, fm, srv
}

func joinCall(t *testing.T, srv *httptest.Server, path, key string, body, out any) (int, http.Header, string) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set(HeaderIdempotencyKey, key)
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
			t.Fatalf("%s: %v\n%s", path, err, b.String())
		}
	}
	return resp.StatusCode, resp.Header, b.String()
}

// waitJoin polls the record until pred holds.
func waitJoin(t *testing.T, rg *rig, id, what string, pred func(*Operation) bool) *Operation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		op, err := rg.ctl.ops().Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if op != nil && pred(op) {
			return op
		}
		if time.Now().After(deadline) {
			t.Fatalf("join %s: %s never happened: %+v", id, what, op)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func hasBlocker(op *Operation, code string) bool {
	return slices.ContainsFunc(op.Blockers, func(b Blocker) bool { return b.Code == code })
}

// A join, end to end: the record reserves the membership, the learner's ID is on it when the join
// answers, the bootstrap hands the node its member list and the key (no-store), and the record
// follows the node through catching up to a vote. The key is on no record.
func TestJoinAddsLearnerAndFollowsItToVoter(t *testing.T) {
	rg, fm, srv := joinRig(t)
	req := JoinRequest{Name: "c2", PeerURL: "HTTP://127.0.0.1:9962/", APIURL: "http://127.0.0.1:9952"}
	var op Operation
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-a", req, &op); code != http.StatusAccepted {
		t.Fatalf("join: %d %s", code, raw)
	}
	if op.Kind != OpControlJoin || op.Member == nil || op.Member.PeerURL != "http://127.0.0.1:9962" || op.Scope == nil || op.Scope.Resource != MembersResource {
		t.Fatalf("the join's record when it answers: %+v", op)
	}
	waitJoin(t, rg, op.ID, "waiting for the node to start", func(o *Operation) bool {
		return o.Phase == PhaseLearnerAdded && hasBlocker(o, BlockerMemberNotStarted)
	})

	// A second membership change while this one is unfinished is refused on its scope.
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-b", JoinRequest{Name: "c3", PeerURL: "http://127.0.0.1:9963"}, nil); code != http.StatusConflict || !strings.Contains(raw, CodeOperationConflict) {
		t.Fatalf("a second join while one is unfinished: %d %s", code, raw)
	}

	var b Bootstrap
	code, hdr, raw := joinCall(t, srv, "/v1/control/joins/"+op.ID+"/bootstrap", "", BootstrapRequest{PeerURL: "http://127.0.0.1:9962"}, &b)
	if code != http.StatusOK || hdr.Get("Cache-Control") != "no-store" {
		t.Fatalf("bootstrap: %d %q %s", code, hdr.Get("Cache-Control"), raw)
	}
	if !bytes.Equal(b.EncryptionKey, joinKey) || b.MemberID != op.Member.ID || b.InitialCluster != "c1=http://127.0.0.1:9961,c2=http://127.0.0.1:9962" {
		t.Fatalf("bootstrap: %+v", b)
	}
	if code, _, raw := joinCall(t, srv, "/v1/control/joins/"+op.ID+"/bootstrap", "", BootstrapRequest{PeerURL: "http://127.0.0.1:9999"}, nil); code != http.StatusConflict {
		t.Fatalf("bootstrap for another peer URL: %d %s", code, raw)
	}

	fm.set(op.Member.ID, func(m *EtcdMember) { m.Name = "c2" })
	waitJoin(t, rg, op.ID, "catching up", func(o *Operation) bool {
		return o.Phase == PhaseCatchingUp && hasBlocker(o, BlockerLearnerCatchingUp)
	})
	fm.set(op.Member.ID, func(m *EtcdMember) { m.Learner = false })
	done := waitJoin(t, rg, op.ID, "success", func(o *Operation) bool { return o.Terminal() })
	var res JoinResult
	if err := json.Unmarshal(done.Result, &res); err != nil || done.Status != StatusSucceeded || res.Voters != 2 || res.Name != "c2" || !strings.Contains(res.Warning, "two voting members") {
		t.Fatalf("the join's end: %+v %+v %v", done, res, err)
	}
	if fm.addCount() != 1 {
		t.Fatalf("learner adds: %d, want 1", fm.addCount())
	}
	// The key is on no record and in no event the record produced.
	stored, _ := json.Marshal(done)
	for _, enc := range []string{hex.EncodeToString(joinKey), base64.StdEncoding.EncodeToString(joinKey)} {
		if strings.Contains(string(stored), enc) {
			t.Fatalf("the data-encryption key is on the join's record: %s", stored)
		}
	}
}

// T12, lost add response: the add lands and its answer is lost. The join lists the members, finds
// its learner by peer URL, and records it; it never sends a second add (which etcd would refuse).
func TestJoinReconcilesALostAddResponse(t *testing.T) {
	rg, fm, srv := joinRig(t)
	fm.loseAnswer = true
	var op Operation
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-lost", JoinRequest{Name: "c2", PeerURL: "http://127.0.0.1:9962"}, &op); code != http.StatusAccepted {
		t.Fatalf("join: %d %s", code, raw)
	}
	got := waitJoin(t, rg, op.ID, "the learner on the record", func(o *Operation) bool { return o.Member != nil || o.Terminal() })
	if got.Member == nil || got.Member.ID != "100" || fm.addCount() != 1 {
		t.Fatalf("after a lost add answer: member %+v, %d adds, record %+v", got.Member, fm.addCount(), got)
	}
	if !strings.Contains(rg.log.String(), "join reconciled a learner already in the member list") {
		t.Fatalf("the reconciliation is not logged: %s", rg.log.String())
	}
}

// T12, owner lost: the record says learner_add and has no member; its owner died after the add
// landed. Resumed on this node, the join reconciles from the member list rather than adding again.
// A retried request with the same Idempotency-Key answers the same record, and one with another
// body under that key is refused.
func TestJoinResumesAfterOwnerLossWithoutAddingAgain(t *testing.T) {
	rg, fm, srv := joinRig(t)
	id, err := fm.add(context.Background(), "http://127.0.0.1:9962") // the dead owner's add, landed
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(JoinRequest{Name: "c2", PeerURL: "http://127.0.0.1:9962"})
	now := time.Now().UTC()
	planted := Operation{ID: "1700000000000-00j01n", Kind: OpControlJoin, Node: "dead", Actor: "test", Status: StatusRunning, Phase: PhaseLearnerAdd,
		EffectState: EffectNone, Sequence: 1, Created: now, Updated: now, Args: args, Scope: &Scope{Resource: MembersResource}}
	if err := rg.ctl.ops().Create(context.Background(), &planted); err != nil {
		t.Fatal(err)
	}
	orphaned := planted
	orphan(&orphaned, now, "gone")
	if orphaned.Status != StatusBlocked || !slices.Contains(orphaned.AllowedActions, ActionResume) {
		t.Fatalf("an orphaned join is resumable: %+v", orphaned)
	}
	if err := rg.ctl.ops().Update(context.Background(), &orphaned); err != nil {
		t.Fatal(err)
	}
	if code, raw := rg.call(http.MethodPost, "/v1/operations/"+planted.ID+"/resume", nil, nil); code != http.StatusAccepted {
		t.Fatalf("resume: %d %s", code, raw)
	}
	got := waitJoin(t, rg, planted.ID, "the learner on the record", func(o *Operation) bool { return o.Member != nil || o.Terminal() })
	if got.Member == nil || got.Member.ID != memberHex(id) || fm.addCount() != 1 {
		t.Fatalf("resumed join: member %+v, %d adds, record %+v", got.Member, fm.addCount(), got)
	}

	// Idempotency: the same key and body answer the same record; another body under it is refused.
	var first, again Operation
	body := JoinRequest{Name: "c3", PeerURL: "http://127.0.0.1:9963"}
	fm.set(got.Member.ID, func(m *EtcdMember) { m.Name, m.Learner = "c2", false })
	waitJoin(t, rg, planted.ID, "the first join to end", func(o *Operation) bool { return o.Terminal() })
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-c3", body, &first); code != http.StatusAccepted {
		t.Fatalf("join c3: %d %s", code, raw)
	}
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-c3", body, &again); code != http.StatusAccepted || again.ID != first.ID {
		t.Fatalf("a retried join: %d %s (want record %s)", code, raw, first.ID)
	}
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-c3", JoinRequest{Name: "c3", PeerURL: "http://127.0.0.1:9964"}, nil); code < 400 {
		t.Fatalf("another body under the same Idempotency-Key: %d %s", code, raw)
	}
	if fm.addCount() != 2 {
		t.Fatalf("learner adds after one resumed join and one retried: %d, want 2", fm.addCount())
	}
}

// A join refuses what would make an ambiguous membership: a taken name, a peer URL a member has, an
// unstarted learner of an earlier join, and a bootstrap asked for before the learner exists.
func TestJoinRefusals(t *testing.T) {
	rg, fm, srv := joinRig(t)
	ended := func(key string, req JoinRequest, want string) {
		t.Helper()
		var op Operation
		if code, _, raw := joinCall(t, srv, "/v1/control/members", key, req, &op); code != http.StatusAccepted {
			t.Fatalf("%s: %d %s", key, code, raw)
		}
		got := waitJoin(t, rg, op.ID, "its end", func(o *Operation) bool { return o.Terminal() })
		if got.Status != StatusFailed || got.Error == nil || !strings.Contains(got.Error.Message, want) || got.Member != nil {
			t.Fatalf("%s: %+v %+v, want failed with %q", key, got, got.Error, want)
		}
	}
	ended("taken-name", JoinRequest{Name: "c1", PeerURL: "http://127.0.0.1:9970"}, "a member named c1 already exists")
	ended("taken-peer", JoinRequest{Name: "c9", PeerURL: "http://127.0.0.1:9961"}, "already belongs to member c1")
	if _, err := fm.add(context.Background(), "http://127.0.0.1:9972"); err != nil { // an earlier join's learner, never started
		t.Fatal(err)
	}
	ended("stale-learner", JoinRequest{Name: "c4", PeerURL: "http://127.0.0.1:9972"}, "has not started")
	ended("second-learner", JoinRequest{Name: "c5", PeerURL: "http://127.0.0.1:9973"}, "one learner at a time")
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "bad-url", JoinRequest{Name: "c6", PeerURL: "http://127.0.0.1"}, nil); code != http.StatusBadRequest {
		t.Fatalf("a peer URL without a port: %d %s", code, raw)
	}
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "", JoinRequest{Name: "c7", PeerURL: "http://127.0.0.1:9975"}, nil); code != http.StatusBadRequest || !strings.Contains(raw, CodeIdempotencyKeyRequired) {
		t.Fatalf("a join without an Idempotency-Key: %d %s", code, raw)
	}
	if fm.addCount() != 1 {
		t.Fatalf("learner adds: %d; only the planted one should exist", fm.addCount())
	}
}
