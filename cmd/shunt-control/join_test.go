package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/cp"
)

// stubCluster is a control API answering a join as a cluster does: the status, the join (its
// operation with the learner on it), and the bootstrap, which can be made to fail.
type stubCluster struct {
	mu          sync.Mutex
	members     []cp.MemberInfo
	joins       []string // the Idempotency-Keys of join requests
	bootstraps  int
	failBoot    int // this many bootstraps answer 500 first
	busy        int // this many join requests answer operation_conflict first
	key         []byte
	lastRequest control.JoinRequest
	ops         []control.Operation // GET /v1/operations/{id} answers these in turn, then the last
	resumes     int
}

func (s *stubCluster) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/control", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(cp.StatusAnswer{Cluster: cp.ClusterStatus{Members: s.members}})
	})
	mux.HandleFunc("POST /v1/control/members", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.joins = append(s.joins, r.Header.Get(control.HeaderIdempotencyKey))
		if err := json.NewDecoder(r.Body).Decode(&s.lastRequest); err != nil {
			t.Error(err)
		}
		if s.busy > 0 {
			s.busy--
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"operation_conflict","message":"control:members is owned by unfinished operation 1700000000000-0ther1"}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(control.Operation{ID: "1700000000000-j0in01", Kind: control.OpControlJoin, Status: control.StatusBlocked,
			Phase: control.PhaseLearnerAdded, Member: &control.JoinMember{ID: "a1", PeerURL: s.lastRequest.PeerURL}})
	})
	mux.HandleFunc("POST /v1/control/joins/{id}/bootstrap", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.bootstraps++
		if s.failBoot > 0 {
			s.failBoot--
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":"internal","message":"the node answering the bootstrap stopped"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(control.Bootstrap{Operation: "1700000000000-j0in01", MemberID: "a1", Name: "c2",
			InitialCluster: "c1=http://127.0.0.1:9961,c2=http://127.0.0.1:9962", EncryptionKey: s.key})
	})
	mux.HandleFunc("GET /v1/operations/{id}", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		op := s.ops[0]
		if len(s.ops) > 1 {
			s.ops = s.ops[1:]
		}
		_ = json.NewEncoder(w).Encode(op)
	})
	mux.HandleFunc("POST /v1/operations/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.resumes++
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(control.Operation{ID: r.PathValue("id"), Kind: control.OpControlJoin, Node: "c1", Status: control.StatusRunning})
	})
	return mux
}

func (s *stubCluster) counts() (joins, bootstraps int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.joins), s.bootstraps
}

// A join interrupted anywhere resumes from join.json: the request id is saved before the cluster is
// asked, a failed bootstrap resumes without a second join request, a node stopped after saving its
// bootstrap starts from it with no request at all, and the key is written 0600 and never into
// join.json.
func TestJoinResumesFromItsLocalRecord(t *testing.T) {
	stub := &stubCluster{members: []cp.MemberInfo{{Name: "c1", ID: "1", PeerURLs: []string{"http://127.0.0.1:9961"}}},
		failBoot: 1, key: bytes.Repeat([]byte{0x42}, 32)}
	srv := httptest.NewServer(stub.handler(t))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	o := &nodeOptions{name: "c2", dataDir: dir, peerURL: "http://127.0.0.1:9962/", api: "0.0.0.0:9952"}
	log := slog.New(slog.DiscardHandler)
	ctx := context.Background()

	if _, _, err := joinCluster(ctx, o, srv.URL, "", "", log); err == nil || !strings.Contains(err.Error(), "run the same command again") {
		t.Fatalf("a join whose bootstrap failed: %v", err)
	}
	st, err := loadJoin(dir)
	if err != nil || st == nil || st.Phase != joinRequested || st.Operation != "1700000000000-j0in01" || !strings.HasPrefix(st.RequestID, "join-") ||
		st.PeerURL != "http://127.0.0.1:9962" || st.APIURL != "http://127.0.0.1:9952" {
		t.Fatalf("join.json after the failed bootstrap: %+v %v", st, err)
	}
	if stub.lastRequest.APIURL != "http://127.0.0.1:9952" {
		t.Fatalf("the join did not advertise its API: %+v", stub.lastRequest)
	}

	// Resumed with the same command: no second join request, the bootstrap saved.
	key, initial, err := joinCluster(ctx, o, "", "", "", log)
	if err != nil {
		t.Fatal(err)
	}
	if joins, _ := stub.counts(); joins != 1 {
		t.Fatalf("join requests after a resume: %d, want 1", joins)
	}
	if !bytes.Equal(key, stub.key) || initial != "c1=http://127.0.0.1:9961,c2=http://127.0.0.1:9962" {
		t.Fatalf("resumed join: key %x, initial %q", key, initial)
	}
	fi, err := os.Stat(filepath.Join(dir, "encryption.key"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("the key file: %v %v", fi, err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, joinFile))
	if strings.Contains(string(raw), hex.EncodeToString(stub.key)) || !strings.Contains(string(raw), joinBootstrapSaved) {
		t.Fatalf("join.json: %s", raw)
	}

	// Stopped after saving the bootstrap, before etcd created its member: no request at all.
	_, bootsBefore := stub.counts()
	if k2, i2, err := joinCluster(ctx, o, "", "", "", log); err != nil || !bytes.Equal(k2, key) || i2 != initial {
		t.Fatalf("a restart after the bootstrap was saved: %v", err)
	}
	if joins, boots := stub.counts(); joins != 1 || boots != bootsBefore {
		t.Fatalf("requests on a restart after the bootstrap was saved: %d joins, %d bootstraps", joins, boots)
	}

	// Another name or peer URL on this data directory is refused.
	other := *o
	other.name = "c9"
	if _, _, err := joinCluster(ctx, &other, srv.URL, "", "", log); err == nil || !strings.Contains(err.Error(), "records a join as c2") {
		t.Fatalf("a join with another name on the same data directory: %v", err)
	}
}

// The preflight refuses before anything is asked of the cluster: a data directory that is not
// empty, and a name or peer URL a member already has.
func TestJoinPreflight(t *testing.T) {
	stub := &stubCluster{members: []cp.MemberInfo{{Name: "c1", ID: "1", PeerURLs: []string{"http://127.0.0.1:9961"}}}, key: bytes.Repeat([]byte{1}, 32)}
	srv := httptest.NewServer(stub.handler(t))
	t.Cleanup(srv.Close)
	log := slog.New(slog.DiscardHandler)
	ctx := context.Background()
	cases := []struct {
		name, peer, want string
		dirty            bool
	}{
		{"c1", "http://127.0.0.1:9970", "already has a member named c1", false},
		{"c5", "http://127.0.0.1:9961", "already has a member at peer URL", false},
		{"c6", "http://127.0.0.1:9971", "is not empty", true},
	}
	for _, c := range cases {
		dir := t.TempDir()
		if c.dirty {
			if err := os.WriteFile(filepath.Join(dir, "leftover"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		o := &nodeOptions{name: c.name, dataDir: dir, peerURL: c.peer, api: "127.0.0.1:9952"}
		if _, _, err := joinCluster(ctx, o, srv.URL, "", "", log); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s at %s: %v, want %q", c.name, c.peer, err, c.want)
		}
		if st, _ := loadJoin(dir); st != nil {
			t.Fatalf("%s: a refused preflight left join.json: %+v", c.name, st)
		}
	}
	if joins, _ := stub.counts(); joins != 0 {
		t.Fatalf("join requests after refused preflights: %d", joins)
	}
}

// A join that arrives while another membership change is unfinished waits for it: the control plane
// refuses the second with operation_conflict rather than queue it, and the node retries with the
// same Idempotency-Key, since the refused request left no record.
func TestJoinWaitsForAnotherMembershipChange(t *testing.T) {
	stub := &stubCluster{members: []cp.MemberInfo{{Name: "c1", ID: "1", PeerURLs: []string{"http://127.0.0.1:9961"}}}, busy: 2, key: bytes.Repeat([]byte{7}, 32)}
	srv := httptest.NewServer(stub.handler(t))
	t.Cleanup(srv.Close)
	o := &nodeOptions{name: "c3", dataDir: t.TempDir(), peerURL: "http://127.0.0.1:9963", api: "127.0.0.1:9953"}
	if _, _, err := joinCluster(context.Background(), o, srv.URL, "", "", slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.joins) != 3 || stub.joins[0] != stub.joins[1] || stub.joins[1] != stub.joins[2] || stub.joins[0] == "" {
		t.Fatalf("join requests: %q; want three with one Idempotency-Key", stub.joins)
	}
}

// A started node follows its join (H3b): it resumes a join whose owner is gone, and marks join.json
// promoted once the join succeeds, so a restart no longer asks the cluster. A canceled join stops the
// node with the join's reason and what to do.
func TestJoinedNodeFollowsItsJoin(t *testing.T) {
	joinFollowPoll = 5 * time.Millisecond
	t.Cleanup(func() { joinFollowPoll = 2 * time.Second })
	id := "1700000000000-j0in01"
	stub := &stubCluster{ops: []control.Operation{
		{ID: id, Kind: control.OpControlJoin, Status: control.StatusBlocked, AllowedActions: []string{control.ActionResume, control.ActionCancel},
			Blockers: []control.Blocker{{Code: control.BlockerOwnerLost}}},
		{ID: id, Kind: control.OpControlJoin, Status: control.StatusBlocked, Phase: control.PhaseCatchingUp},
		{ID: id, Kind: control.OpControlJoin, Status: control.StatusSucceeded},
	}}
	srv := httptest.NewServer(stub.handler(t))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	st := &joinState{Version: 1, Name: "c2", PeerURL: "http://127.0.0.1:9962", Existing: srv.URL, Operation: id, Phase: joinBootstrapSaved}
	if err := saveJoin(dir, st); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	if err := followJoin(context.Background(), &apiClient{base: srv.URL}, dir, st, log); err != nil {
		t.Fatal(err)
	}
	if got, _ := loadJoin(dir); got == nil || got.Phase != joinPromoted {
		t.Fatalf("join.json after the join succeeded: %+v", got)
	}
	stub.mu.Lock()
	resumes := stub.resumes
	stub.mu.Unlock()
	if resumes != 1 {
		t.Fatalf("resumes of a join whose owner was gone: %d, want 1", resumes)
	}

	canceled := &stubCluster{ops: []control.Operation{{ID: id, Kind: control.OpControlJoin, Status: control.StatusCancelled,
		Error: &control.Error{Code: control.StatusCancelled, Message: "canceled by token:ab; learner a1 was removed"}}}}
	srv2 := httptest.NewServer(canceled.handler(t))
	t.Cleanup(srv2.Close)
	st.Phase = joinBootstrapSaved
	err := followJoin(context.Background(), &apiClient{base: srv2.URL}, dir, st, log)
	if err == nil || !strings.Contains(err.Error(), "ended cancelled: canceled by token:ab") || !strings.Contains(err.Error(), "remove "+dir) { //nolint:misspell // the status as the contract spells it
		t.Fatalf("a canceled join: %v", err)
	}
}
