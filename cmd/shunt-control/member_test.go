package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/blakegolliher/shunt/internal/control"
)

// member remove (H3c): the dry run first, then the removal with its token and an Idempotency-Key;
// a member named by --id; a refusal said as the dry run gave it; --dry-run removes nothing.
func TestMemberRemoveCommand(t *testing.T) {
	var mu sync.Mutex
	var removals []*http.Request
	var tokens []string
	mux := http.NewServeMux()
	answer := func(w http.ResponseWriter, r *http.Request, m control.MemberView, allowed bool) {
		if r.URL.Query().Get("dry_run") != "" {
			plan := control.MemberRemoveDryRun{Allowed: allowed, Member: m, VotersAfter: 2, Warning: "two voting members left", Token: "tok-" + m.ID}
			if !allowed {
				plan.Reason = "member 1 is the only voting member"
			}
			_ = json.NewEncoder(w).Encode(plan)
			return
		}
		var req control.RemoveRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		removals, tokens = append(removals, r), append(tokens, req.Token)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(control.MemberRemoveResult{MemberID: m.ID, Name: m.Name, Voters: 2, Operation: "1700000000000-rm0001"})
	}
	mux.HandleFunc("DELETE /v1/control/members/{name}", func(w http.ResponseWriter, r *http.Request) {
		answer(w, r, control.MemberView{ID: "2", Name: r.PathValue("name"), Role: "voter", Started: true}, r.PathValue("name") != "c1")
	})
	mux.HandleFunc("DELETE /v1/control/members/by-id/{id}", func(w http.ResponseWriter, r *http.Request) {
		answer(w, r, control.MemberView{ID: r.PathValue("id"), Role: "learner"}, true)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	run := func(args ...string) (string, error) {
		cmd := newMember()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(append(args, "--api", srv.URL))
		err := cmd.Execute()
		return out.String(), err
	}

	out, err := run("remove", "c2")
	if err != nil || !strings.Contains(out, "removing member c2 (2, voter, started); 2 voting member(s) left") ||
		!strings.Contains(out, "warning: two voting members left") || !strings.Contains(out, "member c2 removed (operation 1700000000000-rm0001)") {
		t.Fatalf("remove c2: %v\n%s", err, out)
	}
	out, err = run("remove", "--id", "8e9e05c52164694d")
	if err != nil || !strings.Contains(out, "(unnamed) (8e9e05c52164694d, learner, never started)") {
		t.Fatalf("remove --id: %v\n%s", err, out)
	}
	mu.Lock()
	if len(removals) != 2 || tokens[0] != "tok-2" || tokens[1] != "tok-8e9e05c52164694d" ||
		removals[0].Header.Get(control.HeaderIdempotencyKey) == "" || removals[0].Header.Get(control.HeaderIdempotencyKey) == removals[1].Header.Get(control.HeaderIdempotencyKey) {
		t.Fatalf("removals sent: %d, tokens %v", len(removals), tokens)
	}
	mu.Unlock()

	if out, err = run("remove", "c1"); err == nil || !strings.Contains(err.Error(), "refused: member 1 is the only voting member") {
		t.Fatalf("remove c1: %v\n%s", err, out)
	}
	if out, err = run("remove", "c3", "--dry-run"); err != nil || !strings.Contains(out, "would remove member c3") {
		t.Fatalf("--dry-run: %v\n%s", err, out)
	}
	if _, err = run("remove"); err == nil {
		t.Fatal("remove with neither a name nor --id")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(removals) != 2 {
		t.Fatalf("a refusal or a dry run sent a removal: %d", len(removals))
	}
}
