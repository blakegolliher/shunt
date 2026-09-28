package control

// Removing a control-plane member by its ID (ADR-0021 D4, H3c; T12): the dry run and its token,
// unnamed learners, name reuse, the only voter and the answering node, a lost removal answer, etcd's
// quorum refusal, owner loss, and a removal beside a join.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// deleteCall sends a DELETE with an optional JSON body and Idempotency-Key.
func deleteCall(t *testing.T, srv *httptest.Server, path, key string, body, out any) (int, string) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(http.MethodDelete, srv.URL+path, rd)
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
	return resp.StatusCode, b.String()
}

// threeVoters is the fake membership with c2 (0x2) and c3 (0x3) beside c1.
func threeVoters(fm *fakeMembers) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.ms = append(fm.ms, EtcdMember{ID: 0x2, Name: "c2", PeerURLs: []string{"http://127.0.0.1:9962"}},
		EtcdMember{ID: 0x3, Name: "c3", PeerURLs: []string{"http://127.0.0.1:9963"}})
}

func memberIDs(fm *fakeMembers) []string {
	ms, _ := fm.state()
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, memberHex(m.ID))
	}
	return out
}

// A voter removed by name, and an unnamed learner (a failed join's, never started) removed by id:
// each dry run names the member, its role and the voters left, and warns at two; each removal,
// with its token, removes that ID and nothing else, and its record ends succeeded. A name finds
// no unnamed member.
func TestMemberRemoveByNameAndByID(t *testing.T) {
	rg, fm, srv := joinRig(t)
	threeVoters(fm)
	var plan MemberRemoveDryRun
	if code, raw := deleteCall(t, srv, "/v1/control/members/c2?dry_run=1", "", nil, &plan); code != http.StatusOK {
		t.Fatalf("dry run: %d %s", code, raw)
	}
	if !plan.Allowed || plan.Member.ID != "2" || plan.Member.Role != "voter" || !plan.Member.Started || plan.VotersAfter != 2 ||
		!strings.Contains(plan.Warning, "two voting members") || plan.Token == "" {
		t.Fatalf("dry run: %+v", plan)
	}
	var res MemberRemoveResult
	if code, raw := deleteCall(t, srv, "/v1/control/members/c2", "remove-c2", RemoveRequest{Token: plan.Token}, &res); code != http.StatusOK {
		t.Fatalf("removal: %d %s", code, raw)
	}
	if res.MemberID != "2" || res.Name != "c2" || res.Voters != 2 {
		t.Fatalf("result: %+v", res)
	}
	if got := memberIDs(fm); !slices.Equal(got, []string{"1", "3"}) {
		t.Fatalf("members after removing c2: %v", got)
	}
	if op, _ := rg.ctl.ops().Get(context.Background(), res.Operation); op == nil || op.Status != StatusSucceeded || op.Kind != OpControlRemove || op.Member == nil || op.Member.ID != "2" {
		t.Fatalf("the removal's record: %+v", op)
	}

	id, err := fm.add(context.Background(), "http://127.0.0.1:9972") // a failed join's learner, never started
	if err != nil {
		t.Fatal(err)
	}
	if code, raw := deleteCall(t, srv, "/v1/control/members/?dry_run=1", "", nil, nil); code < 400 {
		t.Fatalf("an empty name: %d %s", code, raw)
	}
	path := "/v1/control/members/by-id/" + memberHex(id)
	plan = MemberRemoveDryRun{}
	if code, raw := deleteCall(t, srv, path+"?dry_run=1", "", nil, &plan); code != http.StatusOK || plan.Member.Role != "learner" || plan.Member.Started || plan.VotersAfter != 2 {
		t.Fatalf("dry run of an unnamed learner: %d %s", code, raw)
	}
	if code, raw := deleteCall(t, srv, path, "remove-learner", RemoveRequest{Token: plan.Token}, nil); code != http.StatusOK {
		t.Fatalf("removing an unnamed learner: %d %s", code, raw)
	}
	if _, removed := fm.state(); !slices.Equal(removed, []uint64{0x2, id}) {
		t.Fatalf("removed %x, want exactly 2 and %x", removed, id)
	}
}

// The token binds the member and the member list: a name reused by a replacement never removes the
// replacement (its ID differs), and a removal confirmed by a dry run is refused once the membership
// changed around the same member. Without a token, or with a forged one, nothing is removed.
// Negative control: a token bound to the member's ID alone lets the removal after the membership
// changed through.
func TestMemberRemoveTokenBindsTheMembership(t *testing.T) {
	_, fm, srv := joinRig(t)
	threeVoters(fm)
	var plan MemberRemoveDryRun
	deleteCall(t, srv, "/v1/control/members/c3?dry_run=1", "", nil, &plan)

	// c3 is removed by another hand and a replacement joins under the same name.
	fm.mu.Lock()
	fm.ms = slices.DeleteFunc(fm.ms, func(m EtcdMember) bool { return m.ID == 0x3 })
	fm.ms = append(fm.ms, EtcdMember{ID: 0x30, Name: "c3", PeerURLs: []string{"http://127.0.0.1:9973"}})
	fm.mu.Unlock()
	if code, raw := deleteCall(t, srv, "/v1/control/members/c3", "late", RemoveRequest{Token: plan.Token}, nil); code != http.StatusConflict || !strings.Contains(raw, "does not match") {
		t.Fatalf("a late removal of a reused name: %d %s", code, raw)
	}
	// Same member, another membership: c2 confirmed, then a learner added.
	deleteCall(t, srv, "/v1/control/members/c2?dry_run=1", "", nil, &plan)
	if _, err := fm.add(context.Background(), "http://127.0.0.1:9979"); err != nil {
		t.Fatal(err)
	}
	if code, raw := deleteCall(t, srv, "/v1/control/members/c2", "changed", RemoveRequest{Token: plan.Token}, nil); code != http.StatusConflict || !strings.Contains(raw, "does not match") {
		t.Fatalf("a removal after the membership changed: %d %s", code, raw)
	}
	for _, c := range []struct{ key, token, want string }{
		{"no-token", "", "needs the confirmation token"},
		{"forged", "1700000000.AAAA", "expired"},
	} {
		if code, raw := deleteCall(t, srv, "/v1/control/members/by-id/30", c.key, RemoveRequest{Token: c.token}, nil); code != http.StatusConflict || !strings.Contains(raw, c.want) {
			t.Fatalf("%s: %d %s", c.key, code, raw)
		}
	}
	if _, removed := fm.state(); len(removed) != 0 {
		t.Fatalf("refused removals removed %x", removed)
	}
}

// Refused whatever the token: the only voter, and the control node answering.
func TestMemberRemoveRefusals(t *testing.T) {
	rg, fm, srv := joinRig(t)
	var plan MemberRemoveDryRun
	if code, raw := deleteCall(t, srv, "/v1/control/members/c1?dry_run=1", "", nil, &plan); code != http.StatusOK || plan.Allowed || !strings.Contains(plan.Reason, "only voting member") {
		t.Fatalf("dry run of the only voter: %d %s", code, raw)
	}
	threeVoters(fm)
	rg.ctl.Members.Self = func() uint64 { return 0x1 }
	if code, raw := deleteCall(t, srv, "/v1/control/members/c1?dry_run=1", "", nil, &plan); code != http.StatusOK || plan.Allowed || !strings.Contains(plan.Reason, "the control node answering") {
		t.Fatalf("dry run of the answering node: %d %s", code, raw)
	}
	if code, raw := deleteCall(t, srv, "/v1/control/members/by-id/9", "unknown", nil, nil); code != http.StatusNotFound {
		t.Fatalf("an unknown member without a dry run: %d %s", code, raw)
	}
	if code, raw := deleteCall(t, srv, "/v1/control/members/by-id/zz?dry_run=1", "", nil, nil); code != http.StatusBadRequest {
		t.Fatalf("an id that is not hex: %d %s", code, raw)
	}
	if n := fm.removeCount(); n != 0 {
		t.Fatalf("%d removals sent", n)
	}
}

// The removal's answer is lost: the member is gone, and the next round, finding the ID absent with
// the intent on the record, ends succeeded without sending it again. etcd's quorum refusal ends the
// removal refused, with nothing removed.
func TestMemberRemoveLostAnswerAndQuorumRefusal(t *testing.T) {
	_, fm, srv := joinRig(t)
	threeVoters(fm)
	var plan MemberRemoveDryRun
	deleteCall(t, srv, "/v1/control/members/c3?dry_run=1", "", nil, &plan)
	fm.mu.Lock()
	fm.loseRemove = true
	fm.mu.Unlock()
	var res MemberRemoveResult
	if code, raw := deleteCall(t, srv, "/v1/control/members/by-id/3", "lost", RemoveRequest{Token: plan.Token}, &res); code != http.StatusOK || res.Voters != 2 {
		t.Fatalf("a removal whose answer was lost: %d %s", code, raw)
	}
	if fm.removeCount() != 1 || !slices.Equal(memberIDs(fm), []string{"1", "2"}) {
		t.Fatalf("after a lost answer: %d removals, members %v", fm.removeCount(), memberIDs(fm))
	}

	fm.mu.Lock()
	fm.loseRemove, fm.removeErr = false, fmt.Errorf("etcd member remove: %w", ErrQuorumAtRisk)
	fm.mu.Unlock()
	deleteCall(t, srv, "/v1/control/members/c2?dry_run=1", "", nil, &plan)
	if code, raw := deleteCall(t, srv, "/v1/control/members/c2", "quorum", RemoveRequest{Token: plan.Token}, nil); code != http.StatusConflict || !strings.Contains(raw, "could not keep quorum") {
		t.Fatalf("etcd's quorum refusal: %d %s", code, raw)
	}
	if !slices.Equal(memberIDs(fm), []string{"1", "2"}) {
		t.Fatalf("a refused removal changed the membership: %v", memberIDs(fm))
	}
}

// A removal whose owner was lost after it recorded its intent: resumed on this node, it reconciles
// by ID. The member already gone: succeeded, nothing sent. The member still there: sent once for
// that ID, without the dry run's token, which the record no longer needs.
func TestMemberRemoveResumesAfterOwnerLoss(t *testing.T) {
	rg, fm, srv := joinRig(t)
	threeVoters(fm)
	for _, c := range []struct {
		name, id string
		present  bool
	}{{"already removed", "7", false}, {"still a member", "3", true}} {
		t.Run(c.name, func(t *testing.T) {
			before := fm.removeCount()
			args, _ := json.Marshal(MemberRemoveRequest{ID: c.id, Token: "expired long ago"})
			now := time.Now().UTC()
			planted := Operation{ID: "1700000000000-r3m0v" + c.id, Kind: OpControlRemove, Node: "dead", Actor: "test", Status: StatusRunning,
				Phase: PhaseMemberRemove, EffectState: EffectNone, Sequence: 1, Created: now, Updated: now, Args: args,
				Scope: &Scope{Resource: MembersResource}, Member: &JoinMember{ID: c.id}}
			if err := rg.ctl.ops().Create(context.Background(), &planted); err != nil {
				t.Fatal(err)
			}
			orphaned := planted
			orphan(&orphaned, now, "gone")
			if !slices.Contains(orphaned.AllowedActions, ActionResume) {
				t.Fatalf("an orphaned removal offers %v, want resume", orphaned.AllowedActions)
			}
			if err := rg.ctl.ops().Update(context.Background(), &orphaned); err != nil {
				t.Fatal(err)
			}
			if code, raw := rg.call(http.MethodPost, "/v1/operations/"+planted.ID+"/resume", nil, nil); code != http.StatusAccepted {
				t.Fatalf("resume: %d %s", code, raw)
			}
			done := waitJoin(t, rg, planted.ID, "its end", func(o *Operation) bool { return o.Terminal() })
			sent := fm.removeCount() - before
			if done.Status != StatusSucceeded || (c.present && sent != 1) || (!c.present && sent != 0) {
				t.Fatalf("resumed removal: %s, %d removals sent", done.Status, sent)
			}
			if code, raw := deleteCall(t, srv, "/v1/control/members/by-id/"+c.id+"?dry_run=1", "", nil, nil); code != http.StatusNotFound {
				t.Fatalf("the member after its removal: %d %s", code, raw)
			}
		})
	}
}

// One membership change at a time: a removal while a join is unfinished is refused on its scope,
// and a join's leftover learner is the join's to cancel until the join ends.
func TestMemberRemoveBesideAJoin(t *testing.T) {
	rg, fm, srv := joinRig(t)
	threeVoters(fm)
	var op Operation
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-c4", JoinRequest{Name: "c4", PeerURL: "http://127.0.0.1:9964"}, &op); code != http.StatusAccepted {
		t.Fatalf("join: %d %s", code, raw)
	}
	waitJoin(t, rg, op.ID, "waiting for the node", func(o *Operation) bool { return hasBlocker(o, BlockerMemberNotStarted) })
	var plan MemberRemoveDryRun
	deleteCall(t, srv, "/v1/control/members/by-id/"+op.Member.ID+"?dry_run=1", "", nil, &plan)
	if code, raw := deleteCall(t, srv, "/v1/control/members/by-id/"+op.Member.ID, "beside", RemoveRequest{Token: plan.Token}, nil); code != http.StatusConflict || !strings.Contains(raw, CodeOperationConflict) {
		t.Fatalf("a removal beside an unfinished join: %d %s", code, raw)
	}
	if _, removed := fm.state(); len(removed) != 0 {
		t.Fatalf("removed %x beside a join", removed)
	}
}
