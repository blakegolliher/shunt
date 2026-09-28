package cp

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
)

// Removing control-plane members on real etcd (ADR-0021 D4, H3c, T12): of three voters, c3 by name,
// then an unnamed learner (added, never started) by id, then c2, which leaves one voter and says so;
// each through its dry run and token, each removing exactly its ID. The node answering is never
// removable. The first node stays writable throughout.
func TestMemberRemoveOnEtcd(t *testing.T) {
	tc := startCluster(t, 3)
	key := bytes.Repeat([]byte{0x66}, 32)
	_, call := memberAPI(t, tc, key, tc.nodes[0].Promote)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	names := func() []string {
		ms, err := tc.nodes[0].ListMembers(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(ms))
		for _, m := range ms {
			out = append(out, firstNonEmpty(m.Name, fmt.Sprintf("%x", m.ID)))
		}
		slices.Sort(out)
		return out
	}
	remove := func(path, idem string) control.MemberRemoveResult {
		t.Helper()
		var plan control.MemberRemoveDryRun
		if code, raw := call(http.MethodDelete, path+"?dry_run=1", "", nil, &plan); code != http.StatusOK || !plan.Allowed {
			t.Fatalf("dry run %s: %d %s", path, code, raw)
		}
		var res control.MemberRemoveResult
		if code, raw := call(http.MethodDelete, path, idem, control.RemoveRequest{Token: plan.Token}, &res); code != http.StatusOK {
			t.Fatalf("removal %s: %d %s", path, code, raw)
		}
		return res
	}

	var self control.MemberRemoveDryRun
	if code, raw := call(http.MethodDelete, "/v1/control/members/c1?dry_run=1", "", nil, &self); code != http.StatusOK || self.Allowed || !strings.Contains(self.Reason, "answering") {
		t.Fatalf("dry run of the answering node: %d %s", code, raw)
	}
	if res := remove("/v1/control/members/c3", "remove-c3"); res.Voters != 2 || !strings.Contains(res.Warning, "two voting members") {
		t.Fatalf("removing c3: %+v", res)
	}
	if got := names(); !slices.Equal(got, []string{"c1", "c2"}) {
		t.Fatalf("members after removing c3: %v", got)
	}
	id, err := tc.nodes[0].AddLearner(ctx, fmt.Sprintf("http://127.0.0.1:%d", freePort(t))) // a join that never started its node
	if err != nil {
		t.Fatal(err)
	}
	remove(fmt.Sprintf("/v1/control/members/by-id/%x", id), "remove-learner")
	if res := remove("/v1/control/members/c2", "remove-c2"); res.Voters != 1 || !strings.Contains(res.Warning, "one voting member") {
		t.Fatalf("removing c2: %+v", res)
	}
	if got := names(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("members at the end: %v", got)
	}
	if _, err := tc.nodes[0].Client().Put(ctx, "/test/after-removals", "ok"); err != nil {
		t.Fatalf("the first node after the removals: %v", err)
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
