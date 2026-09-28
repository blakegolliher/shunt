package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/cp"
)

// status's exit says what was observed (ADR-0021 D4, contracts §2): 0 all healthy with quorum
// reachable; 1 a member unreachable or quorum unavailable; 3 nothing wrong observed but something
// not observed lately. Unhealthy wins over unknown. The answer is printed, table or JSON, either
// way. Negative control: judging by the member list alone, as status did, exits 0 for all four.
func TestStatusExitSaysWhatWasObserved(t *testing.T) {
	healthy := func(name string) cp.MemberHealth {
		return cp.MemberHealth{ID: name + "-id", Name: name, Role: "voter", Started: true, Health: cp.HealthHealthy}
	}
	reachable := cp.QuorumObservation{State: cp.QuorumReachable, Method: "linearizable_read"}
	cases := []struct {
		name  string
		st    cp.StatusAnswer
		code  int
		cause string
	}{
		{"healthy", cp.StatusAnswer{Members: []cp.MemberHealth{healthy("c1"), healthy("c2"), healthy("c3")}, Quorum: reachable}, 0, ""},
		{"a learner never started", cp.StatusAnswer{Members: []cp.MemberHealth{healthy("c1"), healthy("c2"), {ID: "8e9e05c52164694d", Role: "learner", Health: cp.HealthUnknown}}, Quorum: reachable}, exitUnknown, "health not known for 8e9e05c52164694d"},
		{"c3 stopped and c2 not observed", cp.StatusAnswer{Members: []cp.MemberHealth{healthy("c1"), {ID: "c2-id", Name: "c2", Role: "voter", Health: cp.HealthUnknown}, {ID: "c3-id", Name: "c3", Role: "voter", Health: cp.HealthUnreachable}}, Quorum: reachable}, exitUnhealthy, "not healthy: c3 unreachable"},
		{"quorum lost", cp.StatusAnswer{Members: []cp.MemberHealth{healthy("c1")}, Quorum: cp.QuorumObservation{State: cp.QuorumUnavailable, ErrorCode: "timeout"}}, exitUnhealthy, "not healthy: quorum unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(tc.st) }))
			t.Cleanup(srv.Close)
			for _, asJSON := range []bool{false, true} {
				root := newRoot() // as the binary runs it: no usage text after an exit status
				var out, stderr bytes.Buffer
				root.SetOut(&out)
				root.SetErr(&stderr)
				args := []string{"status", "--api", srv.URL}
				if asJSON {
					args = append(args, "--json")
				}
				root.SetArgs(args)
				err := root.Execute()
				var ee *exitError
				switch {
				case tc.code == 0 && err != nil:
					t.Fatalf("json %v: %v", asJSON, err)
				case tc.code != 0 && (!errors.As(err, &ee) || ee.code != tc.code || !strings.Contains(err.Error(), tc.cause)):
					t.Fatalf("json %v: %v, want exit %d with %q", asJSON, err, tc.code, tc.cause)
				}
				if asJSON && !json.Valid(out.Bytes()) {
					t.Fatalf("--json printed no single JSON answer:\n%s", out.String())
				}
				if stderr.Len() != 0 {
					t.Fatalf("the command wrote to stderr itself (main reports the exit):\n%s", stderr.String())
				}
				if !asJSON && !strings.Contains(out.String(), "MEMBER") {
					t.Fatalf("no table printed:\n%s", out.String())
				}
			}
		})
	}
}
