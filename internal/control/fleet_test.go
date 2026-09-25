package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// recordingFleet is a fleet whose members are what the test says, and which records the
// retirements, requests and resolutions it is asked for.
type recordingFleet struct {
	ms      []Member
	retired []RetireRequest
	asked   []string
	solved  []ResolveRequest
	actors  []string
	forget  error
}

func (f *recordingFleet) Heartbeat(context.Context, string, Heartbeat) (Grant, error) {
	return Grant{LeaseTTL: time.Second}, nil
}
func (f *recordingFleet) Members(context.Context) ([]Member, error) { return f.ms, nil }
func (f *recordingFleet) Retire(_ context.Context, _, inc string, uncertain int64) error {
	f.retired = append(f.retired, RetireRequest{Incarnation: inc, Uncertain: uncertain})
	return nil
}
func (f *recordingFleet) RequestRetire(_ context.Context, id string) error {
	f.asked = append(f.asked, id)
	return nil
}
func (f *recordingFleet) Resolve(_ context.Context, _, inc, attestation, actor string) error {
	f.solved = append(f.solved, ResolveRequest{Incarnation: inc, Attestation: attestation})
	f.actors = append(f.actors, actor)
	return nil
}
func (f *recordingFleet) Forget(context.Context, string) error { return f.forget }

const testInc = "0123456789abcdef0123456789abcdef"

// The retirement routes (ADR-0021 D2): a proxy reports its own retirement, an operator requests
// one, resolves an unresolved incarnation with an attestation, and forget is refused with the
// blockers while an incarnation did not retire.
func TestRetireResolveAndForgetRoutes(t *testing.T) {
	d := lineageDir(t)
	unresolved := Incarnation{ID: testInc, State: IncarnationUnclean, Uncertain: 2, Ended: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)}
	fleet := &recordingFleet{ms: []Member{{ID: "p1", Live: false, Identity: lineageA, Applied: 3, Unresolved: []Incarnation{unresolved}}}}
	fleet.forget = &RetirementError{Proxy: "p1", Incarnations: []Incarnation{unresolved}, What: "forgetting proxy p1 refused"}
	h := (&Server{Dir: d, Fleet: fleet}).Handler()
	call := func(method, path string, body any) (int, []byte) {
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		req := httptest.NewRequest(method, path, strings.NewReader(string(b)))
		req.RemoteAddr = "127.0.0.1:1"
		req.Header.Set("Authorization", "Bearer op-token")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, rec.Body.Bytes()
	}
	if code, body := call(http.MethodPost, "/v1/fleet/p1/retire", RetireRequest{Incarnation: testInc, Uncertain: 1}); code != http.StatusOK || len(fleet.retired) != 1 || fleet.retired[0].Uncertain != 1 {
		t.Fatalf("a proxy's retirement: %d %s %+v", code, body, fleet.retired)
	}
	if code, _ := call(http.MethodPost, "/v1/fleet/p1/retire", RetireRequest{Incarnation: "nope"}); code != http.StatusBadRequest {
		t.Fatalf("a malformed retirement: %d", code)
	}
	code, body := call(http.MethodPost, "/v1/fleet/p1/retire", nil)
	var res MemberResult
	_ = json.Unmarshal(body, &res)
	if code != http.StatusOK || len(fleet.asked) != 1 || fleet.asked[0] != "p1" || res.Member.ID != "p1" {
		t.Fatalf("an operator's retire request: %d %s", code, body)
	}
	if code, _ := call(http.MethodPost, "/v1/fleet/p1/resolve", ResolveRequest{Incarnation: testInc}); code != http.StatusBadRequest {
		t.Fatalf("a resolution without an attestation: %d", code)
	}
	if code, _ := call(http.MethodPost, "/v1/fleet/p1/resolve", ResolveRequest{Incarnation: testInc, Attestation: "host wiped 2026-09-24"}); code != http.StatusOK ||
		len(fleet.solved) != 1 || fleet.solved[0].Attestation != "host wiped 2026-09-24" || !strings.HasPrefix(fleet.actors[0], "token:") {
		t.Fatalf("a resolution: %d, recorded %+v by %v", code, fleet.solved, fleet.actors)
	}
	code, body = call(http.MethodDelete, "/v1/fleet/p1", nil)
	var e Error
	_ = json.Unmarshal(body, &e)
	if code != http.StatusConflict || e.Code != CodeRetirementUnproven || len(e.Blockers) != 1 || e.Blockers[0].Code != BlockerIncarnationUnresolved ||
		e.Blockers[0].Incarnation != testInc || e.Blockers[0].Count != 2 || !strings.Contains(e.Message, "shunt proxy resolve p1 --incarnation") {
		t.Fatalf("forget with an unresolved incarnation: %d %s", code, body)
	}
	// Diagnostics name the unresolved incarnation and how to resolve it.
	code, body = call(http.MethodGet, "/v1/fleet/p1", nil)
	var diag ProxyDiagnostics
	_ = json.Unmarshal(body, &diag)
	if code != http.StatusOK || len(diag.Unresolved) != 1 {
		t.Fatalf("diagnostics: %d %s", code, body)
	}
	found := false
	for _, p := range diag.Problems {
		if strings.Contains(p, "incarnation "+testInc+" did not retire cleanly (2 outcomes unknown") && strings.Contains(p, "shunt proxy resolve p1 --incarnation "+testInc) {
			found = true
		}
	}
	if !found {
		t.Fatalf("diagnostics problems lack the unresolved incarnation: %q", diag.Problems)
	}
}
