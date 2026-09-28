package control

import (
	"context"
	"net/http"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// joinPhases reads shunt_control_join_phase for every phase.
func joinPhases(t *testing.T, s *Server) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, p := range MembershipPhases {
		m := &dto.Metric{}
		if err := s.Metrics.JoinPhase.WithLabelValues(p).Write(m); err != nil {
			t.Fatal(err)
		}
		out[p] = m.GetGauge().GetValue()
	}
	return out
}

// shunt_control_join_phase follows the unfinished membership change: 1 for the phase a join waits
// in, and only that one, while its node has not started; all 0 once a cancel has ended it. Negative
// control: without the sweep publishing it, the gauge never leaves 0.
func TestSweepPublishesTheMembershipPhase(t *testing.T) {
	rg, _, srv := joinRig(t)
	ctx := context.Background()
	if err := rg.ctl.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	for p, v := range joinPhases(t, rg.ctl) {
		if v != 0 {
			t.Fatalf("no membership change: %s = %v", p, v)
		}
	}
	var op Operation
	if code, _, raw := joinCall(t, srv, "/v1/control/members", "join-phase", JoinRequest{Name: "c2", PeerURL: "http://127.0.0.1:9962"}, &op); code != http.StatusAccepted {
		t.Fatalf("join: %d %s", code, raw)
	}
	waitJoin(t, rg, op.ID, "waiting for the node", func(o *Operation) bool { return o.Phase == PhaseLearnerAdded })
	if err := rg.ctl.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	for p, v := range joinPhases(t, rg.ctl) {
		if want := map[bool]float64{true: 1, false: 0}[p == PhaseLearnerAdded]; v != want {
			t.Fatalf("a join waiting for its node: %s = %v, want %v", p, v, want)
		}
	}
	if code, raw := cancelJoin(t, srv, op.ID); code >= 300 {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	waitJoin(t, rg, op.ID, "canceled", func(o *Operation) bool { return o.Terminal() })
	if err := rg.ctl.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	for p, v := range joinPhases(t, rg.ctl) {
		if v != 0 {
			t.Fatalf("after the join was canceled: %s = %v", p, v)
		}
	}
}
