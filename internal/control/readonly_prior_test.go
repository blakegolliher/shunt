package control

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// A read-only change canceled before its commit puts back the switch it replaced (third review,
// R3-09), on a cluster and on a placement: a repeated switch-on leaves the scope read-only; a change
// of reject mode goes back to the mode before it, either way; a switch-on of a writable scope leaves
// it writable. The switch it replaced is on the durable record before the hold is written, so any
// node's cancellation restores it; the record ends canceled, with no effect. Negative control:
// releasing the hold to writable, as before, fails every case whose scope was read-only.
func TestCanceledReadOnlyRestoresThePriorSwitch(t *testing.T) {
	type sw struct{ readOnly, reject bool }
	cases := []struct {
		name         string
		prior, asked sw
	}{
		{"a repeated switch-on", sw{true, false}, sw{true, false}},
		{"retryable to reject", sw{true, false}, sw{true, true}},
		{"reject to retryable", sw{true, true}, sw{true, false}},
		{"writable to read-only", sw{false, false}, sw{true, false}},
	}
	for _, scope := range []string{"cluster", "placement"} {
		for _, c := range cases {
			t.Run(scope+": "+c.name, func(t *testing.T) {
				rg := newRig(t)
				rg.prepare()
				path := "/v1/clusters/vast01/read-only"
				read := func() sw {
					f := rg.dir.Snapshot().File()
					if scope == "cluster" {
						cl := f.Clusters["vast01"]
						return sw{cl.ReadOnly, cl.RejectWrites}
					}
					p := f.Placements["acme/data01"]
					return sw{p.ReadOnly, p.RejectWrites}
				}
				if scope == "placement" {
					path = "/v1/placements/acme/data01/read-only"
				}
				if c.prior.readOnly {
					rg.must("POST", path, ReadOnlyRequest{ReadOnly: true, Reject: c.prior.reject}, nil)
				}
				if got := read(); got != c.prior {
					t.Fatalf("before: %+v, want %+v", got, c.prior)
				}
				updates := operationUpdates(rg)
				sleep, wake := wakingSleep()
				rg.ctl.Sleep, rg.ctl.FencePoll = sleep, 5*time.Millisecond
				snap := rg.dir.Snapshot()
				fleet := &barrierFleet{} // a silent member: the change holds and cannot drain
				fleet.set(Member{ID: "silent", Live: false, Identity: snap.Identity(), Applied: snap.Version(), Durable: snap.Version(),
					Incarnation: &Incarnation{ID: testInc, State: IncarnationActive}})
				rg.ctl.Fleet = fleet
				var op Operation
				if code, raw := rg.call("POST", path, ReadOnlyRequest{ReadOnly: c.asked.readOnly, Reject: c.asked.reject, Wait: "0s"}, &op); code != http.StatusAccepted {
					t.Fatalf("the change: %d %s", code, raw)
				}
				waitOperation(t, updates, op.ID, func(o Operation) bool { return o.Status == StatusBlocked })
				stored, err := rg.ctl.ops().Get(context.Background(), op.ID)
				if err != nil || stored.Barrier == nil || stored.Barrier.Prior == nil ||
					*stored.Barrier.Prior != (ReadOnlyPrior{ReadOnly: c.prior.readOnly, Reject: c.prior.reject}) {
					t.Fatalf("the switch it replaces is not on the durable record: %+v %v", stored.Barrier, err)
				}
				var ended Operation
				rg.must("POST", "/v1/operations/"+op.ID+"/cancel", struct{}{}, &ended)
				wake()
				waitNotRunning(t, rg.ctl, op.ID)
				if ended.Status != StatusCancelled || ended.EffectState != EffectNone {
					t.Fatalf("the canceled record: %s effect %s", ended.Status, ended.EffectState)
				}
				if got := read(); got != c.prior {
					t.Fatalf("after the cancellation: %+v, want the switch it replaced, %+v", got, c.prior)
				}
			})
		}
	}
}
