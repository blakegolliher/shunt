package cp

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// waitFor polls until cond holds or the deadline passes.
func waitFor(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: still not so after %s", what, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func members(t *testing.T, f *Fleet) map[string]control.Member {
	t.Helper()
	ms, err := f.Members(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]control.Member{}
	for _, m := range ms {
		out[m.ID] = m
	}
	return out
}

// A member joins with its first heartbeat, is live while it heartbeats through any node, falls
// silent when its lease expires, stays a member until forgotten, and cannot be forgotten while live.
func TestFleetLeasesAndMembership(t *testing.T) {
	tc := startCluster(t, 2)
	ctx := context.Background()
	fa, fb := NewFleet(tc.nodes[0].Client(), time.Second), NewFleet(tc.nodes[1].Client(), time.Second)
	fa.DropMargin, fb.DropMargin = 500*time.Millisecond, 500*time.Millisecond

	window := &telemetry.Window{Start: time.Now().Add(-20 * time.Second), End: time.Now().Add(-10 * time.Second)}
	g, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 1, Applied: 3, FallbackReads: map[string]float64{"acme/data": 2},
		Host: "host-a", Version: "v1", Telemetry: window, Incarnation: inc1})
	if err != nil || g.LeaseTTL != time.Second || g.Retire {
		t.Fatalf("heartbeat: %+v %v", g, err)
	}
	ms := members(t, fa)
	if m := ms["p1"]; !m.Live || m.Applied != 3 || m.Seq != 1 || m.FallbackReads["acme/data"] != 2 || m.Seen.IsZero() ||
		m.Host != "host-a" || m.Version != "v1" || m.Telemetry == nil || !m.Telemetry.Start.Equal(window.Start) ||
		m.Incarnation == nil || m.Incarnation.ID != inc1 || m.Incarnation.State != control.IncarnationActive {
		t.Fatalf("after one heartbeat: %+v", m)
	}
	// Heartbeats may arrive at another node: it renews the same member.
	if _, err := fb.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 2, Applied: 4, Incarnation: inc1}); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fb)["p1"]; !m.Live || m.Applied != 4 || m.Seq != 2 {
		t.Fatalf("after a heartbeat through node b: %+v", m)
	}
	if err := fa.Forget(ctx, "p1"); err == nil {
		t.Error("forgot a live member")
	}
	// Silence: the lease expires, the member stays, and its incarnation is still active: a process
	// that fell silent may still be running, and its work may still land.
	waitFor(t, 5*time.Second, "p1 to fall silent", func() bool { m, ok := members(t, fa)["p1"]; return ok && !m.Live })
	if m := members(t, fb)["p1"]; m.Live || m.Applied != 0 || m.Incarnation == nil || m.Incarnation.State != control.IncarnationActive {
		t.Fatalf("a silent member keeps no heartbeat data, and its incarnation: %+v", m)
	}
	var unproven *control.RetirementError
	if err := fa.Forget(ctx, "p1"); !errors.As(err, &unproven) || len(unproven.Incarnations) != 1 || unproven.Incarnations[0].ID != inc1 {
		t.Fatalf("forgetting a silent member whose process never retired: %v, want retirement_unproven naming %s", err, inc1)
	}
	// It comes back, is live again, retires cleanly, and is forgotten.
	if _, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 3, Applied: 4, Incarnation: inc1}); err != nil {
		t.Fatal(err)
	}
	if !members(t, fa)["p1"].Live {
		t.Fatal("a returning member is not live")
	}
	if err := fb.Retire(ctx, "p1", inc1, 0); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fa)["p1"]; m.Live || m.Incarnation == nil || m.Incarnation.State != control.IncarnationRetired || m.Incarnation.Ended.IsZero() {
		t.Fatalf("after a clean retirement: %+v", m)
	}
	if err := fb.Retire(ctx, "p1", inc1, 0); err != nil {
		t.Fatalf("a retried retirement: %v", err)
	}
	if err := fa.Forget(ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := members(t, fa)["p1"]; ok {
		t.Fatal("forgotten member still listed")
	}
	if err := fa.Forget(ctx, "p1"); !errors.Is(err, directory.ErrNotFound) {
		t.Errorf("forgetting twice: %v", err)
	}
	if _, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 4, Applied: 4, Incarnation: inc2}); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fb)["p1"]; !m.Live || m.Incarnation == nil || m.Incarnation.ID != inc2 {
		t.Fatalf("a forgotten member that comes back did not re-join: %+v", m)
	}
}

const (
	inc1 = "11111111111111111111111111111111"
	inc2 = "22222222222222222222222222222222"
	inc3 = "33333333333333333333333333333333"
	inc4 = "44444444444444444444444444444444"
)

// The incarnation protocol on etcd (ADR-0021 D2, T06): a new process registering without its
// predecessor's retirement leaves that one unresolved; a retirement with outcomes unknown is
// unresolved too; a late clean retirement discharges one; an operator's attestation resolves one;
// forget refuses while any is there; the previous incarnation a proxy reports from its own marker
// is recorded once; a retire request reaches the next heartbeat; the unresolved bound refuses a
// new registration rather than dropping evidence.
func TestFleetIncarnations(t *testing.T) {
	tc := startCluster(t, 2)
	ctx := context.Background()
	fa, fb := NewFleet(tc.nodes[0].Client(), time.Second), NewFleet(tc.nodes[1].Client(), time.Second)
	fa.DropMargin, fb.DropMargin = 500*time.Millisecond, 500*time.Millisecond
	if _, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 1, Incarnation: inc1}); err != nil {
		t.Fatal(err)
	}
	// A crash: inc2 registers through the other node with no retirement of inc1 on record.
	if _, err := fb.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 1, Incarnation: inc2}); err != nil {
		t.Fatal(err)
	}
	m := members(t, fa)["p1"]
	if m.Incarnation == nil || m.Incarnation.ID != inc2 || len(m.Unresolved) != 1 || m.Unresolved[0].ID != inc1 || m.Unresolved[0].State != control.IncarnationUnclean || m.Unresolved[0].Ended.IsZero() {
		t.Fatalf("after a replacement without retirement: %+v", m)
	}
	// inc1's heartbeat through node a (its cache still says inc1 is current) does not revive it.
	if _, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 2, Incarnation: inc2}); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fb)["p1"]; m.Incarnation.ID != inc2 || len(m.Unresolved) != 1 {
		t.Fatalf("after node a caught up: %+v", m)
	}
	// A late, clean retirement of inc1 (its reply was lost, it retried) discharges it.
	if err := fa.Retire(ctx, "p1", inc1, 0); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fa)["p1"]; len(m.Unresolved) != 0 || m.Incarnation.ID != inc2 {
		t.Fatalf("after inc1's late clean retirement: %+v", m)
	}
	// inc2 retires with outcomes unknown: unresolved until an operator attests.
	if err := fb.Retire(ctx, "p1", inc2, 3); err != nil {
		t.Fatal(err)
	}
	m = members(t, fa)["p1"]
	if m.Live || m.Incarnation != nil || len(m.Unresolved) != 1 || m.Unresolved[0].ID != inc2 || m.Unresolved[0].Uncertain != 3 {
		t.Fatalf("after an unclean retirement: %+v", m)
	}
	var unproven *control.RetirementError
	if err := fa.Forget(ctx, "p1"); !errors.As(err, &unproven) || unproven.Incarnations[0].ID != inc2 {
		t.Fatalf("forget with an unresolved incarnation: %v", err)
	}
	if err := fa.Resolve(ctx, "p1", inc1, "already clean", "token:1"); err == nil {
		t.Fatal("resolved an incarnation that is not unresolved")
	}
	if err := fa.Resolve(ctx, "p1", inc2, "host decommissioned 2026-09-24, backend shows no requests from it", "token:1"); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fb)["p1"]; len(m.Unresolved) != 0 {
		t.Fatalf("after resolving: %+v", m)
	}
	// The next process reports what its marker said of a predecessor the control plane never saw.
	prev := &control.Incarnation{ID: inc3, State: control.IncarnationUnclean, Uncertain: 1, Started: time.Now().Add(-time.Hour)}
	g, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 1, Incarnation: inc4, Previous: prev})
	if err != nil || !g.PreviousRecorded {
		t.Fatalf("a heartbeat with a previous incarnation: %+v %v", g, err)
	}
	m = members(t, fa)["p1"]
	if len(m.Unresolved) != 1 || m.Unresolved[0].ID != inc3 || m.Unresolved[0].Uncertain != 1 {
		t.Fatalf("the previous incarnation the proxy reported: %+v", m)
	}
	if g, err := fb.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 2, Incarnation: inc4, Previous: prev}); err != nil || !g.PreviousRecorded {
		t.Fatalf("the previous incarnation reported again: %+v %v", g, err)
	}
	if m := members(t, fa)["p1"]; len(m.Unresolved) != 1 {
		t.Fatalf("a previous incarnation recorded twice: %+v", m)
	}
	// An operator asks the proxy to retire: its next heartbeat says so.
	if err := fb.RequestRetire(ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	if g, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 3, Incarnation: inc4}); err != nil || !g.Retire {
		t.Fatalf("after a retire request: %+v %v", g, err)
	}
	if err := fa.Retire(ctx, "p1", inc4, 0); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fa)["p1"]; m.RetireRequested || m.Incarnation.State != control.IncarnationRetired {
		t.Fatalf("after retiring on request: %+v", m)
	}
	// A crashed process that was never replaced can be resolved once its lease is gone, not before.
	if _, err := fa.Heartbeat(ctx, "p2", control.Heartbeat{Seq: 1, Incarnation: inc1}); err != nil {
		t.Fatal(err)
	}
	if err := fa.Resolve(ctx, "p2", inc1, "x", "token:1"); err == nil {
		t.Fatal("resolved a live incarnation")
	}
	waitFor(t, 5*time.Second, "p2 to fall silent", func() bool { return !members(t, fa)["p2"].Live })
	if err := fa.Resolve(ctx, "p2", inc1, "host is off", "token:1"); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fa)["p2"]; m.Incarnation != nil || len(m.Unresolved) != 0 {
		t.Fatalf("after resolving a crashed current incarnation: %+v", m)
	}
	if err := fa.Forget(ctx, "p2"); err != nil {
		t.Fatalf("forget after resolving: %v", err)
	}
	// The bound: eight unresolved incarnations refuse a ninth registration.
	for i := range control.MaxUnresolvedIncarnations + 1 {
		id := fmt.Sprintf("%032x", i+0x100)
		_, err := fa.Heartbeat(ctx, "p3", control.Heartbeat{Seq: 1, Incarnation: id})
		switch {
		case i <= control.MaxUnresolvedIncarnations && err != nil:
			t.Fatalf("registration %d: %v", i, err)
		}
	}
	if _, err := fa.Heartbeat(ctx, "p3", control.Heartbeat{Seq: 1, Incarnation: fmt.Sprintf("%032x", 0x200)}); !errors.As(err, &unproven) || len(unproven.Incarnations) != control.MaxUnresolvedIncarnations {
		t.Fatalf("registration past the unresolved bound: %v", err)
	}
}

// Defect 5 of the H2 review. A proxy that retired cleanly while the control plane was unreachable
// says so through its marker on the next process's first heartbeat. Its record must retire that
// incarnation, not take it for a crash: no unresolved incarnation blocks barriers, and once the
// new process retires too, forget is allowed. A marker with uncertain work stays unresolved.
func TestFleetCleanRetirementFromMarker(t *testing.T) {
	tc := startCluster(t, 1)
	ctx := context.Background()
	f := NewFleet(tc.nodes[0].Client(), time.Second)
	f.DropMargin = 500 * time.Millisecond
	if _, err := f.Heartbeat(ctx, "p5", control.Heartbeat{Seq: 1, Incarnation: inc1}); err != nil {
		t.Fatal(err)
	}
	// inc1 drained and wrote its retired marker; its Retire call never reached the control plane.
	prev := &control.Incarnation{ID: inc1, State: control.IncarnationRetired, Started: time.Now().Add(-time.Hour), Ended: time.Now().Add(-time.Minute)}
	g, err := f.Heartbeat(ctx, "p5", control.Heartbeat{Seq: 1, Incarnation: inc2, Previous: prev})
	if err != nil || !g.PreviousRecorded {
		t.Fatalf("the restarted process's first heartbeat: %+v %v", g, err)
	}
	m := members(t, f)["p5"]
	if len(m.Unresolved) != 0 || m.Incarnation == nil || m.Incarnation.ID != inc2 || m.Incarnation.State != control.IncarnationActive {
		t.Fatalf("a clean retirement reported by the marker was recorded as unclean: %+v", m)
	}
	if err := f.Retire(ctx, "p5", inc2, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Forget(ctx, "p5"); err != nil {
		t.Fatalf("forget after both processes retired cleanly: %v", err)
	}

	// A marker that says retired with work whose outcome is unknown is not proof: unresolved.
	if _, err := f.Heartbeat(ctx, "p6", control.Heartbeat{Seq: 1, Incarnation: inc3}); err != nil {
		t.Fatal(err)
	}
	unsure := &control.Incarnation{ID: inc3, State: control.IncarnationRetired, Uncertain: 2}
	if _, err := f.Heartbeat(ctx, "p6", control.Heartbeat{Seq: 1, Incarnation: inc4, Previous: unsure}); err != nil {
		t.Fatal(err)
	}
	m = members(t, f)["p6"]
	if len(m.Unresolved) != 1 || m.Unresolved[0].ID != inc3 || m.Unresolved[0].State != control.IncarnationUnclean || m.Unresolved[0].Uncertain != 2 {
		t.Fatalf("a marker with uncertain work: %+v", m)
	}
}
