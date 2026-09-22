package cp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
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

	ttl, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 1, Applied: 3, FallbackReads: map[string]float64{"acme/data": 2}})
	if err != nil || ttl != time.Second {
		t.Fatalf("heartbeat: %v %v", ttl, err)
	}
	ms := members(t, fa)
	if m := ms["p1"]; !m.Live || m.Applied != 3 || m.Seq != 1 || m.FallbackReads["acme/data"] != 2 || m.Seen.IsZero() {
		t.Fatalf("after one heartbeat: %+v", m)
	}
	// Heartbeats may arrive at another node: it renews the same member.
	if _, err := fb.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 2, Applied: 4}); err != nil {
		t.Fatal(err)
	}
	if m := members(t, fb)["p1"]; !m.Live || m.Applied != 4 || m.Seq != 2 {
		t.Fatalf("after a heartbeat through node b: %+v", m)
	}
	if err := fa.Forget(ctx, "p1"); err == nil {
		t.Error("forgot a live member")
	}
	// Silence: the lease expires, the member stays.
	waitFor(t, 5*time.Second, "p1 to fall silent", func() bool { m, ok := members(t, fa)["p1"]; return ok && !m.Live })
	if m := members(t, fb)["p1"]; m.Live || m.Applied != 0 {
		t.Fatalf("a silent member keeps no heartbeat data: %+v", m)
	}
	// It comes back, is live again, then is forgotten once silent.
	if _, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 3, Applied: 4}); err != nil {
		t.Fatal(err)
	}
	if !members(t, fa)["p1"].Live {
		t.Fatal("a returning member is not live")
	}
	waitFor(t, 5*time.Second, "p1 to fall silent again", func() bool { return !members(t, fa)["p1"].Live })
	if err := fa.Forget(ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := members(t, fa)["p1"]; ok {
		t.Fatal("forgotten member still listed")
	}
	if err := fa.Forget(ctx, "p1"); !errors.Is(err, directory.ErrNotFound) {
		t.Errorf("forgetting twice: %v", err)
	}
	if _, err := fa.Heartbeat(ctx, "p1", control.Heartbeat{Seq: 4, Applied: 4}); err != nil {
		t.Fatal(err)
	}
	if !members(t, fb)["p1"].Live {
		t.Fatal("a forgotten member that comes back did not re-join")
	}
}
