package cp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
)

// The operation records on etcd (ADR-0017): a record written on one node is read on another at
// once, every node's watch reports every change once, and the oldest records past the retention
// go away.

func openOps(t testing.TB, tc *testCluster, i int, retention int) *Operations {
	t.Helper()
	ops := NewOperations(tc.nodes[i].Client())
	ops.Retention = retention
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ops.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ops.Close)
	return ops
}

func record(n int, placement string) *control.Operation {
	return &control.Operation{ID: fmt.Sprintf("%013d-%06x", 1700000000000+int64(n), n), Kind: control.OpRamp, Placement: placement,
		Actor: "test", Node: "c1", Status: control.StatusRunning, Phase: control.PhaseQueued, Sequence: 1}
}

// ended is record n, ended: what the history limit may drop.
func ended(t testing.TB, ops *Operations, n int, placement string) {
	t.Helper()
	op := record(n, placement)
	if err := ops.Create(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	op.Status, op.Phase, op.EffectState, op.Sequence = control.StatusSucceeded, control.PhaseDone, control.EffectNone, 2
	if err := ops.Update(context.Background(), op); err != nil {
		t.Fatal(err)
	}
}

func TestOperationsAcrossNodesAndRetention(t *testing.T) {
	tc := startCluster(t, 2)
	ctx := context.Background()
	a := openOps(t, tc, 0, 5)
	var (
		mu   sync.Mutex
		seen []string
	)
	b := NewOperations(tc.nodes[1].Client())
	b.OnChange = func(op control.Operation) {
		mu.Lock()
		seen = append(seen, op.ID+":"+op.Status)
		mu.Unlock()
	}
	b.Retention = 5
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)

	// A record written on a is readable on b at once, before b's watch has delivered it.
	first := record(1, "acme/data01")
	if err := a.Create(ctx, first); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get(ctx, first.ID)
	if err != nil || got == nil || got.Placement != "acme/data01" || got.Status != control.StatusRunning {
		t.Fatalf("get on the other node: %+v %v", got, err)
	}
	if missing, err := b.Get(ctx, "0000000000000-000000"); err != nil || missing != nil {
		t.Fatalf("get of a record that does not exist: %+v %v", missing, err)
	}

	// An update on a reaches b's OnChange exactly once, with the new status.
	first.Status, first.Phase, first.Sequence = control.StatusSucceeded, control.PhaseDone, 2
	if err := a.Update(ctx, first); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "b's OnChange did not see the update once", func() bool {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, s := range seen {
			if s == first.ID+":"+control.StatusSucceeded {
				n++
			}
		}
		return n == 1
	})

	// Seven more ended records: only the newest five remain, on both nodes, newest first.
	for i := 2; i <= 8; i++ {
		ended(t, a, i, "acme/logs")
	}
	ended(t, a, 9, "acme/logs") // the history limit is applied on a create
	waitFor(t, 5*time.Second, "retention did not leave five records on both nodes", func() bool {
		la, _ := a.List(ctx, "", "", 100)
		lb, _ := b.List(ctx, "", "", 100)
		return len(la) == 5 && len(lb) == 5
	})
	la, _ := a.List(ctx, "", "", 100)
	if la[0].ID != record(9, "").ID || la[4].ID != record(5, "").ID {
		t.Fatalf("listing order: %s … %s", la[0].ID, la[4].ID)
	}
	if got, _ := a.Get(ctx, first.ID); got != nil {
		t.Fatalf("the oldest record survived retention: %+v", got)
	}
	if byPlacement, _ := b.List(ctx, "acme/logs", "", 2); len(byPlacement) != 2 || byPlacement[0].Placement != "acme/logs" {
		t.Fatalf("listing by placement: %+v", byPlacement)
	}
	if none, _ := b.List(ctx, "acme/none", "", 10); len(none) != 0 {
		t.Fatalf("listing an unknown placement: %+v", none)
	}
}

func BenchmarkOperationsCreateUpdate(b *testing.B) {
	tc := startCluster(b, 1)
	ops := openOps(b, tc, 0, 0)
	b.ResetTimer()
	for i := range b.N {
		ended(b, ops, i, "acme/data01")
	}
}
