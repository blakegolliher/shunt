package cp

import (
	"context"
	"errors"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/control/opstest"
	"github.com/blakegolliher/shunt/internal/directory"
)

// The control plane's store on etcd passes the same operation transaction contract as the lab's.
func TestOperationsContract(t *testing.T) {
	opstest.Run(t, func(t *testing.T, o opstest.Options) opstest.Harness {
		tc := startCluster(t, 1)
		store := openStore(t, tc, 0, make([]byte, 32))
		ops := openOps(t, tc, 0, o.Limit)
		ops.Capacity = o.Capacity
		return opstest.Harness{Ops: ops, Dir: store}
	})
}

// A create blocked by an operation whose owner node is no longer live ends that operation as
// orphaned, its effect uncertain, and takes the scope; one whose owner is live keeps it.
func TestOperationsReapALostOwner(t *testing.T) {
	tc := startCluster(t, 1)
	store := openStore(t, tc, 0, make([]byte, 32))
	ctx := context.Background()
	c := config.Cluster{Type: "s3", Scheme: "http", Region: "r", EndpointMode: "static", Endpoints: []string{"127.0.0.1:1"},
		Credentials: config.Credentials{AccessKey: "AK", SecretRef: "env:UNUSED"}}
	if err := store.PutCluster(ctx, "c1", c, "", "t"); err != nil {
		t.Fatal(err)
	}
	if err := store.Adopt(ctx, "acme", "data", "c1", "data", "t"); err != nil {
		t.Fatal(err)
	}
	ops := NewOperations(tc.nodes[0].Client())
	ops.Node = "n2"
	if err := ops.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ops.Close)
	f := store.Snapshot().File()
	scoped := func(n int, node string) *control.Operation {
		op := record(n, "acme/data")
		op.Node, op.Identity = node, f.Identity
		op.Scope = &control.Scope{Resource: directory.PlacementResource("acme/data"), Generation: f.Generation(directory.PlacementResource("acme/data")), Clusters: []string{"c1"}}
		return op
	}
	ghost := scoped(1, "ghost") // a node that has no liveness key: gone
	if err := ops.Create(ctx, ghost); err != nil {
		t.Fatal(err)
	}
	mine := scoped(2, "n2")
	if err := ops.Create(ctx, mine); err != nil {
		t.Fatalf("a create over a lost owner's scope: %v", err)
	}
	got, err := ops.Get(ctx, ghost.ID)
	if err != nil || got == nil || got.Status != control.StatusFailed || got.EffectState != control.EffectUncertain || got.Error == nil || got.Error.Code != "unavailable" {
		t.Fatalf("the lost owner's operation: %+v %v", got, err)
	}
	var busy *control.ScopeBusyError
	if err := ops.Create(ctx, scoped(3, "n2")); !errors.As(err, &busy) || busy.Owner != mine.ID {
		t.Fatalf("a create over a live owner's scope: %v, want busy by %s", err, mine.ID)
	}
}
