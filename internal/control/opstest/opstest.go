// Package opstest is the transaction contract every control.Operations implementation passes
// (ADR-0021): the lab's in-memory store (internal/control) and the control plane's store on etcd
// (internal/cp) run the same cases, so neither can make independent writes look atomic.
package opstest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// Harness is one implementation under test: its operation store, and the directory it checks
// scopes against, which the contract writes through.
type Harness struct {
	Ops control.Operations
	Dir directory.Store
}

// Options size a store under test.
type Options struct {
	Limit    int // ended records kept
	Capacity int // unfinished operations allowed
}

// Run runs the contract. newHarness returns a fresh store sized by o, over an empty directory.
func Run(t *testing.T, newHarness func(t *testing.T, o Options) Harness) {
	std := Options{Limit: 100, Capacity: 100}
	t.Run("reservation", func(t *testing.T) { reservation(t, newHarness(t, std)) })
	t.Run("sequence", func(t *testing.T) { sequence(t, newHarness(t, std)) })
	t.Run("clusters", func(t *testing.T) { clusters(t, newHarness(t, std)) })
	t.Run("generation-and-lineage", func(t *testing.T) { generation(t, newHarness(t, std)) })
	t.Run("retention", func(t *testing.T) { retention(t, newHarness(t, Options{Limit: 3, Capacity: 100})) })
	t.Run("race", func(t *testing.T) { race(t, newHarness(t, std)) })
	t.Run("idempotency", func(t *testing.T) { idempotency(t, newHarness(t, Options{Limit: 2, Capacity: 100})) })
	t.Run("capacity", func(t *testing.T) { capacity(t, newHarness(t, Options{Limit: 100, Capacity: 2})) })
}

var ids atomic.Int64

// newID is a time-sortable operation id, like the server's.
func newID() string {
	n := ids.Add(1)
	return fmt.Sprintf("%013d-%06x", 1700000000000+n, n)
}

// setup writes two clusters and two placements, acme/ppp on c1 and acme/qqq on c2.
func setup(t *testing.T, h Harness) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{"c1", "c2"} {
		c := config.Cluster{Type: "s3", Scheme: "http", Region: "r", EndpointMode: "static", Endpoints: []string{"127.0.0.1:1"},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "env:SHUNT_OPSTEST_UNUSED"}} //nolint:gosec // G101: a secret_ref naming an unset variable, not a secret
		if err := h.Dir.PutCluster(ctx, name, c, "", "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Dir.Adopt(ctx, "acme", "ppp", "c1", "ppp", "test"); err != nil {
		t.Fatal(err)
	}
	if err := h.Dir.Adopt(ctx, "acme", "qqq", "c2", "qqq", "test"); err != nil {
		t.Fatal(err)
	}
}

// placementOp is a pending record reserving placement key at its current generation, touching
// clusters.
func placementOp(h Harness, key string, clusters ...string) *control.Operation {
	f := h.Dir.Snapshot().File()
	res := directory.PlacementResource(key)
	return &control.Operation{ID: newID(), Kind: control.OpRamp, Placement: key, Actor: "test", Node: "test", Identity: f.Identity,
		Scope: &control.Scope{Resource: res, Generation: f.Generation(res), Clusters: clusters}, Sequence: 1,
		Status: control.StatusPending, EffectState: control.EffectNone, AllowedActions: []string{}}
}

func clusterOp(h Harness, name string) *control.Operation {
	f := h.Dir.Snapshot().File()
	res := directory.ClusterResource(name)
	return &control.Operation{ID: newID(), Kind: control.OpClusterReadOnly, Cluster: name, Actor: "test", Node: "test", Identity: f.Identity,
		Scope: &control.Scope{Resource: res, Generation: f.Generation(res)}, Sequence: 1,
		Status: control.StatusPending, EffectState: control.EffectNone, AllowedActions: []string{}}
}

// end writes op as ended with status and effect, over its current sequence.
func end(t *testing.T, h Harness, op *control.Operation, status, effect string) {
	t.Helper()
	op.Status, op.EffectState, op.Sequence, op.Updated = status, effect, op.Sequence+1, time.Now().UTC()
	if err := h.Ops.Update(context.Background(), op); err != nil {
		t.Fatalf("ending %s: %v", op.ID, err)
	}
}

func busyOwner(err error) string {
	var busy *control.ScopeBusyError
	if errors.As(err, &busy) {
		return busy.Owner
	}
	return ""
}

// One unfinished operation owns a scope; others are refused naming it; ending it releases the
// scope; other scopes are unaffected.
func reservation(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	a := placementOp(h, "acme/ppp", "c1")
	if err := h.Ops.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	got, err := h.Ops.Get(ctx, a.ID)
	if err != nil || got == nil || got.Sequence != 1 || got.Scope == nil || got.Scope.Resource != "placement:acme/ppp" {
		t.Fatalf("get after create: %+v %v", got, err)
	}
	if err := h.Ops.Create(ctx, placementOp(h, "acme/ppp", "c1")); busyOwner(err) != a.ID {
		t.Fatalf("a second operation on acme/p: %v, want busy by %s", err, a.ID)
	}
	if err := h.Ops.Create(ctx, placementOp(h, "acme/qqq", "c2")); err != nil {
		t.Fatalf("an operation on another placement: %v", err)
	}
	unscoped := &control.Operation{ID: newID(), Kind: control.OpRamp, Status: control.StatusRunning, Sequence: 1}
	if err := h.Ops.Create(ctx, unscoped); err != nil {
		t.Fatalf("an unscoped record: %v", err)
	}
	a.Status, a.Sequence = control.StatusRunning, 2
	if err := h.Ops.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := h.Ops.Create(ctx, placementOp(h, "acme/ppp", "c1")); busyOwner(err) != a.ID {
		t.Fatalf("while running: %v, want busy by %s", err, a.ID)
	}
	end(t, h, a, control.StatusSucceeded, control.EffectCommitted)
	if err := h.Ops.Create(ctx, placementOp(h, "acme/ppp", "c1")); err != nil {
		t.Fatalf("after the owner ended: %v", err)
	}
}

// An update is accepted only over the sequence it read.
func sequence(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	op := placementOp(h, "acme/ppp", "c1")
	if err := h.Ops.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	stale := *op
	op.Status, op.Sequence = control.StatusRunning, 2
	if err := h.Ops.Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	stale.Status, stale.Sequence = control.StatusFailed, 2
	if err := h.Ops.Update(ctx, &stale); !errors.Is(err, control.ErrStaleSequence) {
		t.Fatalf("a second writer at the same sequence: %v, want ErrStaleSequence", err)
	}
	stale.Sequence = 9
	if err := h.Ops.Update(ctx, &stale); !errors.Is(err, control.ErrStaleSequence) {
		t.Fatalf("a writer ahead of the record: %v, want ErrStaleSequence", err)
	}
	if got, _ := h.Ops.Get(ctx, op.ID); got == nil || got.Status != control.StatusRunning || got.Sequence != 2 {
		t.Fatalf("after refused updates: %+v", got)
	}
	ghost := placementOp(h, "acme/qqq")
	ghost.Sequence = 2
	if err := h.Ops.Update(ctx, ghost); !errors.Is(err, control.ErrUnknownOperation) {
		t.Fatalf("update of a record never created: %v", err)
	}
}

// A cluster operation conflicts with an unfinished placement operation touching its cluster, and
// the other way round; an operation that touches another cluster does not.
func clusters(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	p := placementOp(h, "acme/ppp", "c1")
	if err := h.Ops.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := h.Ops.Create(ctx, clusterOp(h, "c1")); busyOwner(err) != p.ID {
		t.Fatalf("cluster c1 while acme/p touches it: %v, want busy by %s", err, p.ID)
	}
	c2 := clusterOp(h, "c2")
	if err := h.Ops.Create(ctx, c2); err != nil {
		t.Fatalf("cluster c2, untouched: %v", err)
	}
	if err := h.Ops.Create(ctx, placementOp(h, "acme/qqq", "c2")); busyOwner(err) != c2.ID {
		t.Fatalf("acme/q while cluster c2 is held: %v, want busy by %s", err, c2.ID)
	}
	if err := h.Ops.Create(ctx, clusterOp(h, "c2")); busyOwner(err) != c2.ID {
		t.Fatalf("cluster c2 twice: %v", err)
	}
	end(t, h, p, control.StatusFailed, control.EffectNone)
	if err := h.Ops.Create(ctx, clusterOp(h, "c1")); err != nil {
		t.Fatalf("cluster c1 after acme/p ended: %v", err)
	}
}

// A scope accepted against a generation the directory has moved past, or on another lineage, is
// refused, and reserves nothing.
func generation(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	old := placementOp(h, "acme/ppp", "c1")
	if err := h.Dir.SetPlacementReadOnly(ctx, "acme", "ppp", true, false, "", "test"); err != nil {
		t.Fatal(err)
	}
	cur := h.Dir.Snapshot().File().Generation(directory.PlacementResource("acme/ppp"))
	var ge *control.GenerationError
	if err := h.Ops.Create(ctx, old); !errors.As(err, &ge) || ge.Expected != old.Scope.Generation || ge.Current != cur {
		t.Fatalf("an operation against an old generation: %v, want generation %d → %d", err, old.Scope.Generation, cur)
	}
	other := placementOp(h, "acme/ppp", "c1")
	other.Identity.Epoch = "0123456789abcdef0123456789abcdef"
	if err := h.Ops.Create(ctx, other); control.ErrorCode(err) != control.CodeEpochMismatch {
		t.Fatalf("an operation on another epoch: %v (code %q)", err, control.ErrorCode(err))
	}
	fresh := placementOp(h, "acme/ppp", "c1")
	if err := h.Ops.Create(ctx, fresh); err != nil {
		t.Fatalf("at the current generation, after refusals that must reserve nothing: %v", err)
	}
	if err := h.Ops.Create(ctx, placementOp(h, "acme/nope")); err != nil {
		t.Fatalf("a placement that does not exist yet, at generation 0: %v", err)
	}
}

// Past the history limit the oldest ended records go; an unfinished record, or one whose effect
// is uncertain, stays however many records there are.
func retention(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	running := placementOp(h, "acme/ppp", "c1")
	if err := h.Ops.Create(ctx, running); err != nil {
		t.Fatal(err)
	}
	uncertain := placementOp(h, "acme/qqq", "c2")
	if err := h.Ops.Create(ctx, uncertain); err != nil {
		t.Fatal(err)
	}
	end(t, h, uncertain, control.StatusFailed, control.EffectUncertain)
	ended := make([]*control.Operation, 0, 6)
	for range 6 {
		op := &control.Operation{ID: newID(), Kind: control.OpRamp, Placement: "acme/r", Status: control.StatusPending, Sequence: 1}
		if err := h.Ops.Create(ctx, op); err != nil {
			t.Fatal(err)
		}
		end(t, h, op, control.StatusSucceeded, control.EffectCommitted)
		ended = append(ended, op)
	}
	waitFor(t, 10*time.Second, "the oldest ended records to go", func() bool {
		got, _ := h.Ops.Get(ctx, ended[0].ID)
		return got == nil
	})
	for _, keep := range []*control.Operation{running, uncertain, ended[len(ended)-1]} {
		if got, err := h.Ops.Get(ctx, keep.ID); err != nil || got == nil {
			t.Fatalf("record %s (%s, effect %s) did not survive the history limit: %v", keep.ID, keep.Status, keep.EffectState, err)
		}
	}
}

// Many creators race for one scope: exactly one wins, and every other is told who.
func race(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	const n = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners []string
		losers  []string
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op := placementOp(h, "acme/ppp", "c1")
			err := h.Ops.Create(ctx, op)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				winners = append(winners, op.ID)
			case busyOwner(err) != "":
				losers = append(losers, busyOwner(err))
			default:
				t.Errorf("create: %v", err)
			}
		}()
	}
	wg.Wait()
	if len(winners) != 1 {
		t.Fatalf("%d creators won one scope: %v", len(winners), winners)
	}
	for _, owner := range losers {
		if owner != winners[0] {
			t.Errorf("a loser was told %s owns the scope; %s does", owner, winners[0])
		}
	}
}

// keyed is op with an idempotency key and an intent digest.
func keyed(op *control.Operation, actor, key, digest string) *control.Operation {
	op.Actor, op.RequestID, op.IntentDigest = actor, key, digest
	return op
}

// A retry with the same key and intent gets the first record, never a second one; the same key
// with another intent is refused naming it; a key is the actor's own; and an ended record keeps
// its key, and so stays, past the history limit.
func idempotency(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	first := keyed(placementOp(h, "acme/ppp", "c1"), "alice", "k1", "intent-a")
	if err := h.Ops.Create(ctx, first); err != nil {
		t.Fatal(err)
	}
	var rp *control.IdempotentReplay
	if err := h.Ops.Create(ctx, keyed(placementOp(h, "acme/ppp", "c1"), "alice", "k1", "intent-a")); !errors.As(err, &rp) || rp.Existing.ID != first.ID {
		t.Fatalf("a retry of the same request: %v, want a replay of %s", err, first.ID)
	}
	var ic *control.IdempotencyConflictError
	if err := h.Ops.Create(ctx, keyed(placementOp(h, "acme/qqq", "c2"), "alice", "k1", "intent-b")); !errors.As(err, &ic) || ic.Owner != first.ID {
		t.Fatalf("the same key for another request: %v, want a conflict naming %s", err, first.ID)
	}
	bob := keyed(placementOp(h, "acme/qqq", "c2"), "bob", "k1", "intent-b")
	if err := h.Ops.Create(ctx, bob); err != nil {
		t.Fatalf("another actor's request with the same key: %v", err)
	}
	end(t, h, first, control.StatusSucceeded, control.EffectCommitted)
	end(t, h, bob, control.StatusSucceeded, control.EffectCommitted)
	for range 4 { // past the limit of 2
		op := &control.Operation{ID: newID(), Kind: control.OpRamp, Placement: "acme/r", Status: control.StatusPending, Sequence: 1}
		if err := h.Ops.Create(ctx, op); err != nil {
			t.Fatal(err)
		}
		end(t, h, op, control.StatusSucceeded, control.EffectNone)
	}
	if err := h.Ops.Create(ctx, keyed(placementOp(h, "acme/ppp", "c1"), "alice", "k1", "intent-a")); !errors.As(err, &rp) || rp.Existing.ID != first.ID || rp.Existing.Status != control.StatusSucceeded {
		t.Fatalf("a retry after the record ended and the limit was passed: %v, want a replay of the ended %s", err, first.ID)
	}
}

// At the limit on unfinished operations a new one is refused before it reserves anything; one
// ending makes room.
func capacity(t *testing.T, h Harness) {
	setup(t, h)
	ctx := context.Background()
	a, b := placementOp(h, "acme/ppp", "c1"), placementOp(h, "acme/qqq", "c2")
	for _, op := range []*control.Operation{a, b} {
		if err := h.Ops.Create(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	var ce *control.CapacityError
	if err := h.Ops.Create(ctx, placementOp(h, "acme/rrr")); !errors.As(err, &ce) || ce.Limit != 2 {
		t.Fatalf("a third operation at capacity 2: %v", err)
	}
	end(t, h, a, control.StatusSucceeded, control.EffectNone)
	if err := h.Ops.Create(ctx, placementOp(h, "acme/rrr")); err != nil {
		t.Fatalf("after one ended: %v", err)
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
