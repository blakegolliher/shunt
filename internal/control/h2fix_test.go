package control

// Regressions for the blocking defects of the H2 review (summarized in docs/prompts/h2-handoffs.md). Each
// one left an operation blocked with no way out, or reopened admission under destructive work.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/directory"
)

// lostCommit is one barrier kind for the lost-commit regression: how to start it, and the
// directory write its commit makes, done by the test as the owner would have before it was lost.
type lostCommit struct {
	name   string
	req    OperationRequest
	scope  string
	commit func(t *testing.T, rg *rig, id string)
	check  func(t *testing.T, rg *rig)
}

func lostCommitKinds() []lostCommit {
	placement := func(rg *rig) directory.Placement {
		p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
		return *p
	}
	step := func(tr directory.Transition) func(t *testing.T, rg *rig, id string) {
		return func(t *testing.T, rg *rig, id string) {
			t.Helper()
			tr.Complete, tr.Barrier = true, id
			if err := rg.dir.SetState(context.Background(), "acme", "data01", placement(rg).State, tr, "owner"); err != nil {
				t.Fatalf("the owner's commit: %v", err)
			}
		}
	}
	clear := func(scope string) func(t *testing.T, rg *rig, id string) {
		return func(t *testing.T, rg *rig, id string) {
			t.Helper()
			if err := rg.dir.ClearBarrier(context.Background(), scope, id, "owner"); err != nil {
				t.Fatalf("the owner's commit: %v", err)
			}
		}
	}
	return []lostCommit{
		{name: "ramp", scope: directory.PlacementResource("acme/data01"),
			req:    OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":0.5,"wait":"0s"}`)},
			commit: step(directory.Transition{To: directory.StateRamping, Ratio: 0.5}),
			check: func(t *testing.T, rg *rig) {
				if p := placement(rg); p.Held() || p.Barrier != nil || p.Ramp == nil || p.Ramp.Ratio != 0.5 {
					t.Fatalf("ramp after the resumed commit: %+v", p)
				}
			}},
		{name: "migrate", scope: directory.PlacementResource("acme/data01"),
			req:    OperationRequest{Kind: OpMigrate, Placement: "acme/data01", Args: json.RawMessage(`{"wait":"0s"}`)},
			commit: step(directory.Transition{To: directory.StateMigrating}),
			check: func(t *testing.T, rg *rig) {
				if p := placement(rg); p.State != directory.StateMigrating || p.Held() || p.Barrier != nil {
					t.Fatalf("migrate after the resumed commit: %+v", p)
				}
			}},
		{name: "placement-read-only", scope: directory.PlacementResource("acme/data01"),
			req:    OperationRequest{Kind: OpPlacementReadOnly, Placement: "acme/data01", Args: json.RawMessage(`{"read_only":true,"wait":"0s"}`)},
			commit: clear(directory.PlacementResource("acme/data01")),
			check: func(t *testing.T, rg *rig) {
				if p := placement(rg); !p.ReadOnly || p.Barrier != nil {
					t.Fatalf("placement read-only after the resumed commit: %+v", p)
				}
			}},
		{name: "cluster-read-only", scope: directory.ClusterResource("vast01"),
			req:    OperationRequest{Kind: OpClusterReadOnly, Cluster: "vast01", Args: json.RawMessage(`{"read_only":true,"wait":"0s"}`)},
			commit: clear(directory.ClusterResource("vast01")),
			check: func(t *testing.T, rg *rig) {
				if c := rg.dir.Snapshot().File().Clusters["vast01"]; !c.ReadOnly || c.Barrier != nil {
					t.Fatalf("cluster read-only after the resumed commit: %+v", c)
				}
			}},
	}
}

// Defect 1. The owner drains, writes its commit to the directory, and is lost before the record
// says Committed. The scope no longer carries the barrier, so no proxy can acknowledge it: a
// resumed owner that drained again would wait on install_pending forever. It must find the commit
// and end succeeded with Committed.
func TestResumeAfterLostCommitRecordSucceeds(t *testing.T) {
	for _, k := range lostCommitKinds() {
		t.Run(k.name, func(t *testing.T) {
			rg := newRig(t)
			rg.prepare()
			updates := operationUpdates(rg)
			ctx, stopOwner := context.WithCancel(context.Background())
			rg.ctl.Ctx = ctx
			sleep, wake := wakingSleep()
			rg.ctl.Sleep, rg.ctl.FencePoll = sleep, 5*time.Millisecond
			// A live member that has the current version and has not acknowledged the hold yet:
			// the owner blocks in the drain, with its hold durable.
			fleet := &barrierFleet{}
			member := func() Member {
				snap := rg.dir.Snapshot()
				return Member{ID: "p1", Live: true, Identity: snap.File().Identity, Applied: snap.Version(), Installed: snap.Version(), Durable: snap.Version(),
					Incarnation: &Incarnation{ID: testInc, State: IncarnationActive}}
			}
			fleet.set(member())
			rg.ctl.Fleet = fleet

			var started Operation
			if code, raw := rg.call(http.MethodPost, "/v1/operations", k.req, &started); code != http.StatusAccepted {
				t.Fatalf("starting %s: %d %s", k.name, code, raw)
			}
			held := waitOperation(t, updates, started.ID, func(op Operation) bool {
				return op.Status == StatusBlocked && op.Barrier != nil && op.Barrier.HoldVersion > 0 && op.Phase == PhaseDrain
			})

			// The owner is lost: its record goes to a dead node, its process stops.
			mem := rg.ctl.ops().(*MemOperations)
			taken, err := mem.Get(t.Context(), started.ID)
			if err != nil || taken == nil {
				t.Fatalf("reading the operation: %+v %v", taken, err)
			}
			taken.Node = "lost-node"
			taken.Sequence++
			if err := mem.Update(t.Context(), taken); err != nil {
				t.Fatal(err)
			}
			stopOwner()
			wake()
			waitNotRunning(t, rg.ctl, started.ID)

			// Its commit had reached the directory; the record never said so.
			k.commit(t, rg, held.ID)
			fleet.set(member()) // the member installs the commit: no ack of the barrier remains

			mem.Node = "replacement"
			replacement := &Server{Dir: rg.dir, Clusters: rg.ctl.Clusters, Metrics: rg.ctl.Metrics, Log: rg.ctl.Log, Fleet: fleet, Ops: mem, Node: "replacement",
				Now: rg.ctl.Now, Sleep: func(context.Context, time.Duration) error { return nil }, FencePoll: time.Millisecond}
			if err := replacement.Sweep(t.Context()); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/operations/"+started.ID+"/resume", nil)
			req.RemoteAddr = "127.0.0.1:1234"
			rec := httptest.NewRecorder()
			replacement.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("resume: HTTP %d %s", rec.Code, rec.Body.String())
			}
			done := waitOperation(t, updates, started.ID, func(op Operation) bool { return op.Terminal() })
			if done.Status != StatusSucceeded || done.Barrier == nil || !done.Barrier.Committed || done.Node != "replacement" || done.OwnerTerm != 1 {
				t.Fatalf("resumed %s after its lost commit record: %+v blockers %+v", k.name, done, done.Blockers)
			}
			k.check(t, rg)
		})
	}
}

// latchedOps pauses the first Get after it is armed until release returns: a cancellation that
// has read the record and not yet acted on it.
type latchedOps struct {
	*MemOperations
	armed   atomic.Bool
	release func()
}

func (o *latchedOps) Get(ctx context.Context, id string) (*Operation, error) {
	op, err := o.MemOperations.Get(ctx, id)
	if o.armed.CompareAndSwap(true, false) {
		o.release()
	}
	return op, err
}

// Defect 2 (T09, "attempt cancel on newer hold"). A cancellation reads the record before purge
// marks its irreversible point; the owner then writes DispatchStarted and reaches its first
// delete. The cancellation must not reopen the source under the delete: it is refused, the
// source barrier stays, and the purge completes.
func TestCancelCannotReleaseHoldUnderPurgeDispatch(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.cutOver()
	var dry PurgeDryRun
	rg.must(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	gates := admission.New()
	previousInstall := rg.dir.OnInstall
	rg.dir.OnInstall = func(snap *directory.Snapshot) {
		closures, _ := admission.Barriers(snap)
		gates.Apply(closures, false)
		previousInstall(snap)
	}
	rg.ctl.LocalGates = gates
	sleep, wake := wakingSleep()
	rg.ctl.Sleep, rg.ctl.FencePoll = sleep, 5*time.Millisecond
	token, _, ok := gates.Enter("acme/data01", admission.Source)
	if !ok {
		t.Fatal("source work was not admitted before the purge hold")
	}
	ops := &latchedOps{MemOperations: rg.ctl.ops().(*MemOperations)}
	rg.ctl.Ops = ops
	var blocked Operation
	if code, raw := rg.call(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token, Wait: "0s"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("purge: want HTTP 202, got HTTP %d %s", code, raw)
	}
	for deadline := time.Now().Add(5 * time.Second); blocked.Status != StatusBlocked || blocked.Barrier == nil || blocked.Barrier.HoldVersion == 0; {
		if time.Now().After(deadline) {
			t.Fatalf("purge never blocked on the source work: %+v", blocked)
		}
		time.Sleep(2 * time.Millisecond)
		rg.must(http.MethodGet, "/v1/operations/"+blocked.ID, nil, &blocked)
	}
	if !slices.Contains(blocked.AllowedActions, ActionCancel) || blocked.Blockers[0].Code != BlockerOldRequests {
		t.Fatalf("purge blocked on the source work: %+v", blocked)
	}

	// The latch between DispatchStarted and the first delete: the listing purge empties the source
	// with. The source lists: the first check, the re-diff, then the delete phase's own.
	dispatched, releaseOwner := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOwner) }) })
	listed := 0
	rg.vast01.onList = func(string) {
		listed++
		if listed == 2 {
			close(dispatched)
			<-releaseOwner
		}
	}
	// The cancellation reads the record while the owner is still draining; then the owner drains,
	// marks its dispatch, and reaches its first delete before the cancellation acts.
	ops.release = func() {
		token.Release(gates, admission.Definitive)
		wake()
		<-dispatched
	}
	ops.armed.Store(true)
	rg.answers(http.MethodPost, "/v1/operations/"+blocked.ID+"/cancel", struct{}{}, http.StatusConflict, CodeNotCancellable)
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if p.Barrier == nil || p.Barrier.ID != blocked.ID || gates.State("acme/data01").Closed[admission.Source] != blocked.ID {
		t.Fatalf("the cancellation reopened the source under a dispatched purge: placement %+v, gates %+v", p, gates.State("acme/data01"))
	}
	var mid Operation
	rg.must(http.MethodGet, "/v1/operations/"+blocked.ID, nil, &mid)
	if mid.Status == StatusCancelled || mid.Barrier == nil || !mid.Barrier.DispatchStarted {
		t.Fatalf("the record under dispatch: %+v", mid)
	}
	releaseOnce.Do(func() { close(releaseOwner) })
	if done := rg.await(blocked.ID); done.Status != StatusSucceeded || done.Barrier == nil || !done.Barrier.Committed {
		t.Fatalf("purge after the refused cancellation: %+v", done)
	}
}

// Defect 6 (T08). An operation blocked in its precondition by a silent member has no barrier yet;
// it must still be cancellable, and its scope freed for a new operation.
func TestPreconditionBlockedOperationCancels(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	updates := operationUpdates(rg)
	sleep, wake := wakingSleep()
	rg.ctl.Sleep, rg.ctl.FencePoll = sleep, 5*time.Millisecond
	fleet := &barrierFleet{}
	snap := rg.dir.Snapshot()
	fleet.set(Member{ID: "p-silent", Live: false, Identity: snap.File().Identity, Applied: snap.Version(), Durable: snap.Version(),
		Incarnation: &Incarnation{ID: testInc, State: IncarnationActive}})
	rg.ctl.Fleet = fleet

	var started Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":0.5,"wait":"0s"}`)}, &started); code != http.StatusAccepted {
		t.Fatalf("starting the ramp: %d %s", code, raw)
	}
	blocked := waitOperation(t, updates, started.ID, func(op Operation) bool {
		return op.Status == StatusBlocked && op.Phase == PhasePrecondition && slices.Contains(op.AllowedActions, ActionCancel)
	})
	if blocked.Barrier != nil || len(blocked.Blockers) != 1 || blocked.Blockers[0].Code != BlockerProxyMissing {
		t.Fatalf("precondition blocker: %+v", blocked)
	}
	var canceled Operation
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/cancel", struct{}{}, &canceled)
	wake()
	waitNotRunning(t, rg.ctl, started.ID)
	if canceled.Status != StatusCancelled || canceled.EffectState != EffectNone {
		t.Fatalf("canceled precondition: %+v", canceled)
	}
	if p, _ := rg.dir.Snapshot().Lookup("acme", "data01"); p.Held() || p.Barrier != nil || p.State != directory.StateActive {
		t.Fatalf("a canceled precondition changed the placement: %+v", p)
	}
	// The scope is free: a new operation on the bucket is accepted (and blocks on the same member).
	var next Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":0.5,"wait":"0s"}`)}, &next); code != http.StatusAccepted {
		t.Fatalf("a new operation after the cancellation: %d %s", code, raw)
	}
	rg.must(http.MethodPost, "/v1/operations/"+next.ID+"/cancel", struct{}{}, nil)
	wake()
	waitNotRunning(t, rg.ctl, next.ID)
}

// Defect 6, the other half: a barrier record whose owner was lost after it wrote its intent and
// before its hold version was durable. Its hold may be in the directory; cancellation releases it.
func TestCancelOwnerLostBeforeHoldVersion(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	mem := rg.ctl.ops().(*MemOperations)
	op := rg.ctl.clusterOp("vast01")
	now := time.Now().UTC()
	op.ID, op.Kind, op.Actor, op.Node = "lost-hold", OpClusterReadOnly, "test", "lost-node"
	op.Identity, op.Status, op.Phase = rg.dir.Snapshot().File().Identity, StatusBlocked, PhaseHold
	op.Created, op.Updated, op.Sequence, op.EffectState = now, now, 1, EffectNone
	op.Barrier = &BarrierState{ID: op.ID, Scope: directory.ClusterResource("vast01"), Kind: "mutations"}
	if err := mem.Create(t.Context(), &op); err != nil {
		t.Fatal(err)
	}
	if err := rg.dir.SetClusterReadOnly(t.Context(), "vast01", true, false, op.ID, "lost-node"); err != nil {
		t.Fatal(err)
	}
	var canceled Operation
	rg.must(http.MethodPost, "/v1/operations/lost-hold/cancel", struct{}{}, &canceled)
	if canceled.Status != StatusCancelled {
		t.Fatalf("cancel of an owner-lost hold: %+v", canceled)
	}
	if c := rg.dir.Snapshot().File().Clusters["vast01"]; c.ReadOnly || c.Barrier != nil {
		t.Fatalf("the lost owner's hold was not released: %+v", c)
	}
}

// Defect 7. An external mover that died after an error leaves its session unresolved. The
// operator resolves it through the API (the CLI's `shunt operation resolve-worker` and the
// Operations screen send the same request) with the session id and an attestation; the record
// ends with the resolution kept on it, and the placement is free for the next operation.
func TestDeadExternalMoverResolvedByOperator(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	var clock atomic.Int64
	clock.Store(time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC).UnixNano())
	rg.ctl.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	rg.ctl.WorkerTTL = time.Second
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep
	updates := operationUpdates(rg)

	const session = "00112233445566778899aabbccddeeff"
	args, _ := json.Marshal(MoverRequest{External: true, Session: session, Wait: "0s"})
	var started Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpMover, Placement: "acme/data01", Args: args}, &started); code != http.StatusAccepted {
		t.Fatalf("start external mover: HTTP %d %s", code, raw)
	}
	file := rg.dir.Snapshot().File()
	// The mover's last heartbeat: a copy failed after dispatch, then the process died.
	beat := WorkerHeartbeat{Session: session, Identity: file.Identity,
		Generation: file.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Inflight: 1, Uncertain: 1}
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, nil)
	rg.refused(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", ResolveWorkerRequest{Session: session, Attestation: "too early"}, "is live")

	clock.Add(int64(2 * time.Second))
	wake()
	expired := waitOperation(t, updates, started.ID, func(op Operation) bool {
		return op.Status == StatusBlocked && len(op.Blockers) == 1 && op.Blockers[0].Code == BlockerWorkerUnresolved && slices.Contains(op.AllowedActions, ActionResolveWorker)
	})
	if expired.Worker == nil || expired.Worker.ID != session {
		t.Fatalf("expired worker on its record: %+v", expired)
	}
	// Neither cancel nor resume is its exit; a new mover run is refused while it holds the bucket.
	rg.answers(http.MethodPost, "/v1/operations/"+started.ID+"/cancel", struct{}{}, http.StatusConflict, CodeNotCancellable)
	rg.answers(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", ResolveWorkerRequest{Session: session}, http.StatusBadRequest, "bad_request")
	rg.refused(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", ResolveWorkerRequest{Session: "ffeeddccbbaa99887766554433221100", Attestation: "wrong session"}, "not enrolled for worker session")

	const why = "the mover host was powered off at 12:00; the backend's request log shows nothing from it since"
	var resolved Operation
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", ResolveWorkerRequest{Session: session, Attestation: why}, &resolved)
	wake()
	done := rg.await(started.ID)
	if done.Status != StatusFailed || done.Worker == nil || done.Worker.State != WorkerCompleted || done.Worker.Attestation != why || done.Worker.ResolvedBy == "" {
		t.Fatalf("the resolved mover's record: %+v worker %+v", done, done.Worker)
	}
	// The placement is free: the next mover run is accepted.
	args2, _ := json.Marshal(MoverRequest{External: true, Session: "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f", Wait: "0s"})
	var next Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpMover, Placement: "acme/data01", Args: args2}, &next); code != http.StatusAccepted {
		t.Fatalf("a new mover after the resolution: HTTP %d %s", code, raw)
	}
	beat2 := WorkerHeartbeat{Session: "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f", Identity: file.Identity, Generation: beat.Generation, Sequence: 1, Complete: true}
	rg.must(http.MethodPost, "/v1/operations/"+next.ID+"/worker-heartbeat", beat2, nil)
	wake()
	rg.await(next.ID)
}

// Should-fix of the H2 review: an acknowledgement names its barrier by id, so a proxy that has
// installed a later generation of the scope (another write to it, such as a secret rotation that
// stamps a cluster) still acknowledges this hold. An older generation has not installed it.
func TestMemberBlockersAcceptNewerGeneration(t *testing.T) {
	st := &BarrierState{ID: "op-1", Scope: directory.ClusterResource("vast01"), Kind: "mutations", HoldVersion: 10, Generation: 7}
	ack := func(gen int64) []admission.Ack {
		return []admission.Ack{{ID: "op-1", Scope: st.Scope, Kind: st.Kind, Generation: gen, Closed: true}}
	}
	if bs := memberBlockers("p1", ack(7), 10, st); len(bs) != 0 {
		t.Fatalf("the hold's own generation: %+v", bs)
	}
	if bs := memberBlockers("p1", ack(9), 12, st); len(bs) != 0 {
		t.Fatalf("a newer generation of the same barrier: %+v", bs)
	}
	if bs := memberBlockers("p1", ack(6), 10, st); len(bs) != 1 || bs[0].Code != BlockerInstallPending {
		t.Fatalf("an older generation: %+v", bs)
	}
	other := []admission.Ack{{ID: "op-2", Scope: st.Scope, Kind: st.Kind, Generation: 9, Closed: true}}
	if bs := memberBlockers("p1", other, 12, st); len(bs) != 1 || bs[0].Code != BlockerInstallPending {
		t.Fatalf("another barrier's ack: %+v", bs)
	}
}

// Should-fix of the H2 review: shunt_fleet_unresolved_incarnations is the fleet's unresolved
// incarnations, not a gauge that always reads 0.
func TestUnresolvedIncarnationsGauge(t *testing.T) {
	rg := newRig(t)
	fleet := &barrierFleet{}
	fleet.set(Member{ID: "p1", Unresolved: []Incarnation{{ID: testInc, State: IncarnationUnclean}, {ID: "b" + testInc[1:], State: IncarnationUnclean}}},
		Member{ID: "p2", Live: true, Unresolved: []Incarnation{{ID: "c" + testInc[1:], State: IncarnationUnclean}}})
	rg.ctl.Fleet = fleet
	if _, err := rg.ctl.members(t.Context()); err != nil {
		t.Fatal(err)
	}
	metric := &dto.Metric{}
	if err := rg.ctl.Metrics.UnresolvedIncarnations.Write(metric); err != nil {
		t.Fatal(err)
	}
	if got := metric.GetGauge().GetValue(); got != 3 {
		t.Fatalf("shunt_fleet_unresolved_incarnations = %v, want 3", got)
	}
}

// Should-fix of the H2 review: the owner-loss sweep sees an unfinished record however many ended
// records are newer. It listed the newest 1000, and a store never prunes unfinished records, so an
// old barrier whose owner died stayed invisible behind 1000 newer ones.
func TestSweepSeesUnfinishedPastTheHistoryLimit(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	mem := rg.ctl.ops().(*MemOperations)
	mem.Limit = 5000
	now := time.Now().UTC()
	old := rg.ctl.clusterOp("vast01")
	old.ID, old.Kind, old.Actor, old.Node = "0000000000001-000001", OpClusterReadOnly, "test", "dead-node"
	old.Identity, old.Status, old.Phase = rg.dir.Snapshot().File().Identity, StatusBlocked, PhaseDrain
	old.Created, old.Updated, old.Sequence, old.EffectState = now, now, 1, EffectNone
	old.Barrier = &BarrierState{ID: old.ID, Scope: directory.ClusterResource("vast01"), Kind: "mutations", HoldVersion: rg.dir.Snapshot().Version()}
	if err := mem.Create(t.Context(), &old); err != nil {
		t.Fatal(err)
	}
	for i := range defaultOperationLimit + 1 {
		filler := Operation{ID: fmt.Sprintf("%013d-%06x", i+2, i), Kind: OpWatch, Actor: "test", Node: "lab", Status: StatusSucceeded, EffectState: EffectCommitted,
			Sequence: 1, Created: now, Updated: now}
		if err := mem.Create(t.Context(), &filler); err != nil {
			t.Fatal(err)
		}
	}
	if err := rg.ctl.Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, _ := mem.Get(t.Context(), old.ID)
	if got.Status != StatusBlocked || len(got.Blockers) != 1 || got.Blockers[0].Code != BlockerOwnerLost || !slices.Contains(got.AllowedActions, ActionResume) {
		t.Fatalf("an old unfinished record behind %d newer ones: %+v", defaultOperationLimit+1, got)
	}
}

// Found by make fleet (2026-09-25): a step whose control node died while it waited in its
// precondition (a silent proxy; no hold written) was orphaned failed with effect uncertain, and its
// ended record still offered cancel and named the proxy it had waited on. It had written nothing,
// so its effect is none, and an ended record offers no action and carries no blocker. A step that
// had begun writing stays uncertain.
func TestOrphanedBeforeFirstWriteChangedNothing(t *testing.T) {
	now := time.Now().UTC()
	waiting := Operation{ID: "1-a", Kind: OpRamp, Node: "dead", Status: StatusBlocked, Phase: PhasePrecondition, EffectState: EffectNone,
		Blockers: []Blocker{{Code: BlockerProxyMissing, ProxyID: "proxy-b"}}, BlockerCount: 1, AllowedActions: []string{ActionCancel}}
	Orphan(&waiting, now, "gone")
	if waiting.Status != StatusFailed || waiting.EffectState != EffectNone || len(waiting.AllowedActions) != 0 || len(waiting.Blockers) != 0 || waiting.BlockerCount != 0 {
		t.Fatalf("a step orphaned in its precondition: %+v", waiting)
	}
	writing := Operation{ID: "1-b", Kind: OpAdopt, Node: "dead", Status: StatusRunning, Phase: PhaseStep, EffectState: EffectNone}
	Orphan(&writing, now, "gone")
	if writing.Status != StatusFailed || writing.EffectState != EffectUncertain {
		t.Fatalf("a step orphaned while writing: %+v", writing)
	}
}

// Quality item of the H2 review: a member whose process stopped (an unclean retirement leaves no
// current incarnation) is not described as possibly still serving.
func TestMissingTextForStoppedMember(t *testing.T) {
	gone := &Member{ID: "p1", Unresolved: []Incarnation{{ID: testInc, State: IncarnationUnclean}}}
	if got := missingText(gone, "serving"); got != "p1 retired with outcomes unknown; resolve its incarnation, then `shunt proxy forget p1`" {
		t.Fatalf("after an unclean retirement: %q", got)
	}
	silent := &Member{ID: "p2", Incarnation: &Incarnation{ID: testInc, State: IncarnationActive}, SinceSeen: 3 * time.Second}
	if got := missingText(silent, "its process may still serve the old routing"); got != "p2 is silent (last seen 3s ago); its process may still serve the old routing" {
		t.Fatalf("a silent member: %q", got)
	}
}
