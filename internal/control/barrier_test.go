package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// barrierFleet is a fleet whose reports a test may advance while a barrier is waiting.
type barrierFleet struct {
	mu sync.RWMutex
	ms []Member
}

// resumeRaceOps installs a rival owner during the liveness check, between resume's read and CAS.
type resumeRaceOps struct {
	*MemOperations
	once sync.Once
}

func (o *resumeRaceOps) OwnerLive(ctx context.Context, _ string) (bool, error) {
	o.once.Do(func() {
		op, err := o.Get(ctx, "resume-race")
		if err != nil || op == nil {
			return
		}
		op.Node, op.OwnerTerm, op.Status = "winner", op.OwnerTerm+1, StatusRunning
		op.Sequence++
		_ = o.Update(ctx, op)
	})
	return false, nil
}

func (f *barrierFleet) Heartbeat(context.Context, string, Heartbeat) (Grant, error) {
	return Grant{LeaseTTL: time.Second}, nil
}
func (f *barrierFleet) Members(context.Context) ([]Member, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return slices.Clone(f.ms), nil
}
func (f *barrierFleet) Retire(context.Context, string, string, int64) error { return nil }
func (f *barrierFleet) RequestRetire(context.Context, string) error         { return nil }
func (f *barrierFleet) Resolve(context.Context, string, string, string, string) error {
	return nil
}
func (f *barrierFleet) Forget(context.Context, string) error { return nil }
func (f *barrierFleet) set(ms ...Member) {
	f.mu.Lock()
	f.ms = slices.Clone(ms)
	f.mu.Unlock()
}

// operationUpdates replaces the rig's operation hook with a lossless-enough test observation
// channel while retaining the event stream production uses.
func operationUpdates(rg *rig) <-chan Operation {
	updates := make(chan Operation, 128)
	rg.ctl.ops().(*MemOperations).OnChange = func(op Operation) {
		rg.events.Fence(op)
		updates <- *op.clone()
	}
	return updates
}

func waitOperation(t *testing.T, updates <-chan Operation, id string, match func(Operation) bool) Operation {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case op := <-updates:
			if (id == "" || op.ID == id) && match(op) {
				return op
			}
		case <-timer.C:
			t.Fatalf("operation %s did not reach the expected state", id)
		}
	}
}

func wakingSleep() (func(context.Context, time.Duration) error, func()) {
	wake := make(chan struct{}, 16)
	sleep := func(ctx context.Context, _ time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			return nil
		}
	}
	return sleep, func() { wake <- struct{}{} }
}

func waitNotRunning(t *testing.T, s *Server, id string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for s.runs(id) {
		select {
		case <-deadline.C:
			t.Fatalf("operation %s's canceled worker did not exit", id)
		default:
			runtime.Gosched()
		}
	}
}

// T08. Read-only is a durable desired state immediately, even when a proxy is missing. The wait
// deadline answers 202 with the blocker; cancellation restores the switch. A later operation
// becomes effective once that proxy reports a durable, drained acknowledgement, and cannot be
// canceled after its commit.
func TestReadOnlyBarrierBlocksCancelsAndBecomesEffective(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	updates := operationUpdates(rg)
	sleep, wake := wakingSleep()
	rg.ctl.Sleep, rg.ctl.FencePoll = sleep, 5*time.Millisecond
	fleet := &barrierFleet{}
	snap := rg.dir.Snapshot()
	fleet.set(Member{ID: "p-missing", Live: false, Identity: snap.File().Identity, Applied: snap.Version(), Durable: snap.Version(),
		Incarnation: &Incarnation{ID: testInc, State: IncarnationActive}})
	rg.ctl.Fleet = fleet

	type answer struct {
		code int
		raw  string
		op   Operation
	}
	answered := make(chan answer, 1)
	go func() {
		var op Operation
		code, raw := rg.call(http.MethodPost, "/v1/clusters/vast01/read-only", ReadOnlyRequest{ReadOnly: true, Wait: "0s"}, &op)
		answered <- answer{code: code, raw: raw, op: op}
	}()
	blocked := waitOperation(t, updates, "", func(op Operation) bool { return op.Kind == OpClusterReadOnly && op.Status == StatusBlocked })
	ans := <-answered
	if ans.code != http.StatusAccepted {
		t.Fatalf("read-only wait deadline: want 202, got %d %s", ans.code, ans.raw)
	}
	if blocked.Status != StatusBlocked || blocked.Phase != PhaseDrain || len(blocked.Blockers) != 1 || blocked.Blockers[0].Code != BlockerProxyMissing || blocked.Blockers[0].ProxyID != "p-missing" || !slices.Contains(blocked.AllowedActions, ActionCancel) {
		t.Fatalf("blocked read-only operation: %+v", blocked)
	}
	cl := rg.dir.Snapshot().File().Clusters["vast01"]
	if !cl.ReadOnly || cl.Barrier == nil || cl.Barrier.ID != blocked.ID {
		t.Fatalf("read-only was not desired with its barrier: %+v", cl)
	}
	var page BlockerPage
	rg.must(http.MethodGet, "/v1/operations/"+blocked.ID+"/blockers", nil, &page)
	if !page.Complete || len(page.Blockers) != 1 || page.Blockers[0].ProxyID != "p-missing" {
		t.Fatalf("blocker page: %+v", page)
	}
	var canceled Operation
	rg.must(http.MethodPost, "/v1/operations/"+blocked.ID+"/cancel", struct{}{}, &canceled)
	wake()
	waitNotRunning(t, rg.ctl, blocked.ID)
	if canceled.Status != StatusCancelled || canceled.EffectState != EffectNone {
		t.Fatalf("canceled read-only operation: %+v", canceled)
	}
	cl = rg.dir.Snapshot().File().Clusters["vast01"]
	if cl.ReadOnly || cl.Barrier != nil {
		t.Fatalf("cancellation did not restore the cluster: %+v", cl)
	}

	var next Operation
	args := json.RawMessage(`{"read_only":true,"wait":"0s"}`)
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpClusterReadOnly, Cluster: "vast01", Args: args}, &next); code != http.StatusAccepted {
		t.Fatalf("starting the second read-only operation: %d %s", code, raw)
	}
	next = waitOperation(t, updates, next.ID, func(op Operation) bool {
		return op.Status == StatusBlocked && op.Barrier != nil && op.Barrier.HoldVersion > 0
	})
	b := *next.Barrier
	fleet.set(Member{ID: "p-missing", Live: true, Identity: rg.dir.Snapshot().File().Identity, Applied: b.HoldVersion, Durable: b.HoldVersion,
		Incarnation: &Incarnation{ID: testInc, State: IncarnationActive},
		Barriers:    []admission.Ack{{ID: b.ID, Scope: b.Scope, Kind: b.Kind, Generation: b.Generation, Closed: true}}})
	wake()
	done := waitOperation(t, updates, next.ID, func(op Operation) bool { return op.Terminal() })
	if done.Status != StatusSucceeded || done.Barrier == nil || !done.Barrier.Committed {
		t.Fatalf("effective read-only operation: %+v", done)
	}
	var result ReadOnlyResult
	if err := json.Unmarshal(done.Result, &result); err != nil || !result.Desired || !result.Effective {
		t.Fatalf("read-only result: %+v (%v)", result, err)
	}
	cl = rg.dir.Snapshot().File().Clusters["vast01"]
	if !cl.ReadOnly || cl.Barrier != nil {
		t.Fatalf("effective read-only state: %+v", cl)
	}
	rg.answers(http.MethodPost, "/v1/operations/"+next.ID+"/cancel", struct{}{}, http.StatusConflict, CodeNotCancellable)
}

// Cancellation is offered only once the directory hold and its version are both durable. This
// closes the race where a cancel could end the record while its old owner was already inside the
// hold write, leaving a hold with no resumable owner.
func TestCancelWaitsForDurableHold(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	updates := operationUpdates(rg)
	op := rg.ctl.clusterOp("vast01")
	op.Kind = OpClusterReadOnly
	tr, err := rg.ctl.begin("test", op, ReadOnlyRequest{ReadOnly: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	entered, writeHold := make(chan struct{}), make(chan struct{})
	ch := rg.ctl.operate(tr, func(tr *tracker) (any, error) {
		b := &barrier{
			scope: directory.ClusterResource("vast01"),
			kind:  "mutations",
			hold: func() (int64, error) {
				close(entered)
				<-writeHold
				if err := rg.dir.SetClusterReadOnly(tr.ctx, "vast01", true, false, tr.id(), tr.actor); err != nil {
					return 0, err
				}
				return rg.dir.Snapshot().Version(), nil
			},
			commit: func() (int64, bool, error) {
				if err := rg.dir.ClearBarrier(tr.ctx, directory.ClusterResource("vast01"), tr.id(), tr.actor); err != nil {
					return 0, false, err
				}
				return rg.dir.Snapshot().Version(), false, nil
			},
			extra: func(context.Context) ([]Blocker, error) {
				return []Blocker{{Code: BlockerProxyMissing, ProxyID: "p1"}}, nil
			},
		}
		return nil, rg.ctl.runBarrier(tr, b)
	})
	<-entered
	rg.answers(http.MethodPost, "/v1/operations/"+tr.id()+"/cancel", struct{}{}, http.StatusConflict, "refused")
	var pending Operation
	rg.must(http.MethodGet, "/v1/operations/"+tr.id(), nil, &pending)
	if pending.Barrier == nil || pending.Barrier.HoldVersion != 0 || slices.Contains(pending.AllowedActions, ActionCancel) {
		t.Fatalf("operation advertised an unsafe cancellation before its hold was durable: %+v", pending)
	}
	close(writeHold)
	blocked := waitOperation(t, updates, tr.id(), func(op Operation) bool {
		return op.Status == StatusBlocked && op.Barrier != nil && op.Barrier.HoldVersion > 0
	})
	if !slices.Contains(blocked.AllowedActions, ActionCancel) {
		t.Fatalf("durable hold is not cancellable: %+v", blocked)
	}
	var canceled Operation
	rg.must(http.MethodPost, "/v1/operations/"+tr.id()+"/cancel", struct{}{}, &canceled)
	if out := <-ch; !errors.Is(out.err, errCanceled) {
		t.Fatalf("canceled worker ended with %v", out.err)
	}
	cl := rg.dir.Snapshot().File().Clusters["vast01"]
	if cl.ReadOnly || cl.Barrier != nil || canceled.Status != StatusCancelled {
		t.Fatalf("cancellation did not restore the scope: cluster %+v, operation %+v", cl, canceled)
	}
}

func TestResumeDoesNotStealFromConcurrentWinner(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	mem := rg.ctl.ops().(*MemOperations)
	stored := rg.ctl.clusterOp("vast01")
	now := time.Now().UTC()
	stored.ID, stored.Kind, stored.Actor, stored.Node = "resume-race", OpClusterReadOnly, "test", "lost"
	stored.Identity, stored.Status, stored.Phase = rg.dir.Snapshot().File().Identity, StatusBlocked, PhaseDrain
	stored.Created, stored.Updated, stored.Sequence, stored.EffectState = now, now, 1, EffectNone
	stored.Barrier = &BarrierState{ID: stored.ID, Scope: directory.ClusterResource("vast01"), Kind: "mutations", HoldVersion: rg.dir.Snapshot().Version()}
	if err := mem.Create(t.Context(), &stored); err != nil {
		t.Fatal(err)
	}
	server := &Server{Dir: rg.dir, Ops: &resumeRaceOps{MemOperations: mem}, Node: "loser"}
	req := httptest.NewRequest(http.MethodPost, "/v1/operations/resume-race/resume", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("concurrent resume: HTTP %d %s", rec.Code, rec.Body.String())
	}
	got, err := mem.Get(t.Context(), "resume-race")
	if err != nil || got == nil {
		t.Fatalf("reading winner: %+v, %v", got, err)
	}
	if got.Node != "winner" || got.OwnerTerm != 1 {
		t.Fatalf("losing resume stole the record: %+v", got)
	}
}

// T09. The lab proxy participates in the same barrier as a fleet member: an in-flight local
// mutation blocks the commit. After the owner disappears, Sweep preserves the hold as owner_lost,
// another node resumes the same record, and exactly that operation commits it.
func TestLocalBarrierResumesAfterOwnerLoss(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	updates := operationUpdates(rg)
	gates := admission.New()
	previousInstall := rg.dir.OnInstall
	rg.dir.OnInstall = func(snap *directory.Snapshot) {
		closures, _ := admission.Barriers(snap)
		gates.Apply(closures, false)
		previousInstall(snap)
	}
	rg.ctl.LocalGates = gates
	ctx, stopOwner := context.WithCancel(context.Background())
	rg.ctl.Ctx = ctx
	sleep, wakeOwner := wakingSleep()
	rg.ctl.Sleep, rg.ctl.FencePoll = sleep, 5*time.Millisecond
	token, _, ok := gates.Enter("acme/data01", admission.Mutations)
	if !ok {
		t.Fatal("the local mutation was not admitted before the hold")
	}

	var started Operation
	args := json.RawMessage(`{"ratio":0.5,"wait":"0s"}`)
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: args}, &started); code != http.StatusAccepted {
		t.Fatalf("starting the ramp: %d %s", code, raw)
	}
	blocked := waitOperation(t, updates, started.ID, func(op Operation) bool {
		return op.Status == StatusBlocked && len(op.Blockers) == 1 && op.Blockers[0].Code == BlockerOldRequests
	})
	if blocked.Blockers[0].ProxyID != "lab" || blocked.Blockers[0].Count != 1 || blocked.Barrier == nil {
		t.Fatalf("local in-flight blocker: %+v", blocked)
	}

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
	wakeOwner()
	waitNotRunning(t, rg.ctl, started.ID)
	token.Release(gates, admission.Definitive)

	mem.Node = "replacement"
	replacement := &Server{Dir: rg.dir, Clusters: rg.ctl.Clusters, Metrics: rg.ctl.Metrics, Log: rg.ctl.Log, Fleet: NoFleet{}, Ops: mem, Node: "replacement", LocalGates: gates,
		Now: rg.ctl.Now, Sleep: func(context.Context, time.Duration) error { return nil }, FencePoll: time.Millisecond}
	if err := replacement.Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	orphan, _ := mem.Get(t.Context(), started.ID)
	if orphan.Status != StatusBlocked || len(orphan.Blockers) != 1 || orphan.Blockers[0].Code != BlockerOwnerLost || !slices.Contains(orphan.AllowedActions, ActionResume) {
		t.Fatalf("orphaned barrier: %+v", orphan)
	}
	metric := &dto.Metric{}
	if err := replacement.Metrics.Operations.WithLabelValues(StatusBlocked, EffectNone).Write(metric); err != nil {
		t.Fatal(err)
	}
	if got := metric.GetGauge().GetValue(); got != 1 {
		t.Fatalf("shunt_operations{status=blocked,effect=none} = %v, want 1", got)
	}

	api := httptest.NewServer(replacement.Handler())
	defer api.Close()
	req, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/operations/"+started.ID+"/resume", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("resume: HTTP %d", resp.StatusCode)
	}
	done := waitOperation(t, updates, started.ID, func(op Operation) bool { return op.Terminal() })
	if done.Status != StatusSucceeded || done.OwnerTerm != 1 || done.Node != "replacement" || done.Barrier == nil || !done.Barrier.Committed {
		t.Fatalf("resumed operation: %+v", done)
	}
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if p.Held() || p.Barrier != nil || p.Ramp == nil || p.Ramp.Ratio != 0.5 {
		t.Fatalf("resumed ramp was not committed once: %+v", p)
	}
}

func TestCutoverBarrierBlocksOpenSourceMultipartUpload(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must(http.MethodPost, "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	rg.must(http.MethodPost, "/v1/placements/acme/data01/mover-progress", Progress{
		Source: "vast01", Primary: "vast02", Pass: 2, Skipped: 2, Done: true, Converged: true,
	}, nil)

	req, err := http.NewRequest(http.MethodPost, rg.vast01.srv.URL+"/data01/large?uploads", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create multipart upload: HTTP %d", resp.StatusCode)
	}

	var blocked Operation
	if code, raw := rg.call(http.MethodPost, "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "0s", Wait: "0s"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("cutover: want HTTP 202, got HTTP %d %s", code, raw)
	}
	if blocked.Status != StatusBlocked || !slices.ContainsFunc(blocked.Blockers, func(b Blocker) bool {
		return b.Code == BlockerMultipartOpen && b.Count == 1
	}) {
		t.Fatalf("multipart cutover blocker: %+v", blocked)
	}
	var canceled Operation
	rg.must(http.MethodPost, "/v1/operations/"+blocked.ID+"/cancel", struct{}{}, &canceled)
	if canceled.Status != StatusCancelled {
		t.Fatalf("cancel cutover: %+v", canceled)
	}
}

// The quiet window runs with the bucket's writes flowing (2026-09-25). H2e ran it behind the
// closed mutations gate, which answered every write and delete 503 for the whole window (60 s by
// default) and failed make walkthrough's verify; a fallback read is a read, which that gate does not
// stop. The placement carries no barrier while the window runs. A read that falls back after the
// window, before the commit, is still caught under the closed gate: a second window is watched,
// held, and the evidence counts that read.
func TestCutoverWindowKeepsWritesFlowing(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must(http.MethodPost, "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	rg.vast02.put(t, "data01-001", "a", "one")
	rg.vast02.put(t, "data01-001", "dir/b", "two")
	rg.must(http.MethodPost, "/v1/placements/acme/data01/mover-progress", Progress{
		Source: "vast01", Primary: "vast02", Pass: 2, Skipped: 2, Done: true, Converged: true,
	}, nil)
	var mu sync.Mutex
	var heldDuringWindow []bool
	rg.ctl.Sleep = func(_ context.Context, d time.Duration) error {
		if d == 5*time.Second {
			p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
			mu.Lock()
			heldDuringWindow = append(heldDuringWindow, p.Barrier != nil)
			mu.Unlock()
		}
		return nil
	}
	// A read falls back after the window: when the hold is installed.
	previousInstall := rg.dir.OnInstall
	var once sync.Once
	rg.dir.OnInstall = func(snap *directory.Snapshot) {
		if p, ok := snap.Lookup("acme", "data01"); ok && p.Barrier != nil {
			once.Do(func() { rg.ctl.Metrics.FallbackReads.WithLabelValues("acme/data01").Inc() })
		}
		previousInstall(snap)
	}

	var tr TransitionResult
	rg.must(http.MethodPost, "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "5s"}, &tr)
	mu.Lock()
	windows := slices.Clone(heldDuringWindow)
	mu.Unlock()
	if len(windows) == 0 || windows[0] {
		t.Fatalf("the quiet window ran with the placement held (held per window: %v); writes must flow while it runs", windows)
	}
	// The read that fell back after the first window is not lost: the evidence comes from a second
	// window, watched under the hold, that saw it.
	if !slices.Equal(windows, []bool{false, true}) || tr.To != directory.StateCutover || tr.Cutover == nil || tr.Cutover.FallbackReads != 1 {
		t.Fatalf("a read that fell back after the window: windows held %v, result %+v %+v", windows, tr, tr.Cutover)
	}
	var op Operation
	rg.must(http.MethodGet, "/v1/operations/"+tr.Operation, nil, &op)
	if op.Status != StatusSucceeded || op.EffectState != EffectCommitted {
		t.Fatalf("cutover record: %+v", op)
	}
}

func TestPurgeSourceBarrierDrainsLocalSourceWork(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.cutOver()
	var dry PurgeDryRun
	rg.must(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Token == "" {
		t.Fatalf("purge dry run: %+v", dry)
	}

	gates := admission.New()
	previousInstall := rg.dir.OnInstall
	rg.dir.OnInstall = func(snap *directory.Snapshot) {
		closures, _ := admission.Barriers(snap)
		gates.Apply(closures, false)
		previousInstall(snap)
	}
	rg.ctl.LocalGates = gates
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep
	token, _, ok := gates.Enter("acme/data01", admission.Source)
	if !ok {
		t.Fatal("source work was not admitted before the purge hold")
	}

	var blocked Operation
	if code, raw := rg.call(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token, Wait: "0s"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("purge: want HTTP 202, got HTTP %d %s", code, raw)
	}
	if blocked.Status != StatusBlocked || !slices.ContainsFunc(blocked.Blockers, func(b Blocker) bool {
		return b.Code == BlockerOldRequests && b.ProxyID == "lab" && b.Count == 1
	}) || blocked.Barrier == nil || blocked.Barrier.Kind != config.BarrierSource {
		t.Fatalf("source drain blocker: %+v", blocked)
	}
	if ok, _ := rg.vast01.be.BucketExists("data01"); !ok {
		t.Fatal("purge deleted the source before its source-dependent work drained")
	}
	token.Release(gates, admission.Definitive)
	wake()
	done := rg.await(blocked.ID)
	if done.Status != StatusSucceeded || done.Barrier == nil || !done.Barrier.Committed {
		t.Fatalf("purge after source drain: %+v", done)
	}
	if ok, _ := rg.vast01.be.BucketExists("data01"); ok {
		t.Fatal("purge left the source bucket after the gate drained")
	}
}

func TestPurgeDestructiveFailureKeepsReservationAndCannotCancel(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.cutOver()
	var dry PurgeDryRun
	rg.must(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Token == "" {
		t.Fatalf("purge dry run: %+v", dry)
	}

	listed := 0
	rg.vast01.onList = func(string) {
		listed++
		if listed == 3 { // the source's own listing as purge empties it, past its dispatch point
			rg.vast01.reject = "AccessDenied"
		}
	}
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep
	var blocked Operation
	if code, raw := rg.call(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token, Wait: "0s"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("purge: want HTTP 202, got HTTP %d %s", code, raw)
	}
	if blocked.Status != StatusBlocked || blocked.Barrier == nil || !blocked.Barrier.DispatchStarted ||
		!slices.ContainsFunc(blocked.Blockers, func(b Blocker) bool { return b.Code == BlockerBackendOutcomeUnknown }) {
		t.Fatalf("destructive purge blocker: %+v", blocked)
	}
	rg.answers(http.MethodPost, "/v1/operations/"+blocked.ID+"/cancel", struct{}{}, http.StatusConflict, CodeNotCancellable)

	rg.vast01.reject, rg.vast01.onList = "", nil
	wake()
	if done := rg.await(blocked.ID); done.Status != StatusSucceeded || done.Barrier == nil || !done.Barrier.Committed {
		t.Fatalf("reconciled purge: %+v", done)
	}
}

// Defect 4 (H2 review). The diff purge-source repeats once its source barrier has drained runs
// before its irreversible point: a refusal there is a blocker, nothing is deleted, and the
// operation can be canceled, which reopens the source and frees the bucket for the mover.
func TestPurgeRecheckRefusalBeforeDispatchIsCancellable(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.cutOver()
	var dry PurgeDryRun
	rg.must(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Token == "" {
		t.Fatalf("purge dry run: %+v", dry)
	}
	// A key on the source the primary lacks appears between the confirmation and the re-diff.
	listed := 0
	rg.vast01.onList = func(string) {
		listed++
		if listed == 2 { // the re-diff under the drained source barrier
			rg.vast01.put(t, "data01", "late", "only on the source")
		}
	}
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep
	var blocked Operation
	if code, raw := rg.call(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token, Wait: "0s"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("purge: want HTTP 202, got HTTP %d %s", code, raw)
	}
	if blocked.Status != StatusBlocked || blocked.Barrier == nil || blocked.Barrier.DispatchStarted || blocked.Barrier.HoldVersion == 0 ||
		!slices.ContainsFunc(blocked.Blockers, func(b Blocker) bool { return b.Code == BlockerSourceDiff }) ||
		!slices.Contains(blocked.AllowedActions, ActionCancel) {
		t.Fatalf("a re-diff refusal before dispatch: %+v", blocked)
	}
	var canceled Operation
	rg.must(http.MethodPost, "/v1/operations/"+blocked.ID+"/cancel", struct{}{}, &canceled)
	wake()
	waitNotRunning(t, rg.ctl, blocked.ID)
	if canceled.Status != StatusCancelled {
		t.Fatalf("canceled purge: %+v", canceled)
	}
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if p.Barrier != nil || p.State != directory.StateCutover {
		t.Fatalf("cancellation left the placement %+v", p)
	}
	for _, key := range []string{"a", "dir/b", "late"} {
		if !rg.vast01.has("data01", key) {
			t.Fatalf("a purge canceled before dispatch deleted %s from the source", key)
		}
	}
	// The bucket is free: the mover can run on it again.
	rg.must(http.MethodPost, "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 3, Done: true, Converged: true}, nil)
	var again PurgeDryRun
	rg.must(http.MethodPost, "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &again)
	if again.Allowed || len(again.Missing) != 1 || again.Missing[0] != "late" {
		t.Fatalf("dry run after the canceled purge: %+v", again)
	}
}

// A local uncertainty is a permanent blocker, distinct from a missing proxy. Canceling releases
// the hold but does not erase the evidence from this incarnation.
func TestLocalBarrierReportsUnknownBackendOutcome(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	updates := operationUpdates(rg)
	gates := admission.New()
	previousInstall := rg.dir.OnInstall
	rg.dir.OnInstall = func(snap *directory.Snapshot) {
		closures, _ := admission.Barriers(snap)
		gates.Apply(closures, false)
		previousInstall(snap)
	}
	rg.ctl.LocalGates = gates
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep
	token, _, _ := gates.Enter("acme/data01", admission.Mutations)
	token.Release(gates, admission.Uncertain)
	var started Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":0.5,"wait":"0s"}`)}, &started); code != http.StatusAccepted {
		t.Fatalf("starting the ramp: %d %s", code, raw)
	}
	blocked := waitOperation(t, updates, started.ID, func(op Operation) bool { return op.Status == StatusBlocked && len(op.Blockers) > 0 })
	if len(blocked.Blockers) != 1 || blocked.Blockers[0].Code != BlockerBackendOutcomeUnknown || blocked.Blockers[0].Count != 1 || blocked.Blockers[0].ProxyID != "lab" {
		t.Fatalf("unknown outcome blocker: %+v", blocked.Blockers)
	}
	var canceled Operation
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/cancel", struct{}{}, &canceled)
	wake()
	waitNotRunning(t, rg.ctl, started.ID)
	if canceled.Status != StatusCancelled || gates.Uncertain() != 1 {
		t.Fatalf("cancellation erased uncertainty: operation %+v, uncertainty %d", canceled, gates.Uncertain())
	}
}
