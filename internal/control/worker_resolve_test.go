package control

// Regressions for resolve-worker across control nodes: the worker session is written by a compare-
// and-swap on the durable record, never by merging an owner's copy, so a resolution that answers
// 200 is on the record, and a completed session is final.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// peer is a second control node over the rig's directory and operation store: it has no tracker
// for the operations the rig's node runs, as a node the load balancer picked would not.
func (rg *rig) peer(node string, now func() time.Time, ttl time.Duration) *rig {
	rg.t.Helper()
	ctl := &Server{Dir: rg.dir, Clusters: rg.ctl.Clusters, Metrics: telemetry.NewMetrics(), Log: slog.New(slog.DiscardHandler),
		Now: now, WorkerTTL: ttl, Events: rg.events, Ops: rg.ctl.Ops, Node: node}
	api := httptest.NewServer(ctl.Handler())
	rg.t.Cleanup(api.Close)
	p := *rg
	p.ctl, p.api = ctl, api
	return &p
}

// parkingSleep is a Sleep that reports each time a run parks in it and returns only when woken, so
// a test knows the owner is writing nothing.
func parkingSleep() (sleep func(context.Context, time.Duration) error, parked <-chan struct{}, wake func()) {
	in := make(chan struct{}, 64)
	out := make(chan struct{}, 64)
	return func(ctx context.Context, _ time.Duration) error {
			in <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-out:
				return nil
			}
		}, in, func() {
			out <- struct{}{}
		}
}

func waitParked(t *testing.T, parked <-chan struct{}) {
	t.Helper()
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("the owner's run did not park")
	}
}

func tokenActor(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "token:" + hex.EncodeToString(sum[:6])
}

// startExternal starts an external mover on acme/data01 through rg's node.
func (rg *rig) startExternal(session string) Operation {
	rg.t.Helper()
	args, _ := json.Marshal(MoverRequest{External: true, Session: session, Wait: "0s"})
	var op Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpMover, Placement: "acme/data01", Args: args}, &op); code != http.StatusAccepted {
		rg.t.Fatalf("start external mover: HTTP %d %s", code, raw)
	}
	return op
}

func (rg *rig) record(id string) *Operation {
	rg.t.Helper()
	op, err := rg.ctl.ops().Get(context.Background(), id)
	if err != nil || op == nil {
		rg.t.Fatalf("operation %s: %v %v", id, op, err)
	}
	return op
}

// The owner's tracker is behind the durable record when a heartbeat went through another node.
// A resolution through the owner must still land on the record, in one request, and the run
// must end failed with the evidence and free the placement.
func TestResolveWorkerAfterHeartbeatThroughAnotherNode(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	var clock atomic.Int64
	clock.Store(time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	rg.ctl.Now, rg.ctl.WorkerTTL = now, time.Second
	sleep, parked, wake := parkingSleep()
	rg.ctl.Sleep = sleep
	b := rg.peer("b", now, time.Second)

	const session = "11112222333344445555666677778888"
	started := rg.startExternal(session)
	waitParked(t, parked)

	file := rg.dir.Snapshot().File()
	beat := WorkerHeartbeat{Session: session, Identity: file.Identity,
		Generation: file.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Inflight: 1, Uncertain: 1}
	b.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, nil)
	stored := rg.record(started.ID)
	rg.ctl.mu.Lock()
	tr := rg.ctl.running[started.ID]
	rg.ctl.mu.Unlock()
	if tr == nil || tr.snapshot().Sequence >= stored.Sequence {
		t.Fatalf("precondition: the owner's tracker should be behind the durable record at %d", stored.Sequence)
	}

	clock.Add(int64(2 * time.Second))
	const why = "mover host powered off at 12:00; the backend request log shows nothing from it since"
	const token = "operator-7"
	h := http.Header{HeaderIdempotencyKey: {"resolve-1"}, "Authorization": {"Bearer " + token}}
	if code, raw := rg.callWith(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", h, ResolveWorkerRequest{Session: session, Attestation: why}, nil); code != http.StatusOK {
		t.Fatalf("resolve-worker: HTTP %d %s", code, raw)
	}
	w := rg.record(started.ID).Worker
	if w == nil || w.ID != session || w.State != WorkerCompleted || w.ResolvedBy != tokenActor(token) || w.Attestation != why {
		t.Fatalf("resolve-worker answered 200 but the durable record's worker is %+v", w)
	}

	// The old mover's late heartbeat, through either node, cannot reopen a resolved session.
	beat.Sequence = 3
	b.refused(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, "has completed")
	rg.refused(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, "has completed")
	// Repeating the same resolution is the same answer; a different one is refused.
	if code, raw := rg.callWith(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", http.Header{HeaderIdempotencyKey: {"resolve-2"}, "Authorization": {"Bearer " + token}},
		ResolveWorkerRequest{Session: session, Attestation: why}, nil); code != http.StatusOK {
		t.Fatalf("repeated resolve-worker: HTTP %d %s", code, raw)
	}
	b.refused(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", ResolveWorkerRequest{Session: session, Attestation: "another story"}, "has completed")

	wake()
	done := rg.await(started.ID)
	if done.Status != StatusFailed || done.Error == nil || !strings.Contains(done.Error.Message, "resolved by "+tokenActor(token)) ||
		done.Worker == nil || done.Worker.State != WorkerCompleted || done.Worker.Attestation != why || done.Worker.ResolvedBy != tokenActor(token) {
		t.Fatalf("the resolved mover's record: %+v worker %+v", done, done.Worker)
	}
	// A late heartbeat on the ended record changes nothing.
	b.refused(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, "has ended")
	if w := rg.record(started.ID).Worker; w == nil || w.State != WorkerCompleted || w.Attestation != why {
		t.Fatalf("the ended record's worker changed: %+v", w)
	}
	next := b.startExternal("99998888777766665555444433332222")
	if next.Terminal() {
		t.Fatalf("a new mover after the resolution: %+v", next)
	}
}

// interceptOps runs a hook just before each of the next `times` Updates that match accepts.
type interceptOps struct {
	Operations
	mu     sync.Mutex
	times  int
	match  func(*Operation) bool
	before func()
}

func (o *interceptOps) Update(ctx context.Context, op *Operation) error {
	o.mu.Lock()
	fn := o.before
	if fn != nil && o.times > 0 && o.match(op) {
		o.times--
	} else {
		fn = nil
	}
	o.mu.Unlock()
	if fn != nil {
		fn()
	}
	return o.Operations.Update(ctx, op)
}

// A worker that enrolls, through another node, between a resolution's read and its write is live:
// the resolution re-reads the record on the lost compare-and-swap and is refused, and the live
// session stays on the record.
func TestResolveWorkerRefusedWhenAWorkerEnrollsMeanwhile(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	var clock atomic.Int64
	clock.Store(time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	rg.ctl.Now, rg.ctl.WorkerTTL = now, time.Second
	sleep, parked, wake := parkingSleep()
	rg.ctl.Sleep = sleep
	b := rg.peer("b", now, time.Second)
	intercept := &interceptOps{Operations: rg.ctl.Ops}
	rg.ctl.Ops = intercept

	const session = "aaaabbbbccccddddeeeeffff00001111"
	started := rg.startExternal(session)
	waitParked(t, parked)
	clock.Add(int64(2 * time.Second)) // never enrolled within its TTL: resolvable

	file := rg.dir.Snapshot().File()
	beat := WorkerHeartbeat{Session: session, Identity: file.Identity,
		Generation: file.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Inflight: 1}
	var enrolled atomic.Int32
	intercept.mu.Lock()
	intercept.times = 1
	intercept.match = func(op *Operation) bool { return op.Worker != nil && op.Worker.ResolvedBy != "" }
	intercept.before = func() {
		if code, _ := b.call(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, nil); code == http.StatusOK {
			enrolled.Store(1)
		}
	}
	intercept.mu.Unlock()
	rg.refused(http.MethodPost, "/v1/operations/"+started.ID+"/resolve-worker", ResolveWorkerRequest{Session: session, Attestation: "looked dead"}, "is live")
	if enrolled.Load() != 1 {
		t.Fatal("the worker's enrolment through the other node did not land")
	}
	if w := rg.record(started.ID).Worker; w == nil || w.State != WorkerActive || w.ResolvedBy != "" || w.Inflight != 1 {
		t.Fatalf("the live worker was overwritten: %+v", w)
	}
	// The live session goes on and ends itself.
	beat.Sequence, beat.Inflight, beat.Complete = 2, 0, true
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, nil)
	wake()
	if done := rg.await(started.ID); done.Status != StatusSucceeded || done.Worker == nil || done.Worker.Sequence != 2 {
		t.Fatalf("the live mover's record: %+v", done)
	}
}

// A completed session is final: a later sequence is refused, an exact repeat of the completing
// heartbeat answers as before, and after the record ends the same holds.
func TestCompletedWorkerSessionIsFinal(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	sleep, parked, wake := parkingSleep()
	rg.ctl.Sleep = sleep
	b := rg.peer("b", rg.ctl.Now, 0)

	const session = "0123012301230123abcdabcdabcdabcd"
	started := rg.startExternal(session)
	waitParked(t, parked)
	file := rg.dir.Snapshot().File()
	beat := WorkerHeartbeat{Session: session, Identity: file.Identity,
		Generation: file.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Inflight: 1}
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, nil)
	final := beat
	final.Sequence, final.Inflight, final.Complete = 2, 0, true
	b.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", final, nil)

	late := beat
	late.Sequence = 3
	rg.refused(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", late, "has completed")
	b.refused(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", late, "has completed")
	var again WorkerHeartbeatAnswer
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", final, &again) // a lost answer, retried
	if again.Worker.State != WorkerCompleted || again.Worker.Sequence != 2 {
		t.Fatalf("repeated completion: %+v", again)
	}
	wake()
	if done := rg.await(started.ID); done.Status != StatusSucceeded {
		t.Fatalf("completed mover: %+v", done)
	}
	b.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", final, nil)
	b.refused(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", late, "has ended")
	if w := rg.record(started.ID).Worker; w == nil || w.State != WorkerCompleted || w.Sequence != 2 {
		t.Fatalf("the ended record's worker changed: %+v", w)
	}
}

// Worker heartbeats through another node that land between each of the owner's reads and writes
// do not take the record from its owner: the owner takes the session from the record and writes
// its own fields over the newer sequence, and the run ends as the worker says.
func TestOwnerKeepsItsRecordUnderWorkerHeartbeats(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	sleep, parked, wake := parkingSleep()
	rg.ctl.Sleep = sleep
	b := rg.peer("b", rg.ctl.Now, 0)
	intercept := &interceptOps{Operations: rg.ctl.Ops}
	rg.ctl.Ops = intercept

	const session = "cafecafecafecafe0000111122223333"
	started := rg.startExternal(session)
	waitParked(t, parked)
	file := rg.dir.Snapshot().File()
	beat := WorkerHeartbeat{Session: session, Identity: file.Identity,
		Generation: file.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Inflight: 1}
	b.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, nil)

	// Each of the owner's next three writes loses to a heartbeat through b.
	var seq atomic.Int64
	seq.Store(1)
	var beats atomic.Int32
	intercept.mu.Lock()
	intercept.times = 3
	intercept.match = func(op *Operation) bool { return op.ID == started.ID }
	intercept.before = func() {
		next := beat
		next.Sequence = seq.Add(1)
		if code, _ := b.call(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", next, nil); code == http.StatusOK {
			beats.Add(1)
		}
	}
	intercept.mu.Unlock()
	wake()
	waitParked(t, parked)
	rg.ctl.mu.Lock()
	tr := rg.ctl.running[started.ID]
	rg.ctl.mu.Unlock()
	if tr == nil || tr.check() != nil {
		t.Fatalf("the owner gave up its record after %d heartbeats through b landed between its writes", beats.Load())
	}
	if beats.Load() != 3 {
		t.Fatalf("heartbeats through b between the owner's writes: %d, want 3", beats.Load())
	}
	op := rg.record(started.ID)
	if op.Status != StatusBlocked || len(op.Blockers) != 1 || op.Blockers[0].Code != BlockerOldRequests || op.Worker == nil || op.Worker.Sequence != 4 {
		t.Fatalf("the owner's write over the heartbeats: %+v worker %+v", op, op.Worker)
	}

	final := beat
	final.Sequence, final.Inflight, final.Complete = 5, 0, true
	b.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", final, nil)
	wake()
	if done := rg.await(started.ID); done.Status != StatusSucceeded || done.Worker == nil || done.Worker.Sequence != 5 {
		t.Fatalf("the mover's record: %+v", done)
	}
}
