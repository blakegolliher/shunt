package control

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

func TestExternalMoverSessionExpiresUnresolvedAndReconciles(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)

	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	rg.ctl.Now = func() time.Time { return now }
	rg.ctl.WorkerTTL = time.Second
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep
	updates := operationUpdates(rg)

	const session = "0123456789abcdef0123456789abcdef"
	args, _ := json.Marshal(MoverRequest{External: true, Session: session, Wait: "0s"})
	var started Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpMover, Placement: "acme/data01", Args: args}, &started); code != http.StatusAccepted {
		t.Fatalf("start external mover: HTTP %d %s", code, raw)
	}
	file := rg.dir.Snapshot().File()
	beat := WorkerHeartbeat{Session: session, Identity: file.Identity,
		Generation: file.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Inflight: 1}
	var answer WorkerHeartbeatAnswer
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, &answer)
	if answer.Worker.State != WorkerActive || answer.Worker.Inflight != 1 || answer.Hold {
		t.Fatalf("active worker: %+v", answer)
	}
	wake()
	active := waitOperation(t, updates, started.ID, func(op Operation) bool {
		return op.Status == StatusBlocked && len(op.Blockers) == 1 && op.Blockers[0].Code == BlockerOldRequests
	})
	if active.Worker == nil || active.Worker.Sequence != 1 {
		t.Fatalf("worker was not durable on its operation: %+v", active)
	}

	now = now.Add(2 * time.Second)
	wake()
	expired := waitOperation(t, updates, started.ID, func(op Operation) bool {
		return op.Status == StatusBlocked && len(op.Blockers) == 1 && op.Blockers[0].Code == BlockerWorkerUnresolved
	})
	if !slices.Contains(expired.AllowedActions, ActionResume) && expired.Terminal() {
		t.Fatalf("expired worker was not left recoverable: %+v", expired)
	}
	beat.Sequence = 2
	rg.refused(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, "with resolve and an attestation")
	beat.Resolve, beat.Attestation, beat.Complete, beat.Inflight = true, "the mover process stopped and every backend request was observed", true, 0
	rg.must(http.MethodPost, "/v1/operations/"+started.ID+"/worker-heartbeat", beat, &answer)
	if answer.Worker.State != WorkerCompleted || answer.Worker.ResolvedBy == "" {
		t.Fatalf("reconciled worker: %+v", answer)
	}
	wake()
	done := rg.await(started.ID)
	if done.Status != StatusSucceeded || done.Worker == nil || done.Worker.State != WorkerCompleted {
		t.Fatalf("completed external mover: %+v", done)
	}
}

func TestWorkerHeartbeatBindsLineageGenerationAndSequence(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep

	const session = "fedcba9876543210fedcba9876543210"
	args, _ := json.Marshal(MoverRequest{External: true, Session: session, Wait: "0s"})
	var op Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpMover, Placement: "acme/data01", Args: args}, &op); code != http.StatusAccepted {
		t.Fatalf("start external mover: HTTP %d %s", code, raw)
	}
	f := rg.dir.Snapshot().File()
	beat := WorkerHeartbeat{Session: session, Identity: f.Identity,
		Generation: f.Generation(directory.PlacementResource("acme/data01")) + 1, Sequence: 1, Inflight: 1}
	rg.refused(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, "another directory lineage or placement generation")
	beat.Generation--
	var answer WorkerHeartbeatAnswer
	rg.must(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, &answer)
	beat.Sequence = 0
	rg.answers(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, http.StatusBadRequest, "bad_request")
	beat.Sequence = 1
	rg.must(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, &answer) // exact retry is idempotent
	beat.Inflight = 2
	rg.refused(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, "already used with different state")
	beat.Inflight = 1
	beat.Sequence, beat.Complete, beat.Inflight = 2, true, 0
	rg.must(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, &answer)
	wake()
	if done := rg.await(op.ID); done.Status != StatusSucceeded {
		t.Fatalf("completed worker: %+v", done)
	}
}

func TestExternalMoverUncertainBackendOutcomeNeedsResolution(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	sleep, wake := wakingSleep()
	rg.ctl.Sleep = sleep
	updates := operationUpdates(rg)

	const session = "abcdef0123456789abcdef0123456789"
	args, _ := json.Marshal(MoverRequest{External: true, Session: session, Wait: "0s"})
	var op Operation
	if code, raw := rg.call(http.MethodPost, "/v1/operations", OperationRequest{Kind: OpMover, Placement: "acme/data01", Args: args}, &op); code != http.StatusAccepted {
		t.Fatalf("start external mover: HTTP %d %s", code, raw)
	}
	f := rg.dir.Snapshot().File()
	beat := WorkerHeartbeat{Session: session, Identity: f.Identity,
		Generation: f.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Uncertain: 1}
	var answer WorkerHeartbeatAnswer
	rg.must(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, &answer)
	wake()
	waitOperation(t, updates, op.ID, func(op Operation) bool {
		return op.Status == StatusBlocked && len(op.Blockers) == 1 && op.Blockers[0].Code == BlockerBackendOutcomeUnknown
	})

	beat.Sequence, beat.Uncertain = 2, 0
	beat.Resolve, beat.Attestation, beat.Complete = true, "the failed backend request was checked and did not take effect", true
	rg.must(http.MethodPost, "/v1/operations/"+op.ID+"/worker-heartbeat", beat, &answer)
	wake()
	if done := rg.await(op.ID); done.Status != StatusSucceeded {
		t.Fatalf("resolved external mover: %+v", done)
	}
}
