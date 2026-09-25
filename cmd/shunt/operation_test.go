package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
)

// shunt operation list, show and wait read the records a change ran under; wait exits 1 for an
// operation that did not succeed, and 3, not 1, when its timeout passes with the operation still
// unfinished.
func TestOperationVerbs(t *testing.T) {
	rg := newAPIRig(t)
	if err := rg.vast01.CreateBucket("data"); err != nil {
		t.Fatal(err)
	}
	rg.addCluster(t, "vast01", rg.ep01)
	rg.must(t, "adopt", "vast01", "acme/data")
	out, err := rg.cli(t, "operation", "list", "--placement", "acme/data")
	if err != nil || !strings.Contains(out, "adopt") || !strings.Contains(out, "placement:acme/data") || !strings.Contains(out, "succeeded") {
		t.Fatalf("list: %v\n%s", err, out)
	}
	id := strings.Fields(strings.Split(out, "\n")[1])[0]
	out, err = rg.cli(t, "operation", "show", id)
	if err != nil || !strings.Contains(out, "status:      succeeded") || !strings.Contains(out, "effect:      committed") {
		t.Fatalf("show: %v\n%s", err, out)
	}
	if out, err = rg.cli(t, "operation", "wait", id); err != nil || !strings.Contains(out, "succeeded") {
		t.Fatalf("wait on a succeeded operation: %v\n%s", err, out)
	}

	// An unfinished record: wait exits 3 at its timeout, not 1.
	ctl := rg.ctl
	planted := control.Operation{ID: "1700000000000-0000ff", Kind: control.OpMover, Placement: "acme/other", Node: "lab", Status: control.StatusRunning,
		Sequence: 1, EffectState: control.EffectNone, Created: time.Now().UTC(), Updated: time.Now().UTC(),
		Blockers: []control.Blocker{{Code: control.BlockerProxyMissing, ProxyID: "p9"}}, BlockerCount: 1}
	if err := ctl.Ops.Create(t.Context(), &planted); err != nil {
		t.Fatal(err)
	}
	_, err = rg.cli(t, "operation", "wait", planted.ID, "--timeout", "300ms")
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != exitWaitDeadline || !strings.Contains(err.Error(), "still running") || !strings.Contains(err.Error(), "waiting on proxy_missing p9") {
		t.Fatalf("wait past its timeout: %v, want exit %d", err, exitWaitDeadline)
	}
	planted.Status, planted.Sequence, planted.Error = control.StatusFailed, 2, &control.Error{Code: "refused", Message: "no"}
	if err := ctl.Ops.Update(t.Context(), &planted); err != nil {
		t.Fatal(err)
	}
	_, err = rg.cli(t, "operation", "wait", planted.ID)
	if err == nil || errors.As(err, &ee) || !strings.Contains(err.Error(), "failed: no") {
		t.Fatalf("wait on a failed operation: %v, want exit 1", err)
	}
	if _, err = rg.cli(t, "operation", "show", "1700000000000-nope00"); err == nil {
		t.Fatal("show of an unknown operation succeeded")
	}

	// Resume takes a durable barrier from a lost owner and carries the same record to completion.
	gen := rg.dir.Snapshot().Generation("cluster:vast01")
	resumable := control.Operation{ID: "1700000000001-0000aa", Kind: control.OpClusterReadOnly, Cluster: "vast01", Node: "lost-node",
		Identity: rg.dir.Snapshot().File().Identity, Scope: &control.Scope{Resource: "cluster:vast01", Generation: gen},
		Status: control.StatusBlocked, Phase: control.PhaseDrain, Sequence: 1, EffectState: control.EffectNone,
		AllowedActions: []string{control.ActionResume, control.ActionCancel}, Blockers: []control.Blocker{{Code: control.BlockerOwnerLost}}, BlockerCount: 1,
		Barrier: &control.BarrierState{ID: "1700000000001-0000aa", Scope: "cluster:vast01", Kind: "mutations"},
		Args:    json.RawMessage(`{"read_only":true,"wait":"0s"}`), Created: time.Now().UTC(), Updated: time.Now().UTC()}
	if err := ctl.Ops.Create(t.Context(), &resumable); err != nil {
		t.Fatal(err)
	}
	if out := rg.must(t, "operation", "resume", resumable.ID); !strings.Contains(out, "operation "+resumable.ID+" resumed on lab") {
		t.Fatalf("resume: %s", out)
	}
	if out := rg.must(t, "operation", "wait", resumable.ID); !strings.Contains(out, "succeeded") {
		t.Fatalf("resumed outcome: %s", out)
	}
	if out, err := rg.cli(t, "operation", "cancel", resumable.ID); err == nil || !strings.Contains(out, "has ended succeeded") {
		t.Fatalf("cancel after commit: %v\n%s", err, out)
	}

	// Cancel is exposed as a CLI verb and safely ends a precommit record.
	cancellable := resumable
	cancellable.ID, cancellable.Node, cancellable.OwnerTerm = "1700000000002-0000bb", "lab", 0
	if err := rg.dir.SetClusterReadOnly(t.Context(), "vast01", false, false, "", "test"); err != nil {
		t.Fatal(err)
	}
	if err := rg.dir.SetClusterReadOnly(t.Context(), "vast01", true, false, cancellable.ID, "test"); err != nil {
		t.Fatal(err)
	}
	cancellable.Scope = &control.Scope{Resource: "cluster:vast01", Generation: rg.dir.Snapshot().Generation("cluster:vast01")}
	cancellable.Status, cancellable.Phase, cancellable.Sequence = control.StatusBlocked, control.PhaseDrain, 1
	cancellable.AllowedActions = []string{control.ActionCancel}
	cancellable.Blockers, cancellable.BlockerCount = []control.Blocker{{Code: control.BlockerProxyMissing, ProxyID: "p9"}}, 1
	cancellable.Barrier = &control.BarrierState{ID: cancellable.ID, Scope: "cluster:vast01", Kind: "mutations",
		HoldVersion: rg.dir.Snapshot().Version(), Generation: rg.dir.Snapshot().Generation("cluster:vast01")}
	cancellable.Result, cancellable.Error = nil, nil
	if err := ctl.Ops.Create(t.Context(), &cancellable); err != nil {
		t.Fatal(err)
	}
	if out := rg.must(t, "operation", "cancel", cancellable.ID); !strings.Contains(out, "operation "+cancellable.ID+" canceled; nothing changed") {
		t.Fatalf("cancel: %s", out)
	}
}

// shunt operation resolve-worker (defect 7 of the H2 review): an external mover whose worker
// session expired, and whose owner is gone, is ended by the resolution, which it keeps.
func TestOperationResolveWorkerVerb(t *testing.T) {
	rg := newAPIRig(t)
	if err := rg.vast01.CreateBucket("data"); err != nil {
		t.Fatal(err)
	}
	rg.addCluster(t, "vast01", rg.ep01)
	rg.must(t, "adopt", "vast01", "acme/data")
	const session = "0123456789abcdef0123456789abcdef"
	gen := rg.dir.Snapshot().Generation("placement:acme/data")
	stale := time.Now().UTC().Add(-time.Hour)
	planted := control.Operation{ID: "1700000000003-0000cc", Kind: control.OpMover, Placement: "acme/data", Node: "lab",
		Identity: rg.dir.Snapshot().File().Identity, Scope: &control.Scope{Resource: "placement:acme/data", Generation: gen},
		Status: control.StatusBlocked, Phase: control.PhaseMover, Sequence: 1, EffectState: control.EffectNone,
		Blockers: []control.Blocker{{Code: control.BlockerWorkerUnresolved}}, BlockerCount: 1,
		Args:    json.RawMessage(`{"external":true,"session":"` + session + `","wait":"0s"}`),
		Worker:  &control.WorkerSession{ID: session, Identity: rg.dir.Snapshot().File().Identity, Generation: gen, Sequence: 4, State: control.WorkerActive, Inflight: 1, Uncertain: 1, LastSeen: stale},
		Created: stale, Updated: stale}
	if err := rg.ctl.Ops.Create(t.Context(), &planted); err != nil {
		t.Fatal(err)
	}
	if out := rg.must(t, "operation", "show", planted.ID); !strings.Contains(out, "worker:      "+session+" active, sequence 4, 1 in flight, 1 uncertain") {
		t.Fatalf("show of an expired worker: %s", out)
	}
	if _, err := rg.cli(t, "operation", "resolve-worker", planted.ID, "--session", session); err == nil {
		t.Fatal("resolve-worker without an attestation succeeded")
	}
	out := rg.must(t, "operation", "resolve-worker", planted.ID, "--session", session, "--attest", "mover host is off; backend log is quiet")
	if !strings.Contains(out, "worker session "+session+" of operation "+planted.ID+" resolved") {
		t.Fatalf("resolve-worker: %s", out)
	}
	out, err := rg.cli(t, "operation", "wait", planted.ID)
	if err == nil || !strings.Contains(out, "resolved by") || !strings.Contains(out, "mover host is off") || !strings.Contains(out, "status:      failed") {
		t.Fatalf("the resolved operation: %v\n%s", err, out)
	}
}

func TestWatchVerb(t *testing.T) {
	rg := newAPIRig(t)
	if err := rg.vast01.CreateBucket("data"); err != nil {
		t.Fatal(err)
	}
	rg.addCluster(t, "vast01", rg.ep01)
	rg.must(t, "adopt", "vast01", "acme/data")
	if out := rg.must(t, "watch", "acme/data"); !strings.Contains(out, "acme/data: watch true") {
		t.Fatalf("watch: %s", out)
	}
	if p, _ := rg.dir.Snapshot().Lookup("acme", "data"); !p.Watch {
		t.Fatal("not watched in the directory")
	}
	if out := rg.must(t, "watch", "acme/data", "--off"); !strings.Contains(out, "acme/data: watch false") {
		t.Fatalf("watch --off: %s", out)
	}
}
