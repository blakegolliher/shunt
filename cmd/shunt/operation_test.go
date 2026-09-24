package main

import (
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
		Sequence: 1, EffectState: control.EffectNone, Created: time.Now().UTC(), Updated: time.Now().UTC()}
	if err := ctl.Ops.Create(t.Context(), &planted); err != nil {
		t.Fatal(err)
	}
	_, err = rg.cli(t, "operation", "wait", planted.ID, "--timeout", "300ms")
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != exitWaitDeadline || !strings.Contains(err.Error(), "still running") {
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
}
