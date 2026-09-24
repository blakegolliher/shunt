package control

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// Every route that changes a placement, or a cluster the placement touches, is refused with 409
// operation_conflict naming the unfinished operation that owns it, before anything changes; once
// that operation ends, the same step goes ahead (ADR-0021).
func TestScopeReservationsRefuseConflictingRoutes(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	ctx := context.Background()
	planted := rg.ctl.placementOp("acme/data01")
	planted.ID, planted.Kind, planted.Node, planted.Status, planted.Sequence = "1700000000000-000001", OpMover, "lab", StatusRunning, 1
	planted.Identity, planted.EffectState = rg.ctl.Dir.Snapshot().File().Identity, EffectNone
	if err := rg.ctl.ops().Create(ctx, &planted); err != nil {
		t.Fatal(err)
	}
	if got := planted.Scope.Clusters; len(got) != 2 || got[0] != "vast01" || got[1] != "vast02" {
		t.Fatalf("acme/data01 touches %v, want vast01 and vast02", got)
	}
	v := rg.ctl.Dir.Snapshot().Version()
	routes := []struct {
		method, path string
		body         any
	}{
		{"POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}},
		{"POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}},
		{"POST", "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio":0.5}`)}},
		{"POST", "/v1/placements/acme/data01/read-only", ReadOnlyRequest{ReadOnly: true}},
		{"POST", "/v1/placements/acme/data01/prefixes", map[string]string{"prefix": "logs/"}},
		{"DELETE", "/v1/placements/acme/data01/target", nil},
		{"DELETE", "/v1/placements/acme/data01", nil},
		{"POST", "/v1/clusters/vast01/read-only", ReadOnlyRequest{ReadOnly: true}},
		{"POST", "/v1/clusters/vast02/read-only", ReadOnlyRequest{ReadOnly: true}},
	}
	for _, r := range routes {
		code, raw := rg.call(r.method, r.path, r.body, nil)
		var e Error
		_ = json.Unmarshal([]byte(raw), &e)
		if code != http.StatusConflict || e.Code != CodeOperationConflict || e.OperationID != planted.ID {
			t.Errorf("%s %s: HTTP %d %s, want 409 operation_conflict naming %s", r.method, r.path, code, raw, planted.ID)
		}
	}
	if got := rg.ctl.Dir.Snapshot().Version(); got != v {
		t.Fatalf("refused routes changed the directory: version %d → %d", v, got)
	}

	planted.Status, planted.EffectState, planted.Sequence = StatusSucceeded, EffectNone, 2
	if err := rg.ctl.ops().Update(ctx, &planted); err != nil {
		t.Fatal(err)
	}
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, nil)
}
