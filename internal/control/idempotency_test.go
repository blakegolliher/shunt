package control

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A request that creates an operation needs an Idempotency-Key. A retry with the same key and the
// same intent answers with the first request's record or outcome and runs nothing twice; the same
// key with another intent is refused naming the first operation.
func TestIdempotencyOnTheAPI(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	key := func(k string) http.Header { return http.Header{HeaderIdempotencyKey: {k}} }
	errOf := func(raw string) Error {
		var e Error
		_ = json.Unmarshal([]byte(raw), &e)
		return e
	}

	ramp := OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio": 0.25}`)}
	if code, raw := rg.callWith("POST", "/v1/operations", nil, ramp, nil); code != http.StatusBadRequest || errOf(raw).Code != CodeIdempotencyKeyRequired {
		t.Fatalf("no key: %d %s", code, raw)
	}
	if code, raw := rg.callWith("POST", "/v1/operations", key("bad key!"), ramp, nil); code != http.StatusBadRequest {
		t.Fatalf("a malformed key: %d %s", code, raw)
	}
	var first Operation
	code, _ := rg.callWith("POST", "/v1/operations", key("ramp-1"), ramp, &first)
	if code != http.StatusAccepted || first.RequestID != "ramp-1" || first.IntentDigest != "" {
		t.Fatalf("first: %d %+v (the digest must not be answered)", code, first)
	}
	done := rg.await(first.ID)
	v := rg.ctl.Dir.Snapshot().Version()

	// The same request again, keys in another order: the same record, not a second step.
	same := OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{ "ratio" : 0.25 }`)}
	var again Operation
	if code, raw := rg.callWith("POST", "/v1/operations", key("ramp-1"), same, &again); code != http.StatusOK || again.ID != first.ID || again.Status != done.Status {
		t.Fatalf("a retry: %d %s, want 200 with %s", code, raw, first.ID)
	}
	if got := rg.ctl.Dir.Snapshot().Version(); got != v {
		t.Fatalf("a retry wrote the directory: version %d → %d", v, got)
	}
	other := OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio": 0.5}`)}
	if code, raw := rg.callWith("POST", "/v1/operations", key("ramp-1"), other, nil); code != http.StatusConflict || errOf(raw).Code != CodeIdempotencyConflict || errOf(raw).OperationID != first.ID {
		t.Fatalf("the key for another request: %d %s", code, raw)
	}

	// A route alias replays the outcome the route gave, and runs nothing.
	var res1, res2 TransitionResult
	if code, raw := rg.callWith("POST", "/v1/placements/acme/data01/ramp", key("alias-1"), RampRequest{Ratio: 0.5}, &res1); code != http.StatusOK {
		t.Fatalf("alias: %d %s", code, raw)
	}
	v = rg.ctl.Dir.Snapshot().Version()
	if code, raw := rg.callWith("POST", "/v1/placements/acme/data01/ramp", key("alias-1"), RampRequest{Ratio: 0.5}, &res2); code != http.StatusOK || res2.Version != res1.Version || res2.Operation != res1.Operation {
		t.Fatalf("alias retry: %d %s, want the first answer %+v", code, raw, res1)
	}
	if got := rg.ctl.Dir.Snapshot().Version(); got != v {
		t.Fatalf("an alias retry wrote the directory: version %d → %d", v, got)
	}
	// A retry of a refused request answers the refusal again.
	code1, raw1 := rg.callWith("POST", "/v1/placements/acme/data01/ramp", key("alias-2"), RampRequest{Ratio: 0.1}, nil)
	code2, raw2 := rg.callWith("POST", "/v1/placements/acme/data01/ramp", key("alias-2"), RampRequest{Ratio: 0.1}, nil)
	if code1 != http.StatusConflict || code2 != code1 || errOf(raw2).Message != errOf(raw1).Message {
		t.Fatalf("a refused request and its retry: %d %s / %d %s", code1, raw1, code2, raw2)
	}

	// If-Generation: a request made against a generation the placement has moved past.
	gen := rg.ctl.Dir.Snapshot().File().Generation("placement:acme/data01")
	h := key("gen-1")
	h.Set(HeaderIfGeneration, strconv.FormatInt(gen-1, 10))
	if code, raw := rg.callWith("POST", "/v1/placements/acme/data01/ramp", h, RampRequest{Ratio: 0.75}, nil); code != http.StatusConflict ||
		errOf(raw).Code != CodeGenerationConflict || errOf(raw).CurrentGeneration != strconv.FormatInt(gen, 10) {
		t.Fatalf("a stale If-Generation: %d %s", code, raw)
	}
	h = key("gen-2")
	h.Set(HeaderIfGeneration, strconv.FormatInt(gen, 10))
	if code, raw := rg.callWith("POST", "/v1/placements/acme/data01/ramp", h, RampRequest{Ratio: 0.75}, nil); code != http.StatusOK {
		t.Fatalf("the current If-Generation: %d %s", code, raw)
	}

	// A short recorded route: the same, and a secret in its body never reaches the record.
	cl := ClusterRequest{Name: "vast03", Cluster: rg.vast02.definition(true), Secret: "sekret-value"}
	if code, raw := rg.callWith("POST", "/v1/clusters", key("add-3"), cl, nil); code != http.StatusOK {
		t.Fatalf("cluster add: %d %s", code, raw)
	}
	var list OperationList
	rg.must("GET", "/v1/operations?cluster=vast03", nil, &list)
	if len(list.Operations) != 1 || strings.Contains(string(mustJSON(list)), "sekret-value") {
		t.Fatalf("cluster add's record: %+v", list.Operations)
	}
	stored, _ := rg.ctl.ops().Get(t.Context(), list.Operations[0].ID)
	if stored == nil || stored.IntentDigest == "" || strings.Contains(string(mustJSON(stored)), "sekret-value") {
		t.Fatalf("the stored record: %+v", stored)
	}
	if code, raw := rg.callWith("POST", "/v1/clusters", key("add-3"), cl, nil); code != http.StatusOK {
		t.Fatalf("cluster add retry: %d %s", code, raw)
	}
	cl.Secret = "another"
	if code, raw := rg.callWith("POST", "/v1/clusters", key("add-3"), cl, nil); code != http.StatusConflict || errOf(raw).Code != CodeIdempotencyConflict {
		t.Fatalf("cluster add with the key and another secret: %d %s", code, raw)
	}
}

// At the limit on unfinished operations, a new one answers 429 before anything runs.
func TestOperationCapacityOnTheAPI(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	mem := rg.ctl.ops().(*MemOperations)
	mem.Capacity = 1
	planted := rg.ctl.clusterOp("vast02")
	planted.ID, planted.Kind, planted.Node, planted.Status, planted.Sequence = "1700000000000-00000a", OpClusterReadOnly, "lab", StatusRunning, 1
	planted.Identity, planted.EffectState = rg.ctl.Dir.Snapshot().File().Identity, EffectNone
	if err := mem.Create(t.Context(), &planted); err != nil {
		t.Fatal(err)
	}
	code, raw := rg.call("POST", "/v1/clusters/vast01/read-only", ReadOnlyRequest{ReadOnly: true}, nil)
	var e Error
	_ = json.Unmarshal([]byte(raw), &e)
	if code != http.StatusTooManyRequests || e.Code != CodeOperationCapacity {
		t.Fatalf("at capacity: %d %s", code, raw)
	}
	rg.must("GET", "/v1/operations", nil, &OperationList{}) // status stays available
}

// An ended record keeps its idempotency key, and so stays past the history limit, for the
// retention window; then it may go.
func TestPrunableHonorsIdempotencyRetention(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	op := Operation{Status: StatusSucceeded, EffectState: EffectCommitted, RequestID: "k", Updated: now}
	if op.Prunable(now.Add(IdempotencyRetention - time.Second)) {
		t.Error("a keyed record was prunable inside the retention window")
	}
	if !op.Prunable(now.Add(IdempotencyRetention)) {
		t.Error("a keyed record was not prunable after the retention window")
	}
	for _, o := range []Operation{
		{Status: StatusRunning, Updated: now},
		{Status: StatusFailed, EffectState: EffectUncertain, Updated: now},
	} {
		if o.Prunable(now.Add(365 * 24 * time.Hour)) {
			t.Errorf("%s/%s was prunable", o.Status, o.EffectState)
		}
	}
}
