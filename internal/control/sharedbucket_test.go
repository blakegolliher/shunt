package control

import (
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// A bucket another client bucket owns is refused as a destination before any request reaches it.
// Found live (2026-09-29): a move named a bucket adopted as a client bucket of its own; its first
// step probed that bucket for conditional writes, recorded the cluster's measured capabilities, and
// only then met the directory's refusal, as an operation that failed with effect committed. Expand,
// a spread bucket's move and a plain bucket's first step now each refuse it with nothing sent to
// that bucket and nothing written; and expand's generated name skips a spread bucket's leg.
// Negative control: without the early checks, the fake sees requests for the other bucket.
func TestADestinationAnotherBucketOwnsIsRefusedFirst(t *testing.T) {
	rg := newRig(t)
	for _, b := range []struct {
		fc   *fakeCluster
		name string
	}{{rg.vast01, "data01"}, {rg.vast02, "taken"}} {
		if err := b.fc.be.CreateBucket(b.name); err != nil {
			t.Fatal(err)
		}
	}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(false)}, nil)
	rg.must("POST", "/v1/placements/default/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.must("POST", "/v1/placements/default/taken/adopt", AdoptRequest{Cluster: "vast02"}, nil)
	rg.must("POST", "/v1/placements/default/wide/create-backend",
		CreateBackendRequest{Legs: []LegRequest{{Cluster: "vast01", Name: "wide"}, {Cluster: "vast02", Name: "data01-001"}}}, nil)

	var mu sync.Mutex
	var touched []string
	rg.vast02.onRequest = func(r *http.Request) {
		if p := strings.TrimPrefix(r.URL.Path, "/"); p == "taken" || strings.HasPrefix(p, "taken/") {
			mu.Lock()
			touched = append(touched, r.Method+" "+r.URL.Path)
			mu.Unlock()
		}
	}
	before := rg.dir.Snapshot()
	for name, tc := range map[string]struct {
		path string
		body any
	}{
		"expand":                    {"/v1/placements/default/data01/expand", ExpandRequest{To: "vast02", Name: "taken", Create: true}},
		"a spread bucket's move":    {"/v1/placements/default/wide/ramp", RampRequest{To: "vast02", Name: "taken", Leg: "vast01", Share: 0.5, Ratio: 0.25, Create: true}},
		"a plain bucket's 1st step": {"/v1/placements/default/data01/ramp", RampRequest{To: "vast02", Name: "taken", Ratio: 0.25, Create: true}},
	} {
		code, raw := rg.call("POST", tc.path, tc.body, nil)
		if code != http.StatusConflict || !strings.Contains(raw, "bucket taken on vast02 is client bucket default/taken's backend bucket") {
			t.Errorf("%s into another client bucket's bucket: HTTP %d %s", name, code, raw)
		}
	}
	mu.Lock()
	if len(touched) != 0 {
		t.Errorf("requests reached the other client bucket's bucket before the refusal: %v", touched)
	}
	mu.Unlock()
	after := rg.dir.Snapshot()
	if after.Version() != before.Version() {
		t.Errorf("a refused destination wrote the directory: version %d, then %d", before.Version(), after.Version())
	}
	cb, _ := before.Cluster("vast02")
	ca, _ := after.Cluster("vast02")
	if !reflect.DeepEqual(ca.Capabilities, cb.Capabilities) {
		t.Errorf("a refused destination recorded vast02's measured capabilities: %+v, before %+v", ca.Capabilities, cb.Capabilities)
	}

	// Expand's generated name is a bucket no placement uses: data01-001 is wide's leg on vast02.
	var ex ExpandResult
	rg.must("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02", Create: true}, &ex)
	if ex.Name != "data01-002" {
		t.Fatalf("expand's generated name: %q, want data01-002 (data01-001 is a spread bucket's leg)", ex.Name)
	}
}
