package control

import (
	"net/http"
	"testing"
)

// A bucket that is neither spread nor moving is counted by backend only once it is watched; the
// flag is a directory write under an operation record, and setting it twice is a conflict.
func TestWatchRoute(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	var ps PlacementStatus
	rg.must("GET", "/v1/placements/acme/data01/view", nil, &ps)
	if ps.Watch || ps.PerBucket {
		t.Fatalf("before watch: watch %v per-bucket %v", ps.Watch, ps.PerBucket)
	}
	rg.must("POST", "/v1/placements/acme/data01/watch", WatchRequest{Watch: true}, nil)
	rg.must("GET", "/v1/placements/acme/data01/view", nil, &ps)
	if !ps.Watch || !ps.PerBucket {
		t.Fatalf("after watch: watch %v per-bucket %v", ps.Watch, ps.PerBucket)
	}
	if p, _ := rg.ctl.Dir.Snapshot().Lookup("acme", "data01"); !p.Watch {
		t.Fatal("the directory placement is not watched")
	}
	rg.answers("POST", "/v1/placements/acme/data01/watch", WatchRequest{Watch: true}, http.StatusConflict, "conflict")
	var list OperationList
	rg.must("GET", "/v1/operations?placement=acme/data01", nil, &list)
	if len(list.Operations) == 0 || list.Operations[0].Kind != OpWatch {
		t.Fatalf("the watch did not run under an operation record: %+v", list.Operations)
	}
	rg.must("POST", "/v1/placements/acme/data01/watch", WatchRequest{Watch: false}, nil)
	ps = PlacementStatus{}
	rg.must("GET", "/v1/placements/acme/data01/view", nil, &ps)
	if ps.Watch || ps.PerBucket {
		t.Fatalf("after unwatch: watch %v per-bucket %v", ps.Watch, ps.PerBucket)
	}
}
