package proxy

// A request that takes its runtime bundle before a drain barrier and reaches its bucket's gate
// after the barrier has closed and reopened it (third review, R3-01): refused as superseded, never
// admitted by the route the barrier replaced.

import (
	"context"
	"net/http"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// straddle publishes a bundle whose key table runs cycle the first time a request looks its key up:
// the request has taken that bundle, and the whole barrier runs before it goes on.
func straddle(t *testing.T, m *mixedRig, cycle func()) {
	t.Helper()
	keys := mapStore{acmeAK: {AccessKey: acmeAK, Secret: acmeSK, Tenant: "acme"}}
	hook := &hookStore{mapStore: keys, hook: cycle}
	if err := m.h.Runtime.Refresh(m.dir.Snapshot(), hook, m.set.Load()); err != nil {
		t.Fatal(err)
	}
}

// drained checks the barrier is closed on this proxy with nothing in flight: the proof the control
// node would commit on.
func drained(t *testing.T, m *mixedRig, id string) {
	t.Helper()
	if a := ackOf(t, m, id); !a.Closed || a.Inflight != 0 || a.Uncertain != 0 {
		t.Fatalf("barrier %s not drained: %+v", id, a)
	}
}

// Each barrier a proxy's gates enforce, run whole while a request holds the bundle it took before
// the hold: a scoped ramp step (the review's case: the PUT would land on the source after the step
// copied it to the target), a cluster read-only change (the PUT would reach a cluster that is now
// read-only), purge-source's source barrier (a CUTOVER delete's source leg, through the source
// gate), and a spread bucket's DeleteObjects across a read-only change. Each is refused with 503
// superseded, counted, and the retry is answered by the current route.
func TestOldRouteCannotCrossABarrier(t *testing.T) {
	cases := []struct {
		name   string
		bucket string
		setup  func(t *testing.T, m *mixedRig)
		cycle  func(t *testing.T, m *mixedRig)
		send   func(t *testing.T, m *mixedRig) reply
		retry  func(t *testing.T, m *mixedRig)
	}{{
		name:   "scoped ramp step",
		bucket: "data",
		setup:  func(_ *testing.T, m *mixedRig) { m.garage.put("acme-1111-data", "moved/k", []byte("old")) },
		cycle: func(t *testing.T, m *mixedRig) {
			target := ramp(t, m, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}, Hold: true, Barrier: "cycle-ramp"})
			drained(t, m, "cycle-ramp")
			value, _ := m.garage.object("acme-1111-data", "moved/k")
			m.minio.put(target, "moved/k", value) // the step's copy, safe only because the gate drained
			if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateRamping, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}, Complete: true}, "test"); err != nil {
				t.Fatal(err)
			}
		},
		send: func(t *testing.T, m *mixedRig) reply { return m.acme(t, "PUT", "/data/moved/k", []byte("new")) },
		retry: func(t *testing.T, m *mixedRig) {
			if r := m.acme(t, "PUT", "/data/moved/k", []byte("new")); r.StatusCode != http.StatusOK {
				t.Fatalf("the retry on the current route: %d %s", r.StatusCode, r.body)
			}
			if r := m.acme(t, "GET", "/data/moved/k", nil); string(r.body) != "new" {
				t.Fatalf("after the retry: GET %q, want new", r.body)
			}
		},
	}, {
		name:   "cluster read-only",
		bucket: "data",
		cycle: func(t *testing.T, m *mixedRig) {
			if err := m.dir.SetClusterReadOnly(context.Background(), "garage", true, false, "cycle-ro", "test"); err != nil {
				t.Fatal(err)
			}
			drained(t, m, "cycle-ro")
			if err := m.dir.ClearBarrier(context.Background(), directory.ClusterResource("garage"), "cycle-ro", "test"); err != nil {
				t.Fatal(err)
			}
		},
		send: func(t *testing.T, m *mixedRig) reply { return m.acme(t, "PUT", "/data/k", []byte("after read-only")) },
		retry: func(t *testing.T, m *mixedRig) {
			if r := m.acme(t, "PUT", "/data/k", []byte("after read-only")); r.StatusCode != http.StatusServiceUnavailable || m.garage.holds("acme-1111-data", "k") {
				t.Fatalf("the retry on a read-only cluster: %d %s", r.StatusCode, r.body)
			}
		},
	}, {
		name:   "purge-source's source barrier",
		bucket: "data",
		setup: func(t *testing.T, m *mixedRig) {
			target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
			m.garage.put("acme-1111-data", "k", []byte("v"))
			m.minio.put(target, "k", []byte("v"))
			if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateMigrating, directory.Transition{To: directory.StateCutover}, "test"); err != nil {
				t.Fatal(err)
			}
		},
		cycle: func(t *testing.T, m *mixedRig) {
			if err := m.dir.SetBarrier(context.Background(), "acme", "data", directory.Barrier{ID: "cycle-src", Kind: config.BarrierSource}, "test"); err != nil {
				t.Fatal(err)
			}
			drained(t, m, "cycle-src")
			if err := m.dir.ClearBarrier(context.Background(), directory.PlacementResource("acme/data"), "cycle-src", "test"); err != nil {
				t.Fatal(err)
			}
		},
		// In CUTOVER a delete goes to the source, then the primary: it takes a source token.
		send: func(t *testing.T, m *mixedRig) reply { return m.acme(t, "DELETE", "/data/k", nil) },
		retry: func(t *testing.T, m *mixedRig) {
			if r := m.acme(t, "DELETE", "/data/k", nil); r.StatusCode != http.StatusNoContent || m.garage.holds("acme-1111-data", "k") {
				t.Fatalf("the retry's delete: %d %s", r.StatusCode, r.body)
			}
		},
	}, {
		name:   "spread DeleteObjects across a read-only change",
		bucket: "spread",
		setup:  func(t *testing.T, m *mixedRig) { spread(t, m) },
		cycle: func(t *testing.T, m *mixedRig) {
			if err := m.dir.SetClusterReadOnly(context.Background(), "garage", true, false, "cycle-ro", "test"); err != nil {
				t.Fatal(err)
			}
			drained(t, m, "cycle-ro")
			if err := m.dir.ClearBarrier(context.Background(), directory.ClusterResource("garage"), "cycle-ro", "test"); err != nil {
				t.Fatal(err)
			}
		},
		send: func(t *testing.T, m *mixedRig) reply {
			r, _ := deleteObjects(t, m, "spread", false, "a", "b")
			return r
		},
		retry: func(t *testing.T, m *mixedRig) {
			if r, _ := deleteObjects(t, m, "spread", false, "a", "b"); r.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("the retry against a read-only leg: %d %s", r.StatusCode, r.body)
			}
		},
	}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newMixedRig(t, nil)
			if c.setup != nil {
				c.setup(t, m)
			}
			straddle(t, m, func() { c.cycle(t, m) })
			r := c.send(t, m)
			if r.StatusCode != http.StatusServiceUnavailable || r.Header.Get("Retry-After") == "" {
				t.Fatalf("a request holding the pre-barrier bundle: %d %s", r.StatusCode, r.body)
			}
			if got := counterValue(t, m.h.Metrics.RefusedWrites.WithLabelValues("acme/"+c.bucket, "superseded")); got != 1 {
				t.Fatalf("refused as superseded: %v, want 1", got)
			}
			if st := m.gates.State("acme/" + c.bucket); st.Inflight != [2]int64{} || st.Uncertain != [2]int64{} {
				t.Fatalf("a refused request left tokens behind: %+v", st)
			}
			c.retry(t, m)
		})
	}
}
