package member

// Offline restarts keep every earlier process's evidence (third review, R3-04): a proxy restarted
// several times while the control plane is unreachable reports each process the control plane never
// heard of, and drops none before the control plane has recorded it.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/control"
)

// offline starts a process of c's proxy on its cache without reaching the control plane, as a proxy
// serving its cache during an outage does, and leaves it running (a crash, as far as the marker is
// concerned).
func offline(t *testing.T, c *Client) *Client {
	t.Helper()
	p := New(c.cfg, c.log)
	if err := p.Load(); err != nil {
		t.Fatal(err)
	}
	return p
}

func ids(incs []control.Incarnation) []string {
	out := make([]string, len(incs))
	for i, inc := range incs {
		out[i] = inc.ID
	}
	return out
}

// The review's chain: a registered process A, then B and C started and crashed while the control
// plane was down, then D retired cleanly while it was still down. The next process R reports A, B
// and C in its first heartbeat, oldest first, each unclean; D is not reported, since the control
// plane never knew it and nothing of it is uncertain. Once the answer says they are recorded, R's
// marker drops them and the next heartbeat carries none. Negative control: a marker that keeps only
// the last process (as before) reports C alone, and B is lost.
func TestOfflineRestartsKeepEveryPredecessor(t *testing.T) {
	f := newFakeControl(t)
	a := newClient(t, f)
	ctx := context.Background()
	if err := a.Register(ctx); err != nil {
		t.Fatal(err)
	}
	b := offline(t, a)
	c := offline(t, a)
	if got := ids(c.predecessorsToReport()); !slices.Equal(got, []string{a.Incarnation(), b.Incarnation()}) {
		t.Fatalf("C's predecessors: %v", got)
	}
	d := offline(t, a)
	unreachable, cancel := context.WithCancel(ctx)
	cancel()
	_ = d.Retire(unreachable, 0) //nolint:errcheck // the control plane is unreachable: the retirement is only on disk
	r := offline(t, a)
	want := []string{a.Incarnation(), b.Incarnation(), c.Incarnation()}
	if got := ids(r.predecessorsToReport()); !slices.Equal(got, want) {
		t.Fatalf("R's predecessors: %v, want %v (D retired cleanly unregistered)", got, want)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/-/fleet", nil))
	var st Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil || !slices.Equal(ids(st.Unrecorded), want) {
		t.Fatalf("/-/fleet before they are recorded: %+v %v", st.Unrecorded, err)
	}
	if err := r.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.beat(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	beats := slices.Clone(f.beats)
	f.mu.Unlock()
	var first, next *control.Heartbeat
	for i := range beats {
		if beats[i].Incarnation == r.Incarnation() {
			if first == nil {
				first = &beats[i]
			} else {
				next = &beats[i]
			}
		}
	}
	if first == nil || next == nil || !slices.Equal(ids(first.Previous), want) || len(next.Previous) != 0 {
		t.Fatalf("R's heartbeats: first %+v, next %+v", first, next)
	}
	for _, p := range first.Previous {
		if p.State != control.IncarnationUnclean {
			t.Fatalf("a process that never retired reported %s: %+v", p.State, p)
		}
	}
	if m := readMarker(t, r); len(m.Predecessors) != 0 || !m.Registered {
		t.Fatalf("R's marker once they are recorded: %+v", m)
	}
}

// A backlog larger than a heartbeat takes goes out oldest first, a heartbeat's worth at a time, each
// batch dropped only when recorded; a marker past maxPredecessors refuses to start and is left as it
// was, evidence and all.
func TestPredecessorBacklogAndBound(t *testing.T) {
	f := newFakeControl(t)
	a := newClient(t, f)
	ctx := context.Background()
	if err := a.Register(ctx); err != nil {
		t.Fatal(err)
	}
	chain := make([]string, 0, control.MaxUnresolvedIncarnations+3)
	chain = append(chain, a.Incarnation())
	last := a
	for range control.MaxUnresolvedIncarnations + 2 {
		last = offline(t, a)
		chain = append(chain, last.Incarnation())
	}
	r := offline(t, a) // its predecessors: every process in chain
	if got := ids(r.predecessorsToReport()); !slices.Equal(got, chain[:control.MaxUnresolvedIncarnations]) {
		t.Fatalf("the first heartbeat's batch: %v, want the oldest %d", got, control.MaxUnresolvedIncarnations)
	}
	if err := r.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if got := ids(r.predecessorsToReport()); !slices.Equal(got, chain[control.MaxUnresolvedIncarnations:]) {
		t.Fatalf("after the first batch is recorded: %v, want %v", got, chain[control.MaxUnresolvedIncarnations:])
	}
	if err := r.beat(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.predecessorsToReport(); len(got) != 0 {
		t.Fatalf("after the second batch: %v", ids(got))
	}

	// Past the bound: every process of the chain crashed unreported, one more than the marker keeps.
	g := New(a.cfg, a.log)
	g.cfg.CacheDir = t.TempDir()
	if err := g.Load(); err != nil {
		t.Fatal(err)
	}
	for range maxPredecessors {
		g = offline(t, g)
	}
	before, err := os.ReadFile(g.markerPath())
	if err != nil {
		t.Fatal(err)
	}
	over := New(g.cfg, g.log)
	if err := over.Load(); err == nil || !strings.Contains(err.Error(), "ended without the control plane recording them") {
		t.Fatalf("a process past the bound: %v", err)
	}
	after, _ := os.ReadFile(g.markerPath())
	if string(after) != string(before) {
		t.Fatal("a refused start rewrote the marker: evidence may be gone")
	}
}
