package proxy

// The admission gates (ADR-0021 D2), driven through the handler: T05 and T06 of the delivery plan.

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// rewriteDirectory edits the rig's directory file as another writer would and reloads it.
func rewriteDirectory(t *testing.T, m *mixedRig, edit func(f *directory.File)) {
	t.Helper()
	f := m.dir.Snapshot().File()
	edit(f)
	f.Version++
	body, err := yaml.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.WriteAtomic(m.dirPath, body); err != nil {
		t.Fatal(err)
	}
	if _, err := m.dir.Reload(); err != nil {
		t.Fatal(err)
	}
}

// blockPuts makes the fake hold every PUT of key until release is closed, after it has read the
// body: the backend has the whole request and has not answered, exactly the window a drain
// barrier exists for. arrived is sent once per held PUT.
func blockPuts(t *testing.T, f *fakeS3, key string) (arrived chan struct{}, release func()) {
	t.Helper()
	arrived = make(chan struct{}, 8)
	ch := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release) // a failed test must not leave the fake's handler blocked
	f.mu.Lock()
	f.before = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/"+key) {
			arrived <- struct{}{}
			<-ch
		}
	}
	f.mu.Unlock()
	return arrived, release
}

func ackOf(t *testing.T, m *mixedRig, id string) admission.Ack {
	t.Helper()
	for _, a := range m.gates.Acks(m.dir.Snapshot()) {
		if a.ID == id {
			return a
		}
	}
	t.Fatalf("no ack for barrier %s in %+v", id, m.gates.Acks(m.dir.Snapshot()))
	return admission.Ack{}
}

// T05. A PUT the source has accepted but not yet answered is in flight when a ramp step's hold
// arrives. Installed is not drained: the bucket's gate counts the PUT until the backend answers,
// the hold refuses every new mutation, and a step that copies the source to the target only once
// the gate reports drained reads back the PUT's value afterwards. The negative control is the
// version fence as it was: the step commits on install alone, the copy misses the PUT, and the
// final read returns a value the client never last acknowledged.
func TestHoldDrainsAnInFlightWrite(t *testing.T) {
	for _, negative := range []bool{false, true} {
		name := "drained-before-commit"
		if negative {
			name = "negative-control-installed-is-not-drained"
		}
		t.Run(name, func(t *testing.T) {
			m := newMixedRig(t, nil)
			if r := m.acme(t, "PUT", "/data/moved/k", []byte("v1")); r.StatusCode != http.StatusOK {
				t.Fatalf("seed: %d %s", r.StatusCode, r.body)
			}
			arrived, release := blockPuts(t, m.garage, "moved/k")
			var wg sync.WaitGroup
			var late reply
			wg.Add(1)
			go func() {
				defer wg.Done()
				late = m.acme(t, "PUT", "/data/moved/k", []byte("v2"))
			}()
			<-arrived

			target := ramp(t, m, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}, Hold: true, Barrier: "op-1"})
			ack := ackOf(t, m, "op-1")
			if !ack.Closed || ack.Inflight != 1 || ack.Uncertain != 0 || ack.Scope != "placement:acme/data" || ack.Generation != m.dir.Snapshot().Version() {
				t.Fatalf("ack with the PUT in flight: %+v", ack)
			}
			r := m.acme(t, "PUT", "/data/stay/x", []byte("x"))
			if r.StatusCode != http.StatusServiceUnavailable || r.Header.Get("Retry-After") != "1" {
				t.Fatalf("a mutation outside the hold's keys during the barrier: %d Retry-After %q", r.StatusCode, r.Header.Get("Retry-After"))
			}
			if got := counterValue(t, m.h.Metrics.RefusedWrites.WithLabelValues("acme/data", "barrier")); got != 1 {
				t.Errorf("refused writes{reason=barrier} = %v, want 1", got)
			}
			if r := m.acme(t, "GET", "/data/moved/k", nil); r.StatusCode != http.StatusOK || string(r.body) != "v1" {
				t.Fatalf("a read during the barrier: %d %q", r.StatusCode, r.body)
			}

			copyToTarget := func() {
				b, _ := m.garage.object("acme-1111-data", "moved/k")
				m.minio.put(target, "moved/k", b)
			}
			if !negative {
				// The control node waits for the drain: the PUT must end before the copy.
				release()
				wg.Wait()
				if late.StatusCode != http.StatusOK {
					t.Fatalf("the held PUT: %d %s", late.StatusCode, late.body)
				}
				if ack := ackOf(t, m, "op-1"); ack.Inflight != 0 {
					t.Fatalf("ack after the PUT ended: %+v", ack)
				}
				copyToTarget()
			} else {
				copyToTarget() // installed taken for drained: the copy runs with the PUT still in flight
				release()
				wg.Wait()
				if late.StatusCode != http.StatusOK {
					t.Fatalf("the held PUT: %d %s", late.StatusCode, late.body)
				}
			}
			if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateRamping, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}, Complete: true}, "test"); err != nil {
				t.Fatal(err)
			}
			if p, _ := m.dir.Snapshot().Lookup("acme", "data"); p.Barrier != nil || p.Held() {
				t.Fatalf("the completed step left the barrier: %+v", p)
			}
			if st := m.gates.State("acme/data"); st.Closed[admission.Mutations] != "" {
				t.Fatal("the gate stayed closed after the step completed")
			}
			r = m.acme(t, "GET", "/data/moved/k", nil)
			lastAcknowledged := string(r.body) == "v2"
			switch {
			case !negative && !lastAcknowledged:
				t.Fatalf("after the drained step, the read returned %q; the client's last acknowledged write is v2", r.body)
			case negative && lastAcknowledged:
				t.Fatal("the negative control found no violation: the version-only fence would have passed the oracle")
			case negative:
				t.Logf("negative control: the read returned %q after the client was acknowledged v2 (the version-only fence loses the write)", r.body)
			}
			if r := m.acme(t, "PUT", "/data/stay/x", []byte("x")); r.StatusCode != http.StatusOK {
				t.Fatalf("a write after the step: %d %s", r.StatusCode, r.body)
			}
		})
	}
}

// T06. A mutation whose backend outcome is never learned stays counted for the life of the
// process: a PUT the backend read whole and never answered ends 504 for the client, and the
// gate reports it uncertain from then on, so no barrier on the bucket can drain. A response, or a
// request that never reached the backend whole, is definitive.
func TestUncertainOutcomeStaysCounted(t *testing.T) {
	m := newMixedRig(t, nil)
	m.h.IdleTimeout = 300 * time.Millisecond
	_, release := blockPuts(t, m.garage, "hang")
	// The idle watchdog gives up on the backend; at this short an idle time the server's own read
	// deadline on the client connection expires in the same instant, so the client sees either
	// the 504 or a cut connection. Either way the backend had the whole PUT and never answered.
	if r := m.acme(t, "PUT", "/data/hang", []byte("never answered")); r.StatusCode == http.StatusOK && len(r.body) > 0 {
		t.Fatalf("a PUT the backend never answers: %d %s", r.StatusCode, r.body)
	}
	if log := m.accessLog.String(); !strings.Contains(log, "upstream: context canceled") {
		t.Fatalf("the access log does not show the upstream request cut off: %s", log)
	}
	release()
	st := m.gates.State("acme/data")
	if st.Uncertain[admission.Mutations] != 1 || st.Inflight[admission.Mutations] != 0 || m.gates.Uncertain() != 1 {
		t.Fatalf("after an unanswered PUT: %+v, total %d", st, m.gates.Uncertain())
	}
	if r := m.acme(t, "PUT", "/data/ok", []byte("answered")); r.StatusCode != http.StatusOK {
		t.Fatalf("a PUT the backend answers: %d", r.StatusCode)
	}
	if st := m.gates.State("acme/data"); st.Uncertain[admission.Mutations] != 1 || st.Inflight[admission.Mutations] != 0 {
		t.Fatalf("a definitive PUT changed the counts: %+v", st)
	}
	ramp(t, m, directory.Transition{To: directory.StateRamping, Ratio: 0.5, Hold: true, Barrier: "op-2"})
	if ack := ackOf(t, m, "op-2"); ack.Uncertain != 1 || !ack.Closed {
		t.Fatalf("the barrier's ack must carry the uncertainty: %+v", ack)
	}
}

// connectRefused is what a dial to a closed port returns.
func connectRefused() error {
	_, err := net.Dial("tcp", "127.0.0.1:1")
	return err
}

// dispatched classifies a failed exchange: refused at connect or built wrong, never sent; a body
// cut short, never whole; anything else after a whole request, unknown.
func TestDispatched(t *testing.T) {
	short := &progressReader{}
	short.n.Store(3)
	whole := &progressReader{}
	whole.n.Store(10)
	cases := []struct {
		name   string
		err    error
		body   *progressReader
		length int64
		want   bool
	}{
		{"no error", nil, nil, 0, false},
		{"connection refused", connectRefused(), nil, 0, false},
		{"bad request build", &buildError{err: context.Canceled}, nil, 0, false},
		{"timeout, no body", context.DeadlineExceeded, nil, 0, true},
		{"timeout, body cut short", context.DeadlineExceeded, short, 10, false},
		{"timeout, body sent whole", context.DeadlineExceeded, whole, 10, true},
		{"timeout, chunked body", context.DeadlineExceeded, short, -1, true},
	}
	for _, c := range cases {
		if got := dispatched(c.err, c.body, c.length); got != c.want {
			t.Errorf("%s: dispatched=%v, want %v", c.name, got, c.want)
		}
	}
}

// T06. A multipart part or completion carries an uploadId that pins its cluster, and it does not
// slip past a barrier on the bucket: it is a mutation like any other. Listing the parts is a read
// and goes through.
func TestUploadIDDoesNotBypassTheBarrier(t *testing.T) {
	m := newMixedRig(t, nil)
	r := m.acme(t, "POST", "/data/mp?uploads", nil)
	uid := between(r.body, "<UploadId>", "</UploadId>")
	if r.StatusCode != http.StatusOK || uid == "" {
		t.Fatalf("initiate: %d %s", r.StatusCode, r.body)
	}
	ramp(t, m, directory.Transition{To: directory.StateRamping, Prefixes: []string{"elsewhere/"}, Hold: true, Barrier: "op-3"})
	if r := m.acme(t, "PUT", "/data/mp?partNumber=1&uploadId="+uid, []byte("p")); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a part under the barrier: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "POST", "/data/mp?uploadId="+uid, []byte(`<CompleteMultipartUpload/>`)); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a completion under the barrier: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "GET", "/data/mp?uploadId="+uid, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("listing parts under the barrier: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "DELETE", "/data/mp?uploadId="+uid, nil); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("an abort under the barrier: %d %s", r.StatusCode, r.body)
	}
}

// T06. A delete during a migration takes a source token as well as a mutation token, and a copy
// that reads a moving bucket's source takes one on that bucket. A closed source gate (purge-source
// is about to delete the source) skips the delete's source leg, counted as source_closed, and
// refuses a copy that would read the source.
func TestSourceDependentWorkIsCounted(t *testing.T) {
	m := newMixedRig(t, nil)
	if r := m.acme(t, "PUT", "/data/k", []byte("on the source")); r.StatusCode != http.StatusOK {
		t.Fatal(r.StatusCode)
	}
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	// A delete goes to both clusters: its source leg is held by the fake while its tokens are read.
	arrived, release := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	m.garage.mu.Lock()
	m.garage.before = func(r *http.Request) {
		if r.Method == http.MethodDelete {
			arrived <- struct{}{}
			<-release
		}
	}
	m.garage.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); m.acme(t, "DELETE", "/data/k", nil) }()
	<-arrived
	if st := m.gates.State("acme/data"); st.Inflight[admission.Mutations] != 1 || st.Inflight[admission.Source] != 1 {
		t.Fatalf("a dual delete in flight: %+v", st)
	}
	releaseOnce.Do(func() { close(release) })
	wg.Wait()
	if st := m.gates.State("acme/data"); st.Inflight[admission.Mutations] != 0 || st.Inflight[admission.Source] != 0 || st.Uncertain != [2]int64{} {
		t.Fatalf("after the dual delete: %+v", st)
	}
	m.garage.mu.Lock()
	m.garage.before = nil
	m.garage.mu.Unlock()

	// A copy from the moving bucket into an ACTIVE one falls back to the source: it depends on it.
	m.garage.put("acme-1111-data", "k2", []byte("source only"))
	garrived, grelease := make(chan struct{}, 1), make(chan struct{})
	var greleaseOnce sync.Once
	t.Cleanup(func() { greleaseOnce.Do(func() { close(grelease) }) })
	m.garage.mu.Lock()
	m.garage.before = func(r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/k2") {
			garrived <- struct{}{}
			<-grelease
		}
	}
	m.garage.mu.Unlock()
	wg.Add(1)
	var cp reply
	go func() {
		defer wg.Done()
		cp = m.send(t, "PUT", "/pics/copied", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/data/k2"})
	}()
	<-garrived
	if st := m.gates.State("acme/data"); st.Inflight[admission.Source] != 1 {
		t.Fatalf("a copy reading the source: %+v", st)
	}
	if st := m.gates.State("acme/pics"); st.Inflight[admission.Mutations] != 1 {
		t.Fatalf("the copy's write on its own bucket: %+v", st)
	}
	greleaseOnce.Do(func() { close(grelease) })
	wg.Wait()
	if cp.StatusCode != http.StatusOK {
		t.Fatalf("copy: %d %s", cp.StatusCode, cp.body)
	}
	m.garage.mu.Lock()
	m.garage.before = nil
	m.garage.mu.Unlock()

	// Cut over, then close the source: purge-source's barrier.
	if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateMigrating, directory.Transition{To: directory.StateCutover}, "test"); err != nil {
		t.Fatal(err)
	}
	m.minio.put(target, "k3", []byte("everywhere"))
	m.garage.put("acme-1111-data", "k3", []byte("everywhere"))
	if err := m.dir.SetBarrier(context.Background(), "acme", "data", directory.Barrier{ID: "op-p", Kind: config.BarrierSource}, "test"); err != nil {
		t.Fatal(err)
	}
	if ack := ackOf(t, m, "op-p"); !ack.Closed || ack.Kind != config.BarrierSource || ack.Inflight != 0 {
		t.Fatalf("the source barrier's ack: %+v", ack)
	}
	if r := m.acme(t, "DELETE", "/data/k3", nil); r.StatusCode != http.StatusNoContent {
		t.Fatalf("a delete with the source closed: %d %s", r.StatusCode, r.body)
	}
	if _, onSource := m.garage.object("acme-1111-data", "k3"); !onSource {
		t.Fatal("the delete reached the closed source")
	}
	if _, onPrimary := m.minio.object(target, "k3"); onPrimary {
		t.Fatal("the delete did not reach the primary")
	}
	if got := counterValue(t, m.h.Metrics.DualDelete.WithLabelValues("acme/data", "source_closed")); got != 1 {
		t.Errorf("dual deletes{outcome=source_closed} = %v, want 1", got)
	}
	// A copy that has to read the source is refused while it is closed; one served by the primary
	// is not. In CUTOVER a read is primary-only, so the copy plan never names the source.
	if r := m.send(t, "PUT", "/pics/again", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/data/k2"}); r.StatusCode != http.StatusNotFound {
		t.Fatalf("a copy of a source-only object after cutover with the source closed: %d %s (the primary answers)", r.StatusCode, r.body)
	}
	if err := m.dir.ClearBarrier(context.Background(), directory.PlacementResource("acme/data"), "op-p", "test"); err != nil {
		t.Fatal(err)
	}
	if st := m.gates.State("acme/data"); st.Closed[admission.Source] != "" {
		t.Fatal("the source gate stayed closed after the barrier was cleared")
	}
}

// A cluster's barrier closes the mutations of every bucket on it and of no other; its ack sums
// them. A bucket on the cluster whose own barrier came first keeps its own id.
func TestClusterBarrierClosesEveryBucketOnIt(t *testing.T) {
	m := newMixedRig(t, nil)
	rewriteDirectory(t, m, func(f *directory.File) {
		c := f.Clusters["garage"]
		c.Barrier = &config.Barrier{ID: "op-c", Kind: config.BarrierMutations}
		f.Clusters["garage"] = c
	})
	for _, target := range []string{"/data/k", "/pics/k"} {
		if r := m.acme(t, "PUT", target, []byte("x")); r.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("PUT %s on a cluster with a barrier: %d", target, r.StatusCode)
		}
	}
	if r := m.send(t, "PUT", "/data/k", "", []byte("x"), zedAK, zedSK, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("PUT on a bucket of another cluster: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "GET", "/data/k", nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("a read on a cluster with a barrier: %d", r.StatusCode)
	}
	ack := ackOf(t, m, "op-c")
	if ack.Scope != "cluster:garage" || !ack.Closed || ack.Inflight != 0 {
		t.Fatalf("cluster ack: %+v", ack)
	}
	rewriteDirectory(t, m, func(f *directory.File) {
		c := f.Clusters["garage"]
		c.Barrier = nil
		f.Clusters["garage"] = c
	})
	if r := m.acme(t, "PUT", "/data/k", []byte("x")); r.StatusCode != http.StatusOK {
		t.Fatalf("PUT after the cluster barrier cleared: %d %s", r.StatusCode, r.body)
	}
}

// A gate closed by an installed version stays closed while requests still route by an older
// version (install backpressure), and opens once the version without the barrier is served.
func TestGateKeeperStaysClosedUnderBackpressure(t *testing.T) {
	k := &GateKeeper{Gates: admission.New()}
	held := &directory.File{Version: 5, Placements: map[string]directory.Placement{"a/b": {State: directory.StateActive, Primary: "c", Barrier: &config.Barrier{ID: "op", Kind: config.BarrierMutations}}}}
	clear := &directory.File{Version: 6, Placements: map[string]directory.Placement{"a/b": {State: directory.StateActive, Primary: "c"}}}
	old := &directory.File{Version: 4, Placements: map[string]directory.Placement{"a/b": {State: directory.StateActive, Primary: "c"}}}
	k.Served(directory.NewSnapshot(old))
	k.Installed(directory.NewSnapshot(held))
	if st := k.Gates.State("a/b"); st.Closed[admission.Mutations] != "op" {
		t.Fatal("the installed barrier did not close the gate")
	}
	k.Installed(directory.NewSnapshot(clear)) // served is still 4: backpressured
	if st := k.Gates.State("a/b"); st.Closed[admission.Mutations] != "op" {
		t.Fatal("the gate opened while requests still route by an older version")
	}
	k.Served(directory.NewSnapshot(clear))
	if st := k.Gates.State("a/b"); st.Closed[admission.Mutations] != "" {
		t.Fatal("the gate stayed closed once the version without the barrier was served")
	}
}
