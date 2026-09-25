package member

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

func readMarker(t *testing.T, c *Client) marker {
	t.Helper()
	data, err := os.ReadFile(c.markerPath())
	if err != nil {
		t.Fatal(err)
	}
	var m marker
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// The incarnation protocol (ADR-0021 D2). A process marks its incarnation active before it serves
// and reports it in every heartbeat; a clean retirement marks it retired and tells the control
// plane, retrying a lost reply; the next process finds the marker, reports the previous
// incarnation until the control plane has recorded it, and a marker left active means the
// previous process never retired and is reported unclean.
func TestIncarnationMarkerAndRetirement(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	if !control.ValidIncarnation(c.Incarnation()) {
		t.Fatalf("incarnation %q", c.Incarnation())
	}
	if m := readMarker(t, c); m.State != control.IncarnationActive || m.Incarnation != c.Incarnation() || m.ProxyID != "p1" {
		t.Fatalf("marker after Load: %+v", m)
	}
	if st, _ := os.Stat(c.markerPath()); st.Mode().Perm() != 0o600 {
		t.Fatalf("marker mode %v", st.Mode().Perm())
	}
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	first := f.beats[0]
	f.mu.Unlock()
	if first.Incarnation != c.Incarnation() || first.Previous != nil || first.Uncertain != 0 {
		t.Fatalf("first heartbeat: %+v", first)
	}

	// A retirement with a lost reply is retried until it lands.
	c.Gates = admission.New()
	tok, _, _ := c.Gates.Enter("acme/data", admission.Mutations)
	tok.Release(c.Gates, admission.Uncertain)
	f.retireFails.Store(true)
	go func() {
		time.Sleep(120 * time.Millisecond)
		f.retireFails.Store(false)
	}()
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Retire(rctx, c.Gates.Uncertain()+c.Gates.Inflight()); err != nil {
		t.Fatalf("retire: %v", err)
	}
	f.mu.Lock()
	retired := append([]control.RetireRequest(nil), f.retired...)
	f.mu.Unlock()
	if len(retired) != 1 || retired[0].Incarnation != c.Incarnation() || retired[0].Uncertain != 1 {
		t.Fatalf("retirements recorded: %+v", retired)
	}
	if m := readMarker(t, c); m.State != control.IncarnationUnclean || m.Uncertain != 1 || m.Ended.IsZero() {
		t.Fatalf("marker after an unclean retirement: %+v", m)
	}
	// A retired process heartbeats no more: a beat would put its lease key back and show it live.
	f.mu.Lock()
	beatsBefore := len(f.beats)
	f.mu.Unlock()
	if err := c.beat(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	beatsAfter := len(f.beats)
	f.mu.Unlock()
	if beatsAfter != beatsBefore {
		t.Fatalf("a retired process sent a heartbeat (%d → %d)", beatsBefore, beatsAfter)
	}

	// The next process reports the previous one until the control plane has it.
	next := New(c.cfg, c.log)
	if err := next.Load(); err != nil {
		t.Fatal(err)
	}
	if next.Incarnation() == c.Incarnation() {
		t.Fatal("a new process drew the same incarnation")
	}
	if p := next.previousToReport(); p == nil || p.ID != c.Incarnation() || p.State != control.IncarnationUnclean || p.Uncertain != 1 {
		t.Fatalf("previous to report: %+v", p)
	}
	if err := next.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if err := next.beat(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	beats := append([]control.Heartbeat(nil), f.beats...)
	f.mu.Unlock()
	if n := len(beats); n < 3 || beats[1].Previous == nil || beats[1].Previous.ID != c.Incarnation() || beats[2].Previous != nil {
		t.Fatalf("the previous incarnation must be reported until recorded, then not: %+v", beats[1:])
	}
	if m := readMarker(t, next); m.State != control.IncarnationActive || m.Incarnation != next.Incarnation() {
		t.Fatalf("marker of the new process: %+v", m)
	}

	// A process that never retired: its marker still says active when the next one starts.
	crashed := New(c.cfg, c.log)
	if err := crashed.Load(); err != nil {
		t.Fatal(err)
	}
	if p := crashed.previousToReport(); p == nil || p.ID != next.Incarnation() || p.State != control.IncarnationUnclean {
		t.Fatalf("a marker left active is an unclean previous incarnation: %+v", p)
	}

	// Another proxy's marker is not this proxy's evidence.
	other := c.cfg
	other.ProxyID, other.CacheDir = "p2", t.TempDir()
	o := New(other, c.log)
	data, _ := os.ReadFile(crashed.markerPath())
	if err := os.MkdirAll(other.CacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other.CacheDir, markerFile), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := o.Load(); err != nil {
		t.Fatal(err)
	}
	if o.previousToReport() != nil {
		t.Fatal("another proxy's marker was taken as a previous incarnation")
	}
}

// The control plane's retire request reaches the proxy in a heartbeat answer, once.
func TestRetireRequestedByTheControlPlane(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	calls := 0
	c.OnRetire = func() { calls++ }
	f.setAnswer(func(a *control.HeartbeatAnswer) { a.Retire = true })
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.beat(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("OnRetire called %d times", calls)
	}
	st := status(t, c)
	if st.Incarnation != c.Incarnation() || st.Uncertain != 0 {
		t.Fatalf("/-/fleet: %+v", st)
	}
}

// The heartbeat carries this proxy's drain proof for every barrier in its installed directory
// (ADR-0021 D2), and drops its telemetry window, never the proof, when it would pass the cap.
func TestHeartbeatCarriesBarrierAcks(t *testing.T) {
	f := newFakeControl(t)
	f.mu.Lock()
	p := f.dir.Placements["acme/data"]
	p.Barrier = &config.Barrier{ID: "op-7", Kind: config.BarrierMutations}
	f.dir.Placements = cloneP(f.dir.Placements)
	f.dir.Placements["acme/data"] = p
	f.dir.Generations = map[string]int64{directory.PlacementResource("acme/data"): 1}
	f.mu.Unlock()
	c := newClient(t, f)
	c.Gates = admission.New()
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	c.Gates.Close("acme/data", admission.Mutations, "op-7")
	tok, _, _ := c.Gates.Enter("acme/data", admission.Source)
	if err := c.beat(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	last := f.beats[len(f.beats)-1]
	f.mu.Unlock()
	if len(last.Barriers) != 1 || last.Barriers[0].ID != "op-7" || last.Barriers[0].Scope != "placement:acme/data" || last.Barriers[0].Generation != 1 || !last.Barriers[0].Closed || last.Barriers[0].Inflight != 0 {
		t.Fatalf("heartbeat acks: %+v", last.Barriers)
	}
	tok.Release(c.Gates, admission.Definitive)
	st := status(t, c)
	if len(st.Barriers) != 1 || st.LeaseSeq == 0 || st.LeaseGranted != "1s" || st.GrantError != "" {
		t.Fatalf("/-/fleet lease and barriers: %+v", st)
	}

	// The cap: a heartbeat whose telemetry would push it past the cap keeps everything but the window.
	hb := control.Heartbeat{Incarnation: c.Incarnation(), Barriers: []admission.Ack{{ID: "op-7"}},
		Telemetry: &telemetry.Window{Sketches: []telemetry.Sketch{{Data: make([]byte, 2000)}}}}
	if dropped, size := capTelemetry(&hb, 1000); !dropped || size < 2000 || hb.Telemetry != nil || len(hb.Barriers) != 1 {
		t.Fatalf("capTelemetry over the cap: dropped %v size %d %+v", dropped, size, hb)
	}
	hb.Telemetry = &telemetry.Window{}
	if dropped, _ := capTelemetry(&hb, 1<<20); dropped || hb.Telemetry == nil {
		t.Fatal("capTelemetry under the cap dropped the window")
	}
}
