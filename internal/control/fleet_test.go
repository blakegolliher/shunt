package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

// fleetSim plays member proxies against a rig's control node. Time is simulated: every sleep of
// the control node advances the clock by what it asked for, and then each member does what its
// mode says (ADR-0016).
type fleetSim struct {
	rg      *rig
	h       http.Handler
	mu      sync.Mutex
	now     time.Time
	modes   map[string]string             // id → follow | stuck | gone
	applied map[string]int64              // stuck members report this
	reads   map[string]map[string]float64 // id → bucket → fallback reads it reports
	onSleep func(time.Duration)           // extra per-sleep behavior, called before members beat
	// holdsSeen records, at each sleep, whether data01 had a held step in the directory.
	holdsSeen []bool
}

func newFleetSim(rg *rig) *fleetSim {
	fs := &fleetSim{rg: rg, now: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC), modes: map[string]string{},
		applied: map[string]int64{}, reads: map[string]map[string]float64{}}
	rg.ctl.FleetFile = rg.dir.Path() + ".fleet.yaml"
	fs.h = rg.ctl.Handler()
	rg.ctl.Now = func() time.Time {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return fs.now
	}
	rg.ctl.LeaseTTL = 10 * time.Second
	rg.ctl.FencePoll = time.Second
	rg.ctl.Sleep = func(_ context.Context, d time.Duration) error {
		fs.mu.Lock()
		fs.now = fs.now.Add(d)
		fs.mu.Unlock()
		p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
		fs.holdsSeen = append(fs.holdsSeen, p != nil && p.Held())
		if fs.onSleep != nil {
			fs.onSleep(d)
		}
		fs.tick()
		return nil
	}
	return fs
}

// tick sends one heartbeat from every member that is not gone.
func (fs *fleetSim) tick() {
	for id, mode := range fs.modes {
		switch mode {
		case "follow":
			fs.beat(id, fs.rg.dir.Snapshot().Version())
		case "stuck":
			fs.beat(id, fs.applied[id])
		}
	}
}

// beat is one member's heartbeat, sent to the control node's handler directly: it may run inside
// another request's handler goroutine, where the test's Fatal is not allowed.
func (fs *fleetSim) beat(id string, applied int64) {
	body, _ := json.Marshal(Heartbeat{Applied: applied, FallbackReads: fs.reads[id]})
	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/"+id+"/heartbeat", bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:40000"
	rec := httptest.NewRecorder()
	fs.h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		panic("heartbeat " + id + ": " + rec.Body.String())
	}
}

func (fs *fleetSim) advance(d time.Duration) {
	fs.mu.Lock()
	fs.now = fs.now.Add(d)
	fs.mu.Unlock()
}

// expanded sets up data01 on vast01, expanded to vast02: the state before the first step.
func expanded(t *testing.T, rg *rig) {
	t.Helper()
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/expand", ExpandRequest{To: "vast02", Create: true}, nil)
}

func placement(t *testing.T, rg *rig) directory.Placement {
	t.Helper()
	p, ok := rg.dir.Snapshot().Lookup("acme", "data01")
	if !ok {
		t.Fatal("no acme/data01")
	}
	return *p
}

// With a member in the fleet, a ramp step is written as a hold, completed once every member has
// the hold, and answered once every member has the completed step.
func TestRampStepIsHeldUntilEveryProxyHasIt(t *testing.T) {
	rg := newRig(t)
	fs := newFleetSim(rg)
	expanded(t, rg)
	fs.modes["p2"] = "follow"
	fs.tick()
	v0 := rg.dir.Snapshot().Version()

	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, &tr)
	if !tr.Held || tr.Proxies != 1 || len(tr.WaitingOn) != 0 || tr.From != directory.StateActive || tr.To != directory.StateRamping || tr.Ratio != 0.5 {
		t.Fatalf("ramp with a member: %+v", tr)
	}
	if tr.Version != v0+2 {
		t.Errorf("a held step is two versions (hold, then complete): %d → %d", v0, tr.Version)
	}
	p := placement(t, rg)
	if p.State != directory.StateRamping || p.Ramp.Ratio != 0.5 || p.Held() {
		t.Fatalf("after the step: %+v %+v", p, p.Ramp)
	}
	if len(fs.holdsSeen) == 0 || !fs.holdsSeen[0] {
		t.Errorf("the fence waited without the hold in the directory: %v", fs.holdsSeen)
	}
	changes, err := os.ReadFile(rg.dir.ChangeLogPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(changes), `"hold"`) {
		t.Errorf("the change log does not show the hold:\n%s", changes)
	}

	// Next step: the precondition passes (p2 is current), the step is held again, and completes.
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.8}, &tr)
	if !tr.Held || placement(t, rg).Ramp.Ratio != 0.8 {
		t.Fatalf("second step: %+v", tr)
	}
	// migrate start from 0.8 moves writes: held with hold.ratio 1.
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, &tr)
	if !tr.Held || tr.To != directory.StateMigrating || placement(t, rg).State != directory.StateMigrating {
		t.Fatalf("migrate start: %+v", tr)
	}
	if log := rg.log.String(); !strings.Contains(log, "ramp step held") || !strings.Contains(log, "proxy joined the fleet  proxy=p2") {
		t.Errorf("log:\n%s", log)
	}
}

// A member that never installs the hold makes the step undo itself: nothing changes, and the
// refusal names the member.
func TestHeldStepIsReleasedWhenAProxyLags(t *testing.T) {
	rg := newRig(t)
	fs := newFleetSim(rg)
	expanded(t, rg)
	fs.modes["p2"] = "follow"
	fs.tick()
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, nil)
	before := placement(t, rg)

	fs.modes["p3"] = "stuck"
	fs.applied["p3"] = rg.dir.Snapshot().Version()
	fs.tick()
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.8, Wait: "5s"}, "did not reach every proxy within 5s, waiting on p3")
	after := placement(t, rg)
	if after.State != before.State || after.Ramp.Ratio != 0.5 || after.Held() {
		t.Fatalf("a released step changed the placement: %+v %+v", after, after.Ramp)
	}
	if !strings.Contains(rg.log.String(), "held step released") {
		t.Errorf("no release in the log:\n%s", rg.log.String())
	}

	// A step while a member has not installed the current version is refused before anything is
	// written: p3 is still behind the release.
	v := rg.dir.Snapshot().Version()
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.8, Wait: "2s"}, "is not on every proxy yet, waiting on p3")
	if rg.dir.Snapshot().Version() != v {
		t.Error("a refused precondition wrote to the directory")
	}

	// p3 catches up: the step goes through.
	fs.modes["p3"] = "follow"
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.8}, &tr)
	if !tr.Held || tr.Proxies != 2 || placement(t, rg).Ramp.Ratio != 0.8 {
		t.Fatalf("after p3 caught up: %+v", tr)
	}
}

// Releasing a hold taken from ACTIVE puts the bucket back to ACTIVE with its target recorded.
func TestHeldFirstStepReleasesToActive(t *testing.T) {
	rg := newRig(t)
	fs := newFleetSim(rg)
	expanded(t, rg)
	fs.modes["p2"] = "stuck"
	fs.applied["p2"] = rg.dir.Snapshot().Version()
	fs.tick()
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5, Wait: "3s"}, "waiting on p2")
	p := placement(t, rg)
	if p.State != directory.StateActive || p.Primary != "vast01" || p.Target != "vast02" || p.Ramp != nil {
		t.Fatalf("after a released first step: %+v", p)
	}
}

// A bucket's first step waits for every member, live or not: a member cut off before it would keep
// writing every key to the source. The operator can forget a member that is gone for good.
func TestFirstStepWaitsForSilentMembers(t *testing.T) {
	rg := newRig(t)
	fs := newFleetSim(rg)
	expanded(t, rg)
	fs.modes["p2"] = "follow"
	fs.modes["p4"] = "follow"
	fs.tick()
	fs.modes["p4"] = "gone"
	fs.advance(time.Minute) // p4's lease is long gone: silent

	var fl Fleet
	rg.must("GET", "/v1/fleet", nil, &fl)
	if len(fl.Members) != 2 || fl.Members[1].ID != "p4" || fl.Members[1].Live {
		t.Fatalf("fleet: %+v", fl)
	}
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5, Wait: "2s"}, "waiting on p4")
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5, Wait: "2s"}, "shunt proxy forget")
	if placement(t, rg).State != directory.StateActive {
		t.Fatal("a refused first step changed the placement")
	}

	// A live member cannot be forgotten; a silent one can, and then the step goes through.
	rg.refused("DELETE", "/v1/fleet/p2", nil, "is live")
	rg.must("DELETE", "/v1/fleet/p4", nil, nil)
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, &tr)
	if !tr.Held || tr.Proxies != 1 {
		t.Fatalf("after forget: %+v", tr)
	}

	// Once the bucket is moving, a member that falls silent is dropped after its lease: it has a
	// non-ACTIVE snapshot and stops writing to the bucket itself.
	fs.modes["p2"] = "gone"
	fs.advance(time.Minute)
	tr = TransitionResult{}
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.8}, &tr)
	if tr.Held || placement(t, rg).Ramp.Ratio != 0.8 {
		t.Fatalf("a later step with only a silent member: %+v", tr)
	}

	// Membership survives a control-node restart: a fresh server reads it back.
	fresh := &Server{Dir: rg.dir, FleetFile: rg.ctl.FleetFile, Now: rg.ctl.Now}
	ms, err := fresh.members()
	if err != nil || len(ms) != 1 || ms[0].ID != "p2" || ms[0].Live {
		t.Fatalf("membership after a restart: %+v %v", ms, err)
	}
}

// Without members nothing is held and nothing waits: the lab and the README demo.
func TestNoMembersNoHold(t *testing.T) {
	rg := newRig(t)
	newFleetSim(rg)
	expanded(t, rg)
	v0 := rg.dir.Snapshot().Version()
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, &tr)
	if tr.Held || tr.Proxies != 0 || tr.Version != v0+1 {
		t.Fatalf("ramp without members: %+v", tr)
	}
	if _, err := os.Stat(rg.ctl.FleetFile); !os.IsNotExist(err) {
		t.Errorf("a fleet file appeared with no members: %v", err)
	}
}

// Cutover counts fallback reads across the fleet, and a member that goes quiet during the window
// is no evidence of quiet.
func TestCutoverCountsTheFleet(t *testing.T) {
	rg := newRig(t)
	fs := newFleetSim(rg)
	expanded(t, rg)
	fs.modes["p2"] = "follow"
	fs.tick()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 1, Done: true, Converged: true}, nil)

	// p2 serves a fallback read during the window: this proxy's own counter never moves.
	fs.reads["p2"] = map[string]float64{"acme/data01": 3}
	fs.tick()
	fs.onSleep = func(d time.Duration) {
		if d == 5*time.Second {
			fs.reads["p2"] = map[string]float64{"acme/data01": 4}
		}
	}
	rg.refused("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "5s"}, "still fall back")

	// p2 goes silent during the window.
	fs.onSleep = func(d time.Duration) {
		if d == 5*time.Second {
			fs.modes["p2"] = "gone"
		}
	}
	rg.refused("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "5s", Wait: "3s"}, "proxies p2 did not report after the 5s window")

	// Quiet everywhere: cut over, with the fleet's total as the evidence.
	fs.modes["p2"] = "follow"
	fs.onSleep = nil
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "5s"}, &tr)
	if tr.To != directory.StateCutover || tr.Cutover.FallbackReads != 4 || tr.Proxies != 1 {
		t.Fatalf("cutover: %+v %+v", tr, tr.Cutover)
	}
}

// A member's own control API refuses every change and names its control node.
func TestMemberRefusesMutations(t *testing.T) {
	rg := newRig(t)
	rg.ctl.ControlNode = "http://10.0.0.1:9900"
	rg.answers("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, http.StatusConflict, "refused")
	code, raw := rg.call("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, nil)
	if code != http.StatusConflict || !strings.Contains(raw, "its control node is http://10.0.0.1:9900") {
		t.Fatalf("member ramp: %d %s", code, raw)
	}
	rg.must("GET", "/v1/status", nil, nil)
}

// A hold left behind by an interrupted call (the control node restarted between the hold and its
// completion) must not strand the bucket: repeating the step completes it, and a different step
// is refused naming the one to repeat.
func TestLeftoverHoldIsCompletedByRepeatingTheStep(t *testing.T) {
	rg := newRig(t)
	fs := newFleetSim(rg)
	expanded(t, rg)
	fs.modes["p2"] = "follow"
	fs.tick()
	// The hold is written, as by a control node that then died.
	if err := rg.dir.SetState(context.Background(), "acme", "data01", directory.StateActive, directory.Transition{To: directory.StateRamping, Ratio: 0.5, Hold: true}, "test"); err != nil {
		t.Fatal(err)
	}
	fs.tick()
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.8}, "repeat it")
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, &tr)
	p := placement(t, rg)
	if p.Held() || p.State != directory.StateRamping || p.Ramp.Ratio != 0.5 || !tr.Held {
		t.Fatalf("after repeating the held step: %+v %+v (%+v)", p, p.Ramp, tr)
	}
}

// An unreadable membership file fails closed: a fenced step is refused, never run as if the fleet
// were empty.
func TestUnreadableFleetFileFailsClosed(t *testing.T) {
	rg := newRig(t)
	newFleetSim(rg)
	expanded(t, rg)
	if err := os.WriteFile(rg.ctl.FleetFile, []byte("members: {not: [a list"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 { // the second call must fail too: the first error must not be forgotten
		rg.answers("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, http.StatusServiceUnavailable, "unavailable")
	}
	if p := placement(t, rg); p.State != directory.StateActive {
		t.Fatalf("a step ran with the fleet unknown: %+v", p)
	}
}
