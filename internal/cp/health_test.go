package cp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// healthRig is one real etcd member (this node) with fake peers beside it in the sampler's view,
// their APIs registered, their probes answered by probe, and a clock the test moves.
type healthRig struct {
	h     *Health
	now   time.Time
	mu    sync.Mutex
	peers []localMember
}

func newHealthRig(t *testing.T, probe func(ctx context.Context, api string) (LocalStatus, error), peers ...localMember) *healthRig {
	t.Helper()
	tc := startCluster(t, 1)
	n := tc.nodes[0]
	ctx := context.Background()
	rg := &healthRig{now: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC), peers: peers}
	for _, p := range peers {
		if p.name == "" {
			continue // never started: nothing registered
		}
		rec, _ := json.Marshal(nodeRecord{Name: p.name, APIURL: "http://" + p.name + ":9901"})
		if _, err := n.cli.Put(ctx, fmt.Sprintf("%s%x", nodesPrefix, p.id), string(rec)); err != nil {
			t.Fatal(err)
		}
	}
	rg.h = &Health{Node: n, Timeout: time.Second, Now: func() time.Time { rg.mu.Lock(); defer rg.mu.Unlock(); return rg.now }, probe: probe}
	rg.h.members = func() []localMember { return append(n.localMembers(), rg.peers...) }
	return rg
}

func (rg *healthRig) advance(d time.Duration) {
	rg.mu.Lock()
	rg.now = rg.now.Add(d)
	rg.mu.Unlock()
}

func byName(ms []MemberHealth) map[string]MemberHealth {
	out := map[string]MemberHealth{}
	for _, m := range ms {
		out[firstNonEmpty(m.Name, m.ID)] = m
	}
	return out
}

// T13: health is what was observed, per member. This node is healthy (its etcd has a leader); c2
// answers as itself with a leader in view: healthy; c3 refuses the connection (stopped): unreachable;
// c4's API answers as another member: unreachable; a learner added and never started: unknown, never
// healthy. None of it comes from a member's name. Quorum reads reachable. Negative control: judging
// health by a name (started), as status did, calls c3 and c4 healthy.
func TestHealthObservesEachMember(t *testing.T) {
	probe := func(_ context.Context, api string) (LocalStatus, error) {
		switch {
		case strings.Contains(api, "c2"):
			return LocalStatus{Node: "c2", MemberID: "c2", Leader: "1"}, nil
		case strings.Contains(api, "c3"):
			return LocalStatus{}, fmt.Errorf("dial tcp: %w", syscall.ECONNREFUSED)
		default:
			return LocalStatus{Node: "c4", MemberID: "99", Leader: "1"}, nil
		}
	}
	rg := newHealthRig(t, probe, localMember{id: 0xc2, name: "c2"}, localMember{id: 0xc3, name: "c3"}, localMember{id: 0xc4, name: "c4"},
		localMember{id: 0xbeef, learner: true})
	rg.h.Cycle(context.Background())
	ms, q, _ := rg.h.Snapshot(0)
	got := byName(ms)
	want := map[string]string{"c1": HealthHealthy, "c2": HealthHealthy, "c3": HealthUnreachable, "c4": HealthUnreachable, "beef": HealthUnknown}
	for name, health := range want {
		if got[name].Health != health {
			t.Errorf("%s: %s (%s), want %s", name, got[name].Health, got[name].Reason, health)
		}
	}
	if !strings.Contains(got["c3"].Reason, "connection refused") || !strings.Contains(got["c4"].Reason, "answered as member 99") ||
		!strings.Contains(got["beef"].Reason, "never started") || got["beef"].Role != "learner" || got["beef"].Started {
		t.Fatalf("reasons: c3 %q, c4 %q, learner %+v", got["c3"].Reason, got["c4"].Reason, got["beef"])
	}
	if !got["c3"].Started || got["c3"].Observer != "c1" || got["c3"].ObservedAt.IsZero() {
		t.Fatalf("c3's observation: %+v", got["c3"])
	}
	if q.State != QuorumReachable || q.Method != "linearizable_read" {
		t.Fatalf("quorum: %+v", q)
	}
}

// An observation, a member's or quorum's, is only as good as it is recent: 15 s after the last
// cycle every one reads unknown, and says how old it is. Before any cycle, everything is unknown.
// Negative control: without the staleness check the members stay healthy.
func TestHealthObservationsGoStale(t *testing.T) {
	rg := newHealthRig(t, func(context.Context, string) (LocalStatus, error) {
		return LocalStatus{MemberID: "c2", Leader: "1"}, nil
	}, localMember{id: 0xc2, name: "c2"})
	ms, q, _ := rg.h.Snapshot(0)
	for _, m := range ms {
		if m.Health != HealthUnknown {
			t.Fatalf("before any cycle, %s is %s", m.Name, m.Health)
		}
	}
	if q.State != HealthUnknown {
		t.Fatalf("quorum before any read: %+v", q)
	}
	rg.h.Cycle(context.Background())
	rg.advance(10 * time.Second)
	if ms, q, _ = rg.h.Snapshot(0); byName(ms)["c2"].Health != HealthHealthy || q.State != QuorumReachable || byName(ms)["c2"].AgeMS != 10000 {
		t.Fatalf("10 s after a cycle: %+v %+v", byName(ms)["c2"], q)
	}
	rg.advance(6 * time.Second)
	ms, q, _ = rg.h.Snapshot(0)
	for _, m := range ms {
		if m.Health != HealthUnknown || !strings.Contains(m.Reason, "stale") {
			t.Fatalf("16 s after a cycle, %s: %s (%s)", m.Name, m.Health, m.Reason)
		}
	}
	if q.State != HealthUnknown || q.ErrorCode != "stale" {
		t.Fatalf("quorum 16 s after its read: %+v", q)
	}
}

// A cycle is bounded: a member whose API never answers is unreachable at the cycle's deadline and
// the cycle returns then; at most Concurrency probes run at once; and reading the status, however
// often, probes nothing: the work is the sampler's cycles, not its viewers'.
func TestHealthCycleIsBoundedAndViewerIndependent(t *testing.T) {
	var inflight, most atomic.Int64
	probe := func(ctx context.Context, api string) (LocalStatus, error) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		if strings.Contains(api, "hang") {
			<-ctx.Done()
			return LocalStatus{}, ctx.Err()
		}
		time.Sleep(20 * time.Millisecond)
		return LocalStatus{MemberID: strings.TrimSuffix(strings.TrimPrefix(api, "http://m"), ":9901"), Leader: "1"}, nil
	}
	peers := make([]localMember, 0, 13)
	peers = append(peers, localMember{id: 0xabc, name: "hang"})
	for i := range 12 {
		peers = append(peers, localMember{id: uint64(i + 0x10), name: fmt.Sprintf("m%x", i+0x10)})
	}
	rg := newHealthRig(t, probe, peers...)
	rg.h.Timeout = 300 * time.Millisecond
	start := time.Now()
	rg.h.Cycle(context.Background())
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a cycle with a member that never answers took %s, past its 300 ms deadline", took)
	}
	if m := most.Load(); m > 5 {
		t.Fatalf("%d probes ran at once, want at most 5", m)
	}
	ms, _, _ := rg.h.Snapshot(0)
	if h := byName(ms)["hang"]; h.Health != HealthUnreachable || !strings.Contains(h.Reason, "deadline") {
		t.Fatalf("a member that never answered: %+v", h)
	}
	sent := rg.h.probes.Load()
	for range 1000 {
		rg.h.Snapshot(0)
	}
	if rg.h.probes.Load() != sent {
		t.Fatalf("status reads probed: %d probes before, %d after", sent, rg.h.probes.Load())
	}
}

// Quorum that a linearizable read cannot reach reads unavailable, with its code; the status the API
// builds from the samples says so, is partial, and still lists every member.
func TestHealthQuorumUnavailable(t *testing.T) {
	rg := newHealthRig(t, func(context.Context, string) (LocalStatus, error) {
		return LocalStatus{}, errors.New("unreachable")
	}, localMember{id: 0xc2, name: "c2"}, localMember{id: 0xc3, name: "c3"})
	rg.h.quorumRead = func(context.Context) error { return context.DeadlineExceeded }
	rg.h.Cycle(context.Background())
	a := &API{Node: rg.h.Node, Health: rg.h}
	var ans StatusAnswer
	a.observed(context.Background(), &ans)
	if ans.Quorum.State != QuorumUnavailable || ans.Quorum.ErrorCode != "timeout" || ans.Cluster.HasQuorum || !ans.Partial || len(ans.Members) != 3 || ans.MembershipRevision == "" {
		t.Fatalf("status without quorum: %+v", ans)
	}
}

// T13 on a real cluster of three: each member serves its own local status and registers its API.
// All three healthy and quorum reachable; c3 stopped: unreachable, quorum still reachable; c2
// stopped too: unreachable, quorum unavailable, and GET /v1/control/status still answers, from c1's
// own view, every member listed, partial, has_quorum false.
func TestHealthOnARealCluster(t *testing.T) {
	tc := startCluster(t, 3)
	ctx := context.Background()
	servers := make([]*httptest.Server, 3)
	for i, n := range tc.nodes {
		servers[i] = httptest.NewServer((&API{Node: n}).Handler())
		t.Cleanup(servers[i].Close)
		if err := n.RegisterAPI(ctx, servers[i].URL); err != nil {
			t.Fatal(err)
		}
	}
	nd := startNode(t, tc, 0, make([]byte, 32), time.Second)
	h := &Health{Node: tc.nodes[0], Timeout: 2 * time.Second}
	status := httptest.NewServer((&API{Node: tc.nodes[0], Store: nd.store, Fleet: nd.fleet, Health: h}).Handler())
	t.Cleanup(status.Close)
	check := func(step string, want map[string]string, quorum string) StatusAnswer {
		t.Helper()
		h.Cycle(ctx)
		resp, err := http.Get(status.URL + "/v1/control/status")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var ans StatusAnswer
		if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&ans) != nil {
			t.Fatalf("%s: status answered %d", step, resp.StatusCode)
		}
		got := byName(ans.Members)
		for name, health := range want {
			if got[name].Health != health {
				t.Errorf("%s: %s is %s (%s), want %s", step, name, got[name].Health, got[name].Reason, health)
			}
		}
		if ans.Quorum.State != quorum || ans.Cluster.HasQuorum != (quorum == QuorumReachable) || len(ans.Cluster.Members) != 3 {
			t.Fatalf("%s: quorum %+v, has_quorum %v, %d members", step, ans.Quorum, ans.Cluster.HasQuorum, len(ans.Cluster.Members))
		}
		return ans
	}
	check("all running", map[string]string{"c1": HealthHealthy, "c2": HealthHealthy, "c3": HealthHealthy}, QuorumReachable)

	servers[2].Close()
	tc.nodes[2].Close()
	tc.nodes[2] = nil
	check("c3 stopped", map[string]string{"c1": HealthHealthy, "c2": HealthHealthy, "c3": HealthUnreachable}, QuorumReachable)

	servers[1].Close()
	tc.nodes[1].Close()
	tc.nodes[1] = nil
	ans := check("c2 stopped too", map[string]string{"c2": HealthUnreachable, "c3": HealthUnreachable}, QuorumUnavailable)
	if !ans.Partial || ans.Quorum.ErrorCode == "" {
		t.Fatalf("a status without quorum: partial %v, quorum %+v", ans.Partial, ans.Quorum)
	}
}
