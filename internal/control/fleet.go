package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// The fleet (ADR-0016). One proxy is the control node; the others are members that send it a
// heartbeat. The control node keeps, per member, the directory version it has installed and its
// fallback-read counters for moving buckets, and holds a fenced change until every member it must
// wait for has that change installed.

// defaultDropMargin is how far past its lease a silent member stays live (Server.DropMargin). A
// member stops routing writes on moving buckets when its lease lapses, measured from when it sent
// its last acknowledged heartbeat; the control node, measuring from when it received it, drops the
// member only after this margin.
const defaultDropMargin = 5 * time.Second

// defaultFenceWait is how long a fenced change waits for the fleet before answering pending.
const defaultFenceWait = 30 * time.Second

// Heartbeat is a member's report to the control node.
type Heartbeat struct {
	Started time.Time `json:"started"`
	// Applied is the directory version the member has installed.
	Applied int64 `json:"applied"`
	// FallbackReads is shunt_migration_fallback_reads_total for each bucket that is not ACTIVE in
	// the member's snapshot: bounded by migrations in flight, never by buckets.
	FallbackReads map[string]float64 `json:"fallback_reads,omitempty"`
}

// HeartbeatAnswer tells a member the current version and its lease.
type HeartbeatAnswer struct {
	Version  int64         `json:"version"`
	LeaseTTL time.Duration `json:"lease_ttl"`
}

// Member is one registered proxy as the control node sees it.
type Member struct {
	ID            string             `json:"id"`
	Live          bool               `json:"live"`
	Applied       int64              `json:"applied"`
	Started       time.Time          `json:"started,omitzero"`
	Seen          time.Time          `json:"seen,omitzero"`        // when its last heartbeat arrived; zero since this control node started
	SinceSeen     time.Duration      `json:"since_seen,omitempty"` // how long ago, by the control node's clock
	Beats         int64              `json:"-"`                    // heartbeats received since this control node started
	FallbackReads map[string]float64 `json:"fallback_reads,omitempty"`
}

// Fleet is the answer to GET /v1/fleet.
type Fleet struct {
	Version  int64         `json:"version"`
	LeaseTTL time.Duration `json:"lease_ttl"`
	Members  []Member      `json:"members"`
}

// fleet is the control node's table. Membership (ids) is persisted to Server.FleetFile so it
// survives a restart; liveness is memory only.
type fleet struct {
	mu      sync.Mutex
	loaded  bool
	members map[string]*Member
}

type fleetFile struct {
	Members []string `yaml:"members"`
}

// errFleet is a membership file that cannot be read or written. It fails closed: without the
// membership, no fenced change can know whom to wait for, so it answers 503 rather than run as if
// the fleet were empty.
type errFleet struct{ err error }

func (e *errFleet) Error() string { return "fleet membership: " + e.err.Error() }
func (e *errFleet) Unwrap() error { return e.err }

// load reads the membership file the first time it succeeds; until then every call tries again
// and fails. A missing file is an empty fleet.
func (f *fleet) load(path string) error {
	if f.loaded {
		return nil
	}
	members := map[string]*Member{}
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return &errFleet{err}
		default:
			var ff fleetFile
			if err := yaml.Unmarshal(data, &ff); err != nil {
				return &errFleet{fmt.Errorf("%s: %w", path, err)}
			}
			for _, id := range ff.Members {
				if !config.ValidProxyID(id) {
					return &errFleet{fmt.Errorf("%s: proxy id %q is not valid", path, id)}
				}
				members[id] = &Member{ID: id}
			}
		}
	}
	f.members, f.loaded = members, true
	return nil
}

// save writes the membership file durably (directory.WriteAtomic).
func (f *fleet) save(path string) error {
	if path == "" {
		return nil
	}
	ids := slices.Sorted(maps.Keys(f.members))
	data, err := yaml.Dump(fleetFile{Members: ids})
	if err != nil {
		return &errFleet{err}
	}
	data = append([]byte("# shunt fleet membership (ADR-0016): proxy ids only. Written by the control node.\n"), data...)
	if err := directory.WriteAtomic(path, data); err != nil {
		return &errFleet{err}
	}
	return nil
}

// liveAt reports whether a member's last heartbeat is recent enough to count it live at now.
func (s *Server) liveAt(m *Member, now time.Time) bool {
	margin := s.DropMargin
	if margin <= 0 {
		margin = defaultDropMargin
	}
	return !m.Seen.IsZero() && now.Sub(m.Seen) <= s.leaseTTL()+margin
}

func (s *Server) leaseTTL() time.Duration {
	if s.LeaseTTL > 0 {
		return s.LeaseTTL
	}
	return 10 * time.Second
}

// members returns a copy of the table with liveness computed at now, sorted by id.
func (s *Server) members() ([]Member, error) {
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if err := s.fleet.load(s.FleetFile); err != nil {
		return nil, err
	}
	now := s.now()
	out := make([]Member, 0, len(s.fleet.members))
	for _, m := range s.fleet.members {
		c := *m
		c.Live = s.liveAt(m, now)
		if !m.Seen.IsZero() {
			c.SinceSeen = now.Sub(m.Seen)
		}
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b Member) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// PublishFleet refreshes shunt_fleet_members from the table. serve calls it on every directory
// poll, so a member that falls silent shows up without anyone asking (ADR-0016).
func (s *Server) PublishFleet() error {
	ms, err := s.members()
	if err != nil {
		return err
	}
	s.publishFleet(ms)
	return nil
}

// publishFleet sets shunt_fleet_members.
func (s *Server) publishFleet(ms []Member) {
	if s.Metrics == nil {
		return
	}
	live := 0
	for _, m := range ms {
		if m.Live {
			live++
		}
	}
	s.Metrics.FleetMembers.WithLabelValues("live").Set(float64(live))
	s.Metrics.FleetMembers.WithLabelValues("silent").Set(float64(len(ms) - live))
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !config.ValidProxyID(id) {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("proxy id %q: want 1-64 letters, digits, '.', '_' or '-'", id))
		return
	}
	var hb Heartbeat
	if !decode(w, r, &hb) {
		return
	}
	s.fleet.mu.Lock()
	if err := s.fleet.load(s.FleetFile); err != nil {
		s.fleet.mu.Unlock()
		fail(w, err)
		return
	}
	m, known := s.fleet.members[id]
	if !known {
		m = &Member{ID: id}
		s.fleet.members[id] = m
		if err := s.fleet.save(s.FleetFile); err != nil {
			delete(s.fleet.members, id)
			s.fleet.mu.Unlock()
			fail(w, err)
			return
		}
	}
	m.Applied, m.Started, m.Seen, m.FallbackReads = hb.Applied, hb.Started, s.now(), hb.FallbackReads
	m.Beats++
	s.fleet.mu.Unlock()
	if !known {
		s.info(r, "proxy joined the fleet", "proxy", id, "applied", hb.Applied)
	}
	writeJSON(w, http.StatusOK, HeartbeatAnswer{Version: s.Dir.Snapshot().Version(), LeaseTTL: s.leaseTTL()})
}

func (s *Server) fleetList(w http.ResponseWriter, _ *http.Request) {
	ms, err := s.members()
	if err != nil {
		fail(w, err)
		return
	}
	s.publishFleet(ms)
	writeJSON(w, http.StatusOK, Fleet{Version: s.Dir.Snapshot().Version(), LeaseTTL: s.leaseTTL(), Members: ms})
}

// forgetProxy removes a member: the operator's statement that the proxy is gone for good. A proxy
// that is still running re-joins on its next heartbeat, so forgetting a live one is refused.
func (s *Server) forgetProxy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if err := s.fleet.load(s.FleetFile); err != nil {
		fail(w, err)
		return
	}
	m, ok := s.fleet.members[id]
	if !ok {
		fail(w, notFound("no proxy %s in the fleet", id))
		return
	}
	if s.liveAt(m, s.now()) {
		fail(w, refuse("proxy %s sent a heartbeat %s ago and is live; stop it first (it re-joins on its next heartbeat)", id, s.now().Sub(m.Seen).Round(time.Second)))
		return
	}
	delete(s.fleet.members, id)
	if err := s.fleet.save(s.FleetFile); err != nil {
		s.fleet.members[id] = m
		fail(w, err)
		return
	}
	s.info(r, "proxy forgotten", "proxy", id)
	writeJSON(w, http.StatusOK, map[string]string{"forgotten": id})
}

// fenceRound waits until every member it must wait for has installed version v, or until wait
// runs out, and returns the ids it is still waiting on. strict waits for every member, live or
// not: a round that takes a bucket out of ACTIVE, where a silent member may still be writing every
// key to the source. Otherwise only live members count: a member that fell silent after the
// bucket left ACTIVE stops routing its writes when its lease lapses.
func (s *Server) fenceRound(ctx context.Context, v int64, strict bool, wait time.Duration) ([]string, error) {
	start := s.now()
	for {
		ms, err := s.members()
		if err != nil {
			return nil, err
		}
		s.publishFleet(ms)
		var waiting []string
		for _, m := range ms {
			if (strict || m.Live) && m.Applied < v {
				waiting = append(waiting, m.ID)
			}
		}
		if len(waiting) == 0 {
			if s.Metrics != nil && len(ms) > 0 {
				s.Metrics.FenceWait.Observe(s.now().Sub(start).Seconds())
			}
			return nil, nil
		}
		if s.now().Sub(start) >= wait {
			return waiting, nil
		}
		if err := s.sleep(ctx, s.fencePoll()); err != nil {
			return waiting, err
		}
	}
}

func (s *Server) fencePoll() time.Duration {
	if s.FencePoll > 0 {
		return s.FencePoll
	}
	return 100 * time.Millisecond
}

// counted reports whether a fence round with this strictness waits on any member at all, which is
// what decides whether a step that moves writes has to be held first.
func (s *Server) counted(strict bool) (bool, error) {
	ms, err := s.members()
	if err != nil {
		return false, err
	}
	for _, m := range ms {
		if strict || m.Live {
			return true, nil
		}
	}
	return false, nil
}

func waitingOn(ids []string) string {
	return "waiting on " + strings.Join(ids, ", ")
}

// fleetFallbackReads sums the fallback-read counter for key over this proxy and every live member,
// as of each one's last heartbeat, and returns each member's heartbeat count so a caller can tell
// which have reported since.
func (s *Server) fleetFallbackReads(key string) (total float64, beats map[string]int64, err error) {
	if s.Metrics != nil {
		total = counters(s.Metrics.FallbackReads, key, "")[""]
	}
	s.fleet.mu.Lock()
	defer s.fleet.mu.Unlock()
	if err := s.fleet.load(s.FleetFile); err != nil {
		return 0, nil, err
	}
	beats = map[string]int64{}
	now := s.now()
	for id, m := range s.fleet.members {
		if !s.liveAt(m, now) {
			continue
		}
		total += m.FallbackReads[key]
		beats[id] = m.Beats
	}
	return total, beats, nil
}

// awaitReports waits until every member in since has sent two more heartbeats: the second was put
// together after the member had the answer to the first, so its counters are from after the call.
func (s *Server) awaitReports(ctx context.Context, since map[string]int64, wait time.Duration) ([]string, error) {
	start := s.now()
	for {
		var missing []string
		s.fleet.mu.Lock()
		for id, n := range since {
			if m, ok := s.fleet.members[id]; !ok || m.Beats < n+2 {
				missing = append(missing, id)
			}
		}
		s.fleet.mu.Unlock()
		if len(missing) == 0 {
			return nil, nil
		}
		slices.Sort(missing)
		if s.now().Sub(start) >= wait {
			return missing, nil
		}
		if err := s.sleep(ctx, s.fencePoll()); err != nil {
			return missing, err
		}
	}
}

// memberOnly answers every mutation on a member's own control API: changes go through the control
// node, which is the one place that knows the fleet.
func (s *Server) memberOnly(w http.ResponseWriter) {
	writeError(w, http.StatusConflict, "refused", fmt.Sprintf("this proxy is a fleet member; its control node is %s: send changes there (ADR-0016)", s.ControlNode))
}

// Membership is a member proxy's side of the fleet: it sends the control node a heartbeat and
// knows when its lease has lapsed (ADR-0016).
type Membership struct {
	Endpoint string // the control node's admin listener, http(s)://host:port
	Token    string // bearer token for it; empty for a loopback control node without one
	ID       string
	Interval time.Duration
	LeaseTTL time.Duration
	Dir      *directory.FileDir
	Metrics  *telemetry.Metrics
	Log      *slog.Logger
	Client   *http.Client
	Now      func() time.Time

	started time.Time
	// lastAck is the send time of the last acknowledged heartbeat; nil: none yet. A time.Time from
	// time.Now carries a monotonic reading, so the lease is measured by elapsed time, and a wall
	// clock stepped backwards cannot extend it (a Unix timestamp would lose that reading).
	lastAck atomic.Pointer[time.Time]
	stale   atomic.Bool
}

func (m *Membership) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// Stale reports whether the lease has lapsed. A member that has never had a heartbeat answered is
// stale: it may be starting while the control node is unreachable, with no way to know whether a
// bucket it sees as ACTIVE has started to move.
func (m *Membership) Stale() bool {
	ack := m.lastAck.Load()
	return ack == nil || m.now().Sub(*ack) > m.LeaseTTL
}

// Register sends the first heartbeat, before this proxy serves anything, so the control node
// counts it as a member from its first request: a bucket's first step then waits for it. An error
// means the control node could not be reached or this proxy could not install its version; the
// proxy may still start, stale, and Run keeps trying.
func (m *Membership) Register(ctx context.Context) error {
	m.started = m.now().UTC().Truncate(time.Second)
	return m.beat(ctx)
}

// Run sends heartbeats until ctx ends. Register, when called first, sent the first one.
func (m *Membership) Run(ctx context.Context) {
	if m.started.IsZero() {
		m.started = m.now().UTC().Truncate(time.Second)
	}
	t := time.NewTicker(m.Interval)
	defer t.Stop()
	for {
		_ = m.beat(ctx) //nolint:errcheck // beat logs lease changes itself; the next tick retries
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// beat sends one heartbeat and records whether the lease holds.
func (m *Membership) beat(ctx context.Context) error {
	sent := m.now()
	ans, err := m.send(ctx)
	if err == nil {
		if ans.Version > m.Dir.Snapshot().Version() {
			// The control node has a newer directory: read it now rather than at the next poll.
			if _, rerr := m.Dir.Reload(); rerr != nil && m.Log != nil {
				m.Log.Warn("directory reload after heartbeat failed", "err", rerr.Error())
			}
		}
		// The lease renews only once this proxy has what the control node has. A member coming
		// back from silence may have missed steps that went ahead without it; until it has
		// installed them it stays stale, or it would write a moved key to the source.
		if v := m.Dir.Snapshot().Version(); v >= ans.Version {
			m.lastAck.Store(&sent)
		} else {
			err = fmt.Errorf("directory version %d is behind the control node's %d", v, ans.Version)
		}
	}
	stale := m.Stale()
	if was := m.stale.Swap(stale); was != stale && m.Log != nil {
		if stale {
			m.Log.Warn("lease with the control node lapsed: writes on moving buckets are refused until it is back (ADR-0016)",
				"control", m.Endpoint, "lease_ttl", m.LeaseTTL.String(), "err", errString(err))
		} else {
			m.Log.Info("lease with the control node holds", "control", m.Endpoint, "proxy", m.ID)
		}
	}
	if m.Metrics != nil {
		v := 0.0
		if stale {
			v = 1
		}
		m.Metrics.FleetStale.Set(v)
	}
	return err
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (m *Membership) send(ctx context.Context) (HeartbeatAnswer, error) {
	snap := m.Dir.Snapshot()
	hb := Heartbeat{Started: m.started, Applied: snap.Version()}
	f := snap.File()
	for _, key := range sortedKeys(f.Placements) {
		if f.Placements[key].State == directory.StateActive {
			continue
		}
		if hb.FallbackReads == nil {
			hb.FallbackReads = map[string]float64{}
		}
		if m.Metrics != nil {
			hb.FallbackReads[key] = counters(m.Metrics.FallbackReads, key, "")[""]
		}
	}
	body, err := json.Marshal(hb)
	if err != nil {
		return HeartbeatAnswer{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, m.Interval)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(m.Endpoint, "/")+"/v1/fleet/"+m.ID+"/heartbeat", bytes.NewReader(body))
	if err != nil {
		return HeartbeatAnswer{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if m.Token != "" {
		req.Header.Set("Authorization", "Bearer "+m.Token)
	}
	c := m.Client
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return HeartbeatAnswer{}, err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return HeartbeatAnswer{}, err
	}
	if resp.StatusCode != http.StatusOK {
		var e Error
		_ = json.Unmarshal(data, &e) //nolint:errcheck // a non-JSON body just has no reason
		return HeartbeatAnswer{}, fmt.Errorf("control node answered %d: %s", resp.StatusCode, e.Message)
	}
	var ans HeartbeatAnswer
	if err := json.Unmarshal(data, &ans); err != nil {
		return HeartbeatAnswer{}, fmt.Errorf("control node answer: %w", err)
	}
	return ans, nil
}

// MemberStatus is a member's answer on /-/fleet.
type MemberStatus struct {
	ID          string `json:"id"`
	ControlNode string `json:"control_node"`
	Stale       bool   `json:"stale"`
	LastAck     string `json:"last_ack,omitempty"` // how long ago
	Applied     int64  `json:"applied"`
}

// ServeHTTP answers /-/fleet on a member's admin listener.
func (m *Membership) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	st := MemberStatus{ID: m.ID, ControlNode: m.Endpoint, Stale: m.Stale(), Applied: m.Dir.Snapshot().Version()}
	if ack := m.lastAck.Load(); ack != nil {
		st.LastAck = m.now().Sub(*ack).Round(time.Millisecond).String()
	}
	writeJSON(w, http.StatusOK, st)
}
