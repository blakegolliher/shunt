package cp

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/telemetry"
)

// Observed health of the control plane's own members (ADR-0021 D4, H3d, T13). A member is healthy
// only when a node has lately reached its control API and found it running as that member with a
// leader in view; a member that has a name is not healthy for having one. Each node samples on its
// own, in the background, on a fixed cycle, so the work does not grow with the number of people
// watching the Control plane screen: a status read serves the last sample, and a sample older than
// Stale reads unknown, never healthy. Quorum is observed apart, by a bounded linearizable read of
// this node's own: a leader in view and a count of named members are not evidence of it. The
// membership itself is this node's local view, which needs no quorum, so status stays readable,
// marked partial, while quorum is lost.

// Health states of a member, and of quorum.
const (
	HealthHealthy     = "healthy"
	HealthUnreachable = "unreachable"
	HealthUnknown     = "unknown"
	QuorumReachable   = "reachable"
	QuorumUnavailable = "unavailable"
)

// nodesPrefix holds each member's advertised control API, by member ID: where the other members'
// health sampler reaches it. It lies outside the directory's prefix, which the store watches.
const nodesPrefix = "/shunt/control-nodes/"

type nodeRecord struct {
	Name   string `json:"name"`
	APIURL string `json:"api_url"`
}

// MemberHealth is one control-plane member as this node last observed it.
type MemberHealth struct {
	ID      string   `json:"id"`
	Name    string   `json:"name,omitempty"`
	Role    string   `json:"role"` // voter or learner
	Leader  bool     `json:"leader,omitempty"`
	PeerURL []string `json:"peer_urls"`
	// Started says the member has run under its name, once: history, never health.
	Started bool   `json:"started"`
	APIURL  string `json:"api_url,omitempty"`
	// Health is healthy, unreachable or unknown; ObservedAt and Observer say when and by which
	// node, and Reason why it is not healthy. AgeMS is ObservedAt's age when the answer was made.
	Health     string    `json:"health"`
	ObservedAt time.Time `json:"observed_at,omitzero"`
	AgeMS      int64     `json:"age_ms,omitempty"`
	Observer   string    `json:"observer,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

// QuorumObservation is quorum as this node last observed it: reachable when a linearizable read
// through it answered, unavailable when it did not (ErrorCode says how), unknown before the first
// read or once the last is older than Stale.
type QuorumObservation struct {
	State      string    `json:"state"`
	ObservedAt time.Time `json:"observed_at,omitzero"`
	AgeMS      int64     `json:"age_ms,omitempty"`
	Observer   string    `json:"observer,omitempty"`
	Method     string    `json:"method"`
	ErrorCode  string    `json:"error_code,omitempty"`
}

// LocalStatus is GET /v1/control/local-status: this node's own etcd member, from its own view,
// without probing any other: what the other members' health sampler asks.
type LocalStatus struct {
	Node         string `json:"node"`
	MemberID     string `json:"member_id"`
	Learner      bool   `json:"learner"`
	Leader       string `json:"leader,omitempty"` // the leader's member ID as this member sees it; "" for none
	AppliedIndex uint64 `json:"applied_index"`
}

// LocalStatus is this member's own status.
func (n *Node) LocalStatus() LocalStatus {
	ls := LocalStatus{Node: n.cfg.Name, MemberID: fmt.Sprintf("%x", n.ID()), Learner: n.learner(), AppliedIndex: n.e.Server.AppliedIndex()}
	if l := uint64(n.e.Server.Leader()); l != 0 {
		ls.Leader = fmt.Sprintf("%x", l)
	}
	return ls
}

// RegisterAPI records this member's advertised control API, if it is not already, for the other
// members' health sampler. It needs quorum: shunt-control calls it once the store has loaded.
func (n *Node) RegisterAPI(ctx context.Context, apiURL string) error {
	key := fmt.Sprintf("%s%x", nodesPrefix, n.ID())
	want, err := json.Marshal(nodeRecord{Name: n.cfg.Name, APIURL: apiURL})
	if err != nil {
		return err
	}
	resp, err := n.cli.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("etcd: %w", err)
	}
	if len(resp.Kvs) == 1 && bytes.Equal(resp.Kvs[0].Value, want) {
		return nil
	}
	if _, err := n.cli.Put(ctx, key, string(want)); err != nil {
		return fmt.Errorf("etcd: %w", err)
	}
	return nil
}

// apiURLs reads every member's advertised control API from this member's own copy of the store (a
// serializable read: no quorum needed).
func (n *Node) apiURLs(ctx context.Context) (map[uint64]string, error) {
	resp, err := n.cli.Get(ctx, nodesPrefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var id uint64
		var rec nodeRecord
		if _, err := fmt.Sscanf(strings.TrimPrefix(string(kv.Key), nodesPrefix), "%x", &id); err != nil || json.Unmarshal(kv.Value, &rec) != nil {
			continue
		}
		out[id] = rec.APIURL
	}
	return out, nil
}

// localMember is one member of this node's local view of the membership.
type localMember struct {
	id       uint64
	name     string
	learner  bool
	peerURLs []string
}

// localMembers is the membership as this node's own etcd server has applied it: available with or
// without quorum, and as current as this node is.
func (n *Node) localMembers() []localMember {
	ms := n.e.Server.Cluster().Members()
	out := make([]localMember, 0, len(ms))
	for _, m := range ms {
		out = append(out, localMember{id: uint64(m.ID), name: m.Name, learner: m.IsLearner, peerURLs: slices.Clone(m.PeerURLs)})
	}
	slices.SortFunc(out, func(a, b localMember) int { return cmp.Compare(a.id, b.id) })
	return out
}

// membershipRevision is a digest of the membership: it changes when a member is added, removed,
// promoted or starts under its name.
func membershipRevision(ms []localMember) string {
	h := sha256.New()
	for _, m := range ms {
		_, _ = fmt.Fprintf(h, "%x|%s|%t|%s\n", m.id, m.name, m.learner, strings.Join(m.peerURLs, ",")) //nolint:errcheck // a hash does not fail
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Health samples this node's view of every control-plane member's health, and quorum. The zero
// values of its settings take the defaults of the design: a 5 s cycle with up to 1 s of jitter, a
// 3 s deadline per cycle, five probes at a time, and samples stale after 15 s.
type Health struct {
	Node        *Node
	Token       string // the bearer token the other members' API takes
	Interval    time.Duration
	Timeout     time.Duration
	Stale       time.Duration
	Concurrency int
	Now         func() time.Time
	Metrics     *telemetry.Metrics
	// Client probes the other members' APIs; http.DefaultClient's transport when nil.
	Client *http.Client

	// probe and quorumRead replace the HTTP probe and the linearizable read in tests.
	probe      func(ctx context.Context, apiURL string) (LocalStatus, error)
	quorumRead func(ctx context.Context) error
	members    func() []localMember // the local view; Node's when nil

	mu     sync.Mutex
	obs    map[uint64]observation
	apis   map[uint64]string
	quorum QuorumObservation
	probes atomic.Int64 // probes sent, every cycle: what the viewer-independence test counts
}

type observation struct {
	at     time.Time
	health string
	reason string
}

func (h *Health) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *Health) settings() (interval, timeout, stale time.Duration, concurrency int) {
	interval, timeout, stale, concurrency = h.Interval, h.Timeout, h.Stale, h.Concurrency
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if stale <= 0 {
		stale = 15 * time.Second
	}
	if concurrency <= 0 {
		concurrency = 5
	}
	return interval, timeout, stale, concurrency
}

func (h *Health) localMembers() []localMember {
	if h.members != nil {
		return h.members()
	}
	return h.Node.localMembers()
}

func (h *Health) observer() string {
	if h.Node == nil {
		return ""
	}
	return h.Node.Name()
}

// Run samples until ctx ends: one cycle, then the next after the interval and a jitter. A cycle
// ends by its deadline, so none overlaps the next.
func (h *Health) Run(ctx context.Context) {
	interval, _, _, _ := h.settings()
	for {
		h.Cycle(ctx)
		wait := interval + time.Duration(rand.Int64N(int64(interval/5)+1)) //nolint:gosec // G404: jitter, not a secret
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// Cycle observes every member once, and quorum, within one deadline.
func (h *Health) Cycle(ctx context.Context) {
	_, timeout, _, concurrency := h.settings()
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	members := h.localMembers()
	if apis, err := h.readAPIs(cctx); err == nil {
		h.mu.Lock()
		h.apis = apis
		h.mu.Unlock()
	}
	h.mu.Lock()
	apis := h.apis
	h.mu.Unlock()

	var wg sync.WaitGroup
	wg.Go(func() { h.observeQuorum(cctx) })
	sem := make(chan struct{}, concurrency)
	results := make([]observation, len(members))
	self := h.selfID()
	for i, m := range members {
		switch {
		case m.id == self:
			results[i] = h.observeSelf()
			continue
		case m.name == "":
			results[i] = observation{at: h.now(), health: HealthUnknown, reason: "added and never started: nothing to probe"}
			continue
		case apis[m.id] == "":
			results[i] = observation{at: h.now(), health: HealthUnknown, reason: "no control API registered for it: it has not started since it joined"}
			continue
		}
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-cctx.Done():
				results[i] = observation{at: h.now(), health: HealthUnreachable, reason: "not probed within the cycle's deadline"}
				return
			}
			defer func() { <-sem }()
			results[i] = h.observeMember(cctx, m, apis[m.id])
		})
	}
	wg.Wait()
	h.mu.Lock()
	if h.obs == nil {
		h.obs = map[uint64]observation{}
	}
	keep := map[uint64]bool{}
	for i, m := range members {
		h.obs[m.id] = results[i]
		keep[m.id] = true
	}
	for id := range h.obs {
		if !keep[id] {
			delete(h.obs, id)
		}
	}
	h.mu.Unlock()
	h.publish(members)
}

func (h *Health) selfID() uint64 {
	if h.Node == nil {
		return 0
	}
	return h.Node.ID()
}

func (h *Health) readAPIs(ctx context.Context) (map[uint64]string, error) {
	if h.Node == nil {
		return nil, errors.New("no node")
	}
	return h.Node.apiURLs(ctx)
}

// observeSelf is this node's own member: healthy while its etcd server has a leader in view.
func (h *Health) observeSelf() observation {
	if h.Node == nil || h.Node.e.Server.Leader() == 0 {
		return observation{at: h.now(), health: HealthUnreachable, reason: "this node's etcd has no leader in view"}
	}
	return observation{at: h.now(), health: HealthHealthy}
}

// observeMember probes one member's control API: healthy when it answers, as that member, with a
// leader in view.
func (h *Health) observeMember(ctx context.Context, m localMember, apiURL string) observation {
	h.probes.Add(1)
	probe := h.probe
	if probe == nil {
		probe = h.httpProbe
	}
	ls, err := probe(ctx, apiURL)
	o := observation{at: h.now()}
	switch {
	case err != nil:
		o.health, o.reason = HealthUnreachable, probeFailure(err)
	case ls.MemberID != fmt.Sprintf("%x", m.id):
		o.health, o.reason = HealthUnreachable, fmt.Sprintf("its control API answered as member %s", ls.MemberID)
	case ls.Leader == "":
		o.health, o.reason = HealthUnreachable, "its etcd has no leader in view"
	default:
		o.health = HealthHealthy
	}
	return o
}

// probeFailure names why a probe failed, as a code a status can show: never the raw error, which
// may carry a URL or a response body.
func probeFailure(err error) string {
	var se *statusError
	var ne net.Error
	switch {
	case errors.As(err, &se):
		return fmt.Sprintf("its control API answered HTTP %d", se.status)
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "no answer before the cycle's deadline"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused: shunt-control is not running there"
	}
	return "its control API cannot be reached"
}

type statusError struct{ status int }

func (e *statusError) Error() string { return fmt.Sprintf("HTTP %d", e.status) }

// maxLocalStatus bounds a probe's answer.
const maxLocalStatus = 64 << 10

func (h *Health) httpProbe(ctx context.Context, apiURL string) (LocalStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(apiURL, "/")+"/v1/control/local-status", http.NoBody)
	if err != nil {
		return LocalStatus{}, err
	}
	if h.Token != "" {
		req.Header.Set("Authorization", "Bearer "+h.Token)
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return LocalStatus{}, err
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	if resp.StatusCode != http.StatusOK {
		return LocalStatus{}, &statusError{status: resp.StatusCode}
	}
	var ls LocalStatus
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxLocalStatus)).Decode(&ls); err != nil {
		return LocalStatus{}, err
	}
	return ls, nil
}

// observeQuorum reads through this node linearizably: an answer is evidence of quorum, a timeout or
// a refusal of its absence.
func (h *Health) observeQuorum(ctx context.Context) {
	read := h.quorumRead
	if read == nil {
		read = func(ctx context.Context) error {
			_, err := h.Node.cli.Get(ctx, kVersion) // linearizable: it needs a leader and a majority
			return err
		}
	}
	err := read(ctx)
	q := QuorumObservation{ObservedAt: h.now(), Observer: h.observer(), Method: "linearizable_read", State: QuorumReachable}
	if err != nil {
		q.State, q.ErrorCode = QuorumUnavailable, "error"
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline exceeded") {
			q.ErrorCode = "timeout"
		} else if strings.Contains(err.Error(), "no leader") {
			q.ErrorCode = "no_leader"
		}
	}
	h.mu.Lock()
	h.quorum = q
	h.mu.Unlock()
}

// Snapshot is the membership with each member's last observation, and quorum's, as of now: an
// observation older than Stale, or none yet, reads unknown. It reads only what the cycles
// recorded; it never probes.
func (h *Health) Snapshot(leader uint64) ([]MemberHealth, QuorumObservation, string) {
	_, _, stale, _ := h.settings()
	now := h.now()
	members := h.localMembers()
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]MemberHealth, 0, len(members))
	for _, m := range members {
		mh := MemberHealth{ID: fmt.Sprintf("%x", m.id), Name: m.name, Role: "voter", Leader: m.id == leader, PeerURL: m.peerURLs,
			Started: m.name != "", APIURL: h.apis[m.id], Health: HealthUnknown, Reason: "not observed yet"}
		if m.learner {
			mh.Role = "learner"
		}
		if o, ok := h.obs[m.id]; ok {
			age := now.Sub(o.at)
			mh.ObservedAt, mh.AgeMS, mh.Observer = o.at.UTC(), age.Milliseconds(), h.observer()
			mh.Health, mh.Reason = o.health, o.reason
			if age > stale {
				mh.Health, mh.Reason = HealthUnknown, fmt.Sprintf("last observed %s ago, longer than %s: stale", age.Round(time.Second), stale)
			}
		}
		out = append(out, mh)
	}
	q := h.quorum
	if q.Method == "" {
		q = QuorumObservation{State: HealthUnknown, Method: "linearizable_read", Observer: h.observer()}
	} else {
		age := now.Sub(q.ObservedAt)
		q.ObservedAt, q.AgeMS = q.ObservedAt.UTC(), age.Milliseconds()
		if age > stale {
			q.State, q.ErrorCode = HealthUnknown, "stale"
		}
	}
	return out, q, membershipRevision(members)
}

// publish sets each member's observation age on shunt_control_health_observation_age_seconds.
func (h *Health) publish(members []localMember) {
	if h.Metrics == nil {
		return
	}
	now := h.now()
	h.Metrics.ControlHealthAge.Reset()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range members {
		o, ok := h.obs[m.id]
		if !ok {
			continue
		}
		label := m.name
		if label == "" {
			label = fmt.Sprintf("%x", m.id)
		}
		h.Metrics.ControlHealthAge.WithLabelValues(label).Set(now.Sub(o.at).Seconds())
	}
}
