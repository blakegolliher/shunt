package control

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// The fleet (ADR-0016). The control plane keeps, per member proxy, the directory version it has
// installed and its fallback-read counters for moving buckets, and holds a fenced change until
// every member it must wait for has that change installed. The table lives in the control plane's
// store (leased keys in etcd, internal/cp); this package reads and writes it through Fleet.

// defaultFenceWait is how long a fenced change waits for the fleet before answering pending.
const defaultFenceWait = 30 * time.Second

// ErrUnavailable is wrapped by a Fleet or Store that cannot reach its backing store right now: the
// control plane has lost quorum, or this node is starting. Answered as 503.
var ErrUnavailable = errors.New("control plane unavailable")

// Protocol is the fleet protocol this build speaks (ADR-0021). There is no mixed-version fleet: a
// heartbeat in another protocol is refused, and the proxy that sent it never takes a lease.
const Protocol = 2

// Lineage refusal codes: the request was made against a directory lineage that is not the
// control plane's (answered 409 with its current identity).
const (
	CodeClusterMismatch = "cluster_mismatch" // another cluster's directory
	CodeEpochMismatch   = "epoch_mismatch"   // another recovery lineage of this cluster
	CodeResyncRequired  = "resync_required"  // same lineage, but the caller claims a version this node does not have
	CodeProtocol        = "protocol_unsupported"
)

// lineageError is a request made against another lineage than cur.
type lineageError struct {
	code string
	msg  string
	cur  directory.Identity
}

func (e *lineageError) Error() string { return e.msg }

// LineageMismatch is the refusal of a request made on lineage have when the directory is on cur.
func LineageMismatch(cur, have directory.Identity) error {
	if have.ClusterID != cur.ClusterID {
		return &lineageError{code: CodeClusterMismatch, cur: cur, msg: fmt.Sprintf("this control plane serves cluster %s; the caller's directory is from cluster %s", cur.ClusterID, have.ClusterID)}
	}
	return &lineageError{code: CodeEpochMismatch, cur: cur, msg: fmt.Sprintf("the directory is in epoch %s; the caller's is from epoch %s, another recovery lineage: its versions compare with nothing here", cur.Epoch, have.Epoch)}
}

// checkLineage compares a caller's directory (have, at version v) with the installed one. A caller
// with no identity has nothing installed yet. Versions compare only within one identity.
func checkLineage(snap *directory.Snapshot, have directory.Identity, v int64) error {
	cur := snap.File().Identity
	switch {
	case have.IsZero():
		return nil
	case have != cur:
		return LineageMismatch(cur, have)
	case v > snap.Version():
		return &lineageError{code: CodeResyncRequired, cur: cur, msg: fmt.Sprintf("the caller claims directory version %d; this control node has %d in the same epoch", v, snap.Version())}
	}
	return nil
}

// Incarnation is one process of a proxy (ADR-0021 D2). A proxy id is stable across restarts; each
// process draws an incarnation, registers it with its first heartbeat, and retires it when it
// stops cleanly, having drained its backend work. An incarnation that ended any other way is
// unresolved: the work it dispatched may still land on a backend, so every barrier waits on it
// until an operator resolves it (shunt proxy resolve) with an attestation that its work has ended.
type Incarnation struct {
	ID      string    `json:"id"`
	Started time.Time `json:"started,omitzero"`
	Ended   time.Time `json:"ended,omitzero"`
	// State is IncarnationActive, IncarnationRetired, IncarnationUnclean or IncarnationResolved.
	State string `json:"state"`
	// Uncertain is how many backend outcomes the incarnation never learned; a retirement that
	// reports any is unclean.
	Uncertain int64 `json:"uncertain,omitempty"`
	// Attestation, ResolvedBy and ResolvedAt record an operator's resolution of an unclean end.
	Attestation string    `json:"attestation,omitempty"`
	ResolvedBy  string    `json:"resolved_by,omitempty"`
	ResolvedAt  time.Time `json:"resolved_at,omitzero"`
}

// The states of an incarnation.
const (
	IncarnationActive   = "active"   // heartbeating, or silent and not yet replaced
	IncarnationRetired  = "retired"  // stopped cleanly: admission closed, every outcome learned
	IncarnationUnclean  = "unclean"  // replaced without retiring, or retired with outcomes unknown
	IncarnationResolved = "resolved" // an operator attested its backend work has ended
)

// MaxUnresolvedIncarnations bounds the unresolved incarnations kept per proxy. At the bound a new
// incarnation is refused rather than an old one dropped: evidence is not discarded silently.
const MaxUnresolvedIncarnations = 8

// ValidIncarnation reports whether id is an incarnation id: 32 lowercase hex characters.
func ValidIncarnation(id string) bool {
	if len(id) != 32 {
		return false
	}
	for i := 0; i < len(id); i++ {
		if c := id[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Heartbeat is a member's report to the control plane.
type Heartbeat struct {
	// Protocol is the fleet protocol the member speaks; it must be Protocol.
	Protocol int `json:"protocol"`
	// Identity is the lineage of the directory the member has installed (Applied); zero before
	// it has installed one.
	Identity directory.Identity `json:"identity,omitzero"`
	Started  time.Time          `json:"started"`
	// Incarnation is this process's id. Previous is the process before it on this proxy, as the
	// proxy's own marker recorded its end (retired, or unclean: it never got to retire), sent
	// until the control plane says it has recorded it (HeartbeatAnswer.PreviousRecorded): a
	// process that crashed while the control plane was unreachable is evidence only the proxy
	// holds. Uncertain is how many backend outcomes this process has never learned.
	Incarnation string       `json:"incarnation"`
	Previous    *Incarnation `json:"previous,omitempty"`
	Uncertain   int64        `json:"uncertain,omitempty"`
	// Seq counts this member's heartbeats since it started, so a caller can tell which reports
	// were assembled after a point in time.
	Seq int64 `json:"seq"`
	// Applied is the directory version the member has installed.
	Applied int64 `json:"applied"`
	// Durable is the version the member's restart cache holds durably (fsynced, renamed, its
	// directory synced); 0 when none. It lags Applied while a write is in progress or failing
	// (ADR-0021 D1).
	Durable int64 `json:"durable,omitempty"`
	// Installed is the version the member has installed, when it is ahead of Applied: installs are
	// backpressured, waiting for a retired runtime bundle to drain (ADR-0021 D1). 0 otherwise.
	Installed int64 `json:"installed,omitempty"`
	// CacheError is why the member's newest version is not durable, when it is not.
	CacheError string `json:"cache_error,omitempty"`
	// FallbackReads is shunt_migration_fallback_reads_total for each bucket that is not ACTIVE in
	// the member's snapshot: bounded by migrations in flight, never by buckets.
	FallbackReads map[string]float64 `json:"fallback_reads,omitempty"`
	// Host and Version say where the member runs and which build it is, for the fleet view.
	Host    string `json:"host,omitempty"`
	Version string `json:"version,omitempty"`
	// Secrets is the secret generation each cluster's signer has on the member, as decimal
	// strings: what it signs with since its last install (ADR-0021 D1). A cluster the control plane
	// holds no secret for is absent.
	Secrets map[string]string `json:"secrets,omitempty"`
	// Barriers is the member's drain proof for every barrier in its installed directory
	// (ADR-0021 D2): whether it has closed the barrier's gates, and how many requests are still
	// out and how many backend outcomes it never learned through them. Bounded by barriers in
	// flight; a member drops its telemetry window before any of this when a heartbeat would pass
	// the size cap.
	Barriers []admission.Ack `json:"barriers,omitempty"`
	// Telemetry is the member's last completed 10-second window. It is re-sent until the next
	// window closes, and the control node de-duplicates it by proxy and start time.
	Telemetry *telemetry.Window `json:"telemetry,omitempty"`
}

// MaxBarrierAcks bounds the acknowledgements one heartbeat carries: unfinished operations are
// capped (DefaultCapacity), so a member reporting more is malformed.
const MaxBarrierAcks = 4096

// validateAcks checks a heartbeat's barrier acknowledgements: bounded, well-formed, no negative
// counts (T20).
func validateAcks(acks []admission.Ack) error {
	if len(acks) > MaxBarrierAcks {
		return fmt.Errorf("barriers: %d acknowledgements, at most %d", len(acks), MaxBarrierAcks)
	}
	for i := range acks {
		a := &acks[i]
		if !config.ValidBarrierID(a.ID) {
			return fmt.Errorf("barriers[%d].id %q: want 1-64 letters, digits, '.', '_', ':' or '-'", i, a.ID)
		}
		if _, ok := admission.KindOf(a.Kind); !ok {
			return fmt.Errorf("barriers[%d].kind %q: want %s or %s", i, a.Kind, config.BarrierMutations, config.BarrierSource)
		}
		if !strings.HasPrefix(a.Scope, "placement:") && !strings.HasPrefix(a.Scope, "cluster:") {
			return fmt.Errorf("barriers[%d].scope %q: want placement:<tenant>/<bucket> or cluster:<name>", i, a.Scope)
		}
		if a.Generation < 0 || a.Inflight < 0 || a.Uncertain < 0 {
			return fmt.Errorf("barriers[%d]: negative generation or count", i)
		}
	}
	return nil
}

// HeartbeatAnswer tells a member the current version and its lease. Seq echoes the heartbeat it
// answers: a member takes a lease only from the answer to the heartbeat it sent.
type HeartbeatAnswer struct {
	Seq      int64              `json:"seq"`
	Identity directory.Identity `json:"identity"`
	Version  int64              `json:"version"`
	LeaseTTL time.Duration      `json:"lease_ttl"`
	// Retire asks the member to retire: stop admitting, drain, record its retirement, and stop
	// (an operator's `shunt proxy retire`). PreviousRecorded says the previous incarnation the
	// heartbeat carried is on record and need not be sent again.
	Retire           bool `json:"retire,omitempty"`
	PreviousRecorded bool `json:"previous_recorded,omitempty"`
}

// Grant is what the fleet table answers a heartbeat with.
type Grant struct {
	LeaseTTL         time.Duration
	Retire           bool
	PreviousRecorded bool
}

// Member is one registered proxy as the control plane sees it.
type Member struct {
	ID string `json:"id"`
	// Live: its lease has not expired. A member stays a member after its lease expires, until it
	// is forgotten; a bucket's first step waits for it either way.
	Live bool `json:"live"`
	// Incarnation is the member's current process: active, or retired when it stopped cleanly
	// and no process has replaced it yet. Unresolved are earlier processes that ended without a
	// clean retirement; every barrier waits on them, and forget refuses while any is there.
	// RetireRequested: an operator asked the member to retire, and its next heartbeat tells it.
	Incarnation     *Incarnation  `json:"incarnation,omitempty"`
	Unresolved      []Incarnation `json:"unresolved,omitempty"`
	RetireRequested bool          `json:"retire_requested,omitempty"`
	// Uncertain is Heartbeat.Uncertain from the member's last heartbeat; Barriers its drain proof.
	Uncertain int64           `json:"uncertain,omitempty"`
	Barriers  []admission.Ack `json:"barriers,omitempty"`
	// Identity is the lineage of the directory it last reported installed: Applied counts only
	// when it is the control plane's.
	Identity      directory.Identity `json:"identity,omitzero"`
	Applied       int64              `json:"applied"`
	Durable       int64              `json:"durable,omitempty"`     // Heartbeat.Durable
	Installed     int64              `json:"installed,omitempty"`   // Heartbeat.Installed
	CacheError    string             `json:"cache_error,omitempty"` // Heartbeat.CacheError
	Seq           int64              `json:"seq"`
	Started       time.Time          `json:"started,omitzero"`
	Seen          time.Time          `json:"seen,omitzero"`        // when its last heartbeat arrived
	SinceSeen     time.Duration      `json:"since_seen,omitempty"` // how long ago, by the control plane's clock
	FallbackReads map[string]float64 `json:"fallback_reads,omitempty"`
	Host          string             `json:"host,omitempty"`
	Version       string             `json:"version,omitempty"` // the member's build
	// Secrets is the secret generation of each cluster's signer on the member, from its last
	// heartbeat (Heartbeat.Secrets).
	Secrets   map[string]string `json:"secrets,omitempty"`
	Telemetry *telemetry.Window `json:"telemetry,omitempty"`
}

// Has reports whether the member has installed version v of lineage id. A larger version from
// another lineage (a member that outlived a restore) proves nothing about this directory.
func (m Member) Has(id directory.Identity, v int64) bool { return m.Identity == id && m.Applied >= v }

// Fleet is the fleet table. Two implementations: the control plane's, on leased etcd keys
// (internal/cp), and NoFleet for a single-node lab whose proxy has no members.
type Fleet interface {
	// Heartbeat records a member's report and renews its lease; the first from an id makes it a
	// member. A heartbeat from a new incarnation of a known member ends the previous one: retired
	// if it retired, unresolved otherwise. It is refused with a *RetirementError at
	// MaxUnresolvedIncarnations. The grant carries the lease TTL a member measures its own
	// staleness by.
	Heartbeat(ctx context.Context, id string, hb Heartbeat) (Grant, error)
	// Members lists every member, live or not.
	Members(ctx context.Context) ([]Member, error)
	// Retire records a member's own report that incarnation has stopped admitting and drained,
	// with the backend outcomes it never learned: clean at zero, unresolved otherwise. Lost
	// replies are retried; a repeat of a recorded retirement is fine.
	Retire(ctx context.Context, id, incarnation string, uncertain int64) error
	// RequestRetire asks a member to retire; its next heartbeat answer tells it.
	RequestRetire(ctx context.Context, id string) error
	// Resolve records an operator's attestation that an unresolved incarnation's backend work has
	// ended, and stops it blocking barriers. A silent member's current incarnation (a crashed
	// process that never retired) can be resolved too, and is then no longer current.
	Resolve(ctx context.Context, id, incarnation, attestation, actor string) error
	// Forget removes a member that is gone for good. Refused while its lease is live, and with a
	// *RetirementError while it has an incarnation that did not retire.
	Forget(ctx context.Context, id string) error
}

// RetirementError refuses a change that would take an unretired incarnation for gone: a forget,
// or a registration past the unresolved bound (409 retirement_unproven).
type RetirementError struct {
	Proxy        string
	Incarnations []Incarnation
	What         string // what was refused
}

func (e *RetirementError) Error() string {
	ids := make([]string, 0, len(e.Incarnations))
	for _, inc := range e.Incarnations {
		ids = append(ids, inc.ID)
	}
	return fmt.Sprintf("%s: proxy %s has %d incarnation(s) that did not retire cleanly (%s); the work they dispatched may still land on a backend. Once the host is confirmed stopped and its backend work ended, `shunt proxy resolve %s --incarnation <id> --attest <why>` records that, then the change is allowed",
		e.What, e.Proxy, len(e.Incarnations), strings.Join(ids, ", "), e.Proxy)
}

// Blockers are the incarnations, as an operation's blockers list them.
func (e *RetirementError) Blockers() []Blocker {
	out := make([]Blocker, 0, len(e.Incarnations))
	for _, inc := range e.Incarnations {
		out = append(out, Blocker{Code: BlockerIncarnationUnresolved, ProxyID: e.Proxy, Incarnation: inc.ID, Count: inc.Uncertain})
	}
	return out
}

// CodeRetirementUnproven is the refusal of a forget, or of a registration, that would erase an
// unretired incarnation's evidence.
const CodeRetirementUnproven = "retirement_unproven"

// NoFleet is the fleet of a single-node lab: `shunt serve --plaintext`, whose in-process control
// API has no members. Members join shunt-control, never a proxy.
type NoFleet struct{}

// Heartbeat implements Fleet: refused, naming where members belong.
func (NoFleet) Heartbeat(context.Context, string, Heartbeat) (Grant, error) {
	return Grant{}, refuse("this shunt is a single-node lab proxy and takes no members; a fleet's members send their heartbeat to shunt-control (ADR-0015)")
}

// Members implements Fleet: none.
func (NoFleet) Members(context.Context) ([]Member, error) { return nil, nil }

// Retire implements Fleet: no members.
func (NoFleet) Retire(_ context.Context, id, _ string, _ int64) error {
	return notFound("no proxy %s: this shunt has no members", id)
}

// RequestRetire implements Fleet: no members.
func (NoFleet) RequestRetire(_ context.Context, id string) error {
	return notFound("no proxy %s: this shunt has no members", id)
}

// Resolve implements Fleet: no members.
func (NoFleet) Resolve(_ context.Context, id, _, _, _ string) error {
	return notFound("no proxy %s: this shunt has no members", id)
}

// Forget implements Fleet: nothing to forget.
func (NoFleet) Forget(_ context.Context, id string) error {
	return notFound("no proxy %s: this shunt has no members", id)
}

// FleetStatus is the answer to GET /v1/fleet.
type FleetStatus struct {
	Identity directory.Identity `json:"identity"`
	Version  int64              `json:"version"`
	Members  []Member           `json:"members"`
}

func (s *Server) fleet() Fleet {
	if s.Fleet == nil {
		return NoFleet{}
	}
	return s.Fleet
}

// members returns the fleet table, sorted by id, and refreshes shunt_fleet_members.
func (s *Server) members(ctx context.Context) ([]Member, error) {
	ms, err := s.fleet().Members(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(ms, func(a, b Member) int { return strings.Compare(a.ID, b.ID) })
	if s.Metrics != nil {
		live := 0
		for _, m := range ms {
			if m.Live {
				live++
			}
		}
		s.Metrics.FleetMembers.WithLabelValues("live").Set(float64(live))
		s.Metrics.FleetMembers.WithLabelValues("silent").Set(float64(len(ms) - live))
	}
	return ms, nil
}

// PublishFleet refreshes shunt_fleet_members from the table, so a member that falls silent shows
// up without anyone asking (ADR-0016), and publishes what changed as fleet events.
func (s *Server) PublishFleet(ctx context.Context) error {
	ms, err := s.members(ctx)
	if err != nil {
		return err
	}
	s.fleetEvents(ms)
	if err := s.publishTelemetry(ms); err != nil {
		return err
	}
	return nil
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
	if hb.Protocol != Protocol {
		writeError(w, http.StatusBadRequest, CodeProtocol, fmt.Sprintf("proxy %s speaks fleet protocol %d; this control plane speaks %d and takes no other: upgrade the proxy (ADR-0021)", id, hb.Protocol, Protocol))
		return
	}
	if !ValidIncarnation(hb.Incarnation) {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("incarnation %q: want 32 lowercase hex characters", hb.Incarnation))
		return
	}
	if hb.Previous != nil && (!ValidIncarnation(hb.Previous.ID) || hb.Previous.ID == hb.Incarnation || (hb.Previous.State != IncarnationRetired && hb.Previous.State != IncarnationUnclean) || hb.Previous.Uncertain < 0) {
		writeError(w, http.StatusBadRequest, "bad_request", "previous: want another incarnation's id, its state retired or unclean, and a non-negative uncertain count")
		return
	}
	if hb.Uncertain < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "uncertain: want a non-negative count")
		return
	}
	if err := validateAcks(hb.Barriers); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	snap := s.Dir.Snapshot()
	if err := checkLineage(snap, hb.Identity, hb.Applied); err != nil {
		// No lease: a member on another lineage must not count as present for a fence.
		fail(w, err)
		return
	}
	g, err := s.fleet().Heartbeat(r.Context(), id, hb)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, HeartbeatAnswer{Seq: hb.Seq, Identity: snap.File().Identity, Version: snap.Version(), LeaseTTL: g.LeaseTTL,
		Retire: g.Retire, PreviousRecorded: g.PreviousRecorded})
}

// RetireRequest is POST /v1/fleet/{id}/retire. From a proxy, it reports that Incarnation has
// stopped admitting and drained, with the outcomes it never learned; from an operator (no
// incarnation), it asks the proxy to retire, which its next heartbeat answer tells it.
type RetireRequest struct {
	Incarnation string `json:"incarnation,omitempty"`
	Uncertain   int64  `json:"uncertain,omitempty"`
}

// ResolveRequest is POST /v1/fleet/{id}/resolve: an operator's attestation that an unretired
// incarnation's backend work has ended, so it no longer blocks barriers.
type ResolveRequest struct {
	Incarnation string `json:"incarnation"`
	Attestation string `json:"attestation"`
}

// MemberResult answers a fleet change with the member as it stands.
type MemberResult struct {
	Member          Member `json:"member"`
	RetireRequested bool   `json:"retire_requested,omitempty"`
}

func (s *Server) retireProxy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req RetireRequest
	if !decodeOptional(w, r, &req) {
		return
	}
	if req.Incarnation != "" {
		if !ValidIncarnation(req.Incarnation) || req.Uncertain < 0 {
			writeError(w, http.StatusBadRequest, "bad_request", "incarnation: want 32 lowercase hex characters, and a non-negative uncertain count")
			return
		}
		if err := s.fleet().Retire(r.Context(), id, req.Incarnation, req.Uncertain); err != nil {
			fail(w, err)
			return
		}
		s.info(actor(r), "proxy retired", "proxy", id, "incarnation", req.Incarnation, "uncertain", req.Uncertain)
	} else {
		if err := s.fleet().RequestRetire(r.Context(), id); err != nil {
			fail(w, err)
			return
		}
		s.info(actor(r), "proxy retirement requested", "proxy", id)
	}
	s.answerMember(w, r, id)
}

func (s *Server) resolveProxy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req ResolveRequest
	if !decode(w, r, &req) {
		return
	}
	if !ValidIncarnation(req.Incarnation) {
		writeError(w, http.StatusBadRequest, "bad_request", "incarnation: want 32 lowercase hex characters")
		return
	}
	if strings.TrimSpace(req.Attestation) == "" || len(req.Attestation) > 1024 {
		writeError(w, http.StatusBadRequest, "bad_request", "attestation: say, in at most 1024 characters, how it was established that the process is stopped and its backend work has ended")
		return
	}
	if err := s.fleet().Resolve(r.Context(), id, req.Incarnation, req.Attestation, actor(r)); err != nil {
		fail(w, err)
		return
	}
	s.info(actor(r), "proxy incarnation resolved", "proxy", id, "incarnation", req.Incarnation, "attestation", req.Attestation)
	s.answerMember(w, r, id)
}

// answerMember answers a fleet change with the member as the table now lists it.
func (s *Server) answerMember(w http.ResponseWriter, r *http.Request, id string) {
	ms, err := s.members(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	for _, m := range ms {
		if m.ID == id {
			writeJSON(w, http.StatusOK, MemberResult{Member: m, RetireRequested: m.RetireRequested})
			return
		}
	}
	writeError(w, http.StatusNotFound, "not_found", "no proxy "+id)
}

func (s *Server) fleetList(w http.ResponseWriter, r *http.Request) {
	ms, err := s.members(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	if ms == nil {
		ms = []Member{}
	}
	snap := s.Dir.Snapshot()
	writeJSON(w, http.StatusOK, FleetStatus{Identity: snap.File().Identity, Version: snap.Version(), Members: ms})
}

// ProxyDiagnostics is GET /v1/fleet/{id}: one member's install state against the control plane's
// directory (ADR-0021 D1). Problems says in words what the fields show, empty when nothing is off.
type ProxyDiagnostics struct {
	Member
	// Directory is the control plane's version and identity; Lineage whether the member's installed
	// directory is on it (false: another cluster or epoch, and its versions compare with nothing).
	Directory int64              `json:"directory"`
	Current   directory.Identity `json:"current"`
	Lineage   bool               `json:"lineage"`
	// Behind is how many directory versions the proxy's requests are behind; Backpressure whether
	// it has installed a newer one its requests do not use yet.
	Behind       int64 `json:"behind"`
	Backpressure bool  `json:"backpressure"`
	// Secrets compares, per cluster the control plane holds a secret for, the generation it holds
	// with the one the proxy signs with.
	Secrets []SecretInstall `json:"secrets"`
	// Lease is the member's lease as the control plane grants it (T07): what it answered the last
	// heartbeat with, and how long ago the control plane saw that heartbeat. The member measures
	// its own staleness from its send time, never from these.
	Lease    LeaseStatus `json:"lease"`
	Problems []string    `json:"problems"`
}

// LeaseStatus is a member's lease as the control plane sees it.
type LeaseStatus struct {
	// Granted is the TTL every heartbeat answer grants (the control plane's --lease-ttl); the
	// member runs its lease for the shorter of it and its own control.lease_ttl.
	Granted time.Duration `json:"granted"`
	// Seq is the last heartbeat the control plane recorded, Seen when it arrived, Age how long ago
	// by the control plane's clock, and Live whether the lease it renewed has expired.
	Seq  int64         `json:"seq"`
	Seen time.Time     `json:"seen,omitzero"`
	Age  time.Duration `json:"age,omitempty"`
	Live bool          `json:"live"`
}

// SecretInstall is one cluster's secret generation on the control plane (Want) and on a proxy (Have,
// empty when it reported none).
type SecretInstall struct {
	Cluster string `json:"cluster"`
	Want    string `json:"want"`
	Have    string `json:"have"`
	Current bool   `json:"current"`
}

func (s *Server) proxyDiagnostics(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ms, err := s.members(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	i := slices.IndexFunc(ms, func(m Member) bool { return m.ID == id })
	if i < 0 {
		writeError(w, http.StatusNotFound, "not_found", "no proxy "+id)
		return
	}
	snap := s.Dir.Snapshot()
	f := snap.File()
	d := ProxyDiagnostics{Member: ms[i], Directory: snap.Version(), Current: f.Identity, Secrets: []SecretInstall{}, Problems: []string{}}
	m := &d.Member
	d.Lease = LeaseStatus{Granted: s.LeaseTTL, Seq: m.Seq, Seen: m.Seen, Age: m.SinceSeen, Live: m.Live}
	d.Lineage = m.Identity == f.Identity
	d.Backpressure = m.Installed > m.Applied
	if d.Lineage && m.Applied < d.Directory {
		d.Behind = d.Directory - m.Applied
	}
	for _, name := range slices.Sorted(maps.Keys(f.Clusters)) {
		g := f.Generation(directory.SecretResource(name))
		if g == 0 {
			continue
		}
		have, _ := strconv.ParseInt(m.Secrets[name], 10, 64) //nolint:errcheck // absent or malformed is behind
		d.Secrets = append(d.Secrets, SecretInstall{Cluster: name, Want: strconv.FormatInt(g, 10), Have: m.Secrets[name], Current: d.Lineage && have >= g})
	}
	for _, inc := range m.Unresolved {
		d.Problems = append(d.Problems, fmt.Sprintf("incarnation %s did not retire cleanly (%d outcomes unknown; ended %s): every barrier waits on it until `shunt proxy resolve %s --incarnation %s --attest <why>`", inc.ID, inc.Uncertain, inc.Ended.Format(time.RFC3339), m.ID, inc.ID))
	}
	switch {
	case m.Incarnation != nil && m.Incarnation.State == IncarnationRetired:
		d.Problems = append(d.Problems, fmt.Sprintf("retired: its last process stopped cleanly at %s; it re-joins when a new one starts, or `shunt proxy forget %s` removes it", m.Incarnation.Ended.Format(time.RFC3339), m.ID))
	case !m.Live:
		d.Problems = append(d.Problems, fmt.Sprintf("silent: no heartbeat within its lease (last seen %s ago)", m.SinceSeen.Round(time.Second)))
	case !d.Lineage:
		d.Problems = append(d.Problems, "on another lineage: it installed a directory from another cluster or epoch and must be re-enrolled with an empty control.cache_dir")
	case d.Behind > 0:
		d.Problems = append(d.Problems, fmt.Sprintf("behind: its requests use version %d, the directory is at %d", m.Applied, d.Directory))
	}
	if d.Backpressure {
		d.Problems = append(d.Problems, fmt.Sprintf("install backpressure: version %d is installed but waits for long-running requests to release one of the retired runtime bundles; new requests still use %d", m.Installed, m.Applied))
	}
	if m.CacheError != "" {
		d.Problems = append(d.Problems, "restart cache not durable: "+m.CacheError)
	} else if m.Durable < m.Applied {
		d.Problems = append(d.Problems, fmt.Sprintf("restart cache at version %d, behind the %d it serves", m.Durable, m.Applied))
	}
	if m.Uncertain > 0 {
		d.Problems = append(d.Problems, fmt.Sprintf("%d backend outcomes unknown in this process: a barrier on the buckets they touched cannot drain; retire the proxy and resolve the incarnation once its backend work has ended", m.Uncertain))
	}
	if m.RetireRequested {
		d.Problems = append(d.Problems, "retirement requested: the proxy drains and stops at its next heartbeat")
	}
	for _, sec := range d.Secrets {
		if !sec.Current {
			d.Problems = append(d.Problems, fmt.Sprintf("cluster %s: signs with secret generation %q, the control plane holds %s", sec.Cluster, sec.Have, sec.Want))
		}
	}
	writeJSON(w, http.StatusOK, d)
}

// forgetProxy removes a member: the operator's statement that the proxy is gone for good.
func (s *Server) forgetProxy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.fleet().Forget(r.Context(), id); err != nil {
		fail(w, err)
		return
	}
	s.info(actor(r), "proxy forgotten", "proxy", id)
	writeJSON(w, http.StatusOK, map[string]string{"forgotten": id})
}

// fenceRound waits until every member it must wait for has installed version v, or until wait
// runs out, and returns the ids it is still waiting on. strict waits for every member, live or
// not: a round that takes a bucket out of ACTIVE, where a silent member may still be writing every
// key to the source. Otherwise only live members count: a member that fell silent after the
// bucket left ACTIVE stops routing its writes when its lease lapses. The ids it waits on are
// reported on the operation's record as they change.
func (s *Server) fenceRound(tr *tracker, v int64, strict bool, wait time.Duration) ([]string, error) {
	start := s.now()
	for {
		ms, err := s.members(tr.ctx)
		if err != nil {
			return nil, err
		}
		cur := s.Dir.Snapshot().File().Identity
		var waiting []string
		for _, m := range ms {
			if (strict || m.Live) && !m.Has(cur, v) {
				waiting = append(waiting, m.ID)
			}
		}
		tr.waiting(waiting)
		if len(waiting) == 0 {
			if s.Metrics != nil && len(ms) > 0 {
				s.Metrics.FenceWait.Observe(s.now().Sub(start).Seconds())
			}
			return nil, nil
		}
		if s.now().Sub(start) >= wait {
			return waiting, nil
		}
		if err := s.sleep(tr.ctx, s.fencePoll()); err != nil {
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
func (s *Server) counted(ctx context.Context, strict bool) (bool, error) {
	ms, err := s.members(ctx)
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

// fleetFallbackReads sums the fallback-read counter for key over this node and every live member,
// as of each one's last heartbeat, and returns each live member's heartbeat sequence so a caller
// can tell which have reported since.
func (s *Server) fleetFallbackReads(ctx context.Context, key string) (total float64, seqs map[string]int64, err error) {
	if s.Metrics != nil {
		total = counters(s.Metrics.FallbackReads, key, "")[""]
	}
	ms, err := s.members(ctx)
	if err != nil {
		return 0, nil, err
	}
	seqs = map[string]int64{}
	for _, m := range ms {
		if !m.Live {
			continue
		}
		total += m.FallbackReads[key]
		seqs[m.ID] = m.Seq
	}
	return total, seqs, nil
}

// awaitReports waits until every member in since has sent two more heartbeats: the second was put
// together after the member had the answer to the first, so its counters are from after the call.
func (s *Server) awaitReports(ctx context.Context, since map[string]int64, wait time.Duration) ([]string, error) {
	start := s.now()
	for {
		ms, err := s.members(ctx)
		if err != nil {
			return nil, err
		}
		seq := map[string]int64{}
		for _, m := range ms {
			if m.Live {
				seq[m.ID] = m.Seq
			}
		}
		var missing []string
		for id, n := range since {
			if cur, ok := seq[id]; !ok || cur < n+2 {
				missing = append(missing, id)
			}
		}
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

// Keys is the control plane's view of client keys: the credentials clients sign with, imported
// with `adopt --keys` and `client add` (ADR-0012). Two implementations: auth.Static over the
// credentials file of a lab proxy, and the control plane's store (internal/cp), which delivers
// them to every member proxy.
type Keys interface {
	// Tenant returns a tenant's keys, secrets included, for step-out's direct checks.
	Tenant(tenant string) []sigv4.Credential
	// All returns every key, secrets included: what a member proxy verifies clients with.
	All() []sigv4.Credential
	Add(c sigv4.Credential) error
	Remove(accessKey string) error
}

// Directory is the answer to GET /v1/directory: everything a member proxy routes and verifies
// with, at one version (ADR-0015). Secrets are included: a proxy signs upstream with the cluster
// secrets and verifies clients with the client secrets. Until TLS lands (ADR-0015, deferred) the
// control channel carries them in the clear, which is why a member's config must say
// `plaintext: true`, as a listener must.
type Directory struct {
	directory.File
	Credentials []Credential `json:"credentials"`
	// Secrets resolves the secret_refs only the control plane can: control:<cluster>.
	Secrets map[string]string `json:"secrets,omitempty"`
}

// Credential is one client key as GET /v1/directory delivers it.
type Credential struct {
	AccessKey string   `json:"access_key"`
	Secret    string   `json:"secret"`
	Tenant    string   `json:"tenant"`
	Buckets   []string `json:"buckets,omitempty"`
}

// directoryHandler is GET /v1/directory?cluster_id=<id>&epoch=<epoch>&since=<version>&wait=<duration>:
// the whole directory once it is newer than since, or 304 when wait runs out first. A member proxy
// long-polls it. 304 answers only a caller on this lineage (ADR-0021): a caller on another cluster
// or epoch, or ahead of this node in the same epoch, gets 409 and the current identity, and a
// caller with a version must name its lineage.
func (s *Server) directoryHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var since int64
	if v := q.Get("since"); v != "" {
		if _, err := fmt.Sscan(v, &since); err != nil || since < 0 {
			writeError(w, http.StatusBadRequest, "bad_request", "since: want a directory version")
			return
		}
	}
	have := directory.Identity{ClusterID: q.Get("cluster_id"), Epoch: q.Get("epoch")}
	if !have.IsZero() {
		if err := have.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
	} else if since > 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "since needs cluster_id and epoch: a version means nothing outside its lineage")
		return
	}
	wait, err := parseWait(q.Get("wait"))
	if err != nil {
		fail(w, err)
		return
	}
	if q.Get("wait") == "" {
		wait = 0
	}
	start := s.now()
	for {
		snap := s.Dir.Snapshot()
		if err := checkLineage(snap, have, since); err != nil {
			fail(w, err)
			return
		}
		if snap.Version() > since {
			payload := Directory{File: *snap.File(), Credentials: []Credential{}}
			if s.Keys != nil {
				for _, c := range s.Keys.All() {
					payload.Credentials = append(payload.Credentials, Credential{AccessKey: c.AccessKey, Secret: c.Secret, Tenant: c.Tenant, Buckets: c.Buckets})
				}
			}
			if s.ClusterSecrets != nil {
				payload.Secrets = s.ClusterSecrets()
			}
			writeJSON(w, http.StatusOK, payload)
			return
		}
		if s.now().Sub(start) >= wait {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if err := s.sleep(r.Context(), s.fencePoll()); err != nil {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
}
