package control

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

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

// checkLineage compares a caller's directory (have, at version v) with the installed one. A caller
// with no identity has nothing installed yet. Versions compare only within one identity.
func checkLineage(snap *directory.Snapshot, have directory.Identity, v int64) error {
	cur := snap.File().Identity
	switch {
	case have.IsZero():
		return nil
	case have.ClusterID != cur.ClusterID:
		return &lineageError{code: CodeClusterMismatch, cur: cur, msg: fmt.Sprintf("this control plane serves cluster %s; the caller's directory is from cluster %s", cur.ClusterID, have.ClusterID)}
	case have.Epoch != cur.Epoch:
		return &lineageError{code: CodeEpochMismatch, cur: cur, msg: fmt.Sprintf("the directory is in epoch %s; the caller's is from epoch %s, another recovery lineage: its versions compare with nothing here", cur.Epoch, have.Epoch)}
	case v > snap.Version():
		return &lineageError{code: CodeResyncRequired, cur: cur, msg: fmt.Sprintf("the caller claims directory version %d; this control node has %d in the same epoch", v, snap.Version())}
	}
	return nil
}

// Heartbeat is a member's report to the control plane.
type Heartbeat struct {
	// Protocol is the fleet protocol the member speaks; it must be Protocol.
	Protocol int `json:"protocol"`
	// Identity is the lineage of the directory the member has installed (Applied); zero before
	// it has installed one.
	Identity directory.Identity `json:"identity,omitzero"`
	Started  time.Time          `json:"started"`
	// Seq counts this member's heartbeats since it started, so a caller can tell which reports
	// were assembled after a point in time.
	Seq int64 `json:"seq"`
	// Applied is the directory version the member has installed.
	Applied int64 `json:"applied"`
	// FallbackReads is shunt_migration_fallback_reads_total for each bucket that is not ACTIVE in
	// the member's snapshot: bounded by migrations in flight, never by buckets.
	FallbackReads map[string]float64 `json:"fallback_reads,omitempty"`
	// Host and Version say where the member runs and which build it is, for the fleet view.
	Host    string `json:"host,omitempty"`
	Version string `json:"version,omitempty"`
	// Telemetry is the member's last completed 10-second window. It is re-sent until the next
	// window closes, and the control node de-duplicates it by proxy and start time.
	Telemetry *telemetry.Window `json:"telemetry,omitempty"`
}

// HeartbeatAnswer tells a member the current version and its lease. Seq echoes the heartbeat it
// answers: a member takes a lease only from the answer to the heartbeat it sent.
type HeartbeatAnswer struct {
	Seq      int64              `json:"seq"`
	Identity directory.Identity `json:"identity"`
	Version  int64              `json:"version"`
	LeaseTTL time.Duration      `json:"lease_ttl"`
}

// Member is one registered proxy as the control plane sees it.
type Member struct {
	ID string `json:"id"`
	// Live: its lease has not expired. A member stays a member after its lease expires, until it
	// is forgotten; a bucket's first step waits for it either way.
	Live bool `json:"live"`
	// Identity is the lineage of the directory it last reported installed: Applied counts only
	// when it is the control plane's.
	Identity      directory.Identity `json:"identity,omitzero"`
	Applied       int64              `json:"applied"`
	Seq           int64              `json:"seq"`
	Started       time.Time          `json:"started,omitzero"`
	Seen          time.Time          `json:"seen,omitzero"`        // when its last heartbeat arrived
	SinceSeen     time.Duration      `json:"since_seen,omitempty"` // how long ago, by the control plane's clock
	FallbackReads map[string]float64 `json:"fallback_reads,omitempty"`
	Host          string             `json:"host,omitempty"`
	Version       string             `json:"version,omitempty"` // the member's build
	Telemetry     *telemetry.Window  `json:"telemetry,omitempty"`
}

// Has reports whether the member has installed version v of lineage id. A larger version from
// another lineage (a member that outlived a restore) proves nothing about this directory.
func (m Member) Has(id directory.Identity, v int64) bool { return m.Identity == id && m.Applied >= v }

// Fleet is the fleet table. Two implementations: the control plane's, on leased etcd keys
// (internal/cp), and NoFleet for a single-node lab whose proxy has no members.
type Fleet interface {
	// Heartbeat records a member's report and renews its lease; the first from an id makes it a
	// member. It returns the lease TTL a member measures its own staleness by.
	Heartbeat(ctx context.Context, id string, hb Heartbeat) (leaseTTL time.Duration, err error)
	// Members lists every member, live or not.
	Members(ctx context.Context) ([]Member, error)
	// Forget removes a member that is gone for good. Refused while its lease is live.
	Forget(ctx context.Context, id string) error
}

// NoFleet is the fleet of a single-node lab: `shunt serve --plaintext`, whose in-process control
// API has no members. Members join shunt-control, never a proxy.
type NoFleet struct{}

// Heartbeat implements Fleet: refused, naming where members belong.
func (NoFleet) Heartbeat(context.Context, string, Heartbeat) (time.Duration, error) {
	return 0, refuse("this shunt is a single-node lab proxy and takes no members; a fleet's members send their heartbeat to shunt-control (ADR-0015)")
}

// Members implements Fleet: none.
func (NoFleet) Members(context.Context) ([]Member, error) { return nil, nil }

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
	snap := s.Dir.Snapshot()
	if err := checkLineage(snap, hb.Identity, hb.Applied); err != nil {
		// No lease: a member on another lineage must not count as present for a fence.
		fail(w, err)
		return
	}
	ttl, err := s.fleet().Heartbeat(r.Context(), id, hb)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, HeartbeatAnswer{Seq: hb.Seq, Identity: snap.File().Identity, Version: snap.Version(), LeaseTTL: ttl})
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
