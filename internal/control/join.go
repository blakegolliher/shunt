package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
)

// Joining a control node (ADR-0021 D4, H3a). A join is an operation record like any other change:
// it reserves the control plane's membership, so at most one membership change is unfinished, and
// its intent (name, peer URL, advertised API URL) is on the record before the cluster is asked to
// add the learner. The record's phase says learner_add before the add is sent; the learner's
// member ID is on the record before anything else is done with it. An add whose answer is lost is
// reconciled by the member list, by normalized peer URL: etcd refuses a second member with the
// same peer URL, so a retried add can never make two. The joining node then fetches its bootstrap
// (the member list it starts with and the data-encryption key) from an authenticated, no-store
// route; the key is never on the record, in an event, or in a URL. The operation follows the node
// until etcd has promoted it to a voter, and carries on, from any control node, if its owner is
// lost.

// OpControlJoin is the kind of a join's record.
const OpControlJoin = "control-join"

// MembersResource is the scope a membership change reserves.
const MembersResource = "control:members"

// Join phases, after the queued phase every operation has.
const (
	PhaseLearnerAdd   = "learner_add"   // the learner is being added: an unanswered add is reconciled, never repeated blind
	PhaseLearnerAdded = "learner_added" // the learner is in the member list, its ID on the record; the node has not started
	PhaseCatchingUp   = "catching_up"   // the node runs as a learner, catching up with the leader
)

// Join blocker codes.
const (
	BlockerMemberNotStarted  = "member_not_started"  // the joining node has not fetched its bootstrap and started
	BlockerLearnerCatchingUp = "learner_catching_up" // the node runs, but is not yet promoted to a voter
)

// EtcdMember is one control-plane member as the join sees it.
type EtcdMember struct {
	ID       uint64
	Name     string // empty until the member has started
	PeerURLs []string
	Learner  bool
}

// Membership is the control plane's own etcd membership, which a join changes. shunt-control fills
// it from its etcd member; a lab proxy has none, and its join routes answer 404.
type Membership struct {
	// List is the member list, read linearizably: a membership change needs quorum.
	List func(ctx context.Context) ([]EtcdMember, error)
	// AddLearner adds a non-voting member at peerURL and returns its ID.
	AddLearner func(ctx context.Context, peerURL string) (uint64, error)
	// Key is the data-encryption key a joining node needs; it leaves only through the bootstrap.
	Key func() []byte
}

// JoinRequest is POST /v1/control/members: a node asking to join. APIURL is the control API the
// node will serve, which the other nodes' health sampler probes (D4).
type JoinRequest struct {
	Name    string `json:"name"`
	PeerURL string `json:"peer_url"`
	APIURL  string `json:"api_url,omitempty"`
}

// JoinMember is the member a join added: on its record before anything depends on it.
type JoinMember struct {
	ID      string `json:"id"` // hex, as etcd prints it
	PeerURL string `json:"peer_url"`
}

// JoinResult is a join's result once the node votes.
type JoinResult struct {
	MemberID string `json:"member_id"`
	Name     string `json:"name"`
	Voters   int    `json:"voters"`
	Warning  string `json:"warning,omitempty"`
}

// BootstrapRequest is POST /v1/control/joins/{id}/bootstrap: the joining node names the peer URL it
// asked to join with, which must be its record's.
type BootstrapRequest struct {
	PeerURL string `json:"peer_url"`
}

// Bootstrap is what a joining node starts etcd with. It carries the data-encryption key, so it is
// answered with Cache-Control: no-store and never logged, recorded or published.
type Bootstrap struct {
	Operation      string `json:"operation"`
	MemberID       string `json:"member_id"`
	Name           string `json:"name"`
	InitialCluster string `json:"initial_cluster"`
	EncryptionKey  []byte `json:"encryption_key"`
}

// NormalizePeerURL is the one spelling of a peer URL a join compares and adds: an http or https
// scheme and host in lower case, an explicit port, nothing after it. etcd compares peer URLs as
// strings, so two spellings of one address would otherwise pass as two members.
func NormalizePeerURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("peer URL %q: %w", raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch {
	case scheme != "http" && scheme != "https":
		return "", fmt.Errorf("peer URL %q: want http://host:port", raw)
	case u.Port() == "":
		return "", fmt.Errorf("peer URL %q: give the port, http://host:2380", raw)
	case u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "":
		return "", fmt.Errorf("peer URL %q: want only scheme, host and port", raw)
	}
	return scheme + "://" + strings.ToLower(u.Host), nil
}

// normalizeAPIURL checks an advertised control API URL, as NormalizePeerURL does.
func normalizeAPIURL(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	n, err := NormalizePeerURL(raw)
	if err != nil {
		return "", fmt.Errorf("api_url: %s", strings.TrimPrefix(err.Error(), "peer URL "))
	}
	return n, nil
}

// hasPeer reports whether m has the normalized peer URL.
func (m EtcdMember) hasPeer(peer string) bool {
	return slices.ContainsFunc(m.PeerURLs, func(u string) bool {
		n, err := NormalizePeerURL(u)
		return err == nil && n == peer
	})
}

func memberHex(id uint64) string { return strconv.FormatUint(id, 16) }

// checkJoin validates a join request's fields and normalizes its URLs.
func checkJoin(a *JoinRequest) error {
	if !config.ValidProxyID(a.Name) {
		return bad("member name %q: want 1-64 letters, digits, '.', '_' or '-'", a.Name)
	}
	peer, err := NormalizePeerURL(a.PeerURL)
	if err != nil {
		return bad("%v", err)
	}
	api, err := normalizeAPIURL(a.APIURL)
	if err != nil {
		return bad("%v", err)
	}
	a.PeerURL, a.APIURL = peer, api
	return nil
}

// membersOp is a record reserving the control plane's membership.
func (s *Server) membersOp() Operation {
	return Operation{Scope: &Scope{Resource: MembersResource}}
}

// ServeJoin is POST /v1/control/members. It writes the join's record and answers once the learner
// is added and its ID is on the record (202, with the record), or the join has failed: the node
// fetches its bootstrap next, and the operation follows it until it votes. A retry with the same
// Idempotency-Key answers the same record.
func (s *Server) ServeJoin(w http.ResponseWriter, r *http.Request) {
	if s.Members == nil {
		writeError(w, http.StatusNotFound, "not_found", "this server has no control-plane membership to join")
		return
	}
	meta, err := readMeta(w, r)
	if err != nil {
		fail(w, err)
		return
	}
	var a JoinRequest
	if !decode(w, r, &a) {
		return
	}
	if err = checkJoin(&a); err != nil {
		fail(w, err)
		return
	}
	raw, err := json.Marshal(a)
	if err != nil {
		fail(w, err)
		return
	}
	tr, _, err := s.launch(actor(r), OperationRequest{Kind: OpControlJoin, Args: raw}, true, meta)
	var id string
	var rp *IdempotentReplay
	switch {
	case errors.As(err, &rp):
		id = rp.Existing.ID
	case err != nil:
		fail(w, err)
		return
	default:
		id = tr.id()
	}
	// The node needs the learner's ID before it can fetch its bootstrap: wait for it, bounded.
	deadline := s.now().Add(45 * time.Second)
	for {
		op, gerr := s.ops().Get(r.Context(), id)
		if gerr != nil {
			fail(w, gerr)
			return
		}
		if op != nil && (op.Member != nil || op.Terminal() || !s.now().Before(deadline)) {
			w.Header().Set("Location", "/v1/operations/"+op.ID)
			writeJSON(w, http.StatusAccepted, op.Public())
			return
		}
		if serr := s.sleep(r.Context(), s.fencePoll()); serr != nil {
			return // the client left; the record has the outcome
		}
	}
}

// ServeJoinBootstrap is POST /v1/control/joins/{id}/bootstrap: what the joining node starts with.
// It answers only for a join whose learner is on its record, and only to the peer URL the join
// named; the same join answers the same way every time, so a node that crashed before saving it
// fetches it again.
func (s *Server) ServeJoinBootstrap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.Members == nil {
		writeError(w, http.StatusNotFound, "not_found", "this server has no control-plane membership to join")
		return
	}
	var req BootstrapRequest
	if !decode(w, r, &req) {
		return
	}
	peer, err := NormalizePeerURL(req.PeerURL)
	if err != nil {
		fail(w, bad("%v", err))
		return
	}
	op, err := s.ops().Get(r.Context(), r.PathValue("id"))
	switch {
	case err != nil:
		fail(w, err)
		return
	case op == nil || op.Kind != OpControlJoin:
		fail(w, notFound("no join %s", r.PathValue("id")))
		return
	}
	var a JoinRequest
	if err = decodeArgs(op.Args, &a); err != nil {
		fail(w, err)
		return
	}
	switch {
	case a.PeerURL != peer:
		fail(w, refuse("join %s is for peer URL %s, not %s", op.ID, a.PeerURL, peer))
		return
	case op.Member == nil && op.Terminal():
		fail(w, refuse("join %s ended %s before its learner was added: start a new join", op.ID, op.Status))
		return
	case op.Member == nil:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "not_ready", fmt.Sprintf("join %s has not added its learner yet (phase %s); retry", op.ID, op.Phase))
		return
	}
	ms, err := s.Members.List(r.Context())
	if err != nil {
		fail(w, fmt.Errorf("%w: reading the member list: %w", ErrUnavailable, err))
		return
	}
	initial, err := initialCluster(ms, op.Member.ID, a.Name)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, Bootstrap{Operation: op.ID, MemberID: op.Member.ID, Name: a.Name, InitialCluster: initial, EncryptionKey: s.Members.Key()})
}

// initialCluster is the "name=peer-url,..." a joining member starts etcd with: every member,
// the joining one under the name it will start with. Any other member that has not started has no
// name for the list, so the join waits for it.
func initialCluster(ms []EtcdMember, joinID, name string) (string, error) {
	var parts []string
	found := false
	for _, m := range ms {
		mn := m.Name
		if memberHex(m.ID) == joinID {
			mn, found = name, true
		}
		if mn == "" {
			return "", fmt.Errorf("%w: member %s has not started yet; the join's bootstrap waits for it", ErrUnavailable, memberHex(m.ID))
		}
		for _, u := range m.PeerURLs {
			parts = append(parts, mn+"="+u)
		}
	}
	if !found {
		return "", refuse("the learner %s is no longer a member (removed?); start a new join", joinID)
	}
	return strings.Join(parts, ","), nil
}

// runJoin adds the learner and follows the node until it votes. It runs from wherever the record
// stands, so a resumed join carries on: a record in learner_add without a member reconciles the
// member list before it adds anything.
func (s *Server) runJoin(tr *tracker, a JoinRequest) (any, error) {
	for {
		if err := tr.check(); err != nil {
			return nil, err
		}
		op := tr.snapshot()
		ms, err := s.Members.List(tr.ctx)
		if err != nil {
			tr.blocked([]Blocker{{Code: BlockerQuorumUnavailable, Message: "the member list cannot be read: " + err.Error()}})
			if serr := s.sleep(tr.ctx, s.joinPoll()); serr != nil {
				return nil, fmt.Errorf("%w: %w", ErrUnavailable, serr)
			}
			continue
		}
		if op.Member == nil {
			if err := s.addLearner(tr, op, a, ms); err != nil {
				return nil, err
			}
			continue
		}
		i := slices.IndexFunc(ms, func(m EtcdMember) bool { return memberHex(m.ID) == op.Member.ID })
		if i < 0 {
			return nil, refuse("the learner %s this join added is no longer a member (removed?); start a new join", op.Member.ID)
		}
		m := ms[i]
		var blockers []Blocker
		switch {
		case m.Name == "":
			tr.phase(PhaseLearnerAdded)
			blockers = []Blocker{{Code: BlockerMemberNotStarted, Message: fmt.Sprintf("waiting for %s to fetch its bootstrap and start (`shunt-control join` on that host)", a.Name)}}
		case m.Learner:
			tr.phase(PhaseCatchingUp)
			blockers = []Blocker{{Code: BlockerLearnerCatchingUp, Message: fmt.Sprintf("%s runs as a learner and promotes itself once it has caught up with the leader", a.Name)}}
		default:
			voters := 0
			for _, x := range ms {
				if !x.Learner {
					voters++
				}
			}
			res := JoinResult{MemberID: op.Member.ID, Name: m.Name, Voters: voters}
			if voters == 2 {
				res.Warning = "two voting members: writes need both, so losing either stops them; join a third control node"
			}
			tr.blocked(nil)
			return res, nil
		}
		tr.blocked(blockers)
		if serr := s.sleep(tr.ctx, s.joinPoll()); serr != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, serr)
		}
	}
}

// addLearner puts the learner on the record: found in the member list after an add whose answer
// was lost, or added now, with the phase durable before the add is sent.
func (s *Server) addLearner(tr *tracker, op Operation, a JoinRequest, ms []EtcdMember) error {
	var mine *EtcdMember
	for i := range ms {
		if ms[i].hasPeer(a.PeerURL) {
			mine = &ms[i]
		}
	}
	if mine != nil {
		switch {
		case op.Phase != PhaseLearnerAdd:
			// Not this join's: its add was never sent.
			if mine.Name == "" {
				return refuse("peer URL %s already belongs to member %s, which has not started (an earlier join?); remove it with `shunt-control member remove`, or use another peer URL", a.PeerURL, memberHex(mine.ID))
			}
			return refuse("peer URL %s already belongs to member %s (%s)", a.PeerURL, mine.Name, memberHex(mine.ID))
		case mine.Name != "" && mine.Name != a.Name:
			return refuse("peer URL %s belongs to member %s, not %s", a.PeerURL, mine.Name, a.Name)
		}
		tr.setMember(&JoinMember{ID: memberHex(mine.ID), PeerURL: a.PeerURL})
		if s.Log != nil {
			s.Log.Info("join reconciled a learner already in the member list", "operation", op.ID, "member", memberHex(mine.ID), "peer", a.PeerURL)
		}
		return nil
	}
	for _, m := range ms {
		switch {
		case m.Name == a.Name:
			return refuse("a member named %s already exists (%s, peer %s); remove it first, or choose another name", a.Name, memberHex(m.ID), strings.Join(m.PeerURLs, ","))
		case m.Learner:
			return refuse("member %s is a learner not yet promoted; etcd admits one learner at a time, so wait for it, or remove it", firstNonEmpty(m.Name, memberHex(m.ID)))
		}
	}
	tr.phase(PhaseLearnerAdd)
	if err := tr.check(); err != nil {
		return err // the phase write was refused: another node owns the record now
	}
	id, err := s.Members.AddLearner(tr.ctx, a.PeerURL)
	if err != nil {
		// The add may have landed with its answer lost: the next round lists the members and
		// reconciles, and etcd refuses a second member with this peer URL either way.
		if s.Log != nil {
			s.Log.Warn("join's learner add did not answer; reconciling from the member list", "operation", op.ID, "peer", a.PeerURL, "err", err.Error())
		}
		tr.blocked([]Blocker{{Code: BlockerQuorumUnavailable, Message: "adding the learner did not answer: " + err.Error()}})
		if serr := s.sleep(tr.ctx, s.joinPoll()); serr != nil {
			return fmt.Errorf("%w: %w", ErrUnavailable, serr)
		}
		return nil
	}
	tr.setMember(&JoinMember{ID: memberHex(id), PeerURL: a.PeerURL})
	tr.phase(PhaseLearnerAdded)
	s.info(tr.actor, "control node learner added", "operation", op.ID, "name", a.Name, "member", memberHex(id), "peer", a.PeerURL)
	return nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// joinPoll is how often a join reads the member list while it waits.
func (s *Server) joinPoll() time.Duration {
	if s.FencePoll > 0 {
		return s.FencePoll
	}
	return 500 * time.Millisecond
}
