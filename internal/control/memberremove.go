package control

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"
)

// Removing a control-plane member by its ID (ADR-0021 D4, H3c). A removal is an operation like a
// join: it reserves the control plane's membership, so no join or other removal runs beside it,
// and it names the member by etcd's member ID, which a member that never started (a canceled or
// failed join's learner) has and its name does not, and which a node that rejoins under the same
// name does not reuse. A name is only a way to find an ID, once. The dry run answers the member,
// what its removal leaves, and a token bound to the member list it saw: a removal confirmed for
// one membership cannot run against another. The record says phase member_remove before the
// removal is sent, and a removal whose answer was lost is reconciled by ID: a member no longer in
// the list is removed.

// OpControlRemove is the kind of a member removal's record.
const OpControlRemove = "control-remove"

// PhaseMemberRemove says the removal is being sent: an unanswered one is reconciled by ID, never
// repeated against another member.
const PhaseMemberRemove = "member_remove"

// ErrQuorumAtRisk is etcd's refusal of a removal the members left could not keep quorum after:
// too many of them are down. Nothing was removed.
var ErrQuorumAtRisk = errors.New("the members left could not keep quorum")

// memberRemoveScope is the confirmation scope of a member removal's token.
const memberRemoveScope = "member remove"

// MemberRemoveRequest is a removal's intent: the member's ID, as etcd prints it (hex), and the
// token of the dry run that confirmed it.
type MemberRemoveRequest struct {
	ID    string `json:"id"`
	Token string `json:"token,omitempty"`
}

// MemberView is one control-plane member as a removal names it.
type MemberView struct {
	ID       string   `json:"id"`
	Name     string   `json:"name,omitempty"` // empty until it has started
	PeerURLs []string `json:"peer_urls"`
	Role     string   `json:"role"` // voter or learner
	Started  bool     `json:"started"`
}

// MemberRemoveDryRun is the dry run of a removal: the member, what the removal leaves, and the
// token the removal must present.
type MemberRemoveDryRun struct {
	Allowed     bool       `json:"allowed"`
	Reason      string     `json:"reason,omitempty"` // the refusal the removal would give
	Member      MemberView `json:"member"`
	VotersAfter int        `json:"voters_after"`
	Warning     string     `json:"warning,omitempty"`
	Token       string     `json:"token,omitempty"`
	ExpiresAt   time.Time  `json:"expires_at,omitzero"`
}

// MemberRemoveResult is what a removal did.
type MemberRemoveResult struct {
	MemberID  string `json:"member_id"`
	Name      string `json:"name,omitempty"`
	Voters    int    `json:"voters"`
	Warning   string `json:"warning,omitempty"`
	Operation string `json:"operation,omitempty"`
}

// parseMemberID reads a member ID as etcd prints it.
func parseMemberID(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 16, 64)
	if err != nil || id == 0 {
		return 0, bad("member id %q: want the hexadecimal id `shunt-control member list` shows", s)
	}
	return id, nil
}

func controlMember(m EtcdMember) MemberView {
	role := "voter"
	if m.Learner {
		role = "learner"
	}
	return MemberView{ID: memberHex(m.ID), Name: m.Name, PeerURLs: slices.Clone(m.PeerURLs), Role: role, Started: m.Name != ""}
}

// memberBinding is what a removal's token is bound to: the member to remove and the whole member
// list as the dry run saw it. Any membership change since (a member added, removed, promoted, or
// started under a name) makes the token stale.
func memberBinding(ms []EtcdMember, id uint64) [32]byte {
	list := make([]MemberView, 0, len(ms))
	for _, m := range ms {
		list = append(list, controlMember(m))
	}
	slices.SortFunc(list, func(a, b MemberView) int { return cmp.Compare(a.ID, b.ID) })
	return digestOf(memberHex(id), list)
}

// memberRemoveChecks is every refusal a removal gives before its token is looked at, and what it
// would leave: the voters after it, and a warning when that is two or one.
func (s *Server) memberRemoveChecks(ms []EtcdMember, id uint64) (m EtcdMember, votersAfter int, warning string, err error) {
	i := slices.IndexFunc(ms, func(x EtcdMember) bool { return x.ID == id })
	if i < 0 {
		return EtcdMember{}, 0, "", notFound("no control-plane member %s", memberHex(id))
	}
	m = ms[i]
	voters := 0
	for _, x := range ms {
		if !x.Learner {
			voters++
		}
	}
	votersAfter = voters
	if !m.Learner {
		votersAfter--
	}
	switch {
	case s.Members.Self != nil && s.Members.Self() == id:
		return m, votersAfter, "", refuse("member %s (%s) is the control node answering this request; ask another control node to remove it", memberHex(id), firstNonEmpty(m.Name, "unnamed"))
	case !m.Learner && voters == 1:
		return m, votersAfter, "", refuse("member %s is the only voting member; removing it ends the control plane: stop shunt-control instead", memberHex(id))
	case !m.Learner && votersAfter == 2:
		warning = "two voting members left: writes need both, so losing either stops them; join a third control node"
	case !m.Learner && votersAfter == 1:
		warning = "one voting member left: the control plane has no redundancy; join two more control nodes"
	}
	return m, votersAfter, warning, nil
}

// ServeMemberRemove is DELETE /v1/control/members/by-id/{id}, and DELETE /v1/control/members/{name},
// whose name is resolved to one member's ID and goes on as that ID. With ?dry_run=1 it answers
// MemberRemoveDryRun; otherwise it runs the removal as an operation, with the dry run's token.
func (s *Server) ServeMemberRemove(w http.ResponseWriter, r *http.Request) {
	if s.Members == nil {
		writeError(w, http.StatusNotFound, "not_found", "this server has no control-plane membership")
		return
	}
	idText := r.PathValue("id")
	if name := r.PathValue("name"); idText == "" {
		ms, err := s.Members.List(r.Context())
		if err != nil {
			fail(w, fmt.Errorf("%w: reading the member list: %w", ErrUnavailable, err))
			return
		}
		i := slices.IndexFunc(ms, func(m EtcdMember) bool { return m.Name == name })
		if i < 0 {
			fail(w, notFound("no control-plane member named %s; a member that never started has no name: remove it by id (`shunt-control member list`)", name))
			return
		}
		idText = memberHex(ms[i].ID)
	}
	id, err := parseMemberID(idText)
	if err != nil {
		fail(w, err)
		return
	}
	if r.URL.Query().Get("dry_run") != "" {
		res, err := s.memberRemoveDryRun(r.Context(), id)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
		return
	}
	var req RemoveRequest
	if !decodeOptional(w, r, &req) {
		return
	}
	s.serveOperation(w, r, OperationRequest{Kind: OpControlRemove}, MemberRemoveRequest{ID: memberHex(id), Token: req.Token})
}

func (s *Server) memberRemoveDryRun(ctx context.Context, id uint64) (MemberRemoveDryRun, error) {
	ms, err := s.Members.List(ctx)
	if err != nil {
		return MemberRemoveDryRun{}, fmt.Errorf("%w: reading the member list: %w", ErrUnavailable, err)
	}
	m, votersAfter, warning, err := s.memberRemoveChecks(ms, id)
	res := MemberRemoveDryRun{Member: controlMember(m), VotersAfter: votersAfter, Warning: warning}
	var gone *missing
	switch {
	case errors.As(err, &gone):
		return res, err
	case err != nil:
		res.Reason = err.Error()
		return res, nil
	}
	res.Allowed = true
	res.Token, res.ExpiresAt = s.confirmToken(memberRemoveScope, memberBinding(ms, id))
	return res, nil
}

// runMemberRemove removes the member, from wherever its record stands: before its intent, the
// checks and the token against the member list as it is now; after it, a removal whose answer was
// lost is reconciled by ID, and one still pending is sent again for the same ID.
func (s *Server) runMemberRemove(tr *tracker, a MemberRemoveRequest) (MemberRemoveResult, error) {
	id, err := parseMemberID(a.ID)
	if err != nil {
		return MemberRemoveResult{}, err
	}
	res := MemberRemoveResult{MemberID: memberHex(id), Operation: tr.id()}
	for {
		if err := tr.check(); err != nil {
			return MemberRemoveResult{}, err
		}
		ms, err := s.Members.List(tr.ctx)
		if err != nil {
			tr.blocked([]Blocker{{Code: BlockerQuorumUnavailable, Message: "the member list cannot be read: " + err.Error()}})
			if serr := s.sleep(tr.ctx, s.joinPoll()); serr != nil {
				return MemberRemoveResult{}, fmt.Errorf("%w: %w", ErrUnavailable, serr)
			}
			continue
		}
		op := tr.snapshot()
		i := slices.IndexFunc(ms, func(m EtcdMember) bool { return m.ID == id })
		present := i >= 0
		if present {
			res.Name = ms[i].Name
		}
		if !present && op.Phase == PhaseMemberRemove {
			// Removed, by this operation's own request whose answer was lost, or by its first owner.
			res.Voters = voterCount(ms)
			tr.blocked(nil)
			return res, nil
		}
		if op.Phase != PhaseMemberRemove {
			m, _, warning, err := s.memberRemoveChecks(ms, id)
			if err != nil {
				return MemberRemoveResult{}, err
			}
			if err := s.checkToken(a.Token, memberRemoveScope, memberBinding(ms, id)); err != nil {
				return MemberRemoveResult{}, err
			}
			res.Name, res.Warning = m.Name, warning
			tr.setMember(&JoinMember{ID: memberHex(id), PeerURL: firstOf(m.PeerURLs)})
			tr.phase(PhaseMemberRemove)
			if err := tr.check(); err != nil {
				return MemberRemoveResult{}, err // another node took the record: it sends the removal
			}
		}
		rerr := s.Members.Remove(tr.ctx, id)
		switch {
		case rerr == nil:
			s.info(tr.actor, "control-plane member removed", "operation", op.ID, "member", memberHex(id), "name", res.Name)
			left := slices.DeleteFunc(slices.Clone(ms), func(m EtcdMember) bool { return m.ID == id })
			res.Voters = voterCount(left)
			tr.blocked(nil)
			return res, nil
		case errors.Is(rerr, ErrQuorumAtRisk):
			// etcd refused before it changed anything: nothing to reconcile.
			return MemberRemoveResult{}, refuse("etcd refuses to remove member %s: %s; bring the stopped members back first (`shunt-control status`)", memberHex(id), rerr.Error())
		}
		tr.blocked([]Blocker{{Code: BlockerMembershipUnknown, Message: fmt.Sprintf("removing member %s did not answer: %s", memberHex(id), rerr.Error())}})
		if serr := s.sleep(tr.ctx, s.joinPoll()); serr != nil {
			return MemberRemoveResult{}, fmt.Errorf("%w: %w", ErrUnavailable, serr)
		}
	}
}

func voterCount(ms []EtcdMember) int {
	n := 0
	for _, m := range ms {
		if !m.Learner {
			n++
		}
	}
	return n
}

func firstOf(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// ActiveMembershipOperation is the id of the unfinished operation that holds the control plane's
// membership (a join or a removal), or "" when none does or the records cannot be read.
func (s *Server) ActiveMembershipOperation(ctx context.Context) string {
	ops, err := s.ops().Evidence(ctx)
	if err != nil {
		return ""
	}
	if op := membershipChange(ops); op != nil {
		return op.ID
	}
	return ""
}

// membershipChange is the unfinished record holding the control plane's membership, or nil: the
// reservation lets at most one run.
func membershipChange(ops []*Operation) *Operation {
	for _, op := range ops {
		if !op.Terminal() && op.Scope != nil && op.Scope.Resource == MembersResource {
			return op
		}
	}
	return nil
}

// MembershipPhases are the values of shunt_control_join_phase's phase label: every phase a join's
// or a removal's record passes, and PhaseOther for one not listed, so the label set stays bounded.
var MembershipPhases = []string{PhaseQueued, PhaseLearnerAdd, PhaseLearnerAdded, PhaseCatchingUp, PhaseLearnerRemove, PhaseMemberRemove, PhaseOther}

// PhaseOther is shunt_control_join_phase's label for a membership record in a phase not listed.
const PhaseOther = "other"

// publishMembershipPhase sets shunt_control_join_phase: 1 for the phase of the unfinished
// membership change, 0 for every other phase, all 0 when none runs.
func (s *Server) publishMembershipPhase(ops []*Operation) {
	if s.Metrics == nil {
		return
	}
	phase := ""
	if op := membershipChange(ops); op != nil {
		phase = PhaseOther
		if slices.Contains(MembershipPhases, op.Phase) {
			phase = op.Phase
		}
	}
	for _, p := range MembershipPhases {
		v := 0.0
		if p == phase {
			v = 1
		}
		s.Metrics.JoinPhase.WithLabelValues(p).Set(v)
	}
}
