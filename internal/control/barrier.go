package control

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/directory"
)

// The drain barrier (ADR-0021 D2, design §4). A change that moves writes, closes them or takes a
// source away is written in two steps. First its hold, with a barrier id, which every proxy
// installs, closing the gate it names; the operation record carries the barrier durably, so any
// control node can carry it on. Then the operation waits, without a deadline, until every proxy
// that could have served the old routing has reported the gate closed and drained: no mutation
// still out through it, no backend outcome unknown, the hold in its restart cache. Only then is
// the change committed, and the record settles once every live proxy has the commit. Whatever
// stands in the way is a named blocker on the record; a timeout means blocked, never success
// and never a rollback. Before the commit an operator may cancel, which puts the routing back as
// it was; after it, the change is in force and cancellation is refused.

// BarrierState is the drain barrier an operation runs, durable on its record.
type BarrierState struct {
	ID    string `json:"id"`
	Scope string `json:"scope"` // directory.PlacementResource or ClusterResource
	Kind  string `json:"kind"`  // config.BarrierMutations or BarrierSource
	// HoldVersion is the directory version that wrote the hold (0 before it is written), and
	// Generation the scope's generation from then on: what every proxy's acknowledgement must name.
	HoldVersion int64 `json:"hold_version,omitempty"`
	Generation  int64 `json:"generation,omitempty"`
	// Committed says the guarded change is written; CommitVersion is its version. Past this,
	// cancellation is refused.
	Committed     bool  `json:"committed,omitempty"`
	CommitVersion int64 `json:"commit_version,omitempty"`
	// DispatchStarted is the irreversible point of a destructive barrier such as purge-source.
	// It is durable before the first backend delete, so cancellation cannot reopen admission while
	// an owner (or a resumed owner) may still be deleting the source.
	DispatchStarted bool `json:"dispatch_started,omitempty"`
	// HeldAt, DrainedAt and CommittedAt are when each phase ended, for the duration metric.
	HeldAt      time.Time `json:"held_at,omitzero"`
	DrainedAt   time.Time `json:"drained_at,omitzero"`
	CommittedAt time.Time `json:"committed_at,omitzero"`
}

// The actions a record accepts (Operation.AllowedActions).
const (
	ActionResume        = "resume"
	ActionCancel        = "cancel"
	ActionResolveWorker = "resolve-worker" // an external mover whose worker session expired
)

// CodeNotCancellable refuses a cancellation past the safe point: the change is committed, or the
// operation has ended.
const CodeNotCancellable = "not_cancellable"

// errCanceled ends an operation whose barrier was released by a cancellation while it ran.
var errCanceled = errors.New("the operation was canceled before its change was committed")

// errLost ends an operation whose record another node took over.
var errLost = errors.New("another control node took this operation over")

// barrier is one drain barrier as an operation runs it.
type barrier struct {
	scope string
	kind  string
	// hold writes the guarded change's hold on the scope, with the operation's id as its barrier,
	// and returns the version written. A repeat, after an interrupted run, finds the hold in
	// place and returns the current version.
	hold func() (int64, error)
	// commit writes the guarded change and returns its version. It finds the scope still holding
	// this barrier, or reports already when an earlier attempt's write landed and its reply was
	// lost, or fails when the barrier is gone (released by a cancellation).
	commit func() (version int64, already bool, err error)
	// extra, if set, adds blockers of the barrier's own once the fleet has drained: cutover's
	// multipart uploads and quiet window, purge-source's re-diff of the drained source. A mover's
	// worker session needs none here: its operation reserves the placement, so no cutover or
	// purge can start while one is unresolved.
	extra func(ctx context.Context) ([]Blocker, error)
}

// runBarrier drives a barrier from wherever its record stands to its commit: the hold, the
// drain, the commit. The settle that follows is the caller's, since it answers with the result.
func (s *Server) runBarrier(tr *tracker, b *barrier) error {
	st := tr.barrierState()
	if st == nil {
		st = &BarrierState{ID: tr.id(), Scope: b.scope, Kind: b.kind}
		// The intent is durable before the directory write, so a new owner can resume it. Cancel
		// is deliberately not offered until HoldVersion is durable too: otherwise cancellation
		// could end the record while this owner was already writing a hold that nobody would own.
		tr.setBarrier(st, nil)
	}
	if st.HoldVersion == 0 {
		tr.phase(PhaseHold)
		if err := tr.check(); err != nil {
			return err
		}
		start := s.now()
		v, err := b.hold()
		if err != nil {
			return err
		}
		st.HoldVersion, st.Generation, st.HeldAt = v, s.Dir.Snapshot().Generation(b.scope), s.now().UTC()
		tr.setBarrier(st, []string{ActionCancel})
		s.observeBarrier(PhaseHold, s.now().Sub(start))
	}
	if !st.Committed {
		// A resumed record may be behind the directory: the owner wrote the commit and was lost
		// before the record said so. The scope then no longer carries the barrier, no proxy can
		// acknowledge it any more, and a drain would wait forever. Only a scope that still carries
		// the barrier is drained; otherwise the commit reconciles (already written, or released).
		carried, cerr := s.carriesBarrier(tr.ctx, b.scope, st.ID)
		if cerr != nil {
			return cerr
		}
		if carried {
			tr.phase(PhaseDrain)
			start := s.now()
			if err := s.blockUntil(tr, func(ctx context.Context) ([]Blocker, error) { return s.drainBlockers(ctx, st, b.extra) }); err != nil {
				return err
			}
			st.DrainedAt = s.now().UTC()
			s.observeBarrier(PhaseDrain, s.now().Sub(start))
		}
		tr.phase(PhaseCommit)
		// phase writes through the record CAS. A concurrent resume can make that write stale;
		// discover the lost owner before touching the directory.
		if err := tr.check(); err != nil {
			return err
		}
		start := s.now()
		var v int64
		var already bool
		var err error
		for {
			v, already, err = b.commit()
			current := tr.barrierState()
			switch {
			case errors.Is(err, errBarrierGone):
				// Released by a cancellation between the last check and the write.
				return errCanceled
			case err == nil:
				tr.blocked(nil)
				if already {
					s.info(tr.actor, "barrier commit found its change already written", "operation", tr.id(), "scope", b.scope)
				}
			case current != nil && current.DispatchStarted:
				// Destructive work may already have reached the backend. Ending the record would
				// release its scope and strand the source hold. Keep the exact operation alive;
				// its idempotent commit reconciles and retries from the remaining source state.
				tr.blocked([]Blocker{{Code: BlockerBackendOutcomeUnknown, Message: err.Error()}})
				if err = s.sleep(tr.ctx, s.fencePoll()); err != nil {
					return fmt.Errorf("%w: reconciling destructive barrier commit: %w", ErrUnavailable, err)
				}
				if checkErr := tr.check(); checkErr != nil {
					return checkErr
				}
				continue
			default:
				return err
			}
			break
		}
		st.Committed, st.CommitVersion, st.CommittedAt = true, v, s.now().UTC()
		tr.setBarrier(st, nil)
		s.observeBarrier(PhaseCommit, s.now().Sub(start))
	}
	return nil
}

// errBarrierGone is a commit that found the scope no longer carrying the operation's barrier.
var errBarrierGone = errors.New("the barrier is no longer on its scope")

// carriesBarrier reports whether scope (a placement or a cluster resource) carries barrier id in
// the authoritative directory: the same read every commit closure makes before it writes.
func (s *Server) carriesBarrier(ctx context.Context, scope, id string) (bool, error) {
	if err := s.Dir.Sync(ctx); err != nil {
		return false, err
	}
	snap := s.Dir.Snapshot()
	var b *directory.Barrier
	if name, ok := strings.CutPrefix(scope, "cluster:"); ok {
		if c, found := snap.Cluster(name); found {
			b = c.Barrier
		}
	} else {
		tenant, bucket, _ := directory.SplitKey(strings.TrimPrefix(scope, "placement:"))
		if p, found := snap.Lookup(tenant, bucket); found {
			b = p.Barrier
		}
	}
	return b != nil && b.ID == id, nil
}

// blockUntil loops until eval reports no blocker, writing the blockers to the record as they
// change and checking that this node still owns an unfinished record. There is no deadline: a
// timeout means blocked, and an operator resumes or cancels (design §4, "Barrier state machine").
func (s *Server) blockUntil(tr *tracker, eval func(ctx context.Context) ([]Blocker, error)) error {
	for {
		if err := tr.check(); err != nil {
			return err
		}
		blockers, err := eval(tr.ctx)
		if err != nil {
			blockers = []Blocker{{Code: BlockerQuorumUnavailable, Message: err.Error()}}
		}
		tr.blocked(blockers)
		if len(blockers) == 0 {
			return nil
		}
		if err := s.sleep(tr.ctx, s.fencePoll()); err != nil {
			return fmt.Errorf("%w: waiting for the barrier: %w", ErrUnavailable, err)
		}
	}
}

// drainBlockers evaluates a barrier against every proxy that could have served its scope's
// routing before the hold: every member, live or not, and this node's own proxy in a lab. A
// member drains once its acknowledgement of the barrier, at the scope's generation, says the
// gate is closed with nothing out and no outcome unknown, and its restart cache holds the hold's
// version. Silence proves nothing (proxy_missing); an incarnation of any member that ended
// without retiring proves the opposite (incarnation_unresolved). Retired members are counted
// out.
func (s *Server) drainBlockers(ctx context.Context, st *BarrierState, extra func(context.Context) ([]Blocker, error)) ([]Blocker, error) {
	ms, err := s.members(ctx)
	if err != nil {
		return nil, err
	}
	cur := s.Dir.Snapshot().Identity()
	var out []Blocker
	for i := range ms {
		m := &ms[i]
		for _, inc := range m.Unresolved {
			out = append(out, Blocker{Code: BlockerIncarnationUnresolved, ProxyID: m.ID, Incarnation: inc.ID, Count: inc.Uncertain,
				Message: fmt.Sprintf("incarnation %s of %s did not retire cleanly; its backend work may still land (`shunt proxy resolve %s --incarnation %s --attest <why>` once it has ended)", inc.ID, m.ID, m.ID, inc.ID)})
		}
		if m.Retired() {
			continue
		}
		if !m.Live {
			out = append(out, Blocker{Code: BlockerProxyMissing, ProxyID: m.ID, Message: missingText(m, "its process may still serve the old routing")})
			continue
		}
		if m.Identity != cur {
			out = append(out, Blocker{Code: BlockerProxyMissing, ProxyID: m.ID, Message: m.ID + " is on another directory lineage and must be re-enrolled"})
			continue
		}
		out = append(out, memberBlockers(m.ID, m.Barriers, m.Durable, st)...)
	}
	if s.LocalGates != nil {
		// A lab's own proxy: its gates drain like a member's, and its directory is on disk.
		out = append(out, memberBlockers(s.node(), s.LocalGates.Acks(s.Dir.Snapshot()), s.Dir.Snapshot().Version(), st)...)
	}
	if extra != nil && len(out) == 0 {
		// The barrier's own checks (cutover's quiet window, purge's re-diff) prove something only
		// once the fleet has drained, and cutover's sleeps its window: neither runs before.
		more, err := extra(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, more...)
	}
	return out, nil
}

// missingText is the proxy_missing message for a member that is not live. A member with no
// current process has stopped: it retired with outcomes unknown (its incarnation is unresolved),
// or its crashed process was resolved; either way it serves nothing, and forget is what is left.
func missingText(m *Member, serving string) string {
	switch {
	case m.Incarnation == nil && len(m.Unresolved) > 0:
		return fmt.Sprintf("%s retired with outcomes unknown; resolve its incarnation, then `shunt proxy forget %s`", m.ID, m.ID)
	case m.Incarnation == nil:
		return fmt.Sprintf("%s has no running process; `shunt proxy forget %s`", m.ID, m.ID)
	}
	return fmt.Sprintf("%s is silent (last seen %s ago); %s", m.ID, m.SinceSeen.Round(time.Second), serving)
}

// memberBlockers is what one proxy's acknowledgements say stands in a barrier's way.
func memberBlockers(id string, acks []admission.Ack, durable int64, st *BarrierState) []Blocker {
	i := slices.IndexFunc(acks, func(a admission.Ack) bool { return a.ID == st.ID })
	if i < 0 {
		return []Blocker{{Code: BlockerInstallPending, ProxyID: id, Message: id + " has not installed the hold yet"}}
	}
	ack := &acks[i]
	var out []Blocker
	// The ack names this barrier by id, so it is this hold's however far the scope's generation
	// has moved since: only this operation can clear its barrier, and a later write to the scope
	// (a secret rotation stamping a cluster) must not strand the drain. An older generation is a
	// proxy that has not installed the hold yet.
	if ack.Generation < st.Generation || !ack.Closed {
		return []Blocker{{Code: BlockerInstallPending, ProxyID: id, Message: fmt.Sprintf("%s acknowledges the hold at generation %d, closed %v; the barrier is at generation %d", id, ack.Generation, ack.Closed, st.Generation)}}
	}
	if durable < st.HoldVersion {
		out = append(out, Blocker{Code: BlockerCacheNotDurable, ProxyID: id, Message: fmt.Sprintf("%s's restart cache is at version %d, before the hold's %d", id, durable, st.HoldVersion)})
	}
	if ack.Uncertain > 0 {
		out = append(out, Blocker{Code: BlockerBackendOutcomeUnknown, ProxyID: id, Count: ack.Uncertain, Message: fmt.Sprintf("%s never learned the outcome of %d request(s) through this gate; retire and resolve it once the backend is quiet", id, ack.Uncertain)})
	}
	if ack.Inflight > 0 {
		out = append(out, Blocker{Code: BlockerOldRequests, ProxyID: id, Count: ack.Inflight, Message: fmt.Sprintf("%s still has %d request(s) out through this gate", id, ack.Inflight)})
	}
	return out
}

// Retired reports whether the member's last process retired cleanly and none has replaced it: it
// serves nothing and counts out of every barrier.
func (m *Member) Retired() bool {
	return m.Incarnation != nil && m.Incarnation.State == IncarnationRetired
}

// observeBarrier records a phase's duration.
func (s *Server) observeBarrier(phase string, d time.Duration) {
	if s.Metrics != nil {
		s.Metrics.BarrierDuration.WithLabelValues(phase).Observe(d.Seconds())
	}
}

// publishBlockers refreshes shunt_barrier_blockers from every barrier this node runs.
func (s *Server) publishBlockers() {
	if s.Metrics == nil {
		return
	}
	counts := map[string]int{}
	s.mu.Lock()
	trackers := make([]*tracker, 0, len(s.running))
	for _, tr := range s.running {
		trackers = append(trackers, tr)
	}
	s.mu.Unlock()
	for _, tr := range trackers {
		for _, b := range tr.snapshot().Blockers {
			counts[b.Code]++
		}
	}
	s.Metrics.BarrierBlockers.Reset()
	for code, n := range counts {
		s.Metrics.BarrierBlockers.WithLabelValues(code).Set(float64(n))
	}
}

// releaseHold undoes an operation's hold, by kind: what a cancellation before the commit writes.
// Each write is compared on the barrier's id, so a hold another operation wrote is never touched,
// and a hold whose commit already cleared it is refused (TransitionError or ErrConflict). A record
// whose hold version never became durable may still have its hold in the directory (the owner was
// lost between the two writes): it is released the same way.
func (s *Server) releaseHold(ctx context.Context, op *Operation, actor string) error {
	if op.Barrier == nil {
		return nil
	}
	id := op.Barrier.ID
	switch op.Kind {
	case OpRamp, OpMigrate:
		tenant, bucket, _ := directory.SplitKey(op.Placement)
		return s.Dir.SetState(ctx, tenant, bucket, directory.StateRamping, directory.Transition{Release: true, Barrier: id}, actor)
	case OpPlacementReadOnly:
		tenant, bucket, _ := directory.SplitKey(op.Placement)
		return s.Dir.SetPlacementReadOnly(ctx, tenant, bucket, false, false, id, actor)
	case OpClusterReadOnly:
		return s.Dir.SetClusterReadOnly(ctx, op.Cluster, false, false, id, actor)
	case OpCutover, OpPurge:
		return s.Dir.ClearBarrier(ctx, op.Barrier.Scope, id, actor)
	}
	return nil
}

// cancelPhases are the phases of a barrier operation before its first directory write: a record
// in one of them, with no barrier yet, has nothing to release.
var cancelPhases = []string{"", PhaseQueued, PhasePrecondition, PhaseDiff}

// notCancellable says why an unfinished operation cannot be canceled now, or nil when it can.
// ownerGone is whether the node that owns the record is known to have stopped running it. The
// cancellation re-checks this inside its compare-and-swap of the record, so the record is the
// point where a cancellation and the owner's next step (its hold, commit or first destructive
// dispatch, each written to the record first) are ordered.
func notCancellable(op *Operation, ownerGone bool) error {
	switch {
	case op.Kind == OpMover:
		return &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s is a mover: it ends with its worker, and an expired worker session is resolved with `shunt operation resolve-worker %s --session <id> --attest <why>`", op.ID, op.ID)}
	case !resumable(op.Kind):
		return &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s (%s) has no drain barrier that can be safely canceled", op.ID, op.Kind)}
	case op.Barrier == nil:
		if !slices.Contains(cancelPhases, op.Phase) {
			return &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s is writing its change (phase %s)", op.ID, op.Phase)}
		}
		return nil
	case op.Barrier.Committed:
		return &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s committed its change at directory version %d; it settles, and a further change is a new operation", op.ID, op.Barrier.CommitVersion)}
	case op.Barrier.DispatchStarted:
		return &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s has started destructive backend work; it must be resumed to completion", op.ID)}
	case op.Barrier.HoldVersion == 0 && !ownerGone:
		return refuse("operation %s is still writing its hold; retry cancellation once the record shows hold_version", op.ID)
	case op.Phase == PhaseCommit && !ownerGone:
		return &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s is writing its commit; it settles, and a further change is a new operation", op.ID)}
	}
	return nil
}

// cancelOperation is POST /v1/operations/{id}/cancel: a durable request, allowed before the
// operation's change is committed or its destructive work dispatched (notCancellable). The record
// is ended first, compared on its sequence, so an owner still running it finds its next record
// write stale and stops before it touches the directory or a backend. Then the hold is released,
// compared on the barrier's id. Should the owner's commit have landed first after all (its record
// write was lost with its node), the record is corrected to say the change is in force. A record
// that already ended canceled answers as it is, and a hold such a record still carries (its
// release failed) is released again.
func (s *Server) cancelOperation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := context.WithoutCancel(r.Context())
	op, err := s.ops().Get(r.Context(), id)
	switch {
	case err != nil:
		fail(w, err)
		return
	case op == nil:
		fail(w, notFound("no operation %s", id))
		return
	case op.Status == StatusCancelled:
		if rerr := s.releaseCanceled(ctx, op, actor(r)); rerr != nil {
			fail(w, rerr)
			return
		}
		writeJSON(w, http.StatusOK, op.Public())
		return
	case op.Terminal():
		fail(w, &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s has ended %s", id, op.Status)})
		return
	}
	ownerGone, err := s.ownerGone(r.Context(), op)
	if err != nil {
		fail(w, err)
		return
	}
	if nerr := notCancellable(op, ownerGone); nerr != nil {
		fail(w, nerr)
		return
	}
	fromNode, fromTerm := op.Node, op.OwnerTerm
	ended, wrote, err := s.updateRecord(ctx, id, func(cur *Operation) error {
		if cur.Node != fromNode || cur.OwnerTerm != fromTerm {
			return refuse("operation %s was taken over by control node %s at owner term %d; retry the cancellation", id, cur.Node, cur.OwnerTerm)
		}
		if nerr := notCancellable(cur, ownerGone); nerr != nil {
			return nerr
		}
		cur.Status, cur.Phase, cur.Blockers, cur.BlockerCount, cur.AllowedActions = StatusCancelled, PhaseDone, nil, 0, []string{}
		cur.EffectState = EffectNone
		cur.Error = &Error{Code: StatusCancelled, Message: "canceled by " + actor(r) + " before the change was committed; the routing is as it was before the operation"}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if !wrote {
		if ended.Status == StatusCancelled {
			writeJSON(w, http.StatusOK, ended.Public())
			return
		}
		fail(w, &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s has ended %s", id, ended.Status)})
		return
	}
	s.markLost(id)
	if ended.Barrier != nil {
		carried, cerr := s.carriesBarrier(ctx, ended.Barrier.Scope, ended.Barrier.ID)
		if cerr == nil && carried {
			cerr = s.releaseHold(ctx, ended, actor(r))
			var te *directory.TransitionError
			if errors.As(cerr, &te) || errors.Is(cerr, directory.ErrConflict) {
				carried, cerr = false, nil // cleared between the read and the release: by the commit
			}
		}
		switch {
		case cerr != nil:
			fail(w, fmt.Errorf("%w: operation %s is canceled, but releasing its hold failed; retry the cancellation to release it: %w", ErrUnavailable, id, cerr))
			return
		case !carried && ended.Barrier.HoldVersion > 0:
			// The hold was durable and is gone, and only its own commit clears it: the owner
			// committed before this cancellation, and its record write was lost.
			corrected, cerr := s.correctCommitted(ctx, id, actor(r))
			if cerr != nil {
				fail(w, cerr)
				return
			}
			fail(w, &codedError{status: http.StatusConflict, code: CodeNotCancellable, msg: fmt.Sprintf("operation %s committed its change before the cancellation reached it; the change is in force (%s)", id, corrected.Error.Message)})
			return
		}
	}
	s.info(actor(r), "operation canceled", "operation", id, "kind", op.Kind, "placement", op.Placement, "cluster", op.Cluster)
	writeJSON(w, http.StatusOK, ended.Public())
}

// ownerGone reports whether op's owner has stopped running it: this node without a run of it, or
// another node whose liveness in the control plane has lapsed.
func (s *Server) ownerGone(ctx context.Context, op *Operation) (bool, error) {
	if op.Node == s.node() {
		return !s.runs(op.ID), nil
	}
	live, err := s.ops().OwnerLive(ctx, op.Node)
	return !live, err
}

// releaseCanceled releases the hold a canceled record still carries: its cancellation ended the
// record and then failed to release it. Nothing else can clear that hold; its owner has stopped.
func (s *Server) releaseCanceled(ctx context.Context, op *Operation, actor string) error {
	if op.Barrier == nil {
		return nil
	}
	carried, err := s.carriesBarrier(ctx, op.Barrier.Scope, op.Barrier.ID)
	if err != nil || !carried {
		return err
	}
	err = s.releaseHold(ctx, op, actor)
	var te *directory.TransitionError
	if errors.As(err, &te) || errors.Is(err, directory.ErrConflict) {
		return nil // released meanwhile
	}
	return err
}

// correctCommitted rewrites a record a cancellation ended while its commit had landed: the change
// is in force, so the record says so (failed as asked, effect committed), and never canceled.
func (s *Server) correctCommitted(ctx context.Context, id, actor string) (*Operation, error) {
	for range 16 {
		op, err := s.ops().Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if op == nil {
			return nil, notFound("no operation %s", id)
		}
		if op.Barrier != nil {
			op.Barrier.Committed = true
		}
		op.Status, op.EffectState = StatusFailed, EffectCommitted
		op.Error = &Error{Code: CodeNotCancellable, Message: "the change was committed before " + actor + "'s cancellation reached it and is in force; a further change is a new operation"}
		op.Sequence++
		op.Updated = s.now().UTC()
		err = s.ops().Update(ctx, op)
		if err == nil {
			s.info(actor, "cancellation found the change committed", "operation", id, "kind", op.Kind, "placement", op.Placement, "cluster", op.Cluster)
			return op, nil
		}
		if !errors.Is(err, ErrStaleSequence) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("%w: operation %s kept changing while its record was corrected", ErrUnavailable, id)
}

// markLost tells a tracker on this node that its durable record was ended or taken over by
// another request. A blocked tracker may otherwise keep seeing the same blockers and make no CAS
// write through which it could discover the newer sequence.
func (s *Server) markLost(id string) {
	s.mu.Lock()
	tr := s.running[id]
	s.mu.Unlock()
	if tr == nil {
		return
	}
	tr.mu.Lock()
	tr.lost = true
	tr.mu.Unlock()
}

// endRecord writes onto an unfinished record, over whatever the node running it wrote meanwhile;
// a record that ended first is returned as it is.
func (s *Server) endRecord(ctx context.Context, id string, end func(*Operation) error) (*Operation, error) {
	op, _, err := s.updateRecord(ctx, id, end)
	return op, err
}

// updateRecord is endRecord, and reports whether its write landed (false: the record had ended).
func (s *Server) updateRecord(ctx context.Context, id string, end func(*Operation) error) (*Operation, bool, error) {
	for attempt := 0; attempt < 16; attempt++ {
		op, err := s.ops().Get(ctx, id)
		if err != nil {
			return nil, false, err
		}
		if op == nil {
			return nil, false, notFound("no operation %s", id)
		}
		if op.Terminal() {
			return op, false, nil
		}
		if endErr := end(op); endErr != nil {
			return nil, false, endErr
		}
		op.Sequence++
		op.Updated = s.now().UTC()
		err = s.ops().Update(ctx, op)
		if err == nil {
			return op, true, nil
		}
		if !errors.Is(err, ErrStaleSequence) {
			return nil, false, err
		}
	}
	return nil, false, fmt.Errorf("%w: operation %s kept changing under the request; retry", ErrUnavailable, id)
}

// resumeOperation is POST /v1/operations/{id}/resume: this node takes over an unfinished
// operation whose owner is gone, and carries it on from where its record stands. An operation
// whose owner is live is refused: resuming it here would run it twice.
func (s *Server) resumeOperation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	op, err := s.ops().Get(r.Context(), id)
	switch {
	case err != nil:
		fail(w, err)
		return
	case op == nil:
		fail(w, notFound("no operation %s", id))
		return
	case op.Terminal():
		fail(w, refuse("operation %s has ended %s; a change is a new operation", id, op.Status))
		return
	case !resumable(op.Kind) || op.Barrier == nil:
		fail(w, refuse("operation %s (%s) has no durable barrier to resume; repeat a short operation that lost its node", id, op.Kind))
		return
	}
	if s.runs(id) {
		fail(w, refuse("operation %s is running on this node", id))
		return
	}
	if op.Node != s.node() {
		live, lerr := s.ops().OwnerLive(r.Context(), op.Node)
		if lerr != nil {
			fail(w, lerr)
			return
		}
		if live {
			fail(w, refuse("operation %s is owned by control node %s, which is live; wait for it, or cancel the operation", id, op.Node))
			return
		}
	}
	fromNode, fromTerm := op.Node, op.OwnerTerm
	taken, err := s.endRecord(context.WithoutCancel(r.Context()), id, func(op *Operation) error {
		if op.Node != fromNode || op.OwnerTerm != fromTerm {
			return refuse("operation %s was taken over by control node %s at owner term %d; follow that owner", id, op.Node, op.OwnerTerm)
		}
		op.Node, op.OwnerTerm, op.Status = s.node(), op.OwnerTerm+1, StatusRunning
		op.Blockers, op.BlockerCount = nil, 0
		op.AllowedActions = []string{}
		if op.Barrier != nil && op.Barrier.HoldVersion > 0 && !op.Barrier.Committed {
			op.AllowedActions = []string{ActionCancel}
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if taken.Terminal() {
		writeJSON(w, http.StatusOK, taken.Public())
		return
	}
	tr := s.trackerFor(taken, actor(r))
	s.info(actor(r), "operation resumed", "operation", id, "kind", op.Kind, "from", op.Node, "term", taken.OwnerTerm)
	s.operate(tr, func(tr *tracker) (any, error) { return s.rerun(tr) })
	w.Header().Set("Location", "/v1/operations/"+id)
	writeJSON(w, http.StatusAccepted, tr.snapshot().Public())
}

// resumable reports whether an operation of this kind can be carried on from its record.
func resumable(kind string) bool {
	switch kind {
	case OpRamp, OpMigrate, OpPlacementReadOnly, OpClusterReadOnly, OpCutover, OpPurge:
		return true
	}
	return false
}

// rerun carries a resumed operation on from its record, by kind.
func (s *Server) rerun(tr *tracker) (any, error) {
	op := tr.snapshot()
	switch op.Kind {
	case OpRamp:
		var a RampRequest
		if err := decodeArgs(op.Args, &a); err != nil {
			return nil, err
		}
		return s.runRamp(tr, op.Placement, a)
	case OpMigrate:
		var a MigrateRequest
		if err := decodeArgs(op.Args, &a); err != nil {
			return nil, err
		}
		return s.runMigrate(tr, op.Placement, a)
	case OpPlacementReadOnly:
		var a ReadOnlyRequest
		if err := decodeArgs(op.Args, &a); err != nil {
			return nil, err
		}
		return s.runPlacementReadOnly(tr, op.Placement, a)
	case OpClusterReadOnly:
		var a ReadOnlyRequest
		if err := decodeArgs(op.Args, &a); err != nil {
			return nil, err
		}
		return s.runClusterReadOnly(tr, op.Cluster, a)
	case OpCutover:
		var a CutoverRequest
		if err := decodeArgs(op.Args, &a); err != nil {
			return nil, err
		}
		return s.runCutover(tr, op.Placement, a)
	case OpPurge:
		var a PurgeRequest
		if err := decodeArgs(op.Args, &a); err != nil {
			return nil, err
		}
		return s.runPurge(tr, op.Placement, a)
	}
	return nil, refuse("operation %s (%s) is not resumable", op.ID, op.Kind)
}

// ResumeOwn carries on the barrier operations this node owned when it last stopped: their holds
// are written and their records say where they stand, so the node picks them up rather than
// failing them (FailOrphans ends the rest).
func (s *Server) ResumeOwn(ctx context.Context) error {
	ops, err := s.ops().Evidence(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, op := range ops {
		if op.Node != s.node() || op.Terminal() || !resumable(op.Kind) || op.Barrier == nil || s.runs(op.ID) {
			continue
		}
		fromNode, fromTerm := op.Node, op.OwnerTerm
		taken, err := s.endRecord(ctx, op.ID, func(op *Operation) error {
			if op.Node != fromNode || op.OwnerTerm != fromTerm {
				return refuse("operation %s was already resumed by control node %s at owner term %d", op.ID, op.Node, op.OwnerTerm)
			}
			op.OwnerTerm, op.Status, op.Blockers, op.BlockerCount = op.OwnerTerm+1, StatusRunning, nil, 0
			return nil
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if taken.Terminal() {
			continue
		}
		tr := s.trackerFor(taken, "shunt-control")
		if s.Log != nil {
			s.Log.Info("operation resumed after a restart", "operation", op.ID, "kind", op.Kind, "placement", op.Placement, "cluster", op.Cluster, "phase", op.Phase)
		}
		s.operate(tr, func(tr *tracker) (any, error) { return s.rerun(tr) })
	}
	return errors.Join(errs...)
}

// blockersHandler is GET /v1/operations/{id}/blockers: the record's blockers, complete.
func (s *Server) blockersHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	op, err := s.ops().Get(r.Context(), id)
	switch {
	case err != nil:
		fail(w, err)
		return
	case op == nil:
		fail(w, notFound("no operation %s", id))
		return
	}
	blockers := op.Blockers
	if blockers == nil {
		blockers = []Blocker{}
	}
	writeJSON(w, http.StatusOK, BlockerPage{Operation: id, Blockers: blockers, Complete: true})
}

// BlockerPage is GET /v1/operations/{id}/blockers.
type BlockerPage struct {
	Operation string    `json:"operation"`
	Blockers  []Blocker `json:"blockers"`
	Complete  bool      `json:"complete"`
}

// BlockerText is a blocker list in one line, for a log line, a message or the CLI.
func BlockerText(bs []Blocker) string {
	parts := make([]string, 0, len(bs))
	for _, b := range bs {
		p := b.Code
		if b.ProxyID != "" {
			p += " " + b.ProxyID
		}
		if b.Count > 0 {
			p += fmt.Sprintf(" (%d)", b.Count)
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ", ")
}
