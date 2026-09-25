package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

const defaultWorkerTTL = 5 * time.Second

const (
	maxWorkerCount       = 1 << 30
	maxWorkerError       = 4096
	maxWorkerAttestation = 4096
)

const (
	// WorkerActive is a currently heartbeating external mover session.
	WorkerActive = "active"
	// WorkerCompleted is a session explicitly closed with no in-flight or uncertain effects.
	WorkerCompleted = "completed"
	// WorkerUnresolved is an expired session whose last backend effects need reconciliation.
	WorkerUnresolved = "unresolved"
)

// WorkerSession is the durable, generation-bound lease of an out-of-process mover. A session
// that stops heartbeating is unresolved rather than silently clean: its last dispatched backend
// effect may still land, so its operation keeps the placement reserved.
type WorkerSession struct {
	ID          string             `json:"id"`
	Identity    directory.Identity `json:"identity"`
	Generation  int64              `json:"generation"`
	Sequence    int64              `json:"sequence"`
	State       string             `json:"state"`
	Inflight    int64              `json:"inflight,omitempty"`
	Uncertain   int64              `json:"uncertain,omitempty"`
	LastSeen    time.Time          `json:"last_seen,omitzero"`
	CompletedAt time.Time          `json:"completed_at,omitzero"`
	Error       string             `json:"error,omitempty"`
	ResolvedBy  string             `json:"resolved_by,omitempty"`
	Attestation string             `json:"attestation,omitempty"`
}

// WorkerHeartbeat renews one mover session. Resolve is an explicit reconciliation of an expired
// session; merely restarting a CLI and reusing its id cannot erase the unresolved interval.
type WorkerHeartbeat struct {
	Session     string             `json:"session"`
	Identity    directory.Identity `json:"identity"`
	Generation  int64              `json:"generation"`
	Sequence    int64              `json:"sequence"`
	Inflight    int64              `json:"inflight"`
	Uncertain   int64              `json:"uncertain"`
	Complete    bool               `json:"complete,omitempty"`
	Error       string             `json:"error,omitempty"`
	Resolve     bool               `json:"resolve,omitempty"`
	Attestation string             `json:"attestation,omitempty"`
}

// WorkerHeartbeatAnswer returns the durable session and whether its placement has entered a hold.
type WorkerHeartbeatAnswer struct {
	Worker WorkerSession `json:"worker"`
	Hold   bool          `json:"hold,omitempty"`
}

// ValidWorkerSessionID reports whether id is a random 128-bit lowercase hexadecimal id.
func ValidWorkerSessionID(id string) bool { return ValidIncarnation(id) }

func (s *Server) workerTTL() time.Duration {
	if s.WorkerTTL > 0 {
		return s.WorkerTTL
	}
	return defaultWorkerTTL
}

func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req WorkerHeartbeat
	if !decode(w, r, &req) {
		return
	}
	if !ValidWorkerSessionID(req.Session) || req.Sequence <= 0 || req.Generation <= 0 || req.Inflight < 0 || req.Uncertain < 0 ||
		req.Inflight > maxWorkerCount || req.Uncertain > maxWorkerCount {
		fail(w, bad("invalid worker heartbeat: session, generation and sequence must be positive and counts between 0 and %d", maxWorkerCount))
		return
	}
	if len(req.Error) > maxWorkerError || len(req.Attestation) > maxWorkerAttestation {
		fail(w, bad("invalid worker heartbeat: error and attestation are limited to %d bytes", maxWorkerError))
		return
	}
	if req.Complete && (req.Inflight != 0 || req.Uncertain != 0) {
		fail(w, bad("a completed worker session must report zero in-flight and uncertain effects"))
		return
	}
	if req.Resolve && req.Attestation == "" {
		fail(w, bad("resolving an expired worker session requires an attestation"))
		return
	}

	answer, err := s.applyWorker(r.Context(), id, func(*Operation) (WorkerHeartbeat, error) { return req, nil }, actor(r))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, answer)
}

// applyWorker applies a worker heartbeat, built from the record as it stands, by a compare-and-swap
// on the durable record. Every control node writes the session this way, the owner too: a
// heartbeat through another node advances the record past the owner's copy, so a write built on
// that copy would be built on stale state. A lost swap re-reads the record and builds and checks
// the heartbeat again; the owner's own writes take the session from the record (tracker.put).
func (s *Server) applyWorker(ctx context.Context, id string, build func(*Operation) (WorkerHeartbeat, error), actor string) (WorkerHeartbeatAnswer, error) {
	for range 16 {
		op, err := s.ops().Get(ctx, id)
		if err != nil {
			return WorkerHeartbeatAnswer{}, err
		}
		if op == nil {
			return WorkerHeartbeatAnswer{}, fmt.Errorf("%w: operation %s", ErrUnknownOperation, id)
		}
		req, err := build(op)
		if err != nil {
			return WorkerHeartbeatAnswer{}, err
		}
		worker, changed, hold, err := s.nextWorker(op, req, actor)
		if err != nil || !changed {
			return WorkerHeartbeatAnswer{Worker: worker, Hold: hold}, err
		}
		op.Worker = &worker
		op.Sequence++
		op.Updated = s.now().UTC()
		if err = s.ops().Update(context.WithoutCancel(ctx), op); err == nil {
			return WorkerHeartbeatAnswer{Worker: worker, Hold: hold}, nil
		}
		if !errors.Is(err, ErrStaleSequence) {
			return WorkerHeartbeatAnswer{}, err
		}
	}
	return WorkerHeartbeatAnswer{}, fmt.Errorf("%w: operation %s kept changing while its worker heartbeated; retry", ErrUnavailable, id)
}

// ResolveWorkerRequest is POST /v1/operations/{id}/resolve-worker: an operator's reconciliation
// of an external mover whose worker session expired, or never enrolled, with work unresolved.
// Session is the id `shunt operation show` prints for the record's worker; Attestation says how
// the operator established that the worker's backend effects have ended.
type ResolveWorkerRequest struct {
	Session     string `json:"session"`
	Attestation string `json:"attestation"`
}

// workerResolvable says why an external mover's record cannot be resolved now, or nil. A worker
// still heartbeating ends itself; only a session that expired, is marked unresolved, or never
// enrolled within its TTL is the operator's to resolve.
func (s *Server) workerResolvable(op *Operation) error {
	var args MoverRequest
	if op.Kind != OpMover || json.Unmarshal(op.Args, &args) != nil || !args.External {
		return refuse("operation %s is not an external mover; only an external mover's worker session is resolved", op.ID)
	}
	if op.Terminal() {
		return refuse("operation %s has ended %s", op.ID, op.Status)
	}
	now := s.now()
	switch w := op.Worker; {
	case w == nil:
		if now.Sub(op.Created) <= s.workerTTL() {
			return refuse("operation %s's worker session has not enrolled yet; wait %s for it", op.ID, s.workerTTL())
		}
	case w.State == WorkerCompleted:
		return refuse("worker session %s has completed; its operation ends with it", w.ID)
	case w.State == WorkerActive && now.Sub(w.LastSeen) <= s.workerTTL():
		return refuse("worker session %s is live (last heartbeat %s ago); it ends itself, or stop it and resolve it once it has expired", w.ID, now.Sub(w.LastSeen).Round(time.Millisecond))
	}
	return nil
}

// resolveWorker is POST /v1/operations/{id}/resolve-worker. The resolution is recorded on the
// worker session with the actor and the attestation, and closes it: the mover's operation ends
// failed (its copy did not finish), which frees the placement, and the evidence stays on the
// record. It is refused for a worker that is still heartbeating.
func (s *Server) resolveWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req ResolveWorkerRequest
	if !decode(w, r, &req) {
		return
	}
	if !ValidWorkerSessionID(req.Session) {
		fail(w, bad("session: want the worker session id `shunt operation show %s` prints", id))
		return
	}
	if req.Attestation == "" || len(req.Attestation) > maxWorkerAttestation {
		fail(w, bad("attestation: say how the worker's backend effects were established to have ended (1 to %d bytes)", maxWorkerAttestation))
		return
	}
	who := actor(r)
	answer, err := s.applyWorker(r.Context(), id, func(op *Operation) (WorkerHeartbeat, error) {
		// The same resolution again (an answer lost on the way back) repeats the heartbeat that
		// recorded it, which answers as it did and writes nothing.
		if w := op.Worker; w != nil && w.State == WorkerCompleted && w.ID == req.Session && w.ResolvedBy == who && w.Attestation == req.Attestation {
			return WorkerHeartbeat{Session: w.ID, Identity: w.Identity, Generation: w.Generation, Sequence: w.Sequence, Complete: true, Resolve: true,
				Attestation: w.Attestation, Error: w.Error}, nil
		}
		if err := s.workerResolvable(op); err != nil {
			return WorkerHeartbeat{}, err
		}
		seq := int64(1)
		if op.Worker != nil {
			seq = op.Worker.Sequence + 1
		}
		var gen int64
		if op.Scope != nil {
			gen = op.Scope.Generation
		}
		return WorkerHeartbeat{Session: req.Session, Identity: op.Identity, Generation: gen, Sequence: seq, Complete: true, Resolve: true,
			Attestation: req.Attestation, Error: "worker session " + req.Session + " expired with its copy unfinished; resolved by " + who}, nil
	}, who)
	if err != nil {
		fail(w, err)
		return
	}
	s.info(who, "worker session resolved", "operation", id, "session", req.Session)
	op, err := s.ops().Get(r.Context(), id)
	if err != nil || op == nil {
		writeJSON(w, http.StatusOK, WorkerHeartbeatAnswer{Worker: answer.Worker})
		return
	}
	// A running owner ends the record at its next poll. One that is gone (its node restarted, or its
	// liveness lapsed) never will: the record ends here, as the owner would have ended it.
	if gone, gerr := s.ownerGone(r.Context(), op); gerr == nil && gone && !op.Terminal() {
		if ended, eerr := s.endRecord(context.WithoutCancel(r.Context()), id, func(op *Operation) error {
			if op.Worker == nil || op.Worker.State != WorkerCompleted {
				return refuse("operation %s's worker session changed under the resolution; retry", id)
			}
			op.Status, op.Phase, op.Blockers, op.BlockerCount, op.AllowedActions = StatusFailed, PhaseDone, nil, 0, []string{}
			op.Error = &Error{Code: "backend", Message: "external mover: " + op.Worker.Error}
			if op.EffectState == "" {
				op.EffectState = EffectNone
			}
			return nil
		}); eerr == nil {
			op = ended
		}
	}
	writeJSON(w, http.StatusOK, op.Public())
}

// nextWorker is the session req makes of op's, and whether it differs from the stored one. A
// completed session is final, and an ended record keeps the session it ended with: a late
// heartbeat, from a mover whose session an operator resolved or that already completed, is refused
// rather than written over the evidence. An exact repeat of the heartbeat that wrote the stored
// session answers it again.
func (s *Server) nextWorker(op *Operation, req WorkerHeartbeat, actor string) (worker WorkerSession, changed, hold bool, err error) {
	if op.Kind != OpMover || op.Scope == nil {
		return WorkerSession{}, false, false, refuse("operation %s is not a mover operation", op.ID)
	}
	var args MoverRequest
	if json.Unmarshal(op.Args, &args) != nil || !args.External || args.Session != req.Session {
		return WorkerSession{}, false, false, refuse("operation %s is not enrolled for worker session %s", op.ID, req.Session)
	}
	if req.Identity != op.Identity || req.Generation != op.Scope.Generation {
		return WorkerSession{}, false, false, refuse("worker session %s is for another directory lineage or placement generation", req.Session)
	}
	now := s.now().UTC()
	cur := op.Worker
	repeat := cur != nil && req.Sequence == cur.Sequence && sameWorker(cur, req, actor)
	if repeat && cur.State == WorkerCompleted {
		return *cur, false, s.workerHeld(op.Placement), nil
	}
	if op.Terminal() {
		return WorkerSession{}, false, false, refuse("operation %s has ended %s; worker session %s writes nothing more to it", op.ID, op.Status, req.Session)
	}
	if cur != nil && cur.State == WorkerCompleted {
		how := ""
		if cur.ResolvedBy != "" {
			how = ", resolved by " + cur.ResolvedBy
		}
		return WorkerSession{}, false, false, refuse("worker session %s has completed%s; its operation ends with it", cur.ID, how)
	}
	expired := cur != nil && cur.State == WorkerActive && now.Sub(cur.LastSeen) > s.workerTTL()
	if cur != nil && (cur.State == WorkerUnresolved || expired) && !req.Resolve {
		return WorkerSession{}, false, false, refuse("worker session %s expired with work unresolved; reconcile it with resolve and an attestation", req.Session)
	}
	if cur != nil {
		if req.Sequence < cur.Sequence {
			return WorkerSession{}, false, false, refuse("worker session %s heartbeat sequence %d is behind %d", req.Session, req.Sequence, cur.Sequence)
		}
		if req.Sequence == cur.Sequence {
			if !repeat {
				return WorkerSession{}, false, false, refuse("worker session %s heartbeat sequence %d was already used with different state", req.Session, req.Sequence)
			}
			return *cur, false, s.workerHeld(op.Placement), nil
		}
	}
	state := WorkerActive
	if req.Complete {
		state = WorkerCompleted
	}
	worker = WorkerSession{ID: req.Session, Identity: req.Identity, Generation: req.Generation,
		Sequence: req.Sequence, State: state, Inflight: req.Inflight, Uncertain: req.Uncertain,
		LastSeen: now, Error: req.Error}
	if req.Complete {
		worker.CompletedAt = now
	}
	if req.Resolve {
		worker.ResolvedBy, worker.Attestation = actor, req.Attestation
	}
	return worker, true, s.workerHeld(op.Placement), nil
}

// sameWorker reports whether req, at cur's sequence, is the heartbeat that wrote cur.
func sameWorker(cur *WorkerSession, req WorkerHeartbeat, actor string) bool {
	state := WorkerActive
	if req.Complete {
		state = WorkerCompleted
	}
	return cur.ID == req.Session && cur.Identity == req.Identity && cur.Generation == req.Generation &&
		cur.State == state && cur.Inflight == req.Inflight && cur.Uncertain == req.Uncertain && cur.Error == req.Error &&
		((!req.Resolve && cur.ResolvedBy == "" && cur.Attestation == "") ||
			(req.Resolve && cur.ResolvedBy == actor && cur.Attestation == req.Attestation))
}

func (s *Server) workerHeld(key string) bool {
	p, _, err := s.placementOf(key)
	return err != nil || p.Barrier != nil
}

func (s *Server) runExternalMover(tr *tracker, key string, _ MoverRequest) (MoverResult, error) {
	for {
		if err := tr.check(); err != nil {
			return MoverResult{}, err
		}
		op, err := s.ops().Get(tr.ctx, tr.id())
		if err != nil {
			return MoverResult{}, err
		}
		if op == nil {
			return MoverResult{}, fmt.Errorf("%w: operation %s", ErrUnknownOperation, tr.id())
		}
		if op.Terminal() || op.Node != s.node() || op.OwnerTerm != tr.snapshot().OwnerTerm {
			tr.mu.Lock()
			tr.lost = true
			tr.mu.Unlock()
			return MoverResult{}, tr.check()
		}
		// An operator can resolve the session once it is past its TTL (resolve-worker).
		if s.workerResolvable(op) == nil {
			tr.allow([]string{ActionResolveWorker})
		} else {
			tr.allow(nil)
		}
		worker := op.Worker
		switch {
		case worker == nil:
			tr.blocked([]Blocker{{Code: BlockerWorkerUnresolved, Message: "the external mover has not enrolled its worker session"}})
		case worker.State == WorkerCompleted:
			tr.blocked(nil)
			if worker.Error != "" {
				return MoverResult{}, fmt.Errorf("external mover: %s", worker.Error)
			}
			s.mu.Lock()
			p := s.progress[key]
			s.mu.Unlock()
			return MoverResult{Key: key, Passes: p.Pass, Copied: p.Copied, Skipped: p.Skipped,
				Vanished: p.Vanished, Failed: p.Failed, Bytes: p.Bytes, Converged: p.Converged}, nil
		case worker.State == WorkerUnresolved || s.now().Sub(worker.LastSeen) > s.workerTTL():
			tr.blocked([]Blocker{{Code: BlockerWorkerUnresolved, Count: worker.Inflight + worker.Uncertain,
				Message: fmt.Sprintf("worker session %s expired with its work unresolved; once its backend effects are known to have ended, `shunt operation resolve-worker %s --session %s --attest <why>`", worker.ID, tr.id(), worker.ID)}})
		case worker.Uncertain > 0:
			tr.blocked([]Blocker{{Code: BlockerBackendOutcomeUnknown, Count: worker.Uncertain,
				Message: fmt.Sprintf("worker session %s has backend effects whose outcome it did not learn", worker.ID)}})
		default:
			tr.blocked([]Blocker{{Code: BlockerOldRequests, Count: worker.Inflight,
				Message: fmt.Sprintf("worker session %s is still copying from the source", worker.ID)}})
		}
		if err := s.sleep(tr.ctx, s.fencePoll()); err != nil {
			return MoverResult{}, err
		}
	}
}
