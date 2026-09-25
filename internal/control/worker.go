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

	// The owner normally receives the heartbeat. Updating its tracker keeps phase/progress writes
	// and worker sequence in one CAS stream. A different control node falls back to the durable
	// record below; the owner's next write merges that worker-only update.
	s.mu.Lock()
	tr := s.running[id]
	s.mu.Unlock()
	if tr != nil {
		worker, hold, err := tr.applyWorkerHeartbeat(req, actor(r))
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, WorkerHeartbeatAnswer{Worker: worker, Hold: hold})
		return
	}

	for range 8 {
		op, err := s.ops().Get(r.Context(), id)
		if err != nil {
			fail(w, err)
			return
		}
		if op == nil {
			fail(w, fmt.Errorf("%w: operation %s", ErrUnknownOperation, id))
			return
		}
		worker, hold, err := s.nextWorker(op, req, actor(r))
		if err != nil {
			fail(w, err)
			return
		}
		op.Worker = &worker
		op.Sequence++
		op.Updated = s.now().UTC()
		if err = s.ops().Update(context.WithoutCancel(r.Context()), op); err == nil {
			writeJSON(w, http.StatusOK, WorkerHeartbeatAnswer{Worker: worker, Hold: hold})
			return
		}
		if !errors.Is(err, ErrStaleSequence) {
			fail(w, err)
			return
		}
	}
	fail(w, fmt.Errorf("%w: operation %s kept changing while its worker heartbeated", ErrUnavailable, id))
}

func (tr *tracker) applyWorkerHeartbeat(req WorkerHeartbeat, actor string) (WorkerSession, bool, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	worker, hold, err := tr.s.nextWorker(&tr.op, req, actor)
	if err != nil {
		return WorkerSession{}, false, err
	}
	tr.op.Worker = &worker
	tr.put()
	return worker, hold, nil
}

func (s *Server) nextWorker(op *Operation, req WorkerHeartbeat, actor string) (WorkerSession, bool, error) {
	if op.Kind != OpMover || op.Scope == nil {
		return WorkerSession{}, false, refuse("operation %s is not a mover operation", op.ID)
	}
	var args MoverRequest
	if json.Unmarshal(op.Args, &args) != nil || !args.External || args.Session != req.Session {
		return WorkerSession{}, false, refuse("operation %s is not enrolled for worker session %s", op.ID, req.Session)
	}
	if req.Identity != op.Identity || req.Generation != op.Scope.Generation {
		return WorkerSession{}, false, refuse("worker session %s is for another directory lineage or placement generation", req.Session)
	}
	now := s.now().UTC()
	cur := op.Worker
	expired := cur != nil && cur.State == WorkerActive && now.Sub(cur.LastSeen) > s.workerTTL()
	if cur != nil && (cur.State == WorkerUnresolved || expired) && !req.Resolve {
		return WorkerSession{}, false, refuse("worker session %s expired with work unresolved; reconcile it with resolve and an attestation", req.Session)
	}
	if cur != nil {
		if req.Sequence < cur.Sequence {
			return WorkerSession{}, false, refuse("worker session %s heartbeat sequence %d is behind %d", req.Session, req.Sequence, cur.Sequence)
		}
		if req.Sequence == cur.Sequence {
			state := WorkerActive
			if req.Complete {
				state = WorkerCompleted
			}
			exact := cur.ID == req.Session && cur.Identity == req.Identity && cur.Generation == req.Generation &&
				cur.State == state && cur.Inflight == req.Inflight && cur.Uncertain == req.Uncertain && cur.Error == req.Error &&
				((!req.Resolve && cur.ResolvedBy == "" && cur.Attestation == "") ||
					(req.Resolve && cur.ResolvedBy == actor && cur.Attestation == req.Attestation))
			if !exact {
				return WorkerSession{}, false, refuse("worker session %s heartbeat sequence %d was already used with different state", req.Session, req.Sequence)
			}
			return *cur, s.workerHeld(op.Placement), nil
		}
	}
	state := WorkerActive
	if req.Complete {
		state = WorkerCompleted
	}
	worker := WorkerSession{ID: req.Session, Identity: req.Identity, Generation: req.Generation,
		Sequence: req.Sequence, State: state, Inflight: req.Inflight, Uncertain: req.Uncertain,
		LastSeen: now, Error: req.Error}
	if req.Complete {
		worker.CompletedAt = now
	}
	if req.Resolve {
		worker.ResolvedBy, worker.Attestation = actor, req.Attestation
	}
	return worker, s.workerHeld(op.Placement), nil
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
				Message: fmt.Sprintf("worker session %s expired; resume and explicitly reconcile it before source cleanup", worker.ID)}})
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
