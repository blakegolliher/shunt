package control

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	moveengine "github.com/blakegolliher/shunt/internal/mover"
)

const maxMoverPasses = 100

// MoverRequest starts the copy worker for one placement. The worker always reports every pass;
// UntilConverged repeats until a completed pass copies and fails nothing.
type MoverRequest struct {
	UntilConverged        bool `json:"until_converged,omitempty"`
	MaxPasses             int  `json:"max_passes,omitempty"`
	AcceptLostWriteWindow bool `json:"accept_lost_write_window,omitempty"`
}

// MoverRange is one bounded work range as shown by the UI. The current mover has one ordered
// all-keys range; P3d can split it without changing this read model.
type MoverRange struct {
	Name     string `json:"name"`
	Cursor   string `json:"cursor,omitempty"`
	Done     int64  `json:"done"`
	Total    int64  `json:"total,omitempty"`
	Complete bool   `json:"complete"`
}

// MoverResult is the operation outcome, separate from the last Progress report retained for the
// placement view.
type MoverResult struct {
	Key       string `json:"key"`
	Passes    int    `json:"passes"`
	Copied    int    `json:"copied"`
	Skipped   int    `json:"skipped"`
	Vanished  int    `json:"vanished"`
	Failed    int    `json:"failed"`
	Bytes     int64  `json:"bytes"`
	Converged bool   `json:"converged"`
}

// MoverRunner is the existing copy engine installed at the process edge. report publishes
// bucket-level counts and a cursor only; object ledger entries remain append-only outside the
// directory and operation store.
type MoverRunner func(ctx context.Context, key string, req MoverRequest, report func(Progress)) (MoverResult, error)

// MoverLedgerReader returns a bounded newest-first-visible tail of the append-only object ledger.
type MoverLedgerReader func(key string, limit int) ([]moveengine.LedgerEntry, error)

// MoverWorker adapts the shared copy engine to a control Server. Snapshot must return a safe
// current directory copy; Secrets resolves control: refs on a control node and may be nil in a lab.
type MoverWorker struct {
	Snapshot     func() *directory.File
	Secrets      func() map[string]string
	CursorDir    string
	LedgerDir    string
	LedgerBucket string
}

// Run implements MoverRunner.
func (w MoverWorker) Run(ctx context.Context, key string, req MoverRequest, report func(Progress)) (MoverResult, error) {
	if w.Snapshot == nil {
		return MoverResult{}, fmt.Errorf("%w: the mover has no directory source", ErrUnavailable)
	}
	var secrets map[string]string
	if w.Secrets != nil {
		secrets = w.Secrets()
	}
	res, err := moveengine.Run(ctx, w.Snapshot(), secrets, moveengine.Options{
		Key: key, AcceptLostWriteWindow: req.AcceptLostWriteWindow,
		UntilConverged: req.UntilConverged, MaxPasses: req.MaxPasses,
		Paths: moveengine.Paths{CursorDir: w.CursorDir, LedgerDir: w.LedgerDir, LedgerBucket: w.LedgerBucket},
	}, func(p moveengine.Progress) {
		report(Progress{Source: p.Source, Primary: p.Primary, Pass: p.Pass, Copied: p.Copied,
			Skipped: p.Skipped, Vanished: p.Vanished, Failed: p.Failed, Bytes: p.Bytes,
			LastKey: p.LastKey, Done: p.Done, Converged: p.Converged})
	})
	return MoverResult{Key: res.Key, Passes: res.Passes, Copied: res.Copied, Skipped: res.Skipped,
		Vanished: res.Vanished, Failed: res.Failed, Bytes: res.Bytes, Converged: res.Converged}, err
}

// Ledger implements MoverLedgerReader.
func (w MoverWorker) Ledger(key string, limit int) ([]moveengine.LedgerEntry, error) {
	return moveengine.LedgerTail(moveengine.Paths{LedgerDir: w.LedgerDir}, key, limit)
}

func (s *Server) moverLedger(w http.ResponseWriter, r *http.Request) {
	key, _, _, ok := s.lookup(w, r)
	if !ok {
		return
	}
	if s.MoverLedger == nil {
		fail(w, fmt.Errorf("%w: this control node has no mover ledger", ErrUnavailable))
		return
	}
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		var err error
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 500 {
			fail(w, bad("limit %q: want 1 through 500", raw))
			return
		}
	}
	entries, err := s.MoverLedger(key, limit)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Entries []moveengine.LedgerEntry `json:"entries"`
	}{Entries: entries})
}

func (s *Server) checkMover(key string, req MoverRequest) error {
	p, f, err := s.placementOf(key)
	if err != nil {
		return err
	}
	if p.State != directory.StateMigrating && !(p.State == directory.StateRamping && p.Ramp != nil && p.Ramp.Ratio >= 1) {
		return refuse("%s is %s: the mover runs on a MIGRATING placement, or a RAMPING one at ratio 1", key, p.State)
	}
	caps := f.Clusters[p.Primary].Capabilities
	if caps.ConditionalWrite == nil && !req.AcceptLostWriteWindow {
		return refuse("%s: target cluster %s has an assumed conditional-write profile; measure it with expand before copying, or explicitly accept the ADR-0004 lost-write window", key, p.Primary)
	}
	if !caps.ConditionalWriteOr(true) && !req.AcceptLostWriteWindow {
		return refuse("%s", migrate.RefuseLostWriteWindow(key, p.Primary))
	}
	if s.Mover == nil {
		return fmt.Errorf("%w: this control node has no mover worker", ErrUnavailable)
	}
	return nil
}

func (s *Server) runMover(tr *tracker, key string, req MoverRequest) (MoverResult, error) {
	if err := s.checkMover(key, req); err != nil {
		return MoverResult{}, err
	}
	if req.MaxPasses == 0 {
		req.MaxPasses = 10
	}
	if !req.UntilConverged {
		req.MaxPasses = 1
	}
	tr.phase(PhaseMover)
	return s.Mover(tr.ctx, key, req, func(p Progress) {
		p.UpdatedAt = s.now().UTC()
		if len(p.Ranges) == 0 {
			p.Ranges = []MoverRange{{Name: "all keys", Cursor: p.LastKey,
				Done: int64(p.Copied + p.Skipped + p.Vanished + p.Failed), Complete: p.Done}}
		}
		s.setMoverProgress(key, p)
		done := int64(p.Copied + p.Skipped + p.Vanished + p.Failed)
		var total int64
		for _, r := range p.Ranges {
			total += r.Total
		}
		tr.progress(done, total, "objects")
	})
}

func (s *Server) setMoverProgress(key string, p Progress) {
	s.mu.Lock()
	if s.progress == nil {
		s.progress = map[string]Progress{}
	}
	s.progress[key] = p
	s.mu.Unlock()
}
