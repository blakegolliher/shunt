package control

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// ReadOnlyRequest changes a maintenance switch. Reject makes writes fail fast with 403 instead
// of the default retryable 503; Wait bounds each fleet fence.
type ReadOnlyRequest struct {
	ReadOnly bool   `json:"read_only"`
	Reject   bool   `json:"reject,omitempty"`
	Wait     string `json:"wait,omitempty"`
}

// ReadOnlyResult reports a read-only change (ADR-0021 D2). Desired is the flag as written;
// Effective says whether the change is in force: switching on, that every proxy has drained the
// mutations it admitted before the flag (the drain barrier committed); switching off, that every
// live proxy has installed it. WaitingOn are live members that have not installed the commit
// within the wait; Silent, members past their lease.
type ReadOnlyResult struct {
	Target    string   `json:"target"`
	ReadOnly  bool     `json:"read_only"`
	Reject    bool     `json:"reject"`
	Desired   bool     `json:"desired"`
	Effective bool     `json:"effective"`
	Version   int64    `json:"version"`
	Proxies   int      `json:"proxies"`
	WaitingOn []string `json:"waiting_on,omitempty"`
	Silent    []string `json:"silent,omitempty"`
	Operation string   `json:"operation"`
}

// runReadOnly runs a read-only change on a placement or a cluster. Switching on writes the flag
// with a drain barrier: proxies refuse new mutations as soon as they install it, and the change
// is effective once every proxy has drained the ones it admitted before. Switching off is
// generation-checked through the same record and settles like any change.
func (s *Server) runReadOnly(tr *tracker, target, scope string, req ReadOnlyRequest,
	set func(ctx context.Context, readOnly, reject bool, barrier string) error,
) (ReadOnlyResult, error) {
	wait, _ := parseWait(req.Wait)
	res := ReadOnlyResult{Target: target, ReadOnly: req.ReadOnly, Reject: req.ReadOnly && req.Reject, Desired: req.ReadOnly, Operation: tr.id()}
	if !req.ReadOnly {
		if err := s.precondition(tr); err != nil {
			return ReadOnlyResult{}, err
		}
		tr.phase(PhaseStep)
		// The phase write orders this step after any cancellation of the record.
		if err := tr.check(); err != nil {
			return ReadOnlyResult{}, err
		}
		if err := set(tr.ctx, false, false, ""); err != nil {
			return ReadOnlyResult{}, err
		}
		res.Version = s.Dir.Snapshot().Version()
		s.settleReadOnly(tr, &res, wait)
		res.Effective = len(res.WaitingOn) == 0
		return res, nil
	}
	// Switching on is the hold: publish the desired flag and its barrier at once. A member that is
	// behind or silent is then a drain blocker on that durable intent; waiting for it as a
	// precondition would leave the operator-visible desired state false and provide no hold to
	// resume after an owner loss (T08).
	b := &barrier{scope: scope, kind: config.BarrierMutations,
		hold: func() (int64, error) {
			if err := s.Dir.Sync(tr.ctx); err != nil {
				return 0, err
			}
			if ro, bar := s.readOnlyOf(scope); ro && bar != nil && bar.ID == tr.id() {
				return s.Dir.Snapshot().Version(), nil
			}
			if err := set(tr.ctx, true, req.Reject, tr.id()); err != nil {
				return 0, err
			}
			return s.Dir.Snapshot().Version(), nil
		},
		commit: func() (int64, bool, error) {
			if err := s.Dir.Sync(tr.ctx); err != nil {
				return 0, false, err
			}
			ro, bar := s.readOnlyOf(scope)
			if bar == nil || bar.ID != tr.id() {
				if ro {
					return s.Dir.Snapshot().Version(), true, nil // committed by an earlier attempt
				}
				return 0, false, errBarrierGone
			}
			if err := s.Dir.ClearBarrier(tr.ctx, scope, tr.id(), tr.actor); err != nil {
				return 0, false, err
			}
			return s.Dir.Snapshot().Version(), false, nil
		}}
	if err := s.runBarrier(tr, b); err != nil {
		return ReadOnlyResult{}, err
	}
	res.Version, res.Effective = tr.barrierState().CommitVersion, true
	s.settleReadOnly(tr, &res, wait)
	return res, nil
}

// readOnlyOf reads a scope's read-only flag and barrier as they stand.
func (s *Server) readOnlyOf(scope string) (readOnly bool, b *directory.Barrier) {
	f := s.Dir.Snapshot().File()
	if name, ok := strings.CutPrefix(scope, "cluster:"); ok {
		c := f.Clusters[name]
		return c.ReadOnly, c.Barrier
	}
	p := f.Placements[strings.TrimPrefix(scope, "placement:")]
	return p.ReadOnly, p.Barrier
}

// settleReadOnly waits for the version to reach every live member.
func (s *Server) settleReadOnly(tr *tracker, res *ReadOnlyResult, wait time.Duration) {
	var tres TransitionResult
	s.settle(tr, &tres, res.Version, wait)
	res.WaitingOn, res.Silent, res.Proxies = tres.WaitingOn, tres.Silent, tres.Proxies
}

func (s *Server) runClusterReadOnly(tr *tracker, name string, req ReadOnlyRequest) (ReadOnlyResult, error) {
	res, err := s.runReadOnly(tr, "cluster:"+name, directory.ClusterResource(name), req, func(ctx context.Context, readOnly, reject bool, barrier string) error {
		return s.Dir.SetClusterReadOnly(ctx, name, readOnly, reject, barrier, tr.actor)
	})
	if err != nil {
		return res, err
	}
	s.info(tr.actor, "cluster read-only changed", "cluster", name, "read_only", res.ReadOnly, "reject", res.Reject, "effective", res.Effective, "version", res.Version)
	return res, nil
}

func (s *Server) runPlacementReadOnly(tr *tracker, key string, req ReadOnlyRequest) (ReadOnlyResult, error) {
	tenant, bucket, _ := directory.SplitKey(key)
	res, err := s.runReadOnly(tr, "placement:"+key, directory.PlacementResource(key), req, func(ctx context.Context, readOnly, reject bool, barrier string) error {
		return s.Dir.SetPlacementReadOnly(ctx, tenant, bucket, readOnly, reject, barrier, tr.actor)
	})
	if err != nil {
		return res, err
	}
	s.info(tr.actor, "placement read-only changed", "placement", key, "read_only", res.ReadOnly, "reject", res.Reject, "effective", res.Effective, "version", res.Version)
	return res, nil
}

func (s *Server) clusterReadOnly(w http.ResponseWriter, r *http.Request) {
	var req ReadOnlyRequest
	if !decode(w, r, &req) {
		return
	}
	s.serveOperation(w, r, OperationRequest{Kind: OpClusterReadOnly, Cluster: r.PathValue("name")}, req)
}

func (s *Server) placementReadOnly(w http.ResponseWriter, r *http.Request) {
	var req ReadOnlyRequest
	if !decode(w, r, &req) {
		return
	}
	s.serveOperation(w, r, OperationRequest{Kind: OpPlacementReadOnly, Placement: pathKey(r)}, req)
}
