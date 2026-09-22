package control

import (
	"net/http"

	"github.com/blakegolliher/shunt/internal/directory"
)

// ReadOnlyRequest changes a maintenance switch. Reject makes writes fail fast with 403 instead
// of the default retryable 503; Wait bounds each fleet fence.
type ReadOnlyRequest struct {
	ReadOnly bool   `json:"read_only"`
	Reject   bool   `json:"reject,omitempty"`
	Wait     string `json:"wait,omitempty"`
}

// ReadOnlyResult reports the directory revision and whether every registered proxy installed it.
type ReadOnlyResult struct {
	Target    string   `json:"target"`
	ReadOnly  bool     `json:"read_only"`
	Reject    bool     `json:"reject"`
	Version   int64    `json:"version"`
	Proxies   int      `json:"proxies"`
	WaitingOn []string `json:"waiting_on,omitempty"`
	Silent    []string `json:"silent,omitempty"`
	Operation string   `json:"operation"`
}

func (s *Server) finishReadOnly(tr *tracker, res *ReadOnlyResult, wait string) {
	d, _ := parseWait(wait)
	tr.phase(PhaseSettle)
	res.WaitingOn, _ = s.fenceRound(tr, res.Version, true, d) // an absent old proxy must not keep writing
	if ms, err := s.members(tr.ctx); err == nil {
		for _, m := range ms {
			if m.Live {
				res.Proxies++
			} else {
				res.Silent = append(res.Silent, m.ID)
			}
		}
	}
}

func (s *Server) runClusterReadOnly(tr *tracker, name string, req ReadOnlyRequest) (ReadOnlyResult, error) {
	wait, _ := parseWait(req.Wait)
	if err := s.precondition(tr, true, wait); err != nil {
		return ReadOnlyResult{}, err
	}
	tr.phase(PhaseStep)
	if err := s.Dir.SetClusterReadOnly(tr.ctx, name, req.ReadOnly, req.Reject, tr.actor); err != nil {
		return ReadOnlyResult{}, err
	}
	res := ReadOnlyResult{Target: "cluster:" + name, ReadOnly: req.ReadOnly, Reject: req.ReadOnly && req.Reject,
		Version: s.Dir.Snapshot().Version(), Operation: tr.id()}
	s.finishReadOnly(tr, &res, req.Wait)
	s.info(tr.actor, "cluster read-only changed", "cluster", name, "read_only", res.ReadOnly, "reject", res.Reject, "version", res.Version)
	return res, nil
}

func (s *Server) runPlacementReadOnly(tr *tracker, key string, req ReadOnlyRequest) (ReadOnlyResult, error) {
	wait, _ := parseWait(req.Wait)
	if err := s.precondition(tr, true, wait); err != nil {
		return ReadOnlyResult{}, err
	}
	tenant, bucket, _ := directory.SplitKey(key)
	tr.phase(PhaseStep)
	if err := s.Dir.SetPlacementReadOnly(tr.ctx, tenant, bucket, req.ReadOnly, req.Reject, tr.actor); err != nil {
		return ReadOnlyResult{}, err
	}
	res := ReadOnlyResult{Target: "placement:" + key, ReadOnly: req.ReadOnly, Reject: req.ReadOnly && req.Reject,
		Version: s.Dir.Snapshot().Version(), Operation: tr.id()}
	s.finishReadOnly(tr, &res, req.Wait)
	s.info(tr.actor, "placement read-only changed", "placement", key, "read_only", res.ReadOnly, "reject", res.Reject, "version", res.Version)
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
