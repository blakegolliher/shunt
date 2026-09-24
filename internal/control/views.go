package control

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// Read models for a browser (ADR-0017): what a screen shows, in one answer, with no secret in it.

// Capability is one measured or assumed backend capability: Known when the cluster definition
// states it (set by the operator, or measured by expand); assumed otherwise.
type Capability struct {
	Value bool `json:"value"`
	Known bool `json:"known"`
}

// Probe is one reachability check of a cluster, made for the view that shows it.
type Probe struct {
	Reachable bool      `json:"reachable"`
	LatencyMS float64   `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

// ClusterView is GET /v1/clusters/{name}/view.
type ClusterView struct {
	ClusterStatus
	Capabilities struct {
		ConditionalWrite  Capability `json:"conditional_write"`
		ConditionalDelete Capability `json:"conditional_delete"`
	} `json:"capabilities"`
	Probe Probe `json:"probe"`
}

func capability(v *bool, def bool) Capability {
	if v == nil {
		return Capability{Value: def}
	}
	return Capability{Value: *v, Known: true}
}

func clusterStatus(f *directory.File, name string, c config.Cluster) ClusterStatus {
	profile := "assumed"
	if c.Capabilities.ConditionalWrite != nil && c.Capabilities.ConditionalDelete != nil {
		profile = "measured"
	}
	return ClusterStatus{
		Name: name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, Endpoints: endpoints(c),
		AccessKey: c.Credentials.AccessKey, SecretRef: c.Credentials.SecretRef,
		ConditionalWrite: c.Capabilities.ConditionalWriteOr(true), ConditionalDelete: c.Capabilities.ConditionalDeleteOr(false),
		ConditionalWriteKnown: c.Capabilities.ConditionalWrite != nil, ConditionalDeleteKnown: c.Capabilities.ConditionalDelete != nil,
		CapabilityProfile: profile,
		References:        directory.References(f, name), ReadOnly: c.ReadOnly, RejectWrites: c.RejectWrites,
	}
}

func (s *Server) clusterView(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	f := s.Dir.Snapshot().File()
	c, ok := f.Clusters[name]
	if !ok {
		fail(w, notFound("no cluster %q in the directory", name))
		return
	}
	view := ClusterView{ClusterStatus: clusterStatus(f, name, c)}
	view.Capabilities.ConditionalWrite = capability(c.Capabilities.ConditionalWrite, true)
	view.Capabilities.ConditionalDelete = capability(c.Capabilities.ConditionalDelete, false)
	view.Probe = s.probe(r.Context(), name)
	writeJSON(w, http.StatusOK, view)
}

// probe signs one ListBuckets to a cluster, as cluster add does to check credentials, and times it.
func (s *Server) probe(ctx context.Context, name string) Probe {
	p := Probe{CheckedAt: s.now().UTC()}
	b, err := s.backendFor(name)
	if err != nil {
		p.Error = err.Error()
		return p
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	start := time.Now()
	reply, err := b.do(ctx, http.MethodGet, "", "", nil, nil, nil)
	p.LatencyMS = float64(time.Since(start).Microseconds()) / 1000
	switch {
	case err != nil:
		p.Error = err.Error()
	case reply.status >= 500:
		p.Error = "HTTP " + strconv.Itoa(reply.status) + " " + reply.code
	default:
		p.Reachable = true
	}
	return p
}

// FenceStatus is where a placement's last change stands in the fleet (ADR-0016).
type FenceStatus struct {
	Version   int64    `json:"version"`    // the directory version now
	Held      bool     `json:"held"`       // a step is written as a hold and not yet completed
	Proxies   int      `json:"proxies"`    // live members
	WaitingOn []string `json:"waiting_on"` // live members that have not installed the version
	Silent    []string `json:"silent"`     // members past their lease
}

// PlacementView is GET /v1/placements/{tenant}/{bucket}/view.
type PlacementView struct {
	PlacementStatus
	Fence FenceStatus `json:"fence"`
	// SourceUploadsInFlight counts multipart uploads in progress on the source bucket, which
	// cutover must not cut off; nil without a source, or when the source could not be asked.
	SourceUploadsInFlight *int   `json:"source_uploads_in_flight"`
	SourceUploadsError    string `json:"source_uploads_error,omitempty"`
	// Operations lists the running operation records on this placement.
	Operations []string                 `json:"operations"`
	Clusters   map[string]ClusterStatus `json:"clusters"`
}

func (s *Server) placementView(w http.ResponseWriter, r *http.Request) {
	if err := s.flushLocalTelemetry(); err != nil {
		fail(w, err)
		return
	}
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	view := PlacementView{PlacementStatus: s.placementStatus(key, p), Operations: []string{}, Clusters: map[string]ClusterStatus{}}
	view.Fence = s.fenceStatus(r.Context(), p)
	if pv := moving(p); pv.Source != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		n, err := s.uploadsInProgress(ctx, pv.ClusterOf(pv.Source), pv.Names[pv.Source], moveScope(p))
		cancel()
		if err != nil {
			view.SourceUploadsError = err.Error()
		} else {
			view.SourceUploadsInFlight = &n
		}
	}
	if ids := s.runningOperations(r.Context(), key); ids != nil {
		view.Operations = ids
	}
	for _, name := range placementClusters(p) {
		if c, found := f.Clusters[name]; found {
			view.Clusters[name] = clusterStatus(f, name, c)
		}
	}
	writeJSON(w, http.StatusOK, view)
}

// fenceStatus reads the fleet once and says who has the current version.
func (s *Server) fenceStatus(ctx context.Context, p directory.Placement) FenceStatus {
	fs := FenceStatus{Version: s.Dir.Snapshot().Version(), Held: p.Held(), WaitingOn: []string{}, Silent: []string{}}
	ms, err := s.members(ctx)
	if err != nil {
		return fs
	}
	for _, m := range ms {
		switch {
		case !m.Live:
			fs.Silent = append(fs.Silent, m.ID)
		default:
			fs.Proxies++
			if m.Applied < fs.Version {
				fs.WaitingOn = append(fs.WaitingOn, m.ID)
			}
		}
	}
	return fs
}

// AuditPage is GET /v1/audit: change records, newest first.
type AuditPage struct {
	Changes []directory.Change `json:"changes"`
}

// audit is GET /v1/audit?limit=&before=: the last limit change records at or before version
// before (default: the current version).
func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := defaultListLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "bad_request", "limit: want a positive number")
			return
		}
		limit = min(n, maxListLimit)
	}
	var before int64
	if v := q.Get("before"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "bad_request", "before: want a directory version")
			return
		}
		before = n
	}
	changes, err := s.Dir.Changes(r.Context(), before, limit)
	if err != nil {
		fail(w, err)
		return
	}
	if changes == nil {
		changes = []directory.Change{}
	}
	writeJSON(w, http.StatusOK, AuditPage{Changes: changes})
}
