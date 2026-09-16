package control

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// Server is the control API. Every mutation goes through Dir's write path, so it is serialized
// with every other writer and appended to the change log.
type Server struct {
	Dir      *directory.FileDir
	Clusters *upstream.Registry
	Metrics  *telemetry.Metrics
	// Token is the bearer token every request must carry. Empty: loopback peers only.
	Token string
	Log   *slog.Logger
	// Now defaults to time.Now; Sleep to a context-aware wait. Tests replace both.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error

	mu       sync.Mutex
	progress map[string]Progress // placement key → the mover's last report (in memory only)
}

// Handler returns the /v1/ routes, one handler per operation.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("GET /v1/placements/{tenant}/{bucket}", s.placement)
	mux.HandleFunc("POST /v1/clusters", s.putCluster)
	mux.HandleFunc("DELETE /v1/clusters/{name}", s.removeCluster)
	mux.HandleFunc("POST /v1/tenants/{tenant}/default-cluster", s.setTenantDefault)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/adopt", s.adopt)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/expand", s.expand)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/ramp", s.ramp)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/migrate", s.migrateStart)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/mover-progress", s.moverProgress)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/cutover", s.cutover)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/purge-source", s.purgeSource)
	mux.HandleFunc("POST /v1/placements/{tenant}/{bucket}/finish", s.finish)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "the control API needs Authorization: Bearer <admin.control_token_ref>, or a loopback peer when no token is configured")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) authorized(r *http.Request) bool {
	if s.Token != "" {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		return ok && subtle.ConstantTimeCompare([]byte(got), []byte(s.Token)) == 1
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Server) sleep(ctx context.Context, d time.Duration) error {
	if s.Sleep != nil {
		return s.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func actor(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "api:" + host
}

// Error is the body of every non-2xx answer.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, Error{Code: code, Message: msg})
}

// refusal is an operator action the rules forbid right now; it answers 409 with its reason.
type refusal struct{ msg string }

func (r *refusal) Error() string { return r.msg }

func refuse(format string, a ...any) error { return &refusal{msg: fmt.Sprintf(format, a...)} }

// fail maps an error to its HTTP answer.
func fail(w http.ResponseWriter, err error) {
	var (
		ref *refusal
		te  *directory.TransitionError
		ce  *config.Error
	)
	switch {
	case errors.As(err, &ref), errors.Is(err, directory.ErrInUse), errors.As(err, &te):
		writeError(w, http.StatusConflict, "refused", err.Error())
	case errors.Is(err, directory.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, directory.ErrExists), errors.Is(err, directory.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.As(err, &ce):
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, directory.ErrReadOnly), errors.Is(err, directory.ErrLockTimeout):
		writeError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
	default:
		writeError(w, http.StatusBadGateway, "backend", err.Error())
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields() // a cluster's inline "secret" is an unknown field, and refused
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "request body: "+err.Error())
		return false
	}
	return true
}

// ClusterStatus is one cluster as status reports it: the secret_ref, never a secret.
type ClusterStatus struct {
	Name              string   `json:"name"`
	Type              string   `json:"type"`
	Scheme            string   `json:"scheme"`
	Region            string   `json:"region"`
	Endpoints         []string `json:"endpoints"`
	AccessKey         string   `json:"access_key"`
	SecretRef         string   `json:"secret_ref"`
	ConditionalWrite  bool     `json:"conditional_write"`
	ConditionalDelete bool     `json:"conditional_delete"`
	References        []string `json:"references,omitempty"`
}

// PlacementStatus is one placement with the migration signals an operator watches.
type PlacementStatus struct {
	Key           string                     `json:"key"`
	State         string                     `json:"state"`
	Primary       string                     `json:"primary"`
	Source        string                     `json:"source,omitempty"`
	Target        string                     `json:"target,omitempty"`
	Names         map[string]string          `json:"names"`
	Ratio         float64                    `json:"ratio,omitempty"`
	Prefixes      []string                   `json:"prefixes,omitempty"`
	Writes        map[string]float64         `json:"ramp_writes"` // side → shunt_ramp_writes_total on this proxy
	FallbackReads float64                    `json:"fallback_reads"`
	DualDeletes   map[string]float64         `json:"dual_deletes"`
	Cutover       *directory.CutoverEvidence `json:"cutover,omitempty"`
	Mover         *Progress                  `json:"mover,omitempty"`
}

// Status is the answer to GET /v1/status.
type Status struct {
	Version    int64             `json:"version"`
	Clusters   []ClusterStatus   `json:"clusters"`
	Placements []PlacementStatus `json:"placements"`
}

// status lists every cluster, and every placement that is moving or expanded (all of them with
// ?all=1), or the one ?bucket=t/b names.
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	snap := s.Dir.Snapshot()
	f := snap.File()
	out := Status{Version: f.Version, Clusters: []ClusterStatus{}, Placements: []PlacementStatus{}}
	for _, name := range sortedKeys(f.Clusters) {
		c := f.Clusters[name]
		out.Clusters = append(out.Clusters, ClusterStatus{
			Name: name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, Endpoints: endpoints(c),
			AccessKey: c.Credentials.AccessKey, SecretRef: c.Credentials.SecretRef,
			ConditionalWrite: c.Capabilities.ConditionalWriteOr(true), ConditionalDelete: c.Capabilities.ConditionalDeleteOr(false),
			References: directory.References(f, name),
		})
	}
	want := r.URL.Query().Get("bucket")
	if want != "" {
		if _, ok := f.Placements[want]; !ok {
			fail(w, fmt.Errorf("%s: %w", want, directory.ErrNotFound))
			return
		}
	}
	all := r.URL.Query().Get("all") != ""
	for _, key := range sortedKeys(f.Placements) {
		p := f.Placements[key]
		if (want != "" && key != want) || (want == "" && !all && p.State == directory.StateActive && p.Target == "") {
			continue
		}
		out.Placements = append(out.Placements, s.placementStatus(key, p))
	}
	writeJSON(w, http.StatusOK, out)
}

func endpoints(c config.Cluster) []string {
	if c.EndpointMode == "dns" {
		return []string{c.Endpoint}
	}
	return c.Endpoints
}

func (s *Server) placementStatus(key string, p directory.Placement) PlacementStatus {
	ps := PlacementStatus{Key: key, State: p.State, Primary: p.Primary, Source: p.Source, Target: p.Target, Names: p.Names,
		Cutover: p.Cutover, Writes: map[string]float64{}, DualDeletes: map[string]float64{}}
	if p.Ramp != nil {
		ps.Ratio, ps.Prefixes = p.Ramp.Ratio, p.Ramp.Prefixes
	}
	if s.Metrics != nil {
		ps.Writes = counters(s.Metrics.RampWrites, key, "side")
		ps.FallbackReads = counters(s.Metrics.FallbackReads, key, "")[""]
		ps.DualDeletes = counters(s.Metrics.DualDelete, key, "outcome")
	}
	s.mu.Lock()
	if pr, ok := s.progress[key]; ok {
		ps.Mover = &pr
	}
	s.mu.Unlock()
	return ps
}

// counters reads a counter vector's series for one bucket without creating any, keyed by the
// value of label (or "" when label is empty).
func counters(vec *prometheus.CounterVec, bucket, label string) map[string]float64 {
	out := map[string]float64{}
	ch := make(chan prometheus.Metric, 32)
	go func() {
		vec.Collect(ch)
		close(ch)
	}()
	for m := range ch {
		var d dto.Metric
		if m.Write(&d) != nil {
			continue
		}
		var b, v string
		for _, lp := range d.GetLabel() {
			switch lp.GetName() {
			case "bucket":
				b = lp.GetValue()
			case label:
				v = lp.GetValue()
			}
		}
		if b == bucket {
			out[v] += d.GetCounter().GetValue()
		}
	}
	return out
}

// PlacementDetail is the answer to GET /v1/placements/{tenant}/{bucket}: what an out-of-process
// mover needs to copy it.
type PlacementDetail struct {
	Key       string                    `json:"key"`
	Placement directory.Placement       `json:"placement"`
	Clusters  map[string]config.Cluster `json:"clusters"`
}

func (s *Server) placement(w http.ResponseWriter, r *http.Request) {
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	d := PlacementDetail{Key: key, Placement: p, Clusters: map[string]config.Cluster{}}
	for _, name := range []string{p.Primary, p.Source, p.Target} {
		if c, found := f.Clusters[name]; found {
			d.Clusters[name] = c
		}
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) lookup(w http.ResponseWriter, r *http.Request) (string, directory.Placement, *directory.File, bool) {
	key := directory.Key(r.PathValue("tenant"), r.PathValue("bucket"))
	f := s.Dir.Snapshot().File()
	p, ok := f.Placements[key]
	if !ok {
		fail(w, fmt.Errorf("%s: %w", key, directory.ErrNotFound))
		return key, p, f, false
	}
	return key, p, f, true
}

// ClusterRequest adds or replaces a cluster.
type ClusterRequest struct {
	Name    string         `json:"name"`
	Cluster config.Cluster `json:"cluster"`
}

func (s *Server) putCluster(w http.ResponseWriter, r *http.Request) {
	var req ClusterRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "name is required")
		return
	}
	if err := s.Dir.PutCluster(r.Context(), req.Name, req.Cluster, actor(r)); err != nil {
		var ce *config.Error
		if errors.As(err, &ce) || strings.Contains(err.Error(), "clusters.") {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		if !isDirectoryError(err) {
			// Prepare refused it: the proxy could not build the cluster, usually an unresolvable secret_ref.
			writeError(w, http.StatusConflict, "refused", "the proxy cannot use this cluster: "+err.Error())
			return
		}
		fail(w, err)
		return
	}
	c, _ := s.Dir.Snapshot().Cluster(req.Name)
	writeJSON(w, http.StatusOK, ClusterStatus{Name: req.Name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, Endpoints: c.Endpoints,
		AccessKey: c.Credentials.AccessKey, SecretRef: c.Credentials.SecretRef,
		ConditionalWrite: c.Capabilities.ConditionalWriteOr(true), ConditionalDelete: c.Capabilities.ConditionalDeleteOr(false)})
}

func isDirectoryError(err error) bool {
	for _, target := range []error{directory.ErrNotFound, directory.ErrExists, directory.ErrInUse, directory.ErrConflict, directory.ErrReadOnly, directory.ErrLockTimeout} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (s *Server) removeCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.Dir.RemoveCluster(r.Context(), name, actor(r)); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"removed": name})
}

// TenantDefaultRequest repoints the cluster a tenant's new buckets land on.
type TenantDefaultRequest struct {
	Cluster string `json:"cluster"`
}

func (s *Server) setTenantDefault(w http.ResponseWriter, r *http.Request) {
	var req TenantDefaultRequest
	if !decode(w, r, &req) {
		return
	}
	tenant := r.PathValue("tenant")
	if err := s.Dir.SetTenantDefault(r.Context(), tenant, req.Cluster, actor(r)); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant": tenant, "default_cluster": req.Cluster, "version": s.Dir.Snapshot().Version()})
}

// backendFor returns the live cluster a placement role names.
func (s *Server) backendFor(name string) (backend, error) {
	cl, ok := s.Clusters.Load().Get(name)
	if !ok {
		return backend{}, fmt.Errorf("%w: cluster %q is not live in this proxy", directory.ErrNotFound, name)
	}
	return backend{cl: cl}, nil
}

// refuseVersioned fails closed unless bucket's versioning was never enabled (docs/DESIGN.md
// decision 12): version history cannot be carried across.
func refuseVersioned(ctx context.Context, b backend, role, bucket string) error {
	status, err := b.versioning(ctx, bucket)
	switch {
	case err != nil:
		return refuse("refused: cannot read versioning of %s bucket %s on %s: %v", role, bucket, b.cl.Name, err)
	case status != "":
		return refuse("refused: %s bucket %s on %s has versioning %s; versioned buckets cannot be migrated (docs/DESIGN.md §9 item 4)", role, bucket, b.cl.Name, status)
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// lostWriteWindow is the refusal migrate start and the mover give for a target without
// conditional PUT (ADR-0004 race 2).
func lostWriteWindow(key, cluster string) error {
	return &refusal{msg: migrate.RefuseLostWriteWindow(key, cluster).Error()}
}
