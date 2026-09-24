package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blakegolliher/shunt/internal/s3"

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
	Dir      directory.Store
	Clusters *upstream.Registry
	Metrics  *telemetry.Metrics
	// Token is the bearer token every request must carry. Empty: loopback peers only.
	Token string
	// SecretsDir is where a lab proxy writes a secret given to `cluster add`, one 0600 file per
	// cluster, and names it by a file: ref. Empty: the store keeps secrets itself (internal/cp).
	SecretsDir string
	// Keys holds the client keys (ADR-0012). nil: this shunt cannot import or list keys.
	Keys Keys
	// ClusterSecrets resolves the cluster secret_refs only the control plane can (control:<name>),
	// for GET /v1/directory and the mover. nil: every ref resolves on the reader's own host.
	ClusterSecrets func() map[string]string
	// Fleet is the fleet table (ADR-0016). nil: NoFleet, a single-node lab. LeaseTTL is the grant
	// every heartbeat answer carries, for the diagnostics.
	Fleet    Fleet
	LeaseTTL time.Duration
	// FencePoll is how often a fenced change re-reads the fleet; default 100ms.
	FencePoll time.Duration
	Log       *slog.Logger
	// Now defaults to time.Now; Sleep to a context-aware wait. Tests replace both.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	// Ops holds the operation records (ADR-0017). nil: in memory, for a lab.
	Ops Operations
	// Node names this control node in the records it owns; "lab" when empty.
	Node string
	// Events, if set, is the stream GET /v1/events serves.
	Events *Events
	// Telemetry holds the merged 60-minute summary ring. A lab server feeds it from the local
	// proxy collector; shunt-control feeds it from member heartbeats in PublishFleet.
	Telemetry *telemetry.Store
	// ConfirmKey keys the confirmation tokens dry runs issue; every control node shares one.
	// Empty: a random key for this process, so a lab's tokens die with it.
	ConfirmKey []byte
	// Mover runs the copy engine for a browser-started mover operation. It is installed at the
	// process edge and never runs in a proxy request handler.
	Mover MoverRunner
	// MoverLedger reads a bounded tail of the mover's append-only local ledger.
	MoverLedger MoverLedgerReader
	// Ctx is the server's lifetime: operations run on it, never on a request's. nil: Background.
	Ctx context.Context

	mu          sync.Mutex
	progress    map[string]Progress // placement key → the mover's last report (in memory only)
	prevFleet   []Member            // the fleet as of the last PublishFleet, for fleet events
	fleetSeeded bool
	opsOnce     sync.Once
	defaultOps  *MemOperations
	confirmOnce sync.Once
}

// Handler returns the /v1/ routes, one handler per operation, from the route table (routes.go).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		h := rt.h
		if rt.log != "" {
			h = s.logged(rt.log, h)
		}
		mux.HandleFunc(rt.Method+" "+rt.Pattern, h)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "the control API needs Authorization: Bearer <admin.control_token_ref>, or a loopback peer when no token is configured")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// Authorize wraps another handler in the control API's authentication, for routes a control node
// serves beside /v1/ under the same token (internal/cp).
func (s *Server) Authorize(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "the control API needs Authorization: Bearer <token>, or a loopback peer when no token is configured")
			return
		}
		h.ServeHTTP(w, r)
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
	if token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok && token != "" {
		sum := sha256.Sum256([]byte(token))
		return "token:" + hex.EncodeToString(sum[:6])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "api:" + host
}

// pathKey is the placement key a /v1/placements/{tenant}/{bucket} route names.
func pathKey(r *http.Request) string {
	return directory.Key(r.PathValue("tenant"), r.PathValue("bucket"))
}

// Error is the body of every non-2xx answer.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// CurrentIdentity is the control plane's directory lineage, on a lineage refusal.
	CurrentIdentity *directory.Identity `json:"current_identity,omitempty"`
	// OperationID is the unfinished operation in the way, on operation_conflict.
	OperationID string `json:"operation_id,omitempty"`
	// CurrentGeneration is the resource's generation now, on generation_conflict: a decimal string.
	CurrentGeneration string `json:"current_generation,omitempty"`
	// Retryable says whether the same request may succeed later unchanged: the control plane was
	// unavailable, at capacity, or another operation held the scope. Retry with the same
	// Idempotency-Key.
	Retryable bool `json:"retryable"`
	// Blockers name what stands in the way, on retirement_unproven.
	Blockers []Blocker `json:"blockers,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, Error{Code: code, Message: msg, Retryable: retryable(code)})
}

// badRequest is a malformed body or argument; it answers 400.
type badRequest struct{ msg string }

func (b *badRequest) Error() string { return b.msg }

func bad(format string, a ...any) error { return &badRequest{msg: fmt.Sprintf(format, a...)} }

// refusal is an operator action the rules forbid right now; it answers 409 with its reason.
type refusal struct{ msg string }

func (r *refusal) Error() string { return r.msg }

func refuse(format string, a ...any) error { return &refusal{msg: fmt.Sprintf(format, a...)} }

// Refuse is a refusal a Fleet or Store implementation answers with: 409 and the reason.
func Refuse(format string, a ...any) error { return refuse(format, a...) }

// NotFound is a 404 a Fleet or Store implementation answers with.
func NotFound(format string, a ...any) error { return notFound(format, a...) }

// missing is a 404 in its own words: it answers as directory.ErrNotFound without carrying the
// directory's prefix into a message about something else, such as a client key.
type missing struct{ msg string }

func (m *missing) Error() string { return m.msg }

func (m *missing) Is(target error) bool { return target == directory.ErrNotFound }

func notFound(format string, a ...any) error { return &missing{msg: fmt.Sprintf(format, a...)} }

// errorOf maps an error to its HTTP status and body.
func errorOf(err error) (int, Error) {
	var (
		br  *badRequest
		ref *refusal
		te  *directory.TransitionError
		ce  *config.Error
		le  *lineageError
		sb  *ScopeBusyError
		ge  *GenerationError
		ic  *IdempotencyConflictError
		ce2 *CapacityError
		cd  *codedError
		re  *RetirementError
	)
	switch {
	case errors.As(err, &re):
		return http.StatusConflict, Error{Code: CodeRetirementUnproven, Message: err.Error(), Blockers: re.Blockers()}
	case errors.As(err, &cd):
		return cd.status, Error{Code: cd.code, Message: err.Error()}
	case errors.As(err, &ic):
		return http.StatusConflict, Error{Code: CodeIdempotencyConflict, Message: err.Error(), OperationID: ic.Owner}
	case errors.As(err, &ce2):
		return http.StatusTooManyRequests, Error{Code: CodeOperationCapacity, Message: err.Error()}
	case errors.As(err, &sb):
		return http.StatusConflict, Error{Code: CodeOperationConflict, Message: err.Error(), OperationID: sb.Owner}
	case errors.As(err, &ge):
		return http.StatusConflict, Error{Code: CodeGenerationConflict, Message: err.Error(), CurrentGeneration: strconv.FormatInt(ge.Current, 10)}
	case errors.As(err, &le):
		cur := le.cur
		return http.StatusConflict, Error{Code: le.code, Message: err.Error(), CurrentIdentity: &cur}
	case errors.As(err, &br):
		return http.StatusBadRequest, Error{Code: "bad_request", Message: err.Error()}
	case errors.As(err, &ref), errors.Is(err, directory.ErrInUse), errors.Is(err, directory.ErrRefused), errors.As(err, &te):
		return http.StatusConflict, Error{Code: "refused", Message: err.Error()}
	case errors.Is(err, directory.ErrNotFound):
		return http.StatusNotFound, Error{Code: "not_found", Message: err.Error()}
	case errors.Is(err, directory.ErrExists), errors.Is(err, directory.ErrConflict):
		return http.StatusConflict, Error{Code: "conflict", Message: err.Error()}
	case errors.As(err, &ce):
		return http.StatusBadRequest, Error{Code: "invalid", Message: err.Error()}
	case errors.Is(err, directory.ErrReadOnly), errors.Is(err, directory.ErrLockTimeout), errors.Is(err, ErrUnavailable):
		return http.StatusServiceUnavailable, Error{Code: "unavailable", Message: err.Error()}
	default:
		return http.StatusBadGateway, Error{Code: "backend", Message: err.Error()}
	}
}

// ErrorCode is the API error code err answers with.
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	_, e := errorOf(err)
	return e.Code
}

// fail maps an error to its HTTP answer.
func fail(w http.ResponseWriter, err error) {
	status, e := errorOf(err)
	e.Retryable = retryable(e.Code)
	writeJSON(w, status, e)
}

// retryable reports whether a refusal with this code may pass later unchanged.
func retryable(code string) bool {
	switch code {
	case "unavailable", CodeOperationCapacity, CodeOperationConflict, CodeResyncRequired:
		return true
	}
	return false
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

// decodeOptional is decode for a route whose body may be empty: v is then left as it is.
func decodeOptional(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "bad_request", "request body: "+err.Error())
		return false
	}
	return true
}

// ClusterStatus is one cluster as status reports it: the secret_ref, never a secret.
type ClusterStatus struct {
	Name                   string   `json:"name"`
	Type                   string   `json:"type"`
	Scheme                 string   `json:"scheme"`
	Region                 string   `json:"region"`
	Endpoints              []string `json:"endpoints"`
	AccessKey              string   `json:"access_key"`
	SecretRef              string   `json:"secret_ref"`
	ConditionalWrite       bool     `json:"conditional_write"`
	ConditionalDelete      bool     `json:"conditional_delete"`
	ConditionalWriteKnown  bool     `json:"conditional_write_known"`
	ConditionalDeleteKnown bool     `json:"conditional_delete_known"`
	CapabilityProfile      string   `json:"capability_profile"`
	References             []string `json:"references,omitempty"`
	ReadOnly               bool     `json:"read_only"`
	RejectWrites           bool     `json:"reject_writes"`
}

// PlacementStatus is one placement with the migration signals an operator watches.
type PlacementStatus struct {
	Key             string                     `json:"key"`
	State           string                     `json:"state"`
	Primary         string                     `json:"primary"`
	Source          string                     `json:"source,omitempty"`
	Target          string                     `json:"target,omitempty"`
	Names           map[string]string          `json:"names"`
	Ratio           float64                    `json:"ratio,omitempty"`
	Prefixes        []string                   `json:"prefixes,omitempty"`
	Hold            *directory.RampHold        `json:"hold,omitempty"` // a ramp step written but not yet on every proxy (ADR-0016)
	Writes          map[string]float64         `json:"ramp_writes"`    // side → shunt_ramp_writes_total on this proxy
	FallbackReads   float64                    `json:"fallback_reads"`
	DualDeletes     map[string]float64         `json:"dual_deletes"`
	MigrationWindow *MigrationWindow           `json:"migration_window,omitempty"`
	Cutover         *directory.CutoverEvidence `json:"cutover,omitempty"`
	Mover           *Progress                  `json:"mover,omitempty"`
	ReadOnly        bool                       `json:"read_only"`
	RejectWrites    bool                       `json:"reject_writes"`
	// Watch: an operator asked for this bucket's traffic by backend in telemetry. Spread and
	// moving buckets have it anyway; PerBucket says whether proxies count it now.
	Watch     bool `json:"watch,omitempty"`
	PerBucket bool `json:"per_bucket_telemetry"`
	// Legs are the backend buckets of a bucket spread over legs (ADR-0018 N2), in key-space order;
	// Primary and Names are empty then.
	Legs []LegStatus `json:"legs,omitempty"`
	// Scopes are its prefix rules (ADR-0020); Legs is then the scope of the empty prefix.
	Scopes []ScopeStatus `json:"scopes,omitempty"`
	// Move is the part of a spread bucket moving between legs; Primary and Source are its two
	// clusters then (ADR-0018 N3), and PrimaryBucket and SourceBucket its two buckets, which names
	// cannot both hold when the legs share a cluster (N3b).
	Move          *MoveStatus `json:"move,omitempty"`
	PrimaryBucket string      `json:"primary_bucket,omitempty"`
	SourceBucket  string      `json:"source_bucket,omitempty"`
	// ClientKeys counts the tenant's client keys whose bucket allowlist admits this bucket: 0 means
	// no request through shunt can reach it yet. Absent when this shunt holds no client keys.
	ClientKeys *int `json:"client_keys,omitempty"`
}

// LegStatus is one leg of a spread bucket: where it is, and the share of the key space it owns.
type LegStatus struct {
	ID      string                `json:"id"`
	Cluster string                `json:"cluster"`
	Bucket  string                `json:"bucket"`
	Share   float64               `json:"share"`  // fraction of the key hash space, 0..1
	Ranges  []directory.HashRange `json:"ranges"` // the hash ranges it owns, in order
	// Idle is a leg that owns no key in any scope and takes part in no move: expand --clear
	// retires it. A leg owning keys only under a prefix rule has no ranges here but is not idle.
	Idle bool `json:"idle,omitempty"`
}

// ScopeStatus is one prefix rule of a spread bucket (ADR-0020): the keys under Prefix that no
// longer rule claims, and the legs that own them.
type ScopeStatus struct {
	Prefix string      `json:"prefix"`
	Legs   []LegStatus `json:"legs"`
}

// MoveStatus is a move of part of a bucket: its legs, its range, and that range's share of the
// key space.
type MoveStatus struct {
	Scope string              `json:"scope,omitempty"` // the prefix rule whose keys move (ADR-0020); share is of its keys then
	From  string              `json:"from"`
	To    string              `json:"to"`
	Range directory.HashRange `json:"range"`
	Share float64             `json:"share"`
}

// legStatuses lists a spread placement's legs in the order their ranges run in the scope of the
// empty prefix, then the legs owning nothing there.
func legStatuses(p directory.Placement) []LegStatus {
	out, seen := ownerStatuses(p, p.Owners)
	idle := directory.IdleLegs(p)
	for _, id := range sortedKeys(p.Legs) { // a leg owning nothing here: a move's new destination, or one owning only under a prefix rule
		if _, ok := seen[id]; !ok {
			l := p.Legs[id]
			out = append(out, LegStatus{ID: id, Cluster: l.Cluster, Bucket: l.Bucket, Ranges: []directory.HashRange{}, Idle: slices.Contains(idle, id)})
		}
	}
	return out
}

// scopeStatuses lists a spread placement's prefix rules, each with the legs owning its keys.
func scopeStatuses(p directory.Placement) []ScopeStatus {
	out := make([]ScopeStatus, 0, len(p.Prefixes))
	for _, r := range p.Prefixes {
		legs, _ := ownerStatuses(p, r.Owners)
		out = append(out, ScopeStatus{Prefix: r.Prefix, Legs: legs})
	}
	return out
}

// ownerStatuses lists the legs of one owners table in the order their ranges run.
func ownerStatuses(p directory.Placement, owners []directory.Owner) (out []LegStatus, seen map[string]int) {
	seen = map[string]int{}
	for _, o := range owners {
		share := (float64(o.To) - float64(o.From) + 1) / (1 << 64)
		rg := directory.HashRange{From: o.From, To: o.To}
		if i, ok := seen[o.Leg]; ok {
			out[i].Share += share
			out[i].Ranges = append(out[i].Ranges, rg)
			continue
		}
		l := p.Legs[o.Leg]
		seen[o.Leg] = len(out)
		out = append(out, LegStatus{ID: o.Leg, Cluster: l.Cluster, Bucket: l.Bucket, Share: share, Ranges: []directory.HashRange{rg}})
	}
	return out, seen
}

// placementClusters names every cluster a placement uses: its roles, or its legs when spread.
func placementClusters(p directory.Placement) []string {
	legs := legStatuses(p)
	names := make([]string, 0, 3+len(legs))
	names = append(names, p.Primary, p.Source, p.Target)
	for _, l := range legs {
		names = append(names, l.Cluster)
	}
	return names
}

// MigrationWindow is the fleet's last completed 10-second routing signal for one placement.
type MigrationWindow struct {
	Start  time.Time        `json:"start"`
	End    time.Time        `json:"end"`
	Writes map[string]int64 `json:"writes"`
	Reads  map[string]int64 `json:"reads"`
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
	if err := s.flushLocalTelemetry(); err != nil {
		fail(w, err)
		return
	}
	snap := s.Dir.Snapshot()
	f := snap.File()
	out := Status{Version: f.Version, Clusters: []ClusterStatus{}, Placements: []PlacementStatus{}}
	for _, name := range sortedKeys(f.Clusters) {
		out.Clusters = append(out.Clusters, clusterStatus(f, name, f.Clusters[name]))
	}
	want := r.URL.Query().Get("bucket")
	if want != "" {
		if _, ok := f.Placements[want]; !ok {
			fail(w, fmt.Errorf("%w: no bucket %s in the directory", directory.ErrNotFound, want))
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

func (s *Server) placementStatus(key string, pl directory.Placement) PlacementStatus {
	// A bucket part of which is moving reads as the move's two clusters, so everything that shows a
	// migration shows the move as one; Legs and Move say which part (ADR-0018 N3).
	p := moving(pl)
	ps := PlacementStatus{Key: key, State: p.State, Primary: p.ClusterOf(p.Primary), Target: p.Target, Names: p.Names,
		ReadOnly: p.ReadOnly, RejectWrites: p.RejectWrites, Watch: pl.Watch,
		PerBucket: pl.Spread() || pl.State != directory.StateActive || pl.Watch,
		Cutover:   p.Cutover, Writes: map[string]float64{}, DualDeletes: map[string]float64{}}
	if p.Source != "" {
		ps.Source = p.ClusterOf(p.Source)
	}
	if p.LegClusters != nil {
		// A move's roles are legs; names reads by cluster, and PrimaryBucket and SourceBucket keep the
		// two buckets apart when the legs share a cluster (ADR-0018 N3b).
		ps.Names = map[string]string{ps.Source: p.Names[p.Source], ps.Primary: p.Names[p.Primary]}
		ps.PrimaryBucket, ps.SourceBucket = p.Names[p.Primary], p.Names[p.Source]
	}
	if p.Ramp != nil {
		ps.Ratio, ps.Prefixes, ps.Hold = p.Ramp.Ratio, p.Ramp.Prefixes, p.Ramp.Hold
	}
	if pl.Spread() {
		ps.Legs = legStatuses(pl)
		ps.Scopes = scopeStatuses(pl)
	}
	if m := pl.Move; m != nil {
		ps.Move = &MoveStatus{Scope: m.Scope, From: m.From, To: m.To, Range: m.Range, Share: (float64(m.Range.To) - float64(m.Range.From) + 1) / (1 << 64)}
	}
	if s.Keys != nil {
		tenant, bucket, _ := directory.SplitKey(key)
		n := 0
		for _, c := range s.Keys.Tenant(tenant) {
			if len(c.Buckets) == 0 || slices.Contains(c.Buckets, bucket) {
				n++
			}
		}
		ps.ClientKeys = &n
	}
	if s.Metrics != nil {
		ps.Writes = counters(s.Metrics.RampWrites, key, "side")
		ps.FallbackReads = counters(s.Metrics.FallbackReads, key, "")[""]
		ps.DualDeletes = counters(s.Metrics.DualDelete, key, "outcome")
	}
	if s.Telemetry != nil {
		for _, window := range s.Telemetry.Latest() {
			if window.Scope != "fleet" {
				continue
			}
			for _, signal := range window.Migrations {
				if signal.Bucket == key {
					ps.MigrationWindow = &MigrationWindow{Start: window.Start, End: window.End,
						Writes: maps.Clone(signal.Writes), Reads: maps.Clone(signal.Reads)}
				}
			}
		}
	}
	s.mu.Lock()
	if pr, ok := s.progress[key]; ok {
		ps.Mover = &pr
	}
	s.mu.Unlock()
	return ps
}

// Counters reads a counter vector's series for one bucket without creating any, keyed by the
// value of label (or "" when label is empty): what a member proxy reports in its heartbeat.
func Counters(vec *prometheus.CounterVec, bucket, label string) map[string]float64 {
	return counters(vec, bucket, label)
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
	// Secrets resolves the clusters' control: secret_refs, which only the control plane can, so a
	// mover on another host can sign (ADR-0015). Empty on a lab proxy, whose refs are env:/file:.
	Secrets map[string]string `json:"secrets,omitempty"`
}

func (s *Server) placement(w http.ResponseWriter, r *http.Request) {
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	d := PlacementDetail{Key: key, Placement: p, Clusters: map[string]config.Cluster{}}
	var secrets map[string]string
	if s.ClusterSecrets != nil {
		secrets = s.ClusterSecrets()
	}
	for _, name := range placementClusters(p) {
		if c, found := f.Clusters[name]; found {
			d.Clusters[name] = c
			if v, ok := secrets[c.Credentials.SecretRef]; ok {
				if d.Secrets == nil {
					d.Secrets = map[string]string{}
				}
				d.Secrets[c.Credentials.SecretRef] = v
			}
		}
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) lookup(w http.ResponseWriter, r *http.Request) (string, directory.Placement, *directory.File, bool) {
	key := pathKey(r)
	p, f, err := s.placementOf(key)
	if err != nil {
		fail(w, err)
		return key, p, f, false
	}
	return key, p, f, true
}

// placementOf reads a placement from the current snapshot.
func (s *Server) placementOf(key string) (directory.Placement, *directory.File, error) {
	f := s.Dir.Snapshot().File()
	p, ok := f.Placements[key]
	if !ok {
		return p, f, fmt.Errorf("%w: no bucket %s in the directory", directory.ErrNotFound, key)
	}
	return p, f, nil
}

// ClusterRequest adds or replaces a cluster.
type ClusterRequest struct {
	Name    string         `json:"name"`
	Cluster config.Cluster `json:"cluster"`
	// Secret, when set, is stored by the server in SecretsDir and the cluster's secret_ref points at
	// that file. It never reaches the directory, a log, or a response (ADR-0010).
	Secret string `json:"secret,omitempty"`
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
	if !clusterName.MatchString(req.Name) {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("cluster name %q: use lowercase letters, digits, - and _", req.Name))
		return
	}
	if !s.writeCluster(w, r, &req) {
		return
	}
	c, _ := s.Dir.Snapshot().Cluster(req.Name)
	writeJSON(w, http.StatusOK, ClusterStatus{Name: req.Name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, Endpoints: c.Endpoints,
		AccessKey: c.Credentials.AccessKey, SecretRef: c.Credentials.SecretRef,
		ConditionalWrite: c.Capabilities.ConditionalWriteOr(true), ConditionalDelete: c.Capabilities.ConditionalDeleteOr(false)})
}

// writeCluster stores req's secret, checks the cluster's credentials against it and writes the
// definition, answering the error itself when it returns false: cluster add and credential
// rotation share it, so a rotation is refused exactly as an add with the same pair would be.
func (s *Server) writeCluster(w http.ResponseWriter, r *http.Request, req *ClusterRequest) bool {
	stored, secret := "", ""
	if req.Secret != "" && s.SecretsDir == "" {
		// The store keeps secrets itself (internal/cp): encrypted, named by a control: ref.
		secret = req.Secret
		req.Cluster.Credentials.SecretRef = "control:" + req.Name
	} else if req.Secret != "" {
		path, err := s.storeSecret(req.Name, req.Secret)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "unavailable", "storing the secret: "+err.Error())
			return false
		}
		stored = path
		req.Cluster.Credentials.SecretRef = "file:" + path
	}
	discard := func() {
		if stored != "" {
			_ = os.Remove(stored) //nolint:errcheck // best effort: a refused add leaves no secret behind
		}
	}
	inferType := req.Cluster.Type == ""
	if inferType {
		req.Cluster.Type = "s3" // provisional, so the definition builds; the cluster's own answer decides below
	}
	if req.Cluster.Region == "" {
		req.Cluster.Region = regionFor(req.Cluster.Endpoints)
	}
	msg, server := s.checkCredentials(r.Context(), req.Name, req.Cluster, req.Secret)
	if msg != "" {
		discard()
		writeError(w, http.StatusConflict, "refused", msg)
		return false
	}
	if inferType {
		req.Cluster.Type = typeFromServer(server)
	}
	if err := s.Dir.PutCluster(r.Context(), req.Name, req.Cluster, secret, actor(r)); err != nil {
		discard()
		if errors.Is(err, directory.ErrSecretInline) {
			writeError(w, http.StatusConflict, "refused", "this shunt has no secrets directory (directory.secrets_dir) and its directory file carries only secret_refs; give the cluster a --secret-ref instead")
			return false
		}
		var ce *config.Error
		if errors.As(err, &ce) || strings.Contains(err.Error(), "clusters.") {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return false
		}
		if !isDirectoryError(err) {
			// Prepare refused it: the proxy could not build the cluster, usually an unresolvable secret_ref.
			writeError(w, http.StatusConflict, "refused", "the proxy cannot use this cluster: "+err.Error())
			return false
		}
		fail(w, err)
		return false
	}
	if stored != "" {
		s.dropSecrets(req.Name, stored)
	}
	return true
}

// CredentialsRequest is POST /v1/clusters/{name}/credentials: a new secret for the cluster's
// access key, or a new access key with its secret. Exactly one of Secret and SecretRef is given;
// the rest of the definition is kept.
type CredentialsRequest struct {
	AccessKey string `json:"access_key,omitempty"`
	Secret    string `json:"secret,omitempty"`
	SecretRef string `json:"secret_ref,omitempty"`
}

// CredentialsResult is the rotation's answer: the directory version it landed in and the secret
// generation it started, which GET /v1/clusters/{name}/view's secret reports proxies installing.
// Generation is empty for a secret this control plane does not hold (an env: or file: ref).
type CredentialsResult struct {
	Name       string        `json:"name"`
	Version    int64         `json:"version"`
	Generation string        `json:"generation,omitempty"`
	Cluster    ClusterStatus `json:"cluster"`
}

// rotateCredentials replaces a cluster's credentials and nothing else (ADR-0021 D1). The new pair
// is checked with one signed ListBuckets first; a request that already took the old bundle may
// still finish with the old secret, so the backend keeps it valid until the new generation is
// installed everywhere and those requests have ended.
func (s *Server) rotateCredentials(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req CredentialsRequest
	if !decode(w, r, &req) {
		return
	}
	if (req.Secret == "") == (req.SecretRef == "") {
		writeError(w, http.StatusBadRequest, "bad_request", "give exactly one of secret and secret_ref")
		return
	}
	def, ok := s.Dir.Snapshot().Cluster(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no cluster "+name)
		return
	}
	if req.AccessKey != "" {
		def.Credentials.AccessKey = req.AccessKey
	}
	if req.SecretRef != "" {
		def.Credentials.SecretRef = req.SecretRef
	}
	creq := ClusterRequest{Name: name, Cluster: def, Secret: req.Secret}
	if !s.writeCluster(w, r, &creq) {
		return
	}
	snap := s.Dir.Snapshot()
	c, _ := snap.Cluster(name)
	out := CredentialsResult{Name: name, Version: snap.Version(), Cluster: ClusterStatus{Name: name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, Endpoints: c.Endpoints,
		AccessKey: c.Credentials.AccessKey, SecretRef: c.Credentials.SecretRef,
		ConditionalWrite: c.Capabilities.ConditionalWriteOr(true), ConditionalDelete: c.Capabilities.ConditionalDeleteOr(false)}}
	if g := snap.File().Generation(directory.SecretResource(name)); g != 0 {
		out.Generation = strconv.FormatInt(g, 10)
	}
	writeJSON(w, http.StatusOK, out)
}

// ClusterProbeResult is the non-mutating preflight used by the add-cluster drawer. Reachability
// and credentials are measured; a capability is measured only when the proposed definition states
// it, otherwise its conservative runtime default is identified as assumed.
type ClusterProbeResult struct {
	Cluster      ClusterStatus `json:"cluster"`
	Reachable    bool          `json:"reachable"`
	Profile      string        `json:"profile"` // measured when both migration capabilities are known
	Capabilities struct {
		ConditionalWrite  Capability `json:"conditional_write"`
		ConditionalDelete Capability `json:"conditional_delete"`
	} `json:"capabilities"`
}

func (s *Server) probeCluster(w http.ResponseWriter, r *http.Request) {
	var req ClusterRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Name == "" || !clusterName.MatchString(req.Name) {
		writeError(w, http.StatusBadRequest, "bad_request", "name is required and must use lowercase letters, digits, - or _")
		return
	}
	inferType := req.Cluster.Type == ""
	if inferType {
		req.Cluster.Type = "s3"
	}
	if req.Cluster.Region == "" {
		req.Cluster.Region = regionFor(req.Cluster.Endpoints)
	}
	if msg, server := s.checkCredentials(r.Context(), req.Name, req.Cluster, req.Secret); msg != "" {
		writeError(w, http.StatusConflict, "refused", msg)
		return
	} else if inferType {
		req.Cluster.Type = typeFromServer(server)
	}
	if req.Secret != "" && req.Cluster.Credentials.SecretRef == "" {
		// Cluster add stores a typed secret and refers to it; the probe stores nothing, so it
		// validates against the control: ref the stored secret would get (the only one that needs no path).
		req.Cluster.Credentials.SecretRef = "control:" + req.Name
	}
	defs := map[string]config.Cluster{req.Name: req.Cluster}
	config.ApplyClusterDefaults(defs)
	if err := config.ValidateClusters("clusters", defs); err != nil {
		fail(w, err)
		return
	}
	c := defs[req.Name]
	res := ClusterProbeResult{Reachable: true, Profile: "assumed"}
	res.Cluster = ClusterStatus{Name: req.Name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, Endpoints: endpoints(c),
		AccessKey: c.Credentials.AccessKey, SecretRef: c.Credentials.SecretRef, ReadOnly: c.ReadOnly, RejectWrites: c.RejectWrites,
		ConditionalWrite: c.Capabilities.ConditionalWriteOr(true), ConditionalDelete: c.Capabilities.ConditionalDeleteOr(false)}
	res.Capabilities.ConditionalWrite = capability(c.Capabilities.ConditionalWrite, true)
	res.Capabilities.ConditionalDelete = capability(c.Capabilities.ConditionalDelete, false)
	if res.Capabilities.ConditionalWrite.Known && res.Capabilities.ConditionalDelete.Known {
		res.Profile = "measured"
	}
	writeJSON(w, http.StatusOK, res)
}

func isDirectoryError(err error) bool {
	for _, target := range []error{directory.ErrNotFound, directory.ErrExists, directory.ErrInUse, directory.ErrConflict, directory.ErrReadOnly, directory.ErrLockTimeout} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// RemoveRequest is DELETE /v1/clusters/{name}'s body: the confirmation token its dry run issued.
type RemoveRequest struct {
	Token string `json:"token,omitempty"`
}

// RemoveResult is what cluster remove did.
type RemoveResult struct {
	Removed   string `json:"removed"`
	Operation string `json:"operation,omitempty"`
}

// RemoveDryRun is DELETE /v1/clusters/{name}?dry_run=1: what removing the cluster would do, and
// the token the real call must present (ADR-0017).
type RemoveDryRun struct {
	Allowed     bool      `json:"allowed"`
	Reason      string    `json:"reason,omitempty"` // the refusal the real call would give
	Name        string    `json:"name"`
	References  []string  `json:"references"`
	SecretFiles int       `json:"secret_files"` // secret files this shunt stored for it, removed with it
	Token       string    `json:"token,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitzero"`
}

func (s *Server) removeCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if r.URL.Query().Get("dry_run") != "" {
		res, err := s.removeDryRun(name)
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
	s.serveOperation(w, r, OperationRequest{Kind: OpClusterRemove, Cluster: name}, req)
}

// removeBinding is what a cluster remove token is bound to: the definition and what references it.
func removeBinding(f *directory.File, name string) [32]byte {
	return digestOf(f.Clusters[name], directory.References(f, name))
}

// removeChecks is every refusal cluster remove gives before the token is looked at.
func removeChecks(f *directory.File, name string) ([]string, error) {
	if _, ok := f.Clusters[name]; !ok {
		return nil, fmt.Errorf("%w: cluster %q is not in the directory", directory.ErrNotFound, name)
	}
	refs := directory.References(f, name)
	if len(refs) > 0 {
		return refs, fmt.Errorf("%w: cluster %q is still referenced by %s", directory.ErrInUse, name, strings.Join(refs, ", "))
	}
	return nil, nil
}

func (s *Server) removeDryRun(name string) (RemoveDryRun, error) {
	f := s.Dir.Snapshot().File()
	res := RemoveDryRun{Name: name, References: []string{}, SecretFiles: len(s.secretFiles(name))}
	refs, err := removeChecks(f, name)
	if refs != nil {
		res.References = refs
	}
	switch {
	case errors.Is(err, directory.ErrNotFound):
		return res, err
	case err != nil:
		res.Reason = err.Error()
		return res, nil
	}
	res.Allowed = true
	res.Token, res.ExpiresAt = s.confirmToken("cluster remove", removeBinding(f, name))
	return res, nil
}

func (s *Server) runRemoveCluster(tr *tracker, name string, req RemoveRequest) (RemoveResult, error) {
	tr.phase(PhaseStep)
	f := s.Dir.Snapshot().File()
	if _, err := removeChecks(f, name); err != nil {
		return RemoveResult{}, err
	}
	if err := s.checkToken(req.Token, "cluster remove", removeBinding(f, name)); err != nil {
		return RemoveResult{}, err
	}
	if err := s.Dir.RemoveCluster(tr.ctx, name, tr.actor); err != nil {
		return RemoveResult{}, err
	}
	s.dropSecrets(name, "")
	return RemoveResult{Removed: name, Operation: tr.id()}, nil
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
	v := s.Dir.Snapshot().Version()
	s.info(actor(r), "tenant default changed", "tenant", tenant, "default_cluster", req.Cluster, "version", v)
	writeJSON(w, http.StatusOK, map[string]any{"tenant": tenant, "default_cluster": req.Cluster, "version": v})
}

// WatchRequest is POST /v1/placements/{tenant}/{bucket}/watch.
type WatchRequest struct {
	Watch bool `json:"watch"`
}

// placementWatch asks proxies for, or stops, a bucket's traffic by backend cluster in telemetry.
func (s *Server) placementWatch(w http.ResponseWriter, r *http.Request) {
	var req WatchRequest
	if !decode(w, r, &req) {
		return
	}
	tenant, bucket := r.PathValue("tenant"), r.PathValue("bucket")
	if err := s.Dir.SetPlacementWatch(r.Context(), tenant, bucket, req.Watch, actor(r)); err != nil {
		fail(w, err)
		return
	}
	v := s.Dir.Snapshot().Version()
	key := directory.Key(tenant, bucket)
	s.info(actor(r), "bucket watch changed", "placement", key, "watch", req.Watch, "version", v)
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "watch": req.Watch, "version": v})
}

// backendFor returns the live cluster a placement role names.
func (s *Server) backendFor(name string) (backend, error) {
	cl, ok := s.Clusters.Load().Get(name)
	if !ok {
		return backend{}, fmt.Errorf("%w: no cluster named %q (shunt status lists the clusters)", directory.ErrNotFound, name)
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

// checkCredentials signs one ListBuckets to a cluster about to be added, with the access key given
// and the secret this proxy resolves for its secret_ref, and returns a refusal when the cluster says
// the pair is wrong. It catches the mistake that otherwise surfaces later as a 403 on adopt or in
// the mover: an access key from one credential set and a secret from another. Anything short of a
// signature or unknown-key answer passes: a key may be allowed its buckets without ListBuckets.
// An invalid definition returns "" and is refused by the directory write that follows.
func (s *Server) checkCredentials(ctx context.Context, name string, c config.Cluster, secret string) (refusal, server string) {
	defs := map[string]config.Cluster{name: c}
	config.ApplyClusterDefaults(defs)
	if config.ValidateClusters("clusters", defs) != nil {
		return "", ""
	}
	cl, err := s.Clusters.BuildWith(name, defs[name], secret)
	if err != nil {
		return "the proxy cannot use this cluster: " + err.Error(), ""
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	reply, err := backend{cl: cl}.do(ctx, http.MethodGet, "", "", nil, nil, nil)
	if err != nil {
		return fmt.Sprintf("cannot reach cluster %s to check its credentials: %v", name, err), ""
	}
	server = reply.header.Get("Server")
	var e struct {
		Message string `xml:"Message"`
		Region  string `xml:"Region"`
	}
	_ = xml.Unmarshal(reply.body, &e) //nolint:errcheck // no message is fine
	if reply.code == "AuthorizationHeaderMalformed" {
		// A request signed for the wrong region. AWS and Garage name the region they expect;
		// without this check the add succeeds and the first bucket operation fails instead.
		if e.Region != "" && e.Region != c.Region {
			return fmt.Sprintf("cluster %s expects region %s, not %s: set the region to %s", name, e.Region, c.Region, e.Region), server
		}
		return fmt.Sprintf("cluster %s rejected the request's authorization header (%s): %s; check the region (%s)", name, reply.code, e.Message, c.Region), server
	}
	ak, ref := c.Credentials.AccessKey, c.Credentials.SecretRef
	switch s3.ClassifyCredentialError(reply.code, e.Message) {
	case s3.FaultUnknownKey:
		return fmt.Sprintf("cluster %s does not know access key %s (%s): check --access-key", name, ak, reply.code), server
	case s3.FaultSignature:
		msg := fmt.Sprintf("cluster %s rejected the signature (%s): access key %s exists, but the secret shunt serve reads from %s is not that key's secret", name, reply.code, ak, ref)
		if strings.HasPrefix(ref, "env:") {
			msg += "; an env: ref is read from shunt serve's environment as it was when serve started, so set " + strings.TrimPrefix(ref, "env:") + " there and restart serve, or use a file: ref"
		}
		if strings.HasPrefix(ref, "file:") && strings.HasPrefix(strings.TrimPrefix(ref, "file:"), s.SecretsDir+string(os.PathSeparator)) && s.SecretsDir != "" {
			msg = fmt.Sprintf("cluster %s rejected the signature (%s): the secret key you entered is not the secret of access key %s", name, reply.code, ak)
		}
		return msg, server
	}
	return "", server
}

// clusterName is what a cluster may be called: it names a file in SecretsDir and a key in the directory.
var clusterName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// storeSecret writes a cluster's secret to a new 0600 file in SecretsDir and returns its path. Each
// add gets a new file name, so a changed secret is a changed definition and the proxy rebuilds the
// cluster with it; dropSecrets removes the old files once the add has landed.
func (s *Server) storeSecret(name, secret string) (string, error) {
	if err := os.MkdirAll(s.SecretsDir, 0o700); err != nil {
		return "", err
	}
	var id [4]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	path := filepath.Join(s.SecretsDir, name+"-"+hex.EncodeToString(id[:]))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // name is checked against clusterName
	if err != nil {
		return "", err
	}
	if _, err := f.WriteString(secret); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", err
	}
	return path, f.Close()
}

// secretFiles lists the secret files shunt stored for a cluster.
func (s *Server) secretFiles(name string) []string {
	if s.SecretsDir == "" {
		return nil
	}
	entries, err := os.ReadDir(s.SecretsDir)
	if err != nil {
		return nil
	}
	own := regexp.MustCompile(`^` + regexp.QuoteMeta(name) + `-[0-9a-f]{8}$`)
	var paths []string
	for _, e := range entries {
		if own.MatchString(e.Name()) {
			paths = append(paths, filepath.Join(s.SecretsDir, e.Name()))
		}
	}
	return paths
}

// dropSecrets removes the secret files shunt stored for a cluster, except keep.
func (s *Server) dropSecrets(name, keep string) {
	for _, path := range s.secretFiles(name) {
		if path != keep {
			_ = os.Remove(path) //nolint:errcheck // best effort
		}
	}
}

// typeFromServer reads a cluster's type from its Server response header: VAST answers "vast 5.x",
// MinIO "MinIO", AWS "AmazonS3"; anything else (Garage sends none) is a generic S3 backend.
func typeFromServer(server string) string {
	switch v := strings.ToLower(server); {
	case strings.HasPrefix(v, "vast"):
		return "vast"
	case strings.Contains(v, "minio"):
		return "minio"
	case strings.Contains(v, "amazons3"):
		return "aws"
	}
	return "s3"
}

// awsHost matches regional AWS S3 endpoints, e.g. s3.eu-west-1.amazonaws.com or s3-us-west-2.amazonaws.com.
var awsHost = regexp.MustCompile(`^s3[.-]([a-z0-9-]+)\.amazonaws\.com(:\d+)?$`)

// regionFor is the signing region when none is given: the one in an AWS regional endpoint name,
// otherwise us-east-1, which VAST, MinIO and most S3 backends accept.
func regionFor(endpoints []string) string {
	if len(endpoints) > 0 {
		if m := awsHost.FindStringSubmatch(endpoints[0]); m != nil {
			return m[1]
		}
	}
	return "us-east-1"
}

// logged wraps a mutating route so that every answer it refuses or fails is in serve's log with its
// reason, next to the success line each handler writes: an operator watching `shunt serve` sees
// every change and every refusal, whichever terminal ran the command.
func (s *Server) logged(op string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &errorRecorder{ResponseWriter: w}
		h(rec, r)
		if s.Log == nil || rec.status < 400 {
			return
		}
		var e Error
		_ = json.Unmarshal(rec.body, &e) //nolint:errcheck // a non-JSON body just has no reason
		event := op + " failed"
		if e.Code == "refused" {
			event = op + " refused"
		}
		attrs := []any{"actor", actor(r)}
		if t, b := r.PathValue("tenant"), r.PathValue("bucket"); b != "" {
			attrs = append(attrs, "placement", directory.Key(t, b))
		} else if n := r.PathValue("name"); n != "" {
			attrs = append(attrs, "cluster", n)
		} else if t != "" {
			attrs = append(attrs, "tenant", t)
		}
		s.Log.Warn(event, append(attrs, "status", rec.status, "code", e.Code, "reason", e.Message)...)
	}
}

// info writes one success line for an operation, with who asked.
func (s *Server) info(actor, event string, attrs ...any) {
	if s.Log != nil {
		s.Log.Info(event, append(attrs, "actor", actor)...)
	}
}

// errorRecorder keeps the status and, for an error answer, the body (an Error, a few hundred bytes).
type errorRecorder struct {
	http.ResponseWriter
	status int
	body   []byte
}

func (e *errorRecorder) WriteHeader(code int) {
	e.status = code
	e.ResponseWriter.WriteHeader(code)
}

func (e *errorRecorder) Write(b []byte) (int, error) {
	if e.status == 0 {
		e.status = http.StatusOK
	}
	if e.status >= 400 && len(e.body) < 4096 {
		e.body = append(e.body, b...)
	}
	return e.ResponseWriter.Write(b)
}
