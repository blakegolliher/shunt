package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/s3"
)

// backendTimeout bounds the backend calls of one operator action, except purge-source, which
// takes as long as the source bucket takes to empty.
const backendTimeout = 60 * time.Second

// TransitionResult is the answer to a state change.
type TransitionResult struct {
	Key           string                     `json:"key"`
	From          string                     `json:"from"`
	To            string                     `json:"to"`
	Version       int64                      `json:"version"`
	Primary       string                     `json:"primary"`
	Source        string                     `json:"source,omitempty"`
	Ratio         float64                    `json:"ratio,omitempty"`
	CreatedBucket string                     `json:"created_bucket,omitempty"`
	Warning       string                     `json:"warning,omitempty"`
	Cutover       *directory.CutoverEvidence `json:"cutover,omitempty"`
}

// transition applies t to a placement with every check a state change needs: the target bucket
// exists (or is created), neither side was ever versioned, and a MIGRATING target without
// conditional PUT was accepted explicitly.
func (s *Server) transition(r *http.Request, key string, p directory.Placement, f *directory.File, t directory.Transition, create, acceptLoss bool) (TransitionResult, error) {
	tenant, bucket, _ := directory.SplitKey(key)
	switch {
	case p.State == directory.StateActive && p.Target == "" && t.Target != "" && t.Name == "":
		t.Name = directory.BackendName(tenant, bucket, 0)
	case p.State != directory.StateActive && t.Target != "":
		// Repeating the target on a later step is how an operator types it; only a change is refused.
		if t.Target != p.Primary {
			return TransitionResult{}, refuse("%s is already moving to %s; a different target needs a reconcile, not a ramp step", key, p.Primary)
		}
		t.Target, t.Name = "", ""
	}
	np, err := directory.Apply(p, t)
	if err != nil {
		return TransitionResult{}, err
	}
	res := TransitionResult{Key: key, From: p.State, To: np.State, Primary: np.Primary, Source: np.Source}
	if np.Ramp != nil {
		res.Ratio = np.Ramp.Ratio
	}
	lossWindow := np.State == directory.StateMigrating && p.State != directory.StateMigrating &&
		!f.Clusters[np.Primary].Capabilities.ConditionalWriteOr(true)
	if lossWindow {
		if !acceptLoss {
			return TransitionResult{}, lostWriteWindow(key, np.Primary)
		}
		res.Warning = "accepted with accept_lost_write_window: " + lostWriteWindow(key, np.Primary).Error()
	}
	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()
	if p.State == directory.StateActive {
		target, err := s.backendFor(np.Primary)
		if err != nil {
			return TransitionResult{}, err
		}
		name := np.Names[np.Primary]
		exists, err := target.bucketExists(ctx, name)
		switch {
		case err != nil:
			return TransitionResult{}, err
		case !exists && !create:
			return TransitionResult{}, refuse("bucket %s does not exist on %s; create it there, or ask for it to be created (--create)", name, np.Primary)
		case !exists:
			if err := target.createBucket(ctx, name); err != nil {
				return TransitionResult{}, err
			}
			res.CreatedBucket = name
		}
	}
	if np.State == directory.StateRamping || np.State == directory.StateMigrating {
		for _, side := range []struct{ role, cluster string }{{"source", np.Source}, {"primary", np.Primary}} {
			b, err := s.backendFor(side.cluster)
			if err != nil {
				return TransitionResult{}, err
			}
			if err := refuseVersioned(ctx, b, side.role, np.Names[side.cluster]); err != nil {
				return TransitionResult{}, err
			}
		}
	}
	if err := s.Dir.SetState(r.Context(), tenant, bucket, p.State, t, actor(r)); err != nil {
		return TransitionResult{}, err
	}
	res.Version = s.Dir.Snapshot().Version()
	if after, ok := s.Dir.Snapshot().Lookup(tenant, bucket); ok {
		res.Cutover = after.Cutover
	}
	return res, nil
}

// AdoptRequest takes over an existing bucket on a cluster as an ACTIVE placement.
type AdoptRequest struct {
	Cluster string `json:"cluster"`
	Name    string `json:"name,omitempty"` // backend bucket name; default the client bucket name
}

func (s *Server) adopt(w http.ResponseWriter, r *http.Request) {
	var req AdoptRequest
	if !decode(w, r, &req) {
		return
	}
	tenant, bucket := r.PathValue("tenant"), r.PathValue("bucket")
	key := directory.Key(tenant, bucket)
	if req.Name == "" {
		req.Name = bucket
	}
	b, err := s.backendFor(req.Cluster)
	if err != nil {
		fail(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()
	switch exists, err := b.bucketExists(ctx, req.Name); {
	case err != nil:
		fail(w, err)
		return
	case !exists:
		fail(w, refuse("bucket %s does not exist on %s; adopt takes over a bucket that is already there", req.Name, req.Cluster))
		return
	}
	if err := s.Dir.Adopt(r.Context(), tenant, bucket, req.Cluster, req.Name, actor(r)); err != nil {
		fail(w, err)
		return
	}
	p, _ := s.Dir.Snapshot().Lookup(tenant, bucket)
	s.info(r, "bucket adopted", "placement", key, "cluster", req.Cluster, "bucket", req.Name, "state", p.State, "version", s.Dir.Snapshot().Version())
	writeJSON(w, http.StatusOK, s.placementStatus(key, *p))
}

// ExpandRequest prepares a target cluster and bucket for a placement that will move.
type ExpandRequest struct {
	To     string `json:"to"`
	Name   string `json:"name,omitempty"` // default <primary backend name>-NNN, the lowest unused
	Create bool   `json:"create,omitempty"`
}

// ExpandResult reports what expand checked and recorded.
type ExpandResult struct {
	Key               string `json:"key"`
	Target            string `json:"target"`
	Name              string `json:"name"`
	CreatedBucket     bool   `json:"created_bucket"`
	Canary            string `json:"canary"`
	ConditionalWrite  bool   `json:"conditional_write"`
	ConditionalDelete bool   `json:"conditional_delete"`
	Version           int64  `json:"version"`
}

func (s *Server) expand(w http.ResponseWriter, r *http.Request) {
	var req ExpandRequest
	if !decode(w, r, &req) {
		return
	}
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	tenant, bucket, _ := directory.SplitKey(key)
	switch {
	case p.State != directory.StateActive:
		fail(w, refuse("%s is %s; expand prepares an ACTIVE placement", key, p.State))
		return
	case req.To == "" || req.To == p.Primary:
		fail(w, refuse("expand needs a target cluster other than the primary %s", p.Primary))
		return
	}
	target, ok := f.Clusters[req.To]
	if !ok {
		fail(w, fmt.Errorf("%w: cluster %q is not in the directory", directory.ErrNotFound, req.To))
		return
	}
	if req.Name == "" {
		req.Name = nextName(f, req.To, p.Names[p.Primary])
	}
	if !s3.ValidBucketName(req.Name) {
		writeError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("%q is not a valid bucket name", req.Name))
		return
	}
	tb, err := s.backendFor(req.To)
	if err != nil {
		fail(w, err)
		return
	}
	pb, err := s.backendFor(p.Primary)
	if err != nil {
		fail(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()
	res := ExpandResult{Key: key, Target: req.To, Name: req.Name,
		ConditionalWrite: target.Capabilities.ConditionalWriteOr(true), ConditionalDelete: target.Capabilities.ConditionalDeleteOr(false)}
	exists, err := tb.bucketExists(ctx, req.Name)
	switch {
	case err != nil:
		fail(w, err)
		return
	case !exists && !req.Create:
		fail(w, refuse("bucket %s does not exist on %s; create it there, or ask expand to create it (--create)", req.Name, req.To))
		return
	case !exists:
		if err = tb.createBucket(ctx, req.Name); err != nil {
			fail(w, err)
			return
		}
		res.CreatedBucket = true
	}
	if err = refuseVersioned(ctx, pb, "source", p.Names[p.Primary]); err != nil {
		fail(w, err)
		return
	}
	if err = refuseVersioned(ctx, tb, "target", req.Name); err != nil {
		fail(w, err)
		return
	}
	if res.Canary, err = canary(ctx, tb, req.Name); err != nil {
		fail(w, refuse("canary write/read/delete on %s/%s failed: %v", req.To, req.Name, err))
		return
	}
	if err := s.Dir.SetTarget(r.Context(), tenant, bucket, req.To, req.Name, actor(r)); err != nil {
		fail(w, err)
		return
	}
	res.Version = s.Dir.Snapshot().Version()
	s.info(r, "target recorded", "placement", key, "target", req.To, "bucket", req.Name, "created_bucket", res.CreatedBucket, "canary", "ok",
		"conditional_write", res.ConditionalWrite, "conditional_delete", res.ConditionalDelete, "version", res.Version)
	writeJSON(w, http.StatusOK, res)
}

// nextName is base-NNN with the lowest NNN no placement uses on cluster.
func nextName(f *directory.File, cluster, base string) string {
	used := map[string]bool{}
	for k := range f.Placements {
		if n, ok := f.Placements[k].Names[cluster]; ok {
			used[n] = true
		}
	}
	for n := 1; ; n++ {
		name := fmt.Sprintf("%s-%03d", base, n)
		if len(name) > 63 {
			name = fmt.Sprintf("%s-%03d", strings.TrimRight(base[:63-4], ".-"), n)
		}
		if !used[name] {
			return name
		}
	}
}

// canary writes, reads back, and deletes one small object, and returns its key.
func canary(ctx context.Context, b backend, bucket string) (string, error) {
	var id [8]byte
	_, _ = rand.Read(id[:])
	key := ".shunt-canary-" + hex.EncodeToString(id[:])
	body := []byte("shunt expand canary " + key)
	put, err := b.do(ctx, http.MethodPut, bucket, key, nil, body, nil)
	if err != nil {
		return key, err
	}
	if put.status != http.StatusOK {
		return key, fmt.Errorf("PUT answered HTTP %d %s", put.status, put.code)
	}
	defer func() { _, _ = b.do(context.WithoutCancel(ctx), http.MethodDelete, bucket, key, nil, nil, nil) }()
	get, err := b.do(ctx, http.MethodGet, bucket, key, nil, nil, nil)
	if err != nil {
		return key, err
	}
	if get.status != http.StatusOK || !bytes.Equal(get.body, body) {
		return key, fmt.Errorf("GET answered HTTP %d with %d bytes, want the %d written", get.status, len(get.body), len(body))
	}
	del, err := b.do(ctx, http.MethodDelete, bucket, key, nil, nil, nil)
	if err != nil {
		return key, err
	}
	if del.status >= 300 {
		return key, fmt.Errorf("DELETE answered HTTP %d %s", del.status, del.code)
	}
	return key, nil
}

// RampRequest is one ramp step: a higher ratio, more prefixes, or both.
type RampRequest struct {
	Ratio    float64  `json:"ratio,omitempty"`
	Prefixes []string `json:"prefixes,omitempty"`
	To       string   `json:"to,omitempty"`   // leaving ACTIVE without expand: the target cluster
	Name     string   `json:"name,omitempty"` // and its backend bucket
	Create   bool     `json:"create,omitempty"`
}

func (s *Server) ramp(w http.ResponseWriter, r *http.Request) {
	var req RampRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Ratio < 0 || req.Ratio > 1 || (req.Ratio == 0 && len(req.Prefixes) == 0) {
		writeError(w, http.StatusBadRequest, "bad_request", "a ramp step needs a ratio in (0, 1] or at least one prefix")
		return
	}
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	res, err := s.transition(r, key, p, f, directory.Transition{To: directory.StateRamping, Target: req.To, Name: req.Name, Ratio: req.Ratio, Prefixes: req.Prefixes}, req.Create, false)
	if err != nil {
		fail(w, err)
		return
	}
	s.logTransition(r, "ramp", res, "prefixes", req.Prefixes)
	writeJSON(w, http.StatusOK, res)
}

// MigrateRequest moves a placement to MIGRATING: every write to the new primary, reads falling back.
type MigrateRequest struct {
	To                    string `json:"to,omitempty"`
	Name                  string `json:"name,omitempty"`
	Create                bool   `json:"create,omitempty"`
	AcceptLostWriteWindow bool   `json:"accept_lost_write_window,omitempty"`
}

func (s *Server) migrateStart(w http.ResponseWriter, r *http.Request) {
	var req MigrateRequest
	if !decode(w, r, &req) {
		return
	}
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	res, err := s.transition(r, key, p, f, directory.Transition{To: directory.StateMigrating, Target: req.To, Name: req.Name}, req.Create, req.AcceptLostWriteWindow)
	if err != nil {
		fail(w, err)
		return
	}
	s.logTransition(r, "migrate start", res, "accept_lost_write_window", req.AcceptLostWriteWindow)
	writeJSON(w, http.StatusOK, res)
}

// Progress is a mover's report on one placement, held in memory by this proxy for status and for
// cutover's convergence check. It is counts and a cursor key, never a per-object record.
type Progress struct {
	Source    string    `json:"source"`
	Primary   string    `json:"primary"`
	Pass      int       `json:"pass"`
	Copied    int       `json:"copied"`
	Skipped   int       `json:"skipped"`
	Vanished  int       `json:"vanished"`
	Failed    int       `json:"failed"`
	Bytes     int64     `json:"bytes"`
	LastKey   string    `json:"last_key,omitempty"`
	Done      bool      `json:"done"`      // the pass reached the end of the source listing
	Converged bool      `json:"converged"` // a completed pass copied nothing and failed nothing
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *Server) moverProgress(w http.ResponseWriter, r *http.Request) {
	var req Progress
	if !decode(w, r, &req) {
		return
	}
	key, p, _, ok := s.lookup(w, r)
	if !ok {
		return
	}
	if req.Source != p.Source || req.Primary != p.Primary {
		fail(w, refuse("%s moves %s → %s; this report is for %s → %s", key, p.Source, p.Primary, req.Source, req.Primary))
		return
	}
	req.Converged = req.Converged && req.Done && req.Copied == 0 && req.Failed == 0
	req.UpdatedAt = s.now().UTC()
	s.mu.Lock()
	if s.progress == nil {
		s.progress = map[string]Progress{}
	}
	s.progress[key] = req
	s.mu.Unlock()
	if req.Done {
		s.info(r, "mover pass", "placement", key, "pass", req.Pass, "copied", req.Copied, "already_there", req.Skipped, "vanished", req.Vanished,
			"failed", req.Failed, "bytes", req.Bytes, "converged", req.Converged)
	}
	writeJSON(w, http.StatusOK, req)
}

// CutoverRequest asks to stop reading the source, after a quiet window.
type CutoverRequest struct {
	Window string `json:"window,omitempty"` // Go duration; default 60s
}

func (s *Server) cutover(w http.ResponseWriter, r *http.Request) {
	var req CutoverRequest
	if !decode(w, r, &req) {
		return
	}
	window := 60 * time.Second
	if req.Window != "" {
		d, err := time.ParseDuration(req.Window)
		if err != nil || d < 0 {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("window %q: want a non-negative duration such as 60s", req.Window))
			return
		}
		window = d
	}
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	if p.State != directory.StateMigrating {
		fail(w, refuse("%s is %s; cutover happens from MIGRATING", key, p.State))
		return
	}
	s.mu.Lock()
	pr, reported := s.progress[key]
	s.mu.Unlock()
	switch {
	case !reported:
		fail(w, refuse("no mover has reported on %s to this proxy; run `shunt migrate run %s --until-converged` first", key, key))
		return
	case pr.Source != p.Source || pr.Primary != p.Primary || !pr.Converged:
		fail(w, refuse("the mover has not converged on %s (last report: pass %d, %d copied, %d failed, done %v); run it until a pass copies nothing", key, pr.Pass, pr.Copied, pr.Failed, pr.Done))
		return
	}
	before := counters(s.Metrics.FallbackReads, key, "")[""]
	s.info(r, "cutover window started", "placement", key, "window", window.String(), "fallback_reads", before)
	if err := s.sleep(r.Context(), window); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "cutover window interrupted: "+err.Error())
		return
	}
	after := counters(s.Metrics.FallbackReads, key, "")[""]
	if after != before {
		fail(w, refuse("reads still fall back to the source of %s: %v fallback reads during the %s window; something the mover has not copied is still being read", key, after-before, window))
		return
	}
	ev := &directory.CutoverEvidence{At: s.now().UTC().Truncate(time.Second), Window: window, FallbackReads: after}
	res, err := s.transition(r, key, p, f, directory.Transition{To: directory.StateCutover, Cutover: ev}, false, false)
	if err != nil {
		fail(w, err)
		return
	}
	s.logTransition(r, "cutover", res, "window", window.String(), "fallback_reads", after)
	writeJSON(w, http.StatusOK, res)
}

// PurgeResult is what purge-source removed.
type PurgeResult struct {
	Key            string `json:"key"`
	Source         string `json:"source"`
	Bucket         string `json:"bucket"`
	ObjectsDeleted int    `json:"objects_deleted"`
	UploadsAborted int    `json:"uploads_aborted"`
	Version        int64  `json:"version"`
}

func (s *Server) purgeSource(w http.ResponseWriter, r *http.Request) {
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	switch {
	case p.State != directory.StateCutover:
		fail(w, refuse("%s is %s; purge-source runs on a placement in CUTOVER", key, p.State))
		return
	case p.Cutover == nil:
		fail(w, refuse("%s has no cutover evidence: it was cut over without shunt cutover's convergence and fallback checks, so its source is not purged", key))
		return
	}
	src, err := s.backendFor(p.Source)
	if err != nil {
		fail(w, err)
		return
	}
	dst, err := s.backendFor(p.Primary)
	if err != nil {
		fail(w, err)
		return
	}
	srcBucket, dstBucket := p.Names[p.Source], p.Names[p.Primary]
	ctx := r.Context()
	missing, err := missingOn(ctx, src, srcBucket, dst, dstBucket, 20)
	if err != nil {
		fail(w, err)
		return
	}
	if len(missing) > 0 {
		fail(w, refuse("the listing diff is not empty: %s/%s holds keys %s/%s does not, first %d: %s; run the mover again",
			p.Source, srcBucket, p.Primary, dstBucket, len(missing), strings.Join(missing, ", ")))
		return
	}
	objects, uploads, err := src.empty(ctx, srcBucket)
	if err != nil {
		fail(w, err)
		return
	}
	if err := src.deleteBucket(ctx, srcBucket); err != nil {
		fail(w, err)
		return
	}
	if _, err := s.transition(r, key, p, f, directory.Transition{To: directory.StateActive}, false, false); err != nil {
		fail(w, err)
		return
	}
	s.forget(key)
	v := s.Dir.Snapshot().Version()
	s.info(r, "source purged", "placement", key, "cluster", p.Source, "bucket", srcBucket, "objects_deleted", objects, "uploads_aborted", uploads,
		"state", directory.StateActive, "primary", p.Primary, "version", v)
	writeJSON(w, http.StatusOK, PurgeResult{Key: key, Source: p.Source, Bucket: srcBucket, ObjectsDeleted: objects, UploadsAborted: uploads, Version: v})
}

// finish drops the source from a CUTOVER placement without touching its data.
func (s *Server) finish(w http.ResponseWriter, r *http.Request) {
	key, p, f, ok := s.lookup(w, r)
	if !ok {
		return
	}
	res, err := s.transition(r, key, p, f, directory.Transition{To: directory.StateActive}, false, false)
	if err != nil {
		fail(w, err)
		return
	}
	s.forget(key)
	s.logTransition(r, "migrate finish", res)
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.progress, key)
}

// logTransition writes the success line for a state change.
func (s *Server) logTransition(r *http.Request, op string, res TransitionResult, extra ...any) {
	attrs := []any{"placement", res.Key, "from", res.From, "to", res.To, "primary", res.Primary}
	if res.Source != "" {
		attrs = append(attrs, "source", res.Source)
	}
	if res.Ratio > 0 {
		attrs = append(attrs, "ratio", res.Ratio)
	}
	if res.CreatedBucket != "" {
		attrs = append(attrs, "created_bucket", res.CreatedBucket)
	}
	attrs = append(attrs, extra...)
	if res.Warning != "" {
		attrs = append(attrs, "warning", res.Warning)
	}
	s.info(r, op, append(attrs, "version", res.Version)...)
}
