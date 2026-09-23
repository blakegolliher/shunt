package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
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
	// The fleet (ADR-0016). Held: the step was written as a hold first and completed once every
	// proxy had it. Proxies: member proxies the change had to reach, besides this one. WaitingOn:
	// members that have not installed it yet, so it is not in effect everywhere (pending).
	Held      bool     `json:"held,omitempty"`
	Proxies   int      `json:"proxies,omitempty"`
	WaitingOn []string `json:"waiting_on,omitempty"`
	// Silent: members past their lease, not waited for; they refuse writes on moving buckets
	// themselves until they are back and have the change.
	Silent []string `json:"silent,omitempty"`
	// Operation is the record this change ran under (ADR-0017).
	Operation string `json:"operation,omitempty"`
}

// parseWait reads a request's wait for the fleet: a Go duration, default 30s.
func parseWait(v string) (time.Duration, error) {
	if v == "" {
		return defaultFenceWait, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, bad("wait %q: want a non-negative duration such as 30s", v)
	}
	return d, nil
}

// parseWindow reads cutover's quiet window: a Go duration, default 60s.
func parseWindow(v string) (time.Duration, error) {
	if v == "" {
		return 60 * time.Second, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, bad("window %q: want a non-negative duration such as 60s", v)
	}
	return d, nil
}

// settle waits for version v to reach every live member and records who still lacks it.
func (s *Server) settle(tr *tracker, res *TransitionResult, v int64, wait time.Duration) {
	tr.phase(PhaseSettle)
	waiting, _ := s.fenceRound(tr, v, false, wait) //nolint:errcheck // a canceled wait just leaves the change pending
	res.WaitingOn = waiting
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

// precondition refuses a fenced change while the fleet has not installed the current version: the
// previous change is not in effect everywhere, and the next step must start from one that is.
func (s *Server) precondition(tr *tracker, strict bool, wait time.Duration) error {
	tr.phase(PhasePrecondition)
	if err := s.Dir.Sync(tr.ctx); err != nil {
		return err
	}
	v := s.Dir.Snapshot().Version()
	waiting, err := s.fenceRound(tr, v, strict, wait)
	if err != nil {
		return err
	}
	if len(waiting) > 0 {
		hint := "; they install it within a heartbeat once they can reach this proxy"
		if strict {
			hint = "; a bucket's first step waits for every proxy, live or not, because one cut off before it would still write every key to the source. A proxy that is gone for good: `shunt proxy forget <id>`"
		}
		return refuse("directory version %d is not on every proxy yet, %s (after %s)%s", v, waitingOn(waiting), wait, hint)
	}
	return nil
}

// fencedStep applies a step that moves writes or reads for keys that already exist (a ramp step,
// migrate start) under ADR-0016: the fleet must be at the current version first; a step that moves
// writes to the new primary is held until every proxy has the hold when the fleet has members; and
// the answer says which members have not installed the step yet. A hold that does not reach every
// member is released, so the step either happens everywhere or nowhere.
func (s *Server) fencedStep(tr *tracker, key string, t directory.Transition, create, acceptLoss bool, wait time.Duration) (TransitionResult, error) {
	// One fenced step per bucket at a time: two operators stepping the same bucket would otherwise
	// fence, release and complete each other's holds. The placement is read under the lock, so a
	// step starts from what the previous one left.
	tr.phase(PhaseQueued)
	unlock := s.lockStep(key)
	defer unlock()
	if err := s.Dir.Sync(tr.ctx); err != nil {
		return TransitionResult{}, err
	}
	f := s.Dir.Snapshot().File()
	p, ok := f.Placements[key]
	if !ok {
		return TransitionResult{}, fmt.Errorf("%w: no bucket %s in the directory", directory.ErrNotFound, key)
	}
	// A step out of ACTIVE waits for every member, live or not. A hold taken from ACTIVE and not yet
	// completed (nothing in force, only the hold) is still that step.
	fromActive := p.State == directory.StateActive || (p.Held() && p.Ramp.Ratio == 0 && len(p.Ramp.Prefixes) == 0)
	if err := s.precondition(tr, fromActive, wait); err != nil {
		return TransitionResult{}, err
	}
	moves := t.To == directory.StateRamping ||
		(t.To == directory.StateMigrating && (p.State != directory.StateRamping || p.Ramp == nil || p.Ramp.Ratio < 1 || p.Held()))
	hold := false
	if moves {
		var err error
		if hold, err = s.counted(tr.ctx, fromActive); err != nil {
			return TransitionResult{}, err
		}
	}
	if !hold {
		tr.phase(PhaseStep)
		res, err := s.transition(tr, key, p, f, t, create, acceptLoss)
		if err != nil {
			return res, err
		}
		s.settle(tr, &res, res.Version, wait)
		return res, nil
	}

	tenant, bucket, _ := directory.SplitKey(key)
	complete := t // the step as written once every member holds it: never with a target
	complete.Target, complete.Name, complete.Complete = "", "", true
	var heldAt int64
	var created string
	if p.Held() {
		// A hold left by an interrupted call, such as a control-node restart between the hold and
		// its completion. Its keys answer 503 until it completes, so repeating the step resumes it
		// rather than being refused as "already held".
		if t.Target != "" && t.Target != p.Primary {
			return TransitionResult{}, refuse("%s is already moving to %s; a different target needs a reconcile, not a ramp step", key, p.Primary)
		}
		if _, err := directory.Apply(p, complete); err != nil {
			return TransitionResult{}, refuse("%s has a held step to %s left by an interrupted call; repeat it to complete it (%v)", key, holdText(p.Ramp.Hold), err)
		}
		heldAt = s.Dir.Snapshot().Version()
		s.info(tr.actor, "resuming a held step", "placement", key, "version", heldAt, "hold", holdText(p.Ramp.Hold))
	} else {
		// Anything that would refuse the completed step refuses before the hold is written.
		np, err := directory.Apply(p, withDefaultName(key, p, t))
		if err != nil {
			return TransitionResult{}, err
		}
		if np.State == directory.StateMigrating && !f.Clusters[np.Primary].Capabilities.ConditionalWriteOr(true) && !acceptLoss {
			return TransitionResult{}, lostWriteWindow(key, np.Primary)
		}
		th := t
		th.Hold = true
		tr.phase(PhaseStep)
		held, err := s.transition(tr, key, p, f, th, create, acceptLoss)
		if err != nil {
			return held, err
		}
		heldAt, created = held.Version, held.CreatedBucket
		s.info(tr.actor, "ramp step held", "placement", key, "version", heldAt, "to", t.To, "ratio", t.Ratio, "prefixes", t.Prefixes)
	}
	release := func(why string) error {
		if rerr := s.Dir.SetState(context.WithoutCancel(tr.ctx), tenant, bucket, directory.StateRamping, directory.Transition{Release: true}, tr.actor); rerr != nil {
			return fmt.Errorf("%s, and releasing the hold failed: %w; repeat the step once the fleet is back to complete it", why, rerr)
		}
		s.info(tr.actor, "held step released", "placement", key, "reason", why)
		return refuse("%s; the held step was released and nothing changed", why)
	}
	tr.phase(PhaseHold)
	waiting, err := s.fenceRound(tr, heldAt, fromActive, wait)
	switch {
	case err != nil:
		return TransitionResult{}, release("waiting for the fleet was interrupted: " + err.Error())
	case len(waiting) > 0:
		return TransitionResult{}, release(fmt.Sprintf("the held step did not reach every proxy within %s, %s", wait, waitingOn(waiting)))
	}
	f2 := s.Dir.Snapshot().File()
	p2, ok := f2.Placements[key]
	if !ok || !p2.Held() {
		return TransitionResult{}, fmt.Errorf("%w: %s changed while its step was held", directory.ErrConflict, key)
	}
	tr.phase(PhaseStep)
	res, err := s.transition(tr, key, p2, f2, complete, false, acceptLoss)
	if err != nil {
		return res, release("completing the held step failed: " + err.Error())
	}
	res.From, res.Held, res.CreatedBucket = p.State, true, created
	s.settle(tr, &res, res.Version, wait)
	return res, nil
}

// lockStep serializes fenced steps on one placement key and returns the unlock.
func (s *Server) lockStep(key string) func() {
	v, _ := s.steps.LoadOrStore(key, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// holdText describes a held step for an operator.
func holdText(h *directory.RampHold) string {
	switch {
	case h.Ratio >= 1:
		return "every key (ratio 1)"
	case len(h.Prefixes) == 0:
		return fmt.Sprintf("ratio %v", h.Ratio)
	default:
		return fmt.Sprintf("ratio %v, prefixes %s", h.Ratio, strings.Join(h.Prefixes, " "))
	}
}

// withDefaultName fills the backend name a first step gets when neither expand nor the request
// named one, as transition does.
func withDefaultName(key string, p directory.Placement, t directory.Transition) directory.Transition {
	if p.State == directory.StateActive && p.Target == "" && t.Target != "" && t.Name == "" {
		tenant, bucket, _ := directory.SplitKey(key)
		t.Name = directory.BackendName(tenant, bucket, 0)
	}
	return t
}

// transition applies t to a placement with every check a state change needs: the target bucket
// exists (or is created), neither side was ever versioned, and a MIGRATING target without
// conditional PUT was accepted explicitly.
func (s *Server) transition(tr *tracker, key string, p directory.Placement, f *directory.File, t directory.Transition, create, acceptLoss bool) (TransitionResult, error) {
	tenant, bucket, _ := directory.SplitKey(key)
	t = withDefaultName(key, p, t)
	if p.State != directory.StateActive && t.Target != "" {
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
	ctx, cancel := context.WithTimeout(tr.ctx, backendTimeout)
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
			if _, err := target.createBucket(ctx, name); err != nil {
				return TransitionResult{}, err
			}
			res.CreatedBucket = name
		case p.Target == "":
			// A first step that names its own target; one expand prepared was checked there.
			if err := targetEmpty(ctx, target, np.Primary, name, "expand, which can accept them (--accept-existing-objects), before the first step"); err != nil {
				return TransitionResult{}, err
			}
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
	if err := s.Dir.SetState(tr.ctx, tenant, bucket, p.State, t, tr.actor); err != nil {
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
	// Keys are the cluster's own client keys, imported so that clients keep the credentials they
	// already use (ADR-0012). Each is checked against Cluster before the placement is written.
	Keys []ClientKeyRequest `json:"keys,omitempty"`
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
	if err := s.clientNameFree(tenant, bucket); err != nil {
		fail(w, err)
		return
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
	for _, k := range req.Keys {
		k.Cluster = req.Cluster
		res, kerr := s.storeKey(ctx, tenant, k)
		if kerr != nil {
			fail(w, kerr)
			return
		}
		s.info(actor(r), "client key imported", "tenant", res.Tenant, "access_key", res.AccessKey, "checked", res.Checked)
	}
	if err := s.Dir.Adopt(r.Context(), tenant, bucket, req.Cluster, req.Name, actor(r)); err != nil {
		fail(w, err)
		return
	}
	p, _ := s.Dir.Snapshot().Lookup(tenant, bucket)
	s.info(actor(r), "bucket adopted", "placement", key, "cluster", req.Cluster, "bucket", req.Name, "state", p.State, "version", s.Dir.Snapshot().Version())
	writeJSON(w, http.StatusOK, s.placementStatus(key, *p))
}

// ExpandRequest prepares a target cluster and bucket for a placement that will move.
type ExpandRequest struct {
	To     string `json:"to"`
	Name   string `json:"name,omitempty"` // default <primary backend name>-NNN, the lowest unused
	Create bool   `json:"create,omitempty"`
	// AcceptObjects takes a target bucket that already holds objects, stating they are this
	// bucket's (copied ahead, by backend replication for instance). Without it expand refuses a
	// bucket with any object: a move would mix them in (targetEmpty).
	AcceptObjects bool `json:"accept_existing_objects,omitempty"`
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
	// Measured: the target's conditional-write capabilities were not set on the cluster, so expand
	// measured them against the target bucket and recorded them.
	Measured bool  `json:"measured,omitempty"`
	Version  int64 `json:"version"`
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
	if target.ReadOnly {
		fail(w, refuse("cluster %s is read-only for maintenance", req.To))
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
		if _, err = tb.createBucket(ctx, req.Name); err != nil {
			fail(w, err)
			return
		}
		res.CreatedBucket = true
	case !req.AcceptObjects:
		if err = targetEmpty(ctx, tb, req.To, req.Name, "expand --accept-existing-objects"); err != nil {
			fail(w, err)
			return
		}
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
	if target.Capabilities.ConditionalWrite == nil || target.Capabilities.ConditionalDelete == nil {
		cw, cd, perr := probeConditionals(ctx, tb, req.Name)
		if perr != nil {
			fail(w, refuse("measuring conditional writes on %s/%s failed: %v; set --conditional-write and --conditional-delete on the cluster instead", req.To, req.Name, perr))
			return
		}
		if target.Capabilities.ConditionalWrite == nil {
			target.Capabilities.ConditionalWrite = &cw
		}
		if target.Capabilities.ConditionalDelete == nil {
			target.Capabilities.ConditionalDelete = &cd
		}
		if err = s.Dir.PutCluster(r.Context(), req.To, target, "", actor(r)); err != nil {
			fail(w, err)
			return
		}
		res.Measured = true
		res.ConditionalWrite, res.ConditionalDelete = *target.Capabilities.ConditionalWrite, *target.Capabilities.ConditionalDelete
	}
	if err := s.Dir.SetTarget(r.Context(), tenant, bucket, req.To, req.Name, actor(r)); err != nil {
		fail(w, err)
		return
	}
	res.Version = s.Dir.Snapshot().Version()
	s.info(actor(r), "target recorded", "placement", key, "target", req.To, "bucket", req.Name, "created_bucket", res.CreatedBucket, "canary", "ok",
		"conditional_write", res.ConditionalWrite, "conditional_delete", res.ConditionalDelete, "measured", res.Measured, "version", res.Version)
	writeJSON(w, http.StatusOK, res)
}

// ClearTargetResult is what DELETE /v1/placements/{tenant}/{bucket}/target forgot.
type ClearTargetResult struct {
	Key     string `json:"key"`
	Target  string `json:"target"` // the cluster expand had prepared
	Name    string `json:"name"`   // its bucket there, left as it is
	Version int64  `json:"version"`
}

// clearTarget undoes expand before the first step: the target is recorded while ACTIVE and routes
// nothing, so forgetting it needs no fence. The bucket expand checked or created stays on the
// cluster; shunt may not have created it, so it never deletes it.
func (s *Server) clearTarget(w http.ResponseWriter, r *http.Request) {
	key, p, _, ok := s.lookup(w, r)
	if !ok {
		return
	}
	tenant, bucket, _ := directory.SplitKey(key)
	switch {
	case p.State != directory.StateActive:
		fail(w, refuse("%s is %s, moving to %s; a target can only be cleared before the first step", key, p.State, p.Primary))
		return
	case p.Target == "":
		fail(w, refuse("%s has no target to clear", key))
		return
	}
	res := ClearTargetResult{Key: key, Target: p.Target, Name: p.Names[p.Target]}
	if err := s.Dir.ClearTarget(r.Context(), tenant, bucket, actor(r)); err != nil {
		fail(w, err)
		return
	}
	res.Version = s.Dir.Snapshot().Version()
	s.info(actor(r), "target cleared", "placement", key, "target", res.Target, "bucket", res.Name, "version", res.Version)
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

// probeConditionals measures what the mover's guards rely on (ADR-0004): whether the backend refuses
// a PUT with If-None-Match: * over an existing object (412), and a DELETE whose If-Match names the
// wrong ETag (412). It works on one scratch object, which it removes. A backend that ignores a header
// simply does the write, which is the answer.
func probeConditionals(ctx context.Context, b backend, bucket string) (conditionalWrite, conditionalDelete bool, err error) {
	var id [8]byte
	_, _ = rand.Read(id[:])
	key := ".shunt-probe-" + hex.EncodeToString(id[:])
	put, err := b.do(ctx, http.MethodPut, bucket, key, nil, []byte("shunt capability probe"), nil)
	if err != nil {
		return false, false, err
	}
	if put.status != http.StatusOK {
		return false, false, fmt.Errorf("PUT answered HTTP %d %s", put.status, put.code)
	}
	defer func() { _, _ = b.do(context.WithoutCancel(ctx), http.MethodDelete, bucket, key, nil, nil, nil) }()
	again, err := b.do(ctx, http.MethodPut, bucket, key, nil, []byte("overwritten"), map[string]string{"If-None-Match": "*"})
	if err != nil {
		return false, false, err
	}
	conditionalWrite = again.status == http.StatusPreconditionFailed
	del, err := b.do(ctx, http.MethodDelete, bucket, key, nil, nil, map[string]string{"If-Match": `"00000000000000000000000000000000"`})
	if err != nil {
		return false, false, err
	}
	conditionalDelete = del.status == http.StatusPreconditionFailed
	return conditionalWrite, conditionalDelete, nil
}

// RampRequest is one ramp step: a higher ratio, more prefixes, or both.
type RampRequest struct {
	Ratio    float64  `json:"ratio,omitempty"`
	Prefixes []string `json:"prefixes,omitempty"`
	To       string   `json:"to,omitempty"`   // leaving ACTIVE without expand: the target cluster
	Name     string   `json:"name,omitempty"` // and its backend bucket
	Create   bool     `json:"create,omitempty"`
	Wait     string   `json:"wait,omitempty"` // how long to wait for the fleet; default 30s
}

func (s *Server) ramp(w http.ResponseWriter, r *http.Request) {
	var req RampRequest
	if !decode(w, r, &req) {
		return
	}
	s.serveOperation(w, r, OperationRequest{Kind: OpRamp, Placement: pathKey(r)}, req)
}

func (s *Server) runRamp(tr *tracker, key string, req RampRequest) (TransitionResult, error) {
	wait, err := parseWait(req.Wait)
	if err != nil {
		return TransitionResult{}, err
	}
	res, err := s.fencedStep(tr, key, directory.Transition{To: directory.StateRamping, Target: req.To, Name: req.Name, Ratio: req.Ratio, Prefixes: req.Prefixes}, req.Create, false, wait)
	if err != nil {
		return res, err
	}
	res.Operation = tr.id()
	s.logTransition(tr, "ramp", res, "prefixes", req.Prefixes)
	return res, nil
}

// MigrateRequest moves a placement to MIGRATING: every write to the new primary, reads falling back.
type MigrateRequest struct {
	To                    string `json:"to,omitempty"`
	Name                  string `json:"name,omitempty"`
	Create                bool   `json:"create,omitempty"`
	AcceptLostWriteWindow bool   `json:"accept_lost_write_window,omitempty"`
	Wait                  string `json:"wait,omitempty"` // how long to wait for the fleet; default 30s
}

func (s *Server) migrateStart(w http.ResponseWriter, r *http.Request) {
	var req MigrateRequest
	if !decode(w, r, &req) {
		return
	}
	s.serveOperation(w, r, OperationRequest{Kind: OpMigrate, Placement: pathKey(r)}, req)
}

func (s *Server) runMigrate(tr *tracker, key string, req MigrateRequest) (TransitionResult, error) {
	wait, err := parseWait(req.Wait)
	if err != nil {
		return TransitionResult{}, err
	}
	res, err := s.fencedStep(tr, key, directory.Transition{To: directory.StateMigrating, Target: req.To, Name: req.Name}, req.Create, req.AcceptLostWriteWindow, wait)
	if err != nil {
		return res, err
	}
	res.Operation = tr.id()
	s.logTransition(tr, "migrate start", res, "accept_lost_write_window", req.AcceptLostWriteWindow)
	return res, nil
}

// Progress is a mover's report on one placement, held in memory by this proxy for status and for
// cutover's convergence check. It is counts and a cursor key, never a per-object record.
type Progress struct {
	Source    string       `json:"source"`
	Primary   string       `json:"primary"`
	Pass      int          `json:"pass"`
	Copied    int          `json:"copied"`
	Skipped   int          `json:"skipped"`
	Vanished  int          `json:"vanished"`
	Failed    int          `json:"failed"`
	Bytes     int64        `json:"bytes"`
	LastKey   string       `json:"last_key,omitempty"`
	Done      bool         `json:"done"`      // the pass reached the end of the source listing
	Converged bool         `json:"converged"` // a completed pass copied nothing and failed nothing
	UpdatedAt time.Time    `json:"updated_at"`
	Ranges    []MoverRange `json:"ranges,omitempty"`
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
	s.setMoverProgress(key, req)
	if req.Done {
		s.info(actor(r), "mover pass", "placement", key, "pass", req.Pass, "copied", req.Copied, "already_there", req.Skipped, "vanished", req.Vanished,
			"failed", req.Failed, "bytes", req.Bytes, "converged", req.Converged)
	}
	writeJSON(w, http.StatusOK, req)
}

// CutoverRequest asks to stop reading the source, after a quiet window.
type CutoverRequest struct {
	Window string `json:"window,omitempty"` // Go duration; default 60s
	Wait   string `json:"wait,omitempty"`   // how long to wait for the fleet; default 30s
}

func (s *Server) cutover(w http.ResponseWriter, r *http.Request) {
	var req CutoverRequest
	if !decode(w, r, &req) {
		return
	}
	s.serveOperation(w, r, OperationRequest{Kind: OpCutover, Placement: pathKey(r)}, req)
}

func (s *Server) runCutover(tr *tracker, key string, req CutoverRequest) (TransitionResult, error) {
	window, err := parseWindow(req.Window)
	if err != nil {
		return TransitionResult{}, err
	}
	wait, err := parseWait(req.Wait)
	if err != nil {
		return TransitionResult{}, err
	}
	p, f, err := s.placementOf(key)
	if err != nil {
		return TransitionResult{}, err
	}
	if p.State != directory.StateMigrating {
		return TransitionResult{}, refuse("%s is %s; cutover happens from MIGRATING", key, p.State)
	}
	if perr := s.precondition(tr, false, wait); perr != nil {
		return TransitionResult{}, perr
	}
	s.mu.Lock()
	pr, reported := s.progress[key]
	s.mu.Unlock()
	switch {
	case !reported:
		return TransitionResult{}, refuse("no mover has reported on %s to this proxy; run `shunt migrate run %s --until-converged` first", key, key)
	case pr.Source != p.Source || pr.Primary != p.Primary || !pr.Converged:
		return TransitionResult{}, refuse("the mover has not converged on %s (last report: pass %d, %d copied, %d failed, done %v); run it until a pass copies nothing", key, pr.Pass, pr.Copied, pr.Failed, pr.Done)
	}
	// The window counts fallback reads on this proxy and on every live member (ADR-0016).
	before, beats, err := s.fleetFallbackReads(tr.ctx, key)
	if err != nil {
		return TransitionResult{}, err
	}
	s.info(tr.actor, "cutover window started", "placement", key, "window", window.String(), "fallback_reads", before, "members", len(beats))
	tr.phase(PhaseWindow)
	seconds := int64(window / time.Second)
	tr.progress(0, seconds, "seconds")
	if serr := s.sleep(tr.ctx, window); serr != nil {
		return TransitionResult{}, fmt.Errorf("%w: cutover window interrupted: %w", ErrUnavailable, serr)
	}
	tr.progress(seconds, seconds, "seconds")
	silent, err := s.awaitReports(tr.ctx, beats, wait)
	if err != nil {
		return TransitionResult{}, fmt.Errorf("%w: cutover window interrupted: %w", ErrUnavailable, err)
	}
	if len(silent) > 0 {
		return TransitionResult{}, refuse("proxies %s did not report after the %s window, so their reads are not evidence of quiet; run cutover again once they are back", strings.Join(silent, ", "), window)
	}
	after, _, err := s.fleetFallbackReads(tr.ctx, key)
	if err != nil {
		return TransitionResult{}, err
	}
	if after != before {
		return TransitionResult{}, refuse("reads still fall back to the source of %s: %v fallback reads during the %s window; something the mover has not copied is still being read", key, after-before, window)
	}
	ev := &directory.CutoverEvidence{At: s.now().UTC().Truncate(time.Second), Window: window, FallbackReads: after}
	tr.phase(PhaseStep)
	res, err := s.transition(tr, key, p, f, directory.Transition{To: directory.StateCutover, Cutover: ev}, false, false)
	if err != nil {
		return res, err
	}
	s.settle(tr, &res, res.Version, wait)
	res.Operation = tr.id()
	s.logTransition(tr, "cutover", res, "window", window.String(), "fallback_reads", after)
	return res, nil
}

// PurgeRequest is purge-source's body: a dry run, or the confirmation token the dry run issued.
type PurgeRequest struct {
	DryRun bool   `json:"dry_run,omitempty"`
	Token  string `json:"token,omitempty"`
	Wait   string `json:"wait,omitempty"` // how long to wait for the fleet; default 30s
}

// PurgeResult is what purge-source removed.
type PurgeResult struct {
	Key            string `json:"key"`
	Source         string `json:"source"`
	Bucket         string `json:"bucket"`
	ObjectsDeleted int    `json:"objects_deleted"`
	UploadsAborted int    `json:"uploads_aborted"`
	Version        int64  `json:"version"`
	Operation      string `json:"operation,omitempty"`
}

// PurgeDryRun is what purge-source would do (ADR-0017): every check the real call makes, the
// source counted, and the token the real call must present. Allowed false carries the refusal.
type PurgeDryRun struct {
	Allowed         bool      `json:"allowed"`
	Reason          string    `json:"reason,omitempty"`
	Key             string    `json:"key"`
	Source          string    `json:"source,omitempty"`
	Bucket          string    `json:"bucket,omitempty"` // the source bucket's name on its cluster
	Objects         int       `json:"objects"`
	Bytes           int64     `json:"bytes"`
	UploadsInFlight int       `json:"uploads_in_flight"`
	Missing         []string  `json:"missing"` // source keys the primary lacks, first 20
	Version         int64     `json:"version"`
	Token           string    `json:"token,omitempty"`
	ExpiresAt       time.Time `json:"expires_at,omitzero"`
}

func (s *Server) purgeSource(w http.ResponseWriter, r *http.Request) {
	var req PurgeRequest
	if !decodeOptional(w, r, &req) {
		return
	}
	key := pathKey(r)
	if !req.DryRun {
		s.serveOperation(w, r, OperationRequest{Kind: OpPurge, Placement: key}, req)
		return
	}
	res, err := s.purgeDryRun(&tracker{s: s, ctx: r.Context(), actor: actor(r)}, key, req)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// purgeBinding is what a purge token is bound to: the placement and the source cluster's definition.
func purgeBinding(f *directory.File, p directory.Placement) [32]byte {
	return digestOf(p, f.Clusters[p.Source])
}

// purgePlan is what purge-source found out before deleting anything.
type purgePlan struct {
	p                    directory.Placement
	f                    *directory.File
	src                  backend
	srcBucket, dstBucket string
	objects              int
	bytes                int64
	missing              []string
}

// purgeChecks runs every refusal purge-source gives before it deletes: the state, the evidence,
// the fence, and the listing diff, counting the source on the way. A refusal comes back with as
// much of the plan as was gathered, for a dry run to show.
func (s *Server) purgeChecks(tr *tracker, key string, wait time.Duration) (purgePlan, error) {
	var plan purgePlan
	p, f, err := s.placementOf(key)
	if err != nil {
		return plan, err
	}
	plan.p, plan.f = p, f
	if serr := purgeState(key, p); serr != nil {
		return plan, serr
	}
	// A proxy that has not installed the cutover still reads the source on a miss: it must have it
	// before the source is deleted.
	if perr := s.precondition(tr, false, wait); perr != nil {
		return plan, perr
	}
	if plan.src, err = s.backendFor(p.Source); err != nil {
		return plan, err
	}
	dst, err := s.backendFor(p.Primary)
	if err != nil {
		return plan, err
	}
	plan.srcBucket, plan.dstBucket = p.Names[p.Source], p.Names[p.Primary]
	tr.phase(PhaseDiff)
	plan.missing, plan.objects, plan.bytes, err = missingOn(tr.ctx, plan.src, plan.srcBucket, dst, plan.dstBucket, 20)
	if err != nil {
		return plan, err
	}
	if len(plan.missing) > 0 {
		return plan, refuse("the listing diff is not empty: %s/%s holds keys %s/%s does not, first %d: %s; run the mover again",
			p.Source, plan.srcBucket, p.Primary, plan.dstBucket, len(plan.missing), strings.Join(plan.missing, ", "))
	}
	return plan, nil
}

// purgeState is what purge-source refuses on the placement alone.
func purgeState(key string, p directory.Placement) error {
	switch {
	case p.State != directory.StateCutover:
		return refuse("%s is %s; purge-source runs on a placement in CUTOVER", key, p.State)
	case p.Cutover == nil:
		return refuse("%s has no cutover evidence: it was cut over without shunt cutover's convergence and fallback checks, so its source is not purged", key)
	}
	return nil
}

func (s *Server) purgeDryRun(tr *tracker, key string, req PurgeRequest) (PurgeDryRun, error) {
	wait, err := parseWait(req.Wait)
	if err != nil {
		return PurgeDryRun{}, err
	}
	res := PurgeDryRun{Key: key, Missing: []string{}, Version: s.Dir.Snapshot().Version()}
	plan, err := s.purgeChecks(tr, key, wait)
	res.Source, res.Bucket, res.Objects, res.Bytes = plan.p.Source, plan.srcBucket, plan.objects, plan.bytes
	if plan.missing != nil {
		res.Missing = plan.missing
	}
	if err != nil {
		if status, e := errorOf(err); status == http.StatusConflict {
			res.Reason = e.Message
			return res, nil
		}
		return res, err
	}
	ctx, cancel := context.WithTimeout(tr.ctx, backendTimeout)
	defer cancel()
	if res.UploadsInFlight, err = s.uploadsInProgress(ctx, plan.p.Source, plan.srcBucket); err != nil {
		return res, err
	}
	res.Allowed = true
	res.Token, res.ExpiresAt = s.confirmToken("purge-source", purgeBinding(plan.f, plan.p))
	return res, nil
}

func (s *Server) runPurge(tr *tracker, key string, req PurgeRequest) (PurgeResult, error) {
	wait, err := parseWait(req.Wait)
	if err != nil {
		return PurgeResult{}, err
	}
	// The cheap refusals first, so an operator hears about the state before the token; then the
	// token's presence, before the fence and the listing diff are paid for; then its binding.
	p, _, err := s.placementOf(key)
	if err != nil {
		return PurgeResult{}, err
	}
	if serr := purgeState(key, p); serr != nil {
		return PurgeResult{}, serr
	}
	if req.Token == "" {
		return PurgeResult{}, s.checkToken("", "purge-source", [32]byte{})
	}
	plan, err := s.purgeChecks(tr, key, wait)
	if err != nil {
		return PurgeResult{}, err
	}
	if terr := s.checkToken(req.Token, "purge-source", purgeBinding(plan.f, plan.p)); terr != nil {
		return PurgeResult{}, terr
	}
	p = plan.p
	tr.phase(PhasePurge)
	total := int64(plan.objects)
	tr.progress(0, total, "objects")
	objects, uploads, err := plan.src.empty(tr.ctx, plan.srcBucket, func(deleted int) { tr.progress(int64(deleted), max(total, int64(deleted)), "objects") })
	if err != nil {
		return PurgeResult{}, err
	}
	if err := plan.src.deleteBucket(tr.ctx, plan.srcBucket); err != nil {
		return PurgeResult{}, err
	}
	tr.phase(PhaseStep)
	if _, err := s.transition(tr, key, p, plan.f, directory.Transition{To: directory.StateActive}, false, false); err != nil {
		return PurgeResult{}, err
	}
	s.forget(key)
	v := s.Dir.Snapshot().Version()
	s.info(tr.actor, "source purged", "placement", key, "cluster", p.Source, "bucket", plan.srcBucket, "objects_deleted", objects, "uploads_aborted", uploads,
		"state", directory.StateActive, "primary", p.Primary, "version", v)
	return PurgeResult{Key: key, Source: p.Source, Bucket: plan.srcBucket, ObjectsDeleted: objects, UploadsAborted: uploads, Version: v, Operation: tr.id()}, nil
}

// finish drops the source from a CUTOVER placement without touching its data.
func (s *Server) finish(w http.ResponseWriter, r *http.Request) {
	s.serveOperation(w, r, OperationRequest{Kind: OpFinish, Placement: pathKey(r)}, nil)
}

func (s *Server) runFinish(tr *tracker, key string) (TransitionResult, error) {
	p, f, err := s.placementOf(key)
	if err != nil {
		return TransitionResult{}, err
	}
	tr.phase(PhaseStep)
	res, err := s.transition(tr, key, p, f, directory.Transition{To: directory.StateActive}, false, false)
	if err != nil {
		return res, err
	}
	s.settle(tr, &res, res.Version, defaultFenceWait)
	s.forget(key)
	res.Operation = tr.id()
	s.logTransition(tr, "migrate finish", res)
	return res, nil
}

func (s *Server) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.progress, key)
}

// logTransition writes the success line for a state change.
func (s *Server) logTransition(tr *tracker, op string, res TransitionResult, extra ...any) {
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
	if res.Held {
		attrs = append(attrs, "held", true)
	}
	if res.Proxies > 0 {
		attrs = append(attrs, "members", res.Proxies)
	}
	if len(res.WaitingOn) > 0 {
		attrs = append(attrs, "pending", true, "waiting_on", res.WaitingOn)
	}
	attrs = append(attrs, extra...)
	if res.Warning != "" {
		attrs = append(attrs, "warning", res.Warning)
	}
	s.info(tr.actor, op, append(attrs, "version", res.Version)...)
}

// CreateRequest is a member proxy's S3 CreateBucket, forwarded: the proxy claims the placement row
// here, then creates the backend bucket itself (ADR-0015).
type CreateRequest struct {
	Cluster string `json:"cluster"`
	Name    string `json:"name"` // backend bucket name
	Actor   string `json:"actor,omitempty"`
}

// VersionResult is the answer to a change whose only outcome is a new directory version.
type VersionResult struct {
	Key     string `json:"key"`
	Version int64  `json:"version"`
}

func (s *Server) createPlacement(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if !decode(w, r, &req) {
		return
	}
	tenant, bucket := r.PathValue("tenant"), r.PathValue("bucket")
	who := actor(r)
	if req.Actor != "" {
		who = req.Actor + " via " + who
	}
	if err := s.Dir.Create(r.Context(), tenant, bucket, req.Cluster, req.Name, who); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, VersionResult{Key: directory.Key(tenant, bucket), Version: s.Dir.Snapshot().Version()})
}

// clientNameFree answers a client bucket name the tenant already uses, before adopt or create
// touches a backend or imports a key, and says what the name is and what to do instead. The
// directory write stays the authority: two concurrent calls both pass here, and one of them is
// refused there.
func (s *Server) clientNameFree(tenant, bucket string) error {
	p, ok := s.Dir.Snapshot().Lookup(tenant, bucket)
	if !ok {
		return nil
	}
	return nameTaken(fmt.Sprintf("%s already exists (%s on %s as %s); choose another client bucket name, or expand %s to add a cluster to it",
		directory.Key(tenant, bucket), p.State, p.Primary, p.Names[p.Primary], bucket))
}

// nameTaken is directory.ErrExists with a message that says what to do: the API answers it as
// before (409 conflict), in these words.
type nameTaken string

func (e nameTaken) Error() string { return string(e) }
func (e nameTaken) Unwrap() error { return directory.ErrExists }

// CreateBackendRequest is the browser's Create: the member route's fields, less the forwarded
// actor, plus the client keys to import as adopt takes them.
type CreateBackendRequest struct {
	Cluster string `json:"cluster"`
	Name    string `json:"name"` // backend bucket name
	// Keys are client keys the cluster knows, imported so clients can reach the new bucket through
	// shunt (ADR-0012). Each is checked against Cluster before the bucket is created.
	Keys []ClientKeyRequest `json:"keys,omitempty"`
}

// createBackendPlacement is the browser's Create action: unlike the member-only /create route it
// creates the S3 bucket as well as recording the placement.
func (s *Server) createBackendPlacement(w http.ResponseWriter, r *http.Request) {
	var req CreateBackendRequest
	if !decode(w, r, &req) {
		return
	}
	tenant, bucket := r.PathValue("tenant"), r.PathValue("bucket")
	if req.Name == "" {
		req.Name = bucket
	}
	if !s3.ValidBucketName(req.Name) {
		writeError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("%q is not a valid bucket name", req.Name))
		return
	}
	if err := s.clientNameFree(tenant, bucket); err != nil {
		fail(w, err)
		return
	}
	if c, ok := s.Dir.Snapshot().Cluster(req.Cluster); ok && c.ReadOnly {
		fail(w, refuse("cluster %s is read-only for maintenance", req.Cluster))
		return
	}
	b, err := s.backendFor(req.Cluster)
	if err != nil {
		fail(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()
	switch exists, herr := b.bucketExists(ctx, req.Name); {
	case herr != nil:
		fail(w, herr)
		return
	case exists:
		fail(w, refuse("bucket %s already exists on %s; Adopt takes over a bucket that is already there", req.Name, req.Cluster))
		return
	}
	for _, k := range req.Keys {
		k.Cluster = req.Cluster
		res, kerr := s.storeKey(ctx, tenant, k)
		if kerr != nil {
			fail(w, kerr)
			return
		}
		s.info(actor(r), "client key imported", "tenant", res.Tenant, "access_key", res.AccessKey, "checked", res.Checked)
	}
	created, err := b.createBucket(ctx, req.Name)
	if err != nil {
		fail(w, err)
		return
	}
	if err := s.Dir.Adopt(r.Context(), tenant, bucket, req.Cluster, req.Name, actor(r)); err != nil {
		if created { // never a bucket this call found: someone may have made it in between
			_, _ = b.do(context.WithoutCancel(ctx), http.MethodDelete, req.Name, "", nil, nil, nil)
		}
		fail(w, err)
		return
	}
	key := directory.Key(tenant, bucket)
	p, _ := s.Dir.Snapshot().Lookup(tenant, bucket)
	s.info(actor(r), "bucket created", "placement", key, "cluster", req.Cluster, "bucket", req.Name, "version", s.Dir.Snapshot().Version())
	writeJSON(w, http.StatusOK, s.placementStatus(key, *p))
}

func (s *Server) deletePlacement(w http.ResponseWriter, r *http.Request) {
	tenant, bucket := r.PathValue("tenant"), r.PathValue("bucket")
	who := actor(r)
	if a := r.URL.Query().Get("actor"); a != "" {
		who = a + " via " + who
	}
	if err := s.Dir.Delete(r.Context(), tenant, bucket, who); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, VersionResult{Key: directory.Key(tenant, bucket), Version: s.Dir.Snapshot().Version()})
}
