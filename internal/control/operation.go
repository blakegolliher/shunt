package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

// Operation records (ADR-0017). Every long-running operator action runs under a record that says
// which phase it is in, which proxies it is waiting on, and how it ended. A browser polls the
// record or follows it on GET /v1/events; the CLI's --wait polls it. A record is per operation,
// never per object, and is owned by the control node that runs it.

// Kinds of operation.
const (
	OpRamp              = "ramp"
	OpMigrate           = "migrate"
	OpMover             = "mover"
	OpCutover           = "cutover"
	OpPurge             = "purge-source"
	OpFinish            = "finish"
	OpClusterRemove     = "cluster-remove"
	OpClusterReadOnly   = "cluster-read-only"
	OpPlacementReadOnly = "placement-read-only"
	OpClusterAdd        = "cluster-add"
	OpAdopt             = "adopt"
	OpCreate            = "create"
	OpExpand            = "expand"
	OpDelete            = "delete"
	OpClearTarget       = "clear-target"
	OpCarve             = "carve"
	OpMerge             = "merge"
	OpWatch             = "watch"
)

// Statuses of an operation (ADR-0021). A request refused before any record exists is an HTTP
// error, never a record; an operation refused by the rules once it runs ends failed, with the
// refusal's code in its error. Failed never implies rolled back: EffectState says what reached the
// directory.
const (
	StatusPending   = "pending"   // accepted and its scope reserved; not started yet
	StatusRunning   = "running"   // running on its owner node
	StatusBlocked   = "blocked"   // waiting on something named in Blockers
	StatusSucceeded = "succeeded" // ended as asked
	StatusFailed    = "failed"    // ended otherwise
	StatusCancelled = "cancelled" //nolint:misspell // the contract's spelling (docs/design/distributed-correctness-contracts.md); ended by a cancel before any effect committed
)

// Effect states: what an operation did to the directory.
const (
	EffectNone      = "none"      // nothing written
	EffectCommitted = "committed" // at least one directory version written
	EffectUncertain = "uncertain" // its owner was lost while it could have been writing
)

// Phases, in the order a fenced step goes through them.
const (
	PhaseQueued       = "queued"       // waiting for the bucket's step lock
	PhasePrecondition = "precondition" // waiting for every proxy to have the current version
	PhaseHold         = "hold"         // the hold is written; waiting for every proxy to have it
	PhaseStep         = "step"         // the step is being written
	PhaseSettle       = "settle"       // the step is written; waiting for every live proxy to have it
	PhaseWindow       = "window"       // cutover: the quiet window
	PhaseDiff         = "diff"         // purge-source: the listing diff
	PhasePurge        = "purge"        // purge-source: deleting the source
	PhaseMover        = "mover"        // copying source objects and reporting pass progress
	PhaseDone         = "done"
)

const (
	defaultOperationLimit = 1000 // records a store keeps
	defaultListLimit      = 50
	maxListLimit          = 500
)

// OpProgress is how far a phase with a measurable length has come.
type OpProgress struct {
	Done  int64  `json:"done"`
	Total int64  `json:"total"`
	Unit  string `json:"unit"`
}

// Blocker is one named reason an operation is waiting or blocked. Counts and scope details are
// bounded; it never carries an object key or a credential.
type Blocker struct {
	Code    string `json:"code"`
	ProxyID string `json:"proxy_id,omitempty"`
	Message string `json:"message,omitempty"`
}

// Operation is one record.
type Operation struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Placement string `json:"placement,omitempty"`
	Cluster   string `json:"cluster,omitempty"`
	Actor     string `json:"actor"`
	Node      string `json:"node"` // the control node running it: its owner
	// Identity is the directory lineage the operation was accepted on; Scope what it reserves.
	Identity directory.Identity `json:"identity,omitzero"`
	Scope    *Scope             `json:"scope,omitempty"`
	// RequestID is the request's Idempotency-Key. IntentDigest is a keyed digest of its canonical
	// intent (kind, scope, body), which a retry with the same key must match; it is stored for
	// every control node to compare and never answered (Public).
	RequestID    string `json:"request_id,omitempty"`
	IntentDigest string `json:"intent_digest,omitempty"`
	// Sequence counts the record's writes: an update is accepted only over the sequence it read.
	Sequence    int64     `json:"sequence"`
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`
	Status      string    `json:"status"`
	EffectState string    `json:"effect_state"`
	// AllowedActions are the actions the server accepts on the record now; none yet.
	AllowedActions []string        `json:"allowed_actions"`
	Blockers       []Blocker       `json:"blockers,omitempty"`
	Phase          string          `json:"phase,omitempty"`
	WaitingOn      []string        `json:"waiting_on,omitempty"`
	Silent         []string        `json:"silent,omitempty"`
	Progress       *OpProgress     `json:"progress,omitempty"`
	Version        int64           `json:"version,omitempty"` // the directory version the step wrote
	Args           json.RawMessage `json:"args,omitempty"`    // the request as given
	Result         json.RawMessage `json:"result,omitempty"`  // the answer the route gives, once succeeded
	Error          *Error          `json:"error,omitempty"`
}

func (op *Operation) clone() *Operation {
	c := *op
	c.WaitingOn = slices.Clone(op.WaitingOn)
	c.Silent = slices.Clone(op.Silent)
	c.Scope = op.Scope.clone()
	c.AllowedActions = slices.Clone(op.AllowedActions)
	c.Blockers = slices.Clone(op.Blockers)
	c.Args = slices.Clone(op.Args)
	c.Result = slices.Clone(op.Result)
	if op.Progress != nil {
		p := *op.Progress
		c.Progress = &p
	}
	if op.Error != nil {
		e := *op.Error
		c.Error = &e
	}
	return &c
}

// Operations is where the records live. Two implementations: MemOperations for a single-node lab
// and tests, and the control plane's, in etcd (internal/cp), which every control node reads. Both
// pass the same contract test (internal/control/opstest).
type Operations interface {
	// Create writes a new record at Sequence 1. A record with a Scope reserves it in the same
	// atomic step: refused with a *ScopeBusyError while a conflicting unfinished operation exists
	// (scope.go), with a *GenerationError when the scope's generation is no longer
	// Scope.Generation, and with a lineage refusal when the directory's identity is not the
	// record's.
	Create(ctx context.Context, op *Operation) error
	// Update replaces a record whose stored Sequence is op.Sequence-1, and fails with
	// ErrStaleSequence otherwise. A terminal status releases the record's scope in the same step.
	Update(ctx context.Context, op *Operation) error
	// Get returns a record, or nil when there is none.
	Get(ctx context.Context, id string) (*Operation, error)
	// List returns the newest records first, filtered by placement or cluster when given.
	List(ctx context.Context, placement, cluster string, limit int) ([]*Operation, error)
}

// MemOperations keeps records in memory, newest last, and forgets the oldest ended ones past
// Limit. It is the single-node lab's store (ADR-0021: the lab's directory has one writer), and a
// process restart forgets its records and their reservations.
type MemOperations struct {
	Limit int // default 1000
	// Dir, if set, is the directory Create checks scope generations and identity against.
	Dir directory.Directory
	// Capacity is the limit on unfinished operations; default DefaultCapacity.
	Capacity int
	// Now dates the idempotency retention; default time.Now.
	Now func() time.Time
	// OnChange, if set, is called after every Create and Update with a copy of the record.
	OnChange func(Operation)

	mu    sync.Mutex
	order []string
	byID  map[string]*Operation
	idem  map[string]string // IdempotencyIndex → id
}

var _ Operations = (*MemOperations)(nil)

// Get implements Operations.
func (m *MemOperations) Get(_ context.Context, id string) (*Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if op, ok := m.byID[id]; ok {
		return op.clone(), nil
	}
	return nil, nil
}

// List implements Operations.
func (m *MemOperations) List(_ context.Context, placement, cluster string, limit int) ([]*Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Operation
	for i := len(m.order) - 1; i >= 0 && len(out) < limit; i-- {
		op := m.byID[m.order[i]]
		if (placement == "" || op.Placement == placement) && (cluster == "" || op.Cluster == cluster) {
			out = append(out, op.clone())
		}
	}
	return out, nil
}

func (s *Server) ops() Operations {
	if s.Ops != nil {
		return s.Ops
	}
	s.opsOnce.Do(func() { s.defaultOps = &MemOperations{Dir: s.Dir} })
	return s.defaultOps
}

func (s *Server) node() string {
	if s.Node != "" {
		return s.Node
	}
	return "lab"
}

func (s *Server) context() context.Context {
	if s.Ctx != nil {
		return s.Ctx
	}
	return context.Background()
}

// tracker is one operation in flight: its context, its actor, and its record. The fence functions
// report their phase and the proxies they wait on through it.
type tracker struct {
	s     *Server
	ctx   context.Context
	actor string
	async bool // started by POST /v1/operations: its outcome is logged here, not by the route

	mu           sync.Mutex
	op           Operation
	lastProgress time.Time
	// lost is set once an update found the record written by someone else (ErrStaleSequence): a
	// node that saw this one's owner lost took it over, and this tracker writes it no more.
	lost bool
	// startVersion is the directory version when the record was created.
	startVersion int64
}

func (tr *tracker) id() string { return tr.op.ID }

// snapshot returns a copy of the record.
func (tr *tracker) snapshot() Operation {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return *tr.op.clone()
}

func (tr *tracker) put() {
	if tr.lost {
		return
	}
	tr.op.Updated = tr.s.now().UTC()
	tr.op.Sequence++
	c := tr.op.clone()
	err := tr.s.ops().Update(context.WithoutCancel(tr.ctx), c)
	switch {
	case errors.Is(err, ErrStaleSequence):
		tr.lost = true
		if tr.s.Log != nil {
			tr.s.Log.Error("operation record taken over by another control node; this node stops writing it", "operation", tr.op.ID, "kind", tr.op.Kind)
		}
	case err != nil:
		tr.op.Sequence-- // not written: the next write is still over the stored sequence
		if tr.s.Log != nil {
			tr.s.Log.Warn("operation record not written", "operation", tr.op.ID, "kind", tr.op.Kind, "err", err.Error())
		}
	}
}

func (tr *tracker) phase(p string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.op.Phase == p {
		return
	}
	tr.op.Phase = p
	tr.put()
}

func (tr *tracker) waiting(ids []string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if slices.Equal(tr.op.WaitingOn, ids) {
		return
	}
	tr.op.WaitingOn = slices.Clone(ids)
	tr.put()
}

// progress records how far a phase has come; it writes the record at most once a second, and
// always when the phase completes.
func (tr *tracker) progress(done, total int64, unit string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.op.Progress = &OpProgress{Done: done, Total: total, Unit: unit}
	now := tr.s.now()
	if done < total && !tr.lastProgress.IsZero() && now.Sub(tr.lastProgress) < time.Second {
		return
	}
	tr.lastProgress = now
	tr.put()
}

// finish closes the record with the outcome.
func (tr *tracker) finish(res any, err error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.op.Phase = PhaseDone
	switch {
	case err != nil:
		_, e := errorOf(err)
		tr.op.Error = &e
		tr.op.Status = StatusFailed
	default:
		tr.op.Status = StatusSucceeded
		if res != nil {
			if raw, merr := json.Marshal(res); merr == nil {
				tr.op.Result = raw
				var v struct {
					Version int64 `json:"version"`
				}
				_ = json.Unmarshal(raw, &v) //nolint:errcheck // a result without a version is fine
				tr.op.Version = v.Version
			}
		}
	}
	tr.settleEffect()
	tr.put()
	if tr.async && err != nil && tr.s.Log != nil {
		event := tr.op.Kind + " failed"
		if tr.op.Error.Code == "refused" {
			event = tr.op.Kind + " refused"
		}
		attrs := []any{"actor", tr.actor, "operation", tr.op.ID}
		if tr.op.Placement != "" {
			attrs = append(attrs, "placement", tr.op.Placement)
		}
		if tr.op.Cluster != "" {
			attrs = append(attrs, "cluster", tr.op.Cluster)
		}
		tr.s.Log.Warn(event, append(attrs, "code", tr.op.Error.Code, "reason", tr.op.Error.Message)...)
	}
}

// finishHTTP closes a record around an existing synchronous route. These short UI actions keep
// their established handlers while still producing the same operation/SSE history as fenced
// actions. Their request bodies are deliberately not recorded because cluster add may carry a
// typed secret and adopt may carry client secrets.
func (tr *tracker) finishHTTP(status int, body []byte) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.op.Phase = PhaseDone
	if status >= 200 && status < 300 {
		tr.op.Status = StatusSucceeded
		if json.Valid(body) {
			tr.op.Result = bytes.TrimSpace(slices.Clone(body))
			var v struct {
				Version int64  `json:"version"`
				Name    string `json:"name"`
				Key     string `json:"key"`
			}
			_ = json.Unmarshal(body, &v) //nolint:errcheck // a result without a version is fine
			tr.op.Version = v.Version
			if tr.op.Cluster == "" && tr.op.Kind == OpClusterAdd {
				tr.op.Cluster = v.Name
			}
			if tr.op.Placement == "" {
				tr.op.Placement = v.Key
			}
		}
	} else {
		var e Error
		if json.Unmarshal(body, &e) != nil || e.Code == "" {
			e = Error{Code: "failed", Message: http.StatusText(status)}
		}
		tr.op.Error = &e
		tr.op.Status = StatusFailed
	}
	tr.settleEffect()
	tr.put()
}

// settleEffect records, on an ending operation, whether it wrote the directory: the directory
// version moved past the one it started on while it ran. Another writer's change in the same time
// counts too; committed is the safe side of the two. tr.mu is held.
func (tr *tracker) settleEffect() {
	if tr.op.EffectState == EffectNone && (tr.op.Version > 0 || tr.s.Dir.Snapshot().Version() > tr.startVersion) {
		tr.op.EffectState = EffectCommitted
	}
}

type operationWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *operationWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *operationWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.body.Write(p)
	return w.ResponseWriter.Write(p)
}

// recorded wraps a short mutation with an operation record, which reserves its scope while it
// runs, without changing its HTTP contract otherwise: a scope another operation owns answers 409
// operation_conflict before the handler runs.
func (s *Server) recorded(kind string, scope func(*http.Request) (Operation, error), h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		meta, err := readMeta(w, r)
		if err != nil {
			fail(w, err)
			return
		}
		op, err := scope(r)
		if err != nil {
			fail(w, err)
			return
		}
		op.Kind = kind
		meta.apply(s, &op, peekBody(r))
		tr, err := s.begin(actor(r), op, nil, false)
		if s.replayed(w, r, err) {
			return
		}
		if err != nil {
			fail(w, err)
			return
		}
		tr.running()
		ow := &operationWriter{ResponseWriter: w}
		h(ow, r)
		status := ow.status
		if status == 0 {
			status = http.StatusOK
		}
		tr.finishHTTP(status, ow.body.Bytes())
	}
}

// begin creates a pending record, reserving its scope, and returns its tracker. A scope another
// operation owns, a generation the directory has moved past, or another lineage refuses here,
// before anything runs.
func (s *Server) begin(actor string, op Operation, args any, async bool) (*tracker, error) {
	now := s.now().UTC()
	var rnd [3]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, err
	}
	snap := s.Dir.Snapshot()
	op.ID = fmt.Sprintf("%013d-%s", now.UnixMilli(), hex.EncodeToString(rnd[:]))
	op.Actor, op.Node = actor, s.node()
	op.Identity = snap.File().Identity
	op.Created, op.Updated = now, now
	op.Status, op.Phase, op.Sequence = StatusPending, PhaseQueued, 1
	op.EffectState, op.AllowedActions = EffectNone, []string{}
	if args != nil {
		raw, err := json.Marshal(args)
		if err != nil {
			return nil, err
		}
		op.Args = raw
	}
	tr := &tracker{s: s, ctx: s.context(), actor: actor, async: async, op: op, startVersion: snap.Version()}
	if err := s.ops().Create(tr.ctx, op.clone()); err != nil {
		return nil, err
	}
	return tr, nil
}

// running marks a pending record as started.
func (tr *tracker) running() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.op.Status = StatusRunning
	tr.put()
}

// outcome is what an operation ended with.
type outcome struct {
	res any
	err error
}

// operate runs fn under tr's record on the server's context, not a request's, and returns its
// outcome on a channel: the routes wait for it, POST /v1/operations answers 202 and lets it run.
func (s *Server) operate(tr *tracker, fn func(*tracker) (any, error)) <-chan outcome {
	ch := make(chan outcome, 1)
	go func() {
		tr.running()
		var out outcome
		func() {
			defer func() {
				if p := recover(); p != nil {
					out.err = fmt.Errorf("%s: internal error: %v", tr.op.Kind, p)
					if s.Log != nil {
						s.Log.Error("operation panicked", "operation", tr.op.ID, "kind", tr.op.Kind, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
					}
				}
			}()
			out.res, out.err = fn(tr)
		}()
		tr.finish(out.res, out.err)
		ch <- out
	}()
	return ch
}

// OperationRequest is POST /v1/operations: an action by kind, with the request body the action's
// own route takes as args.
type OperationRequest struct {
	Kind      string          `json:"kind"`
	Placement string          `json:"placement,omitempty"` // tenant/bucket
	Cluster   string          `json:"cluster,omitempty"`
	Args      json.RawMessage `json:"args,omitempty"`
}

// OperationList is GET /v1/operations.
type OperationList struct {
	Operations []Operation `json:"operations"`
}

// decodeArgs reads an operation's args into the action's request type; none is the zero value.
func decodeArgs(raw json.RawMessage, v any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return bad("args: %v", err)
	}
	return nil
}

// placementKey checks a tenant/bucket argument and that the placement exists.
func (s *Server) placementKey(arg string) (string, error) {
	tenant, bucket, ok := strings.Cut(arg, "/")
	if !ok || tenant == "" || bucket == "" || strings.Contains(bucket, "/") {
		return "", bad("placement %q: want tenant/bucket", arg)
	}
	key := directory.Key(tenant, bucket)
	if _, _, err := s.placementOf(key); err != nil {
		return "", err
	}
	return key, nil
}

// launch checks an operation request, writes its record and starts it. Every action's route and
// POST /v1/operations go through here, so the checks and the run are written once.
func (s *Server) launch(actor string, req OperationRequest, async bool, meta requestMeta) (*tracker, <-chan outcome, error) {
	start := func(op Operation, args any, fn func(*tracker) (any, error)) (*tracker, <-chan outcome, error) {
		op.Kind = req.Kind
		meta.apply(s, &op, req.Args)
		tr, err := s.begin(actor, op, args, async)
		if err != nil {
			return nil, nil, err
		}
		return tr, s.operate(tr, fn), nil
	}
	switch req.Kind {
	case OpRamp:
		var a RampRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		if a.Ratio < 0 || a.Ratio > 1 || (a.Ratio == 0 && len(a.Prefixes) == 0) {
			return nil, nil, bad("a ramp step needs a ratio in (0, 1] or at least one prefix")
		}
		if _, err := parseWait(a.Wait); err != nil {
			return nil, nil, err
		}
		key, err := s.placementKey(req.Placement)
		if err != nil {
			return nil, nil, err
		}
		return start(s.placementOp(key, a.To), a, func(tr *tracker) (any, error) { return s.runRamp(tr, key, a) })
	case OpMigrate:
		var a MigrateRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		if _, err := parseWait(a.Wait); err != nil {
			return nil, nil, err
		}
		key, err := s.placementKey(req.Placement)
		if err != nil {
			return nil, nil, err
		}
		return start(s.placementOp(key, a.To), a, func(tr *tracker) (any, error) { return s.runMigrate(tr, key, a) })
	case OpMover:
		var a MoverRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		if a.MaxPasses < 0 || a.MaxPasses > maxMoverPasses {
			return nil, nil, bad("max_passes %d: want 1 through %d", a.MaxPasses, maxMoverPasses)
		}
		key, err := s.placementKey(req.Placement)
		if err != nil {
			return nil, nil, err
		}
		if err := s.checkMover(key, a); err != nil {
			return nil, nil, err
		}
		return start(s.placementOp(key), a, func(tr *tracker) (any, error) { return s.runMover(tr, key, a) })
	case OpCutover:
		var a CutoverRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		if _, err := parseWindow(a.Window); err != nil {
			return nil, nil, err
		}
		if _, err := parseWait(a.Wait); err != nil {
			return nil, nil, err
		}
		key, err := s.placementKey(req.Placement)
		if err != nil {
			return nil, nil, err
		}
		return start(s.placementOp(key), a, func(tr *tracker) (any, error) { return s.runCutover(tr, key, a) })
	case OpPurge:
		var a PurgeRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		if a.DryRun {
			return nil, nil, bad("a dry run is not an operation: POST /v1/placements/{tenant}/{bucket}/purge-source with dry_run answers at once")
		}
		if _, err := parseWait(a.Wait); err != nil {
			return nil, nil, err
		}
		key, err := s.placementKey(req.Placement)
		if err != nil {
			return nil, nil, err
		}
		return start(s.placementOp(key), a, func(tr *tracker) (any, error) { return s.runPurge(tr, key, a) })
	case OpFinish:
		if err := decodeArgs(req.Args, &struct{}{}); err != nil {
			return nil, nil, err
		}
		key, err := s.placementKey(req.Placement)
		if err != nil {
			return nil, nil, err
		}
		return start(s.placementOp(key), nil, func(tr *tracker) (any, error) { return s.runFinish(tr, key) })
	case OpClusterRemove:
		var a RemoveRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		name := req.Cluster
		if _, ok := s.Dir.Snapshot().File().Clusters[name]; !ok {
			return nil, nil, fmt.Errorf("%w: cluster %q is not in the directory", directory.ErrNotFound, name)
		}
		return start(s.clusterOp(name), a, func(tr *tracker) (any, error) { return s.runRemoveCluster(tr, name, a) })
	case OpClusterReadOnly:
		var a ReadOnlyRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		if _, err := parseWait(a.Wait); err != nil {
			return nil, nil, err
		}
		if _, ok := s.Dir.Snapshot().File().Clusters[req.Cluster]; !ok {
			return nil, nil, fmt.Errorf("%w: cluster %q is not in the directory", directory.ErrNotFound, req.Cluster)
		}
		return start(s.clusterOp(req.Cluster), a, func(tr *tracker) (any, error) {
			return s.runClusterReadOnly(tr, req.Cluster, a)
		})
	case OpPlacementReadOnly:
		var a ReadOnlyRequest
		if err := decodeArgs(req.Args, &a); err != nil {
			return nil, nil, err
		}
		if _, err := parseWait(a.Wait); err != nil {
			return nil, nil, err
		}
		key, err := s.placementKey(req.Placement)
		if err != nil {
			return nil, nil, err
		}
		return start(s.placementOp(key), a, func(tr *tracker) (any, error) {
			return s.runPlacementReadOnly(tr, key, a)
		})
	}
	return nil, nil, bad("kind %q: want one of ramp, migrate, mover, cutover, purge-source, finish, cluster-remove, cluster-read-only, placement-read-only", req.Kind)
}

// serveOperation runs an action for its own route and answers with its outcome, as the route did
// before records existed. The operation runs on the server's context, not the request's: a client
// that leaves, or a CLI killed while it waits, does not stop a fenced step halfway, and the record
// carries the outcome either way.
func (s *Server) serveOperation(w http.ResponseWriter, r *http.Request, req OperationRequest, args any) {
	meta, err := readMeta(w, r)
	if err != nil {
		fail(w, err)
		return
	}
	raw, err := json.Marshal(args)
	if err != nil {
		fail(w, err)
		return
	}
	req.Args = raw
	_, ch, err := s.launch(actor(r), req, false, meta)
	if s.replayed(w, r, err) {
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	select {
	case out := <-ch:
		if out.err != nil {
			fail(w, out.err)
			return
		}
		writeJSON(w, http.StatusOK, out.res)
	case <-r.Context().Done():
		// The client left; the operation completes on its own and its record has the outcome.
	}
}

func (s *Server) startOperation(w http.ResponseWriter, r *http.Request) {
	meta, err := readMeta(w, r)
	if err != nil {
		fail(w, err)
		return
	}
	var req OperationRequest
	if !decode(w, r, &req) {
		return
	}
	tr, _, err := s.launch(actor(r), req, true, meta)
	var rp *IdempotentReplay
	if errors.As(err, &rp) {
		// A retry of an accepted request: its record as it stands, not a second run.
		w.Header().Set("Location", "/v1/operations/"+rp.Existing.ID)
		writeJSON(w, http.StatusOK, rp.Existing.Public())
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Location", "/v1/operations/"+tr.id())
	writeJSON(w, http.StatusAccepted, tr.snapshot().Public())
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	op, err := s.ops().Get(r.Context(), id)
	switch {
	case err != nil:
		fail(w, err)
	case op == nil:
		fail(w, notFound("no operation %s", id))
	default:
		writeJSON(w, http.StatusOK, op.Public())
	}
}

func (s *Server) listOperations(w http.ResponseWriter, r *http.Request) {
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
	ops, err := s.ops().List(r.Context(), q.Get("placement"), q.Get("cluster"), limit)
	if err != nil {
		fail(w, err)
		return
	}
	out := OperationList{Operations: make([]Operation, 0, len(ops))}
	for _, op := range ops {
		out.Operations = append(out.Operations, op.Public())
	}
	writeJSON(w, http.StatusOK, out)
}

// FailOrphans marks the records this node was running when it last stopped as failed: their
// goroutines died with the process. A held step they left completes by repeating it (ADR-0016).
func (s *Server) FailOrphans(ctx context.Context) error {
	ops, err := s.ops().List(ctx, "", "", defaultOperationLimit)
	if err != nil {
		return err
	}
	var errs []error
	for _, op := range ops {
		if op.Node != s.node() || op.Terminal() {
			continue
		}
		orphan(op, s.now().UTC(), "the control node running this operation restarted before it finished; repeat the step to complete it")
		if err := s.ops().Update(ctx, op); err != nil {
			errs = append(errs, err)
			continue
		}
		if s.Log != nil {
			s.Log.Warn("operation failed by a restart", "operation", op.ID, "kind", op.Kind, "placement", op.Placement, "cluster", op.Cluster)
		}
	}
	return errors.Join(errs...)
}

// Orphan ends a record whose owner stopped running it (a control node that restarted, or whose
// liveness key in the control plane lapsed), ready for Update.
func Orphan(op *Operation, now time.Time, why string) { orphan(op, now, why) }

// orphan ends a record whose owner stopped running it. Its effect is uncertain unless it had not
// started: a step interrupted between its writes may have left a hold, which repeating the step
// completes (ADR-0016). An uncertain record stays past the history limit as evidence.
func orphan(op *Operation, now time.Time, why string) {
	if op.Status != StatusPending && (op.EffectState == EffectNone || op.EffectState == "") {
		op.EffectState = EffectUncertain
	}
	op.Status, op.Phase, op.Updated = StatusFailed, PhaseDone, now
	op.Sequence++
	op.Error = &Error{Code: "unavailable", Message: why}
}

// runningOperations lists the ids of running records for a placement, for its view.
func (s *Server) runningOperations(ctx context.Context, placement string) []string {
	ops, err := s.ops().List(ctx, placement, "", 20)
	if err != nil {
		return nil
	}
	var ids []string
	for _, op := range ops {
		if !op.Terminal() {
			ids = append(ids, op.ID)
		}
	}
	return ids
}
