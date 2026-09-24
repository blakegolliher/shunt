package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/blakegolliher/shunt/internal/directory"
)

// Scopes (ADR-0021, design §2 "Durable intent"). An operation that changes a placement or a
// cluster reserves it when it is created, in the same atomic step, and releases it when it ends:
// at most one unfinished operation owns a scope. A placement operation also names the clusters it
// touches; a cluster operation conflicts with any unfinished placement operation touching its
// cluster, and a placement operation with an unfinished operation on a cluster it touches. The
// reservation is checked against the scope's generation and the directory's identity, so an
// operation accepted against a view the directory has since moved past is refused.

// Scope is what an operation reserves.
type Scope struct {
	// Resource is directory.PlacementResource or directory.ClusterResource.
	Resource string `json:"resource"`
	// Generation is the resource's generation the operation was accepted against: 0 for one that
	// does not exist yet, or has not changed since the schema upgrade.
	Generation int64 `json:"generation"`
	// Clusters are the clusters a placement operation touches, sorted.
	Clusters []string `json:"clusters,omitempty"`
}

func (sc *Scope) clone() *Scope {
	if sc == nil {
		return nil
	}
	c := *sc
	c.Clusters = slices.Clone(sc.Clusters)
	return &c
}

// cluster returns the cluster a cluster scope names, and whether it is one.
func (sc *Scope) cluster() (string, bool) {
	return strings.CutPrefix(sc.Resource, "cluster:")
}

// touches reports whether a placement scope touches cluster.
func (sc *Scope) touches(cluster string) bool {
	_, isCluster := sc.cluster()
	return !isCluster && slices.Contains(sc.Clusters, cluster)
}

// Refusal codes of the operation contract (docs/design/distributed-correctness-contracts.md §1).
const (
	CodeOperationConflict  = "operation_conflict"
	CodeGenerationConflict = "generation_conflict"
	CodeOperationCapacity  = "operation_capacity"
)

// ScopeBusyError refuses an operation whose scope an unfinished operation owns.
type ScopeBusyError struct {
	Resource string // what was asked for
	Owner    string // the unfinished operation in the way
	Held     string // the resource the owner holds, when it is not Resource
}

func (e *ScopeBusyError) Error() string {
	if e.Held != "" && e.Held != e.Resource {
		return fmt.Sprintf("%s conflicts with unfinished operation %s on %s; wait for it to end or follow it on GET /v1/operations/%s", e.Resource, e.Owner, e.Held, e.Owner)
	}
	return fmt.Sprintf("%s is owned by unfinished operation %s; wait for it to end or follow it on GET /v1/operations/%s", e.Resource, e.Owner, e.Owner)
}

// GenerationError refuses an operation accepted against a generation the resource has moved past.
type GenerationError struct {
	Resource          string
	Expected, Current int64
}

func (e *GenerationError) Error() string {
	return fmt.Sprintf("%s changed since this request was made (generation %d, now %d); read it again and resubmit", e.Resource, e.Expected, e.Current)
}

// ErrStaleSequence is an Update whose record is no longer at the sequence it was read at: another
// writer (a node that took the operation over from an owner it saw lost) has written it since.
var ErrStaleSequence = errors.New("operation record changed since it was read")

// ErrUnknownOperation is an Update of a record that does not exist.
var ErrUnknownOperation = errors.New("no such operation record")

// Terminal reports whether an operation has ended.
func (op *Operation) Terminal() bool {
	switch op.Status {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

// prunable reports whether a record may be dropped by the history limit: an ended operation whose
// effect on the directory is known. An unfinished one, or one whose effect is uncertain, is
// evidence and stays however many records there are.
func (op *Operation) prunable() bool {
	return op.Terminal() && op.EffectState != EffectUncertain
}

// conflict returns the unfinished operation among live whose scope sc conflicts with, if any.
func conflict(sc *Scope, live []*Operation) *ScopeBusyError {
	cl, isCluster := sc.cluster()
	for _, o := range live {
		if o.Scope == nil || o.Terminal() {
			continue
		}
		switch {
		case o.Scope.Resource == sc.Resource:
			return &ScopeBusyError{Resource: sc.Resource, Owner: o.ID}
		case isCluster && o.Scope.touches(cl):
			return &ScopeBusyError{Resource: sc.Resource, Owner: o.ID, Held: o.Scope.Resource}
		case !isCluster:
			if ocl, ok := o.Scope.cluster(); ok && slices.Contains(sc.Clusters, ocl) {
				return &ScopeBusyError{Resource: sc.Resource, Owner: o.ID, Held: o.Scope.Resource}
			}
		}
	}
	return nil
}

// checkScope compares an operation's identity and scope generation with a directory snapshot.
func checkScope(snap *directory.Snapshot, op *Operation) error {
	f := snap.File()
	if !op.Identity.IsZero() && op.Identity != f.Identity {
		return checkLineage(snap, op.Identity, 0)
	}
	if op.Scope != nil {
		if cur := f.Generation(op.Scope.Resource); cur != op.Scope.Generation {
			return &GenerationError{Resource: op.Scope.Resource, Expected: op.Scope.Generation, Current: cur}
		}
	}
	return nil
}

// Create implements Operations.
func (m *MemOperations) Create(_ context.Context, op *Operation) error {
	m.mu.Lock()
	if m.byID == nil {
		m.byID = map[string]*Operation{}
	}
	if _, dup := m.byID[op.ID]; dup {
		m.mu.Unlock()
		return fmt.Errorf("operation %s already exists", op.ID)
	}
	if op.Scope != nil {
		if m.Dir != nil {
			if err := checkScope(m.Dir.Snapshot(), op); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		live := make([]*Operation, 0, len(m.byID))
		for _, o := range m.byID {
			live = append(live, o)
		}
		if busy := conflict(op.Scope, live); busy != nil {
			m.mu.Unlock()
			return busy
		}
	}
	m.order = append(m.order, op.ID)
	c := op.clone()
	m.byID[op.ID] = c
	m.trim()
	fn := m.OnChange
	m.mu.Unlock()
	if fn != nil {
		fn(*c.clone())
	}
	return nil
}

// Update implements Operations.
func (m *MemOperations) Update(_ context.Context, op *Operation) error {
	m.mu.Lock()
	cur, ok := m.byID[op.ID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrUnknownOperation, op.ID)
	}
	if cur.Sequence != op.Sequence-1 {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s is at sequence %d, not %d", ErrStaleSequence, op.ID, cur.Sequence, op.Sequence-1)
	}
	c := op.clone()
	m.byID[op.ID] = c // a terminal status releases the scope: conflict skips ended records
	m.trim()
	fn := m.OnChange
	m.mu.Unlock()
	if fn != nil {
		fn(*c.clone())
	}
	return nil
}

// trim drops the oldest prunable records past the limit. m.mu is held.
func (m *MemOperations) trim() {
	limit := m.Limit
	if limit <= 0 {
		limit = defaultOperationLimit
	}
	for i := 0; len(m.order) > limit && i < len(m.order); {
		if op := m.byID[m.order[i]]; op.prunable() {
			delete(m.byID, m.order[i])
			m.order = slices.Delete(m.order, i, i+1)
			continue
		}
		i++
	}
}

// clustersOf lists the clusters placement p involves, and extra, sorted: every role and leg that
// names a cluster in f.
func clustersOf(f *directory.File, p *directory.Placement, extra ...string) []string {
	set := map[string]bool{}
	add := func(name string) {
		if _, ok := f.Clusters[name]; ok {
			set[name] = true
		}
	}
	if p != nil {
		for _, role := range []string{p.Primary, p.Source, p.Target, p.Cold} {
			add(p.ClusterOf(role))
		}
		for role := range p.Names {
			add(p.ClusterOf(role))
		}
		for _, leg := range p.Legs {
			add(leg.Cluster)
		}
	}
	for _, name := range extra {
		add(name)
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

// placementOp is the record a placement operation starts from: its key and its scope at the
// current generation, touching the placement's clusters and extra (a target the request names).
func (s *Server) placementOp(key string, extra ...string) Operation {
	f := s.Dir.Snapshot().File()
	res := directory.PlacementResource(key)
	var p *directory.Placement
	if cur, ok := f.Placements[key]; ok {
		p = &cur
	} else if tenant, _, ok := directory.SplitKey(key); ok {
		extra = append(extra, f.Tenants[tenant].DefaultCluster) // a create lands there unless told otherwise
	}
	return Operation{Placement: key, Scope: &Scope{Resource: res, Generation: f.Generation(res), Clusters: clustersOf(f, p, extra...)}}
}

// clusterOp is the record a cluster operation starts from.
func (s *Server) clusterOp(name string) Operation {
	res := directory.ClusterResource(name)
	return Operation{Cluster: name, Scope: &Scope{Resource: res, Generation: s.Dir.Snapshot().File().Generation(res)}}
}

// scopeFields are the request body fields that name clusters, read by peekScope.
type scopeFields struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
	To      string `json:"to"`
	Legs    []struct {
		Cluster string `json:"cluster"`
	} `json:"legs"`
}

// peekScope reads a control request's JSON body for the clusters it names and puts the body back
// for the handler, which validates it. A body that does not decode names none.
func peekScope(r *http.Request) scopeFields {
	var f scopeFields
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	r.Body = io.NopCloser(bytes.NewReader(data))
	if err == nil {
		_ = json.Unmarshal(data, &f) //nolint:errcheck // the handler reports a bad body
	}
	return f
}

// placementScope is a route's scope: the placement in its path, and the clusters its body names.
func (s *Server) placementScope(r *http.Request) (Operation, error) {
	f := peekScope(r)
	extra := make([]string, 0, 2+len(f.Legs))
	extra = append(extra, f.Cluster, f.To)
	for _, l := range f.Legs {
		extra = append(extra, l.Cluster)
	}
	return s.placementOp(pathKey(r), extra...), nil
}

// clusterAddScope is cluster add's scope: the cluster its body names.
func (s *Server) clusterAddScope(r *http.Request) (Operation, error) {
	name := peekScope(r).Name
	if name == "" {
		return Operation{}, bad("cluster add: name is required")
	}
	return s.clusterOp(name), nil
}
