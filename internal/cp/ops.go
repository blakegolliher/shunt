package cp

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// Operation records in etcd (ADR-0017):
//
//	/shunt/ops/<id>    one control.Operation, JSON; ids are time-sortable (<unix-ms>-<hex>)
//
// Outside /shunt/v1/, so a record never bumps the directory version the proxies install. Every
// control node keeps an index of the records, fed by one watch on the prefix; the watch is where
// OnChange fires, so every node, the writer included, sees each change exactly once. The write
// that creates a record trims the oldest past the retention.
const (
	opsPrefix   = "/shunt/ops/"
	scopePrefix = "/shunt/ops-scope/" // <resource> → the id of the unfinished operation that owns it
	scopeRev    = "/shunt/ops-scope-rev"
	ownerPrefix = "/shunt/ops-owner/" // <node> on a lease: the node is live and runs its operations
	opRetention = 1000
	opTimeout   = 10 * time.Second
	ownerTTL    = 10 // seconds a node's liveness key outlives its last keep-alive
)

// Operations implements control.Operations on etcd.
type Operations struct {
	cli *clientv3.Client
	// OnChange, if set, is called from the watch with every record that changes, on every node.
	OnChange func(control.Operation)
	// Retention is how many ended records are kept; default 1000. Tests shorten it.
	Retention int
	// Node, if set, is this control node's name: Start keeps its liveness key, and a create
	// blocked by an operation whose node's key has lapsed ends that operation as orphaned.
	Node string
	// Now stamps orphaned records; default time.Now.
	Now func() time.Time

	mu       sync.Mutex
	index    map[string][]byte // id → the record as stored
	stop     context.CancelFunc
	stopLive context.CancelFunc
	done     chan struct{}
}

var _ control.Operations = (*Operations)(nil)

// NewOperations returns a store that is not loaded yet: set OnChange, then Start it.
func NewOperations(cli *clientv3.Client) *Operations {
	return &Operations{cli: cli, index: map[string][]byte{}}
}

// Start loads the records, keeps this node's liveness key when Node is set, and starts the watch
// that follows every later change.
func (o *Operations) Start(ctx context.Context) error {
	rev, err := o.load(ctx)
	if err != nil {
		return err
	}
	if o.Node != "" {
		if err := o.live(ctx); err != nil {
			return err
		}
	}
	wctx, cancel := context.WithCancel(context.Background())
	o.stop = cancel
	o.done = make(chan struct{})
	go o.watch(wctx, rev)
	return nil
}

// Close stops the watch and the liveness keep-alive.
func (o *Operations) Close() {
	if o.stopLive != nil {
		o.stopLive()
		o.stopLive = nil
	}
	if o.stop != nil {
		o.stop()
		<-o.done
		o.stop = nil
	}
}

// live writes this node's liveness key on a lease and keeps the lease alive until Close. A node
// that loses the control plane for ownerTTL loses the key, and another node may end its running
// operations as orphaned; its own later writes to them then fail as stale.
func (o *Operations) live(ctx context.Context) error {
	g, err := o.cli.Grant(ctx, ownerTTL)
	if err != nil {
		return fmt.Errorf("%w: liveness lease: %w", control.ErrUnavailable, err)
	}
	if _, err = o.cli.Put(ctx, ownerPrefix+o.Node, "", clientv3.WithLease(g.ID)); err != nil {
		return fmt.Errorf("%w: liveness key: %w", control.ErrUnavailable, err)
	}
	kctx, cancel := context.WithCancel(context.Background())
	ch, err := o.cli.KeepAlive(kctx, g.ID)
	if err != nil {
		cancel()
		return fmt.Errorf("%w: liveness keep-alive: %w", control.ErrUnavailable, err)
	}
	o.stopLive = cancel
	go func() {
		for range ch { //nolint:revive // drained so the client keeps renewing
		}
	}()
	return nil
}

func (o *Operations) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// load reads every record at one revision and replaces the index.
func (o *Operations) load(ctx context.Context) (int64, error) {
	resp, err := o.cli.Get(ctx, opsPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("%w: reading the operation records: %w", control.ErrUnavailable, err)
	}
	index := make(map[string][]byte, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		index[strings.TrimPrefix(string(kv.Key), opsPrefix)] = slices.Clone(kv.Value)
	}
	o.mu.Lock()
	o.index = index
	o.mu.Unlock()
	return resp.Header.Revision, nil
}

// watch follows every change after rev. A watch cut by a lost connection is resumed from the
// last revision; one cut by a compaction past it reloads the index and watches from there.
func (o *Operations) watch(ctx context.Context, rev int64) {
	defer close(o.done)
	for {
		compacted := false
		ch := o.cli.Watch(ctx, opsPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))
		for wr := range ch {
			if wr.CompactRevision != 0 {
				compacted = true
			}
			if wr.Err() != nil || wr.Canceled {
				break
			}
			for _, ev := range wr.Events {
				id := strings.TrimPrefix(string(ev.Kv.Key), opsPrefix)
				if ev.Type == clientv3.EventTypeDelete {
					o.mu.Lock()
					delete(o.index, id)
					o.mu.Unlock()
					continue
				}
				var op control.Operation
				if json.Unmarshal(ev.Kv.Value, &op) != nil {
					continue
				}
				o.mu.Lock()
				o.index[id] = slices.Clone(ev.Kv.Value)
				fn := o.OnChange
				o.mu.Unlock()
				if fn != nil {
					fn(op)
				}
			}
			rev = wr.Header.Revision
		}
		if ctx.Err() != nil {
			return
		}
		if compacted {
			for ctx.Err() == nil {
				r, err := o.load(ctx)
				if err == nil {
					rev = r
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// Create implements control.Operations: one transaction writes the record and, for a scoped
// operation, its reservation, comparing the directory identity and the scope's generation in the
// same step. A conflicting operation whose owner node is no longer live is ended as orphaned
// (effect uncertain) and the create retried.
func (o *Operations) Create(ctx context.Context, op *control.Operation) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	key := opsPrefix + op.ID
	for attempt := 0; ; attempt++ {
		cmps := []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)}
		then := []clientv3.Op{clientv3.OpPut(key, string(raw))}
		var check []clientv3.Op // read back when the transaction fails, to say why
		if sc := op.Scope; sc != nil {
			more, err := o.reserve(ctx, op, sc)
			if err != nil {
				return err
			}
			cmps, then, check = append(cmps, more.cmps...), append(then, more.then...), more.check
		}
		resp, err := o.cli.Txn(ctx).If(cmps...).Then(then...).Else(check...).Commit()
		if err != nil {
			return fmt.Errorf("%w: writing an operation record: %w", control.ErrUnavailable, err)
		}
		if resp.Succeeded {
			o.mu.Lock()
			o.index[op.ID] = raw
			o.mu.Unlock()
			o.trim(ctx)
			return nil
		}
		retry, refusal := o.refusal(ctx, op, resp)
		if !retry || attempt >= 8 {
			if refusal == nil {
				refusal = fmt.Errorf("%w: operation reservations kept changing; retry", control.ErrUnavailable)
			}
			return refusal
		}
	}
}

// reservation is what reserving a scope adds to a create's transaction.
type reservation struct {
	cmps  []clientv3.Cmp
	then  []clientv3.Op
	check []clientv3.Op
}

// reserve builds a scoped create's compares. A cluster operation reads the unfinished placement
// reservations first, refuses one touching its cluster, and compares scopeRev so that a placement
// reservation made after that read fails the transaction.
func (o *Operations) reserve(ctx context.Context, op *control.Operation, sc *control.Scope) (reservation, error) {
	var r reservation
	sk := scopePrefix + sc.Resource
	r.cmps = append(r.cmps, clientv3.Compare(clientv3.CreateRevision(sk), "=", 0))
	r.then = append(r.then, clientv3.OpPut(sk, op.ID))
	r.check = append(r.check, clientv3.OpGet(sk), clientv3.OpGet(kIdentity))
	if !op.Identity.IsZero() {
		id, _ := json.Marshal(op.Identity)
		r.cmps = append(r.cmps, clientv3.Compare(clientv3.Value(kIdentity), "=", string(id)))
	}
	gk := kGenerations + sc.Resource
	if sc.Generation == 0 {
		r.cmps = append(r.cmps, clientv3.Compare(clientv3.CreateRevision(gk), "=", 0))
	} else {
		r.cmps = append(r.cmps, clientv3.Compare(clientv3.Value(gk), "=", strconv.FormatInt(sc.Generation, 10)))
	}
	r.check = append(r.check, clientv3.OpGet(gk))
	if cluster, isCluster := strings.CutPrefix(sc.Resource, "cluster:"); isCluster {
		resp, err := o.cli.Get(ctx, scopePrefix+"placement:", clientv3.WithPrefix())
		if err != nil {
			return r, fmt.Errorf("%w: reading operation reservations: %w", control.ErrUnavailable, err)
		}
		for _, kv := range resp.Kvs {
			owner, err := o.Get(ctx, string(kv.Value))
			if err != nil {
				return r, err
			}
			if owner != nil && !owner.Terminal() && owner.Scope != nil && slices.Contains(owner.Scope.Clusters, cluster) {
				if o.reap(ctx, owner) {
					continue
				}
				return r, &control.ScopeBusyError{Resource: sc.Resource, Owner: owner.ID, Held: owner.Scope.Resource}
			}
		}
		r.cmps = append(r.cmps, clientv3.Compare(clientv3.ModRevision(scopeRev), "<", resp.Header.Revision+1))
		return r, nil
	}
	for _, c := range sc.Clusters {
		ck := scopePrefix + "cluster:" + c
		r.cmps = append(r.cmps, clientv3.Compare(clientv3.CreateRevision(ck), "=", 0))
		r.check = append(r.check, clientv3.OpGet(ck))
	}
	r.then = append(r.then, clientv3.OpPut(scopeRev, op.ID))
	return r, nil
}

// refusal says why a create's transaction failed, from what its Else branch read, and whether to
// retry: after a reaped owner, or a placement reservation made during a cluster operation's read.
func (o *Operations) refusal(ctx context.Context, op *control.Operation, resp *clientv3.TxnResponse) (retry bool, err error) {
	if len(resp.Responses) == 0 {
		return false, fmt.Errorf("operation %s already exists", op.ID)
	}
	get := func(i int) *mvccpb.KeyValue {
		if kvs := resp.Responses[i].GetResponseRange().Kvs; len(kvs) > 0 {
			return kvs[0]
		}
		return nil
	}
	sc := op.Scope
	if kv := get(0); kv != nil {
		return o.busy(ctx, sc.Resource, string(kv.Value), "")
	}
	if kv := get(1); kv != nil && !op.Identity.IsZero() {
		var cur directory.Identity
		if json.Unmarshal(kv.Value, &cur) == nil && cur != op.Identity {
			return false, control.LineageMismatch(cur, op.Identity)
		}
	}
	var cur int64
	if kv := get(2); kv != nil {
		cur, _ = strconv.ParseInt(string(kv.Value), 10, 64)
	}
	if cur != sc.Generation {
		return false, &control.GenerationError{Resource: sc.Resource, Expected: sc.Generation, Current: cur}
	}
	for i := 3; i < len(resp.Responses); i++ {
		if kv := get(i); kv != nil {
			return o.busy(ctx, sc.Resource, string(kv.Value), strings.TrimPrefix(string(kv.Key), scopePrefix))
		}
	}
	return true, nil // scopeRev moved: a placement reservation raced the cluster operation's read
}

// busy is the refusal for a reservation owned by ownerID; a lost owner is reaped and the create
// retried instead.
func (o *Operations) busy(ctx context.Context, resource, ownerID, held string) (retry bool, err error) {
	owner, err := o.Get(ctx, ownerID)
	if err != nil {
		return false, err
	}
	if owner == nil || owner.Terminal() {
		o.release(ctx, held, resource, ownerID)
		return true, nil
	}
	if o.reap(ctx, owner) {
		return true, nil
	}
	return false, &control.ScopeBusyError{Resource: resource, Owner: ownerID, Held: held}
}

// release deletes a reservation left by a record that has ended or gone: Update releases in the
// same transaction as the terminal write, so this only follows an interrupted history trim.
func (o *Operations) release(ctx context.Context, held, resource, ownerID string) {
	res := resource
	if held != "" {
		res = held
	}
	sk := scopePrefix + res
	_, _ = o.cli.Txn(ctx).If(clientv3.Compare(clientv3.Value(sk), "=", ownerID)).Then(clientv3.OpDelete(sk)).Commit() //nolint:errcheck // the retried create says what is left
}

// reap ends owner as orphaned when its node is no longer live, and reports whether it did. Only a
// store that keeps its own node's liveness key judges others by theirs.
func (o *Operations) reap(ctx context.Context, owner *control.Operation) bool {
	if o.Node == "" || owner.Terminal() {
		return false
	}
	resp, err := o.cli.Get(ctx, ownerPrefix+owner.Node, clientv3.WithCountOnly())
	if err != nil || resp.Count > 0 {
		return false
	}
	control.Orphan(owner, o.now().UTC(), fmt.Sprintf("control node %s stopped running this operation (its liveness in the control plane lapsed); repeat the step to complete it", owner.Node))
	if err := o.Update(ctx, owner); err != nil {
		return false
	}
	return true
}

// Update implements control.Operations: a compare-and-swap on the record as read at the sequence
// before op's. A terminal status releases the scope in the same transaction.
func (o *Operations) Update(ctx context.Context, op *control.Operation) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	key := opsPrefix + op.ID
	resp, err := o.cli.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("%w: reading an operation record: %w", control.ErrUnavailable, err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("%w: %s", control.ErrUnknownOperation, op.ID)
	}
	var cur control.Operation
	if err = json.Unmarshal(resp.Kvs[0].Value, &cur); err != nil {
		return fmt.Errorf("operation record %s: %w", op.ID, err)
	}
	if cur.Sequence != op.Sequence-1 {
		return fmt.Errorf("%w: %s is at sequence %d, not %d", control.ErrStaleSequence, op.ID, cur.Sequence, op.Sequence-1)
	}
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	then := []clientv3.Op{clientv3.OpPut(key, string(raw))}
	if op.Terminal() && op.Scope != nil {
		sk := scopePrefix + op.Scope.Resource
		then = append(then, clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.Value(sk), "=", op.ID)}, []clientv3.Op{clientv3.OpDelete(sk)}, nil))
	}
	tresp, err := o.cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)).Then(then...).Commit()
	if err != nil {
		return fmt.Errorf("%w: writing an operation record: %w", control.ErrUnavailable, err)
	}
	if !tresp.Succeeded {
		return fmt.Errorf("%w: %s was written by another node since it was read", control.ErrStaleSequence, op.ID)
	}
	o.mu.Lock()
	o.index[op.ID] = raw
	o.mu.Unlock()
	return nil
}

// trim deletes the oldest ended records past the retention. An unfinished record, or one whose
// effect is uncertain, stays however many there are: it is evidence (ADR-0021).
func (o *Operations) trim(ctx context.Context) {
	limit := o.Retention
	if limit <= 0 {
		limit = opRetention
	}
	o.mu.Lock()
	ids := make([]string, 0, len(o.index))
	for id := range o.index {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var drop []clientv3.Op
	for _, id := range ids {
		if len(ids)-len(drop) <= limit {
			break
		}
		var op control.Operation
		if json.Unmarshal(o.index[id], &op) == nil && op.Terminal() && op.EffectState != control.EffectUncertain {
			drop = append(drop, clientv3.OpDelete(opsPrefix+id))
		}
	}
	o.mu.Unlock()
	for i := 0; i < len(drop); i += maxTxnOps {
		_, _ = o.cli.Txn(ctx).Then(drop[i:min(i+maxTxnOps, len(drop))]...).Commit() //nolint:errcheck // best effort: the next create trims again
	}
}

// Get implements control.Operations: a linearizable read from etcd, so a record another node
// wrote a moment ago is current here before this node's watch has delivered it; the index
// answers when etcd cannot be reached.
func (o *Operations) Get(ctx context.Context, id string) (*control.Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	var raw []byte
	resp, err := o.cli.Get(ctx, opsPrefix+id)
	switch {
	case err == nil && len(resp.Kvs) == 0:
		return nil, nil
	case err == nil:
		raw = resp.Kvs[0].Value
	default:
		o.mu.Lock()
		cached, ok := o.index[id]
		o.mu.Unlock()
		if !ok {
			return nil, fmt.Errorf("%w: reading an operation record: %w", control.ErrUnavailable, err)
		}
		raw = cached
	}
	var op control.Operation
	if err := json.Unmarshal(raw, &op); err != nil {
		return nil, fmt.Errorf("operation record %s: %w", id, err)
	}
	return &op, nil
}

// List implements control.Operations: newest first, from the index.
func (o *Operations) List(_ context.Context, placement, cluster string, limit int) ([]*control.Operation, error) {
	o.mu.Lock()
	ids := make([]string, 0, len(o.index))
	for id := range o.index {
		ids = append(ids, id)
	}
	raws := make(map[string][]byte, len(ids))
	for _, id := range ids {
		raws[id] = o.index[id]
	}
	o.mu.Unlock()
	slices.Sort(ids)
	slices.Reverse(ids)
	var out []*control.Operation
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		var op control.Operation
		if json.Unmarshal(raws[id], &op) != nil {
			continue
		}
		if (placement == "" || op.Placement == placement) && (cluster == "" || op.Cluster == cluster) {
			out = append(out, &op)
		}
	}
	return out, nil
}
