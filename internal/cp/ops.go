package cp

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/control"
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
	opRetention = 1000
	opTimeout   = 10 * time.Second
)

// Operations implements control.Operations on etcd.
type Operations struct {
	cli *clientv3.Client
	// OnChange, if set, is called from the watch with every record that changes, on every node.
	OnChange func(control.Operation)
	// Retention is how many records are kept; default 1000. Tests shorten it.
	Retention int

	mu    sync.Mutex
	index map[string][]byte // id → the record as stored
	stop  context.CancelFunc
	done  chan struct{}
}

var _ control.Operations = (*Operations)(nil)

// NewOperations returns a store that is not loaded yet: set OnChange, then Start it.
func NewOperations(cli *clientv3.Client) *Operations {
	return &Operations{cli: cli, index: map[string][]byte{}}
}

// Start loads the records and starts the watch that follows every later change.
func (o *Operations) Start(ctx context.Context) error {
	rev, err := o.load(ctx)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithCancel(context.Background())
	o.stop = cancel
	o.done = make(chan struct{})
	go o.watch(wctx, rev)
	return nil
}

// Close stops the watch.
func (o *Operations) Close() {
	if o.stop != nil {
		o.stop()
		<-o.done
		o.stop = nil
	}
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

// Put implements control.Operations. The index is updated at once, so a Get on this node right
// after sees the record; other nodes see it through their watch.
func (o *Operations) Put(ctx context.Context, op *control.Operation) error {
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	key := opsPrefix + op.ID
	put := clientv3.OpPut(key, string(raw))
	resp, err := o.cli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).Then(put).Else(put).Commit()
	if err != nil {
		return fmt.Errorf("%w: writing an operation record: %w", control.ErrUnavailable, err)
	}
	o.mu.Lock()
	o.index[op.ID] = raw
	o.mu.Unlock()
	if resp.Succeeded {
		o.trim(ctx)
	}
	return nil
}

// trim deletes the oldest records past the retention; the watch drops them from every index.
func (o *Operations) trim(ctx context.Context) {
	limit := o.Retention
	if limit <= 0 {
		limit = opRetention
	}
	resp, err := o.cli.Get(ctx, opsPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil || len(resp.Kvs) <= limit {
		return
	}
	var ops []clientv3.Op
	for _, kv := range resp.Kvs[:len(resp.Kvs)-limit] {
		ops = append(ops, clientv3.OpDelete(string(kv.Key)))
	}
	for i := 0; i < len(ops); i += maxTxnOps {
		_, _ = o.cli.Txn(ctx).Then(ops[i:min(i+maxTxnOps, len(ops))]...).Commit() //nolint:errcheck // best effort: the next create trims again
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
