package cp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
)

// The directory in etcd (ADR-0015, §12.5): one key per record, every change one transaction
// with compare-and-swap on the directory version, and a watch that keeps this node's in-memory
// copy current. Secrets are sealed with the node's data-encryption key before they are written.
//
//	/shunt/v1/version                      the directory version: bumped by every write, CAS target
//	/shunt/v1/clusters/<name>              {cluster, secret}; the cluster's secret_ref is control:<name>
//	/shunt/v1/tenants/<tenant>
//	/shunt/v1/placements/<tenant>/<bucket>
//	/shunt/v1/credentials/<access-key>     {tenant, buckets, secret}: a client key
//	/shunt/v1/changes/<version>            the audit record of that version (directory.Change)
//
// The fleet's keys are under /shunt/fleet/ (fleet.go), outside this prefix, so heartbeats do not
// bump the directory version.
const (
	prefix       = "/shunt/v1/"
	kVersion     = prefix + "version"
	kClusters    = prefix + "clusters/"
	kTenants     = prefix + "tenants/"
	kPlacements  = prefix + "placements/"
	kCredentials = prefix + "credentials/"
	kChanges     = prefix + "changes/"

	// changeRetention is how many audit records stay in etcd; older ones are deleted by the write
	// that makes them older. Export to object storage is deferred (docs/POC-6.md, P3c-2).
	changeRetention = 10000

	// SecretRefPrefix names a secret the control plane keeps: control:<cluster>.
	SecretRefPrefix = "control:"
)

type clusterRecord struct {
	Cluster config.Cluster `json:"cluster"`
	Secret  string         `json:"secret,omitempty"` // sealed
}

type credentialRecord struct {
	Tenant  string   `json:"tenant"`
	Buckets []string `json:"buckets,omitempty"`
	Secret  string   `json:"secret"` // sealed
}

// state is one version of everything the store holds, decrypted, behind an atomic pointer.
type state struct {
	version int64
	file    *directory.File
	creds   map[string]sigv4.Credential // access key → credential, secret in the clear
	secrets map[string]string           // control:<cluster> → secret in the clear
}

func (st *state) clone() *state {
	return &state{version: st.version, file: st.file.Clone(), creds: maps.Clone(st.creds), secrets: maps.Clone(st.secrets)}
}

// Store is the control plane's directory: a directory.Store and a control.Keys over etcd.
type Store struct {
	cli    *clientv3.Client
	cipher *Cipher
	log    *slog.Logger
	// Prepare, if set, is called with a candidate directory before a write is committed and with
	// each version the watch installs: shunt-control builds its clusters with it. resolve resolves
	// the candidate's own secrets. On a write, an error refuses the write, and the candidate is
	// never committed: the version goes live through the watch, like another node's write, so a
	// failed compare-and-swap leaves the live clusters as they were. On the watch, commit runs just
	// before the version is installed; an error is logged and the version is installed anyway.
	Prepare func(f *directory.File, resolve func(ref string) (string, error)) (commit func(), err error)
	// OnInstall, if set, is called after every installed version.
	OnInstall func(*directory.Snapshot)
	// Now stamps placements and change records; defaults to time.Now.
	Now func() time.Time

	cur  atomic.Pointer[state]
	snap atomic.Pointer[directory.Snapshot]
	mu   sync.Mutex // guards cond
	cond *sync.Cond

	ready  atomic.Bool
	stop   context.CancelFunc
	done   chan struct{}
	writes sync.Mutex // serializes this node's own writes; other nodes' writes are CAS conflicts
}

var (
	_ directory.Store = (*Store)(nil)
	_ control.Keys    = (*Store)(nil)
)

// New returns a store that is not loaded yet: set Prepare and OnInstall, then Start it.
func New(cli *clientv3.Client, cipher *Cipher, log *slog.Logger) *Store {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	s := &Store{cli: cli, cipher: cipher, log: log, done: make(chan struct{})}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Start loads the directory from etcd, installs it, and starts the watch that follows every
// later version. cipher unseals the stored secrets; a wrong key fails here, not later. Without
// quorum the load cannot complete: the caller retries, and Ready stays false meanwhile.
func (s *Store) Start(ctx context.Context) error {
	rev, err := s.load(ctx)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithCancel(context.Background())
	s.stop = cancel
	s.ready.Store(true)
	go s.watch(wctx, rev)
	return nil
}

// Ready reports whether the store has loaded the directory. Before that, nothing may be served
// from it: an empty directory answered to a member would be installed as one.
func (s *Store) Ready() bool { return s.ready.Load() }

// Open is New followed by Start, for a store with no hooks.
func Open(ctx context.Context, cli *clientv3.Client, cipher *Cipher, log *slog.Logger) (*Store, error) {
	s := New(cli, cipher, log)
	if err := s.Start(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Close stops the watch.
func (s *Store) Close() {
	if s.stop != nil {
		s.stop()
		<-s.done
		s.stop = nil
	}
}

// load reads everything under the prefix at one revision and installs it.
func (s *Store) load(ctx context.Context) (int64, error) {
	resp, err := s.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("%w: reading the directory: %w", control.ErrUnavailable, err)
	}
	st := &state{file: &directory.File{}, creds: map[string]sigv4.Credential{}, secrets: map[string]string{}}
	st.file.Clusters, st.file.Tenants, st.file.Placements = map[string]config.Cluster{}, map[string]directory.Tenant{}, map[string]directory.Placement{}
	for _, kv := range resp.Kvs {
		if err := s.apply(st, string(kv.Key), kv.Value, false); err != nil {
			return 0, err
		}
	}
	st.file.Version = st.version
	if err := directory.Validate(st.file); err != nil {
		return 0, fmt.Errorf("the directory in etcd is invalid: %w", err)
	}
	commit, _ := s.prepare(st, false) //nolint:errcheck // logged inside; the version is installed anyway
	commit()
	s.install(st)
	return resp.Header.Revision, nil
}

// apply puts one record into st; deleted: the key was removed.
func (s *Store) apply(st *state, key string, value []byte, deleted bool) error {
	switch {
	case key == kVersion:
		if deleted {
			return errors.New("the directory version key was deleted")
		}
		v, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return fmt.Errorf("directory version %q: %w", value, err)
		}
		st.version = v
	case strings.HasPrefix(key, kClusters):
		name := strings.TrimPrefix(key, kClusters)
		if deleted {
			delete(st.file.Clusters, name)
			delete(st.secrets, SecretRefPrefix+name)
			return nil
		}
		var rec clusterRecord
		if err := json.Unmarshal(value, &rec); err != nil {
			return fmt.Errorf("cluster %s: %w", name, err)
		}
		st.file.Clusters[name] = rec.Cluster
		if rec.Secret != "" {
			plain, err := s.cipher.Decrypt(rec.Secret)
			if err != nil {
				return fmt.Errorf("cluster %s: %w", name, err)
			}
			st.secrets[SecretRefPrefix+name] = plain
		}
	case strings.HasPrefix(key, kTenants):
		name := strings.TrimPrefix(key, kTenants)
		if deleted {
			delete(st.file.Tenants, name)
			return nil
		}
		var t directory.Tenant
		if err := json.Unmarshal(value, &t); err != nil {
			return fmt.Errorf("tenant %s: %w", name, err)
		}
		st.file.Tenants[name] = t
	case strings.HasPrefix(key, kPlacements):
		k := strings.TrimPrefix(key, kPlacements)
		if deleted {
			delete(st.file.Placements, k)
			return nil
		}
		var p directory.Placement
		if err := json.Unmarshal(value, &p); err != nil {
			return fmt.Errorf("placement %s: %w", k, err)
		}
		st.file.Placements[k] = p
	case strings.HasPrefix(key, kCredentials):
		ak := strings.TrimPrefix(key, kCredentials)
		if deleted {
			delete(st.creds, ak)
			return nil
		}
		var rec credentialRecord
		if err := json.Unmarshal(value, &rec); err != nil {
			return fmt.Errorf("credential %s: %w", ak, err)
		}
		plain, err := s.cipher.Decrypt(rec.Secret)
		if err != nil {
			return fmt.Errorf("credential %s: %w", ak, err)
		}
		st.creds[ak] = sigv4.Credential{AccessKey: ak, Secret: plain, Tenant: rec.Tenant, Buckets: rec.Buckets}
	case strings.HasPrefix(key, kChanges):
		// audit records are written, never read back into the state
	default:
		s.log.Warn("unknown key under the directory prefix", "key", key)
	}
	return nil
}

// prepare runs the Prepare hook on a candidate, its secrets resolved from the candidate alone, and
// returns the candidate's commit. onWrite: an error refuses. Otherwise an error is logged and the
// commit returned keeps the live clusters as they are.
func (s *Store) prepare(st *state, onWrite bool) (commit func(), err error) {
	if s.Prepare == nil {
		return func() {}, nil
	}
	commit, err = s.Prepare(st.file, st.resolve)
	if err != nil {
		if onWrite {
			return nil, err
		}
		s.log.Error("a directory version could not be prepared on this node; installed anyway", "version", st.version, "err", err.Error())
		return func() {}, nil
	}
	return commit, nil
}

func (s *Store) install(st *state) {
	s.cur.Store(st)
	snap := directory.NewSnapshot(st.file)
	s.snap.Store(snap)
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()
	if s.OnInstall != nil {
		s.OnInstall(snap)
	}
}

// watch follows every change after rev, one etcd transaction per installed version. When the
// watch fails (compaction past rev, a leader change), it reloads and watches from there.
func (s *Store) watch(ctx context.Context, rev int64) {
	defer close(s.done)
	for {
		ch := s.cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))
		for wr := range ch {
			if wr.Err() != nil || wr.Canceled {
				break
			}
			if len(wr.Events) == 0 {
				continue
			}
			st := s.cur.Load().clone()
			ok := true
			for _, ev := range wr.Events {
				if err := s.apply(st, string(ev.Kv.Key), ev.Kv.Value, ev.Type == clientv3.EventTypeDelete); err != nil {
					s.log.Error("a directory change could not be applied; this node keeps its last version until the next full read", "err", err.Error())
					ok = false
					break
				}
			}
			rev = wr.Header.Revision
			if !ok {
				continue
			}
			st.file.Version = st.version
			if err := directory.Validate(st.file); err != nil {
				s.log.Error("a directory version in etcd is invalid; this node keeps its last version", "version", st.version, "err", err.Error())
				continue
			}
			commit, _ := s.prepare(st, false) //nolint:errcheck // logged inside; the version is installed anyway
			commit()
			s.install(st)
		}
		if ctx.Err() != nil {
			return
		}
		// Reload: the watch was cut (compacted revision, or a lost connection during a quorum loss).
		for ctx.Err() == nil {
			r, err := s.load(ctx)
			if err == nil {
				rev = r
				break
			}
			s.log.Warn("directory reload failed; retrying", "err", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

// Snapshot implements directory.Directory. Before Start has loaded the directory it is empty at
// version 0; callers gate on Ready.
func (s *Store) Snapshot() *directory.Snapshot {
	if snap := s.snap.Load(); snap != nil {
		return snap
	}
	return directory.NewSnapshot(&directory.File{})
}

// Sync implements directory.Store: a linearizable read of the version, then a wait until the
// watch has installed it, so a decision on this node is taken on the current directory.
func (s *Store) Sync(ctx context.Context) error {
	resp, err := s.cli.Get(ctx, kVersion)
	if err != nil {
		return fmt.Errorf("%w: reading the directory version: %w", control.ErrUnavailable, err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}
	v, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return fmt.Errorf("directory version %q: %w", resp.Kvs[0].Value, err)
	}
	return s.WaitVersion(ctx, v)
}

// Version is the installed directory version.
func (s *Store) Version() int64 {
	if st := s.cur.Load(); st != nil {
		return st.version
	}
	return 0
}

// WaitVersion blocks until the installed version is at least v, or ctx ends.
func (s *Store) WaitVersion(ctx context.Context, v int64) error {
	if s.cur.Load().version >= v {
		return nil
	}
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.cur.Load().version < v {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: waiting for directory version %d (at %d)", control.ErrUnavailable, v, s.cur.Load().version)
		}
		s.cond.Wait()
	}
	return nil
}

// Resolve resolves a cluster's secret_ref: a control: ref from the installed version, anything else
// from the environment or a file. It is the resolver shunt-control's cluster registry is built with.
func (s *Store) Resolve(ref string) (string, error) { return s.cur.Load().resolve(ref) }

// resolve resolves a secret_ref against this state's secrets.
func (st *state) resolve(ref string) (string, error) {
	if !strings.HasPrefix(ref, SecretRefPrefix) {
		return config.ResolveSecret(ref)
	}
	if v, ok := st.secrets[ref]; ok {
		return v, nil
	}
	return "", fmt.Errorf("no secret is stored for %s (shunt cluster add stores one)", ref)
}

// ClusterSecrets returns every stored cluster secret by ref, for GET /v1/directory and the mover.
func (s *Store) ClusterSecrets() map[string]string { return maps.Clone(s.cur.Load().secrets) }

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// maxTxnOps is etcd's default --max-txn-ops.
const maxTxnOps = 128

// Changes implements directory.Store: the audit records at or before version before, newest
// first, read as point gets in batches (the keys are decimal, not zero-padded, so no range walks
// them in order). Records past changeRetention are absent.
func (s *Store) Changes(ctx context.Context, before int64, limit int) ([]directory.Change, error) {
	if before == 0 {
		before = s.Version()
	}
	var out []directory.Change
	for v := before; v > 0 && len(out) < limit; {
		var ops []clientv3.Op
		for ; v > 0 && len(ops) < maxTxnOps && len(out)+len(ops) < limit; v-- {
			ops = append(ops, clientv3.OpGet(kChanges+strconv.FormatInt(v, 10)))
		}
		resp, err := s.cli.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return nil, fmt.Errorf("%w: reading the change log: %w", control.ErrUnavailable, err)
		}
		for _, r := range resp.Responses {
			for _, kv := range r.GetResponseRange().Kvs {
				var c directory.Change
				if json.Unmarshal(kv.Value, &c) == nil {
					out = append(out, c)
				}
			}
		}
	}
	return out, nil
}

// mutate applies fn to a copy of the current state and commits the difference as one transaction,
// compare-and-swapped on the directory version. Any control node may write: a conflict means
// another node wrote first, and the change is re-applied to the newer version. On success it
// returns once this node has installed the new version, so the caller's next Snapshot shows it.
// errUnchanged, returned by a mutate function, ends the mutation with nothing written and no error.
var errUnchanged = errors.New("cp: no change")

func (s *Store) mutate(ctx context.Context, actor, op, key string, fn func(st *state) error) error {
	s.writes.Lock()
	defer s.writes.Unlock()
	for attempt := range 8 {
		cur := s.cur.Load()
		next := cur.clone()
		if err := fn(next); errors.Is(err, errUnchanged) {
			return nil
		} else if err != nil {
			return err
		}
		next.version = cur.version + 1
		next.file.Version = next.version
		if err := directory.Validate(next.file); err != nil {
			return err
		}
		// The candidate is checked, not committed: the watch installs the version once it is in
		// etcd, and prepares it again then.
		if _, err := s.prepare(next, true); err != nil {
			return err
		}
		ops, err := s.diff(cur, next)
		if err != nil {
			return err
		}
		change := directory.Change{Time: s.now().UTC(), Actor: actor, Op: op, Key: key, Version: next.version}
		clusterName, isCluster := strings.CutPrefix(key, "clusters/")
		switch {
		case isCluster:
			if c, ok := cur.file.Clusters[clusterName]; ok {
				change.ClusterBefore = &c
			}
			if c, ok := next.file.Clusters[clusterName]; ok {
				change.ClusterAfter = &c
			}
		default:
			if p, ok := cur.file.Placements[key]; ok {
				change.Before = &p
			}
			if p, ok := next.file.Placements[key]; ok {
				change.After = &p
			}
		}
		rec, err := json.Marshal(change)
		if err != nil {
			return err
		}
		v := strconv.FormatInt(next.version, 10)
		ops = append(ops, clientv3.OpPut(kVersion, v), clientv3.OpPut(kChanges+v, string(rec)))
		if old := next.version - changeRetention; old > 0 {
			ops = append(ops, clientv3.OpDelete(kChanges+strconv.FormatInt(old, 10)))
		}
		cmp := clientv3.Compare(clientv3.Value(kVersion), "=", strconv.FormatInt(cur.version, 10))
		if cur.version == 0 {
			cmp = clientv3.Compare(clientv3.Version(kVersion), "=", 0) // the key does not exist yet
		}
		resp, err := s.cli.Txn(ctx).If(cmp).Then(ops...).Commit()
		if err != nil {
			return fmt.Errorf("%w: writing the directory: %w", control.ErrUnavailable, err)
		}
		if resp.Succeeded {
			return s.WaitVersion(ctx, next.version)
		}
		// Another node wrote version cur.version+1 first: wait until this node has it, then retry.
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		werr := s.WaitVersion(wctx, cur.version+1)
		cancel()
		if werr != nil {
			return fmt.Errorf("%w: another control node wrote first and this node has not caught up", control.ErrUnavailable)
		}
		s.log.Info("directory write retried after a concurrent write", "op", op, "key", key, "attempt", attempt+1)
	}
	return fmt.Errorf("%w: eight concurrent writes in a row", directory.ErrConflict)
}

// diff turns the difference between two states into etcd operations, sealing every secret.
func (s *Store) diff(cur, next *state) ([]clientv3.Op, error) {
	var ops []clientv3.Op
	same := func(a, b any) bool {
		x, _ := json.Marshal(a)
		y, _ := json.Marshal(b)
		return bytes.Equal(x, y)
	}
	for name := range next.file.Clusters {
		c := next.file.Clusters[name]
		old, had := cur.file.Clusters[name]
		if had && same(old, c) && cur.secrets[SecretRefPrefix+name] == next.secrets[SecretRefPrefix+name] {
			continue
		}
		rec := clusterRecord{Cluster: c}
		if secret, ok := next.secrets[SecretRefPrefix+name]; ok {
			sealed, err := s.cipher.Encrypt(secret)
			if err != nil {
				return nil, err
			}
			rec.Secret = sealed
		}
		b, err := json.Marshal(rec) //nolint:gosec // G117: the secret field holds AES-GCM ciphertext, sealed above
		if err != nil {
			return nil, err
		}
		ops = append(ops, clientv3.OpPut(kClusters+name, string(b)))
	}
	for name := range cur.file.Clusters {
		if _, ok := next.file.Clusters[name]; !ok {
			ops = append(ops, clientv3.OpDelete(kClusters+name))
		}
	}
	for name, t := range next.file.Tenants {
		if old, had := cur.file.Tenants[name]; had && same(old, t) {
			continue
		}
		b, _ := json.Marshal(t)
		ops = append(ops, clientv3.OpPut(kTenants+name, string(b)))
	}
	for name := range cur.file.Tenants {
		if _, ok := next.file.Tenants[name]; !ok {
			ops = append(ops, clientv3.OpDelete(kTenants+name))
		}
	}
	for k := range next.file.Placements {
		p := next.file.Placements[k]
		if old, had := cur.file.Placements[k]; had && same(old, p) {
			continue
		}
		b, _ := json.Marshal(p)
		ops = append(ops, clientv3.OpPut(kPlacements+k, string(b)))
	}
	for k := range cur.file.Placements {
		if _, ok := next.file.Placements[k]; !ok {
			ops = append(ops, clientv3.OpDelete(kPlacements+k))
		}
	}
	for ak, c := range next.creds {
		if old, had := cur.creds[ak]; had && same(old, c) {
			continue
		}
		sealed, err := s.cipher.Encrypt(c.Secret)
		if err != nil {
			return nil, err
		}
		b, _ := json.Marshal(credentialRecord{Tenant: c.Tenant, Buckets: c.Buckets, Secret: sealed}) //nolint:gosec // G117: sealed above
		ops = append(ops, clientv3.OpPut(kCredentials+ak, string(b)))
	}
	for ak := range cur.creds {
		if _, ok := next.creds[ak]; !ok {
			ops = append(ops, clientv3.OpDelete(kCredentials+ak))
		}
	}
	return ops, nil
}

// Create implements directory.Directory.
func (s *Store) Create(ctx context.Context, tenant, bucket, cluster, backend, actor string) error {
	return s.mutate(ctx, actor, "create", directory.Key(tenant, bucket), func(st *state) error {
		return st.file.Create(tenant, bucket, cluster, backend, s.now())
	})
}

// Delete implements directory.Directory.
func (s *Store) Delete(ctx context.Context, tenant, bucket, actor string) error {
	return s.mutate(ctx, actor, "delete", directory.Key(tenant, bucket), func(st *state) error { return st.file.Delete(tenant, bucket) })
}

// SetState implements directory.Store.
func (s *Store) SetState(ctx context.Context, tenant, bucket, from string, t directory.Transition, actor string) error {
	return s.mutate(ctx, actor, "set-state", directory.Key(tenant, bucket), func(st *state) error { return st.file.SetState(tenant, bucket, from, t) })
}

// SetPlacementReadOnly implements directory.Store.
func (s *Store) SetPlacementReadOnly(ctx context.Context, tenant, bucket string, readOnly, reject bool, actor string) error {
	return s.mutate(ctx, actor, "placement-read-only", directory.Key(tenant, bucket), func(st *state) error {
		return st.file.SetPlacementReadOnly(tenant, bucket, readOnly, reject)
	})
}

// SetClusterReadOnly implements directory.Store.
func (s *Store) SetClusterReadOnly(ctx context.Context, name string, readOnly, reject bool, actor string) error {
	return s.mutate(ctx, actor, "cluster-read-only", "clusters/"+name, func(st *state) error {
		return st.file.SetClusterReadOnly(name, readOnly, reject)
	})
}

// Adopt implements directory.Store.
func (s *Store) Adopt(ctx context.Context, tenant, bucket, cluster, backend, actor string) error {
	return s.mutate(ctx, actor, "adopt", directory.Key(tenant, bucket), func(st *state) error {
		return st.file.Adopt(tenant, bucket, cluster, backend, s.now())
	})
}

// CreateSpread implements directory.Store.
func (s *Store) CreateSpread(ctx context.Context, tenant, bucket string, legs []directory.Leg, actor string) error {
	return s.mutate(ctx, actor, "create-spread", directory.Key(tenant, bucket), func(st *state) error {
		return st.file.CreateSpread(tenant, bucket, legs, s.now())
	})
}

// Carve implements directory.Store.
func (s *Store) Carve(ctx context.Context, tenant, bucket, prefix, actor string) error {
	return s.mutate(ctx, actor, "carve", directory.Key(tenant, bucket), func(st *state) error { return st.file.Carve(tenant, bucket, prefix) })
}

// Merge implements directory.Store.
func (s *Store) Merge(ctx context.Context, tenant, bucket, prefix, actor string) error {
	return s.mutate(ctx, actor, "merge", directory.Key(tenant, bucket), func(st *state) error { return st.file.Merge(tenant, bucket, prefix) })
}

// ClearTarget implements directory.Store.
func (s *Store) ClearTarget(ctx context.Context, tenant, bucket, actor string) error {
	return s.mutate(ctx, actor, "clear-target", directory.Key(tenant, bucket), func(st *state) error { return st.file.ClearTarget(tenant, bucket) })
}

// SetTarget implements directory.Store.
func (s *Store) SetTarget(ctx context.Context, tenant, bucket, cluster, backend, actor string) error {
	return s.mutate(ctx, actor, "set-target", directory.Key(tenant, bucket), func(st *state) error { return st.file.SetTarget(tenant, bucket, cluster, backend) })
}

// SetTenantDefault implements directory.Store.
func (s *Store) SetTenantDefault(ctx context.Context, tenant, cluster, actor string) error {
	return s.mutate(ctx, actor, "set-default", tenant, func(st *state) error { return st.file.SetTenantDefault(tenant, cluster) })
}

// PutCluster implements directory.Store: the secret, when given, is sealed and stored with the
// cluster under the ref control:<name>, which the cluster's credentials must then name.
func (s *Store) PutCluster(ctx context.Context, name string, c config.Cluster, secret, actor string) error {
	ref := SecretRefPrefix + name
	if secret != "" && c.Credentials.SecretRef != ref {
		return fmt.Errorf("a cluster whose secret the control plane stores must name it as %s, not %q", ref, c.Credentials.SecretRef)
	}
	return s.mutate(ctx, actor, "cluster-put", "clusters/"+name, func(st *state) error {
		st.file.PutCluster(name, c)
		switch {
		case secret != "":
			st.secrets[ref] = secret
		case c.Credentials.SecretRef == ref:
			if _, ok := st.secrets[ref]; !ok {
				return fmt.Errorf("cluster %s names %s but no secret is stored for it; give the secret", name, ref)
			}
		default:
			delete(st.secrets, ref) // the cluster now resolves its secret elsewhere (env:, file:)
		}
		return nil
	})
}

// RemoveCluster implements directory.Store.
func (s *Store) RemoveCluster(ctx context.Context, name, actor string) error {
	return s.mutate(ctx, actor, "cluster-remove", "clusters/"+name, func(st *state) error {
		if err := st.file.RemoveCluster(name); err != nil {
			return err
		}
		delete(st.secrets, SecretRefPrefix+name)
		return nil
	})
}

// Tenant implements control.Keys.
func (s *Store) Tenant(tenant string) []sigv4.Credential {
	var out []sigv4.Credential
	for _, c := range s.cur.Load().creds {
		if c.Tenant == tenant {
			out = append(out, c)
		}
	}
	sortCreds(out)
	return out
}

// All implements control.Keys.
func (s *Store) All() []sigv4.Credential {
	m := s.cur.Load().creds
	out := make([]sigv4.Credential, 0, len(m))
	for _, c := range m {
		out = append(out, c)
	}
	sortCreds(out)
	return out
}

func sortCreds(cs []sigv4.Credential) {
	for i := 1; i < len(cs); i++ {
		for j := i; j > 0 && cs[j].AccessKey < cs[j-1].AccessKey; j-- {
			cs[j], cs[j-1] = cs[j-1], cs[j]
		}
	}
}

// keysTimeout bounds a client-key change, which the Keys seam issues without a context.
const keysTimeout = 15 * time.Second

// Add implements control.Keys.
func (s *Store) Add(c sigv4.Credential) error {
	if c.AccessKey == "" || c.Secret == "" {
		return errors.New("a client key needs an access key and a secret")
	}
	if c.Tenant == "" {
		c.Tenant = directory.DefaultTenant
	}
	ctx, cancel := context.WithTimeout(context.Background(), keysTimeout)
	defer cancel()
	return s.mutate(ctx, "api", "key-add", "credentials/"+c.AccessKey, func(st *state) error {
		if held, dup := st.creds[c.AccessKey]; dup {
			merged, err := auth.Merge(held, c)
			if err != nil {
				return err
			}
			if slices.Equal(merged.Buckets, held.Buckets) {
				return errUnchanged // imported again, for a bucket it already reaches
			}
			c = merged
		}
		st.creds[c.AccessKey] = c
		return nil
	})
}

// Remove implements control.Keys.
func (s *Store) Remove(accessKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), keysTimeout)
	defer cancel()
	return s.mutate(ctx, "api", "key-remove", "credentials/"+accessKey, func(st *state) error {
		if _, ok := st.creds[accessKey]; !ok {
			return fmt.Errorf("%w: %s", auth.ErrUnknownKey, accessKey)
		}
		delete(st.creds, accessKey)
		return nil
	})
}
