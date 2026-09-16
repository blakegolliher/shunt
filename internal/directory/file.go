package directory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
)

// defaultLockTimeout bounds how long a write waits for another writer to finish.
const defaultLockTimeout = 5 * time.Second

// FileDir is the file backend (ADR-0005). Reads are a lock-free snapshot pointer. Writes, from
// the proxy and the CLI alike, take an exclusive flock on <file>.lock, re-read the file, apply
// the change against what is on disk, increment version, validate, and replace the file with a
// temp-file rename; each write appends one record to <file>.changes.jsonl. Other writers' changes
// arrive through Reload. The lock needs a local filesystem; NFS flock semantics are not relied on.
type FileDir struct {
	path     string
	clusters map[string]config.Cluster

	// LockTimeout defaults to DefaultLockTimeout.
	LockTimeout time.Duration
	// Now stamps created and change records; defaults to time.Now.
	Now func() time.Time
	// ChangeLogError, if set, is called when a write succeeded but its change record could not be
	// appended. The write is not undone: the placement row is the truth.
	ChangeLogError func(error)

	mu      sync.Mutex // serializes this instance's reloads and writes
	snap    atomic.Pointer[Snapshot]
	stamp   os.FileInfo
	sum     [sha256.Size]byte
	statErr string
}

var _ Directory = (*FileDir)(nil)

// Change is one line of the change log: the file backend's directory_changes (docs/DESIGN.md §2.5).
type Change struct {
	Time    time.Time  `json:"ts"`
	Actor   string     `json:"actor"`
	Op      string     `json:"op"` // create | delete | set-state
	Key     string     `json:"key"`
	Version int64      `json:"version"`
	Before  *Placement `json:"before"`
	After   *Placement `json:"after"`
}

// Open loads and validates the directory file. The file must exist: a mistyped path must not
// silently become an empty directory.
func Open(path string, clusters map[string]config.Cluster) (*FileDir, error) {
	d := &FileDir{path: path, clusters: clusters, LockTimeout: defaultLockTimeout, Now: time.Now}
	data, st, err := readFile(path)
	if err != nil {
		return nil, err
	}
	f, err := parse(data)
	if err != nil {
		return nil, err
	}
	if err := validate(f, clusters); err != nil {
		return nil, fmt.Errorf("directory %s: %w", path, err)
	}
	d.install(f, data, st)
	return d, nil
}

// Load reads, parses, and validates a directory file without opening it for writes.
func Load(path string, clusters map[string]config.Cluster) (*File, error) {
	data, _, err := readFile(path)
	if err != nil {
		return nil, err
	}
	f, err := parse(data)
	if err != nil {
		return nil, err
	}
	if err := validate(f, clusters); err != nil {
		return nil, fmt.Errorf("directory %s: %w", path, err)
	}
	return f, nil
}

// Path is the directory file path.
func (d *FileDir) Path() string { return d.path }

// ChangeLogPath is where change records are appended.
func (d *FileDir) ChangeLogPath() string { return d.path + ".changes.jsonl" }

// Snapshot implements Directory.
func (d *FileDir) Snapshot() *Snapshot { return d.snap.Load() }

func (d *FileDir) install(f *File, data []byte, st os.FileInfo) {
	d.sum = sha256.Sum256(data)
	if st != nil {
		d.stamp = st
	}
	d.snap.Store(newSnapshot(f))
}

func readFile(path string) (data []byte, info os.FileInfo, err error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("directory: %w", err)
	}
	defer fh.Close() //nolint:errcheck // read-only
	st, serr := fh.Stat()
	if serr != nil {
		return nil, nil, fmt.Errorf("directory: %w", serr)
	}
	data, err = io.ReadAll(fh)
	if err != nil {
		return nil, nil, fmt.Errorf("directory: %w", err)
	}
	return data, st, nil
}

func sameStamp(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.ModTime().Equal(b.ModTime()) && a.Size() == b.Size()
}

// Reload re-reads the file if it changed since the last load or write and installs it when it
// parses, validates, and carries a higher version. It returns true when a new version was
// installed. A rejected change keeps the last good snapshot; its error is returned once per
// change, not on every poll.
func (d *FileDir) Reload() (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := os.Stat(d.path)
	if err != nil {
		if msg := err.Error(); msg != d.statErr {
			d.statErr = msg
			return false, fmt.Errorf("directory: keeping version %d: %w", d.snap.Load().Version(), err)
		}
		return false, nil
	}
	d.statErr = ""
	if d.stamp != nil && sameStamp(d.stamp, st) {
		return false, nil
	}
	data, st, err := readFile(d.path)
	if err != nil {
		return false, err
	}
	d.stamp = st
	cur := d.snap.Load().Version()
	f, err := parse(data)
	if err == nil {
		err = validate(f, d.clusters)
	}
	if err != nil {
		return false, fmt.Errorf("directory: reload of %s rejected, keeping version %d: %w", d.path, cur, err)
	}
	if f.Version <= cur {
		if f.Version == cur && sha256.Sum256(data) == d.sum {
			return false, nil // touched, not changed
		}
		return false, fmt.Errorf("%w: %s has version %d, loaded version is %d; keeping version %d", errStaleVersion, d.path, f.Version, cur, cur)
	}
	d.install(f, data, st)
	return true, nil
}

// Create implements Directory.
func (d *FileDir) Create(ctx context.Context, tenant, bucket, cluster, backend, actor string) error {
	k := Key(tenant, bucket)
	return d.mutate(ctx, actor, "create", k, func(f *File) error {
		if _, ok := f.Placements[k]; ok {
			return ErrExists
		}
		if _, ok := f.Tenants[tenant]; !ok {
			return fmt.Errorf("%w: tenant %q is not in the directory", ErrNotFound, tenant)
		}
		f.Placements[k] = Placement{
			State: StateActive, Primary: cluster, Names: map[string]string{cluster: backend},
			Created: d.now().UTC().Truncate(time.Millisecond),
		}
		return nil
	})
}

// Delete implements Directory.
func (d *FileDir) Delete(ctx context.Context, tenant, bucket, actor string) error {
	k := Key(tenant, bucket)
	return d.mutate(ctx, actor, "delete", k, func(f *File) error {
		p, ok := f.Placements[k]
		if !ok {
			return ErrNotFound
		}
		if p.State != StateActive {
			return fmt.Errorf("%w: %s is %s; only an ACTIVE placement can be deleted", errConflict, k, p.State)
		}
		delete(f.Placements, k)
		return nil
	})
}

// SetTenantDefault repoints a tenant's default cluster: the cluster its next CreateBucket lands on.
// It is not part of the Directory seam — the proxy never changes it — but an operator needs it when
// a cluster is being retired, or every new bucket keeps landing on the machine being switched off.
func (d *FileDir) SetTenantDefault(ctx context.Context, tenant, cluster, actor string) error {
	return d.mutate(ctx, actor, "set-default", tenant, func(f *File) error {
		t, ok := f.Tenants[tenant]
		if !ok {
			return fmt.Errorf("%w: tenant %q is not in the directory", ErrNotFound, tenant)
		}
		if _, ok := d.clusters[cluster]; !ok {
			return fmt.Errorf("%w: cluster %q is not configured", ErrNotFound, cluster)
		}
		if t.DefaultCluster == cluster {
			return fmt.Errorf("%w: %s already defaults to %s", errConflict, tenant, cluster)
		}
		t.DefaultCluster = cluster
		f.Tenants[tenant] = t
		return nil
	})
}

// SetState implements Directory.
func (d *FileDir) SetState(ctx context.Context, tenant, bucket, from string, t Transition, actor string) error {
	k := Key(tenant, bucket)
	return d.mutate(ctx, actor, "set-state", k, func(f *File) error {
		p, ok := f.Placements[k]
		if !ok {
			return ErrNotFound
		}
		if p.State != from {
			return fmt.Errorf("%w: %s is %s on disk, expected %s", errConflict, k, p.State, from)
		}
		np, err := Apply(p, t)
		if err != nil {
			return err
		}
		f.Placements[k] = np
		return nil
	})
}

func (d *FileDir) now() time.Time {
	if d.Now == nil {
		return time.Now()
	}
	return d.Now()
}

func (d *FileDir) mutate(ctx context.Context, actor, op, k string, fn func(*File) error) error {
	release, err := d.lock(ctx)
	if err != nil {
		return err
	}
	defer release()
	d.mu.Lock()
	defer d.mu.Unlock()

	data, _, err := readFile(d.path)
	if err != nil {
		return err
	}
	f, err := parse(data)
	if err == nil {
		err = validate(f, d.clusters)
	}
	if err != nil {
		return fmt.Errorf("directory: %s on disk is invalid; refusing to write: %w", d.path, err)
	}
	if f.Placements == nil {
		f.Placements = map[string]Placement{}
	}
	var before *Placement
	if p, ok := f.Placements[k]; ok {
		c := p.clone()
		before = &c
	}
	if ferr := fn(f); ferr != nil {
		return ferr
	}
	f.Version++
	if verr := validate(f, d.clusters); verr != nil {
		return verr
	}
	out, err := marshal(f)
	if err != nil {
		return err
	}
	if err := writeAtomic(d.path, out); err != nil {
		return classifyWrite(err)
	}
	st, _ := os.Stat(d.path)
	d.install(f, out, st)

	var after *Placement
	if p, ok := f.Placements[k]; ok {
		after = &p
	}
	if err := d.appendChange(Change{Time: d.now().UTC(), Actor: actor, Op: op, Key: k, Version: f.Version, Before: before, After: after}); err != nil && d.ChangeLogError != nil {
		d.ChangeLogError(err)
	}
	return nil
}

func (d *FileDir) lock(ctx context.Context) (func(), error) {
	lf, err := os.OpenFile(d.path+".lock", os.O_CREATE|os.O_RDWR, 0o644) //nolint:gosec // G302/G304: operator-configured path; the lock file holds no data
	if err != nil {
		return nil, classifyWrite(err)
	}
	fd := int(lf.Fd()) //nolint:gosec // G115: a file descriptor fits in int
	timeout := d.LockTimeout
	if timeout <= 0 {
		timeout = defaultLockTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(fd, syscall.LOCK_UN)
				_ = lf.Close()
			}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			_ = lf.Close()
			return nil, fmt.Errorf("directory: lock %s: %w", lf.Name(), err)
		}
		if !time.Now().Before(deadline) {
			_ = lf.Close()
			return nil, ErrLockTimeout
		}
		t := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			_ = lf.Close()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
}

// writeAtomic replaces path with data: temp file in the same directory, fsync, rename, fsync the
// directory. Readers see the old file or the new one, never a partial write.
func writeAtomic(path string, data []byte) error {
	mode := os.FileMode(0o644)
	if st, err := os.Stat(path); err == nil {
		mode = st.Mode().Perm()
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	done := false
	defer func() {
		if !done {
			_ = os.Remove(name)
		}
	}()
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	done = true
	if dh, err := os.Open(dir); err == nil { //nolint:gosec // G304: the directory holding the configured file
		_ = dh.Sync()
		_ = dh.Close()
	}
	return nil
}

func classifyWrite(err error) error {
	if errors.Is(err, syscall.EROFS) || errors.Is(err, fs.ErrPermission) {
		return fmt.Errorf("%w: %w", ErrReadOnly, err)
	}
	return fmt.Errorf("directory: write: %w", err)
}

func (d *FileDir) appendChange(c Change) error {
	line, err := json.Marshal(c)
	if err != nil {
		return err
	}
	fh, err := os.OpenFile(d.ChangeLogPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644) //nolint:gosec // G302: the change log holds no secrets
	if err != nil {
		return err
	}
	if _, err := fh.Write(append(line, '\n')); err != nil {
		_ = fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		_ = fh.Close()
		return err
	}
	return fh.Close()
}
