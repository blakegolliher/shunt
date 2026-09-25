package member

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// cacheSchema is the restart cache's format. A cache of any other schema, including the bare
// directory an older shunt wrote, is ignored and the directory fetched.
const cacheSchema = 1

// maxDirectory bounds a directory read from the control plane or from the cache, before any of it
// is decoded.
const maxDirectory = 64 << 20

// cacheEnvelope is the restart cache (ADR-0021 D1): the directory with what proves it is this
// proxy's, whole and of a format this build reads. SHA256 is over Directory exactly as written, so
// a torn or altered file is refused even when it still parses.
type cacheEnvelope struct {
	Schema    int                `json:"schema"`
	Protocol  int                `json:"protocol"`
	ProxyID   string             `json:"proxy_id"`
	Identity  directory.Identity `json:"identity"`
	Version   int64              `json:"version"`
	SHA256    string             `json:"sha256"`
	Directory json.RawMessage    `json:"directory"`
}

// cacheOps are the cache write's file operations, one per failure stage of
// shunt_directory_cache_failures_total; the fault tests fail them one at a time.
type cacheOps struct {
	write   func(f *os.File, data []byte) error
	fsync   func(f *os.File) error
	rename  func(from, to string) error
	dirSync func(dir string) error
}

var osCacheOps = cacheOps{
	write:  func(f *os.File, data []byte) error { _, err := f.Write(data); return err },
	fsync:  func(f *os.File) error { return f.Sync() },
	rename: os.Rename,
	dirSync: func(dir string) error {
		d, err := os.Open(dir) //nolint:gosec // the configured cache directory
		if err != nil {
			return err
		}
		serr := d.Sync()
		if cerr := d.Close(); serr == nil {
			serr = cerr
		}
		return serr
	},
}

// Load installs the cached directory, if any: a proxy restarting with the control plane down
// serves ACTIVE buckets from it, stale, until the control plane is back. A cache that is torn,
// altered, too large, of another format or written by another proxy is ignored with a warning and
// the proxy starts empty: it fetches the directory once the control plane answers.
func (c *Client) Load() error {
	if err := os.MkdirAll(c.cfg.CacheDir, 0o700); err != nil {
		return fmt.Errorf("control.cache_dir: %w", err)
	}
	if c.started.IsZero() {
		c.started = c.Now().UTC().Truncate(time.Second)
	}
	// The incarnation marker (ADR-0021 D2): what the previous process left, then this one's, on
	// disk before anything is served. A marker that cannot be written keeps the proxy from
	// starting: a process whose end no one could learn of must not serve.
	c.previous = c.readPrevious()
	if p := c.previous; p != nil && (p.State == control.IncarnationUnclean || p.Uncertain > 0) {
		c.log.Warn("the previous process of this proxy did not retire cleanly; the control plane is told, and every barrier waits on it until an operator resolves it",
			"incarnation", p.ID, "state", p.State, "uncertain", p.Uncertain)
	}
	if err := c.mark(control.IncarnationActive, 0); err != nil {
		return fmt.Errorf("incarnation marker: %w", err)
	}
	f, err := os.Open(c.cacheFile())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("directory cache: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxDirectory+1))
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("directory cache: %w", err)
	}
	d, why := c.decodeCache(data)
	if why == "" {
		if _, err := c.apply(d); err != nil {
			why = "it does not install: " + err.Error()
		}
	}
	if why != "" {
		c.log.Warn("directory cache ignored; the proxy starts empty and fetches the directory from the control plane", "cache", c.cacheFile(), "why", why)
		return nil
	}
	c.caching.Lock()
	c.cached = d.Version
	c.caching.Unlock()
	c.durable.Store(d.Version)
	c.log.Info("directory loaded from the local cache; serving it stale until the control plane answers", "version", d.Version, "cache", c.cacheFile())
	return nil
}

// decodeCache checks a cache file and returns its directory, or why it cannot be used.
func (c *Client) decodeCache(data []byte) (d *control.Directory, why string) {
	if len(data) > maxDirectory {
		return nil, fmt.Sprintf("larger than %d bytes", maxDirectory)
	}
	var env cacheEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, "torn or corrupt: " + err.Error()
	}
	switch {
	case env.Schema != cacheSchema:
		return nil, fmt.Sprintf("cache schema %d, this build reads %d (an older shunt wrote it)", env.Schema, cacheSchema)
	case env.Protocol != control.Protocol:
		return nil, fmt.Sprintf("fleet protocol %d, this build speaks %d", env.Protocol, control.Protocol)
	case env.ProxyID != c.cfg.ProxyID:
		return nil, fmt.Sprintf("written by proxy %q, this is %q: a cache is never taken from another proxy", env.ProxyID, c.cfg.ProxyID)
	}
	if sum := sha256.Sum256(env.Directory); hex.EncodeToString(sum[:]) != env.SHA256 {
		return nil, "checksum mismatch: torn or altered"
	}
	d = &control.Directory{}
	if err := json.Unmarshal(env.Directory, d); err != nil {
		return nil, "directory does not decode: " + err.Error()
	}
	if d.Identity.IsZero() || d.Identity != env.Identity || d.Version != env.Version {
		return nil, "the directory does not match the cache's identity and version"
	}
	return d, ""
}

// persist writes d to the restart cache unless a newer version was already attempted. Writes are
// serialized, so the cache moves forward like the install. A failure keeps the in-memory version
// and the last durable cache, and is reported (status cache_error, the durable version in the
// heartbeat) until a later write succeeds.
func (c *Client) persist(d *control.Directory) {
	c.caching.Lock()
	defer c.caching.Unlock()
	if d.Version <= c.cached {
		return
	}
	c.cached = d.Version
	stage, err := c.writeCache(d)
	if err != nil {
		why := fmt.Sprintf("version %d not durable: %s: %v", d.Version, stage, err)
		c.cacheErr.Store(&why)
		if c.Metrics != nil {
			c.Metrics.CacheFailures.WithLabelValues(stage).Inc()
		}
		c.log.Warn("directory cache not durable; a restart with the control plane down would serve the last durable version", "version", d.Version, "durable", c.durable.Load(), "stage", stage, "err", err.Error())
		return
	}
	c.cacheErr.Store(nil)
	c.durable.Store(d.Version)
}

// writeCache writes the envelope to a 0600 temporary file in the cache directory, syncs it, renames
// it over the cache and syncs the directory, and returns the stage that failed. Until the rename
// the old cache is untouched; a failed directory sync leaves the new file in place, but not
// provably durable.
func (c *Client) writeCache(d *control.Directory) (stage string, err error) {
	dir, err := json.Marshal(d)
	if err != nil {
		return "write", err
	}
	sum := sha256.Sum256(dir)
	data, err := json.Marshal(cacheEnvelope{Schema: cacheSchema, Protocol: control.Protocol, ProxyID: c.cfg.ProxyID,
		Identity: d.Identity, Version: d.Version, SHA256: hex.EncodeToString(sum[:]), Directory: dir})
	if err != nil {
		return "write", err
	}
	tmp, err := os.CreateTemp(c.cfg.CacheDir, ".directory-*") // 0600
	if err != nil {
		return "write", err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmp.Name())
		}
	}()
	if err := c.ops.write(tmp, data); err != nil {
		_ = tmp.Close()
		return "write", err
	}
	if err := c.ops.fsync(tmp); err != nil {
		_ = tmp.Close()
		return "fsync", err
	}
	if err := tmp.Close(); err != nil {
		return "write", err
	}
	if err := c.ops.rename(tmp.Name(), c.cacheFile()); err != nil {
		return "rename", err
	}
	renamed = true
	if err := c.ops.dirSync(filepath.Dir(c.cacheFile())); err != nil {
		return "dir_fsync", err
	}
	return "", nil
}
