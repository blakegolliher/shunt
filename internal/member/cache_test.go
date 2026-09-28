package member

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// readCache decodes c's cache file as Load would.
func readCache(t *testing.T, c *Client) *control.Directory {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(c.cfg.CacheDir, "directory.json"))
	if err != nil {
		t.Fatal(err)
	}
	d, why := c.decodeCache(data)
	if why != "" {
		t.Fatalf("cache refused: %s", why)
	}
	return d
}

func status(t *testing.T, c *Client) Status {
	t.Helper()
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest("GET", "/-/fleet", nil))
	var st Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	return st
}

func restart(t *testing.T, c *Client) *Client {
	t.Helper()
	c2 := New(c.cfg, slog.New(slog.DiscardHandler))
	if err := c2.Load(); err != nil {
		t.Fatal(err)
	}
	return c2
}

// T04: a cache write that fails at any stage keeps the new version in memory, leaves the last
// durable cache for a restart (no temporary file behind), and is reported: status cache_error, the
// heartbeat's durable version and shunt_directory_cache_failures_total. The next good write clears
// it. A failed directory sync is the one stage after the rename: the new file is there, not proven.
func TestCacheFaults(t *testing.T) {
	injected := errors.New("injected")
	for _, stage := range []string{"write", "fsync", "rename", "dir_fsync"} {
		t.Run(stage, func(t *testing.T) {
			f := newFakeControl(t)
			c := newClient(t, f)
			c.Metrics = telemetry.NewMetrics()
			ctx := context.Background()
			if err := c.Register(ctx); err != nil {
				t.Fatal(err)
			}
			if d := c.durable.Load(); d != 1 {
				t.Fatalf("durable %d after the first install, want 1", d)
			}
			switch stage {
			case "write":
				c.ops.write = func(*os.File, []byte) error { return injected }
			case "fsync":
				c.ops.fsync = func(*os.File) error { return injected }
			case "rename":
				c.ops.rename = func(string, string) error { return injected }
			case "dir_fsync":
				c.ops.dirSync = func(string) error { return injected }
			}
			f.bump()
			if _, err := c.fetch(ctx, 1, 0); err != nil {
				t.Fatal(err)
			}
			if v := c.Snapshot().Version(); v != 2 {
				t.Fatalf("installed %d, want 2 whatever the cache did", v)
			}
			st := status(t, c)
			if st.Durable != 1 || !strings.Contains(st.CacheError, stage) || st.Applied != 2 {
				t.Fatalf("status: applied %d durable %d cache_error %q", st.Applied, st.Durable, st.CacheError)
			}
			var m dto.Metric
			if err := c.Metrics.CacheFailures.WithLabelValues(stage).Write(&m); err != nil || m.GetCounter().GetValue() != 1 {
				t.Fatalf("cache failures{stage=%s} = %v (%v)", stage, m.GetCounter().GetValue(), err)
			}
			if err := c.beat(ctx); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			hb := f.beats[len(f.beats)-1]
			f.mu.Unlock()
			if hb.Applied != 2 || hb.Durable != 1 {
				t.Fatalf("heartbeat applied %d durable %d, want 2 and 1", hb.Applied, hb.Durable)
			}
			entries, _ := os.ReadDir(c.cfg.CacheDir)
			if len(entries) != 2 || entries[0].Name() != "directory.json" || entries[1].Name() != markerFile {
				t.Fatalf("cache dir holds %v, want only directory.json and %s (no temporary file left)", entries, markerFile)
			}
			want := int64(1)
			if stage == "dir_fsync" {
				want = 2
			}
			if v := restart(t, c).Snapshot().Version(); v != want {
				t.Fatalf("a restart served version %d, want %d", v, want)
			}

			c.ops = osCacheOps
			f.bump()
			if _, err := c.fetch(ctx, 2, 0); err != nil {
				t.Fatal(err)
			}
			if st := status(t, c); st.Durable != 3 || st.CacheError != "" {
				t.Fatalf("after a good write: durable %d cache_error %q", st.Durable, st.CacheError)
			}
			if v := restart(t, c).Snapshot().Version(); v != 3 {
				t.Fatalf("a restart served version %d, want 3", v)
			}
		})
	}
}

// A cache that is torn, altered, of another schema, another proxy's, from before identities, or
// oversized is ignored: the proxy starts empty, nothing durable, and fetches. A good one loads.
func TestCacheRejects(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	if err := c.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(filepath.Join(c.cfg.CacheDir, "directory.json"))
	if err != nil {
		t.Fatal(err)
	}
	var env cacheEnvelope
	if err := json.Unmarshal(good, &env); err != nil {
		t.Fatal(err)
	}
	reseal := func(edit func(*cacheEnvelope)) []byte {
		e := env
		edit(&e)
		sum := sha256.Sum256(e.Directory)
		e.SHA256 = hex.EncodeToString(sum[:])
		data, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	bare, _ := json.Marshal(f.version(t, 1)) // the pre-envelope format
	noIdentity := reseal(func(e *cacheEnvelope) {
		d := f.version(t, 1)
		d.Identity = directory.Identity{}
		e.Directory, _ = json.Marshal(d)
		e.Identity = directory.Identity{}
	})
	cases := map[string][]byte{
		"torn":        good[:len(good)/2],
		"altered":     []byte(strings.Replace(string(good), `"cs"`, `"cz"`, 1)),
		"schema":      reseal(func(e *cacheEnvelope) { e.Schema = 2 }),
		"protocol":    reseal(func(e *cacheEnvelope) { e.Protocol = 1 }),
		"other proxy": reseal(func(e *cacheEnvelope) { e.ProxyID = "p2" }),
		"version":     reseal(func(e *cacheEnvelope) { e.Version = 9 }),
		"bare":        bare,
		"no identity": noIdentity,
		"oversized":   append([]byte("{"), make([]byte, maxDirectory)...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if string(data) == string(good) {
				t.Fatal("the case did not change the cache")
			}
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "directory.json"), data, 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := c.cfg
			cfg.CacheDir = dir
			c2 := New(cfg, slog.New(slog.DiscardHandler))
			if err := c2.Load(); err != nil {
				t.Fatal(err)
			}
			if v, d := c2.Snapshot().Version(), c2.durable.Load(); v != 0 || d != 0 {
				t.Fatalf("a %s cache was installed: version %d durable %d", name, v, d)
			}
		})
	}
	if c2 := restart(t, c); c2.Snapshot().Version() != 1 || c2.durable.Load() != 1 {
		t.Fatalf("the good cache: version %d durable %d", c2.Snapshot().Version(), c2.durable.Load())
	}
}

// BenchmarkCacheWrite is one durable cache write of the test directory: marshal, checksum, write,
// fsync, rename, directory sync.
func BenchmarkCacheWrite(b *testing.B) {
	c := New(Config{ProxyID: "p1", CacheDir: b.TempDir()}, slog.New(slog.DiscardHandler))
	d := &control.Directory{File: directory.File{Version: 1, Identity: testIdentity}}
	for b.Loop() {
		if stage, err := c.writeCache(d); err != nil {
			b.Fatal(stage, err)
		}
	}
}

// A cache write that failed is retried from the heartbeat, with no new directory version (third
// review, R3-08): at every stage, once the disk answers again and the retry's backoff has run out,
// the heartbeat writes the installed version, reports it durable, and clears the error, and a
// restart serves it. Before the backoff has run out nothing is written. Negative control: without
// the retry, durable stays behind until a newer version or a restart.
func TestCacheRecoversOnTheHeartbeat(t *testing.T) {
	injected := errors.New("injected")
	for _, stage := range []string{"write", "fsync", "rename", "dir_fsync"} {
		t.Run(stage, func(t *testing.T) {
			f := newFakeControl(t)
			c := newClient(t, f)
			now := time.Now()
			c.Now = func() time.Time { return now }
			ctx := context.Background()
			if err := c.Register(ctx); err != nil {
				t.Fatal(err)
			}
			switch stage {
			case "write":
				c.ops.write = func(*os.File, []byte) error { return injected }
			case "fsync":
				c.ops.fsync = func(*os.File) error { return injected }
			case "rename":
				c.ops.rename = func(string, string) error { return injected }
			case "dir_fsync":
				c.ops.dirSync = func(string) error { return injected }
			}
			f.bump()
			if _, err := c.fetch(ctx, 1, 0); err != nil {
				t.Fatal(err)
			}
			c.ops = osCacheOps // the disk answers again
			if err := c.beat(ctx); err != nil {
				t.Fatal(err)
			}
			if st := status(t, c); st.Durable != 1 {
				t.Fatalf("a retry before its backoff ran out: durable %d", st.Durable)
			}
			now = now.Add(c.cfg.Interval)
			if err := c.beat(ctx); err != nil {
				t.Fatal(err)
			}
			if st := status(t, c); st.Durable != 2 || st.CacheError != "" {
				t.Fatalf("after the retry: durable %d cache_error %q", st.Durable, st.CacheError)
			}
			f.mu.Lock()
			hb := f.beats[len(f.beats)-1]
			f.mu.Unlock()
			if hb.Durable != 2 {
				t.Fatalf("the heartbeat that retried reports durable %d, want 2", hb.Durable)
			}
			if v := restart(t, c).Snapshot().Version(); v != 2 {
				t.Fatalf("a restart served version %d, want 2", v)
			}
		})
	}
}

// The retry's backoff doubles while the disk keeps failing, up to a bound; a newer version written
// meanwhile cancels the retry, and the cache never moves back to the older version.
func TestCacheRetryBackoffAndCancellation(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	c.Metrics = telemetry.NewMetrics()
	now := time.Now()
	c.Now = func() time.Time { return now }
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	c.ops.rename = func(string, string) error { return errors.New("injected") }
	f.bump()
	if _, err := c.fetch(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	failures := func() float64 {
		var m dto.Metric
		if err := c.Metrics.CacheFailures.WithLabelValues("rename").Write(&m); err != nil {
			t.Fatal(err)
		}
		return m.GetCounter().GetValue()
	}
	step := c.cfg.Interval
	for i, want := range []float64{2, 2, 3} { // retry after one interval, then after two
		now = now.Add(step)
		if err := c.beat(ctx); err != nil {
			t.Fatal(err)
		}
		if got := failures(); got != want {
			t.Fatalf("beat %d: %v failed writes, want %v", i, got, want)
		}
	}
	c.ops = osCacheOps
	f.bump()
	if _, err := c.fetch(ctx, 2, 0); err != nil {
		t.Fatal(err)
	}
	now = now.Add(maxCacheRetryWait)
	if err := c.beat(ctx); err != nil {
		t.Fatal(err)
	}
	if st := status(t, c); st.Durable != 3 || st.CacheError != "" {
		t.Fatalf("after a newer version was written: durable %d cache_error %q", st.Durable, st.CacheError)
	}
	if v := restart(t, c).Snapshot().Version(); v != 3 {
		t.Fatalf("a restart served version %d, want 3", v)
	}
}

// The review's case (R3-08): a cache write fails, the disk recovers, and the installed version is
// fetched again, in full, and heartbeats go on. The refetched copy is written at once, without
// waiting for the retry's backoff. Negative control: without it, and without the heartbeat's retry,
// durable stays behind applied.
func TestCacheRecoversOnARefetch(t *testing.T) {
	f := newFakeControl(t)
	c := newClient(t, f)
	if err := c.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.bump()
	c.ops.rename = func(string, string) error { return errors.New("transient disk error") }
	if _, err := c.fetch(context.Background(), 1, 0); err != nil {
		t.Fatal(err)
	}
	c.ops = osCacheOps
	for range 3 {
		if _, err := c.fetch(context.Background(), 0, 0); err != nil {
			t.Fatal(err)
		}
		if err := c.beat(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.durable.Load() != c.Snapshot().Version() {
		t.Fatalf("disk recovered and current directory was fetched repeatedly, but durable=%d applied=%d error=%v", c.durable.Load(), c.Snapshot().Version(), c.cacheErr.Load())
	}
}
