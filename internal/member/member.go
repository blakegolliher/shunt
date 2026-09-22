// Package member is a proxy's side of the control plane (ADR-0015, ADR-0016): it takes the
// directory, the client keys and the cluster secrets from shunt-control, keeps a copy on local
// disk for a restart with the control plane down, forwards bucket creation to the control plane,
// sends it a heartbeat, and knows when its lease has lapsed. Nothing here is shared with another
// host.
package member

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// Config is what a member needs: where the control plane is and who this proxy is.
type Config struct {
	Endpoints []string // shunt-control API addresses; any one is enough
	Token     string
	ProxyID   string
	CacheDir  string
	Interval  time.Duration // heartbeat interval
	LeaseTTL  time.Duration
	// LongPoll is how long a directory poll waits for a newer version before returning empty.
	LongPoll time.Duration
	// Host and Version are reported in every heartbeat, for the fleet view: where this proxy
	// runs and which build it is.
	Host    string
	Version string
}

// Client is a member proxy's directory, credential store and heartbeat. It implements
// directory.Directory: reads are served from the installed snapshot, writes are forwarded.
type Client struct {
	cfg  Config
	keys *auth.Static
	log  *slog.Logger
	http *http.Client
	// Prepare, if set, is called with each directory before it is installed (the proxy builds its
	// clusters with it); an error refuses the version, and the last good one stays.
	Prepare func(*directory.File) error
	// OnInstall, if set, is called after every installed version.
	OnInstall func(*directory.Snapshot)
	Metrics   *telemetry.Metrics
	Telemetry *telemetry.Collector
	Now       func() time.Time

	snap    atomic.Pointer[directory.Snapshot]
	secrets atomic.Pointer[map[string]string]
	mu      sync.Mutex
	cond    *sync.Cond // broadcast on every install, for WaitVersion
	ep      atomic.Int32
	started time.Time
	seq     atomic.Int64
	lastAck atomic.Pointer[time.Time]
	stale   atomic.Bool
}

var _ directory.Directory = (*Client)(nil)

// New returns a member with an empty directory and no keys: Load the cache, then Start.
func New(cfg Config, log *slog.Logger) *Client {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if cfg.LongPoll == 0 {
		cfg.LongPoll = 30 * time.Second
	}
	c := &Client{cfg: cfg, keys: auth.NewEmpty(), log: log, http: &http.Client{Timeout: cfg.LongPoll + 10*time.Second}, Now: time.Now}
	c.cond = sync.NewCond(&c.mu)
	c.snap.Store(directory.NewSnapshot(&directory.File{}))
	empty := map[string]string{}
	c.secrets.Store(&empty)
	return c
}

// Keys is the credential store fed by the control plane: the proxy verifies clients with it.
func (c *Client) Keys() *auth.Static { return c.keys }

// Snapshot implements directory.Directory.
func (c *Client) Snapshot() *directory.Snapshot { return c.snap.Load() }

// Resolve resolves a cluster's secret_ref from the secrets the control plane delivered, or from
// the environment or a file for env:/file: refs. It is the resolver the proxy's cluster registry
// is built with.
func (c *Client) Resolve(ref string) (string, error) {
	if !strings.HasPrefix(ref, "control:") {
		return config.ResolveSecret(ref)
	}
	if v, ok := (*c.secrets.Load())[ref]; ok {
		return v, nil
	}
	return "", fmt.Errorf("the control plane delivered no secret for %s", ref)
}

// Stale reports whether the lease has lapsed (ADR-0016): the last acknowledged heartbeat, sent
// with the current directory installed, is older than the lease. A member that has never had one
// answered is stale: it may be starting with the control plane unreachable, with no way to know
// whether a bucket it sees as ACTIVE has started to move.
func (c *Client) Stale() bool {
	ack := c.lastAck.Load()
	return ack == nil || c.Now().Sub(*ack) > c.cfg.LeaseTTL
}

// cacheFile is the last installed directory, with its secrets, on local disk (0600).
func (c *Client) cacheFile() string { return filepath.Join(c.cfg.CacheDir, "directory.json") }

// Load installs the cached directory, if any: a proxy restarting with the control plane down
// serves ACTIVE buckets from it, stale, until the control plane is back.
func (c *Client) Load() error {
	if err := os.MkdirAll(c.cfg.CacheDir, 0o700); err != nil {
		return fmt.Errorf("control.cache_dir: %w", err)
	}
	data, err := os.ReadFile(c.cacheFile())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("directory cache: %w", err)
	}
	var d control.Directory
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("directory cache %s: %w", c.cacheFile(), err)
	}
	if err := c.install(&d, false); err != nil {
		return fmt.Errorf("directory cache %s: %w", c.cacheFile(), err)
	}
	c.log.Info("directory loaded from the local cache; serving it stale until the control plane answers", "version", d.Version, "cache", c.cacheFile())
	return nil
}

// install validates and installs one directory version, and writes the cache when asked.
func (c *Client) install(d *control.Directory, cache bool) error {
	f := &d.File
	config.ApplyClusterDefaults(f.Clusters)
	if err := directory.Validate(f); err != nil {
		return err
	}
	secrets := d.Secrets
	if secrets == nil {
		secrets = map[string]string{}
	}
	// The candidate's secrets must resolve while Prepare builds its clusters.
	c.secrets.Store(&secrets)
	if c.Prepare != nil {
		if err := c.Prepare(f); err != nil {
			old := c.snapSecrets()
			c.secrets.Store(&old)
			return err
		}
	}
	creds := make([]sigv4.Credential, 0, len(d.Credentials))
	for _, k := range d.Credentials {
		creds = append(creds, sigv4.Credential{AccessKey: k.AccessKey, Secret: k.Secret, Tenant: k.Tenant, Buckets: k.Buckets})
	}
	c.keys.Replace(creds)
	snap := directory.NewSnapshot(f)
	c.snap.Store(snap)
	c.mu.Lock()
	c.cond.Broadcast()
	c.mu.Unlock()
	if c.OnInstall != nil {
		c.OnInstall(snap)
	}
	if cache {
		if err := c.writeCache(d); err != nil {
			c.log.Warn("directory cache not written; a restart with the control plane down would start empty", "err", err.Error())
		}
	}
	return nil
}

// snapSecrets returns the secrets of the installed version, for rolling back a refused candidate.
func (c *Client) snapSecrets() map[string]string {
	if s := c.secrets.Load(); s != nil {
		return *s
	}
	return map[string]string{}
}

func (c *Client) writeCache(d *control.Directory) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.cfg.CacheDir, ".directory-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
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
	return os.Rename(tmp.Name(), c.cacheFile())
}

// WaitVersion blocks until the installed version is at least v, or ctx ends.
func (c *Client) WaitVersion(ctx context.Context, v int64) error {
	if c.Snapshot().Version() >= v {
		return nil
	}
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.Snapshot().Version() < v {
		if ctx.Err() != nil {
			return fmt.Errorf("directory version %d not installed yet (at %d): %w", v, c.Snapshot().Version(), ctx.Err())
		}
		c.cond.Wait()
	}
	return nil
}

// endpoint is the control node this member currently talks to; a failure moves to the next.
func (c *Client) endpoint() string {
	return strings.TrimRight(c.cfg.Endpoints[int(c.ep.Load())%len(c.cfg.Endpoints)], "/")
}

func (c *Client) nextEndpoint() {
	c.ep.Add(1)
}

// call sends one request to the current control node; a non-2xx answer is an error carrying the
// API's message. 304 answers io.EOF-free: out is left untouched and notModified is true.
func (c *Client) call(ctx context.Context, method, path string, body, out any) (notModified bool, err error) {
	var rd io.Reader = http.NoBody
	if body != nil {
		b, merr := json.Marshal(body)
		if merr != nil {
			return false, merr
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint()+path, rd)
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Shunt-Proxy", c.cfg.ProxyID) // who is asking, for the control plane's logs
	if c.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.nextEndpoint()
		return false, err
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	if resp.StatusCode == http.StatusNotModified {
		return true, nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return false, err
	}
	if resp.StatusCode >= 300 {
		if resp.StatusCode >= 500 {
			c.nextEndpoint()
		}
		var e control.Error
		if json.Unmarshal(data, &e) == nil && e.Message != "" {
			return false, &Error{Status: resp.StatusCode, Code: e.Code, Message: e.Message}
		}
		return false, &Error{Status: resp.StatusCode, Code: "http", Message: strings.TrimSpace(string(data))}
	}
	if out != nil {
		return false, json.Unmarshal(data, out)
	}
	return false, nil
}

// Error is a control API answer other than success.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// Is maps the API's codes onto the directory's errors, so the proxy's CreateBucket and
// DeleteBucket paths behave as they do on a file.
func (e *Error) Is(target error) bool {
	switch target { //nolint:errorlint // sentinel identity is the point
	case directory.ErrExists:
		return e.Code == "conflict" && strings.Contains(e.Message, "already exists")
	case directory.ErrConflict:
		return e.Code == "conflict"
	case directory.ErrNotFound:
		return e.Status == http.StatusNotFound
	case directory.ErrReadOnly:
		return e.Status == http.StatusServiceUnavailable
	}
	return false
}

// fetch gets the directory once it is newer than since, waiting up to wait, and installs it.
func (c *Client) fetch(ctx context.Context, since int64, wait time.Duration) (installed bool, err error) {
	var d control.Directory
	notModified, err := c.call(ctx, http.MethodGet, fmt.Sprintf("/v1/directory?since=%d&wait=%s", since, wait), nil, &d)
	if err != nil || notModified {
		return false, err
	}
	if d.Version <= c.Snapshot().Version() {
		return false, nil
	}
	if err := c.install(&d, true); err != nil {
		return false, fmt.Errorf("directory version %d refused on this proxy: %w", d.Version, err)
	}
	c.log.Info("directory installed", "version", d.Version, "clusters", len(d.Clusters), "placements", len(d.Placements), "keys", len(d.Credentials))
	return true, nil
}

// Register fetches the directory and sends the first heartbeat, before this proxy serves anything,
// so the control plane counts it as a member from its first request (ADR-0016). An error means the
// control plane could not be reached; the proxy may still start, stale, and Run keeps trying.
func (c *Client) Register(ctx context.Context) error {
	c.started = c.Now().UTC().Truncate(time.Second)
	if _, err := c.fetch(ctx, c.Snapshot().Version(), 0); err != nil {
		return err
	}
	return c.beat(ctx)
}

// Run long-polls the directory and heartbeats until ctx ends.
func (c *Client) Run(ctx context.Context) {
	if c.started.IsZero() {
		c.started = c.Now().UTC().Truncate(time.Second)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if _, err := c.fetch(ctx, c.Snapshot().Version(), c.cfg.LongPoll); err != nil && ctx.Err() == nil {
				c.log.Warn("directory poll failed", "control", c.endpoint(), "err", err.Error())
				select {
				case <-ctx.Done():
				case <-time.After(c.cfg.Interval):
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		t := time.NewTicker(c.cfg.Interval)
		defer t.Stop()
		for {
			_ = c.beat(ctx) //nolint:errcheck // beat logs lease changes itself; the next tick retries
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	wg.Wait()
}

// beat sends one heartbeat and records whether the lease holds. The lease renews only once this
// proxy has installed the version the control plane answered with: a member back from silence
// may have missed steps that went ahead without it (ADR-0016).
func (c *Client) beat(ctx context.Context) error {
	sent := c.Now()
	snap := c.Snapshot()
	hb := control.Heartbeat{Started: c.started, Seq: c.seq.Add(1), Applied: snap.Version(), Host: c.cfg.Host, Version: c.cfg.Version}
	if c.Telemetry != nil {
		hb.Telemetry = c.Telemetry.Completed(c.Now())
	}
	f := snap.File()
	for key := range f.Placements {
		if f.Placements[key].State == directory.StateActive {
			continue
		}
		if hb.FallbackReads == nil {
			hb.FallbackReads = map[string]float64{}
		}
		if c.Metrics != nil {
			hb.FallbackReads[key] = control.Counters(c.Metrics.FallbackReads, key, "")[""]
		}
	}
	bctx, cancel := context.WithTimeout(ctx, c.cfg.Interval)
	defer cancel()
	var ans control.HeartbeatAnswer
	_, err := c.call(bctx, http.MethodPost, "/v1/fleet/"+c.cfg.ProxyID+"/heartbeat", hb, &ans)
	if err == nil {
		if ans.Version > snap.Version() {
			// The control plane has a newer directory: fetch it now, not at the poll's next turn.
			_, _ = c.fetch(ctx, snap.Version(), 0) //nolint:errcheck // the poll loop retries
		}
		if v := c.Snapshot().Version(); v >= ans.Version {
			c.lastAck.Store(&sent)
		} else {
			err = fmt.Errorf("directory version %d is behind the control plane's %d", v, ans.Version)
		}
	}
	stale := c.Stale()
	if was := c.stale.Swap(stale); was != stale {
		if stale {
			c.log.Warn("lease with the control plane lapsed: writes on moving buckets are refused until it is back (ADR-0016)",
				"control", c.endpoint(), "lease_ttl", c.cfg.LeaseTTL.String(), "err", errString(err))
		} else {
			c.log.Info("lease with the control plane holds", "control", c.endpoint(), "proxy", c.cfg.ProxyID)
		}
	}
	if c.Metrics != nil {
		v := 0.0
		if stale {
			v = 1
		}
		c.Metrics.FleetStale.Set(v)
	}
	return err
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// writeTimeout bounds a forwarded directory write and the wait for its version to arrive.
const writeTimeout = 15 * time.Second

// Create implements directory.Directory: forwarded to the control plane, then waited for, so the
// caller's next Snapshot shows it.
func (c *Client) Create(ctx context.Context, tenant, bucket, cluster, backend, actor string) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	var res control.VersionResult
	if _, err := c.call(ctx, http.MethodPost, "/v1/placements/"+tenant+"/"+bucket+"/create", control.CreateRequest{Cluster: cluster, Name: backend, Actor: actor}, &res); err != nil {
		return err
	}
	return c.after(ctx, res.Version)
}

// Delete implements directory.Directory.
func (c *Client) Delete(ctx context.Context, tenant, bucket, actor string) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	var res control.VersionResult
	if _, err := c.call(ctx, http.MethodDelete, "/v1/placements/"+tenant+"/"+bucket+"?actor="+actor, nil, &res); err != nil {
		return err
	}
	return c.after(ctx, res.Version)
}

// after fetches the version a write produced, so the write is visible here before it is answered.
func (c *Client) after(ctx context.Context, v int64) error {
	if _, err := c.fetch(ctx, c.Snapshot().Version(), 0); err != nil {
		return err
	}
	return c.WaitVersion(ctx, v)
}

// Status is a member's answer on /-/fleet.
type Status struct {
	ID          string `json:"id"`
	ControlNode string `json:"control_node"`
	Stale       bool   `json:"stale"`
	LastAck     string `json:"last_ack,omitempty"` // how long ago
	Applied     int64  `json:"applied"`
}

// ServeHTTP answers /-/fleet on the proxy's admin listener.
func (c *Client) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	st := Status{ID: c.cfg.ProxyID, ControlNode: c.endpoint(), Stale: c.Stale(), Applied: c.Snapshot().Version()}
	if ack := c.lastAck.Load(); ack != nil {
		st.LastAck = c.Now().Sub(*ack).Round(time.Millisecond).String()
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(st)
}
