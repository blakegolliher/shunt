// Package member is a proxy's side of the control plane (ADR-0015, ADR-0016): it takes the
// directory, the client keys and the cluster secrets from shunt-control, keeps a copy on local
// disk for a restart with the control plane down, forwards bucket creation to the control plane,
// sends it a heartbeat, and knows when its lease has lapsed. Nothing here is shared with another
// host.
package member

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
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
	// clusters with it); an error refuses the version, and the last good one stays. resolve
	// resolves the candidate's own secrets: nothing live sees them until commit, which runs just
	// before the version is installed.
	Prepare func(f *directory.File, resolve func(ref string) (string, error)) (commit func(), err error)
	// OnInstall, if set, is called after every installed version.
	OnInstall func(*directory.Snapshot)
	// Serving, if set, returns the snapshot new requests use: the published runtime bundle's, which
	// lags the installed one while installs are backpressured (ADR-0021 D1). Heartbeats report its
	// version as applied, and its secret generations, so a fence never counts a version no request
	// routes with yet. Unset: the installed snapshot.
	Serving   func() *directory.Snapshot
	Metrics   *telemetry.Metrics
	Telemetry *telemetry.Collector
	Now       func() time.Time

	// installing serializes install: the poll, a heartbeat's fetch and a forwarded write each fetch
	// on their own, and a slower older version must not be installed over a newer one.
	installing sync.Mutex
	snap       atomic.Pointer[directory.Snapshot]
	secrets    atomic.Pointer[map[string]string]
	mu         sync.Mutex
	cond       *sync.Cond // broadcast on every install, for WaitVersion
	ep         atomic.Int32
	started    time.Time
	seq        atomic.Int64
	lastAck    atomic.Pointer[time.Time]
	stale      atomic.Bool

	// lease is the grant held, replaced whole by renew; nil: never granted. Stale reads it on the
	// request path for every moving bucket, so it is a pointer load, not a lock.
	lease atomic.Pointer[grant]

	// caching serializes cache writes; cached is the last version a write was attempted for, so
	// a slower older write never lands after a newer one. durable is the version the cache holds
	// durably (renamed and its directory synced), and cacheErr why the newest attempt failed.
	caching  sync.Mutex
	cached   int64
	durable  atomic.Int64
	cacheErr atomic.Pointer[string]
	ops      cacheOps

	// lineage, once set, is why this member's directory is on another lineage than the control
	// plane's (ADR-0021): another cluster, or another epoch after a restore. The member stays stale
	// until it is re-enrolled; nothing it installed compares with the control plane's versions.
	lineage atomic.Pointer[string]
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
	c := &Client{cfg: cfg, keys: auth.NewEmpty(), log: log, http: &http.Client{Timeout: cfg.LongPoll + 10*time.Second}, Now: time.Now, ops: osCacheOps}
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

func (c *Client) serving() *directory.Snapshot {
	if c.Serving != nil {
		return c.Serving()
	}
	return c.Snapshot()
}

// Resolve resolves a cluster's secret_ref from the secrets the control plane delivered, or from
// the environment or a file for env:/file: refs. It is the resolver the proxy's cluster registry
// is built with.
func (c *Client) Resolve(ref string) (string, error) { return resolveFrom(*c.secrets.Load(), ref) }

func resolveFrom(secrets map[string]string, ref string) (string, error) {
	if !strings.HasPrefix(ref, "control:") {
		return config.ResolveSecret(ref)
	}
	if v, ok := secrets[ref]; ok {
		return v, nil
	}
	return "", fmt.Errorf("the control plane delivered no secret for %s", ref)
}

// Stale reports whether the lease has lapsed (ADR-0016). A lease runs from the send time of the
// heartbeat that earned it, for the shorter of the control plane's grant and this proxy's own
// lease_ttl (renew). A member that has never had one granted is stale: it may be starting with the
// control plane unreachable, with no way to know whether a bucket it sees as ACTIVE has started to
// move.
func (c *Client) Stale() bool {
	if c.lineage.Load() != nil {
		return true
	}
	g := c.lease.Load()
	return g == nil || c.Now().After(g.until)
}

// cacheFile is the last installed directory, with its secrets, on local disk (0600).
func (c *Client) cacheFile() string { return filepath.Join(c.cfg.CacheDir, "directory.json") }

// install validates and installs one directory version, and writes the cache when asked. A
// version no newer than the installed one is dropped (installed false, no error): installs are
// serialized, so the installed version, and the cache, only move forward. The cache is written
// after the install lock is let go, so a slow disk never holds up the next install.
func (c *Client) install(d *control.Directory, cache bool) (installed bool, err error) {
	if installed, err = c.apply(d); installed && cache {
		c.persist(d)
	}
	return installed, err
}

func (c *Client) apply(d *control.Directory) (installed bool, err error) {
	c.installing.Lock()
	defer c.installing.Unlock()
	f := &d.File
	if err := f.Identity.Validate(); err != nil {
		return false, fmt.Errorf("directory version %d carries no valid identity: %w", d.Version, err)
	}
	if cur := c.Snapshot().File().Identity; !cur.IsZero() && cur != f.Identity {
		return false, c.lineageFault(fmt.Sprintf("directory version %d is from cluster %s epoch %s; this proxy's is from cluster %s epoch %s", d.Version, f.Identity.ClusterID, f.Identity.Epoch, cur.ClusterID, cur.Epoch))
	}
	if d.Version <= c.Snapshot().Version() {
		return false, nil
	}
	config.ApplyClusterDefaults(f.Clusters)
	if err := directory.Validate(f); err != nil {
		return false, err
	}
	secrets := d.Secrets
	if secrets == nil {
		secrets = map[string]string{}
	}
	commit := func() {}
	if c.Prepare != nil {
		cm, err := c.Prepare(f, func(ref string) (string, error) { return resolveFrom(secrets, ref) })
		if err != nil {
			return false, err
		}
		commit = cm
	}
	c.secrets.Store(&secrets)
	commit()
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
	return true, nil
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
	if method != http.MethodGet {
		// A forwarded bucket create or delete is an operation on the control plane (ADR-0021). The
		// member never retries one itself; the client's own retry is a new request.
		var b [12]byte
		_, _ = rand.Read(b[:]) //nolint:errcheck // crypto/rand does not fail short
		req.Header.Set(control.HeaderIdempotencyKey, c.cfg.ProxyID+"-"+hex.EncodeToString(b[:]))
	}
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
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxDirectory))
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

// lineageFault records that this member's directory is on another lineage than the control
// plane's, and returns it as an error.
func (c *Client) lineageFault(why string) error {
	if c.lineage.Swap(&why) == nil {
		c.log.Error("this proxy's directory is from another lineage than the control plane's; it stays stale, refusing writes on moving buckets, until it is re-enrolled with an empty control.cache_dir (ADR-0021)", "reason", why)
	}
	return errors.New(why)
}

// checkAnswer records a lineage refusal from the control plane.
func (c *Client) checkAnswer(err error) error {
	var e *Error
	if errors.As(err, &e) && (e.Code == control.CodeEpochMismatch || e.Code == control.CodeClusterMismatch) {
		return c.lineageFault(e.Message)
	}
	return err
}

// fetch gets the directory once it is newer than since, waiting up to wait, and installs it. It
// names the installed lineage, so the control plane answers 304 only on the same one.
func (c *Client) fetch(ctx context.Context, since int64, wait time.Duration) (installed bool, err error) {
	var d control.Directory
	path := fmt.Sprintf("/v1/directory?since=%d&wait=%s", since, wait)
	if id := c.Snapshot().File().Identity; !id.IsZero() {
		path += "&cluster_id=" + id.ClusterID + "&epoch=" + id.Epoch
	} else {
		path = fmt.Sprintf("/v1/directory?since=0&wait=%s", wait)
	}
	notModified, err := c.call(ctx, http.MethodGet, path, nil, &d)
	if err != nil || notModified {
		return false, c.checkAnswer(err)
	}
	installed, err = c.install(&d, true)
	if err != nil {
		return false, fmt.Errorf("directory version %d refused on this proxy: %w", d.Version, err)
	}
	if !installed {
		return false, nil
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

// beat sends one heartbeat and records whether the lease holds (renew).
func (c *Client) beat(ctx context.Context) error {
	seq := c.seq.Add(1)
	sent := c.Now()
	snap := c.serving()
	hb := control.Heartbeat{Protocol: control.Protocol, Identity: snap.File().Identity, Started: c.started, Seq: seq, Applied: snap.Version(),
		Durable: c.durable.Load(), Host: c.cfg.Host, Version: c.cfg.Version, Secrets: secretGenerations(snap.File())}
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
		err = c.renew(ctx, seq, sent, snap.Version(), ans)
	} else {
		err = c.checkAnswer(err)
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

// renew takes the lease granted in answer to heartbeat seq, sent at sent with version applied
// installed (ADR-0016). The lease runs from sent, never from the answer's arrival, for the shorter
// of the control plane's grant and this proxy's own lease_ttl, so a delayed answer cannot stretch
// it. It renews only once this proxy has installed the version the answer names: a member back
// from silence may have missed steps that went ahead without it. An answer to another heartbeat,
// a missing grant, a version behind the one this proxy sent, an answer older than the grant held,
// or one that arrives after its own lease has run out renews nothing.
func (c *Client) renew(ctx context.Context, seq int64, sent time.Time, applied int64, ans control.HeartbeatAnswer) error {
	switch {
	case ans.Seq != seq:
		return fmt.Errorf("heartbeat %d answered as heartbeat %d; not a lease", seq, ans.Seq)
	case ans.LeaseTTL <= 0:
		return fmt.Errorf("the control plane granted no lease (lease_ttl %s)", ans.LeaseTTL)
	case ans.Version < applied:
		return fmt.Errorf("the control plane answered with directory version %d, behind this proxy's %d; not a lease", ans.Version, applied)
	}
	if ans.Version > c.Snapshot().Version() {
		// The control plane has a newer directory: fetch it now, not at the poll's next turn.
		_, _ = c.fetch(ctx, c.Snapshot().Version(), 0) //nolint:errcheck // the poll loop retries
	}
	if id := c.Snapshot().File().Identity; id != ans.Identity {
		return fmt.Errorf("the heartbeat was answered for cluster %s epoch %s; this proxy's directory is cluster %s epoch %s; not a lease", ans.Identity.ClusterID, ans.Identity.Epoch, id.ClusterID, id.Epoch)
	}
	if v := c.Snapshot().Version(); v < ans.Version {
		return fmt.Errorf("directory version %d is behind the control plane's %d", v, ans.Version)
	}
	next := &grant{seq: seq, until: sent.Add(min(ans.LeaseTTL, c.cfg.LeaseTTL))}
	if c.Now().After(next.until) {
		return fmt.Errorf("heartbeat %d was answered after the lease it grants had run out", seq)
	}
	for {
		cur := c.lease.Load()
		if cur != nil && seq <= cur.seq {
			return nil // an older answer never replaces a newer grant
		}
		if c.lease.CompareAndSwap(cur, next) {
			c.lastAck.Store(&sent)
			return nil
		}
	}
}

// grant is one lease: the heartbeat that earned it, and when it runs out, from that heartbeat's
// send time on c.Now's clock.
type grant struct {
	seq   int64
	until time.Time
}

// secretGenerations are the secret generations of the installed directory, by cluster: what the
// runtime bundle published with it signs with, since both come from one install.
func secretGenerations(f *directory.File) map[string]string {
	var out map[string]string
	for name := range f.Clusters {
		if g := f.Generation(directory.SecretResource(name)); g > 0 {
			if out == nil {
				out = map[string]string{}
			}
			out[name] = strconv.FormatInt(g, 10)
		}
	}
	return out
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
	// LeaseLeft is how long the lease has to run; negative once it has lapsed.
	LeaseLeft string `json:"lease_left,omitempty"`
	Applied   int64  `json:"applied"`
	// Durable is the version the restart cache holds durably; CacheError why the newest version
	// is not durable, when it is not (ADR-0021 D1: installed and durable are reported apart).
	Durable    int64  `json:"durable"`
	CacheError string `json:"cache_error,omitempty"`
	// Identity is the installed directory's lineage; LineageFault says why it is not the control
	// plane's, when it is not.
	Identity     directory.Identity `json:"identity,omitzero"`
	LineageFault string             `json:"lineage_fault,omitempty"`
}

// ServeHTTP answers /-/fleet on the proxy's admin listener.
func (c *Client) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	st := Status{ID: c.cfg.ProxyID, ControlNode: c.endpoint(), Stale: c.Stale(), Applied: c.serving().Version(), Identity: c.Snapshot().File().Identity}
	if why := c.lineage.Load(); why != nil {
		st.LineageFault = *why
	}
	st.Durable = c.durable.Load()
	if why := c.cacheErr.Load(); why != nil {
		st.CacheError = *why
	}
	now := c.Now()
	if ack := c.lastAck.Load(); ack != nil {
		st.LastAck = now.Sub(*ack).Round(time.Millisecond).String()
	}
	if g := c.lease.Load(); g != nil {
		st.LeaseLeft = g.until.Sub(now).Round(time.Millisecond).String()
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(st)
}
