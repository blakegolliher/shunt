package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/admin"
	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/listener"
	"github.com/blakegolliher/shunt/internal/member"
	"github.com/blakegolliher/shunt/internal/proxy"
	"github.com/blakegolliher/shunt/internal/runtimecfg"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

func newServe() *cobra.Command {
	var (
		cfgPath, stateDir, listen, adminAddr string
		plaintext                            bool
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the proxy",
		Long: "With --config, runs from a config file (TLS, tokens, domains: production).\n" +
			"Without one, `shunt serve --plaintext` runs a lab shunt from a state directory (--state-dir, created on\n" +
			"first start): clusters, buckets, their secrets and a generated client key all live there, and every\n" +
			"change after that is a command (`shunt cluster add`, `shunt adopt`, ...). `shunt client show` prints the\n" +
			"client key. Plain http is never a default: it takes --plaintext, and every log line says so.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cfgPath != "" {
				for _, f := range []string{"plaintext", "state-dir", "listen", "admin"} {
					if cmd.Flags().Changed(f) {
						return fmt.Errorf("--%s applies only without --config; set it in %s instead", f, cfgPath)
					}
				}
				cfg, err := config.Load(cfgPath)
				if err != nil {
					return err
				}
				return serve(cmd.Context(), cfg, cmd.ErrOrStderr())
			}
			if !plaintext {
				return errors.New("shunt serve needs --config (a TLS listener), or --plaintext for a lab: shunt serve --plaintext [--state-dir shunt-data]")
			}
			cfg, err := labConfig(stateDir, listen, adminAddr, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			return serve(cmd.Context(), cfg, cmd.ErrOrStderr())
		},
	}
	f := cmd.Flags()
	f.StringVarP(&cfgPath, "config", "c", "", "config file")
	f.BoolVar(&plaintext, "plaintext", false, "without --config: serve clients over plain http (labs only)")
	f.StringVar(&stateDir, "state-dir", "shunt-data", "without --config: where the directory, secrets and client key live")
	f.StringVar(&listen, "listen", "127.0.0.1:8008", "without --config: the S3 client listener")
	f.StringVar(&adminAddr, "admin", "127.0.0.1:9900", "without --config: the admin and control API listener")
	return cmd
}

// locationLeaks names the cluster types whose CompleteMultipartUpload <Location> echoes the host the
// request was sent to. In resign mode that host is the backend endpoint, so with the rewriter
// switched off the backend address reaches the client. With it on (the default) nothing leaks.
var locationLeaks = map[string]string{
	"minio": "verified: s3diff resign run 2026-09-15 returned <Location>http://127.0.0.1/…",
	"aws":   "AWS builds <Location> from the endpoint host it was addressed by",
	"vast":  "unverified: s3diff cannot see it because its direct and via requests share the VAST endpoint host; assume it leaks",
	"s3":    "unverified for generic S3 backends; assume it leaks (Garage builds <Location> from its root_domain and does not)",
}

// serve runs the proxy and admin servers until SIGTERM/SIGINT or ctx cancellation, then drains.
// In resign mode it also reloads the directory file on SIGHUP and every directory.poll_interval.
func serve(ctx context.Context, cfg *config.Config, stderr io.Writer) error {
	log := newLogger(cfg, stderr)
	if cfg.Listener.Plaintext {
		log.Warn("the client listener is PLAINTEXT http: signatures, credentials in presigned URLs, and object bytes cross the network unencrypted; listener.plaintext is for labs")
	}

	metrics := telemetry.NewMetrics()
	windows := telemetry.NewCollector()
	slow := telemetry.NewSlowRing(cfg.Telemetry.Slow.RingSize, cfg.Telemetry.Slow.Threshold)
	var accessOut *os.File
	if cfg.Telemetry.AccessLog.Enabled {
		accessOut = os.Stdout
		if p := cfg.Telemetry.AccessLog.Path; p != "" {
			f, ferr := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640) //nolint:gosec // operator-chosen log path
			if ferr != nil {
				return fmt.Errorf("access log: %w", ferr)
			}
			defer f.Close() //nolint:errcheck // best effort at exit
			accessOut = f
		}
	}
	var access *telemetry.AccessLogger
	if accessOut != nil {
		access = telemetry.NewAccessLogger(accessOut)
	} else {
		access = telemetry.NewAccessLogger(nil)
	}

	hcfg := proxy.Handler{
		Domains: s3.NewDomains(cfg.Listener.Domains), Metrics: metrics, Telemetry: windows, Access: access, Slow: slow,
		IdleTimeout: cfg.Proxy.IdleTimeout, MetadataTimeout: cfg.Proxy.MetadataTimeout,
		Via: "1.1 shunt/" + version, Log: log,
	}
	var dir *directory.FileDir
	var ctl *control.Server
	var mem *member.Client
	switch {
	case cfg.Control.Member():
		// A fleet member (ADR-0015): the directory, the client keys and the cluster secrets come
		// from shunt-control, and nothing on this host is shared with another.
		m, registry, rt, gates, err := startMember(ctx, cfg, metrics, log)
		if err != nil {
			return err
		}
		defer registry.Close()
		mem = m
		m.Telemetry = windows
		hcfg.Mode, hcfg.Runtime, hcfg.Dir, hcfg.Stale, hcfg.Gates = proxy.ModeResign, rt, m, m.Stale, gates
		hcfg.Rewrite, hcfg.ClockSkew, hcfg.DebugRoute = !cfg.KillSwitches.XMLRewriteDisable, cfg.Auth.ClockSkew, cfg.Features.DebugRouteHeader
		publishRouteState(metrics, m.Snapshot())
	case cfg.Auth.Mode == "resign":
		store, err := auth.Load(cfg.Auth.CredentialsFile)
		if err != nil {
			return err
		}
		dir, err = directory.Open(cfg.Directory.File)
		if err != nil {
			return err
		}
		dir.ChangeLogError = func(err error) {
			log.Error("directory change log append failed; the placement change itself is committed", "change_log", dir.ChangeLogPath(), "err", err.Error())
		}
		// Clusters are directory state (ADR-0008): the registry follows every installed version, and
		// is built before the version that names a cluster becomes visible to requests.
		registry := upstream.NewRegistry(upstream.Options{}, config.ResolveSecret)
		defer registry.Close()
		rewrite := !cfg.KillSwitches.XMLRewriteDisable
		started := false
		applyClusters := func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
			cand, aerr := registry.PrepareWith(f.Clusters, resolve, secretGeneration(f))
			if aerr != nil {
				log.Error("directory version refused: a cluster could not be built", "version", f.Version, "err", aerr.Error())
				return nil, aerr
			}
			return func() {
				before := registry.Load()
				added, removed := cand.Commit()
				for _, name := range added {
					cl, _ := registry.Load().Get(name)
					event := "cluster added"
					switch _, existed := before.Get(name); {
					case !started:
						event = "cluster ready"
					case existed:
						event = "cluster updated"
					}
					logCluster(log, event, cl, f.Clusters[name], rewrite)
				}
				started = true
				for _, name := range removed {
					log.Info("cluster removed", "cluster", name)
				}
			}, nil
		}
		commit, applyErr := applyClusters(dir.Snapshot().File(), nil)
		if applyErr != nil {
			return applyErr
		}
		commit()
		dir.Prepare = applyClusters
		events := control.NewEvents(0)
		// The runtime bundle follows the directory, the key file and the registry: after an install
		// the registry has committed that version's clusters, and a key change keeps the directory.
		rt := runtimecfg.NewPublisher(&runtimecfg.Bundle{Snapshot: dir.Snapshot(), Keys: store.Table(), Clusters: registry.Load()})
		rt.Observe = observeBundles(metrics, log)
		// The admission gates follow the directory (ADR-0021 D2): closed on install of a version
		// carrying a barrier, opened once requests route by the version without it.
		keeper := &proxy.GateKeeper{Gates: admission.New()}
		rt.Published = func(b *runtimecfg.Bundle) { keeper.Served(b.Snapshot) }
		keeper.Served(dir.Snapshot())
		keeper.Installed(dir.Snapshot())
		republish := func(s *directory.Snapshot, keys auth.Table) {
			if perr := rt.Refresh(s, keys, registry.Load()); perr != nil {
				log.Error("runtime bundle not published", "err", perr.Error())
			}
		}
		store.OnChange = func(t auth.Table) { republish(dir.Snapshot(), t) }
		dir.OnInstall = func(s *directory.Snapshot) {
			republish(s, store.Table())
			keeper.Installed(s)
			publishRouteState(metrics, s)
			warnUnknownRampHashes(log, s)
			events.Directory(s)
		}
		events.Directory(dir.Snapshot()) // the baseline: the first write is the first event
		warnUnknownRampHashes(log, dir.Snapshot())
		hcfg.Mode, hcfg.Runtime, hcfg.Dir, hcfg.Gates = proxy.ModeResign, rt, dir, keeper.Gates
		hcfg.Rewrite, hcfg.ClockSkew, hcfg.DebugRoute = rewrite, cfg.Auth.ClockSkew, cfg.Features.DebugRouteHeader
		if hcfg.DebugRoute {
			log.Warn("features.debug_route_header is on: any client sending X-Shunt-Debug: 1 learns which cluster served it (ADR-0006 amendment); for labs")
		}
		publishRouteState(metrics, dir.Snapshot())
		telemetryStore := telemetry.NewStore(10 * time.Second)
		telemetryStore.SetLocal("lab", windows)
		moverDir := filepath.Join(filepath.Dir(cfg.Directory.File), "mover")
		worker := control.MoverWorker{Snapshot: func() *directory.File { return dir.Snapshot().File() },
			CursorDir: filepath.Join(moverDir, "cursor"), LedgerDir: filepath.Join(moverDir, "ledger")}
		ctl = &control.Server{Dir: dir, Clusters: registry, Metrics: metrics, Log: log, SecretsDir: cfg.Directory.SecretsDir, Keys: store, Fleet: control.NoFleet{},
			Ops: &control.MemOperations{Dir: dir, OnChange: events.Fence}, Node: "lab", Events: events, Telemetry: telemetryStore, Ctx: ctx,
			Mover: worker.Run, MoverLedger: worker.Ledger}
		warnHeldSteps(log, dir.Snapshot())
		if ref := cfg.Admin.ControlTokenRef; ref != "" {
			if ctl.Token, err = config.ResolveSecret(ref); err != nil {
				return fmt.Errorf("admin.control_token_ref: %w", err)
			}
		} else if host, _, herr := net.SplitHostPort(cfg.Admin.Address); herr != nil || !isLoopbackHost(host) {
			log.Warn("the control API has no admin.control_token_ref and the admin listener is not loopback-only: it answers loopback peers only, so remote operators are refused", "admin", cfg.Admin.Address)
		}
		log.Info("resign mode", "credentials", store.Len(), "clusters", registry.Load().Names(), "directory", cfg.Directory.File,
			"directory_version", dir.Snapshot().Version(), "poll_interval", cfg.Directory.PollInterval.String(), "xml_rewrite", hcfg.Rewrite)
		if !hcfg.Rewrite {
			log.Warn("kill_switches.xml_rewrite_disable is set: responses carry backend bucket names, cluster endpoints, and backend uploadIds to clients (ADR-0006)")
		}
	default:
		cc := cfg.Clusters[cfg.Proxy.Cluster]
		cl, err := upstream.New(cfg.Proxy.Cluster, cc, upstream.Options{})
		if err != nil {
			return err
		}
		defer cl.Close()
		hcfg.Cluster = cl
		logCluster(log, "cluster ready", cl, cc, true)
	}
	h := proxy.New(hcfg, cfg.Proxy.CopyBufferBytes)

	ln, err := listener.Listen(cfg.Listener)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
		// No ReadTimeout/WriteTimeout: data ops are bounded by the idle-progress deadline the
		// handler sets per chunk (docs/DESIGN.md §2.8), metadata ops by their context deadline.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	adm := admin.New(metrics.Registry, slow)
	if ctl != nil {
		adm.Mount("/v1/", ctl.Handler())
	}
	retire := make(chan struct{}, 1)
	if mem != nil {
		adm.Mount("/-/fleet", mem)
		mem.OnRetire = func() {
			select {
			case retire <- struct{}{}:
			default:
			}
		}
		mctx, stop := context.WithCancel(ctx)
		defer stop()
		go mem.Run(mctx)
	}
	admSrv := adm.Listen(cfg.Admin.Address)

	log.Info("shunt serving", "version", version, "listen", cfg.Listener.Address, "admin", cfg.Admin.Address,
		"auth", cfg.Auth.Mode, "domains", cfg.Listener.Domains)

	errCh := make(chan error, 2)
	go func() { errCh <- srv.Serve(ln) }()
	go func() { errCh <- admSrv.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)
	hup := make(chan os.Signal, 1)
	var tick <-chan time.Time
	if dir != nil {
		signal.Notify(hup, syscall.SIGHUP)
		defer signal.Stop(hup)
		t := time.NewTicker(cfg.Directory.PollInterval)
		defer t.Stop()
		tick = t.C
	}

wait:
	for {
		select {
		case s := <-sig:
			log.Info("signal received, draining", "signal", s.String(), "timeout", cfg.Proxy.DrainTimeout.String())
			break wait
		case <-retire:
			log.Info("retirement requested by the control plane, draining", "timeout", cfg.Proxy.DrainTimeout.String())
			break wait
		case <-ctx.Done():
			log.Info("context done, draining")
			break wait
		case err := <-errCh:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			break wait
		case <-tick:
			reloadDirectory(dir, log, "poll")
		case <-hup:
			reloadDirectory(dir, log, "SIGHUP")
		}
	}

	// Drain: healthz → 503 first so an L4 tier stops sending, then stop accepting and wait for
	// in-flight requests up to the deadline.
	adm.Drain()
	dctx, cancel := context.WithTimeout(context.Background(), cfg.Proxy.DrainTimeout)
	defer cancel()
	cut := int64(0)
	if err := srv.Shutdown(dctx); err != nil {
		log.Warn("drain deadline hit; closing remaining connections", "err", err)
		_ = srv.Close()
		cut = h.Gates.Inflight() // requests cut short: their backend outcomes are never learned
	}
	if mem != nil {
		// A clean retirement (ADR-0021 D2): admission has stopped and every request has ended, so
		// the control plane can count this process out of every barrier. A count of outcomes
		// never learned makes it unclean, and an operator resolves it once the backend is quiet.
		uncertain := h.Gates.Uncertain() + cut
		rctx, rcancel := context.WithTimeout(context.Background(), cfg.Proxy.DrainTimeout)
		if err := mem.Retire(rctx, uncertain); err != nil {
			log.Warn("retirement not recorded by the control plane; the next process of this proxy reports it", "err", err.Error())
		}
		rcancel()
	}
	_ = admSrv.Shutdown(dctx)
	log.Info("stopped")
	return nil
}

// warnHeldSteps names every bucket with a held ramp step at startup: a control node that stopped
// between a hold and its completion leaves the held keys answering 503 until the step is repeated
// (ADR-0016).
func warnHeldSteps(log *slog.Logger, snap *directory.Snapshot) {
	f := snap.File()
	for _, key := range slices.Sorted(maps.Keys(f.Placements)) {
		if p := f.Placements[key]; p.Held() {
			log.Warn("a ramp step was left held by an interrupted call: writes to the keys it moves answer 503 until it is repeated",
				"placement", key, "ratio", p.Ramp.Ratio, "hold_ratio", p.Ramp.Hold.Ratio, "hold_prefixes", p.Ramp.Hold.Prefixes)
		}
	}
}

// startMember builds a fleet member's directory client and its cluster registry, loads the cached
// directory, and registers with the control plane before the proxy serves anything (ADR-0016).
func startMember(ctx context.Context, cfg *config.Config, metrics *telemetry.Metrics, log *slog.Logger) (*member.Client, *upstream.Registry, *runtimecfg.Publisher, *admission.Gates, error) {
	mcfg := member.Config{Endpoints: cfg.Control.Endpoints, ProxyID: cfg.Control.ProxyID, CacheDir: cfg.Control.CacheDir,
		Interval: cfg.Control.HeartbeatInterval, LeaseTTL: cfg.Control.LeaseTTL, Version: version}
	if host, err := os.Hostname(); err == nil {
		mcfg.Host = host
	}
	if ref := cfg.Control.TokenRef; ref != "" {
		tok, err := config.ResolveSecret(ref)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("control.token_ref: %w", err)
		}
		mcfg.Token = tok
	}
	if mcfg.ProxyID == "" {
		host, err := os.Hostname()
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("control.proxy_id is unset and the host name is unknown: %w", err)
		}
		_, port, _ := net.SplitHostPort(cfg.Admin.Address)
		mcfg.ProxyID = defaultProxyID(host, port)
	}
	if !config.ValidProxyID(mcfg.ProxyID) {
		return nil, nil, nil, nil, fmt.Errorf("proxy id %q (from the host name and admin port) is not a valid control.proxy_id: 1-64 letters, digits, '.', '_' or '-'; set control.proxy_id", mcfg.ProxyID)
	}
	m := member.New(mcfg, log)
	m.Metrics = metrics
	registry := upstream.NewRegistry(upstream.Options{}, m.Resolve)
	rewrite := !cfg.KillSwitches.XMLRewriteDisable
	m.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, err := registry.PrepareWith(f.Clusters, resolve, secretGeneration(f))
		if err != nil {
			return nil, err
		}
		return func() {
			before := registry.Load()
			added, removed := cand.Commit()
			for _, name := range added {
				cl, _ := registry.Load().Get(name)
				event := "cluster added"
				if _, existed := before.Get(name); existed {
					event = "cluster updated"
				}
				logCluster(log, event, cl, f.Clusters[name], rewrite)
			}
			for _, name := range removed {
				log.Info("cluster removed", "cluster", name)
			}
		}, nil
	}
	// Each installed version is published as one bundle: the member has committed its clusters and
	// swapped its keys before OnInstall, under its install lock, so bundles never go back (ADR-0021).
	rt := runtimecfg.NewPublisher(&runtimecfg.Bundle{Snapshot: m.Snapshot(), Keys: m.Keys().Table(), Clusters: registry.Load()})
	rt.Observe = observeBundles(metrics, log)
	// The admission gates follow the directory (ADR-0021 D2): closed on install of a version
	// carrying a barrier, opened once requests route by the version without it.
	keeper := &proxy.GateKeeper{Gates: admission.New()}
	m.Gates = keeper.Gates
	rt.Published = func(b *runtimecfg.Bundle) { keeper.Served(b.Snapshot) }
	keeper.Served(m.Snapshot())
	m.Serving = func() *directory.Snapshot { return rt.Load().Snapshot }
	m.OnInstall = func(s *directory.Snapshot) {
		if err := rt.Refresh(s, m.Keys().Table(), registry.Load()); err != nil {
			log.Error("runtime bundle not published", "err", err.Error())
		}
		keeper.Installed(s)
		publishRouteState(metrics, s)
		warnUnknownRampHashes(log, s)
	}
	metrics.FleetStale.Set(1) // until the first heartbeat is answered
	if err := m.Load(); err != nil {
		return nil, nil, nil, nil, err
	}
	log.Info("fleet member", "proxy", mcfg.ProxyID, "control", cfg.Control.Endpoints, "heartbeat", mcfg.Interval.String(), "lease_ttl", mcfg.LeaseTTL.String(),
		"cache_dir", cfg.Control.CacheDir, "control_channel", "PLAINTEXT http (control.plaintext: true; secrets and client keys cross the network in the clear)")
	// Join before serving: a proxy that serves first could route writes to a bucket whose first
	// step the control plane takes without waiting for it (ADR-0016).
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Register(rctx); err != nil {
		log.Warn("fleet member could not register with the control plane before serving: starting stale (writes on moving buckets are refused until it can)",
			"proxy", mcfg.ProxyID, "err", err.Error())
	}
	return m, registry, rt, keeper.Gates, nil
}

// observeBundles exports the runtime publisher's state and logs when installs start and stop
// waiting on the retired-bundle bound. It runs under the publisher's lock, so its state needs none.
func observeBundles(metrics *telemetry.Metrics, log *slog.Logger) func(runtimecfg.Stats) {
	waiting := false
	return func(st runtimecfg.Stats) {
		metrics.BundlesRetired.Set(float64(st.Retired))
		switch {
		case st.Pending != 0 && !waiting:
			metrics.InstallBackpressure.Set(1)
			log.Warn("install backpressure: long-running requests hold the maximum number of replaced runtime bundles; new requests keep using the older version until one finishes",
				"serving", st.Version, "pending", st.Pending, "retired", st.Retired)
		case st.Pending == 0 && waiting:
			metrics.InstallBackpressure.Set(0)
			log.Info("install backpressure cleared", "serving", st.Version)
		}
		waiting = st.Pending != 0
	}
}

// defaultProxyID is <host>-<port> with every character a proxy id cannot hold replaced by '-',
// and the host shortened to its first label if the whole would not fit in 64: a Kubernetes pod
// name can alone be 63 characters.
func defaultProxyID(host, port string) string {
	clean := func(s string) string {
		return strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' {
				return r
			}
			return '-'
		}, s)
	}
	host = strings.TrimLeft(clean(host), "._-")
	if len(host)+1+len(port) > 64 {
		host, _, _ = strings.Cut(host, ".")
	}
	if n := 64 - 1 - len(port); len(host) > n {
		host = host[:n]
	}
	return host + "-" + port
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// logCluster reports one cluster as it goes live, with the warnings its definition earns.
func logCluster(log *slog.Logger, event string, cl *upstream.Cluster, cc config.Cluster, rewrite bool) {
	log.Info(event, "cluster", cl.Name, "type", cl.Type, "scheme", cl.Scheme, "endpoints", cl.Endpoints, "region", cl.Region,
		"access_key", cc.Credentials.AccessKey, "secret_ref", cc.Credentials.SecretRef, "id", cl.ID,
		"enforces_sha256", cl.EnforcesSHA256, "unsigned_trailer", cl.UnsignedTrailer)
	if cc.TLS.InsecureSkipVerify {
		log.Warn("upstream TLS certificate verification is DISABLED (tls.insecure_skip_verify); temporary until the backend has a valid certificate", "cluster", cl.Name)
	}
	if cl.Scheme == "http" {
		log.Warn("upstream scheme is http: requests to this cluster are unencrypted (per-site decision, docs/DESIGN.md §2.9)", "cluster", cl.Name)
	}
	if evidence, leaks := locationLeaks[cl.Type]; leaks && !rewrite {
		log.Warn("CompleteMultipartUpload <Location> will expose the upstream endpoint to clients while kill_switches.xml_rewrite_disable is set (docs/reference/backend-compat.md)",
			"cluster", cl.Name, "type", cl.Type, "endpoints", cl.Endpoints, "evidence", evidence)
	}
}

// publishRouteState exports one gauge per placement that is mid-migration, and drops the series
// of every placement that has returned to ACTIVE (docs/telemetry-catalog.md: the bucket label is
// bounded by the number of migrations in flight).
func publishRouteState(m *telemetry.Metrics, snap *directory.Snapshot) {
	m.RouteState.Reset()
	m.RampRatio.Reset()
	f := snap.File()
	for key := range f.Placements {
		p := f.Placements[key]
		if p.State == directory.StateActive {
			continue
		}
		for _, state := range directory.States {
			v := 0.0
			if state == p.State {
				v = 1
			}
			m.RouteState.WithLabelValues(key, state).Set(v)
		}
		if p.State == directory.StateRamping && p.Ramp != nil {
			m.RampRatio.WithLabelValues(key).Set(p.Ramp.Ratio)
		}
	}
}

func reloadDirectory(d *directory.FileDir, log *slog.Logger, trigger string) {
	changed, err := d.Reload()
	switch {
	case err != nil:
		log.Error("directory reload rejected; serving the last good version", "trigger", trigger, "version", d.Snapshot().Version(), "err", err.Error())
	case changed:
		log.Info("directory reloaded", "trigger", trigger, "version", d.Snapshot().Version()) // OnInstall republished the route gauges
	case trigger == "SIGHUP":
		log.Info("directory unchanged", "trigger", trigger, "version", d.Snapshot().Version())
	}
}

// warnUnknownRampHashes names every ramp split by a hash this build does not implement. Its
// hash-decided requests are refused with 503 until it moves on with a build that does (ADR-0004).
func warnUnknownRampHashes(log *slog.Logger, s *directory.Snapshot) {
	f := s.File()
	for _, key := range slices.Sorted(maps.Keys(f.Placements)) {
		if r := f.Placements[key].Ramp; r != nil && r.Hash != directory.RampHash && r.Ratio > 0 && r.Ratio < 1 {
			log.Error("placement's ramp names a hash this build does not implement: requests its hash would route are refused with 503 rather than re-split; move the placement on with the build that started the ramp",
				"placement", key, "ramp_hash", r.Hash, "implemented", directory.RampHash)
		}
	}
}

// newLogger builds serve's logger in the configured format. With a plaintext client listener every
// line says so: JSON lines carry client_listener="PLAINTEXT http (…)", console lines a [PLAINTEXT] tag.
func newLogger(cfg *config.Config, stderr io.Writer) *slog.Logger {
	format := cfg.Telemetry.LogFormat
	if format == "auto" || format == "" {
		format = "json"
		if f, ok := stderr.(*os.File); ok {
			if st, err := f.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
				format = "console"
			}
		}
	}
	if format == "console" {
		o := telemetry.ConsoleOptions{}
		if cfg.Listener.Plaintext {
			o.Tag = "PLAINTEXT"
		}
		return slog.New(telemetry.NewConsoleHandler(stderr, o))
	}
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	if cfg.Listener.Plaintext {
		log = log.With("client_listener", "PLAINTEXT http (listener.plaintext: true; lab use only)")
	}
	return log
}

// secretGeneration reads each cluster's secret generation from a directory version, for the
// signers the registry builds from it (ADR-0021 D1).
func secretGeneration(f *directory.File) func(string) int64 {
	return func(name string) int64 { return f.Generation(directory.SecretResource(name)) }
}
