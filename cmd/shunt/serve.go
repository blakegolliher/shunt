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
	"slices"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/admin"
	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/listener"
	"github.com/blakegolliher/shunt/internal/proxy"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

func newServe() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the proxy",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}
			return serve(cmd.Context(), cfg, cmd.ErrOrStderr())
		},
	}
	cmd.Flags().StringVarP(&cfgPath, "config", "c", "/etc/shunt/shunt.yaml", "config file")
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
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	if cfg.Listener.Plaintext {
		// Every line serve logs from here on says so, not just one warning an operator can miss.
		log = log.With("client_listener", "PLAINTEXT http (listener.plaintext: true; lab use only)")
		log.Warn("the client listener is PLAINTEXT http: signatures, credentials in presigned URLs, and object bytes cross the network unencrypted; listener.plaintext is for labs")
	}

	metrics := telemetry.NewMetrics()
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
		Domains: s3.NewDomains(cfg.Listener.Domains), Metrics: metrics, Access: access, Slow: slow,
		IdleTimeout: cfg.Proxy.IdleTimeout, MetadataTimeout: cfg.Proxy.MetadataTimeout,
		Via: "1.1 shunt/" + version, Log: log,
	}
	var dir *directory.FileDir
	var ctl *control.Server
	if cfg.Auth.Mode == "resign" {
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
		applyClusters := func(f *directory.File) error {
			added, removed, aerr := registry.Apply(f.Clusters)
			if aerr != nil {
				log.Error("directory version refused: a cluster could not be built", "version", f.Version, "err", aerr.Error())
				return aerr
			}
			for _, name := range added {
				cl, _ := registry.Load().Get(name)
				logCluster(log, cl, f.Clusters[name], rewrite)
			}
			for _, name := range removed {
				log.Info("cluster removed", "cluster", name)
			}
			return nil
		}
		if applyErr := applyClusters(dir.Snapshot().File()); applyErr != nil {
			return applyErr
		}
		dir.Prepare = applyClusters
		dir.OnInstall = func(s *directory.Snapshot) {
			publishRouteState(metrics, s)
			warnUnknownRampHashes(log, s)
		}
		warnUnknownRampHashes(log, dir.Snapshot())
		hcfg.Mode, hcfg.Store, hcfg.Clusters, hcfg.Dir = proxy.ModeResign, store, registry, dir
		hcfg.Rewrite, hcfg.ClockSkew, hcfg.DebugRoute = rewrite, cfg.Auth.ClockSkew, cfg.Features.DebugRouteHeader
		if hcfg.DebugRoute {
			log.Warn("features.debug_route_header is on: any client sending X-Shunt-Debug: 1 learns which cluster served it (ADR-0006 amendment); for labs")
		}
		publishRouteState(metrics, dir.Snapshot())
		ctl = &control.Server{Dir: dir, Clusters: registry, Metrics: metrics, Log: log}
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
	} else {
		cc := cfg.Clusters[cfg.Proxy.Cluster]
		cl, err := upstream.New(cfg.Proxy.Cluster, cc, upstream.Options{})
		if err != nil {
			return err
		}
		defer cl.Close()
		hcfg.Cluster = cl
		logCluster(log, cl, cc, true)
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
	if err := srv.Shutdown(dctx); err != nil {
		log.Warn("drain deadline hit; closing remaining connections", "err", err)
		_ = srv.Close()
	}
	_ = admSrv.Shutdown(dctx)
	log.Info("stopped")
	return nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// logCluster reports one cluster as it goes live, with the warnings its definition earns.
func logCluster(log *slog.Logger, cl *upstream.Cluster, cc config.Cluster, rewrite bool) {
	log.Info("cluster", "cluster", cl.Name, "type", cl.Type, "scheme", cl.Scheme, "region", cl.Region, "endpoints", cl.Endpoints,
		"id", cl.ID, "access_key", cc.Credentials.AccessKey, "enforces_sha256", cl.EnforcesSHA256, "unsigned_trailer", cl.UnsignedTrailer)
	if cc.TLS.InsecureSkipVerify {
		log.Warn("upstream TLS certificate verification is DISABLED (tls.insecure_skip_verify); temporary until the backend has a valid certificate", "cluster", cl.Name)
	}
	if cl.Scheme == "http" {
		log.Warn("upstream scheme is http: bytes to the backend are plaintext (per-site decision, docs/DESIGN.md §2.9)", "cluster", cl.Name)
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
