package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/admin"
	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
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
// request was sent to. In resign mode that host is the backend endpoint, so with features.xml_rewrite
// off the backend address reaches the client. With the rewriter on (the default) nothing leaks.
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
	var clusters []*upstream.Cluster
	var dir *directory.FileDir
	if cfg.Auth.Mode == "resign" {
		store, err := auth.Load(cfg.Auth.CredentialsFile)
		if err != nil {
			return err
		}
		set, err := upstream.NewSet(cfg.Clusters, upstream.Options{}, config.ResolveSecret)
		if err != nil {
			return err
		}
		defer set.Close()
		dir, err = directory.Open(cfg.Directory.File, cfg.Clusters)
		if err != nil {
			return err
		}
		dir.ChangeLogError = func(err error) {
			log.Error("directory change log append failed; the placement change itself is committed", "change_log", dir.ChangeLogPath(), "err", err.Error())
		}
		hcfg.Mode, hcfg.Store, hcfg.Clusters, hcfg.Dir = proxy.ModeResign, store, set, dir
		hcfg.Rewrite, hcfg.ClockSkew = cfg.Features.XMLRewriteOn(), cfg.Auth.ClockSkew
		for _, name := range set.Names() {
			cl, _ := set.Get(name)
			clusters = append(clusters, cl)
		}
		log.Info("resign mode", "credentials", store.Len(), "clusters", set.Names(), "directory", cfg.Directory.File,
			"directory_version", dir.Snapshot().Version(), "poll_interval", cfg.Directory.PollInterval.String(), "xml_rewrite", hcfg.Rewrite)
		if !hcfg.Rewrite {
			log.Warn("features.xml_rewrite is off: responses carry backend bucket names, cluster endpoints, and backend uploadIds to clients (ADR-0006)")
			for _, cl := range clusters {
				if evidence, leaks := locationLeaks[cl.Type]; leaks {
					log.Warn("CompleteMultipartUpload <Location> will expose the upstream endpoint to clients while features.xml_rewrite is off (docs/reference/backend-compat.md)",
						"cluster", cl.Name, "type", cl.Type, "endpoints", cl.Endpoints, "evidence", evidence)
				}
			}
		}
	} else {
		cl, err := upstream.New(cfg.Proxy.Cluster, cfg.Clusters[cfg.Proxy.Cluster], upstream.Options{})
		if err != nil {
			return err
		}
		defer cl.Close()
		hcfg.Cluster = cl
		clusters = []*upstream.Cluster{cl}
	}
	for _, cl := range clusters {
		cc := cfg.Clusters[cl.Name]
		log.Info("cluster", "cluster", cl.Name, "type", cl.Type, "scheme", cl.Scheme, "region", cl.Region, "endpoints", cl.Endpoints,
			"id", cl.ID, "access_key", cc.Credentials.AccessKey, "enforces_sha256", cl.EnforcesSHA256, "unsigned_trailer", cl.UnsignedTrailer)
		if cc.TLS.InsecureSkipVerify {
			log.Warn("upstream TLS certificate verification is DISABLED (tls.insecure_skip_verify); temporary until the backend has a valid certificate", "cluster", cl.Name)
		}
		if cl.Scheme == "http" {
			log.Warn("upstream scheme is http: bytes to the backend are plaintext (per-site decision, docs/DESIGN.md §2.9)", "cluster", cl.Name)
		}
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

func reloadDirectory(d *directory.FileDir, log *slog.Logger, trigger string) {
	changed, err := d.Reload()
	switch {
	case err != nil:
		log.Error("directory reload rejected; serving the last good version", "trigger", trigger, "version", d.Snapshot().Version(), "err", err.Error())
	case changed:
		log.Info("directory reloaded", "trigger", trigger, "version", d.Snapshot().Version())
	case trigger == "SIGHUP":
		log.Info("directory unchanged", "trigger", trigger, "version", d.Snapshot().Version())
	}
}
