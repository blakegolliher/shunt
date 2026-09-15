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
	"github.com/blakegolliher/shunt/internal/listener"
	"github.com/blakegolliher/shunt/internal/proxy"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
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
// request was sent to. In resign mode that host is the backend endpoint, not the client-facing name,
// so the backend address reaches the client. POC-3 rewrites every XML echo and removes this table.
var locationLeaks = map[string]string{
	"minio": "verified: s3diff resign run 2026-09-15 returned <Location>http://127.0.0.1/…",
	"aws":   "AWS builds <Location> from the endpoint host it was addressed by",
	"vast":  "unverified: s3diff cannot see it because its direct and via requests share the VAST endpoint host; assume it leaks",
	"s3":    "unverified for generic S3 backends; assume it leaks (Garage builds <Location> from its root_domain and does not)",
}

// serve runs the proxy and admin servers until SIGTERM/SIGINT or ctx cancellation, then drains.
func serve(ctx context.Context, cfg *config.Config, stderr io.Writer) error {
	log := slog.New(slog.NewJSONHandler(stderr, nil))

	cl, err := upstream.New(cfg.Proxy.Cluster, cfg.Clusters[cfg.Proxy.Cluster], upstream.Options{})
	if err != nil {
		return err
	}
	defer cl.Close()

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
		Cluster: cl, Domains: s3.NewDomains(cfg.Listener.Domains), Metrics: metrics, Access: access, Slow: slow,
		IdleTimeout: cfg.Proxy.IdleTimeout, MetadataTimeout: cfg.Proxy.MetadataTimeout,
		Via: "1.1 shunt/" + version, Log: log,
	}
	if cfg.Auth.Mode == "resign" {
		store, lerr := auth.Load(cfg.Auth.CredentialsFile)
		if lerr != nil {
			return lerr
		}
		ccfg := cfg.Clusters[cfg.Proxy.Cluster]
		secret, serr := config.ResolveSecret(ccfg.Credentials.SecretRef)
		if serr != nil {
			return fmt.Errorf("cluster %s: %w", cfg.Proxy.Cluster, serr)
		}
		hcfg.Mode = proxy.ModeResign
		hcfg.Store = store
		hcfg.ClusterCreds = sigv4.Credentials{AccessKey: ccfg.Credentials.AccessKey, Secret: secret}
		hcfg.Capabilities = proxy.Capabilities{EnforcesSHA256: ccfg.Capabilities.EnforcesSHA256Or(true), UnsignedTrailer: ccfg.Capabilities.UnsignedTrailerOr(true)}
		hcfg.ClockSkew = cfg.Auth.ClockSkew
		log.Info("resign mode", "credentials", store.Len(), "cluster_access_key", ccfg.Credentials.AccessKey,
			"enforces_sha256", hcfg.Capabilities.EnforcesSHA256, "unsigned_trailer", hcfg.Capabilities.UnsignedTrailer)
		if evidence, leaks := locationLeaks[ccfg.Type]; leaks {
			log.Warn("resign mode: CompleteMultipartUpload <Location> will expose the upstream endpoint to clients until POC-3 rewrites XML echoes (docs/reference/backend-compat.md)",
				"cluster", cfg.Proxy.Cluster, "type", ccfg.Type, "endpoints", ccfg.Endpoints, "evidence", evidence)
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

	log.Info("shunt serving",
		"version", version, "listen", cfg.Listener.Address, "admin", cfg.Admin.Address,
		"auth", cfg.Auth.Mode, "cluster", cl.Name, "cluster_type", cl.Type,
		"scheme", cl.Scheme, "endpoints", cl.Endpoints, "domains", cfg.Listener.Domains)
	if cc := cfg.Clusters[cfg.Proxy.Cluster]; cc.TLS.InsecureSkipVerify {
		log.Warn("upstream TLS certificate verification is DISABLED (tls.insecure_skip_verify); temporary until the backend has a valid certificate", "cluster", cl.Name)
	}
	if cl.Scheme == "http" {
		log.Warn("upstream scheme is http: bytes to the backend are plaintext (per-site decision, docs/DESIGN.md §2.9)", "cluster", cl.Name)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- srv.Serve(ln) }()
	go func() { errCh <- admSrv.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)

	select {
	case s := <-sig:
		log.Info("signal received, draining", "signal", s.String(), "timeout", cfg.Proxy.DrainTimeout.String())
	case <-ctx.Done():
		log.Info("context done, draining")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
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
