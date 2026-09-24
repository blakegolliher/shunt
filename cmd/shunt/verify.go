package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/verify"
)

func newVerify() *cobra.Command {
	var (
		c                               verify.Client
		o                               verify.Options
		accessKey, secretRef, jsonOut   string
		telemetryURL, telemetryTokenRef string
		telemetryTolerance              float64
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Read, write and delete through an endpoint, checking every answer against a model",
		Long: "Runs a random-key workload of writes, deletes and reads against --endpoint/--bucket and holds every\n" +
			"answer to an in-memory model of what it did: a write must read back with its bytes, a delete must stay\n" +
			"deleted (a mover may put one back for up to --grace), every request must succeed. With --debug-route it\n" +
			"asks shunt (features.debug_route_header) which side served each request and reports the write split.\n" +
			"It runs until --duration, SIGINT or SIGTERM, prints the report, and exits non-zero on any error.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if c.Endpoint == "" || c.Bucket == "" || accessKey == "" || secretRef == "" {
				return fmt.Errorf("--endpoint, --bucket, --access-key and --secret-ref are required")
			}
			secret, err := config.ResolveSecret(secretRef)
			if err != nil {
				return fmt.Errorf("--secret-ref: %w", err)
			}
			c.Creds = sigv4.Credentials{AccessKey: accessKey, Secret: secret}
			c.HTTP = &http.Client{Timeout: 60 * time.Second}
			o.Progress = cmd.OutOrStdout()
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "verify: %s/%s, %d workers over %d keys\n", c.Endpoint, c.Bucket, o.Workers, o.Keys)
			rep := verify.Run(ctx, &c, o)
			if telemetryURL != "" {
				telemetryToken := ""
				if telemetryTokenRef != "" {
					telemetryToken, err = config.ResolveSecret(telemetryTokenRef)
					if err != nil {
						return fmt.Errorf("--telemetry-token-ref: %w", err)
					}
				}
				rep.Telemetry, err = verify.CompareTelemetry(ctx, telemetryURL, telemetryToken, rep.LatencyWindow, telemetryTolerance)
				if err != nil {
					return fmt.Errorf("verify telemetry cross-check: %w", err)
				}
			}
			if jsonOut != "" {
				b, _ := json.MarshalIndent(rep, "", "  ")                             //nolint:errcheck // plain data
				if err := os.WriteFile(jsonOut, append(b, '\n'), 0o644); err != nil { //nolint:gosec // a report, no secrets
					return err
				}
			}
			printVerify(cmd, rep)
			if rep.Errors > 0 {
				return fmt.Errorf("verify: %d errors in %d operations", rep.Errors, rep.Ops)
			}
			if rep.Telemetry != nil && !rep.Telemetry.WithinTolerance {
				return fmt.Errorf("verify telemetry cross-check: fleet p99 differs from the client by %.1f%% (limit %.1f%%)", rep.Telemetry.DifferencePct, rep.Telemetry.TolerancePct)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&c.Endpoint, "endpoint", "", "the S3 endpoint, e.g. http://shunt:8008")
	f.StringVar(&c.Bucket, "bucket", "", "the bucket to work in")
	f.StringVar(&c.Region, "region", "us-east-1", "the region to sign for")
	f.StringVar(&accessKey, "access-key", "", "the client's access key")
	f.StringVar(&secretRef, "secret-ref", "", "env:NAME or file:/path holding the client's secret")
	f.BoolVar(&c.DebugRoute, "debug-route", false, "ask shunt which side served each request (needs features.debug_route_header)")
	f.IntVar(&o.Workers, "workers", 8, "concurrent clients")
	f.IntVar(&o.Keys, "keys", 200, "distinct keys, all under --prefix")
	f.StringVar(&o.Prefix, "prefix", "", "key prefix (default verify/<start time>/)")
	f.DurationVar(&o.Duration, "duration", 0, "stop after this long (default: until SIGINT or SIGTERM)")
	f.DurationVar(&o.Grace, "grace", 5*time.Second, "how long a deleted key may be visible again while a mover withdraws its copy")
	f.Int64Var(&o.Seed, "seed", 0, "workload seed (default: from the clock; always reported)")
	f.DurationVar(&o.Interval, "interval", 10*time.Second, "progress line interval; 0 for none")
	f.BoolVar(&o.Cleanup, "cleanup", false, "delete the keys still present when the run ends")
	f.BoolVar(&o.NoRetry, "no-retry", false, "count every 503 as an error; by default a 503 with Retry-After is retried with backoff, as every S3 SDK does")
	f.StringVar(&jsonOut, "json-out", "", "also write the report as JSON to this file")
	f.StringVar(&telemetryURL, "telemetry-url", "", "control-node base URL whose fleet client_total p99 must match this client")
	f.StringVar(&telemetryTokenRef, "telemetry-token-ref", "", "env:NAME or file:/path holding the control API bearer token")
	f.Float64Var(&telemetryTolerance, "telemetry-tolerance", 10, "maximum percent difference between client and fleet p99")
	return cmd
}

func printVerify(cmd *cobra.Command, rep verify.Report) {
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "\nverify report: %s/%s prefix %s seed %d\n", rep.Endpoint, rep.Bucket, rep.Prefix, rep.Seed)
	_, _ = fmt.Fprintf(out, "  %d operations in %.0fs: %d PUT, %d GET, %d DELETE\n", rep.Ops, rep.Seconds, rep.Puts, rep.Gets, rep.Deletes)
	_, _ = fmt.Fprintf(out, "  keys present at the end: %d, %d read back after the workload stopped\n", rep.Present, rep.ReadBack)
	_, _ = fmt.Fprintf(out, "  errors: %d\n", rep.Errors)
	if rep.Retried > 0 {
		_, _ = fmt.Fprintf(out, "  503s retried as an S3 SDK would (Retry-After, held ramp steps): %d\n", rep.Retried)
	}
	_, _ = fmt.Fprintf(out, "  client latency: p50 %d us, p99 %d us\n", rep.LatencyP50US, rep.LatencyP99US)
	for _, s := range rep.ErrorSamples {
		_, _ = fmt.Fprintf(out, "    %s\n", s)
	}
	if rep.Resurrections > 0 {
		_, _ = fmt.Fprintf(out, "  deleted keys briefly visible again and withdrawn within the grace: %d\n", rep.Resurrections)
	}
	_, _ = fmt.Fprintf(out, "  writes by side: %s\n  reads by side: %s\n", verify.Share(rep.WriteSides), verify.Share(rep.ReadSides))
	for _, t := range []struct {
		what   string
		routes map[string]int64
	}{{"writes", rep.Writes}, {"reads", rep.Reads}} {
		if len(t.routes) > 0 {
			_, _ = fmt.Fprintf(out, "  %s by route: %s\n", t.what, verify.Share(t.routes))
		}
	}
	if rep.Telemetry != nil {
		_, _ = fmt.Fprintf(out, "  telemetry cross-check: client p99 %d us, fleet p99 %d us, difference %.1f%% (limit %.1f%%)\n",
			rep.Telemetry.ClientP99US, rep.Telemetry.FleetP99US, rep.Telemetry.DifferencePct, rep.Telemetry.TolerancePct)
	}
}
