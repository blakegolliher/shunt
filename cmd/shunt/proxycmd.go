package main

import (
	"context"
	"fmt"
	"net/url"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/control"
)

// newProxy is the fleet verb (ADR-0016): the member proxies the control node knows, and forgetting
// one that is gone for good.
func newProxy() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "proxy",
		Short: "The proxies in this shunt's fleet: list them, forget one that is gone",
		Long: "Several shunt proxies can serve one directory. One is the control node (the one these commands\n" +
			"talk to); the others are members that name it in control.endpoint and send it a heartbeat. A\n" +
			"routing change is in effect only once every member has installed it (ADR-0016).",
	}
	cmd.AddCommand(newProxyList(), newProxyShow(), newProxyRetire(), newProxyResolve(), newProxyForget())
	return cmd
}

func newProxyList() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List member proxies, the directory version each has installed, and whether it is live",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var fl control.FleetStatus
			if err := api.call(cmd.Context(), "GET", "/v1/fleet", nil, &fl); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, fl)
			}
			out := cmd.OutOrStdout()
			if len(fl.Members) == 0 {
				_, _ = fmt.Fprintf(out, "no member proxies: this shunt is the only one (directory version %d)\n", fl.Version)
				return nil
			}
			_, _ = fmt.Fprintf(out, "directory version %d\n\n", fl.Version)
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "PROXY\tSTATE\tAPPLIED\tLAST HEARTBEAT")
			for _, m := range fl.Members {
				state, seen := memberState(m), ""
				if !m.Seen.IsZero() {
					seen = m.SinceSeen.Round(time.Second).String() + " ago" // the control node's clock, not this host's
				} else {
					seen = "none since the control node started"
				}
				applied := fmt.Sprint(m.Applied)
				switch {
				case m.Identity != fl.Identity:
					applied += " (other lineage)"
				case m.Applied < fl.Version:
					applied += " (behind)"
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", m.ID, state, applied, seen)
			}
			return tw.Flush()
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}

// memberState is one word for where a member stands.
func memberState(m control.Member) string {
	switch {
	case len(m.Unresolved) > 0:
		return fmt.Sprintf("UNRESOLVED(%d)", len(m.Unresolved))
	case m.Incarnation != nil && m.Incarnation.State == control.IncarnationRetired:
		return "retired"
	case !m.Live:
		return "SILENT"
	case m.RetireRequested:
		return "retiring"
	}
	return "live"
}

func newProxyShow() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "show <proxy-id>",
		Short: "One proxy's install state: the version its requests use, what it installed, its cache and secrets",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var d control.ProxyDiagnostics
			if err := api.call(cmd.Context(), "GET", "/v1/fleet/"+url.PathEscape(args[0]), nil, &d); err != nil {
				return fmt.Errorf("proxy %s: %w", args[0], err)
			}
			if o.json {
				return printJSON(cmd, d)
			}
			out := cmd.OutOrStdout()
			installed := d.Applied
			if d.Installed > installed {
				installed = d.Installed
			}
			_, _ = fmt.Fprintf(out, "proxy %s (%s, %s)\n", d.ID, memberState(d.Member), d.Host)
			_, _ = fmt.Fprintf(out, "  directory %d; requests use %d; installed %d; restart cache durable at %d\n", d.Directory, d.Applied, installed, d.Durable)
			if l := d.Lease; l.Seq > 0 {
				_, _ = fmt.Fprintf(out, "  lease: grants %s per heartbeat; last heartbeat %d seen %s ago (control node's clock); %s\n", l.Granted, l.Seq, l.Age.Round(time.Second), map[bool]string{true: "live", false: "expired"}[l.Live])
			}
			if inc := d.Incarnation; inc != nil {
				_, _ = fmt.Fprintf(out, "  incarnation %s: %s since %s", inc.ID, inc.State, inc.Started.Format(time.RFC3339))
				if !inc.Ended.IsZero() {
					_, _ = fmt.Fprintf(out, ", ended %s", inc.Ended.Format(time.RFC3339))
				}
				_, _ = fmt.Fprintf(out, "; %d backend outcomes unknown\n", d.Uncertain)
			}
			for _, inc := range d.Unresolved {
				_, _ = fmt.Fprintf(out, "  unresolved incarnation %s: %s, ended %s, %d outcomes unknown\n", inc.ID, inc.State, inc.Ended.Format(time.RFC3339), inc.Uncertain)
			}
			for _, sec := range d.Secrets {
				_, _ = fmt.Fprintf(out, "  cluster %s: secret generation %s, proxy signs with %s\n", sec.Cluster, sec.Want, orNone(sec.Have))
			}
			if len(d.Problems) == 0 {
				_, _ = fmt.Fprintln(out, "  nothing is off")
			}
			for _, p := range d.Problems {
				_, _ = fmt.Fprintf(out, "  ! %s\n", p)
			}
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}

func orNone(s string) string {
	if s == "" {
		return "none reported"
	}
	return s
}

func newProxyRetire() *cobra.Command {
	var o apiOptions
	var wait time.Duration
	cmd := &cobra.Command{
		Use:   "retire <proxy-id>",
		Short: "Ask a proxy to retire: stop admitting requests, drain, record its retirement and stop",
		Long: "The proxy hears the request in its next heartbeat answer, stops accepting connections, waits for\n" +
			"every request in flight to end, records its retirement with the control plane and exits (ADR-0021 D2).\n" +
			"A retired proxy counts out of every barrier. Stopping the service (SIGTERM) does the same; this is\n" +
			"the same retirement started from the control plane. With --wait, waits until the retirement is recorded.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var res control.MemberResult
			if err := api.call(cmd.Context(), "POST", "/v1/fleet/"+url.PathEscape(args[0])+"/retire", nil, &res); err != nil {
				return fmt.Errorf("proxy %s: %w", args[0], err)
			}
			out := cmd.OutOrStdout()
			if wait <= 0 {
				if o.json {
					return printJSON(cmd, res)
				}
				_, _ = fmt.Fprintf(out, "proxy %s: retirement requested; it drains and stops at its next heartbeat (`shunt proxy show %s` follows it)\n", args[0], args[0])
				return nil
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), wait)
			defer cancel()
			var d control.ProxyDiagnostics
			for {
				if err := api.call(ctx, "GET", "/v1/fleet/"+url.PathEscape(args[0]), nil, &d); err != nil {
					return fmt.Errorf("proxy %s: %w", args[0], err)
				}
				if inc := d.Incarnation; inc == nil || inc.State != control.IncarnationActive {
					break
				}
				select {
				case <-ctx.Done():
					if o.json {
						_ = printJSON(cmd, d)
					}
					return &exitError{code: exitWaitDeadline, err: fmt.Errorf("proxy %s has not retired after %s; it retires at its next heartbeat: `shunt proxy show %s`", args[0], wait, args[0])}
				case <-time.After(500 * time.Millisecond):
				}
			}
			if o.json {
				return printJSON(cmd, d)
			}
			switch {
			case d.Incarnation != nil && d.Incarnation.State == control.IncarnationRetired:
				_, _ = fmt.Fprintf(out, "proxy %s retired cleanly (incarnation %s); it counts out of every barrier. `shunt proxy forget %s` removes it from the fleet\n", args[0], d.Incarnation.ID, args[0])
			default:
				_, _ = fmt.Fprintf(out, "proxy %s retired with backend outcomes unknown: resolve its incarnation once its backend work has ended (`shunt proxy show %s`)\n", args[0], args[0])
			}
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().DurationVar(&wait, "wait", 0, "wait this long for the retirement to be recorded (0: return once it is requested)")
	return cmd
}

func newProxyResolve() *cobra.Command {
	var o apiOptions
	var incarnation, attest string
	cmd := &cobra.Command{
		Use:   "resolve <proxy-id> --incarnation <id> --attest <why>",
		Short: "Record that an unretired incarnation's backend work has ended, so barriers stop waiting on it",
		Long: "A process that ended without retiring (a crash, a kill, a host lost) may still have work landing on\n" +
			"a backend, so every barrier waits on it and forget refuses. shunt cannot tell when that work has\n" +
			"ended; an operator can, by establishing that the process is stopped and the backend has no request\n" +
			"of it in flight. --attest records how; it is kept on the incarnation and logged with your actor.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var res control.MemberResult
			req := control.ResolveRequest{Incarnation: incarnation, Attestation: attest}
			if err := api.call(cmd.Context(), "POST", "/v1/fleet/"+url.PathEscape(args[0])+"/resolve", req, &res); err != nil {
				return fmt.Errorf("proxy %s: %w", args[0], err)
			}
			if o.json {
				return printJSON(cmd, res)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "proxy %s: incarnation %s resolved; %d unresolved left\n", args[0], incarnation, len(res.Member.Unresolved))
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().StringVar(&incarnation, "incarnation", "", "the incarnation id, from shunt proxy show (required)")
	cmd.Flags().StringVar(&attest, "attest", "", "how it was established that the process is stopped and its backend work has ended (required)")
	_ = cmd.MarkFlagRequired("incarnation")
	_ = cmd.MarkFlagRequired("attest")
	return cmd
}

func newProxyForget() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "forget <proxy-id>",
		Short: "Remove a member that is gone for good, so a bucket's first step stops waiting for it",
		Long: "A bucket's first migration step waits for every member, live or not: a proxy cut off before it\n" +
			"would keep writing every key to the source. When a member is gone for good (decommissioned,\n" +
			"its host lost), forget it. A live member cannot be forgotten, and nor can one whose last process\n" +
			"did not retire cleanly: resolve its incarnation first (shunt proxy resolve). One that comes back\n" +
			"re-joins on its next heartbeat.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			if err := api.call(cmd.Context(), "DELETE", "/v1/fleet/"+args[0], nil, nil); err != nil {
				return fmt.Errorf("proxy %s: %w", args[0], err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "proxy %s forgotten; it is no longer waited for\n", args[0])
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}
