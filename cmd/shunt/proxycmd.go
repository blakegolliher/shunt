package main

import (
	"fmt"
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
	cmd.AddCommand(newProxyList(), newProxyForget())
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
				state, seen := "live", ""
				if !m.Live {
					state = "SILENT"
				}
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

func newProxyForget() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "forget <proxy-id>",
		Short: "Remove a member that is gone for good, so a bucket's first step stops waiting for it",
		Long: "A bucket's first migration step waits for every member, live or not: a proxy cut off before it\n" +
			"would keep writing every key to the source. When a member is gone for good (decommissioned,\n" +
			"its host lost), forget it. A live member cannot be forgotten, and one that comes back re-joins\n" +
			"on its next heartbeat.",
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
