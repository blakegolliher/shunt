package main

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// errNotReady is step-out's non-zero exit: something still stands between the clients and the cluster.
var errNotReady = errors.New("not ready to step out; nothing was changed")

func newStepOut() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "step-out [tenant]",
		Short: "Check that clients could use their cluster directly, with shunt out of the path",
		Long: "Checks, without changing anything, what stands between a tenant's clients and using their cluster\n" +
			"directly: every bucket ACTIVE on one cluster under the name clients use, no multipart uploads in\n" +
			"progress, and every client key accepted by that cluster and able to reach every bucket. When nothing\n" +
			"does, it prints how to take shunt out of the path. Exits non-zero while anything blocks (ADR-0011).",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tenant := directory.DefaultTenant
			if len(args) == 1 {
				tenant = args[0]
			}
			api, err := o.client()
			if err != nil {
				return err
			}
			var out control.StepOut
			if err := api.call(cmd.Context(), "GET", "/v1/tenants/"+url.PathEscape(tenant)+"/step-out", nil, &out); err != nil {
				return err
			}
			if o.json {
				if err := printJSON(cmd, out); err != nil {
					return err
				}
			} else {
				printStepOut(cmd.OutOrStdout(), out)
			}
			if !out.Ready {
				return errNotReady
			}
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}

func printStepOut(w io.Writer, out control.StepOut) {
	who := "tenant " + out.Tenant
	if out.Tenant == directory.DefaultTenant {
		who = "your clients"
	}
	if out.Cluster != "" {
		_, _ = fmt.Fprintf(w, "step-out check for %s: every bucket is on %s (%s://%s)\n", who, out.Cluster, out.Scheme, strings.Join(out.Endpoints, ", "))
	} else {
		_, _ = fmt.Fprintf(w, "step-out check for %s\n", who)
	}
	problems := 0
	line := func(mark, text string) { _, _ = fmt.Fprintf(w, "  %-7s %s\n", mark, shownText(text)) }
	for _, p := range out.Problems {
		problems++
		line("BLOCKED", p)
	}
	for _, b := range out.Buckets {
		if len(b.Problems) == 0 {
			line("ok", fmt.Sprintf("bucket %s: ACTIVE on %s under the same name, no uploads in progress", b.Bucket, b.Cluster))
		}
		for _, p := range b.Problems {
			problems++
			line("BLOCKED", p)
		}
		for _, n := range b.Notes {
			line("note", n)
		}
	}
	for _, k := range out.Keys {
		if len(k.Problems) == 0 {
			line("ok", fmt.Sprintf("client key %s: %s accepts it and it reaches every bucket", k.AccessKey, out.Cluster))
		}
		for _, p := range k.Problems {
			problems++
			line("BLOCKED", "client key "+k.AccessKey+": "+p)
		}
	}
	for _, n := range out.Notes {
		line("note", n)
	}
	if !out.Ready {
		what := fmt.Sprintf("%d problems block", problems)
		if problems == 1 {
			what = "1 problem blocks"
		}
		_, _ = fmt.Fprintf(w, "\n%s stepping out. Fix them and run shunt step-out again.\n", what)
		return
	}
	_, _ = fmt.Fprintf(w, "\nREADY: clients can use %s directly, with the keys and bucket names they use now. To step out:\n", out.Cluster)
	_, _ = fmt.Fprintf(w, "  1. Point the S3 name your clients use at %s (%s) instead of shunt: its DNS records (lower the TTL a day ahead) or the VIP.\n", out.Cluster, strings.Join(out.Endpoints, ", "))
	if out.Scheme == "http" {
		_, _ = fmt.Fprintf(w, "     Clients reaching shunt over https need %s to serve https with a certificate for that name.\n", out.Cluster)
	} else {
		_, _ = fmt.Fprintf(w, "     %s needs a certificate for that name, and a virtual-host domain if clients use virtual-host style.\n", out.Cluster)
	}
	_, _ = fmt.Fprintln(w, "  2. Wait out the TTL and watch shunt_requests_total on shunt's admin listener stop rising.")
	_, _ = fmt.Fprintln(w, "  3. Stop shunt. Until then, pointing the name back at shunt undoes the step-out: nothing in shunt changed.")
}
