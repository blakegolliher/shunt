// Command shunt is the S3 front-end proxy and its operator CLI. The operator verbs (cluster, tenant,
// adopt, expand, ramp, migrate, cutover, purge-source, status) call a running shunt's control API;
// the other subcommands from docs/DESIGN.md §2.10 arrive with their phases.
package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// Set via -ldflags "-X main.version=… -X main.commit=… -X main.date=…" (see Makefile).
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "shunt:", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "shunt",
		Short:         "S3 front-end proxy",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)
	root.AddCommand(newVersion(), newCheckConfig(), newServe(), newProbe(), newDirectory(),
		newCluster(), newTenant(), newAdopt(), newExpand(), newRamp(), newMigrate(), newCutover(), newPurgeSource(), newStatus(), newStepOut(), newVerify(), newClient(), newProxy())
	return root
}

func newVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, and build date",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "shunt %s (commit %s, built %s, %s %s/%s)\n",
				version, commit, date, runtime.Version(), runtime.GOOS, runtime.GOARCH)
			return err
		},
	}
}

func newCheckConfig() *cobra.Command {
	return &cobra.Command{
		Use:   "check-config <file>",
		Short: "Parse and validate a config file and its directory file; unknown keys are errors",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := config.Load(args[0])
			if err != nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: invalid\n%v\n", args[0], err)
				return err
			}
			summary := fmt.Sprintf("auth %s, %d clusters", c.Auth.Mode, len(c.Clusters))
			if c.Directory.File != "" {
				f, derr := directory.Load(c.Directory.File)
				if derr != nil {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: invalid directory\n%v\n", args[0], derr)
					return derr
				}
				summary = fmt.Sprintf("auth %s; directory %s version %d: %d clusters, %d tenants, %d placements",
					c.Auth.Mode, c.Directory.File, f.Version, len(f.Clusters), len(f.Tenants), len(f.Placements))
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: ok (%s)\n", args[0], summary)
			return err
		},
	}
}
