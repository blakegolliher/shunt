// Command shunt-control is shunt's control plane: an etcd member embedded in a control node that
// serves the control API to operators and the directory to a fleet of proxies (ADR-0015). Three
// nodes form a cluster; one is enough for a lab.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var version, commit, date = "dev", "", "" // set by -ldflags in the Makefile

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "shunt-control:", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "shunt-control",
		Short: "shunt's control plane: the directory, the fleet, and the operator API, on embedded etcd",
		Long: "One shunt-control per control node. `init` starts the first node and forms a new cluster; `join`\n" +
			"adds a node to a running one; either is also how a node is restarted. The other verbs talk to a\n" +
			"running node's API. Proxies join the fleet by naming the nodes in their control.endpoints; the\n" +
			"shunt operator verbs (shunt cluster, adopt, expand, ramp, ...) point --api at any node.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)
	root.AddCommand(newInit(), newJoin(), newMember(), newSnapshot(), newDefrag(), newStatus(), newVersion())
	return root
}

func newVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the build version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "shunt-control %s (%s, %s)\n", version, commit, date)
			return err
		},
	}
}

// signalContext ends when SIGTERM or SIGINT arrives. A second signal, or 15 s without the process
// having exited (etcd bootstrapping with its peers down does not watch the context), ends it
// outright: a control node must never survive its own stop.
func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case <-sig:
			cancel()
		case <-ctx.Done():
			signal.Stop(sig)
			return
		}
		select {
		case <-sig:
		case <-time.After(15 * time.Second):
		}
		fmt.Fprintln(os.Stderr, "shunt-control: not stopped after the signal; exiting now")
		os.Exit(1)
	}()
	return ctx, cancel
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var errTokenRequired = errors.New("--token-ref is required when --api is not loopback: proxies and operators on other hosts must present it")
