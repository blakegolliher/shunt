package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/control"
)

// exitError ends the command with a given exit status (main). Every other error exits 1.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

// exitWaitDeadline is `shunt operation wait` running out of time while the operation is still
// unfinished: not a failure of the operation (docs/design/distributed-correctness-contracts.md §2).
const exitWaitDeadline = 3

// newOperation is the operation verb (ADR-0017, ADR-0021): the records every change runs under,
// read from any control node.
func newOperation() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operation",
		Short: "The operation records changes run under: list them, show one, wait for one to end",
		Long: "Every change to a placement or a cluster runs under an operation record that reserves it while\n" +
			"it runs (ADR-0021). A record is pending, running or blocked until it ends succeeded, failed or\n" +
			"cancelled; its effect_state says whether it wrote the directory (none, committed, uncertain).", //nolint:misspell // the status is spelled as the contract spells it
	}
	cmd.AddCommand(newOperationList(), newOperationShow(), newOperationWait(), newOperationResume(), newOperationCancel())
	return cmd
}

func newOperationResume() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "resume <id>",
		Short: "Carry an operation on from where its record stands, on the control node --api names",
		Long: "An operation whose control node stopped (its record says blocked, owner_lost) is not repeated:\n" +
			"its hold is written and its record says which phase it reached, so another node picks it up there\n" +
			"(ADR-0021 D2). Refused while the owner is live: it would run twice.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var op control.Operation
			if err := api.call(cmd.Context(), "POST", "/v1/operations/"+url.PathEscape(args[0])+"/resume", nil, &op); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, op)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "operation %s resumed on %s (%s, phase %s); `shunt operation wait %s` follows it\n", op.ID, op.Node, op.Status, op.Phase, op.ID)
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}

func newOperationCancel() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "cancel <id>",
		Short: "Cancel an operation before its change is committed; the routing goes back as it was",
		Long: "A step that waits on a proxy (blocked) can be canceled: its hold is released and its record ends\n" +
			"canceled, with nothing changed. Once the change is committed, it is in force and cancellation is\n" +
			"refused (not_cancellable): a further change is a new operation (ADR-0021 D2).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var op control.Operation
			if err := api.call(cmd.Context(), "POST", "/v1/operations/"+url.PathEscape(args[0])+"/cancel", nil, &op); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, op)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "operation %s canceled; nothing changed\n", op.ID)
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}

func newOperationList() *cobra.Command {
	var (
		o                  apiOptions
		placement, cluster string
		limit              int
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List operation records, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			q := url.Values{"limit": {strconv.Itoa(limit)}}
			if placement != "" {
				key, err := placementKey(placement)
				if err != nil {
					return err
				}
				q.Set("placement", key)
			}
			if cluster != "" {
				q.Set("cluster", cluster)
			}
			var list control.OperationList
			if err := api.call(cmd.Context(), "GET", "/v1/operations?"+q.Encode(), nil, &list); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, list)
			}
			out := cmd.OutOrStdout()
			if len(list.Operations) == 0 {
				_, _ = fmt.Fprintln(out, "no operation records")
				return nil
			}
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "ID\tKIND\tSCOPE\tSTATUS\tPHASE\tEFFECT\tUPDATED")
			for i := range list.Operations {
				op := &list.Operations[i]
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", op.ID, op.Kind, scopeText(op), op.Status, op.Phase, op.EffectState, op.Updated.Format(time.RFC3339))
			}
			return tw.Flush()
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().StringVar(&placement, "placement", "", "only this placement's records (tenant/bucket)")
	cmd.Flags().StringVar(&cluster, "cluster", "", "only this cluster's records")
	cmd.Flags().IntVar(&limit, "limit", 50, "at most this many records (the API caps it at 500)")
	return cmd
}

func newOperationShow() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "show <id>",
		Short: "Show one operation record",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var op control.Operation
			if err := api.call(cmd.Context(), "GET", "/v1/operations/"+url.PathEscape(args[0]), nil, &op); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, op)
			}
			printOperation(cmd.OutOrStdout(), &op)
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}

func newOperationWait() *cobra.Command {
	var (
		o       apiOptions
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "wait <id>",
		Short: "Wait for an operation to end; exit 0 if it succeeded, 1 if it did not, 3 if it is still unfinished at --timeout",
		Long: "Polls the record until it ends. Interrupting the wait, or its timeout, does not stop the operation:\n" +
			"it runs on the control node, and its record has the outcome.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			var op control.Operation
			for {
				if err := api.call(ctx, "GET", "/v1/operations/"+url.PathEscape(args[0]), nil, &op); err != nil {
					if errors.Is(ctx.Err(), context.DeadlineExceeded) && op.ID != "" {
						break
					}
					return err
				}
				if op.Terminal() {
					break
				}
				select {
				case <-ctx.Done():
				case <-time.After(250 * time.Millisecond):
				}
				if ctx.Err() != nil {
					break
				}
			}
			if o.json {
				if err := printJSON(cmd, op); err != nil {
					return err
				}
			} else {
				printOperation(cmd.OutOrStdout(), &op)
			}
			switch {
			case !op.Terminal():
				what := fmt.Sprintf("operation %s is still %s after %s", op.ID, op.Status, timeout)
				if len(op.Blockers) > 0 {
					what += "; waiting on " + blockerLine(op.Blockers)
				}
				return &exitError{code: exitWaitDeadline, err: fmt.Errorf("%s; it keeps running: `shunt operation wait %s` again", what, op.ID)}
			case op.Status != control.StatusSucceeded:
				msg := "operation " + op.ID + " " + op.Status
				if op.Error != nil {
					msg += ": " + shownText(op.Error.Message)
				}
				return errors.New(msg)
			}
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "how long to wait before exiting 3 with the operation unfinished")
	return cmd
}

// scopeText is what an operation reserves, for a table.
func scopeText(op *control.Operation) string {
	switch {
	case op.Scope != nil:
		return op.Scope.Resource
	case op.Placement != "":
		return "placement:" + op.Placement
	case op.Cluster != "":
		return "cluster:" + op.Cluster
	}
	return "-"
}

// printOperation writes one record for an operator.
func printOperation(out io.Writer, op *control.Operation) {
	line := func(k, v string) {
		if v != "" {
			_, _ = fmt.Fprintf(out, "%-12s %s\n", k+":", v)
		}
	}
	line("operation", op.ID)
	line("kind", op.Kind)
	line("scope", scopeText(op))
	if op.Scope != nil {
		line("generation", strconv.FormatInt(op.Scope.Generation, 10))
		line("touches", strings.Join(op.Scope.Clusters, ", "))
	}
	line("status", op.Status)
	line("phase", op.Phase)
	line("effect", op.EffectState)
	line("actor", op.Actor)
	line("node", op.Node)
	line("request id", op.RequestID)
	line("waiting on", strings.Join(op.WaitingOn, ", "))
	line("silent", strings.Join(op.Silent, ", "))
	for _, b := range op.Blockers {
		line("blocker", strings.TrimSpace(b.Code+" "+b.ProxyID+" "+b.Message))
	}
	if b := op.Barrier; b != nil {
		state := fmt.Sprintf("%s on %s", b.Kind, b.Scope)
		switch {
		case b.Committed:
			state += fmt.Sprintf(", committed at version %d", b.CommitVersion)
		case b.HoldVersion > 0:
			state += fmt.Sprintf(", held at version %d (generation %d), draining", b.HoldVersion, b.Generation)
		default:
			state += ", hold not written yet"
		}
		line("barrier", state)
	}
	line("actions", strings.Join(op.AllowedActions, ", "))
	if op.Progress != nil {
		line("progress", fmt.Sprintf("%d/%d %s", op.Progress.Done, op.Progress.Total, op.Progress.Unit))
	}
	if op.Version > 0 {
		line("version", strconv.FormatInt(op.Version, 10))
	}
	if op.Error != nil {
		line("error", op.Error.Code+": "+shownText(op.Error.Message))
	}
	line("created", op.Created.Format(time.RFC3339))
	line("updated", op.Updated.Format(time.RFC3339))
}
