package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/cp"
)

// apiOptions are the flags of every verb that talks to a running control node.
type apiOptions struct {
	url      string
	tokenRef string
	json     bool
}

func addAPIFlags(cmd *cobra.Command, o *apiOptions) {
	def := os.Getenv("SHUNT_CONTROL_API")
	if def == "" {
		def = "http://127.0.0.1:9901"
	}
	f := cmd.Flags()
	f.StringVar(&o.url, "api", def, "a running control node's API (env SHUNT_CONTROL_API)")
	f.StringVar(&o.tokenRef, "token-ref", os.Getenv("SHUNT_API_TOKEN_REF"), "env:NAME or file:/path holding the API's token (env SHUNT_API_TOKEN_REF)")
	f.BoolVar(&o.json, "json", false, "print the API's JSON answer instead of a summary")
}

func (o apiOptions) client() (*apiClient, error) {
	c := &apiClient{base: strings.TrimRight(o.url, "/")}
	if o.tokenRef != "" {
		tok, err := config.ResolveSecret(o.tokenRef)
		if err != nil {
			return nil, fmt.Errorf("--token-ref: %w", err)
		}
		c.token = tok
	}
	return c, nil
}

type apiClient struct {
	base  string
	token string
	// idem, if set, is sent as the Idempotency-Key of every request that is not a GET: a join's
	// request id, so a retry answers the same operation (ADR-0021).
	idem string
}

func (c *apiClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rd io.Reader = http.NoBody
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.idem != "" && method != http.MethodGet {
		req.Header.Set(control.HeaderIdempotencyKey, c.idem)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("control API %s: %w (is shunt-control running there?)", c.base, err)
	}
	return resp, nil
}

// call sends one request; a non-2xx answer becomes an error carrying the API's message.
func (c *apiClient) call(ctx context.Context, method, path string, body, out any) error {
	resp, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e control.Error
		if json.Unmarshal(data, &e) == nil && e.Message != "" {
			msg := e.Message
			if e.Code == "refused" {
				msg = "refused: " + msg
			}
			return &apiError{status: resp.StatusCode, code: e.Code, msg: msg}
		}
		return &apiError{status: resp.StatusCode, msg: fmt.Sprintf("control API %s %s: HTTP %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// apiError is a control API answer other than 2xx: its status, its code and its message.
type apiError struct {
	status int
	code   string
	msg    string
}

func (e *apiError) Error() string { return e.msg }

func printJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func newStatus() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "status",
		Short: "The cluster (members, leader, quorum, database size), the fleet, and the directory version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var st cp.StatusAnswer
			if err := api.call(cmd.Context(), http.MethodGet, "/v1/control/status", nil, &st); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, st)
			}
			out := cmd.OutOrStdout()
			quorum := "quorum"
			if !st.Cluster.HasQuorum {
				quorum = "NO QUORUM: no change is possible until a majority of members is back"
			}
			_, _ = fmt.Fprintf(out, "node %s (shunt-control %s): %d of %d members started, %d needed for writes, %s\n",
				st.Node, st.Version, st.Cluster.Started, len(st.Cluster.Members), st.Cluster.Quorum, quorum)
			loaded := ""
			if !st.DirectoryLoaded {
				loaded = " (NOT LOADED on this node yet: waiting for quorum)"
			}
			_, _ = fmt.Fprintf(out, "directory version %d%s; etcd revision %d; database %s of %s in use, quota %s\n\n",
				st.Directory, loaded, st.Cluster.Revision, size(st.Cluster.DBInUse), size(st.Cluster.DBBytes), size(st.Cluster.QuotaByte))
			printMembers(out, st.Cluster.Members)
			_, _ = fmt.Fprintln(out)
			if st.FleetError != "" {
				_, _ = fmt.Fprintln(out, st.FleetError)
			} else {
				printFleet(out, st.Fleet)
			}
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}

func printMembers(out io.Writer, ms []cp.MemberInfo) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "MEMBER\tPEER\tROLE\tSTATE")
	for i := range ms {
		m := &ms[i]
		role, state := "follower", "started"
		if m.Leader {
			role = "leader"
		}
		if m.Learner {
			role = "learner"
		}
		if !m.Started {
			state = "added, not started"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", m.Name, strings.Join(m.PeerURLs, ","), role, state)
	}
	_ = tw.Flush()
}

func printFleet(out io.Writer, ms []control.Member) {
	if len(ms) == 0 {
		_, _ = fmt.Fprintln(out, "no proxies have joined the fleet")
		return
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "PROXY\tSTATE\tAPPLIED\tLAST HEARTBEAT")
	for i := range ms {
		m := &ms[i]
		state, seen := "live", "-"
		if !m.Live {
			state = "SILENT"
		}
		if m.Live && !m.Seen.IsZero() {
			seen = m.SinceSeen.Round(time.Second).String() + " ago"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", m.ID, state, m.Applied, seen)
	}
	_ = tw.Flush()
}

func size(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}

func newMember() *cobra.Command {
	cmd := &cobra.Command{Use: "member", Short: "List the control nodes, or remove one"}
	var o apiOptions
	list := &cobra.Command{
		Use:   "list",
		Short: "List the cluster's members",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var st cp.StatusAnswer
			if err := api.call(cmd.Context(), http.MethodGet, "/v1/control/status", nil, &st); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, st.Cluster.Members)
			}
			printMembers(cmd.OutOrStdout(), st.Cluster.Members)
			return nil
		},
	}
	addAPIFlags(list, &o)
	var ro apiOptions
	remove := &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove a member from the cluster (to replace a failed node: remove, then join with the same name on a fresh data directory)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := ro.client()
			if err != nil {
				return err
			}
			if err := api.call(cmd.Context(), http.MethodDelete, "/v1/control/members/"+args[0], nil, nil); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "member %s removed; stop its shunt-control if it is still running\n", args[0])
			return nil
		},
	}
	addAPIFlags(remove, &ro)
	cmd.AddCommand(list, remove)
	return cmd
}

func newSnapshot() *cobra.Command {
	cmd := &cobra.Command{Use: "snapshot", Short: "Save a snapshot of the store, or restore one into a new data directory"}
	var o apiOptions
	save := &cobra.Command{
		Use:   "save <path>",
		Short: "Write a point-in-time snapshot of the store to a file (0600)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			resp, err := api.do(cmd.Context(), http.MethodGet, "/v1/control/snapshot", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close() //nolint:errcheck // read below
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				return fmt.Errorf("snapshot: HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}
			f, err := os.OpenFile(args[0], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // operator-chosen path; secrets inside are sealed
			if err != nil {
				return err
			}
			n, err := io.Copy(f, resp.Body)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				_ = os.Remove(args[0])
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "snapshot saved: %s (%s). Its secrets are sealed with the cluster's data-encryption key; keep %s from a node's data directory with it\n", args[0], size(n), "encryption.key")
			return nil
		},
	}
	addAPIFlags(save, &o)
	var dataDir, name, peerURL string
	restore := &cobra.Command{
		Use:   "restore <path>",
		Short: "Rebuild a data directory from a snapshot, offline, as a cluster of one for other nodes to join",
		Long: "Runs with no shunt-control running on this host. The restored node needs the data-encryption key\n" +
			"the snapshot was taken under: copy encryption.key into --data-dir before starting it, then start it\n" +
			"with `shunt-control init` and join the other nodes to it on fresh data directories.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cp.Restore(args[0], dataDir, name, peerURL); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "restored into %s as member %s (%s). Copy encryption.key in, then: shunt-control init --name %s --data-dir %s --peer-url %s ...\n",
				dataDir, name, peerURL, name, dataDir, peerURL)
			return nil
		},
	}
	restore.Flags().StringVar(&dataDir, "data-dir", "", "a new data directory for the restored member (required)")
	restore.Flags().StringVar(&name, "name", "", "the restored member's name (required)")
	restore.Flags().StringVar(&peerURL, "peer-url", "", "the restored member's peer URL (required)")
	_ = restore.MarkFlagRequired("data-dir")
	_ = restore.MarkFlagRequired("name")
	_ = restore.MarkFlagRequired("peer-url")
	cmd.AddCommand(save, restore)
	return cmd
}

func newDefrag() *cobra.Command {
	var o apiOptions
	cmd := &cobra.Command{
		Use:   "defrag",
		Short: "Compact this node's database file in place (blocks the node briefly; one node at a time)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var out map[string]string
			if err := api.call(cmd.Context(), http.MethodPost, "/v1/control/defrag", nil, &out); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "defragmented %s\n", out["defragmented"])
			return nil
		},
	}
	addAPIFlags(cmd, &o)
	return cmd
}
