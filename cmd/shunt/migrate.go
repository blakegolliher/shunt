package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v4"
	"golang.org/x/term"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// The operator verbs (docs/DESIGN.md §2.10, docs/walkthrough.md). Each one is a call to the control
// API of a running shunt, which checks it and writes it through the directory's write path: the
// CLI never edits the directory file itself. docs/reference/control-api.md lists the endpoints.

func newCluster() *cobra.Command {
	cmd := &cobra.Command{Use: "cluster", Short: "Add or remove a backend cluster while shunt serves"}
	cmd.AddCommand(newClusterAdd(), newClusterRemove(), newClusterReadOnly())
	return cmd
}

func newClusterReadOnly() *cobra.Command {
	var o apiOptions
	var off, reject bool
	var wait time.Duration
	cmd := &cobra.Command{
		Use:   "readonly <name>",
		Short: "Fence a backend read-only for maintenance, or make it writable again",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var out control.ReadOnlyResult
			req := control.OperationRequest{Kind: control.OpClusterReadOnly, Cluster: args[0], Args: argsOf(control.ReadOnlyRequest{ReadOnly: !off, Reject: reject, Wait: wait.String()})}
			if opErr := api.operate(cmd.Context(), req, &out); opErr != nil {
				return opErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "cluster %s: read-only %t (directory version %d)\n", args[0], out.ReadOnly, out.Version)
			return err
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().BoolVar(&off, "off", false, "make the cluster writable again")
	cmd.Flags().BoolVar(&reject, "reject", false, "fail writes fast with 403 instead of retryable 503")
	cmd.Flags().DurationVar(&wait, "wait", 30*time.Second, "how long to wait for every proxy at each fence")
	return cmd
}

func newBucketReadOnly() *cobra.Command {
	var o apiOptions
	var off, reject bool
	var wait time.Duration
	cmd := &cobra.Command{
		Use:   "readonly <bucket>",
		Short: "Fence one bucket read-only, or make it writable again",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			key := args[0]
			if !strings.Contains(key, "/") {
				key = directory.Key(directory.DefaultTenant, key)
			}
			var out control.ReadOnlyResult
			req := control.OperationRequest{Kind: control.OpPlacementReadOnly, Placement: key, Args: argsOf(control.ReadOnlyRequest{ReadOnly: !off, Reject: reject, Wait: wait.String()})}
			if opErr := api.operate(cmd.Context(), req, &out); opErr != nil {
				return opErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: read-only %t (directory version %d)\n", shown(key), out.ReadOnly, out.Version)
			return err
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().BoolVar(&off, "off", false, "make the bucket writable again")
	cmd.Flags().BoolVar(&reject, "reject", false, "fail writes fast with 403 instead of retryable 503")
	cmd.Flags().DurationVar(&wait, "wait", 30*time.Second, "how long to wait for every proxy at each fence")
	return cmd
}

func newClusterAdd() *cobra.Command {
	var (
		o                            apiOptions
		c                            config.Cluster
		conditionalWrite, condDelete bool
	)
	cmd := &cobra.Command{
		Use:   "add <name> <url>",
		Short: "Add a cluster, or replace its definition; the proxy uses it from the next request",
		Long: "Adds a backend cluster to the directory, e.g.\n\n" +
			"  shunt cluster add vast01 http://10.0.1.10 --access-key AKIA...\n\n" +
			"The URL gives the scheme, endpoint and port (80 or 443 by default). shunt prompts for the secret key\n" +
			"(or reads one line from stdin when it is not a terminal) and stores it in its own secrets directory.\n" +
			"Before anything is saved, shunt signs one request to the cluster with the key and secret and refuses\n" +
			"a wrong pair. The type (vast, minio, aws, s3) is read from the cluster's Server header and the region\n" +
			"defaults to us-east-1 (or the AWS region in an amazonaws.com host); --type and --region override.\n" +
			"Conditional-write capabilities are measured by `shunt expand` unless given here.\n" +
			"--secret-ref env:NAME|file:/path uses a secret you manage instead of prompting.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 2 {
				if c.Scheme != "" || len(c.Endpoints) > 0 {
					return errors.New("give the cluster as a URL or as --scheme and --endpoint, not both")
				}
				scheme, hostport, err := parseClusterURL(args[1])
				if err != nil {
					return err
				}
				c.Scheme, c.Endpoints = scheme, []string{hostport}
			}
			if c.Scheme == "" || len(c.Endpoints) == 0 {
				return errors.New("give the cluster's URL, as in: shunt cluster add vast01 http://10.0.1.10 --access-key <key>")
			}
			if c.Credentials.AccessKey == "" {
				return errors.New("--access-key is required")
			}
			if cmd.Flags().Changed("conditional-write") {
				c.Capabilities.ConditionalWrite = &conditionalWrite
			}
			if cmd.Flags().Changed("conditional-delete") {
				c.Capabilities.ConditionalDelete = &condDelete
			}
			req := control.ClusterRequest{Name: args[0], Cluster: c}
			if c.Credentials.SecretRef == "" {
				secret, err := readSecret(cmd, fmt.Sprintf("%s secret key: ", args[0]))
				if err != nil {
					return err
				}
				req.Secret = secret
			}
			api, err := o.client()
			if err != nil {
				return err
			}
			var out control.ClusterStatus
			if callErr := api.call(cmd.Context(), "POST", "/v1/clusters", req, &out); callErr != nil {
				return callErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "cluster %s: %s %s://%s region %s (conditional_write %v, conditional_delete %v)\n",
				out.Name, out.Type, out.Scheme, strings.Join(out.Endpoints, ","), out.Region, out.ConditionalWrite, out.ConditionalDelete)
			return err
		},
	}
	addAPIFlags(cmd, &o)
	f := cmd.Flags()
	f.StringVar(&c.Credentials.AccessKey, "access-key", "", "the cluster's access key (required)")
	f.StringVar(&c.Credentials.SecretRef, "secret-ref", "", "env:NAME or file:/path holding the secret, resolved by shunt serve, instead of prompting")
	f.StringVar(&c.Type, "type", "", "vast | minio | aws | s3 (default: read from the cluster's Server header)")
	f.StringVar(&c.Region, "region", "", "the signing region (default us-east-1, or the region in an amazonaws.com host)")
	f.StringVar(&c.Scheme, "scheme", "", "instead of a URL: http | https")
	f.StringArrayVar(&c.Endpoints, "endpoint", nil, "instead of a URL: host:port (repeatable)")
	f.StringVar(&c.TLS.CA, "ca", "", "https: CA bundle to verify the cluster's certificate")
	f.BoolVar(&conditionalWrite, "conditional-write", true, "the cluster honors If-None-Match: * on PUT (default: measured by expand)")
	f.BoolVar(&condDelete, "conditional-delete", false, "the cluster honors If-Match on DELETE (default: measured by expand)")
	return cmd
}

// parseClusterURL reads http://host[:port] or https://host[:port] into a scheme and host:port.
func parseClusterURL(raw string) (scheme, hostport string, err error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "", fmt.Errorf("%q: want http://host[:port] or https://host[:port]", raw)
	}
	if u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.User != nil {
		return "", "", fmt.Errorf("%q: give only the scheme, host and port", raw)
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	return u.Scheme, net.JoinHostPort(u.Hostname(), port), nil
}

// readSecret prompts on the terminal without echo, or reads one line from stdin when it is not one
// (so `printf '%s\n' "$SECRET" | shunt cluster add ...` works in scripts).
func readSecret(cmd *cobra.Command, prompt string) (string, error) {
	in := cmd.InOrStdin()
	if f, ok := in.(*os.File); ok && term.IsTerminal(int(f.Fd())) { //nolint:gosec // G115: a file descriptor fits in an int
		_, _ = fmt.Fprint(cmd.ErrOrStderr(), prompt)
		b, err := term.ReadPassword(int(f.Fd())) //nolint:gosec // G115: as above
		_, _ = fmt.Fprintln(cmd.ErrOrStderr())
		if err != nil {
			return "", err
		}
		return checkSecret(string(b))
	}
	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return checkSecret(strings.TrimRight(line, "\r\n"))
}

func checkSecret(s string) (string, error) {
	if s == "" {
		return "", errors.New("no secret key given")
	}
	return s, nil
}

func newClusterRemove() *cobra.Command {
	var (
		o      apiOptions
		dryRun bool
	)
	cmd := &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove a cluster nothing references any more",
		Long: "Refused while any placement or tenant default still names the cluster. The dry run the API answers\n" +
			"first says what would happen and issues the confirmation token the removal presents (ADR-0017);\n" +
			"--dry-run stops there.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			api, err := o.client()
			if err != nil {
				return err
			}
			var plan control.RemoveDryRun
			if callErr := api.call(cmd.Context(), "DELETE", "/v1/clusters/"+url.PathEscape(name)+"?dry_run=1", nil, &plan); callErr != nil {
				return callErr
			}
			if !plan.Allowed {
				return fmt.Errorf("refused: %s", shownText(plan.Reason))
			}
			if dryRun {
				if o.json {
					return printJSON(cmd, plan)
				}
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "would remove cluster %s: nothing references it, and %d stored secret file(s) go with it\n", name, plan.SecretFiles)
				return err
			}
			var out control.RemoveResult
			req := control.OperationRequest{Kind: control.OpClusterRemove, Cluster: name, Args: argsOf(control.RemoveRequest{Token: plan.Token})}
			if opErr := api.operate(cmd.Context(), req, &out); opErr != nil {
				return opErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "cluster %s removed; shunt no longer holds a connection to it\n", name)
			return err
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "say what would be removed and stop")
	return cmd
}

func newTenant() *cobra.Command {
	cmd := &cobra.Command{Use: "tenant", Short: "Change a tenant's settings"}
	var o apiOptions
	setDefault := &cobra.Command{
		Use:   "set-default [tenant] <cluster>",
		Short: "Point the cluster a tenant's new buckets land on elsewhere (the default tenant when none is named)",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				args = []string{directory.DefaultTenant, args[0]}
			}
			api, err := o.client()
			if err != nil {
				return err
			}
			var out map[string]any
			if callErr := api.call(cmd.Context(), "POST", "/v1/tenants/"+url.PathEscape(args[0])+"/default-cluster", control.TenantDefaultRequest{Cluster: args[1]}, &out); callErr != nil {
				return callErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			who := args[0] + ": new"
			if args[0] == directory.DefaultTenant {
				who = "New"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s buckets now land on %s (directory version %v)\n", who, args[1], out["version"])
			return err
		},
	}
	addAPIFlags(setDefault, &o)
	cmd.AddCommand(setDefault)
	return cmd
}

func newAdopt() *cobra.Command {
	var (
		o        apiOptions
		name     string
		keysFile string
		create   bool
		spread   []string
	)
	cmd := &cobra.Command{
		Use:   "adopt <cluster> <bucket>",
		Short: "Serve an existing bucket through shunt under the same name (docs/DESIGN.md §11), or create one",
		Long: "Takes over a bucket that already exists on a cluster, keeping its name.\n\n" +
			"--create makes a new, empty bucket on the cluster instead. --spread <cluster>[:<name>], once per\n" +
			"further cluster, creates a bucket spread over one new bucket on each (ADR-0018): every key lives\n" +
			"in exactly one of them, chosen by its hash, and listings merge them all. It implies --create.\n\n" +
			"--keys imports the client keys that cluster already issued, in the credentials-file schema\n" +
			"(access_key and secret per entry). shunt checks each against the cluster and then verifies\n" +
			"client signatures with them, so clients keep the credentials they have today and keep them\n" +
			"if shunt is taken out of the path later (shunt step-out, ADR-0012).",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := placementPath(args[1])
			if err != nil {
				return err
			}
			keys, err := readClientKeys(keysFile)
			if err != nil {
				return err
			}
			api, err := o.client()
			if err != nil {
				return err
			}
			var out control.PlacementStatus
			switch {
			case create || len(spread) > 0:
				req := control.CreateBackendRequest{Cluster: args[0], Name: name, Keys: keys}
				if len(spread) > 0 {
					req.Legs = []control.LegRequest{{Cluster: args[0], Name: name}}
					for _, l := range spread {
						c, n, _ := strings.Cut(l, ":")
						if c == "" {
							return fmt.Errorf("--spread %q: want <cluster>[:<bucket name>]", l)
						}
						req.Legs = append(req.Legs, control.LegRequest{Cluster: c, Name: n})
					}
				}
				if callErr := api.call(cmd.Context(), "POST", path+"/create-backend", req, &out); callErr != nil {
					return callErr
				}
			default:
				if callErr := api.call(cmd.Context(), "POST", path+"/adopt", control.AdoptRequest{Cluster: args[0], Name: name, Keys: keys}, &out); callErr != nil {
					return callErr
				}
			}
			if o.json {
				return printJSON(cmd, out)
			}
			for _, k := range keys {
				if _, perr := fmt.Fprintf(cmd.OutOrStdout(), "client key %s imported: %s accepts it, and shunt now verifies clients with it\n", k.AccessKey, args[0]); perr != nil {
					return perr
				}
			}
			if len(out.Legs) > 0 {
				where := make([]string, 0, len(out.Legs))
				for _, l := range out.Legs {
					where = append(where, fmt.Sprintf("%s/%s (%.0f%%)", l.Cluster, l.Bucket, 100*l.Share))
				}
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: ACTIVE, spread over %s\n", shown(out.Key), strings.Join(where, ", "))
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: ACTIVE on %s/%s\n", shown(out.Key), out.Primary, out.Names[out.Primary])
			return err
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().BoolVar(&create, "create", false, "create a new, empty bucket on the cluster instead of adopting one")
	cmd.Flags().StringArrayVar(&spread, "spread", nil, "also spread the new bucket over this `cluster[:name]` (repeatable; implies --create)")
	cmd.Flags().StringVar(&name, "name", "", "the bucket's name on the cluster (default: the same name)")
	cmd.Flags().StringVar(&keysFile, "keys", "", "a credentials-file `path` of client keys the cluster already issued, to import")
	return cmd
}

// readClientKeys reads a credentials file of keys to import. It is the operator's own file, in the
// same schema as auth.credentials_file, so an existing one can be handed to adopt as it is.
func readClientKeys(path string) ([]control.ClientKeyRequest, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path) //nolint:gosec // an operator-named file
	if err != nil {
		return nil, err
	}
	var f auth.File
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(f.Credentials) == 0 {
		return nil, fmt.Errorf("%s: no credentials in it", path)
	}
	keys := make([]control.ClientKeyRequest, 0, len(f.Credentials))
	for _, e := range f.Credentials {
		if e.Secret == "" {
			return nil, fmt.Errorf("%s: access key %s has no secret; shunt verifies client signatures, so it needs the secret itself", path, e.AccessKey)
		}
		keys = append(keys, control.ClientKeyRequest{AccessKey: e.AccessKey, Secret: e.Secret, Buckets: e.Buckets})
	}
	return keys, nil
}

func newExpand() *cobra.Command {
	var (
		o           apiOptions
		req         control.ExpandRequest
		clearTarget bool
		carve       string
		merge       string
	)
	cmd := &cobra.Command{
		Use:   "expand <bucket>",
		Short: "Prepare a second cluster for a bucket: its bucket, a versioning check, and a canary",
		Long: "Records the cluster and bucket a later ramp or migrate step moves the bucket to. The target\n" +
			"bucket must be empty: its objects would join this bucket (--accept-existing-objects takes one\n" +
			"whose objects are this bucket's, copied ahead). --clear forgets the target again before the\n" +
			"first step, leaving its bucket where it is.\n\n" +
			"--carve <prefix> gives the keys under a prefix a scope of their own, owned exactly as they are now,\n" +
			"so later moves can place that prefix on its own (ADR-0020); no data moves. --merge <prefix>\n" +
			"removes a scope again once its keys are owned as the rest of their parent scope is.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := placementPath(args[0])
			if err != nil {
				return err
			}
			api, err := o.client()
			if err != nil {
				return err
			}
			if carve != "" || merge != "" {
				if carve != "" && merge != "" {
					return errors.New("give --carve or --merge, not both")
				}
				var out control.PrefixResult
				method, route, body, did := "POST", path+"/prefixes", any(control.PrefixRequest{Prefix: carve}), "carved: its keys have a scope of their own, owned as before"
				if merge != "" {
					method, route, body, did = "DELETE", path+"/prefixes?prefix="+url.QueryEscape(merge), nil, "merged back into its parent scope"
				}
				if callErr := api.call(cmd.Context(), method, route, body, &out); callErr != nil {
					return callErr
				}
				if o.json {
					return printJSON(cmd, out)
				}
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: prefix %q %s; %d prefix rules (directory version %d)\n", shown(out.Key), out.Prefix, did, out.Rules, out.Version)
				return err
			}
			if clearTarget {
				var out control.ClearTargetResult
				if callErr := api.call(cmd.Context(), "DELETE", path+"/target", nil, &out); callErr != nil {
					return callErr
				}
				if o.json {
					return printJSON(cmd, out)
				}
				for _, l := range out.Retired {
					if _, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: leg %s (%s/%s) retired; it owned no keys, and its bucket is still on %s (directory version %d)\n",
						shown(out.Key), l.ID, l.Cluster, l.Bucket, l.Cluster, out.Version); err != nil {
						return err
					}
				}
				if out.Target == "" {
					return nil
				}
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: target %s/%s cleared; the bucket is still on %s (directory version %d)\n",
					shown(out.Key), out.Target, out.Name, out.Target, out.Version)
				return err
			}
			var out control.ExpandResult
			if callErr := api.call(cmd.Context(), "POST", path+"/expand", req, &out); callErr != nil {
				return callErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			created := "exists"
			if out.CreatedBucket {
				created = "created"
			}
			how := "as configured"
			if out.Measured {
				how = "measured"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: target %s/%s (%s); canary write, read, delete ok; conditional_write %v, conditional_delete %v (%s; directory version %d)\n",
				shown(out.Key), out.Target, out.Name, created, out.ConditionalWrite, out.ConditionalDelete, how, out.Version)
			if err != nil {
				return err
			}
			if _, bucket, _ := strings.Cut(out.Key, "/"); out.Name != bucket {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "note: on %s the bucket is named %s, not %s; to take shunt out of the path later, clients would have to use that name (--name %s keeps it; shunt step-out, ADR-0011)\n",
					out.Target, out.Name, bucket, bucket)
			}
			return err
		},
	}
	addAPIFlags(cmd, &o)
	f := cmd.Flags()
	f.StringVar(&req.To, "to", "", "the cluster the bucket will move to")
	f.StringVar(&req.Name, "name", "", "the bucket's name there (default: <name>-001, the lowest unused)")
	f.BoolVar(&req.Create, "create", false, "create the bucket there if it does not exist")
	f.BoolVar(&req.AcceptObjects, "accept-existing-objects", false, "take a target bucket that holds objects: they are this bucket's, copied ahead")
	f.StringVar(&carve, "carve", "", "give the keys under this `prefix` a scope of their own, owned as they are now (ADR-0020)")
	f.StringVar(&merge, "merge", "", "remove the scope of this `prefix`, once it is owned as its parent scope is")
	f.BoolVar(&clearTarget, "clear", false, "forget the target expand recorded, before the first step; for a spread bucket, retire its legs that own no keys")
	return cmd
}

func newRamp() *cobra.Command {
	var (
		o         apiOptions
		req       control.RampRequest
		from      string
		rangeText string
		wait      time.Duration
	)
	cmd := &cobra.Command{
		Use:   "ramp <bucket>",
		Short: "Send a growing share of a bucket's writes to its new cluster",
		Long: "Moves the placement into RAMPING, or raises an existing ramp. --ratio is the fraction of keys,\n" +
			"chosen by a stable hash of the key, whose writes go to the new primary; --prefix names key\n" +
			"prefixes that go there whatever the ratio. A ramp only grows: shrinking needs a reconcile.\n" +
			"Reads still fall back to the source, so a key written on either side is readable throughout.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if req.Ratio == 0 && len(req.Prefixes) == 0 {
				return errors.New("give --ratio, --prefix, or both")
			}
			if req.Ratio < 0 || req.Ratio > 1 {
				return fmt.Errorf("--ratio %v is outside 0..1", req.Ratio)
			}
			rg, err := parseRange(rangeText)
			if err != nil {
				return err
			}
			req.Range, req.Wait = rg, wait.String()
			return forEach(cmd, o, args, from, func(api *apiClient, key string) error {
				return transition(cmd, api, o, key, control.OpRamp, req, time.Minute+3*wait)
			})
		},
	}
	addAPIFlags(cmd, &o)
	f := cmd.Flags()
	f.Float64Var(&req.Ratio, "ratio", 0, "fraction of keys, by hash, whose writes go to the new primary")
	f.StringArrayVar(&req.Prefixes, "prefix", nil, "key prefix whose writes go to the new primary (repeatable)")
	f.StringVar(&req.To, "to", "", "the new cluster, if expand has not recorded one")
	f.StringVar(&req.Name, "name", "", "the bucket's name on --to (default: a generated name)")
	f.BoolVar(&req.Create, "create", false, "create the bucket on --to if it does not exist")
	f.StringVar(&from, "from", "", "instead of one bucket: every bucket this cluster serves")
	f.DurationVar(&wait, "wait", 30*time.Second, "with several proxies: how long to wait for all of them to have the step")
	addMoveFlags(cmd, &rangeText, &req.Leg, &req.Scope)
	return cmd
}

// addMoveFlags adds the flags that make a first step move part of a bucket (ADR-0018 N3).
func addMoveFlags(cmd *cobra.Command, rangeText, leg, scope *string) {
	f := cmd.Flags()
	f.StringVar(scope, "scope", "", "move keys of this prefix rule (shunt expand --carve made it); --range and --leg are read in its table, and a rule one leg owns moves whole (ADR-0020)")
	f.StringVar(rangeText, "range", "", "move only the keys whose hash is in `from-to` (16 hex digits each, inclusive), out of the one leg that owns them")
	f.StringVar(leg, "leg", "", "move the keys of this leg of a spread bucket (its first range; repeat per range); consolidating a bucket is this, once per other leg")
}

// parseRange reads --range: two 16-hex-digit hash bounds joined by '-', or nothing.
func parseRange(s string) (*directory.HashRange, error) {
	if s == "" {
		return nil, nil //nolint:nilnil // no range asked for
	}
	from, to, ok := strings.Cut(s, "-")
	var rg directory.HashRange
	if !ok || rg.From.UnmarshalText([]byte(from)) != nil || rg.To.UnmarshalText([]byte(to)) != nil {
		return nil, fmt.Errorf("--range %q: want <from>-<to>, 16 hex digits each, such as 0000000000000000-7fffffffffffffff", s)
	}
	if rg.From > rg.To {
		return nil, fmt.Errorf("--range %q: the start is after the end", s)
	}
	return &rg, nil
}

func newMigrate() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Move a bucket's data to another cluster while clients keep reading and writing",
	}
	cmd.AddCommand(newMigrateStart(), newMigrateRun(), newMigrateFinish())
	return cmd
}

func newMigrateStart() *cobra.Command {
	var (
		o         apiOptions
		req       control.MigrateRequest
		from      string
		rangeText string
		wait      time.Duration
	)
	cmd := &cobra.Command{
		Use:   "start <bucket>",
		Short: "Send all new writes to the new cluster and serve reads from both",
		Long: "Moves the placement into MIGRATING. From here every write lands on the new primary, reads fall\n" +
			"back to the source when the primary does not have the object, deletes go to both, and listings\n" +
			"are merged. That is the state the mover copies in: `shunt migrate run`.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rg, err := parseRange(rangeText)
			if err != nil {
				return err
			}
			req.Range, req.Wait = rg, wait.String()
			return forEach(cmd, o, args, from, func(api *apiClient, key string) error {
				return transition(cmd, api, o, key, control.OpMigrate, req, time.Minute+3*wait)
			})
		},
	}
	addAPIFlags(cmd, &o)
	f := cmd.Flags()
	f.BoolVar(&req.AcceptLostWriteWindow, migrate.AcceptLostWriteWindowFlag, false,
		"start even though the target ignores If-None-Match: * on PUT, accepting that the mover can overwrite a client write (docs/migrating.md)")
	f.StringVar(&req.To, "to", "", "the new cluster, if expand or ramp has not named one")
	f.StringVar(&req.Name, "name", "", "the bucket's name on --to")
	f.BoolVar(&req.Create, "create", false, "create the bucket on --to if it does not exist")
	f.StringVar(&from, "from", "", "instead of one bucket: every bucket moving off, or served by, this cluster")
	f.DurationVar(&wait, "wait", 30*time.Second, "with several proxies: how long to wait for all of them to have the step")
	addMoveFlags(cmd, &rangeText, &req.Leg, &req.Scope)
	return cmd
}

func newCutover() *cobra.Command {
	var (
		o      apiOptions
		window time.Duration
		from   string
		wait   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "cutover <bucket>",
		Short: "Stop reading the source, once the mover has converged and no read fell back for --window",
		Long: "Moves a MIGRATING placement into CUTOVER: reads stop falling back and listings stop merging;\n" +
			"deletes still reach the source, so it only ever loses keys. Refused unless the mover's last\n" +
			"report says a whole pass copied nothing, and unless this proxy served no fallback read of the\n" +
			"bucket during --window, which the call waits out.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return forEach(cmd, o, args, from, func(api *apiClient, key string) error {
				if !o.json {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s: waiting %s for fallback reads to stay flat\n", key, window)
				}
				return transition(cmd, api, o, key, control.OpCutover, control.CutoverRequest{Window: window.String(), Wait: wait.String()}, window+2*time.Minute+3*wait)
			})
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().DurationVar(&window, "window", 60*time.Second, "how long fallback reads must stay flat")
	cmd.Flags().StringVar(&from, "from", "", "instead of one bucket: every bucket migrating off this cluster")
	cmd.Flags().DurationVar(&wait, "wait", 30*time.Second, "with several proxies: how long to wait for all of them to report and to have the change")
	return cmd
}

func newPurgeSource() *cobra.Command {
	var (
		o      apiOptions
		dryRun bool
		wait   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "purge-source <bucket>",
		Short: "Delete the source bucket of a cut-over placement and forget the source",
		Long: "Refused unless the placement is in CUTOVER with the evidence shunt cutover recorded, and unless the\n" +
			"source holds no key the primary lacks. The dry run the API answers first counts what would go and\n" +
			"issues the confirmation token the purge presents (ADR-0017); --dry-run stops there. Then every\n" +
			"in-progress upload on the source is aborted, every object deleted, the bucket deleted, and the\n" +
			"placement returns to ACTIVE on its primary.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := placementPath(args[0])
			if err != nil {
				return err
			}
			key, err := placementKey(args[0])
			if err != nil {
				return err
			}
			api, err := o.client()
			if err != nil {
				return err
			}
			var plan control.PurgeDryRun
			if callErr := api.call(cmd.Context(), "POST", path+"/purge-source", control.PurgeRequest{DryRun: true, Wait: wait.String()}, &plan); callErr != nil {
				return callErr
			}
			if !plan.Allowed {
				return fmt.Errorf("refused: %s", shownText(plan.Reason))
			}
			out := cmd.OutOrStdout()
			switch {
			case o.json && dryRun:
				return printJSON(cmd, plan)
			case !o.json:
				then := "then forget the source"
				if plan.KeepsBucket {
					then = "and keep the bucket, which holds the rest of the bucket's keys"
				}
				_, _ = fmt.Fprintf(out, "%s: would delete %d objects (%s) and abort %d in-flight uploads from %s/%s, %s\n",
					shown(plan.Key), plan.Objects, humanBytes(plan.Bytes), plan.UploadsInFlight, plan.Source, plan.Bucket, then)
			}
			if dryRun {
				return nil
			}
			var res control.PurgeResult
			req := control.OperationRequest{Kind: control.OpPurge, Placement: key, Args: argsOf(control.PurgeRequest{Token: plan.Token, Wait: wait.String()})}
			if opErr := api.operate(cmd.Context(), req, &res); opErr != nil {
				return opErr
			}
			if o.json {
				return printJSON(cmd, res)
			}
			bucket := "deleted the bucket; ACTIVE on its primary"
			if !res.BucketDeleted {
				bucket = "kept the bucket, which holds the rest of the bucket's keys; ACTIVE"
			}
			_, err = fmt.Fprintf(out, "%s: listing diff empty; deleted %d objects and aborted %d uploads from %s/%s, %s (directory version %d)\n",
				shown(res.Key), res.ObjectsDeleted, res.UploadsAborted, res.Source, res.Bucket, bucket, res.Version)
			return err
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "count what would be deleted and stop")
	cmd.Flags().DurationVar(&wait, "wait", 30*time.Second, "with several proxies: how long to wait for all of them to have the cutover")
	return cmd
}

func newMigrateFinish() *cobra.Command {
	var (
		o    apiOptions
		from string
	)
	cmd := &cobra.Command{
		Use:   "finish <bucket>",
		Short: "Forget the source of a cut-over placement without deleting its data",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := forEach(cmd, o, args, from, func(api *apiClient, key string) error {
				return transition(cmd, api, o, key, control.OpFinish, nil, time.Minute)
			})
			if err == nil && from != "" && !o.json {
				return reportLeftovers(cmd, o, from)
			}
			return err
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().StringVar(&from, "from", "", "instead of one bucket: every bucket that has cut over from this cluster")
	return cmd
}

func newStatus() *cobra.Command {
	var (
		o   apiOptions
		all bool
	)
	cmd := &cobra.Command{
		Use:   "status [bucket]",
		Short: "Show clusters, moving buckets, the write split, fallback reads, and mover progress",
		Long: "Lists the clusters and every bucket that is moving or has a target recorded; --all lists every\n" +
			"bucket. A bucket spread over several backend buckets (ADR-0018) also gets a table of its legs:\n" +
			"each one's bucket, its share of the key space, the hash ranges it owns, and the part moving.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			q := ""
			if len(args) == 1 {
				key, kerr := placementKey(args[0])
				if kerr != nil {
					return kerr
				}
				q = "?bucket=" + url.QueryEscape(key)
			} else if all {
				q = "?all=1"
			}
			var st control.Status
			if err := api.call(cmd.Context(), "GET", "/v1/status"+q, nil, &st); err != nil {
				return err
			}
			if o.json {
				return printJSON(cmd, st)
			}
			return printStatus(cmd, st)
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().BoolVar(&all, "all", false, "every bucket, not only those moving or expanded")
	return cmd
}

func printStatus(cmd *cobra.Command, st control.Status) error {
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "directory version %d\n\n", st.Version)
	// Columns size to their widest value: endpoints and bucket names vary a lot between sites.
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "CLUSTER\tTYPE\tSCHEME\tENDPOINTS\tCOND.PUT\tUSED BY")
	for i := range st.Clusters {
		c := &st.Clusters[i]
		used := shownText(strings.Join(c.References, ", "))
		if used == "" {
			used = "-"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%v\t%s\n", c.Name, c.Type, c.Scheme, strings.Join(c.Endpoints, ","), c.ConditionalWrite, used)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out)
	if len(st.Placements) == 0 {
		_, err := fmt.Fprintln(out, "no bucket is moving (--all lists every bucket)")
		return err
	}
	tw = tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "BUCKET\tSTATE\tRATIO\tPRIMARY\tSOURCE\tWRITES P/S\tFALLBACK READS\tMOVER")
	for i := range st.Placements {
		p := &st.Placements[i]
		ratio := "-"
		if p.Ratio > 0 {
			ratio = strconv.FormatFloat(p.Ratio, 'f', 2, 64)
		}
		if p.Hold != nil {
			// A step written but not on every proxy yet: its keys' writes answer 503 (ADR-0016).
			ratio += " held→" + strconv.FormatFloat(p.Hold.Ratio, 'f', 2, 64)
		}
		primary := p.Primary + "/" + bucketOf(p.PrimaryBucket, p.Names[p.Primary])
		if p.Primary == "" {
			primary = fmt.Sprintf("spread over %d", len(p.Legs))
		}
		source := "-"
		if p.Source != "" {
			source = p.Source + "/" + bucketOf(p.SourceBucket, p.Names[p.Source])
		} else if p.Target != "" {
			source = "(target " + p.Target + "/" + p.Names[p.Target] + ")"
		}
		writes := "-"
		if w, s := p.Writes["primary"], p.Writes["source"]; w+s > 0 {
			writes = fmt.Sprintf("%.0f/%.0f (%.0f%%)", w, s, 100*w/(w+s))
		}
		mover := "-"
		if m := p.Mover; m != nil {
			mover = fmt.Sprintf("pass %d: %d copied, %d already there, %d failed", m.Pass, m.Copied, m.Skipped, m.Failed)
			if m.Converged {
				mover += ", converged"
			}
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%.0f\t%s\n", shown(p.Key), p.State, ratio, primary, source, writes, p.FallbackReads, mover)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for i := range st.Placements {
		if err := printLegs(out, &st.Placements[i]); err != nil {
			return err
		}
	}
	return nil
}

func bucketOf(named, byCluster string) string {
	if named != "" {
		return named
	}
	return byCluster
}

// printLegs is a spread bucket's legs (ADR-0018): bucket, share of the key space, the hash ranges
// it owns, and which part is moving where.
func printLegs(out io.Writer, p *control.PlacementStatus) error {
	if len(p.Legs) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(out, "\n%s is spread over %d backend buckets:\n", shown(p.Key), len(p.Legs)); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	scope := ""
	if len(p.Scopes) > 0 {
		scope = "SCOPE\t"
	}
	_, _ = fmt.Fprintln(tw, "  "+scope+"LEG\tCLUSTER\tBUCKET\tSHARE\tHASH RANGES\tMOVING")
	legRows(tw, p, "", p.Legs)
	// Each prefix rule's keys, owned by its own table (ADR-0020); the rows above are every other key.
	for _, sc := range p.Scopes {
		legRows(tw, p, sc.Prefix, sc.Legs)
	}
	return tw.Flush()
}

func legRows(tw io.Writer, p *control.PlacementStatus, prefix string, legs []control.LegStatus) {
	scope := ""
	if len(p.Scopes) > 0 {
		scope = "(other keys)\t"
		if prefix != "" {
			scope = prefix + "\t"
		}
	}
	for _, l := range legs {
		ranges := make([]string, 0, len(l.Ranges))
		for _, rg := range l.Ranges {
			ranges = append(ranges, fmt.Sprintf("%016x-%016x", uint64(rg.From), uint64(rg.To)))
		}
		switch {
		case len(ranges) > 0:
		case l.Idle:
			ranges = append(ranges, "none (idle: expand --clear retires it)")
		default:
			ranges = append(ranges, "none here")
		}
		moving := "-"
		if m := p.Move; m != nil && prefix == m.Scope {
			of := "keys"
			if m.Scope != "" {
				of = "its keys"
			}
			rg := fmt.Sprintf("%016x-%016x (%.1f%% of %s)", uint64(m.Range.From), uint64(m.Range.To), 100*m.Share, of)
			switch l.ID {
			case m.From:
				moving = "out to " + m.To + ": " + rg
			case m.To:
				moving = "in from " + m.From + ": " + rg
			}
		}
		_, _ = fmt.Fprintf(tw, "  %s%s\t%s\t%s\t%.1f%%\t%s\t%s\n", scope, l.ID, l.Cluster, l.Bucket, 100*l.Share, strings.Join(ranges, " "), moving)
	}
}

// transition runs one state change as an operation record, polls it, and prints what it did.
func transition(cmd *cobra.Command, api *apiClient, o apiOptions, key, kind string, args any, timeout time.Duration) error {
	placement, err := placementKey(key)
	if err != nil {
		return err
	}
	ctx, cancel := waitContext(cmd, timeout)
	defer cancel()
	var res control.TransitionResult
	if err := api.operate(ctx, control.OperationRequest{Kind: kind, Placement: placement, Args: argsOf(args)}, &res); err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if o.json {
		return printJSON(cmd, res)
	}
	out := cmd.OutOrStdout()
	if res.CreatedBucket != "" {
		_, _ = fmt.Fprintf(out, "created %s on %s\n", res.CreatedBucket, res.Primary)
	}
	_, _ = fmt.Fprintf(out, "%s: %s -> %s (directory version %d)\n", shown(res.Key), res.From, res.To, res.Version)
	if rg := res.Range; rg != nil {
		under := ""
		if res.Scope != "" {
			under = fmt.Sprintf(" under %q (no longer prefix rule claims them)", res.Scope)
		}
		_, _ = fmt.Fprintf(out, "only keys%s whose hash is in %016x-%016x move; the ratio is a share of that range\n", under, uint64(rg.From), uint64(rg.To))
	}
	switch res.To {
	case directory.StateRamping:
		_, _ = fmt.Fprintf(out, "writes for %.0f%% of keys now land on %s; reads fall back to %s\n", 100*res.Ratio, res.Primary, res.Source)
	case directory.StateMigrating:
		_, _ = fmt.Fprintf(out, "all writes now land on %s; run `shunt migrate run %s` to copy what is still on %s\n", res.Primary, shown(res.Key), res.Source)
	case directory.StateCutover:
		_, _ = fmt.Fprintf(out, "%s is no longer read; `shunt purge-source %s` deletes it once you are satisfied\n", res.Source, shown(res.Key))
	case directory.StateActive:
		_, _ = fmt.Fprintf(out, "migration complete: %s is served entirely by %s\n", shown(res.Key), res.Primary)
	}
	printFleet(out, res)
	if res.Warning != "" {
		_, _ = fmt.Fprintf(out, "WARNING %s\n", res.Warning)
	}
	return nil
}

// printFleet says where a change is in effect when this shunt has fleet members (ADR-0016). With
// none, it prints nothing: one proxy has every change the moment it is written.
func printFleet(out io.Writer, res control.TransitionResult) {
	if res.Held {
		_, _ = fmt.Fprintf(out, "the keys this step moves paused their writes until every proxy had it\n")
	}
	switch {
	case len(res.WaitingOn) > 0:
		_, _ = fmt.Fprintf(out, "PENDING: not yet installed on %s. The change is written and each proxy applies it as it catches up;\n"+
			"the next step on this bucket is refused until they all have it (`shunt proxy list`)\n", strings.Join(res.WaitingOn, ", "))
	case res.Proxies > 0:
		_, _ = fmt.Fprintf(out, "in effect on every live proxy (this one and %d member(s))\n", res.Proxies)
	}
	if len(res.Silent) > 0 {
		_, _ = fmt.Fprintf(out, "silent, not waited for: %s. A silent proxy refuses writes to moving buckets on its own and installs\n"+
			"the change when it is back; one that is gone for good: `shunt proxy forget <id>`\n", strings.Join(res.Silent, ", "))
	}
}

// forEach runs fn for the named bucket, or with --from for every bucket that cluster serves (ACTIVE)
// or is moving off (every later state). A bucket already past the step fails on its own and the
// rest continue, so the command can be repeated after a partial failure.
func forEach(cmd *cobra.Command, o apiOptions, args []string, from string, fn func(api *apiClient, key string) error) error {
	api, err := o.client()
	if err != nil {
		return err
	}
	switch {
	case len(args) == 1 && from != "":
		return errors.New("give a bucket or --from, not both")
	case len(args) == 1:
		return fn(api, args[0])
	case from == "":
		return errors.New("give a bucket, or --from <cluster> for every bucket on one cluster")
	}
	var st control.Status
	if err := api.call(cmd.Context(), "GET", "/v1/status?all=1", nil, &st); err != nil {
		return err
	}
	var keys []string
	for i := range st.Placements {
		p := &st.Placements[i]
		if (p.State == directory.StateActive && p.Primary == from) || (p.State != directory.StateActive && p.Source == from) {
			keys = append(keys, p.Key)
		}
	}
	if len(keys) == 0 {
		_, err := fmt.Fprintf(cmd.OutOrStdout(), "no bucket on %s needs this step\n", from)
		return err
	}
	if !o.json {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%d buckets on %s\n", len(keys), from)
	}
	var failed []string
	for _, key := range keys {
		if err := fn(api, key); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%v\n", err)
			failed = append(failed, key)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d buckets did not move: %s", len(failed), len(keys), strings.Join(failed, ", "))
	}
	return nil
}

// reportLeftovers says what still points at an evacuated cluster. Moving every bucket off it is not
// the same as being rid of it: shunt cluster remove refuses while anything references it.
func reportLeftovers(cmd *cobra.Command, o apiOptions, cluster string) error {
	api, err := o.client()
	if err != nil {
		return err
	}
	var st control.Status
	if callErr := api.call(context.WithoutCancel(cmd.Context()), "GET", "/v1/status", nil, &st); callErr != nil {
		return callErr
	}
	out := cmd.OutOrStdout()
	for i := range st.Clusters {
		c := &st.Clusters[i]
		if c.Name != cluster {
			continue
		}
		if len(c.References) > 0 {
			_, err = fmt.Fprintf(out, "\nstill pointing at %s: %s. Repoint tenants with `shunt tenant set-default`, then `shunt cluster remove %s`.\n",
				cluster, strings.Join(c.References, ", "), cluster)
			return err
		}
		_, err = fmt.Fprintf(out, "\nnothing references %s any more: `shunt cluster remove %s` takes it out of shunt.\n", cluster, cluster)
		return err
	}
	return nil
}
