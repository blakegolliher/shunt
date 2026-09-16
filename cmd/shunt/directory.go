package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// emptySHA256 is the hex SHA-256 of an empty body.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func newDirectory() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "directory",
		Short: "Inspect and change tenant placements in the directory file",
	}
	cmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/shunt/shunt.yaml", "config file naming the directory file and its clusters")
	cmd.AddCommand(newDirectoryGet(&cfgPath), newDirectorySetState(&cfgPath), newDirectoryValidate(&cfgPath), newDirectorySetDefault(&cfgPath))
	return cmd
}

func loadForDirectory(cfgPath string) (*config.Config, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	if cfg.Directory.File == "" {
		return nil, fmt.Errorf("%s: directory.file is not set (directories are used in resign mode)", cfgPath)
	}
	return cfg, nil
}

func newDirectoryGet(cfgPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "get [tenant/bucket]",
		Short: "Print the directory, or one placement",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadForDirectory(*cfgPath)
			if err != nil {
				return err
			}
			f, err := directory.Load(cfg.Directory.File)
			if err != nil {
				return err
			}
			var v any = f
			if len(args) == 1 {
				p, ok := f.Placements[args[0]]
				if !ok {
					return fmt.Errorf("%s: %w", args[0], directory.ErrNotFound)
				}
				v = p
			}
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(v)
			}
			b, err := yaml.Dump(v, yaml.WithIndent(2))
			if err != nil {
				return err
			}
			_, err = out.Write(b)
			return err
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

func newDirectorySetDefault(cfgPath *string) *cobra.Command {
	var actor string
	cmd := &cobra.Command{
		Use:   "set-default <tenant> <cluster>",
		Short: "Choose the cluster a tenant's new buckets are created on",
		Long: "Existing buckets do not move: this only decides where the tenant's next CreateBucket lands.\n" +
			"Repoint every tenant that still defaults to a cluster before retiring it.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadForDirectory(*cfgPath)
			if err != nil {
				return err
			}
			d, err := directory.Open(cfg.Directory.File)
			if err != nil {
				return err
			}
			if actor == "" {
				actor = "cli:" + os.Getenv("USER")
			}
			if serr := d.SetTenantDefault(cmd.Context(), args[0], args[1], actor); serr != nil {
				return serr
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: new buckets now land on %s (directory version %d)\n",
				args[0], args[1], d.Snapshot().Version())
			return err
		},
	}
	cmd.Flags().StringVar(&actor, "actor", "", "who is making the change, for the change log (default cli:$USER)")
	return cmd
}

func newDirectoryValidate(cfgPath *string) *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate the directory file (or --file) against the configured clusters",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(*cfgPath)
			if err != nil {
				return err
			}
			if file == "" {
				file = cfg.Directory.File
			}
			if file == "" {
				return errors.New("no directory file: set directory.file or pass --file")
			}
			f, err := directory.Load(file)
			if err != nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: invalid\n%v\n", file, err)
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: ok (version %d, %d tenants, %d placements)\n", file, f.Version, len(f.Tenants), len(f.Placements))
			return err
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "directory file to validate instead of the config's directory.file")
	return cmd
}

func newDirectorySetState(cfgPath *string) *cobra.Command {
	var (
		t     directory.Transition
		actor string
	)
	cmd := &cobra.Command{
		Use:   "set-state <tenant/bucket> <ACTIVE|RAMPING|MIGRATING|CUTOVER>",
		Short: "Move a placement to a new state; illegal transitions and versioned buckets are refused",
		Long: "Moves a placement through ACTIVE → RAMPING → MIGRATING → CUTOVER → ACTIVE (RAMPING may be skipped).\n" +
			"Leaving ACTIVE needs --to (the cluster that becomes primary) and --name (the bucket on it, which must exist).\n" +
			"Entering RAMPING or MIGRATING is refused unless GetBucketVersioning shows versioning was never enabled on\n" +
			"both the source and the primary bucket (docs/DESIGN.md decision 12).",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			tenant, bucket, ok := directory.SplitKey(args[0])
			if !ok {
				return fmt.Errorf("%q: want <tenant>/<bucket>", args[0])
			}
			t.To = strings.ToUpper(args[1])
			cfg, err := loadForDirectory(*cfgPath)
			if err != nil {
				return err
			}
			d, err := directory.Open(cfg.Directory.File)
			if err != nil {
				return err
			}
			p, ok := d.Snapshot().Lookup(tenant, bucket)
			if !ok {
				return fmt.Errorf("%s: %w", args[0], directory.ErrNotFound)
			}
			np, err := directory.Apply(*p, t)
			if err != nil {
				return err
			}
			if np.State == directory.StateRamping || np.State == directory.StateMigrating {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				for _, side := range []struct{ role, cluster string }{{"source", np.Source}, {"primary", np.Primary}} {
					if verr := refuseVersioned(ctx, d.Snapshot().File().Clusters, side.role, side.cluster, np.Names[side.cluster]); verr != nil {
						return verr
					}
				}
			}
			if actor == "" {
				actor = "cli:" + os.Getenv("USER")
			}
			if serr := d.SetState(cmd.Context(), tenant, bucket, p.State, t, actor); serr != nil {
				return serr
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s -> %s (directory version %d)\n", args[0], p.State, t.To, d.Snapshot().Version())
			return err
		},
	}
	cmd.Flags().StringVar(&t.Target, "to", "", "leaving ACTIVE: the cluster that becomes primary")
	cmd.Flags().StringVar(&t.Name, "name", "", "leaving ACTIVE: the backend bucket name on --to")
	cmd.Flags().Float64Var(&t.Ratio, "ratio", 0, "RAMPING: fraction of keys (by hash) whose writes go to the new primary; only grows")
	cmd.Flags().StringArrayVar(&t.Prefixes, "prefix", nil, "RAMPING: key prefix whose writes go to the new primary (repeatable); only grows")
	cmd.Flags().StringVar(&actor, "actor", "", "who is making the change, for the change log (default cli:$USER)")
	return cmd
}

// refuseVersioned fails unless the bucket's versioning status is empty: a bucket that was ever
// versioned (Enabled or Suspended) holds version history the migration would not carry. Every
// error, including a missing bucket or denied access, also refuses: the check fails closed.
func refuseVersioned(ctx context.Context, clusters map[string]config.Cluster, role, clusterName, bucket string) error {
	cc, ok := clusters[clusterName]
	if !ok {
		return fmt.Errorf("refused: %s cluster %q is not configured", role, clusterName)
	}
	cl, err := upstream.New(clusterName, cc, upstream.Options{})
	if err != nil {
		return fmt.Errorf("refused: %s cluster %s: %w", role, clusterName, err)
	}
	defer cl.Close()
	secret, err := config.ResolveSecret(cc.Credentials.SecretRef)
	if err != nil {
		return fmt.Errorf("refused: %s cluster %s: %w", role, clusterName, err)
	}
	status, err := bucketVersioning(ctx, cl, sigv4.Credentials{AccessKey: cc.Credentials.AccessKey, Secret: secret}, bucket)
	switch {
	case err != nil:
		return fmt.Errorf("refused: cannot read versioning of %s bucket %s on %s: %w", role, bucket, clusterName, err)
	case status != "":
		return fmt.Errorf("refused: %s bucket %s on %s has versioning %s; versioned buckets cannot be migrated in v1 (docs/DESIGN.md §9 item 4)", role, bucket, clusterName, status)
	}
	return nil
}

// bucketVersioning returns the <Status> of GetBucketVersioning: "", "Enabled", or "Suspended".
func bucketVersioning(ctx context.Context, cl *upstream.Cluster, creds sigv4.Credentials, bucket string) (string, error) {
	endpoint := cl.Next()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cl.Scheme+"://"+endpoint+"/"+bucket+"?versioning", http.NoBody) //nolint:gosec // G704: configured cluster endpoint
	if err != nil {
		return "", err
	}
	req.Host = endpoint
	sigv4.Sign(req, creds, cl.Region, emptySHA256, time.Now())
	resp, err := cl.Transport.RoundTrip(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Code string `xml:"Code"`
		}
		_ = xml.Unmarshal(body, &e)
		return "", fmt.Errorf("HTTP %d %s", resp.StatusCode, e.Code)
	}
	var v struct {
		Status string `xml:"Status"`
	}
	if err := xml.Unmarshal(body, &v); err != nil {
		return "", fmt.Errorf("unreadable GetBucketVersioning response: %w", err)
	}
	return v.Status, nil
}
