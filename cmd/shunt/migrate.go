package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// The migration verbs of docs/DESIGN.md §2.10. Each one is a guarded wrapper over the same state
// machine `directory set-state` uses: they exist so an operator states an intent ("ramp to a
// quarter", "cut over") instead of a state name, and so the target bucket can be created in the
// same step. The guards live in the directory package, not here.

func newRamp() *cobra.Command {
	var (
		cfgPath  string
		to, name string
		from     string
		create   bool
		ratio    float64
		prefixes []string
		actor    string
	)
	cmd := &cobra.Command{
		Use:   "ramp <tenant/bucket>",
		Short: "Send a growing share of a bucket's writes to a new cluster",
		Long: "Moves the placement into RAMPING, or raises an existing ramp. --ratio is the fraction of keys,\n" +
			"chosen by a stable hash of the key, whose writes go to the new primary; --prefix names key\n" +
			"prefixes that go there whatever the ratio. A ramp only grows: shrinking needs a reconcile.\n" +
			"Reads still fall back to the source, so a key written on either side is readable throughout.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if ratio == 0 && len(prefixes) == 0 {
				return fmt.Errorf("give --ratio, --prefix, or both")
			}
			if ratio < 0 || ratio > 1 {
				return fmt.Errorf("--ratio %v is outside 0..1", ratio)
			}
			return transitionAll(cmd, cfgPath, args, from, directory.Transition{
				To: directory.StateRamping, Target: to, Name: name, Ratio: ratio, Prefixes: prefixes,
			}, create, actor)
		},
	}
	f := cmd.Flags()
	f.StringVarP(&cfgPath, "config", "c", "/etc/shunt/shunt.yaml", "config file naming the directory file and its clusters")
	f.StringVar(&to, "to", "", "the cluster that becomes primary (required on the first step)")
	f.StringVar(&from, "from", "", "instead of one bucket: every bucket currently served by this cluster")
	f.StringVar(&name, "name", "", "the backend bucket name on --to (default: the generated name)")
	f.BoolVar(&create, "create", false, "create the backend bucket on --to if it does not exist")
	f.Float64Var(&ratio, "ratio", 0, "fraction of keys, by hash, whose writes go to the new primary")
	f.StringArrayVar(&prefixes, "prefix", nil, "key prefix whose writes go to the new primary (repeatable)")
	f.StringVar(&actor, "actor", "", "who is making the change, for the change log (default cli:$USER)")
	return cmd
}

func newMigrate() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Move a bucket's data to another cluster while clients keep reading and writing",
	}
	cmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/shunt/shunt.yaml", "config file naming the directory file and its clusters")
	cmd.AddCommand(newMigrateStart(&cfgPath), newMigrateFinish(&cfgPath), newMigrateStatus(&cfgPath))
	return cmd
}

func newMigrateStart(cfgPath *string) *cobra.Command {
	var (
		to, name string
		from     string
		create   bool
		actor    string
	)
	cmd := &cobra.Command{
		Use:   "start <tenant/bucket>",
		Short: "Send all new writes to the new cluster and start serving reads from both",
		Long: "Moves the placement into MIGRATING. From here every write lands on the new primary, reads fall\n" +
			"back to the source when the primary does not have the object, deletes go to both, and listings\n" +
			"are merged. That is the state the mover copies in: it is safe to run then, and only then.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return transitionAll(cmd, *cfgPath, args, from, directory.Transition{
				To: directory.StateMigrating, Target: to, Name: name,
			}, create, actor)
		},
	}
	f := cmd.Flags()
	f.StringVar(&to, "to", "", "the cluster that becomes primary (required unless already RAMPING)")
	f.StringVar(&from, "from", "", "instead of one bucket: every bucket currently served by this cluster (how a vendor is evacuated)")
	f.StringVar(&name, "name", "", "the backend bucket name on --to (default: the generated name)")
	f.BoolVar(&create, "create", false, "create the backend bucket on --to if it does not exist")
	f.StringVar(&actor, "actor", "", "who is making the change, for the change log (default cli:$USER)")
	return cmd
}

func newMigrateFinish(cfgPath *string) *cobra.Command {
	var actor, from string
	cmd := &cobra.Command{
		Use:   "finish <tenant/bucket>",
		Short: "Drop the source cluster: the migration is done",
		Long: "Moves a CUTOVER placement back to ACTIVE and forgets the source. After this shunt never reads or\n" +
			"deletes on the old cluster again, so run it only once the source bucket is known to be drained:\n" +
			"`shunt migrate status` shows whether any read still fell back.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return transitionAll(cmd, *cfgPath, args, from, directory.Transition{To: directory.StateActive}, false, actor)
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "instead of one bucket: every bucket that has cut over from this cluster")
	cmd.Flags().StringVar(&actor, "actor", "", "who is making the change, for the change log (default cli:$USER)")
	return cmd
}

func newCutover() *cobra.Command {
	var cfgPath, actor, from string
	cmd := &cobra.Command{
		Use:   "cutover <tenant/bucket>",
		Short: "Stop using the source cluster for a migrating bucket",
		Long: "Moves the placement into CUTOVER: reads stop falling back, deletes stop being doubled, listings\n" +
			"stop being merged. Anything still only on the source becomes invisible, so cut over after the\n" +
			"mover reports convergence and shunt_migration_fallback_reads_total has stopped increasing.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return transitionAll(cmd, cfgPath, args, from, directory.Transition{To: directory.StateCutover}, false, actor)
		},
	}
	cmd.Flags().StringVarP(&cfgPath, "config", "c", "/etc/shunt/shunt.yaml", "config file naming the directory file and its clusters")
	cmd.Flags().StringVar(&from, "from", "", "instead of one bucket: every bucket still migrating off this cluster")
	cmd.Flags().StringVar(&actor, "actor", "", "who is making the change, for the change log (default cli:$USER)")
	return cmd
}

func newMigrateStatus(cfgPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status [tenant/bucket]",
		Short: "Show which buckets are moving, and where they are",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadForDirectory(*cfgPath)
			if err != nil {
				return err
			}
			f, err := directory.Load(cfg.Directory.File, cfg.Clusters)
			if err != nil {
				return err
			}
			keys := make([]string, 0, len(f.Placements))
			for k := range f.Placements {
				if len(args) == 1 && k != args[0] {
					continue
				}
				if len(args) == 0 && f.Placements[k].State == directory.StateActive {
					continue
				}
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := cmd.OutOrStdout()
			if len(keys) == 0 {
				_, err = fmt.Fprintln(out, "no bucket is moving")
				return err
			}
			_, _ = fmt.Fprintf(out, "%-28s %-10s %-24s %-24s %s\n", "BUCKET", "STATE", "PRIMARY", "SOURCE", "RAMP")
			for _, k := range keys {
				p := f.Placements[k]
				ramp := "-"
				if p.Ramp != nil {
					ramp = fmt.Sprintf("ratio %.2f", p.Ramp.Ratio)
					if len(p.Ramp.Prefixes) > 0 {
						ramp += " prefixes " + strings.Join(p.Ramp.Prefixes, ",")
					}
				}
				source := "-"
				if p.Source != "" {
					source = p.Source + "/" + p.Names[p.Source]
				}
				_, _ = fmt.Fprintf(out, "%-28s %-10s %-24s %-24s %s\n", k, p.State,
					p.Primary+"/"+p.Names[p.Primary], source, ramp)
			}
			_, err = fmt.Fprintf(out, "\nWatch shunt_migration_fallback_reads_total and shunt_ramp_writes_total on the metrics listener.\n")
			return err
		},
	}
	return cmd
}

// transitionAll applies one state change to a named bucket, or to every bucket a cluster still
// holds. The second form is how a vendor leaves the estate: one command per step for the whole
// cluster, not one per bucket. A bucket that is already past the step is reported and skipped, so
// the command can be repeated after a partial failure.
func transitionAll(cmd *cobra.Command, cfgPath string, args []string, from string, t directory.Transition, create bool, actor string) error {
	switch {
	case len(args) == 1 && from != "":
		return errors.New("give a bucket or --from, not both")
	case len(args) == 1:
		return transition(cmd, cfgPath, args[0], t, create, actor)
	case from == "":
		return errors.New("give a bucket, or --from <cluster> for every bucket on one cluster")
	}
	cfg, err := loadForDirectory(cfgPath)
	if err != nil {
		return err
	}
	f, err := directory.Load(cfg.Directory.File, cfg.Clusters)
	if err != nil {
		return err
	}
	if _, ok := cfg.Clusters[from]; !ok {
		return fmt.Errorf("cluster %q is not configured", from)
	}
	keys := make([]string, 0, len(f.Placements))
	for k := range f.Placements {
		// Leaving ACTIVE selects what the cluster still serves; every later step selects what is
		// already moving off it.
		p := f.Placements[k]
		if (p.State == directory.StateActive && p.Primary == from) || (p.State != directory.StateActive && p.Source == from) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "no bucket on %s needs this step\n", from)
		return err
	}
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "%d buckets on %s\n", len(keys), from)
	var failed []string
	for _, key := range keys {
		if err := transition(cmd, cfgPath, key, t, create, actor); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: %v\n", key, err)
			failed = append(failed, key)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d buckets did not move: %s", len(failed), len(keys), strings.Join(failed, ", "))
	}
	if t.To == directory.StateActive {
		reportLeftovers(cmd, cfg, f, from)
	}
	return nil
}

// reportLeftovers says what still points at an evacuated cluster. Moving every bucket off it is not
// the same as being rid of it: a tenant that still defaults there would put its next new bucket
// back on the cluster the operator is about to switch off.
func reportLeftovers(cmd *cobra.Command, cfg *config.Config, f *directory.File, cluster string) {
	out := cmd.OutOrStdout()
	var tenants []string
	for name, tn := range f.Tenants {
		if tn.DefaultCluster == cluster {
			tenants = append(tenants, name)
		}
	}
	sort.Strings(tenants)
	if len(tenants) > 0 {
		_, _ = fmt.Fprintf(out, "\nstill pointing at %s: tenants %s default there, so their next new bucket would land on it.\n",
			cluster, strings.Join(tenants, ", "))
		_, _ = fmt.Fprintf(out, "Repoint them in %s before removing the cluster from the config.\n", cfg.Directory.File)
		return
	}
	_, _ = fmt.Fprintf(out, "\nnothing in the directory names %s any more: remove its block from the config and run `shunt check-config`.\n", cluster)
}

// transition applies one state change, resolving and optionally creating the target bucket first.
func transition(cmd *cobra.Command, cfgPath, key string, t directory.Transition, create bool, actor string) error {
	tenant, bucket, ok := directory.SplitKey(key)
	if !ok {
		return fmt.Errorf("%q: want <tenant>/<bucket>", key)
	}
	cfg, err := loadForDirectory(cfgPath)
	if err != nil {
		return err
	}
	d, err := directory.Open(cfg.Directory.File, cfg.Clusters)
	if err != nil {
		return err
	}
	p, ok := d.Snapshot().Lookup(tenant, bucket)
	if !ok {
		return fmt.Errorf("%s: %w", key, directory.ErrNotFound)
	}
	switch {
	case p.State == directory.StateActive && t.Target != "" && t.Name == "":
		t.Name = directory.BackendName(tenant, bucket, 0)
	case p.State != directory.StateActive && t.Target != "":
		// Repeating --to on a later ramp step is how an operator naturally types it: accept it when
		// it names the target already chosen, and refuse only a change of destination mid-move.
		if t.Target != p.Primary {
			return fmt.Errorf("%s is already moving to %s; a different target needs a reconcile, not a ramp step", key, p.Primary)
		}
		if t.Name != "" && t.Name != p.Names[p.Primary] {
			return fmt.Errorf("%s is already moving to %s/%s; --name cannot change mid-move", key, p.Primary, p.Names[p.Primary])
		}
		t.Target, t.Name = "", ""
	}
	np, err := directory.Apply(*p, t)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
	defer cancel()

	// The target bucket has to exist before any write is routed to it, and versioning on either
	// side is refused before the state changes, not after (docs/DESIGN.md §9 item 4).
	if p.State == directory.StateActive {
		target, tname := np.Primary, np.Names[np.Primary]
		switch made, berr := ensureBucket(ctx, cfg, target, tname, create); {
		case berr != nil:
			return berr
		case made:
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "created %s on %s\n", tname, target)
		}
	}
	if np.State == directory.StateRamping || np.State == directory.StateMigrating {
		for _, side := range []struct{ role, cluster string }{{"source", np.Source}, {"primary", np.Primary}} {
			if verr := refuseVersioned(ctx, cfg, side.role, side.cluster, np.Names[side.cluster]); verr != nil {
				return verr
			}
		}
	}
	if actor == "" {
		actor = "cli:" + os.Getenv("USER")
	}
	if err := d.SetState(cmd.Context(), tenant, bucket, p.State, t, actor); err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "%s: %s -> %s (directory version %d)\n", key, p.State, np.State, d.Snapshot().Version())
	switch np.State {
	case directory.StateRamping:
		_, _ = fmt.Fprintf(out, "writes for %s of keys now land on %s; reads still fall back to %s\n",
			rampShare(np.Ramp), np.Primary, np.Source)
	case directory.StateMigrating:
		_, _ = fmt.Fprintf(out, "all writes now land on %s; run the mover to copy what is still on %s\n", np.Primary, np.Source)
	case directory.StateCutover:
		_, _ = fmt.Fprintf(out, "%s is no longer read; run `shunt migrate finish %s` to drop it for good\n", np.Source, key)
	case directory.StateActive:
		_, _ = fmt.Fprintf(out, "migration complete: %s is served entirely by %s\n", key, np.Primary)
	}
	return nil
}

func rampShare(r *directory.Ramp) string {
	switch {
	case r == nil:
		return "no"
	case r.Ratio > 0 && len(r.Prefixes) > 0:
		return fmt.Sprintf("%.0f%% (plus %d prefixes)", r.Ratio*100, len(r.Prefixes))
	case r.Ratio > 0:
		return fmt.Sprintf("%.0f%%", r.Ratio*100)
	}
	return fmt.Sprintf("%d prefixes", len(r.Prefixes))
}

// ensureBucket checks that the backend bucket exists on a cluster, creating it when asked. A
// bucket owned by someone else is an error either way: shunt will not migrate into it.
func ensureBucket(ctx context.Context, cfg *config.Config, clusterName, bucket string, create bool) (bool, error) {
	cc, ok := cfg.Clusters[clusterName]
	if !ok {
		return false, fmt.Errorf("cluster %q is not configured", clusterName)
	}
	cl, err := upstream.New(clusterName, cc, upstream.Options{})
	if err != nil {
		return false, fmt.Errorf("cluster %s: %w", clusterName, err)
	}
	defer cl.Close()
	secret, err := config.ResolveSecret(cc.Credentials.SecretRef)
	if err != nil {
		return false, fmt.Errorf("cluster %s: %w", clusterName, err)
	}
	creds := sigv4.Credentials{AccessKey: cc.Credentials.AccessKey, Secret: secret}

	status, _, err := bucketCall(ctx, cl, creds, http.MethodHead, bucket)
	switch {
	case err != nil:
		return false, fmt.Errorf("cannot reach %s to check bucket %s: %w", clusterName, bucket, err)
	case status == http.StatusOK:
		return false, nil
	case status != http.StatusNotFound:
		return false, fmt.Errorf("%s on %s: HEAD returned HTTP %d", bucket, clusterName, status)
	case !create:
		return false, fmt.Errorf("bucket %s does not exist on %s; create it there or pass --create", bucket, clusterName)
	}
	status, code, err := bucketCall(ctx, cl, creds, http.MethodPut, bucket)
	switch {
	case err != nil:
		return false, fmt.Errorf("creating %s on %s: %w", bucket, clusterName, err)
	case status == http.StatusOK, status == http.StatusNoContent:
		return true, nil
	case code == "BucketAlreadyOwnedByYou":
		return false, nil
	}
	return false, fmt.Errorf("creating %s on %s: HTTP %d %s", bucket, clusterName, status, code)
}

// bucketCall sends one bucket-level request with the cluster's own credentials and returns its
// status and error code.
func bucketCall(ctx context.Context, cl *upstream.Cluster, creds sigv4.Credentials, method, bucket string) (status int, code string, err error) {
	endpoint := cl.Next()
	req, err := http.NewRequestWithContext(ctx, method, cl.Scheme+"://"+endpoint+"/"+bucket, http.NoBody) //nolint:gosec // G704: configured cluster endpoint
	if err != nil {
		return 0, "", err
	}
	req.Host = endpoint
	sigv4.Sign(req, creds, cl.Region, emptySHA256, time.Now())
	resp, err := cl.Transport.RoundTrip(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return resp.StatusCode, "", err
	}
	var e struct {
		Code string `xml:"Code"`
	}
	_ = xml.Unmarshal(body, &e) //nolint:errcheck // a non-XML body just means no code
	return resp.StatusCode, e.Code, nil
}
