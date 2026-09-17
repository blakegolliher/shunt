package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// moverPaths are where a mover run keeps its cursor and ledger, and where it uploads the ledger.
type moverPaths struct {
	cursorDir, ledgerDir, ledgerBucket string
}

func newMigrateRun() *cobra.Command {
	var (
		o              apiOptions
		paths          moverPaths
		from           string
		dryRun, accept bool
		untilConverged bool
		maxPasses      int
	)
	cmd := &cobra.Command{
		Use:   "run <tenant/bucket>",
		Short: "Copy what the source still holds to the new primary: the mover",
		Long: "Runs the mover in this process against the clusters the control API names, resolving their secret_refs\n" +
			"here (the mover host needs the same env: or file: secrets as shunt serve). One pass lists the source and\n" +
			"copies every object the primary lacks, under the guards of ADR-0004. With --until-converged it repeats\n" +
			"passes until one copies nothing, which is what shunt cutover requires; each pass is reported to the API.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 1) == (from != "") {
				return errors.New("give a bucket or --from <cluster>, not both and not neither")
			}
			api, err := o.client()
			if err != nil {
				return err
			}
			keys := args
			if from != "" {
				if keys, err = movingOff(cmd.Context(), api, from); err != nil {
					return err
				}
			}
			dir := &directory.File{Clusters: map[string]config.Cluster{}, Placements: map[string]directory.Placement{}}
			for _, key := range keys {
				path, perr := placementPath(key)
				if perr != nil {
					return perr
				}
				var d control.PlacementDetail
				if callErr := api.call(cmd.Context(), "GET", path, nil, &d); callErr != nil {
					return fmt.Errorf("%s: %w", key, callErr)
				}
				dir.Placements[d.Key] = d.Placement
				maps.Copy(dir.Clusters, d.Clusters)
			}
			one := ""
			if len(args) == 1 {
				one = args[0]
			}
			jobs, err := selectPlacements(dir, one, from, accept)
			if err != nil {
				return err
			}
			if len(jobs) == 0 {
				return errors.New("nothing to move: no MIGRATING placement, or RAMPING at ratio 1, matched")
			}
			return runMover(cmd, api, jobs, paths, dryRun, untilConverged, maxPasses)
		},
	}
	addAPIFlags(cmd, &o)
	f := cmd.Flags()
	f.StringVar(&from, "from", "", "instead of one bucket: every placement moving off this cluster")
	f.StringVar(&paths.cursorDir, "cursor-dir", ".", "where the resumable cursor of each placement is kept")
	f.StringVar(&paths.ledgerDir, "ledger-dir", ".", "where the JSONL ledger of each placement is appended")
	f.StringVar(&paths.ledgerBucket, "ledger-bucket", "", "backend bucket on the target cluster to upload each pass's ledger to")
	f.BoolVar(&dryRun, "dry-run", false, "list what would be copied and stop")
	f.BoolVar(&accept, migrate.AcceptLostWriteWindowFlag, false,
		"copy into a target that ignores If-None-Match: * on PUT, accepting that a client write can be overwritten (docs/migrating.md)")
	f.BoolVar(&untilConverged, "until-converged", false, "repeat passes until one copies nothing and fails nothing")
	f.IntVar(&maxPasses, "max-passes", 10, "with --until-converged, give up after this many passes")
	return cmd
}

// movingOff lists the placements moving off cluster.
func movingOff(ctx context.Context, api *apiClient, cluster string) ([]string, error) {
	var st control.Status
	if err := api.call(ctx, "GET", "/v1/status?all=1", nil, &st); err != nil {
		return nil, err
	}
	var keys []string
	for i := range st.Placements {
		p := &st.Placements[i]
		if p.Source == cluster && (p.State == directory.StateMigrating || p.State == directory.StateRamping) {
			keys = append(keys, p.Key)
		}
	}
	return keys, nil
}

// runMover runs passes over every job, reporting progress to the control API. It fails if an
// object failed or changed ETag, or if --until-converged ran out of passes.
func runMover(cmd *cobra.Command, api *apiClient, jobs []job, paths moverPaths, dryRun, untilConverged bool, maxPasses int) error {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()
	for _, dir := range []string{paths.cursorDir, paths.ledgerDir} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	if !untilConverged || dryRun {
		maxPasses = 1
	}
	total := stats{}
	unconverged := 0
	for i := range jobs {
		j := jobs[i]
		key := j.tenant + "/" + j.client
		_, _ = fmt.Fprintf(out, "== %s: %s/%s → %s/%s (%s guard, %s withdrawal)\n", key,
			j.src.name, j.src.bucket, j.dst.name, j.dst.bucket, guardName(j.conditional), withdrawName(j.condDelete))
		if !j.conditional {
			_, _ = fmt.Fprintf(out, "   WARNING (accepted with --%s): %s\n", migrate.AcceptLostWriteWindowFlag, migrate.LostWriteWindow(key, j.dst.name))
		}
		if err := checkSide(cmd.Context(), "source", j.src); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if err := checkSide(cmd.Context(), "target", j.dst); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		converged := false
		for pass := 1; pass <= maxPasses; pass++ {
			if maxPasses > 1 {
				_, _ = fmt.Fprintf(out, "   pass %d\n", pass)
			}
			report := func(s stats, lastKey string, done bool) {
				p := control.Progress{Source: j.src.name, Primary: j.dst.name, Pass: pass, Copied: s.copied, Skipped: s.skipped,
					Vanished: s.vanished, Failed: s.failed + s.drifted, Bytes: s.bytes, LastKey: lastKey, Done: done,
					Converged: done && s.copied == 0 && s.failed == 0 && s.drifted == 0}
				path, _ := placementPath(key)
				if err := api.call(context.WithoutCancel(cmd.Context()), "POST", path+"/mover-progress", p, nil); err != nil {
					_, _ = fmt.Fprintf(errOut, "   progress report to the control API failed: %v\n", err)
				}
			}
			s, err := move(cmd.Context(), j, paths, dryRun, out, errOut, report)
			total.copied += s.copied
			total.skipped += s.skipped
			total.vanished += s.vanished
			total.failed += s.failed
			total.drifted += s.drifted
			total.bytes += s.bytes
			if err != nil {
				_, _ = fmt.Fprintf(errOut, "mover: %s: %v\n", key, err)
				total.failed++
				break
			}
			if s.copied == 0 && s.failed == 0 && s.drifted == 0 {
				converged = true
				break
			}
		}
		if untilConverged && !dryRun && !converged {
			unconverged++
		}
		if untilConverged && converged {
			_, _ = fmt.Fprintf(out, "   converged: the last pass copied nothing\n")
		}
	}
	_, _ = fmt.Fprintf(out, "\nmover: %d copied, %d already on the target, %d vanished mid-copy, %d failed, %s moved\n",
		total.copied, total.skipped, total.vanished, total.failed, humanBytes(total.bytes))
	switch {
	case total.drifted > 0:
		return fmt.Errorf("%d objects changed ETag across the move", total.drifted)
	case total.failed > 0:
		return fmt.Errorf("%d objects failed to copy", total.failed)
	case unconverged > 0:
		return fmt.Errorf("%d placements did not converge within %d passes", unconverged, maxPasses)
	}
	return nil
}
