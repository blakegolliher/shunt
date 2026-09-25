package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/mover"
)

// moverPaths are where a mover run keeps its cursor and ledger, and where it uploads the ledger.
type moverPaths struct {
	cursorDir, ledgerDir, ledgerBucket string
}

func humanBytes(n int64) string { return mover.HumanBytes(n) }

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
		Use:   "run <bucket>",
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
			dir := &directory.File{Clusters: map[string]config.Cluster{}, Placements: map[string]directory.Placement{}, Generations: map[string]int64{}}
			secrets := map[string]string{}
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
				if dir.Identity.IsZero() {
					dir.Identity = d.Identity
				} else if dir.Identity != d.Identity {
					return fmt.Errorf("%s: the control API changed directory lineage while the mover was preparing", key)
				}
				dir.Generations[directory.PlacementResource(d.Key)] = d.Generation
				maps.Copy(dir.Clusters, d.Clusters)
				maps.Copy(secrets, d.Secrets)
			}
			one := ""
			if len(args) == 1 {
				if one, err = placementKey(args[0]); err != nil {
					return err
				}
			}
			return runMover(cmd, api, dir, secrets, one, from, paths, dryRun, untilConverged, maxPasses, accept)
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
func runMover(cmd *cobra.Command, api *apiClient, dir *directory.File, secrets map[string]string, one, from string,
	paths moverPaths, dryRun, untilConverged bool, maxPasses int, accept bool,
) error {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()
	var sessions []externalMoverSession
	if !dryRun {
		keys := []string{one}
		if one == "" {
			keys = make([]string, 0, len(dir.Placements))
			for key := range dir.Placements {
				keys = append(keys, key)
			}
		}
		for _, key := range keys {
			session, err := startExternalMover(ctx, api, dir, key, untilConverged, maxPasses, accept)
			if err != nil {
				return err
			}
			sessions = append(sessions, session)
			if err := heartbeatMovers(ctx, api, sessions[len(sessions)-1:], false, ""); err != nil {
				return err
			}
		}
	}

	finish := make(chan string)
	leaseDone := make(chan error, 1)
	if len(sessions) > 0 {
		go func() {
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			var leaseErr error
			for {
				select {
				case message := <-finish:
					finalCtx, finalCancel := context.WithTimeout(context.WithoutCancel(cmd.Context()), 5*time.Second)
					err := heartbeatMovers(finalCtx, api, sessions, true, message)
					finalCancel()
					leaseDone <- errors.Join(leaseErr, err)
					return
				case <-tick.C:
					if leaseErr == nil {
						if err := heartbeatMovers(ctx, api, sessions, false, ""); err != nil {
							leaseErr = err
							cancel() // a mover without a current authority lease starts no more backend work
						}
					}
				}
			}
		}()
	}

	_, err := mover.Run(ctx, dir, secrets, mover.Options{
		Key: one, From: from, AcceptLostWriteWindow: accept, DryRun: dryRun,
		UntilConverged: untilConverged, MaxPasses: maxPasses,
		Paths:  mover.Paths{CursorDir: paths.cursorDir, LedgerDir: paths.ledgerDir, LedgerBucket: paths.ledgerBucket},
		Out:    cmd.OutOrStdout(),
		ErrOut: cmd.ErrOrStderr(),
	}, func(p mover.Progress) {
		progress := control.Progress{Source: p.Source, Primary: p.Primary, Pass: p.Pass, Copied: p.Copied,
			Skipped: p.Skipped, Vanished: p.Vanished, Failed: p.Failed, Bytes: p.Bytes,
			LastKey: p.LastKey, Done: p.Done, Converged: p.Converged}
		path, _ := placementPath(p.Key)
		if callErr := api.call(context.WithoutCancel(cmd.Context()), "POST", path+"/mover-progress", progress, nil); callErr != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "   progress report to the control API failed: %v\n", callErr)
		}
	})
	if len(sessions) == 0 {
		return err
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	finish <- message
	leaseErr := <-leaseDone
	if err != nil || leaseErr != nil {
		return errors.Join(err, leaseErr)
	}
	waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(cmd.Context()), 5*time.Second)
	waitErr := waitExternalMovers(waitCtx, api, sessions)
	waitCancel()
	return waitErr
}

type externalMoverSession struct {
	key, operation, session string
	identity                directory.Identity
	generation              int64
	sequence                int64
}

func startExternalMover(ctx context.Context, api *apiClient, dir *directory.File, key string, until bool, maxPasses int, accept bool) (externalMoverSession, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return externalMoverSession{}, err
	}
	session := hex.EncodeToString(raw[:])
	args := control.MoverRequest{UntilConverged: until, MaxPasses: maxPasses, AcceptLostWriteWindow: accept,
		External: true, Session: session, Wait: "0s"}
	var op control.Operation
	err := api.call(ctx, http.MethodPost, "/v1/operations", control.OperationRequest{Kind: control.OpMover, Placement: key, Args: argsOf(args)}, &op)
	if err != nil {
		return externalMoverSession{}, err
	}
	return externalMoverSession{key: key, operation: op.ID, session: session, identity: dir.Identity,
		generation: dir.Generation(directory.PlacementResource(key))}, nil
}

func heartbeatMovers(ctx context.Context, api *apiClient, sessions []externalMoverSession, complete bool, message string) error {
	for i := range sessions {
		s := &sessions[i]
		s.sequence++
		req := control.WorkerHeartbeat{Session: s.session, Identity: s.identity, Generation: s.generation,
			Sequence: s.sequence, Inflight: 1, Complete: complete, Error: message}
		if complete {
			req.Inflight = 0
			if message != "" {
				req.Complete = false
				req.Uncertain = 1 // an error after dispatch is not proof the backend did nothing
			}
		}
		var answer control.WorkerHeartbeatAnswer
		if err := api.call(ctx, http.MethodPost, "/v1/operations/"+s.operation+"/worker-heartbeat", req, &answer); err != nil {
			return fmt.Errorf("worker session %s: %w", s.session, err)
		}
		if answer.Hold {
			return fmt.Errorf("worker session %s: the placement entered a source hold", s.session)
		}
	}
	return nil
}

func waitExternalMovers(ctx context.Context, api *apiClient, sessions []externalMoverSession) error {
	for _, session := range sessions {
		for {
			var op control.Operation
			if err := api.call(ctx, http.MethodGet, "/v1/operations/"+session.operation, nil, &op); err != nil {
				return err
			}
			if op.Terminal() {
				if op.Status != control.StatusSucceeded {
					if op.Error != nil {
						return fmt.Errorf("worker operation %s: %s", op.ID, op.Error.Message)
					}
					return fmt.Errorf("worker operation %s ended %s", op.ID, op.Status)
				}
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("worker operation %s did not finish: %w", op.ID, ctx.Err())
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	return nil
}
