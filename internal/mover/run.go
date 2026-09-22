package mover

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// Paths are the local durable cursor and append-only ledger locations, plus the optional
// backend bucket to which each completed pass uploads its ledger.
type Paths struct {
	CursorDir    string
	LedgerDir    string
	LedgerBucket string
}

// Options selects placements and controls how many passes the engine makes.
type Options struct {
	Key                   string
	From                  string
	AcceptLostWriteWindow bool
	DryRun                bool
	UntilConverged        bool
	MaxPasses             int
	Paths                 Paths
	Out                   io.Writer
	ErrOut                io.Writer
}

// Progress is one mover pass's cumulative bucket-level report. It deliberately contains no
// per-object state; the object-level record remains in the append-only ledger.
type Progress struct {
	Key       string
	Source    string
	Primary   string
	Pass      int
	Copied    int
	Skipped   int
	Vanished  int
	Failed    int
	Bytes     int64
	LastKey   string
	Done      bool
	Converged bool
}

// Result is the aggregate outcome of one Run. Browser-started runs select one Key; the CLI may
// select every placement moving off a cluster.
type Result struct {
	Key       string
	Passes    int
	Copied    int
	Skipped   int
	Vanished  int
	Failed    int
	Bytes     int64
	Converged bool
}

// Run executes the same mover engine for the CLI and the browser operation. It fails if an object
// failed or changed ETag, or if an until-converged run exhausted its pass budget.
func Run(ctx context.Context, dir *directory.File, secrets map[string]string, o Options, report func(Progress)) (Result, error) {
	out, errOut := o.Out, o.ErrOut
	if out == nil {
		out = io.Discard
	}
	if errOut == nil {
		errOut = io.Discard
	}
	if o.MaxPasses <= 0 {
		o.MaxPasses = 10
	}
	if !o.UntilConverged || o.DryRun {
		o.MaxPasses = 1
	}
	for _, path := range []string{o.Paths.CursorDir, o.Paths.LedgerDir} {
		if path == "" {
			return Result{}, errors.New("mover cursor and ledger directories are required")
		}
		if err := os.MkdirAll(path, 0o750); err != nil {
			return Result{}, err
		}
	}
	jobs, err := selectPlacements(dir, secrets, o.Key, o.From, o.AcceptLostWriteWindow)
	if err != nil {
		return Result{}, err
	}
	if len(jobs) == 0 {
		return Result{}, errors.New("nothing to move: no MIGRATING placement, or RAMPING at ratio 1, matched")
	}

	result := Result{Key: o.Key, Converged: true}
	unconverged := 0
	var total stats
	for i := range jobs {
		j := jobs[i]
		key := j.tenant + "/" + j.client
		_, _ = fmt.Fprintf(out, "== %s: %s/%s → %s/%s (%s guard, %s withdrawal)\n", key,
			j.src.name, j.src.bucket, j.dst.name, j.dst.bucket, guardName(j.conditional), withdrawName(j.condDelete))
		if !j.conditional {
			_, _ = fmt.Fprintf(out, "   WARNING (accepted with --%s): %s\n", migrate.AcceptLostWriteWindowFlag, migrate.LostWriteWindow(key, j.dst.name))
		}
		if err := checkSide(ctx, "source", j.src); err != nil {
			return result, fmt.Errorf("%s: %w", key, err)
		}
		if err := checkSide(ctx, "target", j.dst); err != nil {
			return result, fmt.Errorf("%s: %w", key, err)
		}
		converged := false
		for pass := 1; pass <= o.MaxPasses; pass++ {
			result.Passes++
			if o.MaxPasses > 1 {
				_, _ = fmt.Fprintf(out, "   pass %d\n", pass)
			}
			reportPass := func(s stats, lastKey string, done bool) {
				if report != nil {
					report(Progress{Key: key, Source: j.src.name, Primary: j.dst.name, Pass: pass,
						Copied: s.copied, Skipped: s.skipped, Vanished: s.vanished,
						Failed: s.failed + s.drifted, Bytes: s.bytes, LastKey: lastKey,
						Done: done, Converged: done && s.copied == 0 && s.failed == 0 && s.drifted == 0})
				}
			}
			s, moveErr := move(ctx, j, o.Paths, o.DryRun, out, errOut, reportPass)
			total.copied += s.copied
			total.skipped += s.skipped
			total.vanished += s.vanished
			total.failed += s.failed
			total.drifted += s.drifted
			total.bytes += s.bytes
			if moveErr != nil {
				_, _ = fmt.Fprintf(errOut, "mover: %s: %v\n", key, moveErr)
				total.failed++
				break
			}
			if s.copied == 0 && s.failed == 0 && s.drifted == 0 {
				converged = true
				break
			}
		}
		if !converged {
			result.Converged = false
			if o.UntilConverged && !o.DryRun {
				unconverged++
			}
		}
		if o.UntilConverged && converged {
			_, _ = fmt.Fprintln(out, "   converged: the last pass copied nothing")
		}
	}
	result.Copied, result.Skipped, result.Vanished = total.copied, total.skipped, total.vanished
	result.Failed, result.Bytes = total.failed+total.drifted, total.bytes
	_, _ = fmt.Fprintf(out, "\nmover: %d copied, %d already on the target, %d vanished mid-copy, %d failed, %s moved\n",
		total.copied, total.skipped, total.vanished, total.failed, HumanBytes(total.bytes))
	switch {
	case total.drifted > 0:
		return result, fmt.Errorf("%d objects changed ETag across the move", total.drifted)
	case total.failed > 0:
		return result, fmt.Errorf("%d objects failed to copy", total.failed)
	case unconverged > 0:
		return result, fmt.Errorf("%d placements did not converge within %d passes", unconverged, o.MaxPasses)
	}
	return result, nil
}

// LedgerPath returns the append-only local ledger for key.
func LedgerPath(paths Paths, key string) string {
	tenant, bucket, ok := strings.Cut(key, "/")
	if !ok {
		tenant, bucket = directory.DefaultTenant, key
	}
	safe := strings.ReplaceAll(tenant+"-"+bucket, "/", "-")
	return filepath.Join(paths.LedgerDir, "mover-"+safe+".ledger.jsonl")
}

// LedgerTail reads at most limit of the newest append-only records. Missing ledgers are empty.
func LedgerTail(paths Paths, key string, limit int) ([]LedgerEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	f, err := os.Open(LedgerPath(paths, key))
	if errors.Is(err, os.ErrNotExist) {
		return []LedgerEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only
	ring := make([]LedgerEntry, limit)
	count := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		var entry LedgerEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, fmt.Errorf("mover ledger line %d: %w", count+1, err)
		}
		ring[count%limit] = entry
		count++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	n := min(count, limit)
	out := make([]LedgerEntry, 0, n)
	start := max(0, count-limit)
	for i := start; i < count; i++ {
		out = append(out, ring[i%limit])
	}
	return out, nil
}
