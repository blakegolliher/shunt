package proxy

// The POC-4 property test (docs/POC.md P5 item 5). Clients read, write and delete one bucket
// without pause while the operator ramps the writes onto a second cluster, migrates, runs a mover
// over the data, and cuts over. The property under test is the client's view:
//
//	a successful write is immediately readable, with the bytes that were written;
//	a successful delete stays deleted;
//	a conditional write is answered by the whole bucket: create-once succeeds exactly when the key
//	  is absent from both clusters, and update-if-current applies exactly to the current version
//	  (ADR-0013), which the clients check outside the copy-in-flight window;
//	once the dust settles, a listing names exactly the objects that are still there.
//
// The middle invariant has one honest exception, recorded in docs/adr/0004-migration-races.md: a
// mover that has already read an object when the client deletes it will write its copy to the new
// primary and then take it back out, so a delete can be briefly undone while a copy is in flight.
// The test allows that window only while a mover pass is running.
//
// Two populations of keys. Churn keys take every operation all the time; by the time the bucket
// is MIGRATING they are gone from the source, so they exercise routing, not the mover. Kept keys
// are seeded on the source and only read until a mover pass runs, so the mover has something to
// copy. While it copies, the clients aim at the key the mover is on: they overwrite one third of
// the kept keys (the overwrite race, ADR-0004 race 2) and delete another third (the delete/copy
// race, race 1), and leave the last third alone. After the last pass, kept keys take every
// operation like the rest.
//
// It runs for SHUNT_PROPERTY_DURATION (default 3s, 2m in CI, 10m for a local soak), from
// SHUNT_PROPERTY_SEED (default: the clock; every run records the seed it used). The target's two
// conditional headers are separate knobs, because real backends differ on each:
//
//	SHUNT_PROPERTY_GUARD=conditional (default): the target honors If-None-Match: * on PUT, like MinIO;
//	SHUNT_PROPERTY_GUARD=guarded: it ignores it, like Garage 2.3.0, and the mover HEADs before it writes;
//	SHUNT_PROPERTY_WITHDRAW=re-head (default): the target ignores If-Match on DELETE, as both Garage
//	  and MinIO do (docs/reference/backend-compat.md), and the mover HEADs before withdrawing a copy;
//	SHUNT_PROPERTY_WITHDRAW=if-match: the target honors it, and the mover withdraws with it.
//
// Every run leaves its record in test/property/runs/<timestamp>/ (property_record_test.go).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/verify"
)

const (
	churnKeys       = 120 // obj/0000..0119: every operation, always
	keptKeys        = 120 // obj/0120..0239: read-only until a mover pass, then contended
	propertyKeys    = churnKeys + keptKeys
	propertyWorkers = 8
	opHeader        = "X-Property-Op"
	// Conditional writes, as a client that uses S3's create-once and update-if-current semantics
	// sends them. While a bucket is mid-migration shunt judges both against the two clusters
	// (ADR-0013); the model here says what the answer has to be.
	opCreateOnce      = "PUT-IF-NONE-MATCH"
	opUpdateIfCurrent = "PUT-IF-MATCH"
	propSource        = "acme-1111-data"
	propTarget        = "acme-9000-data"
)

// keptGroup says what the clients do to a kept key while the mover copies it.
type keptGroup int

const (
	notKept   keptGroup = iota
	keptPlain           // left alone: the mover's copy must be exact
	keptOverwrite
	keptDelete
)

func groupOf(i int) keptGroup {
	if i < churnKeys || i >= propertyKeys {
		return notKept
	}
	return keptPlain + keptGroup((i-churnKeys)%3)
}

func propertyDuration(t *testing.T) time.Duration {
	t.Helper()
	v := os.Getenv("SHUNT_PROPERTY_DURATION")
	if v == "" {
		return 3 * time.Second
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("SHUNT_PROPERTY_DURATION=%q: %v", v, err)
	}
	return d
}

// propertySeed is the run's base seed; worker w draws from seed+w+1. Seed 0 reproduces the worker
// seeds of every run made before seeds were recorded, which were hard-wired to 1..8.
func propertySeed(t *testing.T) (int64, string) {
	t.Helper()
	v := os.Getenv("SHUNT_PROPERTY_SEED")
	if v == "" {
		return time.Now().UnixNano(), "from the clock"
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("SHUNT_PROPERTY_SEED=%q: %v", v, err)
	}
	return n, "from SHUNT_PROPERTY_SEED"
}

type propertyRun struct {
	m         *mixedRig
	rec       *recorder
	mover     *testMover
	keys      []*verify.Key
	moving    atomic.Bool  // a mover pass is in flight
	moverDone atomic.Bool  // the last pass has ended: kept keys are free
	moverAt   atomic.Int32 // the key index the mover is on
	pass      atomic.Int32 // the current or last mover pass
	ops       atomic.Int64
	fails     atomic.Int64
	stopped   atomic.Bool

	losses       *lossLedger  // writes lost in the HEAD-then-commit window, from the target's commit stream
	tolerateLoss bool         // guarded variant: known-window losses do not fail the run
	knownReads   atomic.Int64 // violations classified as reads of such a loss

	statsMu sync.Mutex
	stats   map[string]int64 // per-pass mover outcomes, recorded in run.json
}

func (r *propertyRun) count(name string) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	r.stats[name]++
}

func TestMigrationPreservesTheClientsView(t *testing.T) {
	dur := propertyDuration(t)
	if testing.Short() {
		t.Skip("property test skipped in -short")
	}
	seed, seedNote := propertySeed(t)
	guard := os.Getenv("SHUNT_PROPERTY_GUARD")
	switch guard {
	case "", "conditional":
		guard = "conditional"
	case "guarded":
	default:
		t.Fatalf("SHUNT_PROPERTY_GUARD=%q: want conditional or guarded", guard)
	}
	withdraw := os.Getenv("SHUNT_PROPERTY_WITHDRAW")
	switch withdraw {
	case "", "re-head":
		withdraw = "re-head"
	case "if-match":
	default:
		t.Fatalf("SHUNT_PROPERTY_WITHDRAW=%q: want re-head or if-match", withdraw)
	}
	r := newPropertyRun(t, seed, seedNote, dur, guard, withdraw)

	// Seed half the churn keys and every kept key, so the migration has something to move.
	for i := 0; i < propertyKeys; i++ {
		if i < churnKeys && i >= churnKeys/2 {
			continue
		}
		body := fmt.Sprintf("seed-%d", i)
		id := r.rec.nextID("seed")
		st := time.Now()
		code, _, err := r.do(http.MethodPut, key(i), []byte(body), id)
		r.rec.emit(propEvent{Start: st, Actor: "seed", Op: http.MethodPut, Key: key(i), OpID: id, Status: code, Err: errString(err), Body: body, Placement: r.placement(key(i))})
		if err != nil || code != 200 {
			t.Fatalf("seeding %s: %d %v", key(i), code, err)
		}
		r.keys[i].Present, r.keys[i].Body = true, body
	}

	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	for w := 0; w < propertyWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r.client(t, w, rand.New(rand.NewSource(seed+int64(w)+1)), deadline) //nolint:gosec // G404: test load, not crypto
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.operator(t, deadline)
	}()
	wg.Wait()

	// Quiescent: the listing must name exactly the objects the client still has.
	r.checkListing(t)
	r.statsMu.Lock()
	t.Logf("mover: %v", r.stats)
	r.statsMu.Unlock()
	h := r.headline()
	t.Logf("headline: %d distinct lost writes in the known HEAD-then-commit window (ADR-0004 race 2), %d violations outside it; %d violation records, %d operations in %s, seed %d",
		h.LostWrites, h.ViolationsOutsideWindow, h.ViolationRecords, r.ops.Load(), dur, seed)
	if strings.HasPrefix(h.Verdict, "fail") {
		t.Fatalf("%s; record in %s", h.Verdict, r.rec.dir)
	}
	t.Log(h.Verdict)
}

// headline is the run's result. Its count is distinct lost writes, taken from the target's commit
// history whether or not any read saw them; violations.jsonl is the evidence, not the count. The
// guarded variant passes when nothing happened outside the known window; the conditional variant
// passes only when nothing happened at all.
func (r *propertyRun) headline() propHeadline {
	fails, known := r.fails.Load(), r.knownReads.Load()
	h := propHeadline{LostWrites: len(r.losses.snapshot()), ViolationsOutsideWindow: fails - known, ViolationRecords: fails}
	switch {
	case h.ViolationsOutsideWindow > 0:
		h.Verdict = fmt.Sprintf("fail: %d violations outside the known window", h.ViolationsOutsideWindow)
	case h.LostWrites > 0 && !r.tolerateLoss:
		h.Verdict = fmt.Sprintf("fail: %d writes lost in the HEAD-then-commit window, and this variant tolerates none", h.LostWrites)
	case h.LostWrites > 0:
		h.Verdict = fmt.Sprintf("pass: %d distinct writes lost in the known HEAD-then-commit window, nothing outside it", h.LostWrites)
	default:
		h.Verdict = "pass: no lost writes, no violations"
	}
	return h
}

// newPropertyRun builds the rig, the recorder, the loss ledger and the mover for one run. guard is
// conditional or guarded, withdraw is re-head or if-match (see the top of this file).
func newPropertyRun(t *testing.T, seed int64, seedNote string, dur time.Duration, guard, withdraw string) *propertyRun {
	t.Helper()
	putINM, deleteIfMatch := guard == "conditional", withdraw == "if-match"

	// The target's declared capability matches what the fake actually does: an operator records
	// conditional_write: false for a backend that ignores If-None-Match: * (docs/reference/
	// backend-compat.md), and shunt guards conditional writes itself on such a target (ADR-0013).
	m := newMixedRig(t, nil, func(cs map[string]config.Cluster) {
		cl := cs["minio"]
		cl.Capabilities.ConditionalWrite = &putINM
		cs["minio"] = cl
	})
	// A soak must not hold every request it served: an in-memory access log and the fakes' request
	// history stalled the whole process for seconds, then a minute, in the first 60-minute run.
	m.accessLog.discard()
	for _, f := range []*fakeS3{m.garage, m.minio} {
		f.mu.Lock()
		f.noHistory = true
		f.mu.Unlock()
	}
	r := &propertyRun{m: m, keys: make([]*verify.Key, propertyKeys), stats: map[string]int64{},
		losses: newLossLedger(), tolerateLoss: !putINM}
	for i := range r.keys {
		r.keys[i] = &verify.Key{}
	}
	m.minio.addBucket(propTarget)
	m.backendNames = append(m.backendNames, propTarget)
	m.minio.mu.Lock()
	m.minio.ignoreINM, m.minio.ignoreIfMatch = !putINM, !deleteIfMatch
	m.minio.mu.Unlock()
	withdrawHow := "DELETE If-Match: <ETag of the mover's PUT>"
	if !deleteIfMatch {
		withdrawHow = "HEAD the target, DELETE only if ETag matches the mover's PUT and Last-Modified is not after its Date"
	}

	r.rec = newRecorder(t, propRunInfo{
		Started: time.Now(), Seed: seed, SeedNote: seedNote, Duration: dur.String(), Workers: propertyWorkers, Keys: propertyKeys,
		Topology: map[string]any{
			"harness":                       "in-process: shunt handler (resign) over two signature-verifying fake clusters (internal/proxy/mixed_test.go); access log discarded, no per-request history",
			"source":                        "garage/" + propSource,
			"target":                        "minio/" + propTarget,
			"target_honors_if_none_match":   putINM,
			"target_honors_if_match_delete": deleteIfMatch,
			"mover_guard":                   guard,
			"mover_withdraw":                withdraw + ": " + withdrawHow,
			"mover":                         "testMover: read source, guard, PUT target, re-check source, withdraw its own copy if the source is gone",
			"keys":                          fmt.Sprintf("%d churn (every op), %d kept (read-only until a mover pass; during a pass the clients overwrite 1/3 and delete 1/3 at the key the mover is on)", churnKeys, keptKeys),
			"operator":                      "step=duration/8: RAMPING 0.1, 0.5, 1.0, MIGRATING, 2 mover passes, CUTOVER, ACTIVE",
			"dual_delete_order":             "source, then primary (internal/proxy/handler.go)",
			"resurrection_allowance":        "5s, only while a mover pass runs",
			"harness_stall":                 "no event from any party for more than 1s; a GET EOF across one is tagged harness and still counts",
			"go":                            runtime.Version(),
			"gomaxprocs":                    runtime.GOMAXPROCS(0),
		},
	})
	t.Cleanup(func() {
		r.statsMu.Lock()
		stats := make(map[string]int64, len(r.stats))
		for k, v := range r.stats {
			stats[k] = v
		}
		r.statsMu.Unlock()
		r.rec.close(t, r.ops.Load(), propOutcome{mover: stats, losses: r.losses.snapshot(), headline: r.headline()})
	})
	observe := func(cluster string) func(backendEvent) {
		return func(ev backendEvent) {
			r.rec.emit(propEvent{Start: ev.start, At: ev.end, Actor: "backend", Op: ev.method, Key: ev.key, OpID: ev.opID, Cluster: cluster + "/" + ev.bucket,
				Status: ev.status, Body: string(ev.body), INM: ev.ifNoneMatch, Placement: r.placement(ev.key)})
			if cluster == "minio" && ev.bucket == propTarget {
				r.losses.observe(ev)
			}
		}
	}
	m.garage.mu.Lock()
	m.garage.observe = observe("garage")
	m.garage.mu.Unlock()
	m.minio.mu.Lock()
	m.minio.observe = observe("minio")
	m.minio.mu.Unlock()
	r.mover = &testMover{m: m, source: propSource, target: propTarget, putIfNoneMatch: putINM, deleteIfMatch: deleteIfMatch}
	return r
}

func key(i int) string { return fmt.Sprintf("obj/%04d", i) }

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// placement describes the placement in force right now, as seen for one key.
func (r *propertyRun) placement(k string) string {
	p, ok := r.m.dir.Snapshot().Lookup("acme", "data")
	if !ok {
		return "none"
	}
	var b strings.Builder
	b.WriteString(p.State)
	b.WriteString(" primary=")
	b.WriteString(p.Primary)
	if p.Source != "" {
		b.WriteString(" source=")
		b.WriteString(p.Source)
	}
	if p.Ramp != nil {
		fmt.Fprintf(&b, " ratio=%g", p.Ramp.Ratio)
		if p.State == directory.StateRamping && k != "" {
			if in, _ := migrate.InRange(p.Ramp, k); in {
				b.WriteString(" key->primary")
			} else {
				b.WriteString(" key->source")
			}
		}
	}
	return b.String()
}

// pick chooses the next key and what to do to it: "PUT", "DELETE", or "GET".
func (r *propertyRun) pick(rnd *rand.Rand) (int, string) {
	// While the mover copies, half the picks go to the kept key it is on or the next one, so the
	// races happen on purpose (ADR-0004 races 1 and 2).
	if r.moving.Load() && rnd.Intn(2) == 0 {
		at := int(r.moverAt.Load())
		for off := 0; off < 3; off++ {
			i := at + rnd.Intn(2) + off
			switch groupOf(i) {
			case keptOverwrite:
				return i, http.MethodPut
			case keptDelete:
				return i, http.MethodDelete
			}
		}
	}
	i := rnd.Intn(propertyKeys)
	if groupOf(i) != notKept && !r.moverDone.Load() {
		return i, http.MethodGet // kept keys stay on the source until the mover has had them
	}
	// Conditional writes stay out of the copy-in-flight window: a mover copy can put an object
	// back for a moment (ADR-0004 race 1), and the answer to a condition would be ambiguous there.
	quiet := !r.moving.Load()
	switch n := rnd.Intn(10); {
	case n < 3:
		return i, http.MethodPut
	case n == 3 && quiet:
		return i, opCreateOnce
	case n == 4 && quiet:
		return i, opUpdateIfCurrent
	case n < 5:
		return i, http.MethodPut
	case n < 6:
		return i, http.MethodDelete
	}
	return i, http.MethodGet
}

// client is one pretend application: it writes, deletes and reads, and holds the model to account.
func (r *propertyRun) client(t *testing.T, w int, rnd *rand.Rand, deadline time.Time) {
	actor := "client-" + strconv.Itoa(w)
	for time.Now().Before(deadline) && !r.stopped.Load() {
		i, method := r.pick(rnd)
		k := r.keys[i]
		k.Lock()
		id := r.rec.nextID("op")
		ev := propEvent{Start: time.Now(), Actor: actor, Key: key(i), OpID: id, Model: k.String(), Placement: r.placement(key(i))}
		switch method {
		case http.MethodPut:
			body := fmt.Sprintf("v%d-%d", i, rnd.Int63())
			code, _, err := r.do(http.MethodPut, key(i), []byte(body), id)
			ev.Op, ev.Status, ev.Err, ev.Body = http.MethodPut, code, errString(err), body
			r.rec.emit(ev)
			switch {
			case err != nil:
				r.violation(t, actor, id, key(i), "put-transport", ev.Start, "", "PUT %s: %v", key(i), err)
			case code == 200:
				k.Present, k.Body = true, body
			default:
				r.violation(t, actor, id, key(i), "put-status", ev.Start, "", "PUT %s: HTTP %d", key(i), code)
			}
		case http.MethodDelete:
			code, _, err := r.do(http.MethodDelete, key(i), nil, id)
			ev.Op, ev.Status, ev.Err = http.MethodDelete, code, errString(err)
			r.rec.emit(ev)
			switch {
			case err != nil:
				r.violation(t, actor, id, key(i), "delete-transport", ev.Start, "", "DELETE %s: %v", key(i), err)
			case code == 204:
				k.Present, k.Body, k.Deleted = false, "", time.Now()
			default:
				r.violation(t, actor, id, key(i), "delete-status", ev.Start, "", "DELETE %s: HTTP %d", key(i), code)
			}
		case opCreateOnce:
			// Create-once: it must succeed exactly when the key is absent, wherever it lives.
			body := fmt.Sprintf("c%d-%d", i, rnd.Int63())
			code, _, err := r.doHeaders(http.MethodPut, key(i), []byte(body), id, map[string]string{"If-None-Match": "*"})
			ev.Op, ev.Status, ev.Err, ev.Body = opCreateOnce, code, errString(err), body
			r.rec.emit(ev)
			switch {
			case err != nil:
				r.violation(t, actor, id, key(i), "put-transport", ev.Start, "", "%s %s: %v", opCreateOnce, key(i), err)
			case !k.Present && code == 200:
				k.Present, k.Body = true, body
			case k.Present && code == 412:
			default:
				r.violation(t, actor, id, key(i), "create-once", ev.Start, "",
					"%s %s over a key the model says is present=%v: HTTP %d", opCreateOnce, key(i), k.Present, code)
			}
		case opUpdateIfCurrent:
			// Update-if-current: the ETag of what the client last wrote, so it must apply; a stale
			// one must not, and neither must an update to a key that is gone.
			stale := !k.Present || rnd.Intn(4) == 0
			want := `"00000000000000000000000000000000"`
			if !stale {
				want = etagOf([]byte(k.Body))
			}
			body := fmt.Sprintf("u%d-%d", i, rnd.Int63())
			code, _, err := r.doHeaders(http.MethodPut, key(i), []byte(body), id, map[string]string{"If-Match": want})
			ev.Op, ev.Status, ev.Err, ev.Body = opUpdateIfCurrent, code, errString(err), body
			r.rec.emit(ev)
			switch {
			case err != nil:
				r.violation(t, actor, id, key(i), "put-transport", ev.Start, "", "%s %s: %v", opUpdateIfCurrent, key(i), err)
			case !stale && code == 200:
				k.Body = body
			case stale && code == 412:
			default:
				r.violation(t, actor, id, key(i), "update-if-current", ev.Start, "",
					"%s %s with a %s ETag over present=%v: HTTP %d", opUpdateIfCurrent, key(i), map[bool]string{true: "stale", false: "current"}[stale], k.Present, code)
			}
		default:
			r.read(t, actor, id, i, k, ev)
		}
		k.Unlock()
		r.ops.Add(1)
	}
}

// read checks one key against the model, allowing only the documented copy-in-flight window.
func (r *propertyRun) read(t *testing.T, actor, id string, i int, k *verify.Key, ev propEvent) {
	code, body, err := r.do(http.MethodGet, key(i), nil, id)
	ev.Op, ev.Status, ev.Err = http.MethodGet, code, errString(err)
	if code == 200 {
		ev.Body = string(body[:min(len(body), 64)])
	}
	r.rec.emit(ev)
	if err != nil {
		r.violation(t, actor, id, key(i), "get-transport", ev.Start, "", "GET %s: %v", key(i), err)
		return
	}
	switch {
	case k.Present && code != 200:
		r.violation(t, actor, id, key(i), "write-readable", ev.Start, "", "GET %s: HTTP %d, but the client wrote %q and never deleted it", key(i), code, k.Body)
	case k.Present && string(body) != k.Body:
		if loss, ok := r.losses.staleRead(key(i), k.Body, string(body)); ok {
			r.violation(t, actor, id, key(i), "write-bytes", ev.Start, "known-window",
				"GET %s: read %q, wrote %q: lost write %d, overwritten by %s inside the HEAD-then-commit window (ADR-0004 race 2)", key(i), body, k.Body, loss.N, loss.MoverOp)
			break
		}
		r.violation(t, actor, id, key(i), "write-bytes", ev.Start, "", "GET %s: read %q, wrote %q", key(i), body, k.Body)
	case !k.Present && code == 200:
		// A copy already in flight when the delete landed may put the object back for as long as
		// that one object's copy takes; the mover then removes it (ADR-0004). Outside a mover pass
		// there is no such excuse.
		if !r.moving.Load() {
			r.violation(t, actor, id, key(i), "delete-stays-deleted", ev.Start, "", "GET %s: read %q, deleted %s ago, with no mover running", key(i), body, time.Since(k.Deleted).Round(time.Millisecond))
			return
		}
		if ok := r.eventuallyGone(actor, i, k, 5*time.Second); !ok {
			r.violation(t, actor, id, key(i), "delete-withdrawn-in-5s", ev.Start, "", "GET %s: read %q, deleted %s ago and still readable 5s after a mover pass touched it", key(i), body, time.Since(k.Deleted).Round(time.Millisecond))
		}
	case !k.Present && code != 404:
		r.violation(t, actor, id, key(i), "delete-status-404", ev.Start, "", "GET %s: HTTP %d, expected 404 for a deleted key", key(i), code)
	}
}

// eventuallyGone waits for the mover to withdraw a copy it made of a deleted object.
func (r *propertyRun) eventuallyGone(actor string, i int, k *verify.Key, within time.Duration) bool {
	for end := time.Now().Add(within); time.Now().Before(end); {
		time.Sleep(20 * time.Millisecond)
		id := r.rec.nextID("op")
		ev := propEvent{Start: time.Now(), Actor: actor, Op: "GET(recheck)", Key: key(i), OpID: id, Model: k.String(), Placement: r.placement(key(i))}
		code, body, err := r.do(http.MethodGet, key(i), nil, id)
		ev.Status, ev.Err = code, errString(err)
		if code == 200 {
			ev.Body = string(body[:min(len(body), 64)])
		}
		r.rec.emit(ev)
		if err == nil && code == 404 {
			return true
		}
	}
	return false
}

// operator walks the migration while the clients keep working.
func (r *propertyRun) operator(t *testing.T, deadline time.Time) {
	total := time.Until(deadline)
	step := total / 8
	sleep := func() { time.Sleep(step) }
	set := func(tr directory.Transition) {
		from := directory.StateActive
		if p, ok := r.m.dir.Snapshot().Lookup("acme", "data"); ok {
			from = p.State
		}
		// The target is named once, on the way out of ACTIVE; every later step inherits it.
		if from == directory.StateActive {
			tr.Target, tr.Name = "minio", propTarget
		}
		start := time.Now()
		err := r.m.dir.SetState(t.Context(), "acme", "data", from, tr, "property")
		r.rec.emit(propEvent{Start: start, Actor: "operator", Op: "set-state", Err: errString(err), Placement: r.placement(""),
			Detail: fmt.Sprintf("%s -> %s ratio=%g", from, tr.To, tr.Ratio)})
		if err != nil {
			r.violation(t, "operator", "", "", "operator", start, "", "set-state %s: %v", tr.To, err)
		}
	}

	sleep()
	set(directory.Transition{To: directory.StateRamping, Ratio: 0.1})
	sleep()
	set(directory.Transition{To: directory.StateRamping, Ratio: 0.5})
	sleep()
	set(directory.Transition{To: directory.StateRamping, Ratio: 1})
	sleep()
	set(directory.Transition{To: directory.StateMigrating})

	// Two passes: the first moves the bulk, the second catches what the clients wrote behind it.
	for pass := 1; pass <= 2 && time.Now().Before(deadline); pass++ {
		r.movePass(t, pass)
		sleep()
	}
	r.moverDone.Store(true)
	set(directory.Transition{To: directory.StateCutover})
	sleep()
	set(directory.Transition{To: directory.StateActive})

	// Stop the clients a little before the deadline so the final listing check is quiescent.
	time.Sleep(time.Until(deadline))
	r.stopped.Store(true)
}

// movePass runs one mover pass over every key and records each step and its outcome.
func (r *propertyRun) movePass(t *testing.T, pass int) {
	mode := r.mover.mode()
	r.pass.Store(int32(pass)) //nolint:gosec // G115: 1 or 2
	r.moving.Store(true)
	r.rec.emit(propEvent{Actor: "mover", Op: "pass_start", Pass: pass, Mode: mode, Placement: r.placement("")})
	defer func() {
		r.moving.Store(false)
		r.rec.emit(propEvent{Actor: "mover", Op: "pass_end", Pass: pass, Mode: mode, Placement: r.placement("")})
	}()
	for i := 0; i < propertyKeys; i++ {
		r.moverAt.Store(int32(i)) //nolint:gosec // G115: < propertyKeys
		k := key(i)
		id := r.rec.nextID("mover")
		class := "churn"
		if groupOf(i) != notKept {
			class = "kept"
		}
		step := func(op string, status int, body, detail string) {
			r.rec.emit(propEvent{Actor: "mover", Op: op, Key: k, OpID: id, Status: status, Body: body, Mode: mode, Pass: pass, Placement: r.placement(k), Detail: detail})
			r.count(fmt.Sprintf("pass%d.%s.%s.%d", pass, class, op, status))
		}
		data, ok := r.mover.read(k)
		if !ok {
			step("read_source", 404, "", "not on the source; nothing to copy")
			continue
		}
		step("read_source", 200, string(data), "")
		if err := r.mover.commit(k, id, data, step); err != nil {
			r.violation(t, "mover", id, k, "mover-transport", time.Now(), "", "mover %s: %v", k, err)
			return
		}
	}
}

// testMover is the mover contract of shunt migrate run (cmd/shunt/mover.go) against the fake clusters: copy what the source still holds,
// never overwrite a client's newer write, and withdraw its own copy of an object that has been
// deleted, without touching a client's newer write. putIfNoneMatch and deleteIfMatch say which
// conditional headers the target honors (capabilities.conditional_write and conditional_delete);
// without them the mover HEADs before it writes and before it withdraws, as shunt migrate run does.
type testMover struct {
	m                             *mixedRig
	source, target                string
	putIfNoneMatch, deleteIfMatch bool
	afterPut                      func() // test hook: runs once a copy has landed, before the source is re-checked
}

func (mv *testMover) mode() string {
	put, del := "guarded", "re-head"
	if mv.putIfNoneMatch {
		put = "conditional"
	}
	if mv.deleteIfMatch {
		del = "if-match"
	}
	return put + "/" + del
}

// read is the mover's GET of the source.
func (mv *testMover) read(k string) ([]byte, bool) { return mv.m.garage.object(mv.source, k) }

// send makes one signed request to the target cluster.
func (mv *testMover) send(method, k, id string, body []byte, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequest(method, "http://"+mv.m.mAddr+"/"+mv.target+"/"+k, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))
	if len(body) == 0 {
		req.Body = http.NoBody
	}
	for name, v := range hdr {
		req.Header.Set(name, v)
	}
	if id != "" {
		req.Header.Set(opHeader, id)
	}
	sigv4.Sign(req, sigv4.Credentials{AccessKey: "MINIOCLUSTERKEY", Secret: "minio-cluster-secret"}, "us-east-1", sigv4.UnsignedPayload, time.Now())
	resp, err := fresh().Do(req)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp, nil
}

// commit copies data to the target under the guard, then withdraws the copy if the source has
// lost the object meanwhile. step records each step as (op, status, body, detail).
func (mv *testMover) commit(k, id string, data []byte, step func(op string, status int, body, detail string)) error {
	if !mv.putIfNoneMatch {
		resp, err := mv.send(http.MethodHead, k, id, nil, nil)
		if err != nil {
			return err
		}
		step("head_target", resp.StatusCode, "", "")
		if resp.StatusCode == http.StatusOK {
			return nil // already on the target
		}
	}
	var hdr map[string]string
	if mv.putIfNoneMatch {
		hdr = map[string]string{"If-None-Match": "*"}
	}
	put, err := mv.send(http.MethodPut, k, id, data, hdr)
	if err != nil {
		return err
	}
	step("put_target", put.StatusCode, string(data), "")
	if put.StatusCode != http.StatusOK {
		return nil // 412: a client got there first; its copy is the newer one
	}
	if mv.afterPut != nil {
		mv.afterPut()
	}
	// Re-HEAD the source: if the object was deleted while this copy was in flight, the copy is
	// a resurrection and has to come back out.
	if _, still := mv.read(k); still {
		step("recheck_source", 200, "", "still on the source; the copy stands")
		return nil
	}
	step("recheck_source", 404, "", "gone from the source; withdrawing the copy")
	return mv.withdraw(k, id, put, step)
}

// withdraw deletes the mover's own copy and nothing newer: If-Match on the ETag its PUT returned
// where the target supports it, else a HEAD that must show that ETag and a Last-Modified no later
// than the PUT's Date before an unconditional DELETE.
func (mv *testMover) withdraw(k, id string, put *http.Response, step func(op string, status int, body, detail string)) error {
	etag := put.Header.Get("ETag")
	if mv.deleteIfMatch {
		resp, err := mv.send(http.MethodDelete, k, id, nil, map[string]string{"If-Match": etag})
		if err != nil {
			return err
		}
		step("withdraw_if_match", resp.StatusCode, "", map[int]string{204: "removed", 412: "a newer write is there; kept", 404: "already gone"}[resp.StatusCode])
		return nil
	}
	head, err := mv.send(http.MethodHead, k, id, nil, nil)
	if err != nil {
		return err
	}
	if head.StatusCode == http.StatusNotFound {
		step("withdraw_head", 404, "", "already gone")
		return nil
	}
	date, _ := http.ParseTime(put.Header.Get("Date"))
	modified, _ := http.ParseTime(head.Header.Get("Last-Modified"))
	if !ownCopy(head.Header.Get("ETag"), etag, modified, date) {
		step("withdraw_head", head.StatusCode, "", "not the mover's copy; kept")
		return nil
	}
	step("withdraw_head", head.StatusCode, "", "the mover's copy")
	resp, err := mv.send(http.MethodDelete, k, id, nil, nil)
	if err != nil {
		return err
	}
	step("withdraw_delete", resp.StatusCode, "", "removed")
	return nil
}

// ownCopy mirrors cmd/shunt/mover.go: the object on the target is the mover's copy when it carries the
// ETag the mover's PUT returned and was not modified after that PUT's Date. Both times are the
// backend's own clock, at one-second resolution.
func ownCopy(headETag, putETag string, modified, putDate time.Time) bool {
	if headETag == "" || headETag != putETag {
		return false
	}
	return putDate.IsZero() || !modified.After(putDate)
}

// checkListing compares a listing through shunt against the model, with every key held still.
func (r *propertyRun) checkListing(t *testing.T) {
	t.Helper()
	for _, k := range r.keys {
		k.Lock()
		defer k.Unlock() //nolint:gocritic // deferInLoop: all keys stay locked until the check ends
	}
	var want []string
	for i, k := range r.keys {
		if k.Present {
			want = append(want, key(i))
		}
	}
	sort.Strings(want)

	var got []string
	token := ""
	for page := 0; page < 40; page++ {
		target := "/data?list-type=2&max-keys=50"
		if token != "" {
			target += "&continuation-token=" + token
		}
		resp := r.m.acme(t, http.MethodGet, target, nil)
		if resp.StatusCode != 200 {
			t.Fatalf("listing: %d %s", resp.StatusCode, resp.body)
		}
		body := string(resp.body)
		for rest := body; ; {
			i := strings.Index(rest, "<Key>")
			if i < 0 {
				break
			}
			rest = rest[i+5:]
			got = append(got, rest[:strings.Index(rest, "</Key>")])
		}
		token = between(resp.body, "<NextContinuationToken>", "</NextContinuationToken>")
		if token == "" {
			break
		}
	}
	sort.Strings(got)
	now := time.Now()
	for _, k := range missing(got, want) {
		r.violation(t, "checker", "", k, "listing", now, "", "listed, but the client's model has it deleted")
	}
	for _, k := range missing(want, got) {
		r.violation(t, "checker", "", k, "listing", now, "", "the client's model has it, but the listing does not")
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("listing has %d keys, the client has %d\nonly in the listing: %v\nonly in the client's model: %v",
			len(got), len(want), missing(got, want), missing(want, got))
	}
}

func missing(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, s := range b {
		in[s] = true
	}
	var out []string
	for _, s := range a {
		if !in[s] {
			out = append(out, s)
		}
	}
	return out
}

// do sends one client request through shunt without failing the test on a transport error.
func (r *propertyRun) do(method, k string, body []byte, id string) (int, []byte, error) {
	return r.doHeaders(method, k, body, id, nil)
}

// doHeaders is do with the client's own headers, for conditional writes.
func (r *propertyRun) doHeaders(method, k string, body []byte, id string, extra map[string]string) (int, []byte, error) {
	c := verify.Client{Endpoint: r.m.front.URL, Bucket: "data", Creds: sigv4.Credentials{AccessKey: acmeAK, Secret: acmeSK}, HTTP: fresh()}
	hdr := map[string]string{opHeader: id}
	for k, v := range extra {
		hdr[k] = v
	}
	rep, err := c.Do(context.Background(), method, k, body, hdr)
	return rep.Status, rep.Body, err
}

// violation records one broken property: to disk first (violations.jsonl, then the run's report),
// then to the test log. A GET that died with EOF across a harness stall is tagged "harness"; it
// still counts. A stale read of a write the ledger saw lost in the HEAD-then-commit window is
// tagged "known-window"; the guarded variant passes on those alone, the conditional variant does
// not. The run continues so the record counts them all.
func (r *propertyRun) violation(t *testing.T, actor, id, k, invariant string, start time.Time, class, format string, a ...any) {
	t.Helper()
	now := time.Now()
	if class == "" {
		class = "property"
	}
	v := propViolation{At: now, Key: k, Invariant: invariant, Detail: fmt.Sprintf(format, a...), Actor: actor, OpID: id,
		Placement: r.placement(k), MoverRunning: r.moving.Load(), MoverPass: int(r.pass.Load()), Class: class}
	if invariant == "get-transport" && strings.Contains(v.Detail, "EOF") {
		if s, ok := r.rec.stallWithin(start, now); ok {
			v.Class, v.Stall = "harness", s
		}
	}
	v = r.rec.violation(v)
	n := r.fails.Add(1)
	if v.Class == "known-window" {
		n = r.knownReads.Add(1)
		if r.tolerateLoss {
			if n <= 5 {
				t.Logf("known-window violation %d (recorded in %s): %s", v.N, r.rec.dir, v.Detail)
			}
			return
		}
	}
	if n <= 20 {
		t.Errorf("property violation %d (%s, %s, recorded in %s): %s", v.N, invariant, v.Class, r.rec.dir, v.Detail)
	}
}
