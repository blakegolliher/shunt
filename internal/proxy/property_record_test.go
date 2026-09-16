package proxy

// The property test's record. Every run writes test/property/runs/<timestamp>/run.json with its
// seed and topology. Every event of the run — each client op, each request a backend served, each
// mover step, each operator transition — is streamed to events.jsonl.gz as it happens, stamped
// with the placement in force. A violation is appended to violations.jsonl the moment it is seen;
// when the run ends, each violating key's full history is extracted to keys/<key>.jsonl and
// report.md sets every violation beside the events around it. A clean run keeps run.json only.
//
// Nothing here is printed only to stdout: stdout has lost this evidence before (docs/STATUS.md).

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// propEvent is one line of events.jsonl.gz.
type propEvent struct {
	Seq       int64     `json:"seq"`
	Start     time.Time `json:"start,omitzero"`
	At        time.Time `json:"at"`    // when it finished; for a backend, the moment it committed
	Actor     string    `json:"actor"` // client-N | backend | mover | operator
	Op        string    `json:"op"`
	Key       string    `json:"key,omitempty"`
	OpID      string    `json:"op_id,omitempty"` // client op or mover step; a backend event names its sender
	Cluster   string    `json:"cluster,omitempty"`
	Status    int       `json:"status,omitempty"`
	Err       string    `json:"err,omitempty"`
	Body      string    `json:"body,omitempty"`
	Model     string    `json:"model,omitempty"` // client: the model before the op
	Mode      string    `json:"mode,omitempty"`  // mover: conditional | guarded
	INM       bool      `json:"if_none_match,omitempty"`
	Pass      int       `json:"pass,omitempty"`
	Placement string    `json:"placement"`
	Detail    string    `json:"detail,omitempty"`
}

// propViolation is one line of violations.jsonl.
type propViolation struct {
	N            int64     `json:"n"`
	At           time.Time `json:"at"`
	Key          string    `json:"key"`
	Invariant    string    `json:"invariant"`
	Detail       string    `json:"detail"`
	Actor        string    `json:"actor"`
	OpID         string    `json:"op_id,omitempty"`
	Placement    string    `json:"placement"`
	MoverRunning bool      `json:"mover_running"`
	MoverPass    int       `json:"mover_pass,omitempty"`
	Class        string    `json:"class"`           // property | harness
	Stall        string    `json:"stall,omitempty"` // the harness stall a "harness" violation crossed
}

// propStall is an interval of more than stallThreshold in which no party committed an event.
type propStall struct {
	From    time.Time `json:"from"`
	To      time.Time `json:"to"`
	Seconds float64   `json:"seconds"`
}

const stallThreshold = time.Second

type propRunInfo struct {
	Started    time.Time        `json:"started"`
	Ended      time.Time        `json:"ended,omitzero"`
	Seed       int64            `json:"seed"`
	SeedNote   string           `json:"seed_note"`
	Duration   string           `json:"duration"`
	Workers    int              `json:"workers"`
	Keys       int              `json:"keys"`
	Topology   any              `json:"topology"`
	Ops        int64            `json:"ops,omitempty"`
	Violations int64            `json:"violations"`
	ByClass    map[string]int64 `json:"violations_by_class,omitempty"`
	Stalls     []propStall      `json:"harness_stalls"`
	Mover      map[string]int64 `json:"mover,omitempty"` // pass<N>.<churn|kept>.<step>.<status> -> count
	LostWrites []propLoss       `json:"lost_writes_known_window"`
	Verdict    string           `json:"verdict,omitempty"`
	Result     string           `json:"result,omitempty"`
	Events     string           `json:"events,omitempty"`
}

type recorder struct {
	dir  string
	info propRunInfo

	seq, ids atomic.Int64
	maxAt    atomic.Int64 // latest event time seen, UnixNano

	smu    sync.Mutex
	stalls []propStall

	mu     sync.RWMutex // held for reading by every emit, for writing by close
	closed bool
	events chan propEvent
	wrote  chan error

	vmu        sync.Mutex
	vfile      *os.File
	violations []propViolation
}

// propertyRunDir is where this run's artifacts go: SHUNT_PROPERTY_RUN_DIR if set (make property
// sets it), else test/property/runs/<UTC timestamp> at the repository root.
func propertyRunDir() string {
	if d := os.Getenv("SHUNT_PROPERTY_RUN_DIR"); d != "" {
		return d
	}
	return filepath.Join("..", "..", "test", "property", "runs", time.Now().UTC().Format("20060102T150405.000Z"))
}

func newRecorder(t *testing.T, info propRunInfo) *recorder {
	t.Helper()
	dir := propertyRunDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("property record: %v", err)
	}
	abs, _ := filepath.Abs(dir)
	rec := &recorder{dir: abs, info: info, events: make(chan propEvent, 1<<16), wrote: make(chan error, 1)}
	rec.writeInfo(t)
	vf, err := os.Create(filepath.Join(abs, "violations.jsonl"))
	if err != nil {
		t.Fatalf("property record: %v", err)
	}
	rec.vfile = vf
	ef, err := os.Create(filepath.Join(abs, "events.jsonl.gz"))
	if err != nil {
		t.Fatalf("property record: %v", err)
	}
	go func() {
		zw, _ := gzip.NewWriterLevel(ef, gzip.BestSpeed)
		bw := bufio.NewWriterSize(zw, 1<<20)
		enc := json.NewEncoder(bw)
		var werr error
		for ev := range rec.events {
			if werr == nil {
				werr = enc.Encode(ev)
			}
		}
		for _, err := range []error{bw.Flush(), zw.Close(), ef.Close()} {
			if werr == nil {
				werr = err
			}
		}
		rec.wrote <- werr
	}()
	t.Logf("property run: seed %d, artifacts in %s", info.Seed, abs)
	return rec
}

func (rec *recorder) writeInfo(t *testing.T) {
	b, _ := json.MarshalIndent(rec.info, "", "  ")                                                   //nolint:errcheck // plain data
	if err := os.WriteFile(filepath.Join(rec.dir, "run.json"), append(b, '\n'), 0o644); err != nil { //nolint:gosec // test artifact
		t.Errorf("property record: run.json: %v", err)
	}
}

func (rec *recorder) nextID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, rec.ids.Add(1))
}

// emit records one event. It never drops: a full history is the point.
func (rec *recorder) emit(ev propEvent) {
	rec.mu.RLock()
	defer rec.mu.RUnlock()
	if rec.closed {
		return
	}
	ev.Seq = rec.seq.Add(1)
	if ev.At.IsZero() {
		ev.At = time.Now()
	}
	rec.noteTime(ev.At)
	rec.events <- ev
}

// noteTime advances the latest event time and records a stall when it jumps by more than
// stallThreshold: nobody committed anything in between.
func (rec *recorder) noteTime(at time.Time) {
	n := at.UnixNano()
	for {
		cur := rec.maxAt.Load()
		if n <= cur {
			return
		}
		if rec.maxAt.CompareAndSwap(cur, n) {
			if cur != 0 && time.Duration(n-cur) > stallThreshold {
				from := time.Unix(0, cur)
				rec.smu.Lock()
				rec.stalls = append(rec.stalls, propStall{From: from, To: at, Seconds: at.Sub(from).Seconds()})
				rec.smu.Unlock()
			}
			return
		}
	}
}

// stallWithin reports a harness stall that overlaps [start, end], as text for the violation record.
func (rec *recorder) stallWithin(start, end time.Time) (string, bool) {
	rec.smu.Lock()
	defer rec.smu.Unlock()
	for i := len(rec.stalls) - 1; i >= 0; i-- {
		s := rec.stalls[i]
		if s.From.Before(end) && s.To.After(start) {
			return fmt.Sprintf("%.3fs -> %.3fs (%.3fs)", rec.offset(s.From), rec.offset(s.To), s.Seconds), true
		}
	}
	return "", false
}

// violation records one broken property to disk at once, before anything else can go wrong.
func (rec *recorder) violation(v propViolation) propViolation {
	rec.vmu.Lock()
	defer rec.vmu.Unlock()
	v.N = int64(len(rec.violations) + 1)
	rec.violations = append(rec.violations, v)
	b, _ := json.Marshal(v) //nolint:errcheck // plain data
	_, _ = rec.vfile.Write(append(b, '\n'))
	_ = rec.vfile.Sync()
	rec.emit(propEvent{At: v.At, Actor: "checker", Op: "VIOLATION", Key: v.Key, OpID: v.OpID, Placement: v.Placement, Detail: v.Invariant + ": " + v.Detail})
	return v
}

// close ends the event stream and, if anything was violated, writes the per-key histories and the
// report. A clean run's event log is removed; its run.json stays.
// propOutcome is what the run knows once it has stopped.
type propOutcome struct {
	mover   map[string]int64
	losses  []propLoss
	verdict string
}

func (rec *recorder) close(t *testing.T, ops int64, out propOutcome) {
	rec.mu.Lock()
	rec.closed = true
	close(rec.events)
	rec.mu.Unlock()
	if err := <-rec.wrote; err != nil {
		t.Errorf("property record: events: %v", err)
	}
	_ = rec.vfile.Close()

	rec.vmu.Lock()
	violations := append([]propViolation(nil), rec.violations...)
	rec.vmu.Unlock()
	rec.info.Ended, rec.info.Ops, rec.info.Violations, rec.info.Mover = time.Now(), ops, int64(len(violations)), out.mover
	rec.info.LostWrites, rec.info.Verdict = out.losses, out.verdict
	rec.smu.Lock()
	rec.info.Stalls = append([]propStall{}, rec.stalls...)
	rec.smu.Unlock()
	rec.info.ByClass = map[string]int64{}
	for _, v := range violations {
		rec.info.ByClass[v.Class]++
	}
	events := filepath.Join(rec.dir, "events.jsonl.gz")
	if len(violations) == 0 {
		rec.info.Result = "clean"
		_ = os.Remove(events)
		_ = os.Remove(filepath.Join(rec.dir, "violations.jsonl"))
		rec.writeInfo(t)
		return
	}
	rec.info.Result, rec.info.Events = "violations", "events.jsonl.gz"
	rec.writeInfo(t)
	if err := rec.report(violations, events); err != nil {
		t.Errorf("property record: report: %v", err)
	}
	t.Logf("property run: %d violations; report in %s", len(violations), filepath.Join(rec.dir, "report.md"))
}

func keyFile(key string) string { return strings.NewReplacer("/", "_").Replace(key) + ".jsonl" }

// report extracts each violating key's full history (its own events plus every operator and mover
// pass event, which carry no key) and writes report.md.
func (rec *recorder) report(violations []propViolation, events string) error {
	keys := map[string][]propEvent{}
	for _, v := range violations {
		keys[v.Key] = nil
	}
	var global []propEvent
	f, err := os.Open(events)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read-only
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		isGlobal := bytes.Contains(line, []byte(`"actor":"operator"`)) || bytes.Contains(line, []byte(`"op":"pass_`))
		var mine string
		if !isGlobal {
			for k := range keys {
				if bytes.Contains(line, []byte(`"key":"`+k+`"`)) {
					mine = k
					break
				}
			}
			if mine == "" {
				continue
			}
		}
		var ev propEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return err
		}
		if isGlobal {
			global = append(global, ev)
		} else {
			keys[mine] = append(keys[mine], ev)
		}
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		return err
	}

	if err := os.MkdirAll(filepath.Join(rec.dir, "keys"), 0o755); err != nil {
		return err
	}
	for k, evs := range keys {
		evs = append(evs, global...)
		sort.Slice(evs, func(i, j int) bool {
			return evs[i].At.Before(evs[j].At) || (evs[i].At.Equal(evs[j].At) && evs[i].Seq < evs[j].Seq)
		})
		keys[k] = evs
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		for _, ev := range evs {
			_ = enc.Encode(ev)
		}
		if err := os.WriteFile(filepath.Join(rec.dir, "keys", keyFile(k)), b.Bytes(), 0o644); err != nil { //nolint:gosec // test artifact
			return err
		}
	}

	var md strings.Builder
	fmt.Fprintf(&md, "# Property run %s\n\nSeed %d (%s). Duration %s, %d workers, %d keys, %d ops, **%d violations**.\n\n",
		rec.info.Started.UTC().Format(time.RFC3339), rec.info.Seed, rec.info.SeedNote, rec.info.Duration, rec.info.Workers, rec.info.Keys, rec.info.Ops, len(violations))
	topo, _ := json.MarshalIndent(rec.info.Topology, "", "  ") //nolint:errcheck // plain data
	fmt.Fprintf(&md, "Topology:\n\n```json\n%s\n```\n\n", topo)
	md.WriteString("Files: `run.json`, `violations.jsonl` (written as each violation happened), `events.jsonl.gz` (every event of the run), `keys/<key>.jsonl` (full history of each violating key, with every operator transition and mover pass boundary).\n\n")
	md.WriteString("Times are offsets from the run start in seconds. `backend` rows are what a fake cluster served, in commit order, and name the client op or mover step that sent them.\n\n")

	fmt.Fprintf(&md, "**Verdict: %s.** Distinct writes lost in the known HEAD-then-commit window (ADR-0004 race 2): **%d**. Harness stalls over %s: %d.\n\n",
		rec.info.Verdict, len(rec.info.LostWrites), stallThreshold, len(rec.info.Stalls))
	if len(rec.info.LostWrites) > 0 {
		md.WriteString("| loss | key | mover op | mover HEAD committed | client op | client body | client PUT committed | mover PUT committed | mover body | stale reads |\n|---|---|---|---|---|---|---|---|---|---|\n")
		for _, l := range rec.info.LostWrites {
			fmt.Fprintf(&md, "| %d | `%s` | %s | %.6f | %s | %s | %.6f | %.6f | %s | %d |\n", l.N, l.Key, l.MoverOp, rec.offset(l.MoverHeadAt),
				l.ClientOp, mdEscape(l.ClientBody), rec.offset(l.ClientPutAt), rec.offset(l.MoverPutAt), mdEscape(l.MoverBody), l.Reads)
		}
		md.WriteString("\n")
	}
	// A lost write is read many times before anyone writes the key again, and each read is a
	// violation: group them, so the report shows each distinct failure once with its count.
	type group struct {
		first, last propViolation
		n           int
	}
	var groups []*group
	byKey := map[string]*group{}
	for _, v := range violations {
		id := v.Key + "\x00" + v.Invariant + "\x00" + v.Class + "\x00" + v.Detail
		g, ok := byKey[id]
		if !ok {
			g = &group{first: v}
			byKey[id] = g
			groups = append(groups, g)
		}
		g.last, g.n = v, g.n+1
	}
	fmt.Fprintf(&md, "## Violations: %d, in %d distinct groups\n\n", len(violations), len(groups))
	md.WriteString("A group is one key, invariant, class and detail; `count` is how many times it was seen, from `first` to `last`.\n\n")
	md.WriteString("| group | first # | first at | last at | count | key | invariant | class | mover at first | placement at first | detail |\n|---|---|---|---|---|---|---|---|---|---|---|\n")
	for gi, g := range groups {
		v := g.first
		fmt.Fprintf(&md, "| %d | %d | %.3f | %.3f | %d | `%s` | %s | %s %s | %v pass %d | %s | %s |\n", gi+1, v.N, rec.offset(v.At), rec.offset(g.last.At), g.n,
			v.Key, v.Invariant, v.Class, v.Stall, v.MoverRunning, v.MoverPass, v.Placement, mdEscape(v.Detail))
	}

	md.WriteString("\n## Mover copies of each violating key\n\n| key | pass | at | step | mode | status | detail |\n|---|---|---|---|---|---|---|\n")
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		for _, ev := range keys[k] {
			if ev.Actor == "mover" && ev.Key != "" {
				fmt.Fprintf(&md, "| `%s` | %d | %.3f | %s | %s | %d | %s |\n", k, ev.Pass, rec.offset(ev.At), ev.Op, ev.Mode, ev.Status, mdEscape(ev.Detail))
			}
		}
	}

	const contextRows = 300
	for gi, g := range groups {
		v := g.first
		fmt.Fprintf(&md, "\n## Group %d: `%s` %s, first at %.3f (violation %d), seen %d times\n\n%s\n\n", gi+1, v.Key, v.Invariant, rec.offset(v.At), v.N, g.n, mdEscape(v.Detail))
		fmt.Fprintf(&md, "The last %d events on the key up to half a second after the first violation, within 8 seconds before it. The full history is keys/%s.\n\n", contextRows, keyFile(v.Key))
		md.WriteString("| seq | start | at | actor | op | op_id | cluster | status | body | model | mode | pass | placement | detail |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		lo, hi := v.At.Add(-8*time.Second), v.At.Add(500*time.Millisecond)
		var window []propEvent
		for _, ev := range keys[v.Key] {
			if !ev.At.Before(lo) && !ev.At.After(hi) {
				window = append(window, ev)
			}
		}
		if len(window) > contextRows {
			window = window[len(window)-contextRows:]
		}
		for _, ev := range window {
			start := ""
			if !ev.Start.IsZero() {
				start = fmt.Sprintf("%.3f", rec.offset(ev.Start))
			}
			fmt.Fprintf(&md, "| %d | %s | %.3f | %s | %s | %s | %s | %d | %s | %s | %s | %d | %s | %s |\n",
				ev.Seq, start, rec.offset(ev.At), ev.Actor, ev.Op, ev.OpID, ev.Cluster, ev.Status, mdEscape(ev.Body), mdEscape(ev.Model), ev.Mode, ev.Pass, ev.Placement, mdEscape(ev.Detail))
		}
	}
	return os.WriteFile(filepath.Join(rec.dir, "report.md"), []byte(md.String()), 0o644) //nolint:gosec // test artifact
}

func (rec *recorder) offset(at time.Time) float64 { return at.Sub(rec.info.Started).Seconds() }

func mdEscape(s string) string { return strings.NewReplacer("|", `\|`, "\n", " ").Replace(s) }

// TestPropertyReportRebuild rewrites report.md and keys/ for an existing run directory from its
// run.json, violations.jsonl and events.jsonl.gz, without rerunning it. It runs only when asked:
//
//	SHUNT_PROPERTY_REPORT_DIR=test/property/runs/<timestamp> go test ./internal/proxy -run TestPropertyReportRebuild
func TestPropertyReportRebuild(t *testing.T) {
	dir := os.Getenv("SHUNT_PROPERTY_REPORT_DIR")
	if dir == "" {
		t.Skip("set SHUNT_PROPERTY_REPORT_DIR to rebuild a run's report")
	}
	rec := &recorder{dir: dir}
	b, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &rec.info); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "violations.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	var violations []propViolation
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var v propViolation
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		violations = append(violations, v)
	}
	if len(violations) == 0 {
		t.Fatalf("%s has no violations to report", dir)
	}
	if err := rec.report(violations, filepath.Join(dir, "events.jsonl.gz")); err != nil {
		t.Fatal(err)
	}
	t.Logf("rebuilt %s", filepath.Join(dir, "report.md"))
}
