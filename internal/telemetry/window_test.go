package telemetry

import (
	"encoding/json"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

var windowBase = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func collectWindow(t testing.TB, values []int64, cluster string) *Window {
	t.Helper()
	c := NewCollector()
	for _, value := range values {
		c.Observe(Observation{At: windowBase.Add(time.Second), Operation: "GetObject", Cluster: cluster,
			Status: 200, BytesOut: 1, ClientTotal: time.Duration(value) * time.Microsecond})
	}
	w := c.Completed(windowBase.Add(WindowDuration + time.Millisecond))
	if w == nil {
		t.Fatal("collector produced no completed window")
	}
	return w
}

func findSummary(t testing.TB, windows []ScopeWindow, scope, series string, op OpClass) Summary {
	t.Helper()
	for _, w := range windows {
		if w.Scope != scope {
			continue
		}
		for _, s := range w.Series {
			if s.Series == series && s.Op == op {
				return s
			}
		}
	}
	t.Fatalf("no %s %s/%s summary in %v", scope, series, op, windows)
	return Summary{}
}

func TestCollectorRotatesLazilyAndResends(t *testing.T) {
	c := NewCollector()
	if c.Completed(windowBase) != nil || c.sketches != nil {
		t.Fatal("an empty collector allocated or emitted a window")
	}
	c.Observe(Observation{At: windowBase.Add(time.Second), Operation: "PutObject", Cluster: "a", Status: 503,
		BytesIn: 7, BytesOut: 11, ClientTotal: 5 * time.Millisecond, UpstreamTTFB: time.Millisecond, UpstreamTotal: 4 * time.Millisecond})
	if len(c.sketches) != 4 {
		t.Fatalf("allocated sketches = %d, want the four observed series", len(c.sketches))
	}
	w := c.Completed(windowBase.Add(WindowDuration))
	if w == nil || len(w.Sketches) != 4 || len(w.Counters) != 1 {
		t.Fatalf("completed window: %#v", w)
	}
	if got := w.Counters[0].Errors["5xx"]; got != 1 {
		t.Fatalf("5xx errors = %d, want 1", got)
	}
	if again := c.Completed(windowBase.Add(2 * WindowDuration)); again != w {
		t.Fatal("an empty interval did not re-send the last non-empty window")
	}
}

func TestMigrationSignalsUseAndMergeCompletedWindows(t *testing.T) {
	window := func(writesSource, writesPrimary, hits, fallback, misses int) *Window {
		c := NewCollector()
		for outcome, count := range map[string]int{"source": writesSource, "primary": writesPrimary} {
			for range count {
				c.ObserveMigration(windowBase.Add(time.Second), "acme/data", "writes", outcome)
			}
		}
		for outcome, count := range map[string]int{"target_hit": hits, "fallback_source": fallback, "miss": misses} {
			for range count {
				c.ObserveMigration(windowBase.Add(time.Second), "acme/data", "reads", outcome)
			}
		}
		return c.Completed(windowBase.Add(WindowDuration))
	}
	a, b := window(4, 6, 7, 2, 1), window(5, 5, 8, 1, 0)
	if a == nil || len(a.Sketches) != 0 || len(a.Migrations) != 1 {
		t.Fatalf("signal-only window: %#v", a)
	}
	merged, err := mergeWindows(map[string]*Window{"a": a, "b": b})
	if err != nil {
		t.Fatal(err)
	}
	var fleet *MigrationCounter
	for _, scope := range merged {
		if scope.Scope == "fleet" && len(scope.Migrations) == 1 {
			fleet = &scope.Migrations[0]
		}
	}
	if fleet == nil || fleet.Writes["source"] != 9 || fleet.Writes["primary"] != 11 ||
		fleet.Reads["target_hit"] != 15 || fleet.Reads["fallback_source"] != 3 || fleet.Reads["miss"] != 1 {
		t.Fatalf("merged migration signals: %#v", fleet)
	}
}

func TestMergedSketchEqualsUnionProperty(t *testing.T) {
	rnd := rand.New(rand.NewSource(931337)) //nolint:gosec // deterministic property data
	for trial := 0; trial < 100; trial++ {
		a, b := make([]int64, 0, 200), make([]int64, 0, 200)
		union := newHistogram()
		for i := 0; i < 400; i++ {
			v := int64(1 + rnd.Intn(int(maxLatencyMicros)))
			_ = union.RecordValue(v)
			if rnd.Intn(2) == 0 {
				a = append(a, v)
			} else {
				b = append(b, v)
			}
		}
		got, err := mergeWindows(map[string]*Window{"a": collectWindow(t, a, "source"), "b": collectWindow(t, b, "target")})
		if err != nil {
			t.Fatal(err)
		}
		s := findSummary(t, got, "fleet", SeriesClientTotal, OpRead)
		if s.Count != union.TotalCount() || s.P50 != union.ValueAtQuantile(50) || s.P90 != union.ValueAtQuantile(90) ||
			s.P99 != union.ValueAtQuantile(99) || s.P999 != union.ValueAtQuantile(99.9) || s.Max != union.Max() {
			t.Fatalf("trial %d merged summary %#v differs from union count=%d p50=%d p90=%d p99=%d p999=%d max=%d",
				trial, s, union.TotalCount(), union.ValueAtQuantile(50), union.ValueAtQuantile(90), union.ValueAtQuantile(99), union.ValueAtQuantile(99.9), union.Max())
		}
	}
}

func TestHistogramThreeFigureErrorBound(t *testing.T) {
	h := newHistogram()
	values := make([]int64, 20_000)
	for i := range values {
		values[i] = int64(10_000 + i*5_000)
		_ = h.RecordValue(values[i])
	}
	for _, q := range []float64{50, 90, 99, 99.9} {
		// HdrHistogram returns the highest equivalent value; three significant figures bound the
		// quantization error to about 0.1%. Allow one integer microsecond around that bound.
		idx := int(math.Ceil(q/100*float64(len(values)))) - 1
		want, got := values[idx], h.ValueAtQuantile(q)
		if rel := math.Abs(float64(got-want)) / float64(want); rel > 0.0011 {
			t.Errorf("q%.1f got %d want %d: relative error %.6f exceeds 0.11%%", q, got, want, rel)
		}
	}
}

func TestStoreEvictsAtSixtyMinutes(t *testing.T) {
	s := NewStore(10 * time.Second)
	now := windowBase
	s.now = func() time.Time { return now }
	for i := 0; i < maxRingWindows+1; i++ {
		start := windowBase.Add(time.Duration(i) * WindowDuration)
		w := collectWindow(t, []int64{int64(100 + i)}, "a")
		w.Start, w.End = start, start.Add(WindowDuration)
		now = w.End
		if _, err := s.Ingest([]MemberWindow{{ID: "p", Live: true, Telemetry: w}}); err != nil {
			t.Fatal(err)
		}
	}
	ring := s.ring["fleet"]
	if len(ring) != maxRingWindows {
		t.Fatalf("ring length = %d, want %d", len(ring), maxRingWindows)
	}
	if want := windowBase.Add(WindowDuration); !ring[0].Start.Equal(want) {
		t.Fatalf("oldest = %s, want %s", ring[0].Start, want)
	}
}

func TestHeartbeatPayloadBound(t *testing.T) {
	h := newHistogram()
	rnd := rand.New(rand.NewSource(420032)) //nolint:gosec // deterministic value spread
	for i := 0; i < 10_000; i++ {
		// A broad, realistic local-service spread: 50 us through about 2 s with a long tail.
		v := int64(50 + math.Exp(rnd.Float64()*math.Log(40_000)))
		_ = h.RecordValue(v)
	}
	data, err := h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		t.Fatal(err)
	}
	ops := []OpClass{OpRead, OpWrite, OpList, OpDelete, OpMultipart, OpOther}
	series := []string{SeriesClientTotal, SeriesUpstreamTTFB, SeriesUpstreamTotal, SeriesProxyOverhead}
	w := Window{Start: windowBase, End: windowBase.Add(WindowDuration)}
	var payloads [14]int
	for cluster := 0; cluster < 13; cluster++ {
		name := "cluster-" + string(rune('a'+cluster%26)) + string(rune('a'+cluster/26))
		for _, op := range ops {
			w.Counters = append(w.Counters, WindowCounter{Op: op, Cluster: name, Requests: 10_000, BytesIn: 1 << 30, BytesOut: 2 << 30,
				Errors: map[string]int64{"4xx": 17, "5xx": 3}})
			for _, nameSeries := range series {
				w.Sketches = append(w.Sketches, Sketch{Series: nameSeries, Op: op, Cluster: name, Data: data})
			}
		}
		payload, err := json.Marshal(struct {
			Telemetry Window `json:"telemetry"`
		}{Telemetry: w})
		if err != nil {
			t.Fatal(err)
		}
		payloads[cluster+1] = len(payload)
	}
	t.Logf("6 op classes x 4 series: compressed sketch %d bytes; 12-cluster heartbeat %d bytes; 13-cluster heartbeat %d bytes",
		len(data), payloads[12], payloads[13])
	if payloads[12] >= 1<<20 || payloads[13] < 1<<20 {
		t.Fatalf("dense heartbeat ceiling is not 12 clusters: 12=%d, 13=%d, decoder cap=%d", payloads[12], payloads[13], 1<<20)
	}
}

func TestOperationClassesAreBounded(t *testing.T) {
	cases := map[string]OpClass{
		"GetObject": OpRead, "HeadBucket": OpRead, "PutObject": OpWrite, "CreateBucket": OpWrite,
		"ListObjectsV2": OpList, "DeleteObject": OpDelete, "CreateMultipartUpload": OpMultipart,
		"UploadPart": OpMultipart, "ListParts": OpMultipart, "Preflight": OpOther,
	}
	for op, want := range cases {
		if got := OperationClass(op); got != want {
			t.Errorf("%s: got %s want %s", op, got, want)
		}
	}
	classes := []OpClass{OpRead, OpWrite, OpList, OpDelete, OpMultipart, OpOther}
	slices.Sort(classes)
	if len(classes) != 6 {
		t.Fatal(classes)
	}
}

func BenchmarkWindowObserve(b *testing.B) {
	c := NewCollector()
	base := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Observe(Observation{At: base, Operation: "GetObject", Cluster: "a", Status: 200,
			ClientTotal: 2 * time.Millisecond, UpstreamTTFB: time.Millisecond, UpstreamTotal: 1500 * time.Microsecond})
	}
}
