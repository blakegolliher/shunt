package telemetry

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// WindowDuration is the wall-clock interval proxies summarize and carry in heartbeats.
const WindowDuration = 10 * time.Second

// Latency series names are the values accepted by the telemetry API.
const (
	minLatencyMicros   = int64(1)
	maxLatencyMicros   = int64(120 * time.Second / time.Microsecond)
	significantFigures = 3
	maxRingWindows     = int(time.Hour / WindowDuration)

	SeriesClientTotal   = "client_total"   // SeriesClientTotal is client-observed request latency.
	SeriesUpstreamTTFB  = "upstream_ttfb"  // SeriesUpstreamTTFB covers upstream request headers through response headers.
	SeriesUpstreamTotal = "upstream_total" // SeriesUpstreamTotal covers upstream request headers through the last body byte.
	SeriesProxyOverhead = "proxy_overhead" // SeriesProxyOverhead is client total less upstream total.
)

// OpClass is the bounded operation dimension carried in window telemetry.
type OpClass string

// Operation classes are the bounded values emitted by proxies, plus the control-node union.
const (
	OpRead      OpClass = "read"      // OpRead groups object and bucket reads.
	OpWrite     OpClass = "write"     // OpWrite groups non-multipart writes.
	OpList      OpClass = "list"      // OpList groups listing operations.
	OpDelete    OpClass = "delete"    // OpDelete groups deletion operations.
	OpMultipart OpClass = "multipart" // OpMultipart groups the multipart lifecycle and parts.
	OpOther     OpClass = "other"     // OpOther groups operations outside the five named classes.
	OpAll       OpClass = "all"       // OpAll is the control node's union of all classes.
)

// OperationClass maps the S3 classifier's AWS operation name onto the telemetry classes. It is
// deliberately independent from internal/migrate's routing classes.
func OperationClass(op string) OpClass {
	if strings.Contains(op, "Multipart") || op == "UploadPart" || op == "UploadPartCopy" || op == "ListParts" {
		return OpMultipart
	}
	if strings.HasPrefix(op, "List") {
		return OpList
	}
	if strings.HasPrefix(op, "Delete") || op == "AbortMultipartUpload" {
		return OpDelete
	}
	if strings.HasPrefix(op, "Get") || strings.HasPrefix(op, "Head") || op == "SelectObjectContent" {
		return OpRead
	}
	if strings.HasPrefix(op, "Put") || strings.HasPrefix(op, "Create") || strings.HasPrefix(op, "Copy") ||
		strings.HasPrefix(op, "Post") || op == "RestoreObject" || op == "CompleteMultipartUpload" {
		return OpWrite
	}
	return OpOther
}

// Observation is the one record proxy.Handler.finish contributes to its current window.
type Observation struct {
	At            time.Time
	Operation     string
	Cluster       string
	Status        int
	BytesIn       int64
	BytesOut      int64
	ClientTotal   time.Duration
	UpstreamTTFB  time.Duration
	UpstreamTotal time.Duration
}

// Sketch is one compressed HdrHistogram in a completed proxy window. Data is JSON base64.
type Sketch struct {
	Series  string  `json:"series"`
	Op      OpClass `json:"op"`
	Cluster string  `json:"cluster"`
	Data    []byte  `json:"data"`
}

// WindowCounter is one exact counter tuple in a completed proxy window.
type WindowCounter struct {
	Op       OpClass          `json:"op"`
	Cluster  string           `json:"cluster"`
	Requests int64            `json:"requests"`
	BytesIn  int64            `json:"bytes_in"`
	BytesOut int64            `json:"bytes_out"`
	Errors   map[string]int64 `json:"errors,omitempty"`
}

// MigrationCounter is one placement's exact routing outcomes in a completed window. It is kept
// separate from the general request dimensions because bucket is intentionally not a global
// telemetry label; only placements currently moving call ObserveMigration.
type MigrationCounter struct {
	Bucket string           `json:"bucket"`
	Writes map[string]int64 `json:"writes,omitempty"` // source | primary
	Reads  map[string]int64 `json:"reads,omitempty"`  // target_hit | fallback_source | miss
}

// Window is the last completed telemetry window carried by a proxy heartbeat.
type Window struct {
	Start      time.Time          `json:"start"`
	End        time.Time          `json:"end"`
	Sketches   []Sketch           `json:"sketches"`
	Counters   []WindowCounter    `json:"counters"`
	Migrations []MigrationCounter `json:"migrations,omitempty"`
}

type sketchKey struct {
	series, cluster string
	op              OpClass
}

type counterKey struct {
	cluster string
	op      OpClass
}

// Collector owns only the current raw sketches and one encoded completed window. Histograms are
// allocated lazily on the first observation of a tuple.
type Collector struct {
	mu         sync.Mutex
	start      time.Time
	sketches   map[sketchKey]*hdrhistogram.Histogram
	counters   map[counterKey]*WindowCounter
	migrations map[string]*MigrationCounter
	last       *Window
}

// NewCollector returns an empty window collector.
func NewCollector() *Collector { return &Collector{} }

func alignedStart(t time.Time) time.Time {
	return time.Unix(0, t.UnixNano()-t.UnixNano()%WindowDuration.Nanoseconds()).UTC()
}

func (c *Collector) rotateLocked(now time.Time) {
	start := alignedStart(now)
	if c.start.IsZero() {
		c.start = start
		return
	}
	if !start.After(c.start) {
		return
	}
	if len(c.sketches) > 0 || len(c.migrations) > 0 {
		w := &Window{Start: c.start, End: c.start.Add(WindowDuration)}
		keys := make([]sketchKey, 0, len(c.sketches))
		for k := range c.sketches {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b sketchKey) int {
			if n := strings.Compare(a.series, b.series); n != 0 {
				return n
			}
			if n := strings.Compare(string(a.op), string(b.op)); n != 0 {
				return n
			}
			return strings.Compare(a.cluster, b.cluster)
		})
		for _, k := range keys {
			data, err := c.sketches[k].Encode(hdrhistogram.V2CompressedEncodingCookieBase)
			if err == nil { // the fixed histogram parameters are always encodable
				w.Sketches = append(w.Sketches, Sketch{Series: k.series, Op: k.op, Cluster: k.cluster, Data: data})
			}
		}
		counterKeys := make([]counterKey, 0, len(c.counters))
		for k := range c.counters {
			counterKeys = append(counterKeys, k)
		}
		slices.SortFunc(counterKeys, func(a, b counterKey) int {
			if n := strings.Compare(string(a.op), string(b.op)); n != 0 {
				return n
			}
			return strings.Compare(a.cluster, b.cluster)
		})
		for _, k := range counterKeys {
			v := *c.counters[k]
			v.Errors = cloneErrors(v.Errors)
			w.Counters = append(w.Counters, v)
		}
		for _, bucket := range slices.Sorted(maps.Keys(c.migrations)) {
			v := *c.migrations[bucket]
			v.Writes = maps.Clone(v.Writes)
			v.Reads = maps.Clone(v.Reads)
			w.Migrations = append(w.Migrations, v)
		}
		c.last = w
	}
	c.start, c.sketches, c.counters, c.migrations = start, nil, nil, nil
}

func cloneErrors(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func latencyMicros(d time.Duration) int64 {
	v := d.Microseconds()
	if v < minLatencyMicros {
		return minLatencyMicros
	}
	if v > maxLatencyMicros {
		return maxLatencyMicros
	}
	return v
}

func newHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(minLatencyMicros, maxLatencyMicros, significantFigures)
}

func (c *Collector) record(k sketchKey, d time.Duration) {
	if c.sketches == nil {
		c.sketches = map[sketchKey]*hdrhistogram.Histogram{}
	}
	h := c.sketches[k]
	if h == nil {
		h = newHistogram()
		c.sketches[k] = h
	}
	_ = h.RecordValue(latencyMicros(d))
}

// Observe records one completed request.
func (c *Collector) Observe(o Observation) {
	if o.At.IsZero() {
		o.At = time.Now()
	}
	if o.Cluster == "" {
		o.Cluster = "none"
	}
	op := OperationClass(o.Operation)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotateLocked(o.At)
	c.record(sketchKey{series: SeriesClientTotal, op: op, cluster: o.Cluster}, o.ClientTotal)
	if o.UpstreamTTFB > 0 {
		c.record(sketchKey{series: SeriesUpstreamTTFB, op: op, cluster: o.Cluster}, o.UpstreamTTFB)
	}
	if o.UpstreamTotal > 0 {
		c.record(sketchKey{series: SeriesUpstreamTotal, op: op, cluster: o.Cluster}, o.UpstreamTotal)
		overhead := o.ClientTotal - o.UpstreamTotal
		if overhead < 0 {
			overhead = 0
		}
		c.record(sketchKey{series: SeriesProxyOverhead, op: op, cluster: o.Cluster}, overhead)
	}
	if c.counters == nil {
		c.counters = map[counterKey]*WindowCounter{}
	}
	ck := counterKey{op: op, cluster: o.Cluster}
	count := c.counters[ck]
	if count == nil {
		count = &WindowCounter{Op: op, Cluster: o.Cluster}
		c.counters[ck] = count
	}
	count.Requests++
	count.BytesIn += o.BytesIn
	count.BytesOut += o.BytesOut
	if o.Status == 0 || o.Status >= 400 {
		if count.Errors == nil {
			count.Errors = map[string]int64{}
		}
		count.Errors[StatusClass(o.Status)]++
	}
}

// ObserveMigration records one bounded migration-routing signal in the current telemetry window.
func (c *Collector) ObserveMigration(at time.Time, bucket, series, outcome string) {
	if at.IsZero() {
		at = time.Now()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotateLocked(at)
	if c.migrations == nil {
		c.migrations = map[string]*MigrationCounter{}
	}
	v := c.migrations[bucket]
	if v == nil {
		v = &MigrationCounter{Bucket: bucket}
		c.migrations[bucket] = v
	}
	switch series {
	case "writes":
		if v.Writes == nil {
			v.Writes = map[string]int64{}
		}
		v.Writes[outcome]++
	case "reads":
		if v.Reads == nil {
			v.Reads = map[string]int64{}
		}
		v.Reads[outcome]++
	}
}

// Completed rotates on the clock and returns the same immutable last non-empty completed window
// until another non-empty window closes.
func (c *Collector) Completed(now time.Time) *Window {
	if now.IsZero() {
		now = time.Now()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotateLocked(now)
	return c.last
}

// Summary is one emitted percentile tuple.
type Summary struct {
	Series string  `json:"series"`
	Op     OpClass `json:"op"`
	Count  int64   `json:"count"`
	P50    int64   `json:"p50_us"`
	P90    int64   `json:"p90_us"`
	P99    int64   `json:"p99_us"`
	P999   int64   `json:"p999_us"`
	Max    int64   `json:"max_us"`
}

// ScopeWindow is one control-node summary window for a fleet, cluster, or proxy scope.
type ScopeWindow struct {
	Scope      string             `json:"scope"`
	Start      time.Time          `json:"start"`
	End        time.Time          `json:"end"`
	Series     []Summary          `json:"series"`
	Counters   []WindowCounter    `json:"counters"`
	Migrations []MigrationCounter `json:"migrations,omitempty"`
}

// MemberWindow is the part of a control.Member the telemetry store needs.
type MemberWindow struct {
	ID        string
	Live      bool
	Telemetry *Window
}

// Store merges proxy sketches and retains only emitted summaries.
type Store struct {
	mu       sync.Mutex
	leaseTTL time.Duration
	now      func() time.Time
	pending  map[int64]map[string]*Window
	merged   map[int64]bool
	ring     map[string][]ScopeWindow
	localID  string
	local    *Collector
}

// NewStore returns a 60-minute summary store. leaseTTL controls the half-lease merge deadline.
func NewStore(leaseTTL time.Duration) *Store {
	if leaseTTL <= 0 {
		leaseTTL = 10 * time.Second
	}
	return &Store{leaseTTL: leaseTTL, now: time.Now, pending: map[int64]map[string]*Window{}, merged: map[int64]bool{}, ring: map[string][]ScopeWindow{}}
}

// SetLocal makes a lab proxy's collector feed this store when API reads flush it.
func (s *Store) SetLocal(id string, c *Collector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localID, s.local = id, c
}

// FlushLocal rotates and ingests the lab proxy's collector.
func (s *Store) FlushLocal() ([]time.Time, error) {
	s.mu.Lock()
	id, c, now := s.localID, s.local, s.now
	s.mu.Unlock()
	if c == nil {
		return nil, nil
	}
	return s.Ingest([]MemberWindow{{ID: id, Live: true, Telemetry: c.Completed(now())}})
}

// Ingest adds the last completed window from each member and merges every ready start. It returns
// the starts merged by this call, for telemetry SSE events.
func (s *Store) Ingest(members []MemberWindow) ([]time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := map[string]bool{}
	for _, m := range members {
		if m.Live {
			live[m.ID] = true
		}
		if m.Telemetry == nil || len(m.Telemetry.Sketches) == 0 && len(m.Telemetry.Migrations) == 0 {
			continue
		}
		start := m.Telemetry.Start.UnixNano()
		if s.merged[start] {
			continue
		}
		if s.pending[start] == nil {
			s.pending[start] = map[string]*Window{}
		}
		s.pending[start][m.ID] = m.Telemetry
	}
	starts := make([]int64, 0, len(s.pending))
	for start := range s.pending {
		starts = append(starts, start)
	}
	slices.Sort(starts)
	now := s.now()
	var merged []time.Time
	for _, start := range starts {
		windows := s.pending[start]
		all := len(live) > 0
		for id := range live {
			if windows[id] == nil {
				all = false
				break
			}
		}
		end := time.Unix(0, start).UTC().Add(WindowDuration)
		if !all && now.Before(end.Add(s.leaseTTL/2)) {
			continue
		}
		scopes, err := mergeWindows(windows)
		if err != nil {
			return merged, err
		}
		for _, sw := range scopes {
			ring := s.ring[sw.Scope]
			ring = append(ring, sw)
			if len(ring) > maxRingWindows {
				ring = append([]ScopeWindow(nil), ring[len(ring)-maxRingWindows:]...)
			}
			s.ring[sw.Scope] = ring
		}
		delete(s.pending, start)
		s.merged[start] = true
		merged = append(merged, time.Unix(0, start).UTC())
	}
	if len(s.merged) > maxRingWindows*2 {
		cutoff := now.Add(-2 * time.Hour).UnixNano()
		for start := range s.merged {
			if start < cutoff {
				delete(s.merged, start)
			}
		}
	}
	return merged, nil
}

type summaryKey struct {
	series string
	op     OpClass
}

func mergeHistogram(dst map[summaryKey]*hdrhistogram.Histogram, key summaryKey, src *hdrhistogram.Histogram) {
	h := dst[key]
	if h == nil {
		h = newHistogram()
		dst[key] = h
	}
	h.Merge(src)
}

func addCounter(dst map[OpClass]*WindowCounter, src WindowCounter) {
	d := dst[src.Op]
	if d == nil {
		d = &WindowCounter{Op: src.Op, Errors: map[string]int64{}}
		dst[src.Op] = d
	}
	d.Requests += src.Requests
	d.BytesIn += src.BytesIn
	d.BytesOut += src.BytesOut
	for k, v := range src.Errors {
		d.Errors[k] += v
	}
}

func addMigration(dst map[string]*MigrationCounter, src MigrationCounter) {
	d := dst[src.Bucket]
	if d == nil {
		d = &MigrationCounter{Bucket: src.Bucket, Writes: map[string]int64{}, Reads: map[string]int64{}}
		dst[src.Bucket] = d
	}
	for k, v := range src.Writes {
		d.Writes[k] += v
	}
	for k, v := range src.Reads {
		d.Reads[k] += v
	}
}

func mergeWindows(windows map[string]*Window) ([]ScopeWindow, error) {
	if len(windows) == 0 {
		return nil, nil
	}
	type rawScope struct {
		h map[summaryKey]*hdrhistogram.Histogram
		c map[OpClass]*WindowCounter
		m map[string]*MigrationCounter
	}
	newRaw := func() *rawScope {
		return &rawScope{h: map[summaryKey]*hdrhistogram.Histogram{}, c: map[OpClass]*WindowCounter{}, m: map[string]*MigrationCounter{}}
	}
	raw := map[string]*rawScope{}
	get := func(scope string) *rawScope {
		r := raw[scope]
		if r == nil {
			r = newRaw()
			raw[scope] = r
		}
		return r
	}
	var start, end time.Time
	for _, w := range windows {
		if start.IsZero() || w.Start.Before(start) {
			start, end = w.Start, w.End
		}
	}
	addAll := func(r *rawScope) {
		keys := make([]summaryKey, 0, len(r.h))
		for k := range r.h {
			keys = append(keys, k)
		}
		for _, k := range keys {
			mergeHistogram(r.h, summaryKey{series: k.series, op: OpAll}, r.h[k])
		}
		counts := make([]WindowCounter, 0, len(r.c))
		for _, c := range r.c {
			counts = append(counts, *c)
		}
		for _, c := range counts {
			c.Op = OpAll
			addCounter(r.c, c)
		}
	}
	finalize := func(scope string, r *rawScope) ScopeWindow {
		addAll(r)
		sw := ScopeWindow{Scope: scope, Start: start, End: end}
		keys := make([]summaryKey, 0, len(r.h))
		for k := range r.h {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b summaryKey) int {
			if n := strings.Compare(a.series, b.series); n != 0 {
				return n
			}
			return strings.Compare(string(a.op), string(b.op))
		})
		for _, k := range keys {
			h := r.h[k]
			sw.Series = append(sw.Series, Summary{Series: k.series, Op: k.op, Count: h.TotalCount(),
				P50: h.ValueAtQuantile(50), P90: h.ValueAtQuantile(90), P99: h.ValueAtQuantile(99),
				P999: h.ValueAtQuantile(99.9), Max: h.Max()})
		}
		ops := make([]OpClass, 0, len(r.c))
		for op := range r.c {
			ops = append(ops, op)
		}
		slices.Sort(ops)
		for _, op := range ops {
			c := *r.c[op]
			c.Cluster = ""
			c.Errors = cloneErrors(c.Errors)
			sw.Counters = append(sw.Counters, c)
		}
		for _, bucket := range slices.Sorted(maps.Keys(r.m)) {
			m := *r.m[bucket]
			m.Writes = maps.Clone(m.Writes)
			m.Reads = maps.Clone(m.Reads)
			sw.Migrations = append(sw.Migrations, m)
		}
		return sw
	}
	// A proxy scope is reduced and released one proxy at a time. Keeping one raw histogram per
	// proxy tuple for a 1,000-member fleet would otherwise turn a bounded merge into gigabytes.
	proxies := make([]string, 0, len(windows))
	for proxy := range windows {
		proxies = append(proxies, proxy)
	}
	slices.Sort(proxies)
	out := make([]ScopeWindow, 0, len(windows)+len(raw))
	for _, proxy := range proxies {
		w := windows[proxy]
		proxyRaw := newRaw()
		for _, sk := range w.Sketches {
			h, err := hdrhistogram.Decode(sk.Data)
			if err != nil {
				return nil, fmt.Errorf("telemetry %s %s/%s/%s: %w", proxy, sk.Series, sk.Op, sk.Cluster, err)
			}
			key := summaryKey{series: sk.Series, op: sk.Op}
			mergeHistogram(get("fleet").h, key, h)
			mergeHistogram(proxyRaw.h, key, h)
			if sk.Cluster != "" && sk.Cluster != "none" {
				mergeHistogram(get("cluster:"+sk.Cluster).h, key, h)
			}
		}
		for _, count := range w.Counters {
			addCounter(get("fleet").c, count)
			addCounter(proxyRaw.c, count)
			if count.Cluster != "" && count.Cluster != "none" {
				addCounter(get("cluster:"+count.Cluster).c, count)
			}
		}
		for _, migration := range w.Migrations {
			addMigration(get("fleet").m, migration)
			addMigration(proxyRaw.m, migration)
		}
		out = append(out, finalize("proxy:"+proxy, proxyRaw))
	}
	scopes := make([]string, 0, len(raw))
	for scope := range raw {
		scopes = append(scopes, scope)
	}
	slices.Sort(scopes)
	for _, scope := range scopes {
		out = append(out, finalize(scope, raw[scope]))
	}
	slices.SortFunc(out, func(a, b ScopeWindow) int { return strings.Compare(a.Scope, b.Scope) })
	return out, nil
}

// Latest returns the newest window for every scope.
func (s *Store) Latest() []ScopeWindow {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ScopeWindow, 0, len(s.ring))
	for _, ring := range s.ring {
		if len(ring) > 0 {
			out = append(out, ring[len(ring)-1])
		}
	}
	slices.SortFunc(out, func(a, b ScopeWindow) int { return strings.Compare(a.Scope, b.Scope) })
	return out
}

// Series returns matching points in chronological order.
func (s *Store) Series(scope, series string, op OpClass, from, to time.Time) []SeriesPoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []SeriesPoint
	for _, w := range s.ring[scope] {
		if !from.IsZero() && w.End.Before(from) || !to.IsZero() && w.Start.After(to) {
			continue
		}
		for _, v := range w.Series {
			if v.Series == series && v.Op == op {
				out = append(out, SeriesPoint{Start: w.Start, End: w.End, Summary: v})
			}
		}
	}
	return out
}

// SeriesPoint is one point returned by GET /v1/telemetry/series.
type SeriesPoint struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Summary
}
