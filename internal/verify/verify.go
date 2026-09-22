// Package verify is the client's view of a bucket, checked continuously: a workload of random
// writes, deletes and reads against an S3 endpoint, each held to account by an in-memory model of
// what the client did. `shunt verify` runs it against a live endpoint during an operator procedure
// (docs/walkthrough.md); the POC-4 property test runs the same model through its in-process rig.
//
// The rules a migration must keep, from the client's side:
//
//	a successful write is readable at once, with the bytes that were written;
//	a successful delete stays deleted (a mover may put an object back for a moment; it must be gone
//	again within Grace, ADR-0004 race 1);
//	every request succeeds.
package verify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// Client sends SigV4-signed path-style requests for one bucket.
type Client struct {
	Endpoint string // scheme://host:port
	Bucket   string
	Region   string
	Creds    sigv4.Credentials
	HTTP     *http.Client
	// DebugRoute sends X-Shunt-Debug: 1, so a shunt with features.debug_route_header answers with
	// X-Shunt-Route.
	DebugRoute bool
}

// Reply is one response, read whole.
type Reply struct {
	Status   int
	Body     []byte
	Route    string // X-Shunt-Route: "<side> <cluster>", when asked for and given
	Header   http.Header
	Duration time.Duration // HTTP round trip through the last response byte; excludes request signing
}

// Do sends one request for key with body (nil for none) and extra headers.
func (c *Client) Do(ctx context.Context, method, key string, body []byte, hdr map[string]string) (Reply, error) {
	target := strings.TrimRight(c.Endpoint, "/") + "/" + c.Bucket + "/" + escapeKey(key)
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return Reply{}, err
	}
	req.ContentLength = int64(len(body))
	if len(body) == 0 {
		req.Body = http.NoBody
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if c.DebugRoute {
		req.Header.Set("X-Shunt-Debug", "1")
	}
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	sigv4.Sign(req, c.Creds, region, sigv4.UnsignedPayload, time.Now())
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	started := time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		return Reply{}, err
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	b, err := io.ReadAll(resp.Body)
	return Reply{Status: resp.StatusCode, Body: b, Route: resp.Header.Get("X-Shunt-Route"), Header: resp.Header, Duration: time.Since(started)}, err
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = sigv4.Escape(p)
	}
	return strings.Join(parts, "/")
}

// Key is the model of one key: what the client last did to it. Hold the lock across the request
// and the update, so the model is never a guess.
type Key struct {
	mu      sync.Mutex
	Present bool
	Body    string
	Deleted time.Time // when the delete that emptied it returned
	// Unsure: the run ended while a write or delete of this key was in flight, so the backend may
	// hold either outcome; the final read-back skips it.
	Unsure bool
}

// Lock holds the key across a request and the model update that follows it.
func (k *Key) Lock() { k.mu.Lock() }

// Unlock releases the key.
func (k *Key) Unlock() { k.mu.Unlock() }

func (k *Key) String() string {
	if k.Present {
		return "present:" + k.Body
	}
	return "absent"
}

// Options shape a Run.
type Options struct {
	Workers  int           // default 8
	Keys     int           // default 200
	Prefix   string        // every key starts with it; default verify/<unix nanos>/
	Duration time.Duration // 0: until ctx is done
	Grace    time.Duration // how long a resurrected delete may stay visible; default 5s
	Seed     int64         // 0: from the clock
	Interval time.Duration // progress lines to Progress; 0: none
	Progress io.Writer
	Cleanup  bool // delete every key still present at the end
}

// Report is what a Run saw.
type Report struct {
	Endpoint      string               `json:"endpoint"`
	Bucket        string               `json:"bucket"`
	Prefix        string               `json:"prefix"`
	Seed          int64                `json:"seed"`
	Started       time.Time            `json:"started"`
	Seconds       float64              `json:"seconds"`
	Ops           int64                `json:"ops"`
	Puts          int64                `json:"puts"`
	Gets          int64                `json:"gets"`
	Deletes       int64                `json:"deletes"`
	Errors        int64                `json:"errors"`
	ErrorSamples  []string             `json:"error_samples,omitempty"` // the first 50
	Resurrections int64                `json:"resurrections_withdrawn"` // deleted keys seen again, then gone within the grace
	Writes        map[string]int64     `json:"writes_by_route"`         // X-Shunt-Route of successful PUTs
	Reads         map[string]int64     `json:"reads_by_route"`          // X-Shunt-Route of successful GETs
	WriteSides    map[string]int64     `json:"writes_by_side"`          // primary | source | unrouted
	ReadSides     map[string]int64     `json:"reads_by_side"`
	Present       int                  `json:"keys_present_at_end"`
	ReadBack      int                  `json:"keys_read_back_at_end"` // present keys read back whole after the workload stopped, less any whose last write the end cut off
	CleanupErrors int64                `json:"cleanup_errors,omitempty"`
	LatencyP50US  int64                `json:"latency_p50_us"`
	LatencyP99US  int64                `json:"latency_p99_us"`
	Telemetry     *TelemetryComparison `json:"telemetry,omitempty"`
	LatencyWindow *telemetry.Window    `json:"-"`
}

type runner struct {
	c    *Client
	o    Options
	keys []*Key

	ops, puts, gets, deletes, errors, resurrections atomic.Int64

	mu      sync.Mutex
	samples []string
	writes  map[string]int64
	reads   map[string]int64
	latency *hdrhistogram.Histogram
	windows *telemetry.Collector
}

// Run drives the workload until Duration passes or ctx is done, and reports.
func Run(ctx context.Context, c *Client, o Options) Report {
	if o.Workers <= 0 {
		o.Workers = 8
	}
	if o.Keys <= 0 {
		o.Keys = 200
	}
	if o.Grace <= 0 {
		o.Grace = 5 * time.Second
	}
	if o.Seed == 0 {
		o.Seed = time.Now().UnixNano()
	}
	if o.Prefix == "" {
		o.Prefix = fmt.Sprintf("verify/%d/", time.Now().UnixNano())
	}
	if o.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Duration)
		defer cancel()
	}
	r := &runner{c: c, o: o, keys: make([]*Key, o.Keys), writes: map[string]int64{}, reads: map[string]int64{},
		latency: hdrhistogram.New(1, int64(120*time.Second/time.Microsecond), 3), windows: telemetry.NewCollector()}
	for i := range r.keys {
		r.keys[i] = &Key{}
	}
	start := time.Now()
	stopProgress := r.progress(ctx, start)
	var wg sync.WaitGroup
	for w := 0; w < o.Workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r.worker(ctx, rand.New(rand.NewSource(seed))) //nolint:gosec // G404: test load, not crypto
		}(o.Seed + int64(w) + 1)
	}
	wg.Wait()
	stopProgress()
	readBack := r.readBack()
	rep := r.report(start)
	rep.LatencyWindow = r.windows.Completed(time.Now())
	rep.ReadBack = readBack
	if o.Cleanup {
		rep.CleanupErrors = r.cleanup()
	}
	return rep
}

func (r *runner) key(i int) string { return fmt.Sprintf("%s%04d", r.o.Prefix, i) }

func (r *runner) fail(format string, a ...any) {
	r.errors.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) < 50 {
		r.samples = append(r.samples, time.Now().UTC().Format(time.RFC3339Nano)+" "+fmt.Sprintf(format, a...))
	}
}

func (r *runner) tally(m map[string]int64, route string) {
	if route == "" {
		route = "unrouted"
	}
	r.mu.Lock()
	m[route]++
	r.mu.Unlock()
}

func verifyOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetObject"
	case http.MethodPut:
		return "PutObject"
	case http.MethodDelete:
		return "DeleteObject"
	}
	return "Unknown"
}

func (r *runner) do(ctx context.Context, method, key string, body []byte, hdr map[string]string) (Reply, error) {
	rep, err := r.c.Do(ctx, method, key, body, hdr)
	if err != nil {
		return rep, err
	}
	d := rep.Duration
	micros := d.Microseconds()
	if micros < 1 {
		micros = 1
	}
	if micros > int64(120*time.Second/time.Microsecond) {
		micros = int64(120 * time.Second / time.Microsecond)
	}
	r.mu.Lock()
	_ = r.latency.RecordValue(micros)
	r.mu.Unlock()
	r.windows.Observe(telemetry.Observation{At: time.Now(), Operation: verifyOperation(method), Cluster: "client", Status: rep.Status,
		BytesIn: int64(len(body)), BytesOut: int64(len(rep.Body)), ClientTotal: d})
	return rep, nil
}

func (r *runner) worker(ctx context.Context, rnd *rand.Rand) {
	for ctx.Err() == nil {
		i := rnd.Intn(len(r.keys))
		k := r.keys[i]
		k.Lock()
		switch n := rnd.Intn(10); {
		case n < 4:
			r.put(ctx, i, k, rnd)
		case n < 6:
			r.del(ctx, i, k)
		default:
			r.get(ctx, i, k)
		}
		k.Unlock()
	}
}

// done reports an error caused only by the run ending; those are not failures.
func done(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() != nil
}

func (r *runner) put(ctx context.Context, i int, k *Key, rnd *rand.Rand) {
	body := fmt.Sprintf("v%d-%d", i, rnd.Int63())
	rep, err := r.do(ctx, http.MethodPut, r.key(i), []byte(body), nil)
	if done(ctx, err) {
		k.Unsure = true
		return
	}
	r.ops.Add(1)
	r.puts.Add(1)
	switch {
	case err != nil:
		r.fail("PUT %s: %v", r.key(i), err)
	case rep.Status != http.StatusOK:
		r.fail("PUT %s: HTTP %d %s", r.key(i), rep.Status, snippet(rep.Body))
	default:
		k.Present, k.Body = true, body
		r.tally(r.writes, rep.Route)
	}
}

func (r *runner) del(ctx context.Context, i int, k *Key) {
	rep, err := r.do(ctx, http.MethodDelete, r.key(i), nil, nil)
	if done(ctx, err) {
		k.Unsure = true
		return
	}
	r.ops.Add(1)
	r.deletes.Add(1)
	switch {
	case err != nil:
		r.fail("DELETE %s: %v", r.key(i), err)
	case rep.Status != http.StatusNoContent:
		r.fail("DELETE %s: HTTP %d %s", r.key(i), rep.Status, snippet(rep.Body))
	default:
		k.Present, k.Body, k.Deleted = false, "", time.Now()
	}
}

func (r *runner) get(ctx context.Context, i int, k *Key) {
	rep, err := r.do(ctx, http.MethodGet, r.key(i), nil, nil)
	if done(ctx, err) {
		return
	}
	r.ops.Add(1)
	r.gets.Add(1)
	switch {
	case err != nil:
		r.fail("GET %s: %v", r.key(i), err)
	case k.Present && rep.Status != http.StatusOK:
		r.fail("GET %s: HTTP %d, but it was written (%q) and never deleted", r.key(i), rep.Status, k.Body)
	case k.Present && string(rep.Body) != k.Body:
		r.fail("GET %s: read %q, wrote %q", r.key(i), snippet(rep.Body), k.Body)
	case k.Present:
		r.tally(r.reads, rep.Route)
	case rep.Status == http.StatusOK:
		if r.gone(ctx, i) {
			r.resurrections.Add(1)
			return
		}
		r.fail("GET %s: deleted %s ago and still readable after %s", r.key(i), time.Since(k.Deleted).Round(time.Millisecond), r.o.Grace)
	case rep.Status != http.StatusNotFound:
		r.fail("GET %s: HTTP %d, want 404 for a deleted key", r.key(i), rep.Status)
	}
}

// gone waits up to Grace for a deleted key to read 404 again.
func (r *runner) gone(ctx context.Context, i int) bool {
	for end := time.Now().Add(r.o.Grace); time.Now().Before(end) && ctx.Err() == nil; {
		time.Sleep(50 * time.Millisecond)
		if rep, err := r.do(context.WithoutCancel(ctx), http.MethodGet, r.key(i), nil, nil); err == nil && rep.Status == http.StatusNotFound {
			return true
		}
	}
	return ctx.Err() != nil // the run ended while waiting: not a finding
}

func (r *runner) progress(ctx context.Context, start time.Time) func() {
	if r.o.Interval <= 0 || r.o.Progress == nil {
		return func() {}
	}
	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(r.o.Interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				rep := r.report(start)
				// By route, not side: "primary" names a different cluster before and after a ramp.
				_, _ = fmt.Fprintf(r.o.Progress, "verify: %s %d ops, %d errors; writes %s; reads %s\n",
					time.Since(start).Round(time.Second), rep.Ops, rep.Errors, share(rep.Writes), share(rep.Reads))
			}
		}
	}()
	return func() {
		close(stop)
		<-finished
	}
}

func (r *runner) report(start time.Time) Report {
	rep := Report{Endpoint: r.c.Endpoint, Bucket: r.c.Bucket, Prefix: r.o.Prefix, Seed: r.o.Seed, Started: start.UTC(),
		Seconds: time.Since(start).Seconds(), Ops: r.ops.Load(), Puts: r.puts.Load(), Gets: r.gets.Load(), Deletes: r.deletes.Load(),
		Errors: r.errors.Load(), Resurrections: r.resurrections.Load(),
		Writes: map[string]int64{}, Reads: map[string]int64{}, WriteSides: map[string]int64{}, ReadSides: map[string]int64{}}
	r.mu.Lock()
	rep.LatencyP50US = r.latency.ValueAtQuantile(50)
	rep.LatencyP99US = r.latency.ValueAtQuantile(99)
	rep.ErrorSamples = append([]string(nil), r.samples...)
	for route, n := range r.writes {
		rep.Writes[route] = n
		rep.WriteSides[side(route)] += n
	}
	for route, n := range r.reads {
		rep.Reads[route] = n
		rep.ReadSides[side(route)] += n
	}
	r.mu.Unlock()
	for _, k := range r.keys {
		k.Lock()
		if k.Present {
			rep.Present++
		}
		k.Unlock()
	}
	return rep
}

func side(route string) string {
	s, _, _ := strings.Cut(route, " ")
	return s
}

// share renders per-side counts with percentages, e.g. "primary 510 (51%), source 490 (49%)".
func share(m map[string]int64) string {
	var total int64
	names := make([]string, 0, len(m))
	for k, v := range m {
		total += v
		names = append(names, k)
	}
	if total == 0 {
		return "none yet"
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, k := range names {
		parts = append(parts, fmt.Sprintf("%s %d (%.0f%%)", k, m[k], 100*float64(m[k])/float64(total)))
	}
	return strings.Join(parts, ", ")
}

// Share renders a Report's per-side map the way progress lines do.
func Share(m map[string]int64) string { return share(m) }

// readBack reads every key the model holds as present once more, after the workers have stopped,
// so a run's last word on each surviving key is a checked read rather than whatever it did last.
func (r *runner) readBack() int {
	ctx := context.Background()
	n := 0
	for i, k := range r.keys {
		k.Lock()
		if k.Present && !k.Unsure {
			r.get(ctx, i, k)
			n++
		}
		k.Unlock()
	}
	return n
}

func (r *runner) cleanup() int64 {
	var errs int64
	ctx := context.Background()
	for i, k := range r.keys {
		k.Lock()
		if k.Present {
			if rep, err := r.do(ctx, http.MethodDelete, r.key(i), nil, nil); err != nil || rep.Status != http.StatusNoContent {
				errs++
			} else {
				k.Present = false
			}
		}
		k.Unlock()
	}
	return errs
}

func snippet(b []byte) string {
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}
