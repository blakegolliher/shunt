package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// The fleet property (ADR-0016, docs/design/distributed.md §12.9). Two proxies serve one directory
// file: A installs every change the moment the control node writes it, and B reads the file only
// after a lag, as a proxy polling a shared file does. Clients own disjoint keys and write, delete
// and read each one through A or B at random, so the same key is written through both proxies in
// turn while ramp steps and migrate start roll out. The property is the client's view: every read,
// through either proxy, returns the key's last acknowledged write (or 404 after its last
// acknowledged delete). A 503 is not an acknowledgement; the client retries.
//
// TestFleetStepsKeepTheClientsView runs the steps through the control node with B as a fleet
// member: fenced, held, and with a stretch where B's lease lapses. TestFleetWithoutTheFence is the
// negative control: the same run with the steps written straight to the directory, which must
// find a violation, or the positive run proves nothing.

type fleetProxy struct {
	name  string
	front *httptest.Server
	h     *Handler
	dir   *directory.FileDir
}

// secondProxy is another shunt over the rig's directory file: its own FileDir, cluster registry,
// metrics and front server, like a second process on the same shared file.
func secondProxy(t *testing.T, m *mixedRig) fleetProxy {
	t.Helper()
	secrets := map[string]string{"env:G": "garage-cluster-secret", "env:M": "minio-cluster-secret"}
	set := upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, func(ref string) (string, error) { return secrets[ref], nil })
	t.Cleanup(set.Close)
	dir, err := directory.Open(m.dirPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := set.Apply(dir.Snapshot().File().Clusters); err != nil {
		t.Fatal(err)
	}
	dir.Prepare = func(f *directory.File) error {
		_, _, aerr := set.Apply(f.Clusters)
		return aerr
	}
	h := New(Handler{
		Mode: ModeResign, Store: mapStore{acmeAK: {AccessKey: acmeAK, Secret: acmeSK, Tenant: "acme"}}, Clusters: set, Dir: dir, Rewrite: true,
		Domains: s3.NewDomains([]string{"*.shunt.example.com"}), Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil),
		Slow: telemetry.NewSlowRing(10, time.Hour), IdleTimeout: 2 * time.Second, MetadataTimeout: 5 * time.Second, Via: "1.1 shunt/test",
	}, 64<<10)
	front := httptest.NewServer(h)
	front.Config.ErrorLog = nil
	t.Cleanup(front.Close)
	return fleetProxy{name: "B", front: front, h: h, dir: dir}
}

type fleetRun struct {
	m        *mixedRig
	proxies  [2]fleetProxy
	lag      time.Duration
	ops      atomic.Int64
	retried  atomic.Int64
	mu       sync.Mutex
	failures []string
}

func (fr *fleetRun) violation(format string, args ...any) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if len(fr.failures) < 20 {
		fr.failures = append(fr.failures, fmt.Sprintf(format, args...))
	} else if len(fr.failures) == 20 {
		fr.failures = append(fr.failures, "…")
	}
}

func (fr *fleetRun) violations() []string {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return append([]string(nil), fr.failures...)
}

// client owns keys w, w+n, w+2n, … and never shares them, so its model of each is exact.
func (fr *fleetRun) client(t *testing.T, w, n, keys int, seed uint64, stop <-chan struct{}) {
	rnd := rand.New(rand.NewPCG(seed, uint64(w)))
	var owned []int
	for k := w; k < keys; k += n {
		owned = append(owned, k)
	}
	model := map[int]string{} // key → last acknowledged body; absent: never written or deleted
	seq := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		k := owned[rnd.IntN(len(owned))]
		path := fmt.Sprintf("/data/f/%03d", k)
		p := fr.proxies[rnd.IntN(2)]
		fr.ops.Add(1)
		switch op := rnd.IntN(10); {
		case op < 5:
			seq++
			body := fmt.Sprintf("w%d-%d via %s", w, seq, p.name)
			if fr.retry(t, p, stop, func(p fleetProxy) int {
				return fr.m.sendTo(t, p.front.URL, "PUT", path, []byte(body)).StatusCode
			}) == http.StatusOK {
				model[k] = body
			} else {
				return // stopped while a write was refused: its outcome is unknown to the model, so stop here
			}
		case op < 6:
			if fr.retry(t, p, stop, func(p fleetProxy) int {
				return fr.m.sendTo(t, p.front.URL, "DELETE", path, nil).StatusCode
			}) == http.StatusNoContent {
				delete(model, k)
			} else {
				return
			}
		default:
			r := fr.m.sendTo(t, p.front.URL, "GET", path, nil)
			want, written := model[k]
			switch {
			case r.StatusCode == http.StatusServiceUnavailable:
				fr.violation("GET %s via %s: 503: reads are never refused", path, p.name)
			case written && (r.StatusCode != http.StatusOK || string(r.body) != want):
				fr.violation("GET %s via %s: %d %q, last acknowledged write was %q (%s)", path, p.name, r.StatusCode, r.body, want, fr.describe(p))
			case !written && r.StatusCode != http.StatusNotFound:
				fr.violation("GET %s via %s: %d %q, want 404 after its last acknowledged delete (%s)", path, p.name, r.StatusCode, r.body, fr.describe(p))
			}
		}
	}
}

// retry sends a write until it is not refused with 503, trying either proxy: a held key or a
// stale proxy answers 503, and a client retries.
func (fr *fleetRun) retry(t *testing.T, p fleetProxy, stop <-chan struct{}, send func(fleetProxy) int) int {
	for {
		code := send(p)
		if code != http.StatusServiceUnavailable {
			return code
		}
		fr.retried.Add(1)
		select {
		case <-stop:
			return code
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (fr *fleetRun) describe(p fleetProxy) string {
	pl, _ := p.dir.Snapshot().Lookup("acme", "data")
	if pl == nil {
		return "no placement"
	}
	r := ""
	if pl.Ramp != nil {
		r = fmt.Sprintf(" ramp %v", pl.Ramp.Ratio)
		if pl.Ramp.Hold != nil {
			r += fmt.Sprintf(" hold %v", pl.Ramp.Hold.Ratio)
		}
	}
	return fmt.Sprintf("%s at version %d: %s%s", p.name, p.dir.Snapshot().Version(), pl.State, r)
}

// fleetClient is shared by every client worker: keep-alive connections, not one per request.
var fleetClient = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}

// sendTo signs and sends one acme request to a given front server. Errors are reported with
// t.Error, since it runs on client goroutines.
func (m *mixedRig) sendTo(t testing.TB, base, method, target string, body []byte) reply {
	req, err := http.NewRequest(method, base+target, bytes.NewReader(body))
	if err != nil {
		t.Error(err)
		return reply{Response: &http.Response{}}
	}
	req.ContentLength = int64(len(body))
	if len(body) == 0 {
		req.Body = http.NoBody
	}
	sigv4.Sign(req, sigv4.Credentials{AccessKey: acmeAK, Secret: acmeSK}, "us-east-1", sigv4.UnsignedPayload, time.Now())
	resp, err := fleetClient.Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, target, err)
		return reply{Response: &http.Response{}}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return reply{resp, b}
}

// newFleetRun builds the rig with B lagging A by lag.
func newFleetRun(t *testing.T, lag time.Duration) *fleetRun {
	m := newMixedRig(t, nil)
	fr := &fleetRun{m: m, lag: lag}
	fr.proxies[0] = fleetProxy{name: "A", front: m.front, h: m.h, dir: m.dir}
	fr.proxies[1] = secondProxy(t, m)
	m.minio.addBucket("acme-9000-data")
	m.backendNames = append(m.backendNames, "acme-9000-data")
	if err := m.dir.SetTarget(context.Background(), "acme", "data", "minio", "acme-9000-data", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := fr.proxies[1].dir.Reload(); err != nil {
		t.Fatal(err)
	}
	return fr
}

// follow reloads B's directory lag after each change A made, and calls report with B's version.
func (fr *fleetRun) follow(stop <-chan struct{}, report func(v int64)) {
	b := fr.proxies[1].dir
	for {
		select {
		case <-stop:
			return
		case <-time.After(fr.lag / 4):
		}
		if fr.m.dir.Snapshot().Version() > b.Snapshot().Version() {
			select {
			case <-stop:
				return
			case <-time.After(fr.lag):
			}
			_, _ = b.Reload() //nolint:errcheck // a stat race only delays the next reload
		}
		if report != nil {
			report(b.Snapshot().Version())
		}
	}
}

func (fr *fleetRun) run(t *testing.T, dur time.Duration, operator func(stop <-chan struct{})) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	const workers, keys = 8, 48
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d, B lags by %s", seed, fr.lag)
	for w := range workers {
		wg.Add(1)
		go func() { defer wg.Done(); fr.client(t, w, workers, keys, seed, stop) }()
	}
	opDone := make(chan struct{})
	go func() { defer close(opDone); operator(stop) }()
	select {
	case <-time.After(dur):
	case <-opDone:
		time.Sleep(fr.lag * 3) // let clients read across the last step
	}
	close(stop)
	wg.Wait()
	<-opDone
}

// The steps a migration takes, as ramp ratios, then MIGRATING.
var fleetSteps = []float64{0.1, 0.25, 0.4, 0.55, 0.7, 0.85, 1}

func TestFleetStepsKeepTheClientsView(t *testing.T) {
	fr := newFleetRun(t, 60*time.Millisecond)
	b := fr.proxies[1]

	ctl := &control.Server{Dir: fr.m.dir, Clusters: fr.m.h.Clusters, Metrics: fr.m.h.Metrics, FleetFile: fr.m.dirPath + ".fleet.yaml",
		LeaseTTL: 200 * time.Millisecond, DropMargin: 150 * time.Millisecond, FencePoll: 5 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	api := httptest.NewServer(ctl.Handler())
	t.Cleanup(api.Close)
	post := func(path string, body any) (int, string) {
		data, _ := json.Marshal(body)
		resp, err := http.Post(api.URL+path, "application/json", bytes.NewReader(data))
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}
	// B is a member through the real membership code; cut severs its link to the control node.
	var cut atomic.Bool
	member := &control.Membership{Endpoint: api.URL, ID: "proxy-b", Interval: 40 * time.Millisecond, LeaseTTL: 200 * time.Millisecond,
		Dir: b.dir, Client: &http.Client{Transport: cuttable{cut: &cut, rt: http.DefaultTransport}}}
	b.h.Stale = member.Stale
	mctx, stopMember := context.WithCancel(context.Background())
	t.Cleanup(stopMember)
	go member.Run(mctx)
	stopFollow := make(chan struct{})
	t.Cleanup(func() { close(stopFollow) })
	go fr.follow(stopFollow, nil)
	for i := 0; member.Stale() && i < 100; i++ {
		time.Sleep(10 * time.Millisecond) // B joins
	}

	held := 0
	fr.run(t, propertyDurationOr(t, 6*time.Second), func(stop <-chan struct{}) {
		pause := func(d time.Duration) bool {
			select {
			case <-stop:
				return false
			case <-time.After(d):
				return true
			}
		}
		for i, ratio := range fleetSteps {
			if !pause(250 * time.Millisecond) {
				return
			}
			if i == 3 {
				// B loses the control node: its lease lapses, it refuses writes to the moving bucket,
				// and the control node drops it once lease and margin have passed, so the step goes
				// ahead without it. B keeps serving reads meanwhile, then comes back and catches up.
				cut.Store(true)
				if !pause(500 * time.Millisecond) {
					return
				}
				if !member.Stale() {
					t.Error("B is not stale with its link to the control node cut")
				}
			}
			code, body := post("/v1/placements/acme/data/ramp", control.RampRequest{Ratio: ratio, Wait: "5s"})
			if code != http.StatusOK {
				t.Errorf("ramp %v: %d %s", ratio, code, body)
				return
			}
			var res control.TransitionResult
			_ = json.Unmarshal([]byte(body), &res)
			if res.Held {
				held++
			}
			if i == 3 {
				if len(res.Silent) != 1 {
					t.Errorf("ramp %v with B's lease lapsed: want B silent, got %+v", ratio, res)
				}
				cut.Store(false)
			}
		}
		if !pause(250 * time.Millisecond) {
			return
		}
		if code, body := post("/v1/placements/acme/data/migrate", control.MigrateRequest{Wait: "5s"}); code != http.StatusOK {
			t.Errorf("migrate start: %d %s", code, body)
		}
	})

	p, _ := fr.m.dir.Snapshot().Lookup("acme", "data")
	t.Logf("%d client operations, %d writes retried after a 503; %d of %d steps held; ended %s", fr.ops.Load(), fr.retried.Load(), held, len(fleetSteps), p.State)
	if p.State != directory.StateMigrating {
		t.Errorf("the run ended in %s before every step was taken; lengthen SHUNT_PROPERTY_DURATION", p.State)
	}
	if held < len(fleetSteps)-2 {
		t.Errorf("only %d steps were held: B was a member for all but one of them", held)
	}
	if v := fr.violations(); len(v) > 0 {
		t.Fatalf("%d violations of the client's view:\n%s", len(v), joinLines(v))
	}
}

// The negative control: the same run, with the steps written straight to the directory as they
// were before ADR-0016. B's lag must show up as a lost write or a stale read.
func TestFleetWithoutTheFence(t *testing.T) {
	fr := newFleetRun(t, 60*time.Millisecond)
	stopFollow := make(chan struct{})
	t.Cleanup(func() { close(stopFollow) })
	go fr.follow(stopFollow, nil)
	ctx := context.Background()
	fr.run(t, propertyDurationOr(t, 6*time.Second), func(stop <-chan struct{}) {
		from := directory.StateActive
		for _, ratio := range fleetSteps {
			select {
			case <-stop:
				return
			case <-time.After(250 * time.Millisecond):
			}
			if err := fr.m.dir.SetState(ctx, "acme", "data", from, directory.Transition{To: directory.StateRamping, Ratio: ratio}, "test"); err != nil {
				t.Errorf("ramp %v: %v", ratio, err)
				return
			}
			from = directory.StateRamping
		}
	})
	v := fr.violations()
	t.Logf("%d client operations; %d violations without the fence, first: %v", fr.ops.Load(), len(v), first(v))
	if len(v) == 0 {
		t.Fatal("no violation without the fence: the fleet property test cannot see the hazard it guards against")
	}
}

func propertyDurationOr(t *testing.T, def time.Duration) time.Duration {
	t.Helper()
	if testing.Short() {
		return def / 2
	}
	return def
}

func joinLines(v []string) string {
	var b bytes.Buffer
	for _, s := range v {
		b.WriteString("  " + s + "\n")
	}
	return b.String()
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

// cuttable fails every request while cut is set: a member partitioned from its control node.
type cuttable struct {
	cut *atomic.Bool
	rt  http.RoundTripper
}

func (c cuttable) RoundTrip(r *http.Request) (*http.Response, error) {
	if c.cut.Load() {
		return nil, fmt.Errorf("partitioned from the control node")
	}
	return c.rt.RoundTrip(r)
}
