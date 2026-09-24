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
	"github.com/blakegolliher/shunt/internal/cp"
	"github.com/blakegolliher/shunt/internal/cp/cptest"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/member"
	"github.com/blakegolliher/shunt/internal/runtimecfg"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// The fleet property (ADR-0016 on ADR-0015, docs/design/distributed.md §12.9). Two proxies, A and
// B, are members of a control plane on an embedded etcd; each takes the directory from it with its
// own member.Client, B's long-poll delayed so it installs every version later than A. Clients own
// disjoint keys and write, delete and read each one through A or B at random, so the same key is
// written through both proxies in turn while ramp steps and migrate start roll out. The property
// is the client's view: every read, through either proxy, returns the key's last acknowledged
// write (or 404 after its last acknowledged delete). A 503 is not an acknowledgement; the client
// retries. Halfway through, B is cut off from the control plane.
//
// TestFleetWithoutTheFence is the negative control: the same run with the steps written straight
// into the store, no fence and no hold, which must find a violation or the positive run proves
// nothing.

type fleetProxy struct {
	name  string
	front *httptest.Server
	h     *Handler
	dir   directory.Directory
	mem   *member.Client
}

type fleetRun struct {
	m       *mixedRig
	tc      *cp.Store // the control plane's store, for the negative control
	api     *httptest.Server
	ctl     *control.Server
	cut     atomic.Bool // set: proxy B is partitioned from the control plane
	proxies [2]fleetProxy
	ops     atomic.Int64
	retried atomic.Int64
	mu      sync.Mutex
	fails   []string
}

func (fr *fleetRun) violation(format string, args ...any) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if len(fr.fails) < 20 {
		fr.fails = append(fr.fails, fmt.Sprintf(format, args...))
	} else if len(fr.fails) == 20 {
		fr.fails = append(fr.fails, "…")
	}
}

func (fr *fleetRun) violations() []string {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return append([]string(nil), fr.fails...)
}

// fleetClient is shared by every client worker: keep-alive connections, not one per request.
var fleetClient = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}

// sendTo signs and sends one acme request to a front server. Errors are reported with t.Error,
// since it runs on client goroutines.
func sendTo(t testing.TB, base, method, target string, body []byte) reply {
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

// newFleetRun builds a one-node control plane over the mixed rig's fake clusters, and two member
// proxies. lag delays B's installs.
func newFleetRun(t *testing.T, lag time.Duration) *fleetRun {
	t.Helper()
	m := newMixedRig(t, nil)
	m.minio.addBucket("acme-9000-data")
	m.backendNames = append(m.backendNames, "acme-9000-data")
	fr := &fleetRun{m: m}

	// The control plane: one etcd member, the store, the fleet, the control API.
	node := cptest.StartNode(t)
	c, _ := cp.NewCipher(make([]byte, 32))
	store := cp.New(node.Client(), c, slog.New(slog.DiscardHandler))
	registry := upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, store.Resolve)
	t.Cleanup(registry.Close)
	store.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, err := registry.Prepare(f.Clusters, resolve)
		if err != nil {
			return nil, err
		}
		return func() { cand.Commit() }, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	fr.tc = store
	fleet := cp.NewFleet(node.Client(), 200*time.Millisecond)
	fleet.DropMargin = 150 * time.Millisecond
	metrics := telemetry.NewMetrics()
	fr.ctl = &control.Server{Dir: store, Clusters: registry, Metrics: metrics, Log: slog.New(slog.DiscardHandler),
		Keys: store, Fleet: fleet, ClusterSecrets: store.ClusterSecrets, FencePoll: 5 * time.Millisecond}
	fr.api = httptest.NewServer(cuttable{cut: &fr.cut, h: fr.ctl.Handler()})
	t.Cleanup(fr.api.Close)

	// The directory: the rig's clusters (with the secrets the control plane stores) and its
	// placements, written into the store; plus the client key.
	f := m.dir.Snapshot().File()
	for name, cl := range f.Clusters {
		secret := map[string]string{"env:G": "garage-cluster-secret", "env:M": "minio-cluster-secret"}[cl.Credentials.SecretRef]
		cl.Credentials.SecretRef = "control:" + name
		if err := store.PutCluster(ctx, name, cl, secret, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SetTenantDefault(ctx, "acme", "garage", "test"); err != nil && err.Error() == "" {
		t.Fatal(err)
	}
	if err := store.Adopt(ctx, "acme", "data", "garage", "acme-1111-data", "test"); err != nil {
		t.Fatal(err)
	}
	if err := store.Adopt(ctx, "acme", "pics", "garage", "acme-7777-pics", "test"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTarget(ctx, "acme", "data", "minio", "acme-9000-data", "test"); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(sigv4.Credential{AccessKey: acmeAK, Secret: acmeSK, Tenant: "acme"}); err != nil {
		t.Fatal(err)
	}

	for i, name := range []string{"A", "B"} {
		fr.proxies[i] = fr.startProxy(t, name, lag*time.Duration(i))
	}
	return fr
}

// startProxy is one member proxy over its own member.Client; lag delays every install.
func (fr *fleetRun) startProxy(t *testing.T, name string, lag time.Duration) fleetProxy {
	t.Helper()
	mem := member.New(member.Config{Endpoints: []string{fr.api.URL}, ProxyID: "proxy-" + name, CacheDir: t.TempDir(),
		Interval: 40 * time.Millisecond, LeaseTTL: 200 * time.Millisecond, LongPoll: 500 * time.Millisecond}, slog.New(slog.DiscardHandler))
	set := upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, mem.Resolve)
	t.Cleanup(set.Close)
	mem.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		if lag > 0 {
			time.Sleep(lag)
		}
		cand, err := set.Prepare(f.Clusters, resolve)
		if err != nil {
			return nil, err
		}
		return func() { cand.Commit() }, nil
	}
	rt := runtimecfg.NewPublisher(&runtimecfg.Bundle{Snapshot: mem.Snapshot(), Keys: mem.Keys().Table(), Clusters: set.Load()})
	mem.OnInstall = func(s *directory.Snapshot) {
		if err := rt.Refresh(s, mem.Keys().Table(), set.Load()); err != nil {
			t.Error(err)
		}
	}
	if err := mem.Load(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	defer rcancel()
	if err := mem.Register(rctx); err != nil {
		t.Fatal(err)
	}
	go mem.Run(ctx)
	h := New(Handler{
		Mode: ModeResign, Runtime: rt, Dir: mem, Rewrite: true, Stale: mem.Stale,
		Domains: s3.NewDomains([]string{"*.shunt.example.com"}), Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil),
		Slow: telemetry.NewSlowRing(10, time.Hour), IdleTimeout: 2 * time.Second, MetadataTimeout: 5 * time.Second, Via: "1.1 shunt/test",
	}, 64<<10)
	front := httptest.NewServer(h)
	front.Config.ErrorLog = nil
	t.Cleanup(front.Close)
	return fleetProxy{name: name, front: front, h: h, dir: mem, mem: mem}
}

// client owns keys w, w+n, w+2n, … and never shares them, so its model of each is exact.
func (fr *fleetRun) client(t *testing.T, w, n, keys int, seed uint64, stop <-chan struct{}) {
	rnd := rand.New(rand.NewPCG(seed, uint64(w)))
	var owned []int
	for k := w; k < keys; k += n {
		owned = append(owned, k)
	}
	model := map[int]string{}
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
			if fr.retry(stop, func() int { return sendTo(t, p.front.URL, "PUT", path, []byte(body)).StatusCode }) == http.StatusOK {
				model[k] = body
			} else {
				return // stopped while a write was refused: its outcome is unknown to the model
			}
		case op < 6:
			if fr.retry(stop, func() int { return sendTo(t, p.front.URL, "DELETE", path, nil).StatusCode }) == http.StatusNoContent {
				delete(model, k)
			} else {
				return
			}
		default:
			r := sendTo(t, p.front.URL, "GET", path, nil)
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

// retry sends a write until it is not refused with 503: a held key or a stale proxy answers 503,
// and a client retries.
func (fr *fleetRun) retry(stop <-chan struct{}, send func() int) int {
	for {
		code := send()
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

func (fr *fleetRun) run(t *testing.T, dur time.Duration, operator func(stop <-chan struct{})) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	const workers, keys = 8, 48
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	for w := range workers {
		wg.Add(1)
		go func() { defer wg.Done(); fr.client(t, w, workers, keys, seed, stop) }()
	}
	opDone := make(chan struct{})
	go func() { defer close(opDone); operator(stop) }()
	select {
	case <-time.After(dur):
	case <-opDone:
		time.Sleep(300 * time.Millisecond) // let clients read across the last step
	}
	close(stop)
	wg.Wait()
	<-opDone
}

var fleetRatios = []float64{0.1, 0.25, 0.4, 0.55, 0.7, 0.85, 1}

func (fr *fleetRun) post(path string, body any) (int, string) {
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, fr.api.URL+path, bytes.NewReader(data))
	if err != nil {
		return 0, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(control.HeaderIdempotencyKey, fmt.Sprintf("fleet-%d", fleetKeys.Add(1)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

var fleetKeys atomic.Int64

func TestFleetStepsKeepTheClientsView(t *testing.T) { fleetSteps(t, nil, false, false) }

// TestFleetRangeMoveKeepsTheClientsView is the same run for a move of part of the bucket (ADR-0018
// N3): only the lower half of the key space moves, so clients' keys in the other half stay on the
// source throughout, and the property holds for both.
func TestFleetRangeMoveKeepsTheClientsView(t *testing.T) {
	fleetSteps(t, &directory.HashRange{From: 0, To: 1<<63 - 1}, false, false)
}

// TestFleetSameClusterMoveKeepsTheClientsView moves the bucket to another bucket on its own
// cluster (ADR-0018 N3b): the two buckets share garage, so only the roles tell them apart.
func TestFleetSameClusterMoveKeepsTheClientsView(t *testing.T) { fleetSteps(t, nil, true, false) }

// TestFleetScopedMoveKeepsTheClientsView moves the keys of one prefix rule (ADR-0020): f/0 is
// carved with two nested rules, f/00 and f/03, whose keys stay on garage while the rest of f/0
// moves to minio. Clients' keys are f/000 to f/047, so a key's scope, not its hash alone, decides
// whether it moves.
func TestFleetScopedMoveKeepsTheClientsView(t *testing.T) { fleetSteps(t, nil, false, true) }

// fleetScope is the scoped run's prefix rules and the one that moves.
const fleetScope = "f/0"

// scopedTarget carves the rules of the scoped run; the recorded target is cleared, since a bucket
// with prefix rules names the destination of each move itself.
func (fr *fleetRun) scopedTarget(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := fr.tc.ClearTarget(ctx, "acme", "data", "test"); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{fleetScope, "f/00", "f/03"} {
		if err := fr.tc.Carve(ctx, "acme", "data", prefix, "test"); err != nil {
			t.Fatal(err)
		}
	}
}

// strays are the keys minio holds that the scoped move must never have sent there: those of the
// nested rules.
func (fr *fleetRun) strays() []string {
	fr.m.minio.mu.Lock()
	defer fr.m.minio.mu.Unlock()
	var out []string
	for k := range fr.m.minio.buckets["acme-9000-data"] {
		if len(k) < 4 || k[:4] == "f/00" || k[:4] == "f/03" || k[:3] != fleetScope {
			out = append(out, k)
		}
	}
	return out
}

// sameClusterTarget turns the run's move into one within garage: the recorded target is cleared
// and the first step names a new bucket there.
func (fr *fleetRun) sameClusterTarget(t *testing.T) {
	t.Helper()
	fr.m.garage.addBucket("acme-data-new")
	fr.m.backendNames = append(fr.m.backendNames, "acme-data-new")
	if err := fr.tc.ClearTarget(context.Background(), "acme", "data", "test"); err != nil {
		t.Fatal(err)
	}
}

func fleetSteps(t *testing.T, rg *directory.HashRange, sameCluster, scoped bool) {
	fr := newFleetRun(t, 60*time.Millisecond)
	if sameCluster {
		fr.sameClusterTarget(t)
	}
	if scoped {
		fr.scopedTarget(t)
	}
	b := fr.proxies[1]
	cut := &fr.cut // set: B is severed from the control plane, its heartbeats and polls fail

	held := 0
	fr.run(t, 8*time.Second, func(stop <-chan struct{}) {
		pause := func(d time.Duration) bool {
			select {
			case <-stop:
				return false
			case <-time.After(d):
				return true
			}
		}
		for i, ratio := range fleetRatios {
			if !pause(250 * time.Millisecond) {
				return
			}
			if i == 3 {
				// B loses the control plane: its lease lapses, it refuses writes to the moving bucket
				// and reads it target-first, and the control plane drops it once lease and margin
				// have passed, so the step goes ahead without it. B comes back and catches up.
				cut.Store(true)
				if !pause(500 * time.Millisecond) {
					return
				}
				if !b.mem.Stale() {
					t.Error("B is not stale with its link to the control plane cut")
				}
			}
			req := control.RampRequest{Ratio: ratio, Wait: "5s"}
			if i == 0 {
				req.Range = rg // the first step starts the move: of every key, or of the range
				if sameCluster {
					req.To, req.Name = "garage", "acme-data-new"
				}
				if scoped {
					req.Scope, req.To, req.Name = fleetScope, "minio", "acme-9000-data"
				}
			}
			code, body := fr.post("/v1/placements/acme/data/ramp", req)
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
		if code, body := fr.post("/v1/placements/acme/data/migrate", control.MigrateRequest{Wait: "5s"}); code != http.StatusOK {
			t.Errorf("migrate start: %d %s", code, body)
		}
	})

	p, _ := fr.tc.Snapshot().Lookup("acme", "data")
	t.Logf("%d client operations, %d writes retried after a 503; %d of %d steps held; ended %s", fr.ops.Load(), fr.retried.Load(), held, len(fleetRatios), p.State)
	if p.State != directory.StateMigrating {
		t.Errorf("the run ended in %s before every step was taken", p.State)
	}
	if rg != nil && (p.Move == nil || p.Move.Range != *rg) {
		t.Errorf("the run was to move part of the bucket, and ended with move %+v", p.Move)
	}
	if sameCluster && (p.Move == nil || p.Legs[p.Move.To].Cluster != "garage" || p.Legs[p.Move.To].Bucket != "acme-data-new") {
		t.Errorf("the run was to move within garage, and ended with %+v legs %+v", p.Move, p.Legs)
	}
	if scoped && (p.Move == nil || p.Move.Scope != fleetScope || len(p.Prefixes) != 3) {
		t.Errorf("the run was to move the keys under %s, and ended with move %+v rules %+v", fleetScope, p.Move, p.Prefixes)
	}
	if st := fr.strays(); scoped && len(st) > 0 {
		t.Errorf("keys outside the moving scope reached minio: %v", st)
	}
	if held < len(fleetRatios)-2 {
		t.Errorf("only %d steps were held: B was a member for all but one of them", held)
	}
	if v := fr.violations(); len(v) > 0 {
		t.Fatalf("%d violations of the client's view:\n%s", len(v), joinLines(v))
	}
}

// The negative control: the same run with the steps written straight into the store, no fence
// and no hold. B's lag must show up as a lost write or a stale read.
func TestFleetWithoutTheFence(t *testing.T) { fleetWithoutTheFence(t, nil, false, false) }

// TestFleetSameClusterMoveWithoutTheFence is the negative control of the move within one cluster.
func TestFleetSameClusterMoveWithoutTheFence(t *testing.T) { fleetWithoutTheFence(t, nil, true, false) }

// TestFleetScopedMoveWithoutTheFence is the negative control of the move of one prefix rule's keys.
func TestFleetScopedMoveWithoutTheFence(t *testing.T) { fleetWithoutTheFence(t, nil, false, true) }

// TestFleetRangeMoveWithoutTheFence is the negative control of the move of part of the bucket: it
// must find a violation too, or the positive run proves nothing for moves.
func TestFleetRangeMoveWithoutTheFence(t *testing.T) {
	fleetWithoutTheFence(t, &directory.HashRange{From: 0, To: 1<<63 - 1}, false, false)
}

func fleetWithoutTheFence(t *testing.T, rg *directory.HashRange, sameCluster, scoped bool) {
	fr := newFleetRun(t, 60*time.Millisecond)
	if sameCluster {
		fr.sameClusterTarget(t)
	}
	if scoped {
		fr.scopedTarget(t)
	}
	ctx := context.Background()
	fr.run(t, 8*time.Second, func(stop <-chan struct{}) {
		from := directory.StateActive
		for i, ratio := range fleetRatios {
			select {
			case <-stop:
				return
			case <-time.After(250 * time.Millisecond):
			}
			tr := directory.Transition{To: directory.StateRamping, Ratio: ratio}
			if i == 0 {
				tr.Range = rg
				if sameCluster {
					tr.Target, tr.Name = "garage", "acme-data-new"
				}
				if scoped {
					tr.Scope, tr.Target, tr.Name = fleetScope, "minio", "acme-9000-data"
				}
			}
			if err := fr.tc.SetState(ctx, "acme", "data", from, tr, "test"); err != nil {
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

// cuttable fails every request from proxy B while cut is set: that member partitioned from its
// control plane. Members are told apart by the X-Shunt-Proxy header a member sends on every call.
type cuttable struct {
	cut *atomic.Bool
	h   http.Handler
}

func (c cuttable) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c.cut.Load() && r.Header.Get("X-Shunt-Proxy") == "proxy-B" {
		http.Error(w, "partitioned", http.StatusBadGateway)
		return
	}
	c.h.ServeHTTP(w, r)
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
