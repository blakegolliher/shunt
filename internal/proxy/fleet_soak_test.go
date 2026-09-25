package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/blakegolliher/shunt/internal/admission"
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

// The H2 acceptance soak (docs/prompts/distributed-hardening.md §3; regression matrix T14): the
// fleet property of fleet_property_test.go held for minutes while the control plane is broken on
// purpose. Two control nodes, c1 and c2, run over one embedded etcd, each with its own store,
// fleet table and operation records (and so its own liveness key); two member proxies, A and B,
// know both nodes' addresses. Every cycle takes a fresh bucket, acme/soak-N adopted on garage with
// a target on minio, through ramp steps and migrate start, each a durable operation, and a mover
// pass, while the model clients read, write and delete their own keys through A and B at random.
// Every cycle injects:
//
//   - a partition: B is cut from both control nodes before one step. The step must block (B is
//     silent: proxy_missing), never succeed while B is cut, and succeed once B is back.
//   - an owner crash: B is cut and a ramp step is started on one control node, the victim, so it
//     writes its hold and blocks in its drain. The victim then stops (its context canceled, its
//     API refusing connections, its records and liveness keep-alive closed). Once its liveness
//     key lapses the survivor's sweep marks the record blocked owner_lost; the step is resumed on
//     the survivor, B reconnects, and the step must succeed there. The victim is then started
//     again, on the same address, as a fresh process. The victim alternates between c1 and c2,
//     so the members fail over in both directions. Every other crash revokes the victim's
//     liveness lease rather than waiting out its 10 s TTL: the same lapse, sooner.
//
// The oracle is the client's view: every read, through either proxy, returns the key's last
// acknowledged write, or 404 after its last acknowledged delete; at the end of every cycle each
// client reads every key it owns through both proxies. Growth is sampled after every cycle, at a
// quiet point: goroutines, heap in use, each proxy's retained runtime bundles and its upstream
// transports. The first cycle is the warm baseline; the last sample, after recovery, must not
// have grown past it (see soakSlack).
//
// It runs only with SHUNT_SOAK_DURATION set: `make soak` (SOAK_TIME, SOAK_SEED). The unsafe
// negative control is TestFleetWithoutTheFence, which must find a violation.

// soakRatios are one cycle's ramp steps; migrate start follows the last.
var soakRatios = []float64{0.25, 0.5, 0.75, 1}

// soakOwnerPrefix is where internal/cp keeps a control node's liveness key (cp/ops.go
// ownerPrefix), read here only to revoke a stopped node's lease early.
const soakOwnerPrefix = "/shunt/ops-owner/"

// soakHTTP is the operator's client of the control API.
var soakHTTP = &http.Client{Timeout: 15 * time.Second}

// soakNode is one control node: its store, fleet table and operation records over the shared
// etcd, the control API on a fixed loopback address, and the once-a-second sweep shunt-control
// runs (cmd/shunt-control/serve.go).
type soakNode struct {
	name   string
	addr   string
	srv    *httptest.Server
	ctl    *control.Server
	ops    *cp.Operations
	store  *cp.Store
	fleet  *cp.Fleet
	reg    *upstream.Registry
	cancel context.CancelFunc
	done   chan struct{} // closed when the sweep loop has stopped
	up     atomic.Bool
	once   sync.Once
}

func (n *soakNode) url() string { return "http://" + n.addr }

// stop is the node's crash: its context (every operation it runs) ends, its API stops accepting
// connections, its watch and liveness keep-alive stop. The liveness key itself is left to lapse.
func (n *soakNode) stop() {
	n.once.Do(func() {
		n.up.Store(false)
		n.cancel()
		n.srv.Close()
		<-n.done
		n.ops.Close()
		n.store.Close()
		n.reg.Close()
	})
}

type soakRun struct {
	*fleetRun // the model's violations, the retry loop, the cut flag and proxies A and B
	etcd      *cp.Node
	cipher    *cp.Cipher
	nodes     [2]*soakNode
	rts       [2]*runtimecfg.Publisher
	sets      [2]*upstream.Registry
}

func newSoakRun(t *testing.T) *soakRun {
	t.Helper()
	m := newMixedRig(t, nil)
	for _, f := range []*fakeS3{m.garage, m.minio} {
		f.mu.Lock()
		f.noHistory = true // a soak would otherwise hold every request it made
		f.mu.Unlock()
	}
	sr := &soakRun{fleetRun: &fleetRun{m: m}}
	sr.etcd = cptest.StartNode(t)
	c, err := cp.NewCipher(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	sr.cipher = c
	sr.nodes[0] = sr.startNode(t, "c1", "")
	sr.nodes[1] = sr.startNode(t, "c2", "")
	t.Cleanup(func() {
		for _, n := range sr.nodes {
			n.stop()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := sr.nodes[1].store
	f := m.dir.Snapshot().File()
	for name, cl := range f.Clusters {
		secret := map[string]string{"env:G": "garage-cluster-secret", "env:M": "minio-cluster-secret"}[cl.Credentials.SecretRef]
		cl.Credentials.SecretRef = "control:" + name
		if err := store.PutCluster(ctx, name, cl, secret, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Add(sigv4.Credential{AccessKey: acmeAK, Secret: acmeSK, Tenant: "acme"}); err != nil {
		t.Fatal(err)
	}
	if err := sr.nodes[0].store.WaitVersion(ctx, store.Version()); err != nil {
		t.Fatal(err)
	}
	endpoints := []string{sr.nodes[0].url(), sr.nodes[1].url()}
	for i, name := range []string{"A", "B"} {
		sr.startProxy(t, i, name, 60*time.Millisecond*time.Duration(i), endpoints)
	}
	return sr
}

// startNode starts control node name, on addr when it is given (a restart keeps its address, so
// the members' endpoint lists stay right), else on a fresh loopback port.
func (sr *soakRun) startNode(t *testing.T, name, addr string) *soakNode {
	t.Helper()
	cli := sr.etcd.Client()
	log := slog.New(slog.DiscardHandler)
	n := &soakNode{name: name, done: make(chan struct{})}
	n.store = cp.New(cli, sr.cipher, log)
	n.reg = upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, n.store.Resolve)
	n.store.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, err := n.reg.Prepare(f.Clusters, resolve)
		if err != nil {
			return nil, err
		}
		return func() { cand.Commit() }, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
	defer scancel()
	if err := n.store.Start(sctx); err != nil {
		t.Fatal(err)
	}
	n.ops = cp.NewOperations(cli)
	n.ops.Node = name
	if err := n.ops.Start(sctx); err != nil {
		t.Fatal(err)
	}
	n.fleet = cp.NewFleet(cli, 200*time.Millisecond)
	n.fleet.DropMargin = 150 * time.Millisecond
	n.ctl = &control.Server{Dir: n.store, Clusters: n.reg, Metrics: telemetry.NewMetrics(), Log: log, Keys: n.store, Fleet: n.fleet,
		ClusterSecrets: n.store.ClusterSecrets, FencePoll: 5 * time.Millisecond, Ops: n.ops, Node: name, Ctx: ctx,
		ConfirmKey: sr.cipher.Derive("confirm")}
	// What shunt-control does once its records are loaded.
	if err := n.ctl.FailOrphans(sctx); err != nil {
		t.Fatal(err)
	}
	if err := n.ctl.ResumeOwn(sctx); err != nil {
		t.Fatal(err)
	}
	n.srv = httptest.NewUnstartedServer(cuttable{cut: &sr.cut, h: n.ctl.Handler()})
	if addr != "" {
		_ = n.srv.Listener.Close() //nolint:errcheck // replaced by the listener on the node's own address
		n.srv.Listener = listenOn(t, addr)
	}
	n.srv.Start()
	n.addr = n.srv.Listener.Addr().String()
	go func() {
		defer close(n.done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			// shunt-control logs these errors and tries again at the next tick; so does the soak.
			_ = n.ctl.PublishFleet(ctx) //nolint:errcheck // see above
			if n.store.Ready() {
				_ = n.ctl.Sweep(ctx) //nolint:errcheck // see above
			}
		}
	}()
	n.up.Store(true)
	return n
}

// listenOn binds addr again after the node that held it stopped.
func listenOn(t *testing.T, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		l, err := net.Listen("tcp", addr)
		if err == nil {
			return l
		}
		if time.Now().After(deadline) {
			t.Fatalf("rebinding a control node's address %s: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// startProxy is fleetRun.startProxy with every control node's address: a member moves to the
// next endpoint when one refuses or fails.
func (sr *soakRun) startProxy(t *testing.T, i int, name string, lag time.Duration, endpoints []string) {
	t.Helper()
	mem := member.New(member.Config{Endpoints: endpoints, ProxyID: "proxy-" + name, CacheDir: t.TempDir(),
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
	keeper := &GateKeeper{Gates: admission.New()}
	mem.Gates = keeper.Gates
	rt.Published = func(b *runtimecfg.Bundle) { keeper.Served(b.Snapshot) }
	keeper.Served(mem.Snapshot())
	mem.Serving = func() *directory.Snapshot { return rt.Load().Snapshot }
	mem.SecretsHeld = func() map[string]int64 { return rt.Stats().SecretsHeld }
	mem.OnInstall = func(s *directory.Snapshot) {
		if err := rt.Refresh(s, mem.Keys().Table(), set.Load()); err != nil {
			t.Error(err)
		}
		keeper.Installed(s)
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
		Mode: ModeResign, Runtime: rt, Gates: keeper.Gates, Dir: mem, Rewrite: true, Stale: mem.Stale,
		Domains: s3.NewDomains([]string{"*.shunt.example.com"}), Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil),
		Slow: telemetry.NewSlowRing(10, time.Hour), IdleTimeout: 2 * time.Second, MetadataTimeout: 5 * time.Second, Via: "1.1 shunt/test",
	}, 64<<10)
	front := httptest.NewServer(h)
	front.Config.ErrorLog = nil
	t.Cleanup(front.Close)
	sr.proxies[i] = fleetProxy{name: name, front: front, h: h, dir: mem, mem: mem}
	sr.rts[i], sr.sets[i] = rt, set
}

// client is fleetRun.client on one cycle's bucket. When stopped with its model exact, it reads
// every key it owns through both proxies: no acknowledged update may have been lost.
func (sr *soakRun) client(t *testing.T, bucket string, w, n, keys int, seed uint64, stop <-chan struct{}) {
	rnd := rand.New(rand.NewPCG(seed, uint64(w)))
	var owned []int
	for k := w; k < keys; k += n {
		owned = append(owned, k)
	}
	path := func(k int) string { return fmt.Sprintf("/%s/f/%03d", bucket, k) }
	model := map[int]string{}
	check := func(p fleetProxy, k int, final bool) {
		r := sendTo(t, p.front.URL, "GET", path(k), nil)
		want, written := model[k]
		when := ""
		if final {
			when = " (final read)"
		}
		switch {
		case r.StatusCode == http.StatusServiceUnavailable:
			sr.violation("GET %s via %s%s: 503: reads are never refused", path(k), p.name, when)
		case written && (r.StatusCode != http.StatusOK || string(r.body) != want):
			sr.violation("GET %s via %s%s: %d %q, last acknowledged write was %q (%s)", path(k), p.name, when, r.StatusCode, r.body, want, sr.describeIn(p, bucket))
		case !written && r.StatusCode != http.StatusNotFound:
			sr.violation("GET %s via %s%s: %d %q, want 404 after its last acknowledged delete (%s)", path(k), p.name, when, r.StatusCode, r.body, sr.describeIn(p, bucket))
		}
	}
	// acknowledged reports whether a write was acknowledged. Refused (503) is retried until the
	// client stops; any other answer is a finding, since a write is either acknowledged or refused.
	acknowledged := func(code, want int, method, target string, p fleetProxy) bool {
		switch code {
		case want:
			return true
		case http.StatusServiceUnavailable:
		default:
			sr.violation("%s %s via %s: %d, want %d or a 503 refusal (%s)", method, target, p.name, code, want, sr.describeIn(p, bucket))
		}
		return false
	}
	seq := 0
	for {
		select {
		case <-stop:
			for _, k := range owned {
				for _, p := range sr.proxies {
					check(p, k, true)
				}
			}
			return
		default:
		}
		k := owned[rnd.IntN(len(owned))]
		p := sr.proxies[rnd.IntN(2)]
		sr.ops.Add(1)
		switch op := rnd.IntN(10); {
		case op < 5:
			seq++
			body := fmt.Sprintf("w%d-%d via %s", w, seq, p.name)
			code := sr.retry(stop, func() int { return sendTo(t, p.front.URL, "PUT", path(k), []byte(body)).StatusCode })
			if !acknowledged(code, http.StatusOK, "PUT", path(k), p) {
				return // stopped while a write was refused: its outcome is unknown to the model
			}
			model[k] = body
		case op < 6:
			code := sr.retry(stop, func() int { return sendTo(t, p.front.URL, "DELETE", path(k), nil).StatusCode })
			if !acknowledged(code, http.StatusNoContent, "DELETE", path(k), p) {
				return
			}
			delete(model, k)
		default:
			check(p, k, false)
		}
	}
}

func (sr *soakRun) describeIn(p fleetProxy, bucket string) string {
	snap := p.dir.Snapshot()
	pl, _ := snap.Lookup("acme", bucket)
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
	return fmt.Sprintf("%s at version %d, stale %v: %s%s", p.name, snap.Version(), p.mem.Stale(), pl.State, r)
}

// call sends one control API request; a mutation carries a fresh Idempotency-Key.
func soakCall(base, method, path string, body, out any) (int, string) {
	var rd io.Reader = http.NoBody
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err.Error()
		}
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, base+path, rd)
	if err != nil {
		return 0, err.Error()
	}
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(control.HeaderIdempotencyKey, fmt.Sprintf("soak-%d", fleetKeys.Add(1)))
	}
	resp, err := soakHTTP.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err.Error()
	}
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return 0, err.Error()
		}
	}
	return resp.StatusCode, string(raw)
}

// start posts one step as an operation on node n and returns its record (202).
func (sr *soakRun) start(n *soakNode, kind, bucket string, args any) (control.Operation, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return control.Operation{}, err
	}
	var op control.Operation
	code, body := soakCall(n.url(), http.MethodPost, "/v1/operations", control.OperationRequest{Kind: kind, Placement: "acme/" + bucket, Args: raw}, &op)
	if code != http.StatusAccepted {
		return op, fmt.Errorf("%s %s on %s: %d %s", kind, bucket, n.name, code, body)
	}
	return op, nil
}

// soakGet reads a record through node n.
func soakGet(n *soakNode, id string) (control.Operation, error) {
	var op control.Operation
	code, body := soakCall(n.url(), http.MethodGet, "/v1/operations/"+id, nil, &op)
	if code != http.StatusOK {
		return op, fmt.Errorf("get operation %s on %s: %d %s", id, n.name, code, body)
	}
	return op, nil
}

// soakWait waits for a record to end, read through node n.
func soakWait(n *soakNode, id string, timeout time.Duration) (control.Operation, error) {
	deadline := time.Now().Add(timeout)
	for {
		op, err := soakGet(n, id)
		if err == nil && op.Terminal() {
			return op, nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return op, err
			}
			return op, fmt.Errorf("operation %s remained %s in phase %s with blockers %+v", id, op.Status, op.Phase, op.Blockers)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// soakSucceeded is the check every step ends with.
func soakSucceeded(op control.Operation, err error) error {
	switch {
	case err != nil:
		return err
	case op.Status != control.StatusSucceeded:
		return fmt.Errorf("operation %s (%s) ended %s: %+v", op.ID, op.Kind, op.Status, op.Error)
	}
	return nil
}

// settled waits until both proxies have installed n's directory version and have heartbeat it.
func (sr *soakRun) settled(n *soakNode) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.store.Sync(ctx); err != nil {
		return err
	}
	v := n.store.Version()
	for _, p := range sr.proxies {
		if err := p.mem.WaitVersion(ctx, v); err != nil {
			return fmt.Errorf("proxy %s: %w", p.name, err)
		}
	}
	time.Sleep(150 * time.Millisecond) // a few heartbeats: the fleet table has the version too
	return nil
}

// bLive reports whether n's fleet table holds B's lease.
func (sr *soakRun) bLive(n *soakNode) bool {
	ms, err := n.fleet.Members(context.Background())
	if err != nil {
		return true
	}
	for i := range ms {
		if ms[i].ID == "proxy-B" {
			return ms[i].Live
		}
	}
	return false
}

// step runs one step with nothing broken.
func (sr *soakRun) step(n *soakNode, kind, bucket string, args any) (control.Operation, error) {
	op, err := sr.start(n, kind, bucket, args)
	if err != nil {
		return op, err
	}
	done, err := soakWait(n, op.ID, 20*time.Second)
	return done, soakSucceeded(done, err)
}

// partition runs one step with B cut from the control plane for hold, then reconnects it. The step
// must be blocked while B is cut, never end, and succeed once B is back.
func (sr *soakRun) partition(n *soakNode, kind, bucket string, args any, hold time.Duration) (string, error) {
	sr.cut.Store(true)
	defer sr.cut.Store(false)
	b := sr.proxies[1]
	deadline := time.Now().Add(10 * time.Second)
	for !b.mem.Stale() || sr.bLive(n) {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("B cut for 10s: stale %v, live in the fleet table %v", b.mem.Stale(), sr.bLive(n))
		}
		time.Sleep(20 * time.Millisecond)
	}
	op, err := sr.start(n, kind, bucket, args)
	if err != nil {
		return "", err
	}
	codes := map[string]bool{}
	blocked := 0
	for end := time.Now().Add(hold); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		cur, gerr := soakGet(n, op.ID)
		if gerr != nil {
			return "", gerr
		}
		if cur.Terminal() {
			return "", fmt.Errorf("%s %s ended %s with B partitioned from the control plane: %+v", kind, op.ID, cur.Status, cur)
		}
		if cur.Status == control.StatusBlocked {
			blocked++
			for _, bl := range cur.Blockers {
				codes[bl.Code+":"+bl.ProxyID] = true
			}
		}
	}
	if blocked == 0 || !codes[control.BlockerProxyMissing+":proxy-B"] {
		return "", fmt.Errorf("%s %s with B partitioned for %s: blocked %d times with %v, want proxy_missing proxy-B", kind, op.ID, hold, blocked, slices.Sorted(maps.Keys(codes)))
	}
	sr.cut.Store(false)
	reconnected := time.Now()
	done, err := soakWait(n, op.ID, 20*time.Second)
	if serr := soakSucceeded(done, err); serr != nil {
		return "", fmt.Errorf("%s after B reconnected: %w", kind, serr)
	}
	return fmt.Sprintf("partition %s %s on %s: blocked %s by %v, done %s after reconnect", kind, op.ID, n.name, hold.Round(time.Millisecond),
		slices.Sorted(maps.Keys(codes)), time.Since(reconnected).Round(time.Millisecond)), nil
}

// crash runs a ramp step on the victim until it holds, B cut so it cannot drain; stops the victim;
// waits for the survivor to mark the record owner_lost; resumes it there; reconnects B; and wants
// it to succeed on the survivor. fast revokes the victim's liveness lease instead of waiting out its
// TTL. The victim is started again afterwards.
func (sr *soakRun) crash(t *testing.T, vi int, bucket string, args control.RampRequest, fast bool, rnd *rand.Rand) (string, error) {
	victim, survivor := sr.nodes[vi], sr.nodes[1-vi]
	defer sr.cut.Store(false)
	var op control.Operation
	misses := 0
	for {
		if err := sr.settled(survivor); err != nil {
			return "", err
		}
		sr.cut.Store(true)
		var err error
		if op, err = sr.start(victim, control.OpRamp, bucket, args); err != nil {
			return "", err
		}
		held := false
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			cur, gerr := soakGet(survivor, op.ID)
			if gerr != nil {
				return "", gerr
			}
			if cur.Terminal() {
				return "", fmt.Errorf("ramp %s ended %s on %s with B cut before it could drain: %+v", op.ID, cur.Status, victim.name, cur)
			}
			if cur.Status == control.StatusBlocked && cur.Barrier != nil && cur.Barrier.HoldVersion > 0 && cur.Phase == control.PhaseDrain {
				held = true
				break
			}
		}
		if held {
			break
		}
		// B dropped out of the fleet table before the step's precondition passed, so it blocked
		// before writing its hold, with nothing to resume. Cancel it and try again.
		misses++
		if misses > 3 {
			return "", fmt.Errorf("ramp on %s never held with B cut in %d tries", victim.name, misses)
		}
		if code, body := soakCall(victim.url(), http.MethodPost, "/v1/operations/"+op.ID+"/cancel", struct{}{}, nil); code != http.StatusOK {
			return "", fmt.Errorf("cancel of the unheld ramp %s: %d %s", op.ID, code, body)
		}
		sr.cut.Store(false)
		if _, err := soakWait(victim, op.ID, 10*time.Second); err != nil {
			return "", err
		}
	}

	crashed := time.Now()
	victim.stop()
	if fast {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := sr.etcd.Client().Get(ctx, soakOwnerPrefix+victim.name)
		if err == nil && len(resp.Kvs) == 1 && resp.Kvs[0].Lease != 0 {
			_, err = sr.etcd.Client().Revoke(ctx, clientv3.LeaseID(resp.Kvs[0].Lease))
		}
		cancel()
		if err != nil {
			return "", fmt.Errorf("revoking %s's liveness lease: %w", victim.name, err)
		}
	}
	var lost control.Operation
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		cur, err := soakGet(survivor, op.ID)
		if err != nil {
			return "", err
		}
		if cur.Terminal() {
			return "", fmt.Errorf("ramp %s ended %s after its owner %s stopped: %+v", op.ID, cur.Status, victim.name, cur)
		}
		if cur.Status == control.StatusBlocked && len(cur.Blockers) == 1 && cur.Blockers[0].Code == control.BlockerOwnerLost {
			lost = cur
			break
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("ramp %s not marked owner_lost on %s %s after %s stopped: %s %+v", op.ID, survivor.name, time.Since(crashed).Round(time.Millisecond), victim.name, cur.Status, cur.Blockers)
		}
	}
	lapse := time.Since(crashed)
	if !slices.Contains(lost.AllowedActions, control.ActionResume) {
		return "", fmt.Errorf("owner_lost ramp %s does not allow resume: %v", op.ID, lost.AllowedActions)
	}
	var resumed control.Operation
	if code, body := soakCall(survivor.url(), http.MethodPost, "/v1/operations/"+op.ID+"/resume", struct{}{}, &resumed); code != http.StatusAccepted {
		return "", fmt.Errorf("resume of %s on %s: %d %s", op.ID, survivor.name, code, body)
	}
	// Resumed, it drains again: still blocked while B is cut.
	for end := time.Now().Add(time.Duration(300+rnd.IntN(500)) * time.Millisecond); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		cur, err := soakGet(survivor, op.ID)
		if err != nil {
			return "", err
		}
		if cur.Terminal() {
			return "", fmt.Errorf("resumed ramp %s ended %s on %s with B still cut: %+v", op.ID, cur.Status, survivor.name, cur)
		}
	}
	sr.cut.Store(false)
	done, err := soakWait(survivor, op.ID, 20*time.Second)
	if serr := soakSucceeded(done, err); serr != nil {
		return "", fmt.Errorf("resumed ramp after B reconnected: %w", serr)
	}
	if done.Node != survivor.name || done.OwnerTerm < 1 || done.Barrier == nil || !done.Barrier.Committed {
		return "", fmt.Errorf("resumed ramp %s ended on %s at owner term %d, barrier %+v; want %s, a new term, committed", op.ID, done.Node, done.OwnerTerm, done.Barrier, survivor.name)
	}
	restarted := time.Now()
	sr.nodes[vi] = sr.startNode(t, victim.name, victim.addr)
	return fmt.Sprintf("crash %s: ramp %s held (%d misses), owner_lost on %s after %s (fast %v), resumed at term %d, done; %s restarted in %s",
		victim.name, op.ID, misses, survivor.name, lapse.Round(time.Millisecond), fast, done.OwnerTerm, victim.name,
		time.Since(restarted).Round(time.Millisecond)), nil
}

// moverPass copies what the source holds to the target as `shunt migrate run` does (testMover,
// conditional): never over a client's newer write, withdrawing a copy whose source was deleted.
func (sr *soakRun) moverPass(src, dst string) (int, error) {
	mv := &testMover{m: sr.m, source: src, target: dst, putIfNoneMatch: true, deleteIfMatch: true}
	sr.m.garage.mu.Lock()
	keys := slices.Sorted(maps.Keys(sr.m.garage.buckets[src]))
	sr.m.garage.mu.Unlock()
	copied := 0
	for _, k := range keys {
		data, ok := mv.read(k)
		if !ok {
			continue
		}
		if err := mv.commit(k, "", data, func(op string, status int, _, _ string) {
			if op == "put_target" && status == http.StatusOK {
				copied++
			}
		}); err != nil {
			return copied, err
		}
	}
	return copied, nil
}

// soakSample is one growth reading, at a quiet point.
type soakSample struct {
	goroutines int
	heapInuse  uint64
	retired    [2]int
	pending    [2]int64
	transports [2]int
}

func (s soakSample) String() string {
	return fmt.Sprintf("goroutines %d, heap in use %.1f MiB, retained bundles A %d B %d (pending %d/%d), upstream transports A %d B %d",
		s.goroutines, float64(s.heapInuse)/(1<<20), s.retired[0], s.retired[1], s.pending[0], s.pending[1], s.transports[0], s.transports[1])
}

// sample reads goroutines, heap and each proxy's bundles and transports. The idle connections of
// the test's own clients and of the proxies' upstream transports are closed first, and the
// goroutine count is the least of a few readings, so connection goroutines on their way out are
// not counted as growth.
func (sr *soakRun) sample() soakSample {
	fleetClient.CloseIdleConnections()
	soakHTTP.CloseIdleConnections()
	for i := range sr.sets {
		sr.sets[i].Load().Close() // idle upstream connections only; a stuck one stays and counts
	}
	var s soakSample
	s.goroutines = runtime.NumGoroutine()
	for range 10 {
		time.Sleep(50 * time.Millisecond)
		s.goroutines = min(s.goroutines, runtime.NumGoroutine())
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.heapInuse = ms.HeapInuse
	for i := range sr.rts {
		st := sr.rts[i].Stats()
		s.retired[i], s.pending[i] = st.Retired, st.Pending
		seen := map[*http.Transport]bool{}
		set := sr.sets[i].Load()
		for _, name := range set.Names() {
			if cl, ok := set.Get(name); ok {
				seen[cl.Transport] = true
			}
		}
		s.transports[i] = len(seen)
	}
	return s
}

// profile writes every goroutine's stack, grouped, to goroutines-<name>.txt in the run directory
// (SHUNT_SOAK_RUN_DIR, which `make soak` sets), so growth can be read from the two files.
func (sr *soakRun) profile(t *testing.T, name string) {
	dir := os.Getenv("SHUNT_SOAK_RUN_DIR")
	if dir == "" {
		return
	}
	var b bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&b, 1); err != nil {
		t.Errorf("goroutine profile: %v", err)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "goroutines-"+name+".txt"), b.Bytes(), 0o644); err != nil {
		t.Errorf("goroutine profile: %v", err)
	}
}

// soakSlack is how many more goroutines the end of the soak may have than the warm baseline: 50,
// or 10% of the baseline when that is more. A quiet point still holds goroutines whose number
// wanders a little from one cycle to the next without anything leaking: the proxies' idle
// upstream connections (two per connection on the client side, one in the fake backend), the
// members' long-poll and heartbeat connections to whichever control node they last moved to, and
// embedded etcd's lease and watch streams. A leak per request, per step, per crash or per
// reconnect grows with the cycles and passes this within a few of them.
func soakSlack(base int) int { return max(50, base/10) }

func TestFleetSoak(t *testing.T) {
	raw := os.Getenv("SHUNT_SOAK_DURATION")
	if raw == "" {
		t.Skip("the H2 fleet soak runs for minutes; `make soak` runs it (SOAK_TIME, default 10m; SOAK_SEED to replay)")
	}
	dur, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("SHUNT_SOAK_DURATION %q: %v", raw, err)
	}
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("SHUNT_SOAK_SEED"); s != "" {
		if seed, err = strconv.ParseUint(s, 10, 64); err != nil {
			t.Fatalf("SHUNT_SOAK_SEED %q: %v", s, err)
		}
	}
	t.Logf("soak %s, seed %d (replay: make soak SOAK_SEED=%d)", dur, seed, seed)
	t.Logf("negative control: TestFleetWithoutTheFence runs the same model with steps written straight into the store, no fence and no hold, and must find a violation")
	sr := newSoakRun(t)
	rnd := rand.New(rand.NewPCG(seed, 0x50a4))

	began := time.Now()
	var base soakSample
	steps, crashes := 0, 0
	cycle := 0
	for ; cycle == 0 || time.Since(began) < dur; cycle++ {
		cycleStart := time.Now()
		bucket := fmt.Sprintf("soak-%d", cycle)
		src, dst := "acme-"+bucket, "acme-"+bucket+"-t"
		sr.m.garage.addBucket(src)
		sr.m.minio.addBucket(dst)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		store := sr.nodes[rnd.IntN(2)].store
		if err := store.Adopt(ctx, "acme", bucket, "garage", src, "soak"); err != nil {
			t.Fatal(err)
		}
		if err := store.SetTarget(ctx, "acme", bucket, "minio", dst, "soak"); err != nil {
			t.Fatal(err)
		}
		cancel()
		// Adopting a bucket is not a fenced step: the clients start once both proxies have it.
		if err := sr.settled(sr.nodes[0]); err != nil {
			t.Fatalf("seed %d cycle %d: %v", seed, cycle, err)
		}

		// Keys no client touches, written before the move: only the mover brings them to the
		// target, and until it does every read of them falls back to the source.
		const statics = 8
		for i := range statics {
			if r := sendTo(t, sr.proxies[i%2].front.URL, "PUT", fmt.Sprintf("/%s/static/%d", bucket, i), []byte(bucket+" static")); r.StatusCode != http.StatusOK {
				t.Fatalf("seed %d cycle %d: seeding static/%d: %d %s", seed, cycle, i, r.StatusCode, r.body)
			}
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		const workers, keys = 8, 48
		for w := range workers {
			wg.Add(1)
			go func() { defer wg.Done(); sr.client(t, bucket, w, workers, keys, seed+uint64(cycle), stop) }()
		}
		opsBefore := sr.ops.Load()

		partitionAt := rnd.IntN(len(soakRatios) + 1) // len(soakRatios): migrate start
		crashAt := rnd.IntN(len(soakRatios))
		for crashAt == partitionAt {
			crashAt = rnd.IntN(len(soakRatios))
		}
		victim := cycle % 2
		var trace []string
		failed := func(format string, args ...any) {
			close(stop)
			wg.Wait()
			t.Fatalf("seed %d cycle %d acme/%s: %s\ntrace so far:\n%s", seed, cycle, bucket, fmt.Sprintf(format, args...), joinLines(trace))
		}
		for i := 0; i <= len(soakRatios); i++ {
			time.Sleep(time.Duration(150+rnd.IntN(200)) * time.Millisecond)
			kind, what := control.OpMigrate, "migrate"
			var args any = control.MigrateRequest{Wait: "5s"}
			if i < len(soakRatios) {
				kind, what = control.OpRamp, fmt.Sprintf("ramp %v", soakRatios[i])
				args = control.RampRequest{Ratio: soakRatios[i], Wait: "5s"}
			}
			stepStart := time.Now()
			n := sr.nodes[rnd.IntN(2)]
			switch i {
			case partitionAt:
				hold := time.Duration(800+rnd.IntN(1700)) * time.Millisecond
				line, err := sr.partition(n, kind, bucket, args, hold)
				if err != nil {
					failed("%s: %v", what, err)
				}
				trace = append(trace, fmt.Sprintf("%s: %s", what, line))
			case crashAt:
				crashes++
				line, err := sr.crash(t, victim, bucket, args.(control.RampRequest), crashes%2 == 0, rnd)
				if err != nil {
					failed("%s: %v", what, err)
				}
				trace = append(trace, fmt.Sprintf("%s: %s", what, line))
			default:
				op, err := sr.step(n, kind, bucket, args)
				if err != nil {
					failed("%s on %s: %v", what, n.name, err)
				}
				trace = append(trace, fmt.Sprintf("%s: %s on %s, done in %s", what, op.ID, n.name, time.Since(stepStart).Round(time.Millisecond)))
			}
			steps++
		}
		copied, err := sr.moverPass(src, dst)
		if err != nil {
			failed("mover pass: %v", err)
		}
		time.Sleep(300 * time.Millisecond) // let the clients read across the last step
		close(stop)
		wg.Wait()

		for i := range statics {
			if _, ok := sr.m.minio.object(dst, fmt.Sprintf("static/%d", i)); !ok {
				sr.violation("static/%d is not on the target after the mover pass", i)
			}
			for _, p := range sr.proxies {
				if r := sendTo(t, p.front.URL, "GET", fmt.Sprintf("/%s/static/%d", bucket, i), nil); r.StatusCode != http.StatusOK || string(r.body) != bucket+" static" {
					sr.violation("GET /%s/static/%d via %s: %d %q, want the write from before the move (%s)", bucket, i, p.name, r.StatusCode, r.body, sr.describeIn(p, bucket))
				}
			}
		}
		if v := sr.violations(); len(v) > 0 {
			t.Fatalf("seed %d cycle %d acme/%s: %d violations of the client's view:\n%s\ntrace:\n%s", seed, cycle, bucket, len(v), joinLines(v), joinLines(trace))
		}
		pl, ok := sr.nodes[0].store.Snapshot().Lookup("acme", bucket)
		if !ok || pl.State != directory.StateMigrating || pl.Held() || pl.Barrier != nil {
			t.Fatalf("seed %d cycle %d: acme/%s ended %+v, want MIGRATING with no hold", seed, cycle, bucket, pl)
		}
		s := sr.sample()
		for i := range s.retired {
			if s.retired[i] > runtimecfg.MaxRetired {
				t.Fatalf("seed %d cycle %d: proxy %s retains %d bundles, over the bound of %d", seed, cycle, sr.proxies[i].name, s.retired[i], runtimecfg.MaxRetired)
			}
		}
		if cycle == 0 {
			base = s
			sr.profile(t, "warm")
		}
		t.Logf("cycle %d acme/%s in %s: %d client operations (%d total, %d writes retried after a 503), mover copied %d; partition at step %d, crash of %s at step %d\n%s  %s",
			cycle, bucket, time.Since(cycleStart).Round(time.Millisecond), sr.ops.Load()-opsBefore, sr.ops.Load(), sr.retried.Load(), copied,
			partitionAt, sr.nodes[victim].name, crashAt, joinLines(trace), s)
	}

	// Recovery: nothing is cut, both control nodes are up, and every step has ended.
	end := sr.sample()
	sr.profile(t, "end")
	t.Logf("soak done: %d cycles, %d steps, %d owner crashes, %d client operations, %d writes retried after a 503, 0 violations, in %s",
		cycle, steps, crashes, sr.ops.Load(), sr.retried.Load(), time.Since(began).Round(time.Second))
	t.Logf("growth: warm (after cycle 0): %s", base)
	t.Logf("growth: end (after recovery): %s", end)
	if end.goroutines > base.goroutines+soakSlack(base.goroutines) {
		t.Errorf("goroutines grew from %d to %d, past the slack of %d", base.goroutines, end.goroutines, soakSlack(base.goroutines))
	}
	for i := range end.retired {
		if end.retired[i] > base.retired[i] || end.pending[i] != 0 {
			t.Errorf("proxy %s retains %d bundles (pending %d) after recovery, %d at the start", sr.proxies[i].name, end.retired[i], end.pending[i], base.retired[i])
		}
		if end.transports[i] > base.transports[i] {
			t.Errorf("proxy %s has %d upstream transports after recovery, %d at the start", sr.proxies[i].name, end.transports[i], base.transports[i])
		}
	}
}
