package proxy

// The guarded property run's classifier. A target that ignores If-None-Match: * leaves the mover a
// HEAD-then-commit guard, and ADR-0004 race 2 documents the write it can lose: a client PUT that
// commits on the target after the mover's HEAD found the key absent and before the mover's own PUT
// commits over it. lossLedger reads the target's commit stream (the fake backend's observer runs
// under its lock, so events arrive in commit order) and records every write lost that way. A
// violation is a known-window loss only when it is a stale read of exactly such a write: the model
// holds the lost client body and the read returned the mover's bytes. Anything else still fails.

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

// propLoss is one client write overwritten inside the HEAD-then-commit window.
type propLoss struct {
	N           int       `json:"n"`
	Key         string    `json:"key"`
	MoverOp     string    `json:"mover_op"`
	MoverHeadAt time.Time `json:"mover_head_committed"`
	ClientOp    string    `json:"client_op"`
	ClientBody  string    `json:"client_body"`
	ClientPutAt time.Time `json:"client_put_committed"`
	MoverBody   string    `json:"mover_body"`
	MoverPutAt  time.Time `json:"mover_put_committed"`
	Reads       int64     `json:"stale_reads"` // violations that are reads of this loss
}

// openCopy is a mover copy whose HEAD found the key absent and whose PUT has not committed yet.
type openCopy struct {
	moverOp string
	headAt  time.Time
	lastPut *backendEvent // the latest client PUT that committed since the HEAD, if any
}

type lossLedger struct {
	mu     sync.Mutex
	open   map[string]*openCopy // key -> the mover copy in its window
	losses []*propLoss
}

func newLossLedger() *lossLedger { return &lossLedger{open: map[string]*openCopy{}} }

func isMoverOp(id string) bool { return strings.HasPrefix(id, "mover-") }

// observe takes one committed request on the migration target, in commit order.
func (l *lossLedger) observe(ev backendEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	oc := l.open[ev.key]
	switch {
	case isMoverOp(ev.opID) && ev.method == http.MethodHead:
		delete(l.open, ev.key)
		if ev.status == http.StatusNotFound {
			l.open[ev.key] = &openCopy{moverOp: ev.opID, headAt: ev.end}
		}
	case oc == nil:
	case ev.opID == oc.moverOp && ev.method == http.MethodPut:
		delete(l.open, ev.key)
		if ev.status == http.StatusOK && oc.lastPut != nil {
			// Earlier client PUTs in the window were superseded by later client PUTs, not by the
			// mover; only the last one is the write the mover destroyed.
			l.losses = append(l.losses, &propLoss{N: len(l.losses) + 1, Key: ev.key, MoverOp: oc.moverOp, MoverHeadAt: oc.headAt,
				ClientOp: oc.lastPut.opID, ClientBody: string(oc.lastPut.body), ClientPutAt: oc.lastPut.end,
				MoverBody: string(ev.body), MoverPutAt: ev.end})
		}
	case !isMoverOp(ev.opID) && ev.method == http.MethodPut && ev.status == http.StatusOK:
		put := ev
		oc.lastPut = &put
	case !isMoverOp(ev.opID) && ev.method == http.MethodDelete && ev.status/100 == 2:
		// A delete in the window leaves nothing for the mover to overwrite; what follows is the
		// delete/copy race (race 1), which this classifier does not excuse.
		oc.lastPut = nil
	}
}

// staleRead reports the loss a read belongs to: the model holds a lost client body for the key and
// the read returned the bytes the mover wrote over it.
func (l *lossLedger) staleRead(key, modelBody, readBody string) (*propLoss, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.losses) - 1; i >= 0; i-- {
		loss := l.losses[i]
		if loss.Key == key && loss.ClientBody == modelBody && loss.MoverBody == readBody {
			loss.Reads++
			return loss, true
		}
	}
	return nil, false
}

// snapshot copies the recorded losses for run.json.
func (l *lossLedger) snapshot() []propLoss {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]propLoss, len(l.losses))
	for i, loss := range l.losses {
		out[i] = *loss
	}
	return out
}

func TestLossLedgerRecognisesTheHeadThenCommitWindow(t *testing.T) {
	at := time.Now()
	ev := func(op, method string, status int, body string) backendEvent {
		at = at.Add(time.Millisecond)
		return backendEvent{end: at, method: method, key: "k", status: status, body: []byte(body), opID: op}
	}
	cases := []struct {
		name   string
		events []backendEvent
		losses int
		lost   string
	}{
		{"client PUT between the mover's HEAD and PUT", []backendEvent{
			ev("mover-1", "HEAD", 404, ""), ev("op-1", "PUT", 200, "client"), ev("mover-1", "PUT", 200, "seed"),
		}, 1, "client"},
		{"two client PUTs in the window: only the last is lost to the mover", []backendEvent{
			ev("mover-1", "HEAD", 404, ""), ev("op-1", "PUT", 200, "first"), ev("op-2", "PUT", 200, "second"), ev("mover-1", "PUT", 200, "seed"),
		}, 1, "second"},
		{"client PUT before the mover's HEAD", []backendEvent{
			ev("op-1", "PUT", 200, "client"), ev("mover-1", "HEAD", 404, ""), ev("mover-1", "PUT", 200, "seed"),
		}, 0, ""},
		{"client PUT after the mover's PUT", []backendEvent{
			ev("mover-1", "HEAD", 404, ""), ev("mover-1", "PUT", 200, "seed"), ev("op-1", "PUT", 200, "client"),
		}, 0, ""},
		{"the mover's HEAD found the key", []backendEvent{
			ev("mover-1", "HEAD", 200, ""), ev("op-1", "PUT", 200, "client"), ev("mover-1", "PUT", 200, "seed"),
		}, 0, ""},
		{"client PUT then DELETE in the window", []backendEvent{
			ev("mover-1", "HEAD", 404, ""), ev("op-1", "PUT", 200, "client"), ev("op-2", "DELETE", 204, ""), ev("mover-1", "PUT", 200, "seed"),
		}, 0, ""},
		{"a different mover copy's PUT", []backendEvent{
			ev("mover-1", "HEAD", 404, ""), ev("op-1", "PUT", 200, "client"), ev("mover-2", "PUT", 200, "seed"),
		}, 0, ""},
	}
	for _, tc := range cases {
		l := newLossLedger()
		for _, e := range tc.events {
			l.observe(e)
		}
		got := l.snapshot()
		if len(got) != tc.losses {
			t.Errorf("%s: %d losses, want %d: %+v", tc.name, len(got), tc.losses, got)
			continue
		}
		if tc.losses == 1 && got[0].ClientBody != tc.lost {
			t.Errorf("%s: lost %q, want %q", tc.name, got[0].ClientBody, tc.lost)
		}
	}

	l := newLossLedger()
	for _, e := range cases[0].events {
		l.observe(e)
	}
	if _, ok := l.staleRead("k", "client", "seed"); !ok {
		t.Error("a read of the mover's bytes where the client wrote the lost body is the known window")
	}
	for _, bad := range [][2]string{{"client", "other"}, {"other", "seed"}} {
		if _, ok := l.staleRead("k", bad[0], bad[1]); ok {
			t.Errorf("model %q, read %q: classified as the known window, and must not be", bad[0], bad[1])
		}
	}
	if _, ok := l.staleRead("other-key", "client", "seed"); ok {
		t.Error("another key's read classified as this key's loss")
	}
	if got := l.snapshot()[0].Reads; got != 1 {
		t.Errorf("stale reads counted %d, want 1", got)
	}
}

// The whole guarded path, end to end: a client PUT forced between the mover's HEAD and its PUT is
// seen in the target's commit stream, the stale read that follows is classified as that loss, and
// the run's verdict passes with one distinct lost write.
func TestGuardedRunClassifiesAKnownWindowLoss(t *testing.T) {
	t.Setenv("SHUNT_PROPERTY_RUN_DIR", t.TempDir())
	r := newPropertyRun(t, 1, "test", time.Second, "guarded", "re-head")
	const i = churnKeys + 1
	k := r.keys[i]
	if code, _, err := r.do(http.MethodPut, key(i), []byte("seed-body"), r.rec.nextID("seed")); err != nil || code != 200 {
		t.Fatalf("seed: %d %v", code, err)
	}
	k.Present, k.Body = true, "seed-body"
	if err := r.m.dir.SetState(t.Context(), "acme", "data", directory.StateActive,
		directory.Transition{To: directory.StateMigrating, Target: "minio", Name: propTarget}, "test"); err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	r.m.minio.mu.Lock()
	r.m.minio.before = func(req *http.Request) {
		if req.Method != http.MethodPut || !isMoverOp(req.Header.Get(opHeader)) {
			return
		}
		once.Do(func() {
			k.Lock()
			defer k.Unlock()
			if code, _, err := r.do(http.MethodPut, key(i), []byte("client-body"), r.rec.nextID("op")); err != nil || code != 200 {
				t.Errorf("client PUT in the window: %d %v", code, err)
				return
			}
			k.Body = "client-body"
		})
	}
	r.m.minio.mu.Unlock()

	data, ok := r.mover.read(key(i))
	if !ok {
		t.Fatal("not on the source")
	}
	if err := r.mover.commit(key(i), r.rec.nextID("mover"), data, func(string, int, string, string) {}); err != nil {
		t.Fatal(err)
	}
	k.Lock()
	r.read(t, "client-0", r.rec.nextID("op"), i, k, propEvent{Start: time.Now(), Actor: "client-0", Key: key(i)})
	k.Unlock()

	losses := r.losses.snapshot()
	if len(losses) != 1 || losses[0].ClientBody != "client-body" || losses[0].MoverBody != "seed-body" {
		t.Fatalf("losses: %+v", losses)
	}
	if r.fails.Load() != 1 || r.knownReads.Load() != 1 {
		t.Errorf("violations %d, known-window %d; want 1 and 1", r.fails.Load(), r.knownReads.Load())
	}
	if h := r.headline(); h.LostWrites != 1 || h.ViolationsOutsideWindow != 0 || !strings.HasPrefix(h.Verdict, "pass:") {
		t.Errorf("headline %+v", h)
	}
	r.tolerateLoss = false // the conditional variant's rule
	if h := r.headline(); !strings.HasPrefix(h.Verdict, "fail:") {
		t.Errorf("a variant that tolerates no loss must fail: %+v", h)
	}
	r.tolerateLoss = true
}

// The headline counts losses from the commit history, not from reads: a write overwritten in the
// window and replaced by a later client write before anyone read it is still one lost write, and
// still fails the conditional variant, with no violation on record.
func TestHeadlineCountsLossesNobodyRead(t *testing.T) {
	l := newLossLedger()
	now := time.Now()
	for _, e := range []backendEvent{
		{end: now, method: "HEAD", key: "k", status: 404, opID: "mover-1"},
		{end: now, method: "PUT", key: "k", status: 200, body: []byte("client"), opID: "op-1"},
		{end: now, method: "PUT", key: "k", status: 200, body: []byte("seed"), opID: "mover-1"},
		{end: now, method: "PUT", key: "k", status: 200, body: []byte("later"), opID: "op-2"},
	} {
		l.observe(e)
	}
	guarded := &propertyRun{losses: l, tolerateLoss: true}
	if h := guarded.headline(); h.LostWrites != 1 || h.ViolationRecords != 0 || !strings.HasPrefix(h.Verdict, "pass: 1 distinct") {
		t.Errorf("guarded: %+v", h)
	}
	conditional := &propertyRun{losses: l}
	if h := conditional.headline(); h.LostWrites != 1 || !strings.HasPrefix(h.Verdict, "fail:") {
		t.Errorf("conditional: a loss nobody read must still fail: %+v", h)
	}
}

func BenchmarkLossLedgerObserve(b *testing.B) {
	l := newLossLedger()
	now := time.Now()
	events := []backendEvent{
		{end: now, method: "HEAD", key: "k", status: 404, opID: "mover-1"},
		{end: now, method: "PUT", key: "k", status: 200, body: []byte("client"), opID: "op-1"},
		{end: now, method: "PUT", key: "k", status: 200, body: []byte("seed"), opID: "mover-1"},
	}
	for b.Loop() {
		for _, e := range events {
			l.observe(e)
		}
		l.losses = l.losses[:0]
	}
}
