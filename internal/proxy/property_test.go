package proxy

// The POC-4 property test (docs/POC.md P5 item 5). Clients read, write and delete one bucket
// without pause while the operator ramps the writes onto a second cluster, migrates, runs a mover
// over the data, and cuts over. The property under test is the client's view:
//
//	a successful write is immediately readable, with the bytes that were written;
//	a successful delete stays deleted;
//	once the dust settles, a listing names exactly the objects that are still there.
//
// The middle invariant has one honest exception, recorded in docs/adr/0004-migration-races.md: a
// mover that has already read an object when the client deletes it will write its copy to the new
// primary and then take it back out, so a delete can be briefly undone while a copy is in flight.
// The test allows that window only while a mover pass is running, and only for as long as one pass.
//
// It runs for SHUNT_PROPERTY_DURATION (default 3s, 2m in CI, 10m for a local soak).

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
)

const propertyKeys = 120

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

// modelKey is one key's authoritative state. A worker holds the lock across the request and the
// model update, so the model is never a guess about what the client did.
type modelKey struct {
	sync.Mutex
	present bool
	body    string
	deleted time.Time // when the delete that emptied it returned
}

type propertyRun struct {
	m       *mixedRig
	keys    []*modelKey
	moving  atomic.Bool // a mover pass is in flight
	ops     atomic.Int64
	fails   atomic.Int64
	stopped atomic.Bool
}

func TestMigrationPreservesTheClientsView(t *testing.T) {
	dur := propertyDuration(t)
	if testing.Short() {
		t.Skip("property test skipped in -short")
	}
	m := newMixedRig(t, nil)
	r := &propertyRun{m: m, keys: make([]*modelKey, propertyKeys)}
	for i := range r.keys {
		r.keys[i] = &modelKey{}
	}
	const source, target = "acme-1111-data", "acme-9000-data"
	m.minio.addBucket(target)
	m.backendNames = append(m.backendNames, target)

	// Seed, so the migration has something to move before the first write lands.
	for i := 0; i < propertyKeys/2; i++ {
		body := fmt.Sprintf("seed-%d", i)
		if code, _, err := r.do(http.MethodPut, key(i), []byte(body)); err != nil || code != 200 {
			t.Fatalf("seeding %s: %d %v", key(i), code, err)
		}
		r.keys[i].present, r.keys[i].body = true, body
	}

	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r.client(t, rand.New(rand.NewSource(seed)), deadline) //nolint:gosec // G404: test load, not crypto
		}(int64(w) + 1)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.operator(t, source, target, deadline)
	}()
	wg.Wait()

	if r.fails.Load() > 0 {
		t.Fatalf("%d property violations over %d operations", r.fails.Load(), r.ops.Load())
	}

	// Quiescent: the listing must name exactly the objects the client still has.
	r.checkListing(t)
	t.Logf("property: %d operations in %s, no violations", r.ops.Load(), dur)
}

func key(i int) string { return fmt.Sprintf("obj/%04d", i) }

// client is one pretend application: it writes, deletes and reads, and holds the model to account.
func (r *propertyRun) client(t *testing.T, rnd *rand.Rand, deadline time.Time) {
	for time.Now().Before(deadline) && !r.stopped.Load() {
		i := rnd.Intn(propertyKeys)
		k := r.keys[i]
		k.Lock()
		switch n := rnd.Intn(10); {
		case n < 4: // write
			body := fmt.Sprintf("v%d-%d", i, rnd.Int63())
			code, _, err := r.do(http.MethodPut, key(i), []byte(body))
			switch {
			case err != nil:
				r.violation(t, "PUT %s: %v", key(i), err)
			case code == 200:
				k.present, k.body = true, body
			default:
				r.violation(t, "PUT %s: HTTP %d", key(i), code)
			}
		case n < 6: // delete
			code, _, err := r.do(http.MethodDelete, key(i), nil)
			switch {
			case err != nil:
				r.violation(t, "DELETE %s: %v", key(i), err)
			case code == 204:
				k.present, k.body, k.deleted = false, "", time.Now()
			default:
				r.violation(t, "DELETE %s: HTTP %d", key(i), code)
			}
		default: // read
			r.read(t, i, k)
		}
		k.Unlock()
		r.ops.Add(1)
	}
}

// read checks one key against the model, allowing only the documented copy-in-flight window.
func (r *propertyRun) read(t *testing.T, i int, k *modelKey) {
	code, body, err := r.do(http.MethodGet, key(i), nil)
	if err != nil {
		r.violation(t, "GET %s: %v", key(i), err)
		return
	}
	switch {
	case k.present && code != 200:
		r.violation(t, "GET %s: HTTP %d, but the client wrote it and never deleted it", key(i), code)
	case k.present && string(body) != k.body:
		r.violation(t, "GET %s: read %q, wrote %q", key(i), body, k.body)
	case !k.present && code == 200:
		// A copy already in flight when the delete landed may put the object back for as long as
		// that one object's copy takes; the mover then removes it (ADR-0004). Outside a mover pass
		// there is no such excuse.
		if !r.moving.Load() {
			r.violation(t, "GET %s: deleted %s ago but still readable, with no mover running", key(i), time.Since(k.deleted).Round(time.Millisecond))
			return
		}
		if ok := r.eventuallyGone(i, 5*time.Second); !ok {
			r.violation(t, "GET %s: deleted and still readable 5s after a mover pass touched it", key(i))
		}
	case !k.present && code != 404:
		r.violation(t, "GET %s: HTTP %d, expected 404 for a deleted key", key(i), code)
	}
}

// eventuallyGone waits for the mover to withdraw a copy it made of a deleted object.
func (r *propertyRun) eventuallyGone(i int, within time.Duration) bool {
	for end := time.Now().Add(within); time.Now().Before(end); {
		time.Sleep(20 * time.Millisecond)
		if code, _, err := r.do(http.MethodGet, key(i), nil); err == nil && code == 404 {
			return true
		}
	}
	return false
}

// operator walks the migration while the clients keep working.
func (r *propertyRun) operator(t *testing.T, source, target string, deadline time.Time) {
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
			tr.Target, tr.Name = "minio", target
		}
		if err := r.m.dir.SetState(t.Context(), "acme", "data", from, tr, "property"); err != nil {
			r.violation(t, "set-state %s: %v", tr.To, err)
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
	for pass := 0; pass < 2 && time.Now().Before(deadline); pass++ {
		r.mover(t, source, target)
		sleep()
	}
	set(directory.Transition{To: directory.StateCutover})
	sleep()
	set(directory.Transition{To: directory.StateActive})

	// Stop the clients a little before the deadline so the final listing check is quiescent.
	time.Sleep(time.Until(deadline))
	r.stopped.Store(true)
}

// mover is test/mover's contract against the fake clusters: copy what the source still holds,
// never overwrite a client's newer write, and withdraw a copy of an object that has been deleted.
func (r *propertyRun) mover(t *testing.T, source, target string) {
	r.moving.Store(true)
	defer r.moving.Store(false)
	for i := 0; i < propertyKeys; i++ {
		data, ok := r.m.garage.object(source, key(i))
		if !ok {
			continue
		}
		req, err := http.NewRequest(http.MethodPut, "http://"+r.m.mAddr+"/"+target+"/"+key(i), bytes.NewReader(data))
		if err != nil {
			r.violation(t, "mover PUT %s: %v", key(i), err)
			return
		}
		req.ContentLength = int64(len(data))
		req.Header.Set("If-None-Match", "*") // the target's capability profile allows it
		sigv4.Sign(req, sigv4.Credentials{AccessKey: "MINIOCLUSTERKEY", Secret: "minio-cluster-secret"}, "us-east-1", sigv4.UnsignedPayload, time.Now())
		resp, err := fresh().Do(req)
		if err != nil {
			r.violation(t, "mover PUT %s: %v", key(i), err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusPreconditionFailed {
			continue // a client got there first; its copy is the newer one
		}
		// Re-HEAD the source: if the object was deleted while this copy was in flight, the copy is
		// a resurrection and has to come back out.
		if _, still := r.m.garage.object(source, key(i)); !still {
			r.m.minio.deleteObject(target, key(i))
		}
	}
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
		if k.present {
			want = append(want, key(i))
		}
	}
	sort.Strings(want)

	var got []string
	token := ""
	for page := 0; page < 20; page++ {
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
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("listing has %d keys, the client has %d\nonly in the listing: %v\nonly in the client's model: %v",
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
func (r *propertyRun) do(method, k string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(method, r.m.front.URL+"/data/"+k, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.ContentLength = int64(len(body))
	if len(body) == 0 {
		req.Body = http.NoBody
	}
	sigv4.Sign(req, sigv4.Credentials{AccessKey: acmeAK, Secret: acmeSK}, "us-east-1", sigv4.UnsignedPayload, time.Now())
	resp, err := fresh().Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// violation records one broken property. The run continues so the report counts them all.
func (r *propertyRun) violation(t *testing.T, format string, a ...any) {
	t.Helper()
	if n := r.fails.Add(1); n <= 20 {
		t.Errorf("property violation: "+format, a...)
	}
}
