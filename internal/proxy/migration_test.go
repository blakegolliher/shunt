package proxy

// POC-4: the routing table of docs/DESIGN.md §2.5 driven through the handler, against two fake
// clusters. Each test puts a bucket into a migration state and checks where requests actually go.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

// ramp moves acme/data from garage to minio and returns the target's backend bucket name.
func ramp(t testing.TB, m *mixedRig, tr directory.Transition) string {
	t.Helper()
	const target = "acme-9000-data"
	m.minio.addBucket(target)
	m.backendNames = append(m.backendNames, target)
	tr.Target, tr.Name = "minio", target
	if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateActive, tr, "test"); err != nil {
		t.Fatal(err)
	}
	return target
}

// While RAMPING, a key's own hash or prefix decides which cluster its writes go to, and its reads
// follow the same rule.
func TestRampSplitsWritesByKey(t *testing.T) {
	m := newMixedRig(t, nil)
	target := ramp(t, m, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}})

	if r := m.acme(t, "PUT", "/data/moved/k", []byte("on the target")); r.StatusCode != 200 {
		t.Fatalf("write in range: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "PUT", "/data/stay/k", []byte("on the source")); r.StatusCode != 200 {
		t.Fatalf("write out of range: %d %s", r.StatusCode, r.body)
	}
	if b, ok := m.minio.object(target, "moved/k"); !ok || string(b) != "on the target" {
		t.Fatalf("prefixed key did not go to the target: %q %v", b, ok)
	}
	if b, ok := m.garage.object("acme-1111-data", "stay/k"); !ok || string(b) != "on the source" {
		t.Fatalf("unprefixed key did not stay on the source: %q %v", b, ok)
	}
	// Reads follow the same split, so neither side is asked for a key it cannot have.
	if r := m.acme(t, "GET", "/data/stay/k", nil); string(r.body) != "on the source" {
		t.Fatalf("read out of range: %d %q", r.StatusCode, r.body)
	}
	if r := m.acme(t, "GET", "/data/moved/k", nil); string(r.body) != "on the target" {
		t.Fatalf("read in range: %d %q", r.StatusCode, r.body)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_ramp_writes_total", `bucket="acme/data",side="primary"`); v != 1 {
		t.Errorf("writes to the target: %v", v)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_ramp_writes_total", `bucket="acme/data",side="source"`); v != 1 {
		t.Errorf("writes to the source: %v", v)
	}
}

// A ramp written by a build whose hash this proxy does not implement is refused where the hash
// decides, never re-split: routing it by another hash would send a key's writes to the other side
// mid-ramp (ADR-0004, POC-5 amendment). Requests the hash does not decide still work.
func TestUnknownRampHashRefusesInsteadOfResplitting(t *testing.T) {
	m := newMixedRig(t, nil)
	ramp(t, m, directory.Transition{To: directory.StateRamping, Ratio: 0.5, Prefixes: []string{"moved/"}})
	if p, _ := m.dir.Snapshot().Lookup("acme", "data"); p.Ramp.Hash != directory.RampHash {
		t.Fatalf("RAMPING start did not record the hash: %+v", p.Ramp)
	}
	raw, err := os.ReadFile(m.dirPath)
	if err != nil {
		t.Fatal(err)
	}
	f, err := directory.Load(m.dirPath)
	if err != nil {
		t.Fatal(err)
	}
	next := strings.Replace(string(raw), "hash: "+directory.RampHash, "hash: fnv1a-fmix64-v2", 1)
	next = strings.Replace(next, fmt.Sprintf("version: %d", f.Version), fmt.Sprintf("version: %d", f.Version+1), 1)
	if next == string(raw) || !strings.Contains(next, "fnv1a-fmix64-v2") {
		t.Fatalf("could not rewrite the ramp hash in:\n%s", raw)
	}
	if err := os.WriteFile(m.dirPath, []byte(next), 0o600); err != nil {
		t.Fatal(err)
	}
	if changed, err := m.dir.Reload(); err != nil || !changed {
		t.Fatalf("a directory naming an unknown ramp hash must still load: %v %v", changed, err)
	}

	for _, req := range []struct{ method, path string }{{"PUT", "/data/stay/k"}, {"GET", "/data/stay/k"}} {
		var body []byte
		if req.method == "PUT" {
			body = []byte("x")
		}
		r := m.acme(t, req.method, req.path, body)
		if r.StatusCode != http.StatusServiceUnavailable || (req.method == "PUT" && !strings.Contains(string(r.body), "ramp")) {
			t.Fatalf("%s by hash: want 503 naming the ramp, got %d %s", req.method, r.StatusCode, r.body)
		}
	}
	if !strings.Contains(m.alerts.String(), `fnv1a-fmix64-v2`) {
		t.Errorf("no error log naming the hash: %s", m.alerts.String())
	}
	if _, ok := m.garage.object("acme-1111-data", "stay/k"); ok {
		t.Error("a refused write reached the source")
	}
	// A prefix decides without the hash, and a listing does not depend on it.
	if r := m.acme(t, "PUT", "/data/moved/k", []byte("x")); r.StatusCode != 200 {
		t.Fatalf("write decided by prefix: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "GET", "/data?list-type=2", nil); r.StatusCode != 200 {
		t.Fatalf("listing: %d %s", r.StatusCode, r.body)
	}
	// The control path refuses to extend the ramp rather than re-split it.
	if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateRamping, directory.Transition{To: directory.StateRamping, Ratio: 0.9}, "test"); err == nil || !strings.Contains(err.Error(), "does not implement") {
		t.Errorf("extending a ramp with an unknown hash: %v", err)
	}
}

// While MIGRATING, every write goes to the new primary and a read the primary does not have yet
// falls back to the source.
func TestMigratingReadFallsBackToSource(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/only-on-source", []byte("old data"))
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})

	r := m.acme(t, "GET", "/data/only-on-source", nil)
	if r.StatusCode != 200 || string(r.body) != "old data" {
		t.Fatalf("fallback read: %d %q", r.StatusCode, r.body)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_migration_fallback_reads_total", `bucket="acme/data"`); v != 1 {
		t.Errorf("fallback not counted: %v", v)
	}
	m.noLeak(t, "fallback read", r)

	// A write lands on the new primary, and reading it needs no fallback.
	m.acme(t, "PUT", "/data/fresh", []byte("new data"))
	if b, ok := m.minio.object(target, "fresh"); !ok || string(b) != "new data" {
		t.Fatalf("write did not go to the primary: %q %v", b, ok)
	}
	if r := m.acme(t, "GET", "/data/fresh", nil); string(r.body) != "new data" {
		t.Fatalf("read from the primary: %d %q", r.StatusCode, r.body)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_migration_fallback_reads_total", `bucket="acme/data"`); v != 1 {
		t.Errorf("a key on the primary should not fall back: %v", v)
	}
	// A key on neither side is still a 404, once, from the primary.
	if r := m.acme(t, "GET", "/data/nowhere", nil); r.StatusCode != 404 || !strings.Contains(string(r.body), "NoSuchKey") {
		t.Fatalf("missing key: %d %s", r.StatusCode, r.body)
	}
}

// A delete during a migration removes the object from both clusters, or the mover copies it back
// (ADR-0004).
func TestMigratingDeleteHitsBothClusters(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/everywhere", []byte("x"))
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	m.acme(t, "PUT", "/data/everywhere", []byte("newer")) // now on the primary too

	if r := m.acme(t, "DELETE", "/data/everywhere", nil); r.StatusCode != 204 {
		t.Fatalf("delete: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.minio.object(target, "everywhere"); ok {
		t.Error("still on the primary")
	}
	if _, ok := m.garage.object("acme-1111-data", "everywhere"); ok {
		t.Error("still on the source: the mover would copy it back")
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_migration_dual_delete_total", `bucket="acme/data",outcome="both"`); v != 1 {
		t.Errorf("dual delete not counted: %v", v)
	}
	// S3 deletes are idempotent, so a key the source never held still answers 204: same outcome.
	m.acme(t, "PUT", "/data/primary-only", []byte("y"))
	m.acme(t, "DELETE", "/data/primary-only", nil)
	if v := metricCounter(t, m.h.Metrics, "shunt_migration_dual_delete_total", `bucket="acme/data",outcome="both"`); v != 2 {
		t.Errorf("second dual delete not counted: %v", v)
	}
	// A source that has lost the bucket is the outcome an operator watches for.
	m.garage.dropBucket("acme-1111-data")
	m.acme(t, "PUT", "/data/after", []byte("z"))
	m.acme(t, "DELETE", "/data/after", nil)
	if v := metricCounter(t, m.h.Metrics, "shunt_migration_dual_delete_total", `bucket="acme/data",outcome="source_missing"`); v != 1 {
		t.Errorf("source_missing not counted: %v", v)
	}
}

// A listing mid-migration is a sorted merge of both clusters, with the primary winning a tie.
func TestMigratingListingMerges(t *testing.T) {
	m := newMixedRig(t, nil)
	for _, k := range []string{"a", "c", "same"} {
		m.acme(t, "PUT", "/data/"+k, []byte("source copy"))
	}
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	for _, k := range []string{"b", "d", "same"} {
		m.acme(t, "PUT", "/data/"+k, []byte("primary copy!"))
	}

	r := m.acme(t, "GET", "/data?list-type=2", nil)
	if r.StatusCode != 200 {
		t.Fatalf("merged listing: %d %s", r.StatusCode, r.body)
	}
	body := string(r.body)
	var keys []string
	for rest := body; ; {
		i := strings.Index(rest, "<Key>")
		if i < 0 {
			break
		}
		rest = rest[i+5:]
		keys = append(keys, rest[:strings.Index(rest, "</Key>")])
	}
	if strings.Join(keys, ",") != "a,b,c,d,same" {
		t.Fatalf("merged keys: %v\n%s", keys, body)
	}
	if !strings.Contains(body, "<Name>data</Name>") || strings.Count(body, "<Key>same</Key>") != 1 {
		t.Fatalf("collision not resolved once: %s", body)
	}
	// The primary's copy wins the tie: its size, not the source's.
	if !strings.Contains(body, "<Key>same</Key><LastModified>2026-09-15T18:00:00.000Z</LastModified><ETag>&#34;minio&#34;</ETag><Size>13</Size>") {
		t.Errorf("the primary should win a name held by both:\n%s", body)
	}
	m.noLeak(t, "merged listing", r)
	_ = target
}

// The merged listing pages: each page continues where the last one stopped, on both sides.
func TestMergedListingPaginates(t *testing.T) {
	m := newMixedRig(t, nil)
	for _, k := range []string{"a", "c", "e", "g"} {
		m.acme(t, "PUT", "/data/"+k, []byte("s"))
	}
	ramp(t, m, directory.Transition{To: directory.StateMigrating})
	for _, k := range []string{"b", "d", "f", "h"} {
		m.acme(t, "PUT", "/data/"+k, []byte("p"))
	}
	var got []string
	token := ""
	for page := 0; page < 10; page++ {
		target := "/data?list-type=2&max-keys=3"
		if token != "" {
			target += "&continuation-token=" + token
		}
		r := m.acme(t, "GET", target, nil)
		if r.StatusCode != 200 {
			t.Fatalf("page %d: %d %s", page, r.StatusCode, r.body)
		}
		body := string(r.body)
		for rest := body; ; {
			i := strings.Index(rest, "<Key>")
			if i < 0 {
				break
			}
			rest = rest[i+5:]
			got = append(got, rest[:strings.Index(rest, "</Key>")])
		}
		if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
			break
		}
		token = between(r.body, "<NextContinuationToken>", "</NextContinuationToken>")
		if token == "" {
			t.Fatal("truncated page with no token")
		}
	}
	if strings.Join(got, ",") != "a,b,c,d,e,f,g,h" {
		t.Fatalf("paged keys: %v", got)
	}
}

// A listing survives the target bucket not existing yet: that side is empty, not an error.
func TestMergedListingToleratesMissingSide(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/only", []byte("s"))
	if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateActive,
		directory.Transition{To: directory.StateMigrating, Target: "minio", Name: "acme-not-created-yet"}, "test"); err != nil {
		t.Fatal(err)
	}
	r := m.acme(t, "GET", "/data?list-type=2", nil)
	if r.StatusCode != 200 || !strings.Contains(string(r.body), "<Key>only</Key>") {
		t.Fatalf("listing with a missing target: %d %s", r.StatusCode, r.body)
	}
}

// After CUTOVER the source is out of the read and write path: no fallback, no merge. Deletes still
// reach it, source first, so purge-source can require that it holds nothing the primary lacks.
func TestCutoverStopsUsingTheSource(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/left-behind", []byte("on the source"))
	ramp(t, m, directory.Transition{To: directory.StateMigrating})
	ctx := context.Background()
	if err := m.dir.SetState(ctx, "acme", "data", directory.StateMigrating, directory.Transition{To: directory.StateCutover}, "test"); err != nil {
		t.Fatal(err)
	}
	before := m.upstreamCalls()
	if r := m.acme(t, "GET", "/data/left-behind", nil); r.StatusCode != 404 {
		t.Fatalf("CUTOVER must not fall back to the source: %d %s", r.StatusCode, r.body)
	}
	if got := m.upstreamCalls() - before; got != 1 {
		t.Errorf("CUTOVER read made %d upstream calls, want 1", got)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_migration_fallback_reads_total", `bucket="acme/data"`); v != 0 {
		t.Errorf("fallback after cutover: %v", v)
	}
	if r := m.acme(t, "DELETE", "/data/left-behind", nil); r.StatusCode != 204 {
		t.Fatalf("delete after cutover: %d", r.StatusCode)
	}
	if _, ok := m.garage.object("acme-1111-data", "left-behind"); ok {
		t.Error("a delete after cutover left the key on the source; purge-source's diff would never empty")
	}
}

// Backends disagree about what encoding-type=url means: Garage 2.3.0 percent-encodes "/" in a
// listing key, MinIO returns it verbatim. Merging the two spellings as different names showed one
// object twice per key in a live Garage/MinIO migration, so the merge normalises both sides.
func TestMergedListingNormalisesKeyEncodingAcrossBackends(t *testing.T) {
	m := newMixedRig(t, nil)
	for _, k := range []string{"seed/1", "seed/2", "shared/x"} {
		m.acme(t, "PUT", "/data/"+k, []byte("source copy"))
	}
	ramp(t, m, directory.Transition{To: directory.StateMigrating})
	m.minio.escapes = true // the new primary is the one that escapes
	for _, k := range []string{"seed/2", "shared/x", "new/z"} {
		m.acme(t, "PUT", "/data/"+k, []byte("primary copy!"))
	}

	for _, enc := range []string{"", "&encoding-type=url"} {
		r := m.acme(t, "GET", "/data?list-type=2"+enc, nil)
		if r.StatusCode != 200 {
			t.Fatalf("encoding-type=%q: %d %s", enc, r.StatusCode, r.body)
		}
		body := string(r.body)
		var keys []string
		for rest := body; ; {
			i := strings.Index(rest, "<Key>")
			if i < 0 {
				break
			}
			rest = rest[i+5:]
			keys = append(keys, rest[:strings.Index(rest, "</Key>")])
		}
		want := "new/z,seed/1,seed/2,shared/x"
		if enc != "" {
			want = "new%2Fz,seed%2F1,seed%2F2,shared%2Fx"
		}
		if strings.Join(keys, ",") != want {
			t.Fatalf("encoding-type=%q: merged keys %v, want %s\n%s", enc, keys, want, body)
		}
		m.noLeak(t, "merged listing", r)
	}
}

// A CopyObject whose source bucket is mid-migration used to be refused: no backend can read the
// half of the bucket that is on the other cluster. shunt streams it instead (ADR-0014).
func TestCopyFromAMigratingBucketIsStreamed(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/original", []byte("bytes"))
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})

	r := m.send(t, "PUT", "/data/copy", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/data/original"})
	if r.StatusCode != 200 {
		t.Fatalf("copy from a migrating bucket: %d %s", r.StatusCode, r.body)
	}
	// The source object is still only on the source cluster; the copy lands on the new primary.
	if b, _ := m.minio.object(target, "copy"); string(b) != "bytes" {
		t.Fatalf("the copy did not land on the primary: %q", b)
	}
	if b, _ := m.garage.object("acme-1111-data", "original"); string(b) != "bytes" {
		t.Fatalf("the source object changed: %q", b)
	}
	m.noLeak(t, "streamed copy", r)

	// Once it has cut over, the same copy works: everything is on one cluster again.
	if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateMigrating,
		directory.Transition{To: directory.StateCutover}, "test"); err != nil {
		t.Fatal(err)
	}
	m.acme(t, "PUT", "/data/original", []byte("bytes")) // on the new primary
	r = m.send(t, "PUT", "/data/copy", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/data/original"})
	if r.StatusCode != 200 {
		t.Fatalf("copy after cutover: %d %s", r.StatusCode, r.body)
	}
}

// ADR-0004 race 1, the order of the dual delete. The five steps: the mover reads k from the source;
// the client deletes k; the mover's copy lands on the primary; the mover re-checks the source; the
// delete's other leg runs. The mover's copy and re-check run between shunt's two delete legs. With
// the primary deleted first, the re-check still finds k on the source and the copy stays forever.
// With the source deleted first, the re-check finds k gone and the copy comes back out.
func TestDeleteRacingAMoverCopyStaysDeleted(t *testing.T) {
	for _, conditional := range []bool{true, false} {
		t.Run(map[bool]string{true: "conditional", false: "guarded"}[conditional], func(t *testing.T) {
			m := newMixedRig(t, nil)
			m.acme(t, "PUT", "/data/k", []byte("before")) // on the source
			target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
			m.minio.mu.Lock()
			m.minio.ignoreINM = !conditional
			m.minio.mu.Unlock()
			mv := &testMover{m: m, source: "acme-1111-data", target: target, putIfNoneMatch: conditional}

			data, ok := mv.read("k") // 1
			if !ok {
				t.Fatal("k is not on the source")
			}
			var mu sync.Mutex
			var legs, steps []string
			leg := func(side string) func(*http.Request) {
				return func(r *http.Request) {
					if r.Method != http.MethodDelete || r.Header.Get("Via") == "" {
						return // not one of shunt's delete legs
					}
					mu.Lock()
					legs = append(legs, side)
					second := len(legs) == 2
					mu.Unlock()
					if !second {
						return
					}
					err := mv.commit("k", "", data, func(op string, status int, _, detail string) { // 3, 4
						mu.Lock()
						defer mu.Unlock()
						steps = append(steps, fmt.Sprintf("%s %d %s", op, status, detail))
					})
					if err != nil {
						t.Errorf("mover: %v", err)
					}
				}
			}
			m.garage.mu.Lock()
			m.garage.before = leg("source")
			m.garage.mu.Unlock()
			m.minio.mu.Lock()
			m.minio.before = leg("primary")
			m.minio.mu.Unlock()

			if r := m.acme(t, "DELETE", "/data/k", nil); r.StatusCode != 204 { // 2, 5
				t.Fatalf("delete: %d %s", r.StatusCode, r.body)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(legs) != 2 || legs[0] != "source" {
				t.Errorf("delete legs went %v; the source must come first (ADR-0004 race 1)", legs)
			}
			if _, ok := m.minio.object(target, "k"); ok {
				t.Errorf("the mover's copy is still on the primary after the delete; mover steps: %v", steps)
			}
			if r := m.acme(t, "GET", "/data/k", nil); r.StatusCode != 404 {
				t.Errorf("GET after delete: %d %q", r.StatusCode, r.body)
			}
			if v := metricCounter(t, m.h.Metrics, "shunt_migration_dual_delete_total", `bucket="acme/data",outcome="both"`); v != 1 {
				t.Errorf("dual delete not counted as both: %v", v)
			}
		})
	}
}

// ADR-0004 race 1, the mover's withdrawal: the client deletes k, the mover's stale copy lands on the
// empty primary, and a client writes k again before the mover re-checks the source. The mover sees
// the source gone and withdraws its copy, and must not take the client's newer write with it.
func TestMoverWithdrawLeavesANewerClientWrite(t *testing.T) {
	for _, ifMatch := range []bool{true, false} {
		t.Run(map[bool]string{true: "if-match", false: "re-head"}[ifMatch], func(t *testing.T) {
			m := newMixedRig(t, nil)
			m.acme(t, "PUT", "/data/k", []byte("before"))
			target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
			m.minio.mu.Lock()
			m.minio.ignoreIfMatch = !ifMatch
			m.minio.mu.Unlock()
			mv := &testMover{m: m, source: "acme-1111-data", target: target, putIfNoneMatch: true, deleteIfMatch: ifMatch}

			data, _ := mv.read("k")
			if r := m.acme(t, "DELETE", "/data/k", nil); r.StatusCode != 204 {
				t.Fatalf("delete: %d", r.StatusCode)
			}
			mv.afterPut = func() {
				if r := m.acme(t, "PUT", "/data/k", []byte("newer")); r.StatusCode != 200 {
					t.Errorf("client rewrite: %d", r.StatusCode)
				}
			}
			var steps []string
			if err := mv.commit("k", "", data, func(op string, status int, _, detail string) {
				steps = append(steps, fmt.Sprintf("%s %d %s", op, status, detail))
			}); err != nil {
				t.Fatal(err)
			}
			if r := m.acme(t, "GET", "/data/k", nil); r.StatusCode != 200 || string(r.body) != "newer" {
				t.Errorf("GET: %d %q, want the client's newer write; mover steps: %v", r.StatusCode, r.body, steps)
			}
		})
	}
}

// The ownership test the re-HEAD path uses, on the backend's own one-second clock.
func TestOwnCopy(t *testing.T) {
	at := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name           string
		head, put      string
		modified, date time.Time
		want           bool
	}{
		{"same etag, same second", `"a"`, `"a"`, at, at, true},
		{"same etag, modified before the put's date", `"a"`, `"a"`, at.Add(-time.Second), at, true},
		{"same etag, modified after the put", `"a"`, `"a"`, at.Add(time.Second), at, false},
		{"different etag", `"b"`, `"a"`, at, at, false},
		{"no etag on the head", "", `"a"`, at, at, false},
		{"no date on the put: etag decides", `"a"`, `"a"`, at, time.Time{}, true},
	} {
		if got := ownCopy(tc.head, tc.put, tc.modified, tc.date); got != tc.want {
			t.Errorf("%s: ownCopy = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A delete whose source leg succeeds and whose primary leg fails: the client gets the primary's
// error, the object is gone from the source but still on the primary, and the metric says so.
func TestDualDeletePrimaryFailure(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/k", []byte("old"))
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	m.acme(t, "PUT", "/data/k", []byte("new")) // on the primary too
	m.minio.mu.Lock()
	m.minio.deleteStatus = http.StatusServiceUnavailable
	m.minio.mu.Unlock()

	if r := m.acme(t, "DELETE", "/data/k", nil); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("delete: %d, want the primary's 503", r.StatusCode)
	}
	if _, ok := m.garage.object("acme-1111-data", "k"); ok {
		t.Error("the source leg should have run first and removed k")
	}
	if b, ok := m.minio.object(target, "k"); !ok || string(b) != "new" {
		t.Errorf("primary: %q %v, want the object still there", b, ok)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_migration_dual_delete_total", `bucket="acme/data",outcome="primary_failed"`); v != 1 {
		t.Errorf("primary_failed not counted: %v", v)
	}
	if !strings.Contains(m.alerts.String(), "removed the object from the source but the primary refused it") {
		t.Errorf("no alert for the partial failure: %s", m.alerts.String())
	}
}

// One step of the mover's ownership check, the only arithmetic on the withdraw path.
func BenchmarkOwnCopy(b *testing.B) {
	at := time.Now()
	for b.Loop() {
		_ = ownCopy(`"a"`, `"a"`, at, at)
	}
}

// ADR-0007: a DeleteObjects body during a migration is held in memory, up to 1 MiB, so it can be
// sent to both clusters. This is the cost of the largest body S3 allows: 1,000 keys.
func BenchmarkMigratingDeleteObjects1000Keys(b *testing.B) {
	m := newMixedRig(b, nil)
	ramp(b, m, directory.Transition{To: directory.StateMigrating})
	var body strings.Builder
	body.WriteString(`<Delete><Quiet>true</Quiet>`)
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&body, "<Object><Key>dir/object-with-a-long-name-%06d</Key></Object>", i)
	}
	body.WriteString(`</Delete>`)
	payload := []byte(body.String())
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		if r := m.acme(b, "POST", "/data?delete", payload); r.StatusCode != 200 {
			b.Fatalf("DeleteObjects: %d %s", r.StatusCode, r.body)
		}
	}
}

// ADR-0007: a merged listing page holds one upstream page per side in memory and renders the
// merged page into a buffer before writing it. The cost of a full 1,000-key page from each side.
func BenchmarkMergedListingPage1000Keys(b *testing.B) {
	m := newMixedRig(b, nil)
	for i := 0; i < 1000; i++ {
		m.acme(b, "PUT", fmt.Sprintf("/data/src/%06d", i), []byte("x"))
	}
	target := ramp(b, m, directory.Transition{To: directory.StateMigrating})
	for i := 0; i < 1000; i++ {
		m.acme(b, "PUT", fmt.Sprintf("/data/dst/%06d", i), []byte("y"))
	}
	_ = target
	b.ReportAllocs()
	for b.Loop() {
		if r := m.acme(b, "GET", "/data?list-type=2&max-keys=1000", nil); r.StatusCode != 200 {
			b.Fatalf("listing: %d", r.StatusCode)
		}
	}
}

// features.debug_route_header: X-Shunt-Route names the side and cluster that served a request, only
// when the flag is on and the request asks with X-Shunt-Debug: 1; the backend never sees the ask.
func TestDebugRouteHeader(t *testing.T) {
	route := func(m *mixedRig, method, target string, debug bool) (reply, string) {
		hdr := map[string]string{}
		if debug {
			hdr["X-Shunt-Debug"] = "1"
		}
		var body []byte
		if method == "PUT" {
			body = []byte("x")
		}
		r := m.send(t, method, target, "", body, acmeAK, acmeSK, hdr)
		return r, r.Header.Get("X-Shunt-Route")
	}
	off := newMixedRig(t, nil)
	if _, got := route(off, "PUT", "/data/k", true); got != "" {
		t.Errorf("flag off: header %q", got)
	}

	rigDebugRoute = true
	m := newMixedRig(t, nil)
	rigDebugRoute = false
	if _, got := route(m, "PUT", "/data/k", false); got != "" {
		t.Errorf("flag on, no X-Shunt-Debug: header %q", got)
	}
	if _, got := route(m, "PUT", "/data/on-source", true); got != "primary garage" {
		t.Errorf("ACTIVE write: %q", got)
	}
	if _, h := m.garage.last(); h.Get("X-Shunt-Debug") != "" {
		t.Error("X-Shunt-Debug reached the backend")
	}
	ramp(t, m, directory.Transition{To: directory.StateMigrating})
	if r, got := route(m, "GET", "/data/on-source", true); r.StatusCode != 200 || got != "source garage" {
		t.Errorf("MIGRATING read falling back to the source: %d %q", r.StatusCode, got)
	}
	if _, got := route(m, "PUT", "/data/new", true); got != "primary minio" {
		t.Errorf("MIGRATING write: %q", got)
	}
	if r, got := route(m, "GET", "/data?list-type=2", true); r.StatusCode != 200 || got != "merged minio+garage" {
		t.Errorf("merged listing: %d %q", r.StatusCode, got)
	}
}
