package proxy

// POC-4: the routing table of docs/DESIGN.md §2.5 driven through the handler, against two fake
// clusters. Each test puts a bucket into a migration state and checks where requests actually go.

import (
	"context"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/directory"
)

// ramp moves acme/data from garage to minio and returns the target's backend bucket name.
func ramp(t *testing.T, m *mixedRig, tr directory.Transition) string {
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

// After CUTOVER the source is out of the path: no fallback, no dual delete, no merge.
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

// A CopyObject whose source bucket is mid-migration is refused: the backend doing the copy can read
// only its own cluster, and half the objects may still be on the other one.
func TestCopyFromAMigratingBucketIsRefused(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/original", []byte("bytes"))
	ramp(t, m, directory.Transition{To: directory.StateMigrating})

	r := m.send(t, "PUT", "/data/copy", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/data/original"})
	if r.StatusCode != 501 || !strings.Contains(string(r.body), "being migrated") {
		t.Fatalf("copy from a migrating bucket: %d %s", r.StatusCode, r.body)
	}
	m.noLeak(t, "refused copy", r)

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
