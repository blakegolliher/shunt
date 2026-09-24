package mover

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// The withdrawal's ownership test (ADR-0004 race 1): the object is the mover's copy only when it
// carries the ETag the mover's PUT returned and was not modified after that PUT, on the backend's
// one-second clock.
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
		{"a client's newer write", `"b"`, `"a"`, at, at, false},
		{"no etag on the head", "", `"a"`, at, at, false},
		{"no date on the put: the etag decides", `"a"`, `"a"`, at, time.Time{}, true},
	} {
		if got := ownCopy(tc.head, tc.put, tc.modified, tc.date); got != tc.want {
			t.Errorf("%s: ownCopy = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestWithdrawAndGuardNames(t *testing.T) {
	if withdrawName(true) != "If-Match" || withdrawName(false) != "re-HEAD" || guardName(false) != "HEAD-then-commit" {
		t.Error("names drifted from docs/migrating.md")
	}
}

func BenchmarkOwnCopy(b *testing.B) {
	at := time.Now()
	for b.Loop() {
		_ = ownCopy(`"a"`, `"a"`, at, at)
	}
}

func TestLedgerTailIsBoundedAndOrdered(t *testing.T) {
	paths := Paths{LedgerDir: t.TempDir()}
	path := LedgerPath(paths, "acme/data")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for _, key := range []string{"a", "b", "c", "d"} {
		if err := enc.Encode(LedgerEntry{Key: key, Result: "copied"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := LedgerTail(paths, "acme/data", 2)
	if err != nil || len(entries) != 2 || entries[0].Key != "c" || entries[1].Key != "d" {
		t.Fatalf("tail: %+v, %v", entries, err)
	}
	if filepath.Base(path) != "mover-acme-data.ledger.jsonl" {
		t.Fatalf("ledger path: %s", path)
	}
	missing, err := LedgerTail(paths, "acme/missing", 20)
	if err != nil || len(missing) != 0 {
		t.Fatalf("missing ledger: %+v, %v", missing, err)
	}
}

// moverFixture is a config and directory with one placement moving from minio to garage, in state.
func moverFixture(t *testing.T, state string, ratio float64, targetConditional *bool) *directory.File {
	t.Helper()
	t.Setenv("MOVER_TEST_SECRET", "secret")
	cluster := func(region string, caps config.Capabilities) config.Cluster {
		return config.Cluster{Type: "s3", Scheme: "http", Region: region, EndpointMode: "static", Endpoints: []string{"127.0.0.1:1"},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "env:MOVER_TEST_SECRET"}, Capabilities: caps}
	}
	clusters := map[string]config.Cluster{
		"minio":  cluster("us-east-1", config.Capabilities{}),
		"garage": cluster("garage", config.Capabilities{ConditionalWrite: targetConditional}),
	}
	p := directory.Placement{State: state, Primary: "garage", Source: "minio", Names: map[string]string{"minio": "data", "garage": "acme-1111-data"}}
	if ratio > 0 {
		p.Ramp = &directory.Ramp{Hash: directory.RampHash, Ratio: ratio}
	}
	return &directory.File{Clusters: clusters, Placements: map[string]directory.Placement{"acme/data": p}}
}

// The mover refuses a target that ignores If-None-Match: *, with migrate start's own message, unless
// the operator accepts the window. It checks for itself because it also runs on a placement that is
// RAMPING at ratio 1, which migrate start never saw.
func TestMoverRefusesATargetWithoutConditionalPut(t *testing.T) {
	no, yes := false, true
	for _, state := range []struct {
		name  string
		state string
		ratio float64
	}{{"migrating", directory.StateMigrating, 0}, {"ramping at ratio 1", directory.StateRamping, 1}} {
		dir := moverFixture(t, state.state, state.ratio, &no)
		for _, sel := range [][2]string{{"acme/data", ""}, {"", "minio"}} {
			_, err := selectPlacements(dir, nil, sel[0], sel[1], false)
			if err == nil {
				t.Fatalf("%s, -bucket %q -from %q: a target without conditional PUT was accepted", state.name, sel[0], sel[1])
			}
			if want := migrate.RefuseLostWriteWindow("acme/data", "garage").Error(); err.Error() != want {
				t.Errorf("%s: refusal differs from migrate start's:\n got %s\nwant %s", state.name, err, want)
			}
			jobs, err := selectPlacements(dir, nil, sel[0], sel[1], true)
			if err != nil || len(jobs) != 1 || jobs[0].conditional {
				t.Errorf("%s: with -accept-lost-write-window: %v, %+v", state.name, err, jobs)
			}
		}
		dir = moverFixture(t, state.state, state.ratio, &yes)
		if jobs, err := selectPlacements(dir, nil, "acme/data", "", false); err != nil || len(jobs) != 1 || !jobs[0].conditional {
			t.Errorf("%s: a conditional target needs no flag: %v, %+v", state.name, err, jobs)
		}
	}
}

// Before copying, the mover checks each side's credentials in its own process, and says where a
// wrong secret comes from rather than failing on the first listing with a raw SDK error.
func TestCheckSideNamesTheSecretSource(t *testing.T) {
	code := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if code == "" {
			_, _ = w.Write([]byte(`<ListBucketResult><Name>b</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<Error><Code>` + code + `</Code><Message>no</Message></Error>`))
	}))
	defer srv.Close()
	cl := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("AK", "SK", ""), RetryMaxAttempts: 1},
		func(o *s3.Options) { o.BaseEndpoint = aws.String(srv.URL); o.UsePathStyle = true })
	sd := side{name: "vast01", bucket: "b", cl: cl, accessKey: "AK", secretRef: "env:VAST01_SECRET"}
	ctx := context.Background()
	if err := checkSide(ctx, "source", sd); err != nil {
		t.Fatalf("good credentials: %v", err)
	}
	code = "SignatureDoesNotMatch"
	if err := checkSide(ctx, "source", sd); err == nil || !strings.Contains(err.Error(), "reads VAST01_SECRET from this terminal's environment") {
		t.Fatalf("stale secret: %v", err)
	}
	code = "InvalidAccessKeyId"
	if err := checkSide(ctx, "target", sd); err == nil || !strings.Contains(err.Error(), "does not know access key AK") {
		t.Fatalf("unknown key: %v", err)
	}
	code = "AccessDenied"
	if err := checkSide(ctx, "target", sd); err != nil {
		t.Fatalf("a key that may not list is left to the copy: %v", err)
	}
}

// A move within a prefix rule (ADR-0020) lists only the rule's prefix, and copies only its own
// keys: not those of a nested rule, which the listing under the prefix also returns.
func TestScopedMoveListsOnlyItsPrefix(t *testing.T) {
	yes := true
	dir := moverFixture(t, directory.StateMigrating, 0, &yes)
	legs := map[string]directory.Leg{"minio": {Cluster: "minio", Bucket: "data"}, "garage": {Cluster: "garage", Bucket: "acme-1111-data"}}
	whole := []directory.Owner{{From: 0, To: directory.FullRange.To, Leg: "minio"}}
	dir.Placements["acme/data"] = directory.Placement{State: directory.StateMigrating, KeyHash: directory.RampHash, Legs: legs, Owners: whole,
		Prefixes: []directory.PrefixRule{{Prefix: "archive/", Owners: whole}, {Prefix: "archive/keep/", Owners: whole}},
		Move:     &directory.Move{Scope: "archive/", Range: directory.FullRange, From: "minio", To: "garage"}}
	jobs, err := selectPlacements(dir, nil, "acme/data", "", false)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs: %v %+v", err, jobs)
	}
	j := jobs[0]
	if j.prefix != "archive/" || j.src.bucket != "data" || j.dst.bucket != "acme-1111-data" {
		t.Fatalf("job: prefix %q, %s -> %s", j.prefix, j.src.bucket, j.dst.bucket)
	}
	for key, want := range map[string]bool{"archive/a": true, "archive/keep/a": false, "data/a": false} {
		if got := j.keep(key); got != want {
			t.Errorf("copies %s: %v, want %v", key, got, want)
		}
	}
}
