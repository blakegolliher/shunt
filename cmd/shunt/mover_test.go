package main

import (
	"testing"
	"time"

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
			_, err := selectPlacements(dir, sel[0], sel[1], false)
			if err == nil {
				t.Fatalf("%s, -bucket %q -from %q: a target without conditional PUT was accepted", state.name, sel[0], sel[1])
			}
			if want := migrate.RefuseLostWriteWindow("acme/data", "garage").Error(); err.Error() != want {
				t.Errorf("%s: refusal differs from migrate start's:\n got %s\nwant %s", state.name, err, want)
			}
			jobs, err := selectPlacements(dir, sel[0], sel[1], true)
			if err != nil || len(jobs) != 1 || jobs[0].conditional {
				t.Errorf("%s: with -accept-lost-write-window: %v, %+v", state.name, err, jobs)
			}
		}
		dir = moverFixture(t, state.state, state.ratio, &yes)
		if jobs, err := selectPlacements(dir, "acme/data", "", false); err != nil || len(jobs) != 1 || !jobs[0].conditional {
			t.Errorf("%s: a conditional target needs no flag: %v, %+v", state.name, err, jobs)
		}
	}
}
