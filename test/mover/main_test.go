package main

import (
	"testing"
	"time"
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
