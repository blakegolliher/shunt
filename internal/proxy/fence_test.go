package proxy

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/blakegolliher/shunt/internal/directory"
)

// A key inside a held ramp step is written nowhere: 503 with Retry-After, and no upstream call, so
// no proxy puts it on the target while another may still put it on the source (ADR-0016). Reads
// of it try the target first; keys outside the hold are untouched.
func TestHeldKeyWritesAreRefused(t *testing.T) {
	m := newMixedRig(t, nil)
	if r := m.acme(t, "PUT", "/data/moved/k", []byte("before the move")); r.StatusCode != http.StatusOK {
		t.Fatalf("seed on the source: %d %s", r.StatusCode, r.body)
	}
	ramp(t, m, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}, Hold: true})

	before := m.upstreamCalls()
	r := m.acme(t, "PUT", "/data/moved/k", []byte("during the hold"))
	if r.StatusCode != http.StatusServiceUnavailable || r.Header.Get("Retry-After") != "1" {
		t.Fatalf("held write: %d Retry-After %q %s", r.StatusCode, r.Header.Get("Retry-After"), r.body)
	}
	if n := m.upstreamCalls() - before; n != 0 {
		t.Errorf("a held write reached a backend: %d upstream calls", n)
	}
	if got := counterValue(t, m.h.Metrics.RefusedWrites.WithLabelValues("acme/data", "hold")); got != 1 {
		t.Errorf("refused writes{reason=hold} = %v, want 1", got)
	}
	if r := m.acme(t, "GET", "/data/moved/k", nil); r.StatusCode != http.StatusOK || string(r.body) != "before the move" {
		t.Errorf("held key read (target, then source): %d %q", r.StatusCode, r.body)
	}
	if r := m.acme(t, "PUT", "/data/stay/k", []byte("x")); r.StatusCode != http.StatusOK {
		t.Errorf("a key outside the hold was refused: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "DELETE", "/data/moved/k", nil); r.StatusCode != http.StatusNoContent {
		t.Errorf("a held key's delete goes to both clusters and is not held: %d %s", r.StatusCode, r.body)
	}
}

// A stale proxy refuses writes and deletes on a moving bucket and serves everything else.
func TestStaleProxyRefusesWritesOnMovingBuckets(t *testing.T) {
	m := newMixedRig(t, nil)
	ramp(t, m, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}})
	if r := m.acme(t, "PUT", "/data/stay/k", []byte("x")); r.StatusCode != http.StatusOK {
		t.Fatalf("seed: %d %s", r.StatusCode, r.body)
	}
	stale := true
	m.h.Stale = func() bool { return stale }

	for _, c := range []struct{ method, target string }{{"PUT", "/data/stay/k"}, {"PUT", "/data/moved/k"}, {"DELETE", "/data/stay/k"}} {
		r := m.acme(t, c.method, c.target, []byte("y"))
		if r.StatusCode != http.StatusServiceUnavailable || r.Header.Get("Retry-After") != "1" {
			t.Errorf("stale %s %s: %d Retry-After %q", c.method, c.target, r.StatusCode, r.Header.Get("Retry-After"))
		}
	}
	if got := counterValue(t, m.h.Metrics.RefusedWrites.WithLabelValues("acme/data", "stale")); got != 3 {
		t.Errorf("refused writes{reason=stale} = %v, want 3", got)
	}
	if r := m.acme(t, "GET", "/data/stay/k", nil); r.StatusCode != http.StatusOK || string(r.body) != "x" {
		t.Errorf("stale proxy read on a moving bucket: %d %q", r.StatusCode, r.body)
	}
	if r := m.acme(t, "PUT", "/pics/k", []byte("z")); r.StatusCode != http.StatusOK {
		t.Errorf("stale proxy write on an ACTIVE bucket: %d %s", r.StatusCode, r.body)
	}
	// A key this proxy routes to the source only may have moved in a step it missed, and been
	// written to the target by another proxy: a stale proxy reads the target first.
	m.garage.putObject("acme-1111-data", "stay/k3", []byte("older, on the source"))
	m.minio.putObject("acme-9000-data", "stay/k3", []byte("newer, on the target"))
	stale = false
	if r := m.acme(t, "GET", "/data/stay/k3", nil); string(r.body) != "older, on the source" {
		t.Fatalf("a live proxy routes a key outside its ramp to the source only: %d %q", r.StatusCode, r.body)
	}
	stale = true
	if r := m.acme(t, "GET", "/data/stay/k3", nil); r.StatusCode != http.StatusOK || string(r.body) != "newer, on the target" {
		t.Errorf("a stale proxy must read the target first: %d %q", r.StatusCode, r.body)
	}
	if r := m.acme(t, "GET", "/data/stay/k", nil); r.StatusCode != http.StatusOK || string(r.body) != "x" {
		t.Errorf("a stale proxy falls back to the source: %d %q", r.StatusCode, r.body)
	}
	stale = false
	if r := m.acme(t, "PUT", "/data/stay/k", []byte("y")); r.StatusCode != http.StatusOK {
		t.Errorf("write after the lease is back: %d %s", r.StatusCode, r.body)
	}
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// putObject stores an object straight on a fake cluster, as another proxy's write would.
func (f *fakeS3) putObject(bucket, key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buckets[bucket][key] = body
}
