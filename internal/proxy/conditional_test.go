package proxy

// ADR-0013: a conditional write to a bucket whose objects are split across two clusters is judged
// against both. Each test puts the current version on one side and asserts the answer S3 promises.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// putOnSource writes a key straight into the migration source, the way it was there before the
// ramp started, and returns its ETag.
func putOnSource(t *testing.T, m *mixedRig, key string, body []byte) string {
	t.Helper()
	m.garage.mu.Lock()
	defer m.garage.mu.Unlock()
	m.garage.buckets["acme-1111-data"][key] = body
	return etagOf(body)
}

func TestCreateOnceHoldsAcrossTheRamp(t *testing.T) {
	for _, state := range []string{directory.StateRamping, directory.StateMigrating} {
		t.Run(state, func(t *testing.T) {
			m := newMixedRig(t, nil)
			tr := directory.Transition{To: state}
			if state == directory.StateRamping {
				tr.Prefixes = []string{"moved/"}
			}
			target := ramp(t, m, tr)
			putOnSource(t, m, "moved/k", []byte("the copy that is still on the source"))

			// The object exists, on the other cluster: create-once must fail, and write nothing.
			r := m.send(t, "PUT", "/data/moved/k", "", []byte("second copy"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"})
			if r.StatusCode != http.StatusPreconditionFailed {
				t.Fatalf("If-None-Match: * over a key held by the source: %d %s", r.StatusCode, r.body)
			}
			if _, ok := m.minio.object(target, "moved/k"); ok {
				t.Error("the refused write reached the target anyway")
			}
			if b, _ := m.garage.object("acme-1111-data", "moved/k"); string(b) != "the copy that is still on the source" {
				t.Errorf("the source copy changed: %q", b)
			}

			// A key neither side holds is created, and the client sees the ordinary 200.
			if r := m.send(t, "PUT", "/data/moved/new", "", []byte("first"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"}); r.StatusCode != http.StatusOK {
				t.Fatalf("If-None-Match: * on a key nobody holds: %d %s", r.StatusCode, r.body)
			}
			if b, _ := m.minio.object(target, "moved/new"); string(b) != "first" {
				t.Errorf("the created object: %q", b)
			}
			// ... and a second create-once now loses to the copy on the target itself.
			if r := m.send(t, "PUT", "/data/moved/new", "", []byte("second"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"}); r.StatusCode != http.StatusPreconditionFailed {
				t.Fatalf("create-once over the target's own copy: %d %s", r.StatusCode, r.body)
			}
		})
	}
}

func TestUpdateIfCurrentHoldsAcrossTheRamp(t *testing.T) {
	m := newMixedRig(t, nil)
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	etag := putOnSource(t, m, "k", []byte("version one"))

	// The current version is on the source: the update applies, and lands on the target.
	r := m.send(t, "PUT", "/data/k", "", []byte("version two"), acmeAK, acmeSK, map[string]string{"If-Match": etag})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("If-Match against the source's current version: %d %s", r.StatusCode, r.body)
	}
	if b, _ := m.minio.object(target, "k"); string(b) != "version two" {
		t.Fatalf("the update did not land on the target: %q", b)
	}
	// shunt made the check itself, so the backend saw a create-only write instead of If-Match.
	if _, hdr := m.minio.last(); hdr.Get("If-Match") != "" || hdr.Get("If-None-Match") != "*" {
		t.Errorf("upstream headers: If-Match=%q If-None-Match=%q", hdr.Get("If-Match"), hdr.Get("If-None-Match"))
	}

	// A stale ETag is refused, wherever the current version lives.
	if r := m.send(t, "PUT", "/data/k", "", []byte("three"), acmeAK, acmeSK, map[string]string{"If-Match": `"0000"`}); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match with a stale ETag: %d %s", r.StatusCode, r.body)
	}
	// So is an update to a key neither cluster holds.
	if r := m.send(t, "PUT", "/data/nothing", "", []byte("x"), acmeAK, acmeSK, map[string]string{"If-Match": `"0000"`}); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match on a key nobody holds: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.minio.object(target, "nothing"); ok {
		t.Error("a refused update created the object")
	}
}

// A target that ignores If-None-Match: * cannot hold the create-only guard, so the converted write
// goes out plain; the client's condition was still checked, and the weakening is logged.
func TestUpdateIfCurrentOnATargetWithoutConditionalWrite(t *testing.T) {
	no := false
	m := newMixedRig(t, nil, func(cs map[string]config.Cluster) {
		cl := cs["minio"]
		cl.Capabilities.ConditionalWrite = &no
		cs["minio"] = cl
	})
	m.minio.ignoreINM = true
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	etag := putOnSource(t, m, "k", []byte("version one"))

	if r := m.send(t, "PUT", "/data/k", "", []byte("version two"), acmeAK, acmeSK, map[string]string{"If-Match": etag}); r.StatusCode != http.StatusOK {
		t.Fatalf("If-Match against a target without conditional writes: %d %s", r.StatusCode, r.body)
	}
	if b, _ := m.minio.object(target, "k"); string(b) != "version two" {
		t.Fatalf("the update did not land: %q", b)
	}
	if _, hdr := m.minio.last(); hdr.Get("If-None-Match") != "" || hdr.Get("If-Match") != "" {
		t.Errorf("a target that ignores the guard must not be sent one: %v", hdr)
	}
	if !strings.Contains(m.alerts.String(), "ignores If-None-Match") {
		t.Errorf("the weaker guard must be logged:\n%s", m.alerts.String())
	}
}

// A cluster that ignores If-None-Match: * gets the same guard when the bucket is not migrating at
// all: answering "created" for a write that overwrote is what create-once cannot survive.
func TestCreateOnceOnAnActiveBucketWithoutConditionalWrite(t *testing.T) {
	no := false
	m := newMixedRig(t, nil, func(cs map[string]config.Cluster) {
		cl := cs["garage"]
		cl.Capabilities.ConditionalWrite = &no
		cs["garage"] = cl
	})
	m.garage.ignoreINM = true

	if r := m.send(t, "PUT", "/data/k", "", []byte("first"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"}); r.StatusCode != http.StatusOK {
		t.Fatalf("first create-once: %d %s", r.StatusCode, r.body)
	}
	if r := m.send(t, "PUT", "/data/k", "", []byte("second"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"}); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("create-once over an existing object on a backend that ignores the header: %d %s", r.StatusCode, r.body)
	}
	if b, _ := m.garage.object("acme-1111-data", "k"); string(b) != "first" {
		t.Fatalf("the refused write overwrote the object: %q", b)
	}
}

// An ACTIVE bucket on a cluster that judges the header itself is left alone: no extra round trip.
func TestConditionalWriteOnAnActiveBucketIsUntouched(t *testing.T) {
	m := newMixedRig(t, nil)
	before := m.upstreamCalls()
	if r := m.send(t, "PUT", "/data/k", "", []byte("one"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"}); r.StatusCode != http.StatusOK {
		t.Fatalf("conditional write on an ACTIVE bucket: %d %s", r.StatusCode, r.body)
	}
	if n := m.upstreamCalls() - before; n != 1 {
		t.Errorf("an ACTIVE bucket must not be checked against a second cluster: %d upstream calls", n)
	}
	if _, hdr := m.garage.last(); hdr.Get("If-None-Match") != "*" {
		t.Errorf("the client's own condition must reach the backend: %v", hdr)
	}
}

// A destination that ignores If-None-Match: * cannot refuse an overwrite itself. Mid-migration it
// is shunt that sent the key to that side, so shunt makes the check instead (HEAD-then-commit).
func TestCreateOnceOnATargetWithoutConditionalWrite(t *testing.T) {
	no := false
	m := newMixedRig(t, nil, func(cs map[string]config.Cluster) {
		cl := cs["minio"]
		cl.Capabilities.ConditionalWrite = &no
		cs["minio"] = cl
	})
	m.minio.ignoreINM = true
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})

	if r := m.send(t, "PUT", "/data/k", "", []byte("first"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"}); r.StatusCode != http.StatusOK {
		t.Fatalf("first create-once: %d %s", r.StatusCode, r.body)
	}
	r := m.send(t, "PUT", "/data/k", "", []byte("second"), acmeAK, acmeSK, map[string]string{"If-None-Match": "*"})
	if r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("create-once over the target's own copy, on a target that ignores the header: %d %s", r.StatusCode, r.body)
	}
	if b, _ := m.minio.object(target, "k"); string(b) != "first" {
		t.Fatalf("the refused write overwrote the object: %q", b)
	}
}
