package proxy

// Defect 4 of the H2 review, end to end: purge-source run by the control plane against this
// proxy's own gates (the lab path), with client DELETEs arriving while the source barrier drains.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// controlCall sends one control API request with a fresh Idempotency-Key and decodes the answer.
func controlCall(t *testing.T, url, method, path string, body, out any) int {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodGet {
		req.Header.Set(control.HeaderIdempotencyKey, fmt.Sprintf("purge-delete-%d", time.Now().UnixNano()))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw bytes.Buffer
	_, _ = raw.ReadFrom(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw.Bytes(), out); err != nil {
			t.Fatalf("%s %s: decoding %s: %v", method, path, raw.String(), err)
		}
	}
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusAccepted {
		t.Logf("%s %s: HTTP %d %s", method, path, resp.StatusCode, raw.String())
	}
	return resp.StatusCode
}

// DELETEs during a purge drain pause (the DELETE rule), so the source stays a subset of the
// primary and purge-source completes once the old work has drained. With the old rule (the
// delete's source leg skipped, the primary leg sent) the key leaves the primary alone, the re-diff
// finds it on the source only, and the purge can neither finish nor proceed.
func TestDeletesDuringPurgeDrainLetPurgeComplete(t *testing.T) {
	m := newMixedRig(t, nil)
	for _, k := range []string{"k1", "k2"} {
		if r := m.acme(t, "PUT", "/data/"+k, []byte("value of "+k)); r.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s: %d %s", k, r.StatusCode, r.body)
		}
	}
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	for _, k := range []string{"k1", "k2"} {
		m.minio.put(target, k, []byte("value of "+k))
	}
	evidence := &directory.CutoverEvidence{At: time.Now().UTC().Truncate(time.Second), Window: time.Second}
	if err := m.dir.SetState(context.Background(), "acme", "data", directory.StateMigrating, directory.Transition{To: directory.StateCutover, Cutover: evidence}, "test"); err != nil {
		t.Fatal(err)
	}

	wake := make(chan struct{}, 64)
	ctl := &control.Server{Dir: m.dir, Clusters: m.set, LocalGates: m.gates, FencePoll: 5 * time.Millisecond,
		Sleep: func(ctx context.Context, _ time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wake:
				return nil
			}
		}}
	api := httptest.NewServer(ctl.Handler())
	t.Cleanup(api.Close)

	var dry control.PurgeDryRun
	if code := controlCall(t, api.URL, http.MethodPost, "/v1/placements/acme/data/purge-source", control.PurgeRequest{DryRun: true}, &dry); code != http.StatusOK || !dry.Allowed {
		t.Fatalf("dry run: %d %+v", code, dry)
	}

	// A dual delete admitted before the hold, held at its source leg: old source-dependent work.
	arrived, release := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	m.garage.mu.Lock()
	m.garage.before = func(r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/k1") {
			arrived <- struct{}{}
			<-release
		}
	}
	m.garage.mu.Unlock()
	var wg sync.WaitGroup
	var old reply
	wg.Add(1)
	go func() { defer wg.Done(); old = m.acme(t, "DELETE", "/data/k1", nil) }()
	<-arrived

	var blocked control.Operation
	if code := controlCall(t, api.URL, http.MethodPost, "/v1/placements/acme/data/purge-source", control.PurgeRequest{Token: dry.Token, Wait: "0s"}, &blocked); code != http.StatusAccepted {
		t.Fatalf("purge: want 202 while the old delete is out, got %d", code)
	}
	for deadline := time.Now().Add(10 * time.Second); blocked.Status != control.StatusBlocked || blocked.Barrier == nil || blocked.Barrier.HoldVersion == 0; {
		if time.Now().After(deadline) {
			t.Fatalf("purge never blocked on the old delete: %+v", blocked)
		}
		time.Sleep(2 * time.Millisecond)
		controlCall(t, api.URL, http.MethodGet, "/v1/operations/"+blocked.ID, nil, &blocked)
	}
	if blocked.Barrier.DispatchStarted || len(blocked.Blockers) != 1 || blocked.Blockers[0].Code != control.BlockerOldRequests {
		t.Fatalf("purge while draining: %+v", blocked)
	}

	// A client delete during the drain pauses rather than reach the primary alone.
	if r := m.acme(t, "DELETE", "/data/k2", nil); r.StatusCode != http.StatusServiceUnavailable || r.Header.Get("Retry-After") == "" {
		t.Fatalf("a delete during the purge drain: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.minio.object(target, "k2"); !ok {
		t.Fatal("the delete during the purge drain removed k2 from the primary alone")
	}

	releaseOnce.Do(func() { close(release) })
	wg.Wait()
	if old.StatusCode != http.StatusNoContent {
		t.Fatalf("the old dual delete: %d %s", old.StatusCode, old.body)
	}
	m.garage.mu.Lock()
	m.garage.before = nil
	m.garage.mu.Unlock()

	deadline := time.Now().Add(10 * time.Second)
	var done control.Operation
	for {
		select {
		case wake <- struct{}{}:
		default:
		}
		controlCall(t, api.URL, http.MethodGet, "/v1/operations/"+blocked.ID, nil, &done)
		if done.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("purge never completed after the drain: %+v", done)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if done.Status != control.StatusSucceeded || done.Barrier == nil || !done.Barrier.Committed {
		t.Fatalf("purge after deletes during its drain: %+v", done)
	}
	if _, ok := m.garage.object("acme-1111-data", "k2"); ok {
		t.Fatal("the purge left the source's keys")
	}
	if _, ok := m.minio.object(target, "k2"); !ok {
		t.Fatal("k2 is gone from the primary, which the paused delete never reached")
	}
	// Once the purge is done the placement is ACTIVE on the primary and the delete goes through.
	if r := m.acme(t, "DELETE", "/data/k2", nil); r.StatusCode != http.StatusNoContent {
		t.Fatalf("the retried delete after the purge: %d %s", r.StatusCode, r.body)
	}
}
