package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

// staticFleet is a fleet table whose members are what the test says.
type staticFleet struct{ ms []Member }

func (f staticFleet) Heartbeat(context.Context, string, Heartbeat) (time.Duration, error) {
	return time.Second, nil
}
func (f staticFleet) Members(context.Context) ([]Member, error) { return f.ms, nil }
func (f staticFleet) Forget(context.Context, string) error      { return nil }

var (
	lineageA = directory.Identity{ClusterID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Epoch: "11111111111111111111111111111111"}
	lineageB = directory.Identity{ClusterID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Epoch: "22222222222222222222222222222222"}
)

// lineageDir is a file directory at version 3 of lineage A.
func lineageDir(t *testing.T) *directory.FileDir {
	t.Helper()
	path := filepath.Join(t.TempDir(), "directory.yaml")
	body := "version: 3\nschema: 2\nidentity:\n  cluster_id: " + lineageA.ClusterID + "\n  epoch: " + lineageA.Epoch + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	d, err := directory.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// A member that outlived a restore reports a larger version from another epoch. A version-only
// fence counts it as having every version up to that one; the lineage-aware fence waits for it
// (ADR-0021, the negative regression H0 requires).
func TestFenceCountsOnlyTheCurrentLineage(t *testing.T) {
	d := lineageDir(t)
	survivor := Member{ID: "old", Live: true, Identity: lineageB, Applied: 40}
	current := Member{ID: "new", Live: true, Identity: lineageA, Applied: 3}
	s := &Server{Dir: d, Fleet: staticFleet{ms: []Member{survivor, current}}, FencePoll: time.Millisecond}
	tr := &tracker{s: s, ctx: context.Background()}
	v := d.Snapshot().Version()

	// The negative control: comparing versions alone, the survivor has v.
	if survivor.Applied < v {
		t.Fatal("the negative control is broken: the survivor must look ahead by version")
	}
	waiting, err := s.fenceRound(tr, v, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(waiting) != 1 || waiting[0] != "old" {
		t.Fatalf("waiting on %v; want only the member on another epoch", waiting)
	}
	if fs := s.fenceStatus(context.Background(), directory.Placement{}); len(fs.WaitingOn) != 1 || fs.WaitingOn[0] != "old" {
		t.Fatalf("fence status waiting on %v; want the member on another epoch", fs.WaitingOn)
	}
}

// The directory long-poll answers 304 only on the caller's own lineage; a heartbeat from another
// lineage, or in another protocol, gets no lease.
func TestDirectoryAndHeartbeatRefuseAnotherLineage(t *testing.T) {
	d := lineageDir(t)
	s := &Server{Dir: d, Fleet: staticFleet{}, FencePoll: time.Millisecond}
	h := s.Handler()
	get := func(q string) (int, Error) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/directory?"+q, nil)
		req.RemoteAddr = "127.0.0.1:1234"
		h.ServeHTTP(rec, req)
		var e Error
		_ = json.Unmarshal(rec.Body.Bytes(), &e)
		return rec.Code, e
	}
	cases := []struct {
		q    string
		code int
		err  string
	}{
		{"since=3&cluster_id=" + lineageA.ClusterID + "&epoch=" + lineageA.Epoch, http.StatusNotModified, ""},
		{"since=0", http.StatusOK, ""},
		{"since=3", http.StatusBadRequest, "bad_request"},
		{"since=40&cluster_id=" + lineageB.ClusterID + "&epoch=" + lineageB.Epoch, http.StatusConflict, CodeEpochMismatch},
		{"since=1&cluster_id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb&epoch=" + lineageA.Epoch, http.StatusConflict, CodeClusterMismatch},
		{"since=9&cluster_id=" + lineageA.ClusterID + "&epoch=" + lineageA.Epoch, http.StatusConflict, CodeResyncRequired},
		{"since=1&cluster_id=nothex&epoch=x", http.StatusBadRequest, "bad_request"},
	}
	for _, c := range cases {
		code, e := get(c.q)
		if code != c.code || e.Code != c.err {
			t.Errorf("GET /v1/directory?%s: %d %q, want %d %q", c.q, code, e.Code, c.code, c.err)
		}
		if c.code == http.StatusConflict && (e.CurrentIdentity == nil || *e.CurrentIdentity != lineageA) {
			t.Errorf("GET /v1/directory?%s: current identity %v, want lineage A", c.q, e.CurrentIdentity)
		}
	}

	beat := func(hb Heartbeat) (int, string) {
		b, _ := json.Marshal(hb)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/fleet/p1/heartbeat", strings.NewReader(string(b)))
		req.RemoteAddr = "127.0.0.1:1234"
		h.ServeHTTP(rec, req)
		var e Error
		_ = json.Unmarshal(rec.Body.Bytes(), &e)
		return rec.Code, e.Code
	}
	if code, e := beat(Heartbeat{Protocol: Protocol, Identity: lineageA, Applied: 3}); code != http.StatusOK {
		t.Errorf("a heartbeat on the current lineage: %d %s", code, e)
	}
	if code, e := beat(Heartbeat{Protocol: Protocol, Identity: lineageB, Applied: 40}); code != http.StatusConflict || e != CodeEpochMismatch {
		t.Errorf("a heartbeat from another epoch: %d %s", code, e)
	}
	if code, e := beat(Heartbeat{Protocol: 1, Identity: lineageA, Applied: 3}); code != http.StatusBadRequest || e != CodeProtocol {
		t.Errorf("a protocol-1 heartbeat: %d %s", code, e)
	}
	if err := checkLineage(d.Snapshot(), lineageB, 1); !errors.As(err, new(*lineageError)) {
		t.Errorf("checkLineage across epochs: %v", err)
	}
}
