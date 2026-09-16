package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// fakeCluster is an in-process S3 backend; versioned names buckets that report versioning Enabled.
type fakeCluster struct {
	srv       *httptest.Server
	be        *s3mem.Backend
	versioned map[string]bool
}

func newFakeCluster(t *testing.T) *fakeCluster {
	t.Helper()
	fc := &fakeCluster{be: s3mem.New(), versioned: map[string]bool{}}
	fake := gofakes3.New(fc.be, gofakes3.WithTimeSkewLimit(0)).Server()
	fc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if _, ok := r.URL.Query()["versioning"]; ok && r.Method == http.MethodGet {
			if fc.versioned[bucket] {
				_, _ = w.Write([]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
			} else {
				_, _ = w.Write([]byte(`<VersioningConfiguration/>`))
			}
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(fc.srv.Close)
	return fc
}

func (fc *fakeCluster) put(t *testing.T, bucket, key, body string) {
	t.Helper()
	if _, err := fc.be.PutObject(bucket, key, map[string]string{}, strings.NewReader(body), int64(len(body)), nil); err != nil {
		t.Fatal(err)
	}
}

func (fc *fakeCluster) has(bucket, key string) bool {
	_, err := fc.be.HeadObject(bucket, key)
	return err == nil
}

func (fc *fakeCluster) definition(conditionalWrite bool) config.Cluster {
	cw := conditionalWrite
	return config.Cluster{Type: "vast", Scheme: "http", Region: "us-east-1", Endpoints: []string{strings.TrimPrefix(fc.srv.URL, "http://")},
		Credentials: config.Credentials{AccessKey: "AK", SecretRef: "file:/etc/shunt/cluster.secret"}, Capabilities: config.Capabilities{ConditionalWrite: &cw}}
}

type rig struct {
	t      *testing.T
	api    *httptest.Server
	ctl    *Server
	dir    *directory.FileDir
	vast01 *fakeCluster
	vast02 *fakeCluster
	slept  []time.Duration
}

func newRig(t *testing.T) *rig {
	t.Helper()
	path := filepath.Join(t.TempDir(), "directory.yaml")
	if err := os.WriteFile(path, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir, err := directory.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, func(ref string) (string, error) {
		if strings.Contains(ref, "missing") {
			return "", fmt.Errorf("%s: no such file", ref)
		}
		return "secret", nil
	})
	t.Cleanup(reg.Close)
	dir.Prepare = func(f *directory.File) error {
		_, _, err := reg.Apply(f.Clusters)
		return err
	}
	rg := &rig{t: t, dir: dir, vast01: newFakeCluster(t), vast02: newFakeCluster(t)}
	rg.ctl = &Server{Dir: dir, Clusters: reg, Metrics: telemetry.NewMetrics(),
		Now: func() time.Time { return time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC) },
		Sleep: func(_ context.Context, d time.Duration) error {
			rg.slept = append(rg.slept, d)
			return nil
		}}
	rg.api = httptest.NewServer(rg.ctl.Handler())
	t.Cleanup(rg.api.Close)
	return rg
}

// call sends one API request and decodes the answer into out (if not nil); it returns the status.
func (rg *rig) call(method, path string, body any, out any) (int, string) {
	rg.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			rg.t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, rg.api.URL+path, &buf)
	if err != nil {
		rg.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		rg.t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw bytes.Buffer
	_, _ = raw.ReadFrom(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw.Bytes(), out); err != nil {
			rg.t.Fatalf("%s %s: decoding %s: %v", method, path, raw.String(), err)
		}
	}
	return resp.StatusCode, raw.String()
}

func (rg *rig) must(method, path string, body any, out any) {
	rg.t.Helper()
	if code, raw := rg.call(method, path, body, out); code != http.StatusOK {
		rg.t.Fatalf("%s %s: HTTP %d %s", method, path, code, raw)
	}
}

// answers checks a call fails with this status and error code.
func (rg *rig) answers(method, path string, body any, status int, want string) {
	rg.t.Helper()
	code, raw := rg.call(method, path, body, nil)
	var e Error
	if err := json.Unmarshal([]byte(raw), &e); err != nil || code != status || e.Code != want {
		rg.t.Fatalf("%s %s: want HTTP %d %s, got HTTP %d %s", method, path, status, want, code, raw)
	}
}

func (rg *rig) refused(method, path string, body any, want string) {
	rg.t.Helper()
	code, raw := rg.call(method, path, body, nil)
	var e Error
	_ = json.Unmarshal([]byte(raw), &e)
	refusal := code == http.StatusConflict && e.Code == "refused" || code == http.StatusBadRequest && e.Code == "invalid"
	if !refusal || !strings.Contains(e.Message, want) {
		rg.t.Fatalf("%s %s: want a refusal (409 refused, or 400 invalid) mentioning %q, got HTTP %d %s", method, path, want, code, raw)
	}
}

// The POC-5 walkthrough through the API alone, against two in-process clusters: adopt, expand,
// ramp, migrate, cutover, purge the source, and remove its cluster.
func TestWalkthroughThroughTheAPI(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	rg.vast01.put(t, "data01", "a", "one")
	rg.vast01.put(t, "data01", "dir/b", "two")

	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.refused("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01", Name: "nope"}, "does not exist on vast01")
	var adopted PlacementStatus
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, &adopted)
	if adopted.State != directory.StateActive || adopted.Names["vast01"] != "data01" {
		t.Fatalf("adopt: %+v", adopted)
	}
	rg.answers("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, http.StatusConflict, "conflict")

	// vast02 arrives live.
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.refused("POST", "/v1/placements/acme/data01/expand", ExpandRequest{To: "vast02"}, "does not exist on vast02")
	var ex ExpandResult
	rg.must("POST", "/v1/placements/acme/data01/expand", ExpandRequest{To: "vast02", Create: true}, &ex)
	if ex.Name != "data01-001" || !ex.CreatedBucket || !strings.HasPrefix(ex.Canary, ".shunt-canary-") || !ex.ConditionalWrite {
		t.Fatalf("expand: %+v", ex)
	}
	if rg.vast02.has("data01-001", ex.Canary) {
		t.Error("the canary object was left behind")
	}

	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, &tr)
	if tr.From != directory.StateActive || tr.To != directory.StateRamping || tr.Primary != "vast02" || tr.Source != "vast01" || tr.Ratio != 0.5 {
		t.Fatalf("ramp 0.5: %+v", tr)
	}
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.2}, "only grows")
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 1}, &tr)

	// Writes per side come from the proxy's own counters.
	rg.ctl.Metrics.RampWrites.WithLabelValues("acme/data01", "primary").Add(51)
	rg.ctl.Metrics.RampWrites.WithLabelValues("acme/data01", "source").Add(49)
	var st Status
	rg.must("GET", "/v1/status", nil, &st)
	if len(st.Placements) != 1 || st.Placements[0].Writes["primary"] != 51 || st.Placements[0].Writes["source"] != 49 || len(st.Clusters) != 2 {
		t.Fatalf("status: %+v", st)
	}

	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, &tr)
	if tr.To != directory.StateMigrating {
		t.Fatalf("migrate: %+v", tr)
	}
	rg.refused("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{}, "no mover has reported")
	rg.refused("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast02", Primary: "vast01"}, "moves vast01 → vast02")
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 1, Copied: 2, Done: true, Converged: true}, nil)
	rg.refused("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{}, "has not converged")

	// The mover's second pass copies nothing: converged. The cutover window sees a fallback read.
	rg.vast02.put(t, "data01-001", "a", "one")
	var pr Progress
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 2, Skipped: 1, Done: true, Converged: true}, &pr)
	if !pr.Converged {
		t.Fatalf("a completed pass copying nothing is converged: %+v", pr)
	}
	rg.ctl.Sleep = func(context.Context, time.Duration) error {
		rg.ctl.Metrics.FallbackReads.WithLabelValues("acme/data01").Inc()
		return nil
	}
	rg.refused("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "5s"}, "still fall back")
	rg.ctl.Sleep = func(_ context.Context, d time.Duration) error { rg.slept = append(rg.slept, d); return nil }
	rg.must("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "5s"}, &tr)
	if tr.To != directory.StateCutover || tr.Cutover == nil || tr.Cutover.Window != 5*time.Second || tr.Cutover.FallbackReads != 1 {
		t.Fatalf("cutover: %+v", tr)
	}

	// dir/b never reached vast02: the diff refuses the purge and names it.
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", nil, "dir/b")
	rg.vast02.put(t, "data01-001", "dir/b", "two")
	var pg PurgeResult
	rg.must("POST", "/v1/placements/acme/data01/purge-source", nil, &pg)
	if pg.ObjectsDeleted != 2 || pg.Bucket != "data01" || pg.Source != "vast01" {
		t.Fatalf("purge: %+v", pg)
	}
	if ok, _ := rg.vast01.be.BucketExists("data01"); ok {
		t.Error("the source bucket still exists")
	}
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if p.State != directory.StateActive || p.Primary != "vast02" || p.Source != "" || len(p.Names) != 1 {
		t.Fatalf("placement after purge: %+v", p)
	}

	// vast01 is still the tenant's default: refused until repointed, then removed.
	rg.refused("DELETE", "/v1/clusters/vast01", nil, "tenants.acme.default_cluster")
	rg.must("POST", "/v1/tenants/acme/default-cluster", TenantDefaultRequest{Cluster: "vast02"}, nil)
	rg.must("DELETE", "/v1/clusters/vast01", nil, nil)
	if _, ok := rg.ctl.Clusters.Load().Get("vast01"); ok {
		t.Error("vast01 is still live in the proxy")
	}
	if len(rg.slept) != 1 || rg.slept[0] != 5*time.Second {
		t.Errorf("cutover windows waited: %v", rg.slept)
	}
}

func TestClusterAddRefusals(t *testing.T) {
	rg := newRig(t)
	code, raw := rg.call("POST", "/v1/clusters", map[string]any{"name": "vast01", "cluster": map[string]any{
		"type": "vast", "scheme": "http", "region": "r", "endpoints": []string{"127.0.0.1:1"},
		"credentials": map[string]any{"access_key": "AK", "secret": "inline"}}}, nil)
	if code != http.StatusBadRequest || !strings.Contains(raw, "secret") {
		t.Fatalf("an inline secret was accepted: %d %s", code, raw)
	}
	bad := rg.vast01.definition(true)
	bad.Scheme = "ftp"
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: bad}, "clusters.vast01.scheme")
	unresolvable := rg.vast01.definition(true)
	unresolvable.Credentials.SecretRef = "file:/missing"
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: unresolvable}, "cannot use this cluster")
	if _, ok := rg.dir.Snapshot().Cluster("vast01"); ok {
		t.Fatal("a refused cluster reached the directory")
	}
	rg.answers("DELETE", "/v1/clusters/nope", nil, http.StatusNotFound, "not_found")
}

func TestMigrateRefusals(t *testing.T) {
	rg := newRig(t)
	for _, name := range []string{"data01", "versioned"} {
		if err := rg.vast01.be.CreateBucket(name); err != nil {
			t.Fatal(err)
		}
	}
	rg.vast01.versioned["versioned"] = true
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "garage", Cluster: rg.vast02.definition(false)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.must("POST", "/v1/placements/acme/versioned/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.refused("POST", "/v1/placements/acme/versioned/expand", ExpandRequest{To: "garage", Create: true}, "versioning Enabled")
	rg.must("POST", "/v1/placements/acme/data01/expand", ExpandRequest{To: "garage", Create: true}, nil)
	rg.refused("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, "--accept-lost-write-window")
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{AcceptLostWriteWindow: true}, &tr)
	if !strings.Contains(tr.Warning, "HEAD-then-commit") {
		t.Errorf("an accepted window is not warned about: %+v", tr)
	}
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", nil, "purge-source runs on a placement in CUTOVER")
	rg.answers("POST", "/v1/placements/acme/nope/ramp", RampRequest{Ratio: 0.5}, http.StatusNotFound, "not_found")
	rg.answers("POST", "/v1/placements/acme/data01/ramp", RampRequest{}, http.StatusBadRequest, "bad_request")
}

func TestAuthorization(t *testing.T) {
	ctl := &Server{Token: "t0ken"}
	h := ctl.Handler()
	for _, tc := range []struct {
		name, auth string
		want       int
	}{{"no token", "", 401}, {"wrong token", "Bearer nope", 401}, {"right token", "Bearer t0ken", 404}} {
		req := httptest.NewRequest("GET", "/v1/nothing-here", nil)
		req.RemoteAddr = "10.1.2.3:4444"
		if tc.auth != "" {
			req.Header.Set("Authorization", tc.auth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, rec.Code, tc.want)
		}
	}
	open := (&Server{}).Handler()
	for addr, want := range map[string]int{"127.0.0.1:5555": 404, "[::1]:5555": 404, "10.1.2.3:5555": 401} {
		req := httptest.NewRequest("GET", "/v1/nothing-here", nil)
		req.RemoteAddr = addr
		rec := httptest.NewRecorder()
		open.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("no token, peer %s: %d, want %d", addr, rec.Code, want)
		}
	}
}

func TestNextName(t *testing.T) {
	f := &directory.File{Placements: map[string]directory.Placement{
		"a/x": {Names: map[string]string{"vast02": "data01-001"}},
		"a/y": {Names: map[string]string{"vast03": "data01-002"}},
	}}
	if got := nextName(f, "vast02", "data01"); got != "data01-002" {
		t.Errorf("nextName: %s", got)
	}
	long := strings.Repeat("b", 63)
	if got := nextName(f, "vast02", long); len(got) > 63 || !strings.HasSuffix(got, "-001") {
		t.Errorf("long base: %s", got)
	}
}

func BenchmarkStatus(b *testing.B) {
	path := filepath.Join(b.TempDir(), "directory.yaml")
	body := "version: 1\nclusters:\n  c: { type: s3, scheme: http, region: r, endpoints: [\"127.0.0.1:1\"], credentials: { access_key: A, secret_ref: env:S } }\ntenants: { t: { default_cluster: c } }\nplacements:\n"
	for i := 0; i < 200; i++ {
		body += fmt.Sprintf("  t/b%03d: { state: ACTIVE, primary: c, names: { c: b%03d } }\n", i, i)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		b.Fatal(err)
	}
	dir, err := directory.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	h := (&Server{Dir: dir, Clusters: upstream.NewRegistry(upstream.Options{}, nil), Metrics: telemetry.NewMetrics()}).Handler()
	req := httptest.NewRequest("GET", "/v1/status?bucket=t/b007", nil)
	req.RemoteAddr = "127.0.0.1:1"
	b.ReportAllocs()
	for b.Loop() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}
