package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// fakeCluster is an in-process S3 backend; versioned names buckets that report versioning Enabled.
type fakeCluster struct {
	srv       *httptest.Server
	be        *s3mem.Backend
	versioned map[string]bool
	reject    string             // when set, every request answers 403 with this error code
	server    string             // the Server response header, when set
	condPut   bool               // honor If-None-Match: * on PUT over an existing object
	condDel   bool               // honor a mismatched If-Match on DELETE
	onList    func(token string) // called before a ListObjectsV2 page is served, with its continuation token
	keys      map[string]bool    // when set, an access key not in it answers 403 InvalidAccessKeyId
	noCreate  bool               // CreateBucket answers 403 AccessDenied, as VAST does for a key without the permission
	owned     bool               // CreateBucket of an existing bucket answers 409 BucketAlreadyOwnedByYou, as MinIO does
	listMu    sync.Mutex
	listed    []string // the prefix of every ListObjectsV2 and ListMultipartUploads request, in order
}

func newFakeCluster(t *testing.T) *fakeCluster {
	t.Helper()
	fc := &fakeCluster{be: s3mem.New(), versioned: map[string]bool{}}
	fake := gofakes3.New(fc.be, gofakes3.WithTimeSkewLimit(0)).Server()
	fc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fc.server != "" && r.Method == http.MethodGet && r.URL.Path == "/" && fc.reject == "" {
			w.Header().Set("Server", fc.server) // gofakes3 would answer as AmazonS3
			_, _ = w.Write([]byte(`<ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>`))
			return
		}
		if bucket, key, ok := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/"); ok && key != "" {
			_, err := fc.be.HeadObject(bucket, key)
			switch {
			case fc.condPut && r.Method == http.MethodPut && r.Header.Get("If-None-Match") == "*" && err == nil,
				fc.condDel && r.Method == http.MethodDelete && r.Header.Get("If-Match") != "" && err == nil:
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			if !fc.condPut {
				r.Header.Del("If-None-Match") // a backend that ignores it
			}
			if !fc.condDel {
				r.Header.Del("If-Match")
			}
		}
		if q := r.URL.Query(); r.Method == http.MethodGet && (q.Get("list-type") == "2" || q.Has("uploads")) {
			fc.listMu.Lock()
			fc.listed = append(fc.listed, q.Get("prefix"))
			fc.listMu.Unlock()
		}
		if q := r.URL.Query(); fc.onList != nil && r.Method == http.MethodGet && q.Get("list-type") == "2" {
			fc.onList(q.Get("continuation-token"))
		}
		if fc.keys != nil {
			_, cred, _ := strings.Cut(r.Header.Get("Authorization"), "Credential=")
			if ak, _, _ := strings.Cut(cred, "/"); !fc.keys[ak] {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`<Error><Code>InvalidAccessKeyId</Code><Message>unknown key</Message></Error>`))
				return
			}
		}
		if fc.noCreate && r.Method == http.MethodPut && !strings.Contains(strings.Trim(r.URL.Path, "/"), "/") && r.URL.RawQuery == "" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
			return
		}
		if fc.owned && r.Method == http.MethodPut && !strings.Contains(strings.Trim(r.URL.Path, "/"), "/") && r.URL.RawQuery == "" {
			if ok, _ := fc.be.BucketExists(strings.Trim(r.URL.Path, "/")); ok {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`<Error><Code>BucketAlreadyOwnedByYou</Code><Message>yours</Message></Error>`))
				return
			}
		}
		if code := fc.reject; code != "" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<Error><Code>` + code + `</Code><Message>no</Message><Region>garage</Region></Error>`))
			return
		}
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
	events *Events
	slept  []time.Duration
	log    *syncLog
}

// syncLog is the control server's log, safe to read while handlers write.
type syncLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
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
	dir.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, err := reg.Prepare(f.Clusters, resolve)
		if err != nil {
			return nil, err
		}
		return func() { cand.Commit() }, nil
	}
	rg := &rig{t: t, dir: dir, vast01: newFakeCluster(t), vast02: newFakeCluster(t), events: NewEvents(16), log: &syncLog{}}
	// The event stream is wired as the lab proxy wires it: the directory's install hook, seeded
	// with the current snapshot, and the operation store's change hook.
	dir.OnInstall = rg.events.Directory
	rg.events.Directory(dir.Snapshot())
	rg.ctl = &Server{Dir: dir, Clusters: reg, Metrics: telemetry.NewMetrics(), SecretsDir: filepath.Join(t.TempDir(), "secrets"), Log: slog.New(telemetry.NewConsoleHandler(rg.log, telemetry.ConsoleOptions{})),
		Now: func() time.Time { return time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC) },
		Sleep: func(_ context.Context, d time.Duration) error {
			rg.slept = append(rg.slept, d)
			return nil
		},
		Events: rg.events, Ops: &MemOperations{OnChange: rg.events.Fence}}
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

	// dir/b never reached vast02: the dry run says the purge would be refused and names it, and
	// issues no token; the real call needs one (ADR-0017).
	var dry PurgeDryRun
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if dry.Allowed || !strings.Contains(dry.Reason, "dir/b") || dry.Token != "" || dry.Objects != 2 || len(dry.Missing) != 1 {
		t.Fatalf("dry run with a missing key: %+v", dry)
	}
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", nil, "needs the confirmation token")
	rg.vast02.put(t, "data01-001", "dir/b", "two")
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Token == "" || dry.Objects != 2 || dry.Bytes != 6 || dry.Bucket != "data01" || dry.Source != "vast01" || len(dry.Missing) != 0 {
		t.Fatalf("dry run: %+v", dry)
	}
	var pg PurgeResult
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, &pg)
	if pg.ObjectsDeleted != 2 || pg.Bucket != "data01" || pg.Source != "vast01" || pg.Operation == "" {
		t.Fatalf("purge: %+v", pg)
	}
	if ok, _ := rg.vast01.be.BucketExists("data01"); ok {
		t.Error("the source bucket still exists")
	}
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if p.State != directory.StateActive || p.Primary != "vast02" || p.Source != "" || len(p.Names) != 1 {
		t.Fatalf("placement after purge: %+v", p)
	}

	// vast01 is still the tenant's default: refused until repointed, then removed with the token
	// its dry run issues.
	rg.refused("DELETE", "/v1/clusters/vast01", nil, "tenants.acme.default_cluster")
	rg.must("POST", "/v1/tenants/acme/default-cluster", TenantDefaultRequest{Cluster: "vast02"}, nil)
	var rd RemoveDryRun
	rg.must("DELETE", "/v1/clusters/vast01?dry_run=1", nil, &rd)
	if !rd.Allowed || rd.Token == "" || len(rd.References) != 0 {
		t.Fatalf("remove dry run: %+v", rd)
	}
	rg.refused("DELETE", "/v1/clusters/vast01", nil, "needs the confirmation token")
	var rm RemoveResult
	rg.must("DELETE", "/v1/clusters/vast01", RemoveRequest{Token: rd.Token}, &rm)
	if rm.Removed != "vast01" || rm.Operation == "" {
		t.Fatalf("remove: %+v", rm)
	}
	if _, ok := rg.ctl.Clusters.Load().Get("vast01"); ok {
		t.Error("vast01 is still live in the proxy")
	}
	if len(rg.slept) != 1 || rg.slept[0] != 5*time.Second {
		t.Errorf("cutover windows waited: %v", rg.slept)
	}

	// Every operator action and every refusal is in the server's log, whoever ran it.
	log := rg.log.String()
	for _, want := range []string{
		"INFO  bucket adopted  placement=acme/data01 cluster=vast01 bucket=data01",
		"WARN  adopt refused  actor=api:127.0.0.1 placement=acme/data01 status=409 code=refused reason=\"bucket nope does not exist on vast01",
		"INFO  target recorded  placement=acme/data01 target=vast02 bucket=data01-001",
		"INFO  ramp  placement=acme/data01 from=ACTIVE to=RAMPING primary=vast02 source=vast01 ratio=0.5",
		"WARN  ramp refused",
		"INFO  migrate start  placement=acme/data01 from=RAMPING to=MIGRATING",
		"WARN  mover report refused",
		"INFO  mover pass  placement=acme/data01 pass=2 copied=0 already_there=1",
		"WARN  cutover refused  actor=api:127.0.0.1 placement=acme/data01 status=409 code=refused reason=\"no mover has reported",
		"INFO  cutover window started  placement=acme/data01 window=5s",
		"INFO  cutover  placement=acme/data01 from=MIGRATING to=CUTOVER",
		"WARN  purge-source refused",
		"INFO  source purged  placement=acme/data01 cluster=vast01 bucket=data01",
		"WARN  cluster remove refused  actor=api:127.0.0.1 cluster=vast01",
		"INFO  tenant default changed  tenant=acme default_cluster=vast02",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("server log lacks %q", want)
		}
	}
	if t.Failed() {
		t.Logf("server log:\n%s", log)
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

	// The cluster itself says the credentials are wrong: refused before anything is written, with
	// the half that is wrong named. A key without ListBuckets permission still passes.
	rg.vast01.reject = "SignatureDoesNotMatch"
	envRef := rg.vast01.definition(true)
	envRef.Credentials.SecretRef = "env:SHUNT_CONTROL_TEST_SECRET"
	t.Setenv("SHUNT_CONTROL_TEST_SECRET", "stale")
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: envRef}, "set SHUNT_CONTROL_TEST_SECRET there and restart serve")
	rg.vast01.reject = "InvalidAccessKeyId"
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, "does not know access key AK")
	if _, ok := rg.dir.Snapshot().Cluster("vast01"); ok {
		t.Fatal("a cluster with rejected credentials reached the directory")
	}
	// Signed for a region the cluster does not serve, as Garage answers: refused with the region it
	// expects, by the probe as well as the add.
	rg.vast01.reject = "AuthorizationHeaderMalformed"
	rg.refused("POST", "/v1/clusters/probe", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, "expects region garage, not us-east-1")
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, "expects region garage, not us-east-1")
	rg.vast01.reject = "AccessDenied"
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.vast01.reject = ""
	gone := rg.vast02.definition(true)
	gone.Endpoints = []string{"127.0.0.1:1"}
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "gone", Cluster: gone}, "cannot reach cluster gone")
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

// A secret given to cluster add is stored by the server, 0600, and referenced as a file: ref; the
// type comes from the cluster's Server header and the region defaults. A refused add stores nothing,
// a replaced secret removes the old file, and removing the cluster removes its secret.
func TestClusterAddStoresTheSecret(t *testing.T) {
	rg := newRig(t)
	rg.vast01.server = "vast 5.5.0.1"
	def := rg.vast01.definition(true)
	def.Type, def.Region, def.Credentials.SecretRef = "", "", ""
	var out ClusterStatus
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: def, Secret: "first-secret"}, &out)
	if out.Type != "vast" || out.Region != "us-east-1" || !strings.HasPrefix(out.SecretRef, "file:"+rg.ctl.SecretsDir+"/vast01-") {
		t.Fatalf("stored cluster: %+v", out)
	}
	first := strings.TrimPrefix(out.SecretRef, "file:")
	if b, err := os.ReadFile(first); err != nil || string(b) != "first-secret" {
		t.Fatalf("secret file: %q %v", b, err)
	}
	if st, err := os.Stat(first); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("secret file mode: %v %v", st.Mode(), err)
	}
	if raw, _ := os.ReadFile(rg.dir.Path()); strings.Contains(string(raw), "first-secret") {
		t.Fatal("the secret reached the directory file")
	}
	if strings.Contains(rg.log.String(), "first-secret") {
		t.Fatal("the secret reached the log")
	}

	rg.vast01.reject = "SignatureDoesNotMatch"
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: def, Secret: "wrong"}, "the secret key you entered is not the secret of access key AK")
	rg.vast01.reject = ""
	if n := secretFiles(t, rg.ctl.SecretsDir); len(n) != 1 {
		t.Fatalf("a refused add left secret files: %v", n)
	}

	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: def, Secret: "second-secret"}, &out)
	if files := secretFiles(t, rg.ctl.SecretsDir); len(files) != 1 || "file:"+filepath.Join(rg.ctl.SecretsDir, files[0]) != out.SecretRef {
		t.Fatalf("after replacing the secret: %v, ref %s", files, out.SecretRef)
	}
	rg.answers("POST", "/v1/clusters", ClusterRequest{Name: "../etc", Cluster: def, Secret: "x"}, http.StatusBadRequest, "bad_request")
	var rd RemoveDryRun
	rg.must("DELETE", "/v1/clusters/vast01?dry_run=1", nil, &rd)
	if !rd.Allowed || rd.SecretFiles != 1 {
		t.Fatalf("remove dry run: %+v", rd)
	}
	rg.must("DELETE", "/v1/clusters/vast01", RemoveRequest{Token: rd.Token}, nil)
	if files := secretFiles(t, rg.ctl.SecretsDir); len(files) != 0 {
		t.Fatalf("removing the cluster left its secret: %v", files)
	}
	rg.ctl.SecretsDir = ""
	rg.refused("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: def, Secret: "x"}, "no secrets directory")
}

func secretFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestTypeAndRegionInference(t *testing.T) {
	for server, want := range map[string]string{"vast 5.4.6.0": "vast", "MinIO": "minio", "AmazonS3": "aws", "": "s3", "garage": "s3"} {
		if got := typeFromServer(server); got != want {
			t.Errorf("typeFromServer(%q) = %q, want %q", server, got, want)
		}
	}
	for ep, want := range map[string]string{"s3.eu-west-1.amazonaws.com:443": "eu-west-1", "s3-us-west-2.amazonaws.com:443": "us-west-2", "10.0.1.10:80": "us-east-1"} {
		if got := regionFor([]string{ep}); got != want {
			t.Errorf("regionFor(%q) = %q, want %q", ep, got, want)
		}
	}
}

// expand measures a target's conditional PUT and DELETE when the cluster does not state them, and
// records what it found; a stated capability is left alone.
// A target key that may not create buckets is named in the error, with the way around it: a bucket
// created there by someone else, given to expand by name, is used as it is.
func TestExpandIntoABucketTheKeyCannotCreate(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	rg.vast02.noCreate = true
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/default/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	code, raw := rg.call("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02", Create: true}, nil)
	for _, want := range []string{"creating bucket data01-001: HTTP 403 AccessDenied (Access Denied)", "access key AK may not create buckets on vast02"} {
		if code == http.StatusOK || !strings.Contains(raw, want) {
			t.Fatalf("expand into a bucket the key cannot create: HTTP %d %s, want %q", code, raw, want)
		}
	}
	if err := rg.vast02.be.CreateBucket("team-archive"); err != nil {
		t.Fatal(err)
	}
	var ex ExpandResult
	rg.must("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02", Name: "team-archive", Create: true}, &ex)
	if ex.Name != "team-archive" || ex.CreatedBucket {
		t.Fatalf("expand by name: %+v", ex)
	}
}

func TestExpandMeasuresConditionals(t *testing.T) {
	for _, tc := range []struct {
		name             string
		condPut, condDel bool
	}{{"honors both", true, true}, {"honors neither", false, false}, {"put only", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			if err := rg.vast01.be.CreateBucket("data01"); err != nil {
				t.Fatal(err)
			}
			if err := rg.vast02.be.CreateBucket("data01-001"); err != nil {
				t.Fatal(err)
			}
			rg.vast02.condPut, rg.vast02.condDel = tc.condPut, tc.condDel
			rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
			unstated := rg.vast02.definition(true)
			unstated.Capabilities = config.Capabilities{}
			rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: unstated}, nil)
			rg.must("POST", "/v1/placements/default/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
			var ex ExpandResult
			rg.must("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02"}, &ex)
			if !ex.Measured || ex.ConditionalWrite != tc.condPut || ex.ConditionalDelete != tc.condDel {
				t.Fatalf("expand: %+v", ex)
			}
			c, _ := rg.dir.Snapshot().Cluster("vast02")
			if c.Capabilities.ConditionalWriteOr(!tc.condPut) != tc.condPut || c.Capabilities.ConditionalDeleteOr(!tc.condDel) != tc.condDel {
				t.Fatalf("recorded capabilities: %+v", c.Capabilities)
			}
			if objs, err := rg.vast02.be.ListBucket("data01-001", nil, gofakes3.ListBucketPage{}); err != nil || len(objs.Contents) != 0 {
				t.Fatalf("probe objects left behind: %v %v", objs, err)
			}
		})
	}
	rg := newRig(t)
	_ = rg.vast01.be.CreateBucket("data01")
	_ = rg.vast02.be.CreateBucket("data01-001")
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	stated := rg.vast02.definition(true)
	no := false
	stated.Capabilities.ConditionalDelete = &no
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: stated}, nil)
	rg.must("POST", "/v1/placements/default/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	var ex ExpandResult
	rg.must("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02"}, &ex)
	if ex.Measured || !ex.ConditionalWrite {
		t.Fatalf("a stated capability was measured over: %+v", ex)
	}
}

// TestBackendReplaysOnAStaleConnection holds the keep-alive race MinIO produces after answering a
// conditional PUT with 412: the connection is closed after the next request is sent and before it
// is answered. Go replays only requests it knows are idempotent, so a PUT or DELETE failed with a
// bare EOF and expand refused spuriously.
func TestBackendReplaysOnAStaleConnection(t *testing.T) {
	type connKey struct{}
	var mu sync.Mutex
	served := map[any]int{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := r.Context().Value(connKey{})
		mu.Lock()
		served[c]++
		n := served[c]
		mu.Unlock()
		if n > 1 { // every connection answers one request, then drops the next unanswered
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context { return context.WithValue(ctx, connKey{}, c) }
	srv.Start()
	defer srv.Close()

	cl, err := upstream.New("stale", config.Cluster{Type: "s3", Scheme: "http", Region: "us-east-1",
		Endpoints: []string{strings.TrimPrefix(srv.URL, "http://")}}, upstream.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Transport.CloseIdleConnections()
	cl.Creds.Secret = "secret"
	b := backend{cl: cl}
	for i, req := range []struct {
		method string
		body   []byte
	}{{http.MethodDelete, nil}, {http.MethodDelete, nil}, {http.MethodPut, []byte("body")}, {http.MethodPut, []byte("body")}} {
		r, err := b.do(context.Background(), req.method, "b", "k", nil, req.body, nil)
		if err != nil {
			t.Fatalf("request %d (%s): %v", i, req.method, err)
		}
		if r.status != http.StatusNoContent {
			t.Fatalf("request %d (%s): HTTP %d", i, req.method, r.status)
		}
	}
}

// TestPurgeDiffIgnoresConcurrentDeletes holds the spurious purge refusal seen under warp load: a
// client delete (source, then primary) landing after the source's listing page was read and before
// the primary's was made the diff report keys the primary never lacked. More of them than the
// report limit must not hide a key that really is missing, either.
func TestPurgeDiffIgnoresConcurrentDeletes(t *testing.T) {
	src, dst := newFakeCluster(t), newFakeCluster(t)
	for _, fc := range []*fakeCluster{src, dst} {
		if err := fc.be.CreateBucket("b"); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 1025 { // two listing pages: gofakes3 serves 1000 keys a page
		k := fmt.Sprintf("k%04d", i)
		src.put(t, "b", k, "x")
		dst.put(t, "b", k, "x")
	}
	src.put(t, "b", "z-only-on-source", "x")
	// Keys k1000..k1024 are deleted by a client just before the primary's second page is served,
	// after the source's second page listed them.
	dst.onList = func(token string) {
		if token == "" {
			return
		}
		for i := 1000; i < 1025; i++ {
			k := fmt.Sprintf("k%04d", i)
			for _, fc := range []*fakeCluster{src, dst} {
				if _, err := fc.be.DeleteObject("b", k); err != nil {
					t.Error(err)
				}
			}
		}
		dst.onList = nil
	}
	missing, _, _, err := missingOn(context.Background(), fakeBackend(t, "src", src), "b", fakeBackend(t, "dst", dst), "b", 20, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != "z-only-on-source" {
		t.Fatalf("missing = %v, want only z-only-on-source", missing)
	}
}

func fakeBackend(t *testing.T, name string, fc *fakeCluster) backend {
	t.Helper()
	cl, err := upstream.New(name, config.Cluster{Type: "s3", Scheme: "http", Region: "us-east-1",
		Endpoints: []string{strings.TrimPrefix(fc.srv.URL, "http://")}}, upstream.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Transport.CloseIdleConnections)
	cl.Creds.AccessKey, cl.Creds.Secret = "AK", "SK"
	return backend{cl: cl}
}

// TestStepOut checks what stands between a tenant's clients and their cluster, and that nothing it
// finds is missed or invented: a renamed bucket, an upload in progress, a key the cluster does not
// know, buckets on two clusters, a move under way; and a tenant with none of those is ready.
func TestStepOut(t *testing.T) {
	rg := newRig(t)
	for _, b := range []string{"data01-001", "logs", "other"} {
		if err := rg.vast02.be.CreateBucket(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := rg.vast01.be.CreateBucket("moving"); err != nil {
		t.Fatal(err)
	}
	rg.vast02.keys = map[string]bool{"AK": true, "SHARED": true}
	keys := map[string][]sigv4.Credential{directory.DefaultTenant: {{AccessKey: "SHUNTONLY", Secret: "s", Tenant: directory.DefaultTenant}}}
	rg.ctl.Keys = &stubKeys{byTenant: keys}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/default/data01/adopt", AdoptRequest{Cluster: "vast02", Name: "data01-001"}, nil)
	rg.must("POST", "/v1/placements/default/logs/adopt", AdoptRequest{Cluster: "vast02"}, nil)
	vast02, err := rg.ctl.backendFor("vast02")
	if err != nil {
		t.Fatal(err)
	}
	if r, err := vast02.do(context.Background(), http.MethodPost, "logs", "big", url.Values{"uploads": {""}}, nil, nil); err != nil || r.status != http.StatusOK {
		t.Fatalf("starting an upload: %v %+v", err, r)
	}

	var so StepOut
	rg.must("GET", "/v1/tenants/default/step-out", nil, &so)
	if so.Ready || so.Cluster != "vast02" {
		t.Fatalf("step-out: %+v", so)
	}
	wantProblems(t, so.Buckets[0].Problems, "named data01-001 on vast02", "--name data01")
	wantProblems(t, so.Buckets[1].Problems, "multipart uploads in progress")
	if len(so.Keys) != 1 {
		t.Fatalf("keys: %+v", so.Keys)
	}
	wantProblems(t, so.Keys[0].Problems, "vast02 does not know this access key")

	// Tenant team2: a key vast02 issued, a bucket under its own name, nothing in flight: ready.
	keys["team2"] = []sigv4.Credential{{AccessKey: "SHARED", Secret: "s", Tenant: "team2"}}
	rg.must("POST", "/v1/placements/team2/other/adopt", AdoptRequest{Cluster: "vast02"}, nil)
	so = StepOut{}
	rg.must("GET", "/v1/tenants/team2/step-out", nil, &so)
	if !so.Ready || so.Cluster != "vast02" || len(so.Endpoints) != 1 || len(so.Keys) != 1 {
		t.Fatalf("want ready: %+v", so)
	}
	wantProblems(t, so.Notes, "shows 2 that shunt does not: data01-001, logs")

	// A bucket on a second cluster, and then a move under way, block it again.
	rg.must("POST", "/v1/placements/team2/moving/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	so = StepOut{}
	rg.must("GET", "/v1/tenants/team2/step-out", nil, &so)
	if so.Ready || so.Cluster != "" || len(so.Keys) != 0 {
		t.Fatalf("two clusters: %+v", so)
	}
	wantProblems(t, so.Problems, "on 2 clusters (vast01, vast02)")
	rg.must("POST", "/v1/placements/team2/moving/ramp", RampRequest{Ratio: 0.5, To: "vast02", Create: true}, nil)
	so = StepOut{}
	rg.must("GET", "/v1/tenants/team2/step-out", nil, &so)
	wantProblems(t, so.Buckets[0].Problems, "is RAMPING")

	rg.answers("GET", "/v1/tenants/nobody/step-out", nil, http.StatusNotFound, "not_found")
}

func wantProblems(t *testing.T, got []string, want ...string) {
	t.Helper()
	all := strings.Join(got, "\n")
	for _, w := range want {
		if !strings.Contains(all, w) {
			t.Errorf("want %q in:\n%s", w, all)
		}
	}
}

// Importing the cluster's own client keys is what lets clients keep their credentials: through
// shunt, and again when shunt steps out. A key the cluster does not know is refused.
func TestImportClientKeys(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	rg.vast01.keys = map[string]bool{"AK": true, "CLUSTERKEY": true}
	sk := &stubKeys{}
	rg.ctl.Keys = sk
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)

	// adopt --keys: a key vast01 does not know is refused, and nothing is adopted or stored.
	rg.refused("POST", "/v1/placements/default/data01/adopt",
		AdoptRequest{Cluster: "vast01", Keys: []ClientKeyRequest{{AccessKey: "NOPE", Secret: "s"}}}, "does not know this access key")
	if len(sk.stored) != 0 {
		t.Fatalf("a refused key was stored: %+v", sk.stored)
	}
	if _, ok := rg.dir.Snapshot().Lookup("default", "data01"); ok {
		t.Fatal("the bucket was adopted although its keys were refused")
	}

	rg.must("POST", "/v1/placements/default/data01/adopt",
		AdoptRequest{Cluster: "vast01", Keys: []ClientKeyRequest{{AccessKey: "CLUSTERKEY", Secret: "s", Buckets: []string{"data01"}}}}, nil)
	if len(sk.stored) != 1 || sk.stored[0].AccessKey != "CLUSTERKEY" || sk.stored[0].Tenant != "default" || len(sk.stored[0].Buckets) != 1 {
		t.Fatalf("stored: %+v", sk.stored)
	}
	if !strings.Contains(rg.log.String(), "client key imported") || strings.Contains(rg.log.String(), "secret") {
		t.Fatalf("one INFO line, and never a secret:\n%s", rg.log.String())
	}

	// With the cluster's own key, step-out is ready: clients could leave shunt behind.
	var so StepOut
	rg.must("GET", "/v1/tenants/default/step-out", nil, &so)
	if !so.Ready {
		t.Fatalf("want ready with an imported key: %+v", so)
	}

	// A key the store refuses as a duplicate (another secret or tenant; auth.Merge), and a shunt with
	// no credentials file, are both refused.
	sk.addErr = fmt.Errorf("%w: twice", auth.ErrDuplicateKey)
	rg.refused("POST", "/v1/tenants/default/client-keys", ClientKeyRequest{AccessKey: "CLUSTERKEY", Secret: "s"}, "already holds access key CLUSTERKEY")
	sk.addErr = nil
	rg.ctl.Keys = nil
	rg.refused("POST", "/v1/tenants/default/client-keys", ClientKeyRequest{AccessKey: "CLUSTERKEY", Secret: "s"}, "no credentials file")

	// Removing a key: the lab key shunt generated for itself, once the real ones are in.
	rg.ctl.Keys = sk
	rg.answers("DELETE", "/v1/tenants/default/client-keys/NOSUCHKEY", nil, http.StatusNotFound, "not_found")
	var removed ClientKeyResult
	rg.must("DELETE", "/v1/tenants/default/client-keys/CLUSTERKEY", nil, &removed)
	if removed.Left != 0 || len(sk.stored) != 0 {
		t.Fatalf("after removing the only key: %+v %+v", removed, sk.stored)
	}
	rg.ctl.Keys = nil
	rg.refused("DELETE", "/v1/tenants/default/client-keys/CLUSTERKEY", nil, "cannot change its client keys")
}

// The browser's Create takes client keys as adopt does: each is checked before the bucket is
// created, and status then counts the keys that can reach the bucket, so the UI can say when none can.
func TestCreateBackendImportsClientKeys(t *testing.T) {
	rg := newRig(t)
	rg.vast01.keys = map[string]bool{"AK": true, "CLUSTERKEY": true}
	sk := &stubKeys{}
	rg.ctl.Keys = sk
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)

	rg.refused("POST", "/v1/placements/default/data01/create-backend",
		CreateBackendRequest{Cluster: "vast01", Keys: []ClientKeyRequest{{AccessKey: "NOPE", Secret: "s"}}}, "does not know this access key")
	if ok, _ := rg.vast01.be.BucketExists("data01"); ok || len(sk.stored) != 0 {
		t.Fatalf("a refused key created the bucket or was stored: %+v", sk.stored)
	}
	if _, ok := rg.dir.Snapshot().Lookup("default", "data01"); ok {
		t.Fatal("the bucket was placed although its keys were refused")
	}

	var ps PlacementStatus
	rg.must("POST", "/v1/placements/default/data01/create-backend",
		CreateBackendRequest{Cluster: "vast01", Keys: []ClientKeyRequest{{AccessKey: "CLUSTERKEY", Secret: "s", Buckets: []string{"data01"}}}}, &ps)
	if ok, _ := rg.vast01.be.BucketExists("data01"); !ok || len(sk.stored) != 1 || sk.stored[0].Tenant != "default" {
		t.Fatalf("created bucket %v, stored %+v", ok, sk.stored)
	}
	if ps.ClientKeys == nil || *ps.ClientKeys != 1 {
		t.Fatalf("client keys of the created bucket: %v", ps.ClientKeys)
	}

	// A second bucket without keys: the only key is limited to data01, so nothing reaches data02.
	rg.must("POST", "/v1/placements/default/data02/create-backend", CreateBackendRequest{Cluster: "vast01"}, nil)
	var st Status
	rg.must("GET", "/v1/status?all=1", nil, &st)
	counts := map[string]int{}
	for _, p := range st.Placements {
		if p.ClientKeys == nil {
			t.Fatalf("%s: no client key count", p.Key)
		}
		counts[p.Key] = *p.ClientKeys
	}
	if counts["default/data01"] != 1 || counts["default/data02"] != 0 {
		t.Fatalf("client key counts: %v", counts)
	}

	// A shunt that holds no keys reports no count rather than a misleading zero.
	rg.ctl.Keys = nil
	var bare Status
	rg.must("GET", "/v1/status?all=1", nil, &bare)
	for _, p := range bare.Placements {
		if p.ClientKeys != nil {
			t.Fatalf("%s: a count without a key store: %d", p.Key, *p.ClientKeys)
		}
	}
}

// A client bucket name the tenant already uses is refused before create or adopt touches a
// backend or imports a key, naming the bucket and pointing at expand. Create refuses a backend
// bucket that already exists (that is adopt's job), and never deletes a bucket it did not make.
func TestCreateAndAdoptRefuseATakenClientName(t *testing.T) {
	rg := newRig(t)
	rg.vast02.keys = map[string]bool{"AK": true, "CLUSTERKEY": true}
	sk := &stubKeys{}
	rg.ctl.Keys = sk
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/default/data01/create-backend", CreateBackendRequest{Cluster: "vast01"}, nil)

	taken := "default/data01 already exists (ACTIVE on vast01 as data01); choose another client bucket name, or expand data01"
	conflict := func(path string, body any) {
		t.Helper()
		code, raw := rg.call("POST", path, body, nil)
		var e Error
		_ = json.Unmarshal([]byte(raw), &e)
		if code != http.StatusConflict || e.Code != "conflict" || !strings.Contains(e.Message, taken) {
			t.Fatalf("POST %s: want 409 conflict %q, got HTTP %d %s", path, taken, code, raw)
		}
	}
	conflict("/v1/placements/default/data01/create-backend",
		CreateBackendRequest{Cluster: "vast02", Name: "data01-b", Keys: []ClientKeyRequest{{AccessKey: "CLUSTERKEY", Secret: "s"}}})
	if ok, _ := rg.vast02.be.BucketExists("data01-b"); ok || len(sk.stored) != 0 {
		t.Fatalf("a refused create made a bucket or imported a key: %+v", sk.stored)
	}
	if err := rg.vast02.be.CreateBucket("found"); err != nil {
		t.Fatal(err)
	}
	conflict("/v1/placements/default/data01/adopt", AdoptRequest{Cluster: "vast02", Name: "found"})

	// Create names a bucket that is already there: refused, and the bucket stays.
	rg.refused("POST", "/v1/placements/default/data02/create-backend", CreateBackendRequest{Cluster: "vast02", Name: "found"},
		"bucket found already exists on vast02; Adopt takes over a bucket that is already there")
	if ok, _ := rg.vast02.be.BucketExists("found"); !ok {
		t.Fatal("create deleted a bucket it did not make")
	}
	if _, ok := rg.dir.Snapshot().Lookup("default", "data02"); ok {
		t.Fatal("a refused create was placed")
	}
}

// A move never lands in a bucket that already holds objects: expand refuses it, naming the first
// key, unless the operator states they are this bucket's; a first step that names its own target
// is checked the same way. clear-target forgets a target before the first step and leaves its
// bucket alone, and only then.
func TestExpandRefusesANonEmptyTargetAndClearsATarget(t *testing.T) {
	rg := newRig(t)
	for _, b := range []struct {
		cl   *fakeCluster
		name string
	}{{rg.vast01, "data01"}, {rg.vast02, "old"}, {rg.vast02, "data01-001"}} {
		if err := b.cl.be.CreateBucket(b.name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rg.vast02.be.PutObject("old", "leftover/k1", nil, strings.NewReader("stale"), 5, nil); err != nil {
		t.Fatal(err)
	}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/default/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)

	rg.refused("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02", Name: "old"},
		`bucket old on vast02 already holds objects (the first is "leftover/k1")`)
	if p, _ := rg.dir.Snapshot().Lookup("default", "data01"); p.Target != "" {
		t.Fatalf("a refused expand recorded %s", p.Target)
	}
	// The same first step without expand is refused alike.
	rg.refused("POST", "/v1/placements/default/data01/ramp", RampRequest{Ratio: 0.1, To: "vast02", Name: "old"}, "already holds objects")

	// Stated to be this bucket's own objects: accepted.
	var ex ExpandResult
	rg.must("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02", Name: "old", AcceptObjects: true}, &ex)
	if ex.Target != "vast02" || ex.Name != "old" {
		t.Fatalf("expand: %+v", ex)
	}

	// clear-target forgets it; the bucket and its object stay.
	var cl ClearTargetResult
	rg.must("DELETE", "/v1/placements/default/data01/target", nil, &cl)
	p, _ := rg.dir.Snapshot().Lookup("default", "data01")
	if cl.Target != "vast02" || cl.Name != "old" || p.Target != "" || p.Names["vast02"] != "" || p.State != directory.StateActive {
		t.Fatalf("after clear: result %+v, placement %+v", cl, p)
	}
	if ok, _ := rg.vast02.be.BucketExists("old"); !ok {
		t.Fatal("clearing the target deleted its bucket")
	}
	rg.refused("DELETE", "/v1/placements/default/data01/target", nil, "has no target to clear")

	// An empty bucket needs no statement; once a step has used the target it can no longer be cleared.
	rg.must("POST", "/v1/placements/default/data01/expand", ExpandRequest{To: "vast02"}, &ex)
	rg.must("POST", "/v1/placements/default/data01/ramp", RampRequest{Ratio: 0.1}, nil)
	rg.refused("DELETE", "/v1/placements/default/data01/target", nil, "is RAMPING, moving to vast02; a target can only be cleared before the first step")
}

// create-backend with legs makes a bucket spread over one new bucket per cluster (ADR-0018 N2):
// everything is checked before anything is made, and nothing moves a spread bucket in this build.
func TestCreateSpreadBucket(t *testing.T) {
	rg := newRig(t)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)

	rg.answers("POST", "/v1/placements/default/spd/create-backend", CreateBackendRequest{Legs: []LegRequest{{Cluster: "vast01"}}}, http.StatusBadRequest, "invalid")
	rg.refused("POST", "/v1/placements/default/spd/create-backend",
		CreateBackendRequest{Legs: []LegRequest{{Cluster: "vast01", Name: "sp-one"}, {Cluster: "vast01", Name: "sp-two"}}}, "cluster vast01 is named twice")
	if err := rg.vast02.be.CreateBucket("sp-b"); err != nil {
		t.Fatal(err)
	}
	rg.refused("POST", "/v1/placements/default/spd/create-backend",
		CreateBackendRequest{Legs: []LegRequest{{Cluster: "vast01", Name: "sp-a"}, {Cluster: "vast02", Name: "sp-b"}}}, "bucket sp-b already exists on vast02")
	if ok, _ := rg.vast01.be.BucketExists("sp-a"); ok {
		t.Fatal("a refused spread create made a leg's bucket")
	}

	var ps PlacementStatus
	rg.must("POST", "/v1/placements/default/spd/create-backend",
		CreateBackendRequest{Legs: []LegRequest{{Cluster: "vast01", Name: "sp-a"}, {Cluster: "vast02", Name: "sp-c"}}}, &ps)
	if len(ps.Legs) != 2 || ps.Legs[0].Cluster != "vast01" || ps.Legs[1].Bucket != "sp-c" || ps.Primary != "" || ps.State != directory.StateActive {
		t.Fatalf("created: %+v", ps)
	}
	if d := ps.Legs[0].Share + ps.Legs[1].Share; d < 0.999 || d > 1.001 || ps.Legs[0].Share < 0.49 {
		t.Fatalf("shares: %+v", ps.Legs)
	}
	for _, b := range []struct {
		cl   *fakeCluster
		name string
	}{{rg.vast01, "sp-a"}, {rg.vast02, "sp-c"}} {
		if ok, _ := b.cl.be.BucketExists(b.name); !ok {
			t.Fatalf("leg bucket %s was not created", b.name)
		}
	}
	code, raw := rg.call("POST", "/v1/placements/default/spd/create-backend", CreateBackendRequest{Cluster: "vast01", Name: "other"}, nil)
	if code != http.StatusConflict || !strings.Contains(raw, "default/spd already exists (spread over 2 backend buckets)") {
		t.Fatalf("a taken spread name: HTTP %d %s", code, raw)
	}
	rg.refused("POST", "/v1/placements/default/spd/expand", ExpandRequest{To: "vast02", Create: true}, "spread over 2 backend buckets; adding one or moving keys between them is ADR-0018 N3")
	rg.refused("POST", "/v1/placements/default/spd/ramp", RampRequest{Ratio: 0.5, To: "vast02", Create: true}, "name the range of keys to move")
	var so StepOut
	rg.must("GET", "/v1/tenants/default/step-out", nil, &so)
	if so.Ready || len(so.Buckets) != 1 || len(so.Buckets[0].Problems) != 1 || !strings.Contains(so.Buckets[0].Problems[0], "spread over 2 backend buckets (vast01/sp-a, vast02/sp-c)") {
		t.Fatalf("step-out of a spread bucket: %+v", so)
	}
	var st Status
	rg.must("GET", "/v1/status?all=1", nil, &st)
	if len(st.Placements) != 1 || len(st.Placements[0].Legs) != 2 {
		t.Fatalf("status: %+v", st.Placements)
	}
}

// Half of a plain bucket moves to another cluster through the API (ADR-0018 N3): ramp, migrate,
// the mover's reports, cutover and purge all act on the move; purge compares and deletes only the
// moving range and leaves the source bucket, which keeps the other half; finish is refused while the
// source keeps keys. Moving the other half and purging it deletes the source bucket, and the
// bucket settles back to one cluster.
func TestMoveHalfABucketThroughTheAPI(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	lower := directory.HashRange{From: 0, To: 1<<63 - 1}
	upper := directory.HashRange{From: 1 << 63, To: directory.FullRange.To}
	var in, out []string
	for i := 0; len(in) < 4 || len(out) < 4; i++ {
		k := fmt.Sprintf("k%02d", i)
		if migrate.InRangeHash(lower, k) {
			in = append(in, k)
		} else {
			out = append(out, k)
		}
		rg.vast01.put(t, "data01", k, "v")
	}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)

	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 1, To: "vast02", Name: "data01-b", Create: true, Range: &lower}, &tr)
	if tr.To != directory.StateRamping || tr.Primary != "vast02" || tr.Source != "vast01" || tr.Range == nil || *tr.Range != lower {
		t.Fatalf("first step of the move: %+v", tr)
	}
	// Status shows the move as a migration between its two clusters, and which part moves.
	var st Status
	rg.must("GET", "/v1/status?all=1", nil, &st)
	if ps := st.Placements[0]; ps.Source != "vast01" || ps.Primary != "vast02" || ps.Ratio != 1 || ps.Move == nil ||
		ps.Move.Share < 0.499 || ps.Move.Share > 0.501 || len(ps.Legs) != 2 || ps.Legs[1].Share != 0 {
		t.Fatalf("status during the move: %+v move %+v legs %+v", ps, ps.Move, ps.Legs)
	}
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, &tr)
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 1, Done: true, Converged: true}, nil)
	rg.ctl.Sleep = func(context.Context, time.Duration) error { return nil }
	rg.must("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "1s"}, &tr)

	// The mover copied the range except one key: purge names it, and counts the range alone.
	for _, k := range in[1:] {
		rg.vast02.put(t, "data01-b", k, "v")
	}
	var dry PurgeDryRun
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if dry.Allowed || len(dry.Missing) != 1 || dry.Missing[0] != in[0] || dry.Objects != len(in) {
		t.Fatalf("dry run with one key of the range missing: %+v", dry)
	}
	rg.refused("POST", "/v1/placements/acme/data01/finish", nil, "keeps other keys of the bucket")
	rg.vast02.put(t, "data01-b", in[0], "v")
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Objects != len(in) || dry.Source != "vast01" {
		t.Fatalf("dry run: %+v", dry)
	}
	var pg PurgeResult
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, &pg)
	if pg.ObjectsDeleted != len(in) {
		t.Fatalf("purge deleted %d objects, the range has %d", pg.ObjectsDeleted, len(in))
	}
	for _, k := range in {
		if rg.vast01.has("data01", k) {
			t.Fatalf("%s of the moved range is still on the source", k)
		}
	}
	for _, k := range out {
		if !rg.vast01.has("data01", k) {
			t.Fatalf("%s is outside the range and was purged", k)
		}
	}
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if !p.Spread() || p.State != directory.StateActive || len(p.Owners) != 2 || p.Owners[0].Leg != "vast02" {
		t.Fatalf("after the first move: %+v", p)
	}

	// The other half: the source leg owns nothing afterwards, so purge deletes its bucket.
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{To: "vast02", Range: &upper}, &tr)
	for _, k := range out {
		rg.vast02.put(t, "data01-b", k, "v")
	}
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 1, Done: true, Converged: true}, nil)
	rg.must("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "1s"}, &tr)
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, &pg)
	if ok, _ := rg.vast01.be.BucketExists("data01"); ok || !pg.BucketDeleted {
		t.Fatalf("the source leg owns nothing, but its bucket was kept (bucket_deleted %v)", pg.BucketDeleted)
	}
	p, _ = rg.dir.Snapshot().Lookup("acme", "data01")
	if p.Spread() || p.Primary != "vast02" || p.Names["vast02"] != "data01-b" || len(p.Names) != 1 {
		t.Fatalf("after moving every key: %+v", p)
	}
}

// A bucket moves to a new bucket on its own cluster through the API (ADR-0018 N3b): migrate names
// the same cluster and another bucket, status keeps the two buckets apart, and purge deletes the
// old bucket once its leg owns nothing, leaving a plain bucket under the new name.
func TestMoveToAnotherBucketOnTheSameClusterThroughTheAPI(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b", "c"} {
		rg.vast01.put(t, "data01", k, "v")
	}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)

	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{To: "vast01", Name: "data01-final", Create: true}, &tr)
	if tr.To != directory.StateMigrating || tr.Primary != "vast01" || tr.Source != "vast01" || tr.CreatedBucket != "data01-final" {
		t.Fatalf("migrate within vast01: %+v", tr)
	}
	var st Status
	rg.must("GET", "/v1/status?all=1", nil, &st)
	if ps := st.Placements[0]; ps.PrimaryBucket != "data01-final" || ps.SourceBucket != "data01" || ps.Names["vast01"] != "data01-final" {
		t.Fatalf("status of a move within one cluster: %+v", ps)
	}
	for _, k := range []string{"a", "b", "c"} {
		rg.vast01.put(t, "data01-final", k, "v") // the mover's work
	}
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast01", Pass: 1, Done: true, Converged: true}, nil)
	rg.ctl.Sleep = func(context.Context, time.Duration) error { return nil }
	rg.must("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "1s"}, &tr)
	var dry PurgeDryRun
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Bucket != "data01" || dry.Objects != 3 {
		t.Fatalf("dry run: %+v", dry)
	}
	var pg PurgeResult
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, &pg)
	if ok, _ := rg.vast01.be.BucketExists("data01"); ok || pg.Bucket != "data01" || pg.ObjectsDeleted != 3 {
		t.Fatalf("purge of the old bucket: %+v, still there %v", pg, ok)
	}
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if p.Spread() || p.Primary != "vast01" || len(p.Names) != 1 || p.Names["vast01"] != "data01-final" {
		t.Fatalf("after the move: %+v", p)
	}
}

// A spread bucket consolidates into one new bucket through the API (ADR-0018 N3c): each leg's keys
// move by naming the leg, one move at a time; each purge deletes a source leg's bucket once it owns
// nothing; the bucket ends plain under the new name, and step-out no longer reports it spread.
func TestConsolidateASpreadBucketThroughTheAPI(t *testing.T) {
	rg := newRig(t)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	// vast02's conditional-write profile is only assumed: the first step into it measures it, as
	// expand would, or the mover would refuse the destination.
	unmeasured := rg.vast02.definition(true)
	unmeasured.Capabilities = config.Capabilities{}
	rg.vast02.condPut = true
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: unmeasured}, nil)
	rg.must("POST", "/v1/placements/acme/spd/create-backend",
		CreateBackendRequest{Legs: []LegRequest{{Cluster: "vast01", Name: "sp-a"}, {Cluster: "vast02", Name: "sp-b"}}}, nil)
	p, _ := rg.dir.Snapshot().Lookup("acme", "spd")
	legs := map[string]struct {
		cl     *fakeCluster
		bucket string
	}{"vast01": {rg.vast01, "sp-a"}, "vast02": {rg.vast02, "sp-b"}}
	keys := map[string][]string{}
	for i := range 12 {
		k := fmt.Sprintf("k%02d", i)
		owner, err := migrate.OwnerOf(p, k)
		if err != nil {
			t.Fatal(err)
		}
		keys[owner] = append(keys[owner], k)
		legs[owner].cl.put(t, legs[owner].bucket, k, "v")
	}
	if len(keys["vast01"]) == 0 || len(keys["vast02"]) == 0 {
		t.Fatalf("every leg needs keys: %v", keys)
	}
	rg.refused("DELETE", "/v1/placements/acme/spd/target", nil, "no idle leg to retire")
	rg.refused("POST", "/v1/placements/acme/spd/migrate", MigrateRequest{To: "vast02", Name: "sp-final", Leg: "nope"}, "has no leg nope")
	rg.ctl.Sleep = func(context.Context, time.Duration) error { return nil }

	moveLeg := func(leg string) {
		t.Helper()
		var tr TransitionResult
		rg.must("POST", "/v1/placements/acme/spd/migrate", MigrateRequest{To: "vast02", Name: "sp-final", Create: true, Leg: leg}, &tr)
		if tr.To != directory.StateMigrating || tr.Range == nil {
			t.Fatalf("moving leg %s: %+v", leg, tr)
		}
		if cl, _ := rg.dir.Snapshot().Cluster("vast02"); cl.Capabilities.ConditionalWrite == nil || !*cl.Capabilities.ConditionalWrite || cl.Capabilities.ConditionalDelete == nil {
			t.Fatalf("the destination's profile was not measured: %+v", cl.Capabilities)
		}
		if err := rg.ctl.checkMover("acme/spd", MoverRequest{}); err != nil && strings.Contains(err.Error(), "conditional-write profile") {
			t.Fatalf("the mover refuses the move's destination: %v", err)
		}
		rg.refused("POST", "/v1/placements/acme/spd/ramp", RampRequest{Ratio: 1, Leg: "other"}, "finish it before moving another leg")
		for _, k := range keys[leg] {
			rg.vast02.put(t, "sp-final", k, "v") // the mover's work
		}
		rg.must("POST", "/v1/placements/acme/spd/mover-progress", Progress{Source: leg, Primary: "vast02", Pass: 1, Done: true, Converged: true}, nil)
		rg.must("POST", "/v1/placements/acme/spd/cutover", CutoverRequest{Window: "1s"}, &tr)
		var dry PurgeDryRun
		rg.must("POST", "/v1/placements/acme/spd/purge-source", PurgeRequest{DryRun: true}, &dry)
		if !dry.Allowed || dry.Bucket != legs[leg].bucket || dry.Objects != len(keys[leg]) {
			t.Fatalf("dry run for leg %s: %+v", leg, dry)
		}
		var pg PurgeResult
		rg.must("POST", "/v1/placements/acme/spd/purge-source", PurgeRequest{Token: dry.Token}, &pg)
		if ok, _ := legs[leg].cl.be.BucketExists(legs[leg].bucket); ok {
			t.Fatalf("leg %s owns nothing, but its bucket %s was kept", leg, legs[leg].bucket)
		}
	}
	moveLeg("vast01")
	p, _ = rg.dir.Snapshot().Lookup("acme", "spd")
	if !p.Spread() || len(p.Legs) != 2 || p.Legs["vast02-2"].Bucket != "sp-final" {
		t.Fatalf("after the first leg: %+v", p)
	}
	var so StepOut
	rg.must("GET", "/v1/tenants/acme/step-out", nil, &so)
	if so.Ready || len(so.Buckets) != 1 || !strings.Contains(strings.Join(so.Buckets[0].Problems, " "), "consolidate it first") {
		t.Fatalf("step-out of a spread bucket: %+v", so)
	}
	moveLeg("vast02")
	p, _ = rg.dir.Snapshot().Lookup("acme", "spd")
	if p.Spread() || p.Primary != "vast02" || len(p.Names) != 1 || p.Names["vast02"] != "sp-final" {
		t.Fatalf("after consolidating: %+v", p)
	}
	for _, k := range append(keys["vast01"], keys["vast02"]...) {
		if !rg.vast02.has("sp-final", k) {
			t.Fatalf("%s is not in the consolidated bucket", k)
		}
	}
	rg.must("GET", "/v1/tenants/acme/step-out", nil, &so)
	for _, b := range so.Buckets {
		if strings.Contains(strings.Join(b.Problems, " "), "spread over") {
			t.Fatalf("a consolidated bucket is still reported spread: %+v", b)
		}
	}
}

// Prefix rules through the API (ADR-0020 P1): carve gives a prefix a scope of its own without moving
// anything, status and step-out show it, a move is refused while it exists (P2), and merge gives
// back the plain bucket.
func TestCarveAndMergeThroughTheAPI(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	rg.vast01.put(t, "data01", "archive/a", "v")
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	before, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	plain := *before

	rg.answers("POST", "/v1/placements/acme/data01/prefixes", PrefixRequest{}, http.StatusBadRequest, "invalid")
	var res PrefixResult
	rg.must("POST", "/v1/placements/acme/data01/prefixes", PrefixRequest{Prefix: "archive/"}, &res)
	if res.Rules != 1 || res.Prefix != "archive/" {
		t.Fatalf("carve: %+v", res)
	}
	rg.refused("POST", "/v1/placements/acme/data01/prefixes", PrefixRequest{Prefix: "archive/"}, "already has a rule")
	var st Status
	rg.must("GET", "/v1/status?bucket=acme/data01", nil, &st)
	if ps := st.Placements[0]; len(ps.Scopes) != 1 || ps.Scopes[0].Prefix != "archive/" || len(ps.Scopes[0].Legs) != 1 || ps.Scopes[0].Legs[0].Bucket != "data01" || ps.Scopes[0].Legs[0].Share != 1 {
		t.Fatalf("status of a carved bucket: %+v", st.Placements[0])
	} else if len(ps.Legs) != 1 || ps.Legs[0].Idle {
		t.Fatalf("its legs: %+v", ps.Legs)
	}
	var so StepOut
	rg.must("GET", "/v1/tenants/acme/step-out", nil, &so)
	if len(so.Buckets) != 1 || !strings.Contains(strings.Join(so.Buckets[0].Problems, " "), "merge them (shunt expand data01 --merge <prefix>") {
		t.Fatalf("step-out of a carved bucket: %+v", so)
	}
	rg.refused("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5, To: "vast02", Name: "data01-b", Create: true, Scope: "logs/"}, "no prefix rule \"logs/\"; carve it first")
	if ok, _ := rg.vast02.be.BucketExists("data01-b"); ok {
		t.Fatal("a refused move made a bucket")
	}
	if !rg.vast01.has("data01", "archive/a") {
		t.Fatal("carving moved data")
	}

	rg.refused("DELETE", "/v1/placements/acme/data01/prefixes?prefix=logs/", nil, "no rule")
	rg.must("DELETE", "/v1/placements/acme/data01/prefixes?prefix=archive/", nil, &res)
	if res.Rules != 0 {
		t.Fatalf("merge: %+v", res)
	}
	after, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if !reflect.DeepEqual(*after, plain) {
		t.Fatalf("carve then merge did not give back the plain bucket:\n got  %+v\n want %+v", *after, plain)
	}
}

// A move of one prefix rule's keys through the API (ADR-0020 P2): archive/ is carved in a plain
// bucket and moved whole to vast02; the purge compares and deletes archive/'s keys alone, listing
// only that prefix, and keeps the source bucket, which holds the rest.
func TestScopedMoveThroughTheAPI(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	archive := []string{"archive/a", "archive/b", "archive/c"}
	rest := []string{"data/a", "data/b", "archived"} // "archived" shares the prefix's letters, not the prefix
	for _, k := range append(slices.Clone(archive), rest...) {
		rg.vast01.put(t, "data01", k, "v")
	}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.must("POST", "/v1/placements/acme/data01/prefixes", PrefixRequest{Prefix: "archive/"}, nil)

	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{Scope: "archive/", To: "vast02", Name: "data01-arch", Create: true}, &tr)
	if tr.To != directory.StateMigrating || tr.Primary != "vast02" || tr.Source != "vast01" || tr.Range == nil || *tr.Range != directory.FullRange {
		t.Fatalf("moving archive/: %+v", tr)
	}
	var st Status
	rg.must("GET", "/v1/status?bucket=acme/data01", nil, &st)
	if m := st.Placements[0].Move; m == nil || m.Scope != "archive/" {
		t.Fatalf("status of the move: %+v", st.Placements[0])
	}
	rg.refused("DELETE", "/v1/placements/acme/data01/prefixes?prefix=archive/", nil, "only at rest")
	for _, k := range archive {
		rg.vast02.put(t, "data01-arch", k, "v") // the mover's work
	}
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 1, Done: true, Converged: true}, nil)
	rg.ctl.Sleep = func(context.Context, time.Duration) error { return nil }
	rg.must("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "1s"}, &tr)
	rg.refused("POST", "/v1/placements/acme/data01/finish", nil, "keeps other keys of the bucket")

	rg.vast01.listMu.Lock()
	rg.vast01.listed = nil
	rg.vast01.listMu.Unlock()
	var dry PurgeDryRun
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Objects != len(archive) || dry.Bucket != "data01" || !dry.KeepsBucket {
		t.Fatalf("dry run: %+v", dry)
	}
	var pg PurgeResult
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, &pg)
	if pg.ObjectsDeleted != len(archive) || pg.BucketDeleted {
		t.Fatalf("purge deleted %d objects (archive/ has %d), bucket deleted %v", pg.ObjectsDeleted, len(archive), pg.BucketDeleted)
	}
	rg.vast01.listMu.Lock()
	listed := slices.Clone(rg.vast01.listed)
	rg.vast01.listMu.Unlock()
	if len(listed) == 0 || slices.ContainsFunc(listed, func(p string) bool { return p != "archive/" }) {
		t.Fatalf("purge listed the source under %q; want archive/ only", listed)
	}
	for _, k := range archive {
		if rg.vast01.has("data01", k) {
			t.Fatalf("%s of the moved prefix is still on the source", k)
		}
	}
	for _, k := range rest {
		if !rg.vast01.has("data01", k) {
			t.Fatalf("%s is outside the moved prefix and was purged", k)
		}
	}
	if ok, _ := rg.vast01.be.BucketExists("data01"); !ok {
		t.Fatal("the source bucket holds the rest of the bucket, but was deleted")
	}
	p, _ := rg.dir.Snapshot().Lookup("acme", "data01")
	if p.State != directory.StateActive || p.Move != nil || len(p.Prefixes) != 1 || p.Prefixes[0].Owners[0].Leg != "vast02" || p.Owners[0].Leg != "vast01" {
		t.Fatalf("after the move: %+v", p)
	}
}

// stubKeys is a Keys for tests: an in-memory list, with an Add that can be made to fail.
type stubKeys struct {
	stored   []sigv4.Credential
	byTenant map[string][]sigv4.Credential // when set, Tenant answers from it instead of stored
	addErr   error
}

func (k *stubKeys) Tenant(tenant string) []sigv4.Credential {
	if k.byTenant != nil {
		return k.byTenant[tenant]
	}
	var out []sigv4.Credential
	for _, c := range k.stored {
		if c.Tenant == tenant {
			out = append(out, c)
		}
	}
	return out
}

func (k *stubKeys) All() []sigv4.Credential { return k.stored }

func (k *stubKeys) Add(c sigv4.Credential) error {
	if k.addErr != nil {
		return k.addErr
	}
	k.stored = append(k.stored, c)
	return nil
}

func (k *stubKeys) Remove(ak string) error {
	k.stored = slices.DeleteFunc(k.stored, func(c sigv4.Credential) bool { return c.AccessKey == ak })
	return nil
}
