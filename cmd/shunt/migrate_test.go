package main

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// apiRig is a control API over a directory file and two in-process S3 clusters, the way `shunt
// serve` mounts it, for driving the operator CLI end to end.
type apiRig struct {
	url            string
	dir            *directory.FileDir
	dirPath        string
	ctl            *control.Server
	vast01, vast02 *s3mem.Backend
	ep01, ep02     string
}

func newAPIRig(t *testing.T) *apiRig {
	t.Helper()
	rg := &apiRig{vast01: s3mem.New(), vast02: s3mem.New()}
	for be, ep := range map[*s3mem.Backend]*string{rg.vast01: &rg.ep01, rg.vast02: &rg.ep02} {
		srv := httptest.NewServer(gofakes3.New(be, gofakes3.WithTimeSkewLimit(0)).Server())
		t.Cleanup(srv.Close)
		*ep = strings.TrimPrefix(srv.URL, "http://")
	}
	t.Setenv("SHUNT_TEST_CLUSTER_SECRET", "cluster-secret")
	rg.dirPath = filepath.Join(t.TempDir(), "directory.yaml")
	if err := os.WriteFile(rg.dirPath, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var err error
	if rg.dir, err = directory.Open(rg.dirPath); err != nil {
		t.Fatal(err)
	}
	reg := upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, config.ResolveSecret)
	t.Cleanup(reg.Close)
	rg.dir.Prepare = func(f *directory.File) error {
		_, _, aerr := reg.Apply(f.Clusters)
		return aerr
	}
	rg.ctl = &control.Server{Dir: rg.dir, Clusters: reg, Metrics: telemetry.NewMetrics()}
	api := httptest.NewServer(rg.ctl.Handler())
	t.Cleanup(api.Close)
	rg.url = api.URL
	return rg
}

// cli runs one shunt command against the rig's API and returns stdout and stderr together.
func (rg *apiRig) cli(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, stderr, err := run(t, append(args, "--api", rg.url)...)
	msg := out + stderr
	if err != nil {
		msg += err.Error()
	}
	return msg, err
}

func (rg *apiRig) must(t *testing.T, args ...string) string {
	t.Helper()
	out, err := rg.cli(t, args...)
	if err != nil {
		t.Fatalf("shunt %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func put(t *testing.T, be *s3mem.Backend, bucket, key, body string) {
	t.Helper()
	if _, err := be.PutObject(bucket, key, map[string]string{}, strings.NewReader(body), int64(len(body)), nil); err != nil {
		t.Fatal(err)
	}
}

func (rg *apiRig) addCluster(t *testing.T, name, endpoint string, extra ...string) string {
	t.Helper()
	return rg.must(t, append([]string{"cluster", "add", name, "--type", "vast", "--scheme", "http", "--region", "us-east-1", "--endpoint", endpoint,
		"--access-key", "AK", "--secret-ref", "env:SHUNT_TEST_CLUSTER_SECRET"}, extra...)...)
}

// The walkthrough, through the CLI: every verb an operator types, and what each one prints.
func TestOperatorVerbsWalkTheMove(t *testing.T) {
	rg := newAPIRig(t)
	if err := rg.vast01.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	put(t, rg.vast01, "data01", "a", "one")
	put(t, rg.vast01, "data01", "dir/b", "two")

	if out := rg.addCluster(t, "vast01", rg.ep01); !strings.Contains(out, "cluster vast01: vast http://"+rg.ep01) || !strings.Contains(out, "conditional_write true") {
		t.Fatalf("cluster add: %s", out)
	}
	if out := rg.must(t, "adopt", "vast01", "acme/data01"); !strings.Contains(out, "acme/data01: ACTIVE on vast01/data01") {
		t.Fatalf("adopt: %s", out)
	}
	rg.addCluster(t, "vast02", rg.ep02)
	if out := rg.must(t, "expand", "acme/data01", "--to", "vast02", "--create"); !strings.Contains(out, "target vast02/data01-001 (created); canary write, read, delete ok") {
		t.Fatalf("expand: %s", out)
	}
	if out := rg.must(t, "ramp", "acme/data01", "--ratio", "0.5"); !strings.Contains(out, "ACTIVE -> RAMPING") || !strings.Contains(out, "writes for 50% of keys now land on vast02") {
		t.Fatalf("ramp: %s", out)
	}
	rg.ctl.Metrics.RampWrites.WithLabelValues("acme/data01", "primary").Add(52)
	rg.ctl.Metrics.RampWrites.WithLabelValues("acme/data01", "source").Add(48)
	if out := rg.must(t, "status"); !strings.Contains(out, "52/48 (52%)") || !strings.Contains(out, "RAMPING") {
		t.Fatalf("status: %s", out)
	}
	var st control.Status
	if err := json.Unmarshal([]byte(rg.must(t, "status", "acme/data01", "--json")), &st); err != nil || len(st.Placements) != 1 || st.Placements[0].Writes["primary"] != 52 {
		t.Fatalf("status --json: %v %+v", err, st)
	}
	rg.must(t, "ramp", "acme/data01", "--ratio", "1")
	if out := rg.must(t, "migrate", "start", "acme/data01"); !strings.Contains(out, "RAMPING -> MIGRATING") {
		t.Fatalf("migrate start: %s", out)
	}
	if out, err := rg.cli(t, "cutover", "acme/data01", "--window", "0s"); err == nil || !strings.Contains(out, "no mover has reported") {
		t.Fatalf("cutover before the mover: %v %s", err, out)
	}

	cursors := t.TempDir()
	out := rg.must(t, "migrate", "run", "acme/data01", "--until-converged", "--cursor-dir", cursors, "--ledger-dir", cursors)
	if !strings.Contains(out, "(If-None-Match guard, re-HEAD withdrawal)") || !strings.Contains(out, "converged: the last pass copied nothing") ||
		!strings.Contains(out, "mover: 2 copied, 2 already on the target") {
		t.Fatalf("migrate run: %s", out)
	}
	if _, err := rg.vast02.HeadObject("data01-001", "dir/b"); err != nil {
		t.Fatalf("the mover did not copy dir/b: %v", err)
	}
	if out := rg.must(t, "status", "acme/data01"); !strings.Contains(out, "pass 2: 0 copied, 2 already there, 0 failed, converged") {
		t.Fatalf("status after the mover: %s", out)
	}
	if out := rg.must(t, "cutover", "acme/data01", "--window", "10ms"); !strings.Contains(out, "MIGRATING -> CUTOVER") {
		t.Fatalf("cutover: %s", out)
	}
	if out := rg.must(t, "purge-source", "acme/data01"); !strings.Contains(out, "deleted 2 objects and aborted 0 uploads from vast01/data01") {
		t.Fatalf("purge-source: %s", out)
	}
	if out, err := rg.cli(t, "cluster", "remove", "vast01"); err == nil || !strings.Contains(out, "refused: ") || !strings.Contains(out, "tenants.acme.default_cluster") {
		t.Fatalf("removing the tenant's default: %v %s", err, out)
	}
	rg.must(t, "tenant", "set-default", "acme", "vast02")
	if out := rg.must(t, "cluster", "remove", "vast01"); !strings.Contains(out, "cluster vast01 removed") {
		t.Fatalf("cluster remove: %s", out)
	}
	if d := readDir(t, rg.dirPath); strings.Contains(d, "vast01") {
		t.Fatalf("vast01 still in the directory:\n%s", d)
	}
}

// --from is the fleet form: every bucket on a cluster, one command per step.
func TestEvacuateACluster(t *testing.T) {
	rg := newAPIRig(t)
	for _, b := range []string{"data", "logs"} {
		if err := rg.vast01.CreateBucket(b); err != nil {
			t.Fatal(err)
		}
		put(t, rg.vast01, b, "k", b)
	}
	rg.addCluster(t, "vast01", rg.ep01)
	rg.addCluster(t, "vast02", rg.ep02)
	rg.must(t, "adopt", "vast01", "acme/data")
	rg.must(t, "adopt", "vast01", "acme/logs")
	if out := rg.must(t, "migrate", "start", "--from", "vast01", "--to", "vast02", "--create"); !strings.Contains(out, "2 buckets on vast01") || strings.Count(out, "ACTIVE -> MIGRATING") != 2 {
		t.Fatalf("migrate start --from: %s", out)
	}
	if _, err := rg.cli(t, "migrate", "start", "acme/data", "--from", "vast01"); err == nil {
		t.Fatal("a bucket and --from together were accepted")
	}
	dir := t.TempDir()
	out := rg.must(t, "migrate", "run", "--from", "vast01", "--until-converged", "--cursor-dir", dir, "--ledger-dir", dir)
	if strings.Count(out, "converged: the last pass copied nothing") != 2 {
		t.Fatalf("migrate run --from: %s", out)
	}
	rg.must(t, "cutover", "--from", "vast01", "--window", "0s")
	out = rg.must(t, "migrate", "finish", "--from", "vast01")
	if strings.Count(out, "CUTOVER -> ACTIVE") != 2 || !strings.Contains(out, "still pointing at vast01: tenants.acme.default_cluster") {
		t.Fatalf("migrate finish --from: %s", out)
	}
}

// A target that ignores If-None-Match: * gives the mover a guard that can lose a client write.
// migrate start and the mover both refuse it, naming the window, unless the operator accepts it.
func TestLostWriteWindowRefusals(t *testing.T) {
	rg := newAPIRig(t)
	if err := rg.vast01.CreateBucket("data"); err != nil {
		t.Fatal(err)
	}
	put(t, rg.vast01, "data", "k", "v")
	rg.addCluster(t, "vast01", rg.ep01)
	rg.addCluster(t, "garage", rg.ep02, "--conditional-write=false")
	rg.must(t, "adopt", "vast01", "acme/data")
	rg.must(t, "expand", "acme/data", "--to", "garage", "--create")
	out, err := rg.cli(t, "migrate", "start", "acme/data")
	for _, want := range []string{"conditional_write: false", "HEAD-then-commit", "docs/migrating.md", "--accept-lost-write-window"} {
		if err == nil || !strings.Contains(out, want) {
			t.Fatalf("migrate start refusal does not mention %q: %v\n%s", want, err, out)
		}
	}
	if out := rg.must(t, "migrate", "start", "acme/data", "--accept-lost-write-window"); !strings.Contains(out, "WARNING accepted with accept_lost_write_window") {
		t.Fatalf("accepted start: %s", out)
	}
	dir := t.TempDir()
	if out, err := rg.cli(t, "migrate", "run", "acme/data", "--cursor-dir", dir, "--ledger-dir", dir); err == nil || !strings.Contains(out, "--accept-lost-write-window") {
		t.Fatalf("the mover ran into a target without conditional PUT: %v %s", err, out)
	}
	if out := rg.must(t, "migrate", "run", "acme/data", "--accept-lost-write-window", "--cursor-dir", dir, "--ledger-dir", dir); !strings.Contains(out, "HEAD-then-commit guard") {
		t.Fatalf("accepted mover run: %s", out)
	}
}

func TestAPIUnreachable(t *testing.T) {
	_, _, err := run(t, "status", "--api", "http://127.0.0.1:1")
	if err == nil || !strings.Contains(err.Error(), "is shunt serve running") {
		t.Fatalf("unreachable API: %v", err)
	}
	if _, _, err := run(t, "status", "--api", "http://127.0.0.1:1", "--token-ref", "env:SHUNT_NO_SUCH_TOKEN"); err == nil || !strings.Contains(err.Error(), "--token-ref") {
		t.Fatalf("unresolvable token: %v", err)
	}
}

func readDir(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// shunt verify against a plain in-process endpoint: a clean run reports no errors and writes JSON.
func TestVerifyCommand(t *testing.T) {
	be := s3mem.New()
	if err := be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gofakes3.New(be, gofakes3.WithTimeSkewLimit(0)).Server())
	defer srv.Close()
	t.Setenv("SHUNT_TEST_CLIENT_SECRET", "s")
	report := filepath.Join(t.TempDir(), "verify.json")
	out, stderr, err := run(t, "verify", "--endpoint", srv.URL, "--bucket", "data01", "--access-key", "AK", "--secret-ref", "env:SHUNT_TEST_CLIENT_SECRET",
		"--duration", "300ms", "--keys", "10", "--workers", "2", "--interval", "0", "--json-out", report, "--cleanup")
	if err != nil || !strings.Contains(out, "errors: 0") {
		t.Fatalf("verify: %v\n%s%s", err, out, stderr)
	}
	var rep map[string]any
	if b, rerr := os.ReadFile(report); rerr != nil || json.Unmarshal(b, &rep) != nil || rep["errors"].(float64) != 0 {
		t.Fatalf("json report: %v %v", rerr, rep)
	}
	if _, _, err := run(t, "verify", "--endpoint", srv.URL); err == nil {
		t.Fatal("verify without a bucket and credentials was accepted")
	}
}

// A bare bucket name is the default tenant's bucket; tenant/bucket still names a tenant, and the
// default tenant is never shown.
func TestBucketArguments(t *testing.T) {
	for in, want := range map[string]string{"data01": "default/data01", "acme/data01": "acme/data01"} {
		if got, err := placementKey(in); err != nil || got != want {
			t.Errorf("placementKey(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "/data01", "acme/", "a/b/c"} {
		if _, err := placementKey(bad); err == nil {
			t.Errorf("placementKey(%q) accepted", bad)
		}
	}
	if shown("default/data01") != "data01" || shown("acme/data01") != "acme/data01" {
		t.Error("shown")
	}
}

// step-out prints how to take shunt out of the path when nothing blocks it, and exits non-zero,
// naming what blocks, when something does.
func TestStepOutCommand(t *testing.T) {
	rg := newAPIRig(t)
	for _, b := range []string{"data01", "logs-001"} {
		if err := rg.vast02.CreateBucket(b); err != nil {
			t.Fatal(err)
		}
	}
	rg.ctl.TenantKeys = func(string) []sigv4.Credential {
		return []sigv4.Credential{{AccessKey: "CLIENTKEY", Secret: "s", Tenant: directory.DefaultTenant}}
	}
	rg.addCluster(t, "vast02", rg.ep02)
	rg.must(t, "adopt", "vast02", "data01")

	out := rg.must(t, "step-out")
	for _, want := range []string{
		"step-out check for your clients: every bucket is on vast02 (http://" + rg.ep02 + ")",
		"ok      bucket data01: ACTIVE on vast02 under the same name, no uploads in progress",
		"ok      client key CLIENTKEY: vast02 accepts it and it reaches every bucket",
		"note    listing buckets directly as CLIENTKEY shows 1 that shunt does not: logs-001",
		"READY: clients can use vast02 directly",
		"1. Point the S3 name your clients use at vast02 (" + rg.ep02 + ")",
		"3. Stop shunt.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("step-out output lacks %q:\n%s", want, out)
		}
	}

	rg.must(t, "adopt", "vast02", "logs", "--name", "logs-001")
	out, err := rg.cli(t, "step-out")
	if !errors.Is(err, errNotReady) {
		t.Fatalf("want errNotReady, got %v\n%s", err, out)
	}
	for _, want := range []string{
		"BLOCKED bucket logs is named logs-001 on vast02: clients going direct would have to use that name",
		"1 problem blocks stepping out.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("step-out output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "READY") || strings.Contains(out, "default/") {
		t.Errorf("not ready, and the default tenant is never shown:\n%s", out)
	}
}

// Importing the keys a cluster already issued: clients keep their own credentials through shunt,
// and keep them when shunt steps out (ADR-0012).
func TestClientKeyImport(t *testing.T) {
	rg := newAPIRig(t)
	if err := rg.vast01.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	var stored []sigv4.Credential
	rg.ctl.AddKey = func(c sigv4.Credential) error { stored = append(stored, c); return nil }
	rg.ctl.TenantKeys = func(string) []sigv4.Credential { return stored }
	rg.addCluster(t, "vast01", rg.ep01)

	keys := filepath.Join(t.TempDir(), "keys.yaml")
	if err := os.WriteFile(keys, []byte("credentials:\n  - { access_key: CLUSTERKEY, secret: cluster-secret }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := rg.must(t, "adopt", "vast01", "data01", "--keys", keys)
	for _, want := range []string{"client key CLUSTERKEY imported: vast01 accepts it", "data01: ACTIVE on vast01/data01"} {
		if !strings.Contains(out, want) {
			t.Errorf("adopt --keys output lacks %q:\n%s", want, out)
		}
	}
	if len(stored) != 1 || stored[0].AccessKey != "CLUSTERKEY" || stored[0].Secret != "cluster-secret" {
		t.Fatalf("stored: %+v", stored)
	}

	// A second key, typed at the prompt, checked against the tenant's default cluster.
	out, stderr, err := runIn(t, "second-secret\n", "client", "add", "OTHERKEY", "--api", rg.url)
	if err != nil {
		t.Fatalf("client add: %v\n%s%s", err, out, stderr)
	}
	if !strings.Contains(out, "client key OTHERKEY imported: vast01 accepts it") {
		t.Errorf("client add output: %q", out)
	}
	if len(stored) != 2 || stored[1].Secret != "second-secret" {
		t.Fatalf("stored: %+v", stored)
	}
	if strings.Contains(out+stderr, "second-secret") {
		t.Error("the secret must never be printed")
	}

	// Removing the key shunt generated for itself, once the clients' own key is in.
	rg.ctl.RemoveKey = func(ak string) error {
		stored = slices.DeleteFunc(stored, func(c sigv4.Credential) bool { return c.AccessKey == ak })
		return nil
	}
	out = rg.must(t, "client", "remove", "CLUSTERKEY")
	if !strings.Contains(out, "client key CLUSTERKEY removed; 1 key(s) left") {
		t.Errorf("client remove output: %q", out)
	}

	// A keys file without secrets cannot work: shunt verifies client signatures itself.
	if err := os.WriteFile(keys, []byte("credentials:\n  - { access_key: NOSECRET, secret_ref: 'env:X' }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := rg.cli(t, "adopt", "vast01", "logs", "--keys", keys); err == nil || !strings.Contains(out, "has no secret") {
		t.Fatalf("want a clear error about the missing secret: %v\n%s", err, out)
	}
}
