package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// migrationBackend answers the three bucket-level calls the migration verbs make: HEAD to see
// whether the target exists, PUT to create it, and GET ?versioning for the refusal check.
type migrationBackend struct {
	mu       sync.Mutex
	exists   map[string]bool
	versions string // status reported by GetBucketVersioning
	puts     []string
}

func (b *migrationBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	bucket := strings.TrimPrefix(r.URL.Path, "/")
	if _, ok := r.URL.Query()["versioning"]; ok {
		if b.versions != "" {
			_, _ = w.Write([]byte(`<VersioningConfiguration><Status>` + b.versions + `</Status></VersioningConfiguration>`))
			return
		}
		_, _ = w.Write([]byte(`<VersioningConfiguration/>`))
		return
	}
	switch r.Method {
	case http.MethodHead:
		if b.exists[bucket] {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case http.MethodPut:
		b.puts = append(b.puts, bucket)
		b.exists[bucket] = true
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "unexpected "+r.Method, http.StatusBadRequest)
	}
}

func migrationRig(t *testing.T) (cfgPath, dirPath string, be *migrationBackend) {
	t.Helper()
	be = &migrationBackend{exists: map[string]bool{"acme-1111-data": true}}
	srv := httptest.NewServer(be)
	t.Cleanup(srv.Close)
	t.Setenv("SHUNT_TEST_CLUSTER_SECRET", "cluster-secret")
	dir := t.TempDir()
	dirPath = filepath.Join(dir, "directory.yaml")
	if err := os.WriteFile(dirPath, []byte("version: 1\ntenants: { acme: { default_cluster: garage } }\nplacements:\n  acme/data: { state: ACTIVE, primary: garage, names: { garage: acme-1111-data } }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ep := strings.TrimPrefix(srv.URL, "http://")
	cfgPath = filepath.Join(dir, "shunt.yaml")
	cfg := "listener: { address: \":8443\", tls: { cert: c.pem, key: k.pem } }\nauth: { mode: resign, credentials_file: creds.yaml }\n" +
		"directory: { file: " + dirPath + " }\nclusters:\n" +
		"  garage: { type: s3, scheme: http, region: garage, endpoints: [\"" + ep + "\"], credentials: { access_key: GARAGEKEY, secret_ref: env:SHUNT_TEST_CLUSTER_SECRET } }\n" +
		"  minio: { type: minio, scheme: http, region: us-east-1, endpoints: [\"" + ep + "\"], credentials: { access_key: MINIOKEY, secret_ref: env:SHUNT_TEST_CLUSTER_SECRET } }\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfgPath, dirPath, be
}

// TestMigrationVerbsWalkTheWholeMove is the operator's path from one cluster to another, in the
// order docs/DESIGN.md §2.5 prescribes, with the directory checked after every step.
func TestMigrationVerbsWalkTheWholeMove(t *testing.T) {
	cfg, dirPath, be := migrationRig(t)

	out, _, err := run(t, "ramp", "acme/data", "--to", "minio", "--create", "--ratio", "0.25", "-c", cfg)
	if err != nil {
		t.Fatalf("ramp: %v\n%s", err, out)
	}
	if !strings.Contains(out, "created acme-") || !strings.Contains(out, "ACTIVE -> RAMPING") || !strings.Contains(out, "25% of keys") {
		t.Fatalf("ramp said:\n%s", out)
	}
	if len(be.puts) != 1 {
		t.Fatalf("expected one CreateBucket on the target, got %v", be.puts)
	}
	if d := readDir(t, dirPath); !strings.Contains(d, "state: RAMPING") || !strings.Contains(d, "ratio: 0.25") {
		t.Fatalf("directory after ramp:\n%s", d)
	}

	// A ramp step raises the ratio. Repeating --to is accepted because it names the same target;
	// naming a different one is refused rather than silently redirecting the move.
	if out, _, err = run(t, "ramp", "acme/data", "--to", "minio", "--ratio", "0.5", "-c", cfg); err != nil {
		t.Fatalf("ramp step with a repeated --to: %v\n%s", err, out)
	}
	if out, _, err = run(t, "ramp", "acme/data", "--to", "garage", "--ratio", "0.75", "-c", cfg); err == nil {
		t.Fatalf("a ramp step redirected the move:\n%s", out)
	}
	if out, _, err = run(t, "ramp", "acme/data", "--ratio", "1", "-c", cfg); err != nil {
		t.Fatalf("ramp step: %v\n%s", err, out)
	}
	if out, _, err = run(t, "ramp", "acme/data", "--ratio", "0.5", "-c", cfg); err == nil {
		t.Fatalf("a shrinking ramp was accepted:\n%s", out)
	}

	if out, _, err = run(t, "migrate", "start", "acme/data", "-c", cfg); err != nil {
		t.Fatalf("migrate start: %v\n%s", err, out)
	}
	if !strings.Contains(out, "run the mover") {
		t.Fatalf("migrate start said:\n%s", out)
	}
	out, _, err = run(t, "migrate", "status", "-c", cfg)
	if err != nil || !strings.Contains(out, "MIGRATING") || !strings.Contains(out, "garage/acme-1111-data") {
		t.Fatalf("status: %v\n%s", err, out)
	}

	// Finishing before cutover is refused: the states are not skippable.
	if out, _, err = run(t, "migrate", "finish", "acme/data", "-c", cfg); err == nil {
		t.Fatalf("MIGRATING -> ACTIVE was allowed:\n%s", out)
	}
	if out, _, err = run(t, "cutover", "acme/data", "-c", cfg); err != nil {
		t.Fatalf("cutover: %v\n%s", err, out)
	}
	if out, _, err = run(t, "migrate", "finish", "acme/data", "-c", cfg); err != nil {
		t.Fatalf("finish: %v\n%s", err, out)
	}
	if !strings.Contains(out, "served entirely by minio") {
		t.Fatalf("finish said:\n%s", out)
	}
	d := readDir(t, dirPath)
	if !strings.Contains(d, "state: ACTIVE") || !strings.Contains(d, "primary: minio") || strings.Contains(d, "source:") {
		t.Fatalf("directory after finish still names a source:\n%s", d)
	}
	if strings.Contains(d, "acme-1111-data") {
		t.Fatalf("the old backend bucket is still in the placement:\n%s", d)
	}
	out, _, err = run(t, "migrate", "status", "-c", cfg)
	if err != nil || !strings.Contains(out, "no bucket is moving") {
		t.Fatalf("status after finish: %v\n%s", err, out)
	}
}

// TestMigrationVerbsRefuseUnsafeStarts covers the guards that keep data from being stranded.
func TestMigrationVerbsRefuseUnsafeStarts(t *testing.T) {
	cfg, _, be := migrationRig(t)

	// Without --create the target bucket must already exist, so a typo is not a silent 404 storm.
	out, _, err := run(t, "migrate", "start", "acme/data", "--to", "minio", "-c", cfg)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("a missing target was accepted: %v\n%s", err, out)
	}
	// Versioned buckets are out of scope for v1 and are refused before the state changes.
	be.versions = "Enabled"
	out, _, err = run(t, "migrate", "start", "acme/data", "--to", "minio", "--create", "-c", cfg)
	if err == nil || !strings.Contains(err.Error(), "versioning") {
		t.Fatalf("a versioned bucket was accepted: %v\n%s", err, out)
	}
	be.versions = ""
	if _, _, err = run(t, "ramp", "acme/data", "--to", "minio", "--create", "-c", cfg); err == nil {
		t.Fatal("a ramp with neither ratio nor prefix was accepted")
	}
	if _, _, err = run(t, "ramp", "acme/data", "--to", "minio", "--create", "--ratio", "2", "-c", cfg); err == nil {
		t.Fatal("a ratio above 1 was accepted")
	}
	if _, _, err = run(t, "cutover", "acme/nope", "-c", cfg); err == nil {
		t.Fatal("cutover of a missing placement was accepted")
	}
	if _, _, err = run(t, "cutover", "acme/data", "-c", cfg); err == nil {
		t.Fatal("cutover of an ACTIVE placement was accepted")
	}
}

// TestEvacuateACluster is the vendor-removal path: one command per step for every bucket a cluster
// still holds, repeatable after a partial failure, and nothing left pointing at it at the end.
func TestEvacuateACluster(t *testing.T) {
	cfg, dirPath, _ := migrationRig(t)
	if err := os.WriteFile(dirPath, []byte("version: 1\ntenants: { acme: { default_cluster: garage } }\nplacements:\n"+
		"  acme/data: { state: ACTIVE, primary: garage, names: { garage: acme-1111-data } }\n"+
		"  acme/logs: { state: ACTIVE, primary: garage, names: { garage: acme-2222-logs } }\n"+
		"  acme/kept: { state: ACTIVE, primary: minio, names: { minio: acme-3333-kept } }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, _, err := run(t, "migrate", "start", "--from", "garage", "--to", "minio", "--create", "-c", cfg)
	if err != nil {
		t.Fatalf("evacuate: %v\n%s", err, out)
	}
	if !strings.Contains(out, "2 buckets on garage") {
		t.Fatalf("evacuate said:\n%s", out)
	}
	if d := readDir(t, dirPath); strings.Count(d, "state: MIGRATING") != 2 {
		t.Fatalf("directory after evacuate:\n%s", d)
	}
	// The bucket that was never on garage is untouched.
	if d := readDir(t, dirPath); !strings.Contains(d, "acme/kept: {state: ACTIVE, primary: minio") && !strings.Contains(d, "primary: minio") {
		t.Fatalf("the unrelated placement moved:\n%s", d)
	}
	if out, _, err = run(t, "cutover", "--from", "garage", "-c", cfg); err != nil {
		t.Fatalf("cutover --from: %v\n%s", err, out)
	}
	if out, _, err = run(t, "migrate", "finish", "--from", "garage", "-c", cfg); err != nil {
		t.Fatalf("finish --from: %v\n%s", err, out)
	}
	d := readDir(t, dirPath)
	if strings.Contains(d, "primary: garage") || strings.Contains(d, "source: garage") || strings.Contains(d, "garage: acme-") {
		t.Fatalf("garage still holds a placement after the evacuation:\n%s", d)
	}
	// Moving every bucket is not the same as being rid of the cluster: the tenant still defaults there.
	if !strings.Contains(out, "tenants acme default there") {
		t.Fatalf("finish did not warn about the tenant default:\n%s", out)
	}
	// Repeating a finished step is a no-op, not an error: the operator can rerun after a partial failure.
	out, _, err = run(t, "migrate", "finish", "--from", "garage", "-c", cfg)
	if err != nil || !strings.Contains(out, "no bucket on garage needs this step") {
		t.Fatalf("repeat: %v\n%s", err, out)
	}
	if _, _, err = run(t, "migrate", "start", "acme/data", "--from", "garage", "-c", cfg); err == nil {
		t.Fatal("a bucket and --from together were accepted")
	}
	// Repointing the tenant is the last step of retiring a cluster, and then nothing names it.
	if out, _, err = run(t, "directory", "set-default", "acme", "minio", "-c", cfg); err != nil {
		t.Fatalf("set-default: %v\n%s", err, out)
	}
	if d := readDir(t, dirPath); strings.Contains(d, "garage") {
		t.Fatalf("garage is still named in the directory:\n%s", d)
	}
	if _, _, err = run(t, "directory", "set-default", "acme", "minio", "-c", cfg); err == nil {
		t.Fatal("a no-op set-default was accepted")
	}
	if _, _, err = run(t, "directory", "set-default", "acme", "nosuch", "-c", cfg); err == nil {
		t.Fatal("set-default accepted an unconfigured cluster")
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

// A target that ignores If-None-Match: * on PUT gives the mover a guard that can lose a client
// write. migrate start refuses it, naming the window and the doc, unless the operator accepts it
// explicitly; a target that honors the header needs no flag.
func TestMigrateStartRefusesATargetWithoutConditionalPut(t *testing.T) {
	cfg, dirPath, _ := migrationRig(t)
	raw, err := os.ReadFile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	withProfile := strings.ReplaceAll(string(raw), "secret_ref: env:SHUNT_TEST_CLUSTER_SECRET } }\n", "secret_ref: env:SHUNT_TEST_CLUSTER_SECRET }, capabilities: { conditional_write: false } }\n")
	if withProfile == string(raw) {
		t.Fatal("rig config did not change")
	}
	if err := os.WriteFile(cfg, []byte(withProfile), 0o600); err != nil {
		t.Fatal(err)
	}

	out, _, err := run(t, "migrate", "start", "acme/data", "--to", "minio", "--create", "-c", cfg)
	if err == nil {
		t.Fatalf("migrate start into a cluster without conditional PUT was accepted:\n%s", out)
	}
	for _, want := range []string{"conditional_write: false", "If-None-Match", "HEAD-then-commit", "overwritten", "docs/migrating.md", "--accept-lost-write-window"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q: %v", want, err)
		}
	}
	if b, _ := os.ReadFile(dirPath); !strings.Contains(string(b), "state: ACTIVE") || strings.Contains(string(b), "MIGRATING") {
		t.Errorf("the refusal changed the directory:\n%s", b)
	}

	// --from refuses every bucket the same way.
	if _, _, err := run(t, "migrate", "start", "--from", "garage", "--to", "minio", "--create", "-c", cfg); err == nil {
		t.Error("migrate start --from into a cluster without conditional PUT was accepted")
	}

	out, _, err = run(t, "migrate", "start", "acme/data", "--to", "minio", "--create", "--accept-lost-write-window", "-c", cfg)
	if err != nil {
		t.Fatalf("migrate start with --accept-lost-write-window: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ACTIVE -> MIGRATING") || !strings.Contains(out, "WARNING (accepted with --accept-lost-write-window)") {
		t.Errorf("accepted start did not move and warn:\n%s", out)
	}
	if b, _ := os.ReadFile(dirPath); !strings.Contains(string(b), "MIGRATING") {
		t.Errorf("directory not MIGRATING after an accepted start:\n%s", b)
	}
}

// The profile default is a conformant backend, so a cluster with no capabilities block migrates
// without the flag and without the warning.
func TestMigrateStartNeedsNoFlagForAConditionalTarget(t *testing.T) {
	cfg, _, _ := migrationRig(t)
	out, _, err := run(t, "migrate", "start", "acme/data", "--to", "minio", "--create", "-c", cfg)
	if err != nil {
		t.Fatalf("migrate start: %v\n%s", err, out)
	}
	if strings.Contains(out, "WARNING") {
		t.Errorf("warned for a target with conditional PUT:\n%s", out)
	}
}
