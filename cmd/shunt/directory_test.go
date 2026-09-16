package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// versioningBackend answers GetBucketVersioning with a configurable status and records the buckets asked about.
type versioningBackend struct {
	mu      sync.Mutex
	status  string // "", "Enabled", "Suspended", or "403"
	buckets []string
	auth    []string
}

func (v *versioningBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := r.URL.Query()["versioning"]; !ok || r.Method != http.MethodGet {
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.String(), http.StatusBadRequest)
		return
	}
	v.buckets = append(v.buckets, strings.TrimPrefix(r.URL.Path, "/"))
	v.auth = append(v.auth, r.Header.Get("Authorization"))
	switch v.status {
	case "403":
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code></Error>`))
	case "":
		_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`))
	default:
		fmt.Fprintf(w, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>`, v.status)
	}
}

func directoryRig(t *testing.T) (cfgPath, dirPath string, be *versioningBackend) {
	t.Helper()
	be = &versioningBackend{}
	srv := httptest.NewServer(be)
	t.Cleanup(srv.Close)
	t.Setenv("SHUNT_TEST_CLUSTER_SECRET", "cluster-secret")
	dir := t.TempDir()
	dirPath = filepath.Join(dir, "directory.yaml")
	ep := strings.TrimPrefix(srv.URL, "http://")
	directoryBody := "version: 1\nclusters:\n" +
		"  garage: { type: s3, scheme: http, region: garage, endpoints: [\"" + ep + "\"], credentials: { access_key: GARAGEKEY, secret_ref: env:SHUNT_TEST_CLUSTER_SECRET } }\n" +
		"  minio: { type: minio, scheme: http, region: us-east-1, endpoints: [\"" + ep + "\"], credentials: { access_key: MINIOKEY, secret_ref: env:SHUNT_TEST_CLUSTER_SECRET } }\n" +
		"tenants: { acme: { default_cluster: garage } }\nplacements:\n  acme/data: { state: ACTIVE, primary: garage, names: { garage: acme-1111-data } }\n"
	if err := os.WriteFile(dirPath, []byte(directoryBody), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath = filepath.Join(dir, "shunt.yaml")
	cfg := "listener: { address: \":8443\", tls: { cert: c.pem, key: k.pem } }\nauth: { mode: resign, credentials_file: creds.yaml }\n" +
		"directory: { file: " + dirPath + " }\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfgPath, dirPath, be
}

func TestDirectoryGetAndValidate(t *testing.T) {
	cfg, _, _ := directoryRig(t)
	out, _, err := run(t, "directory", "get", "-c", cfg)
	if err != nil || !strings.Contains(out, "acme/data:") || !strings.Contains(out, "version: 1") {
		t.Fatalf("get: %v\n%s", err, out)
	}
	out, _, err = run(t, "directory", "get", "acme/data", "--json", "-c", cfg)
	if err != nil || !strings.Contains(out, `"state": "ACTIVE"`) || !strings.Contains(out, `"garage": "acme-1111-data"`) {
		t.Fatalf("get one: %v\n%s", err, out)
	}
	if _, _, err = run(t, "directory", "get", "acme/nope", "-c", cfg); err == nil {
		t.Fatal("get of a missing placement succeeded")
	}
	out, _, err = run(t, "directory", "validate", "-c", cfg)
	if err != nil || !strings.Contains(out, "ok (version 1, 1 tenants, 1 placements)") {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	bad := filepath.Join(t.TempDir(), "bad.yaml")
	_ = os.WriteFile(bad, []byte("version: 1\ntenants: { acme: { default_cluster: nowhere } }\n"), 0o644)
	if _, stderr, err := run(t, "directory", "validate", "--file", bad, "-c", cfg); err == nil || !strings.Contains(stderr, "tenants.acme.default_cluster") {
		t.Fatalf("validate bad file: %v\n%s", err, stderr)
	}
}

func TestDirectorySetStateRefusesIllegalTransitions(t *testing.T) {
	cfg, dirPath, be := directoryRig(t)
	before, _ := os.ReadFile(dirPath)
	for _, to := range []string{"CUTOVER", "ACTIVE"} {
		_, _, err := run(t, "directory", "set-state", "acme/data", to, "-c", cfg)
		if err == nil || !strings.Contains(err.Error(), "ACTIVE -> "+to) {
			t.Fatalf("ACTIVE -> %s: %v", to, err)
		}
	}
	if _, _, err := run(t, "directory", "set-state", "acme/data", "MIGRATING", "-c", cfg); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("leaving ACTIVE without --to: %v", err)
	}
	after, _ := os.ReadFile(dirPath)
	if string(before) != string(after) || len(be.buckets) != 0 {
		t.Fatalf("refused transitions touched the file or the backend (%d requests)", len(be.buckets))
	}
}

func TestDirectorySetStateVersioningRefusal(t *testing.T) {
	cfg, dirPath, be := directoryRig(t)
	before, _ := os.ReadFile(dirPath)
	for _, status := range []string{"Enabled", "Suspended", "403"} {
		be.status = status
		_, _, err := run(t, "directory", "set-state", "acme/data", "MIGRATING", "--to", "minio", "--name", "acme-2222-data", "-c", cfg)
		if err == nil || !strings.Contains(err.Error(), "refused") {
			t.Fatalf("versioning %s: %v", status, err)
		}
		if status != "403" && !strings.Contains(err.Error(), status) {
			t.Fatalf("refusal does not name the status %s: %v", status, err)
		}
		if after, _ := os.ReadFile(dirPath); string(after) != string(before) {
			t.Fatalf("versioning %s: directory file changed despite the refusal", status)
		}
	}
	be.status = ""
	be.buckets, be.auth = nil, nil
	out, _, err := run(t, "directory", "set-state", "acme/data", "RAMPING", "--to", "minio", "--name", "acme-2222-data", "--prefix", "2026-09/", "--actor", "test", "-c", cfg)
	if err != nil || !strings.Contains(out, "ACTIVE -> RAMPING (directory version 2)") {
		t.Fatalf("unversioned ramp: %v\n%s", err, out)
	}
	if strings.Join(be.buckets, ",") != "acme-1111-data,acme-2222-data" {
		t.Fatalf("versioning checked on %v, want source and primary", be.buckets)
	}
	if !strings.Contains(be.auth[0], "Credential=GARAGEKEY/") || !strings.Contains(be.auth[0], "/garage/s3/") || !strings.Contains(be.auth[1], "Credential=MINIOKEY/") {
		t.Fatalf("versioning checks not signed with each cluster's key and region: %v", be.auth)
	}
	out, _, err = run(t, "directory", "set-state", "acme/data", "MIGRATING", "-c", cfg)
	if err != nil || !strings.Contains(out, "RAMPING -> MIGRATING") {
		t.Fatalf("ramp to migrating: %v\n%s", err, out)
	}
	out, _, _ = run(t, "directory", "get", "acme/data", "-c", cfg)
	if !strings.Contains(out, "state: MIGRATING") || !strings.Contains(out, "source: garage") || !strings.Contains(out, "primary: minio") {
		t.Fatalf("placement after transitions:\n%s", out)
	}
	log, err := os.ReadFile(dirPath + ".changes.jsonl")
	if err != nil || strings.Count(string(log), "\n") != 2 || !strings.Contains(string(log), `"actor":"test"`) {
		t.Fatalf("change log: %v\n%s", err, log)
	}
}
