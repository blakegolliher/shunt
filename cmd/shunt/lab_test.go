package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/auth"
)

// runIn is run with stdin.
func runIn(t *testing.T, stdin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errb bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(&errb)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(args)
	err = root.Execute()
	return out.String(), errb.String(), err
}

func TestParseClusterURL(t *testing.T) {
	for in, want := range map[string][2]string{
		"http://10.0.1.10":                {"http", "10.0.1.10:80"},
		"https://s3.example.net":          {"https", "s3.example.net:443"},
		"http://s3.vast01.example.com:80": {"http", "s3.vast01.example.com:80"},
		"http://[fd00::1]:9000/":          {"http", "[fd00::1]:9000"},
	} {
		scheme, hp, err := parseClusterURL(in)
		if err != nil || scheme != want[0] || hp != want[1] {
			t.Errorf("parseClusterURL(%q) = %q %q %v, want %v", in, scheme, hp, err, want)
		}
	}
	for _, bad := range []string{"10.0.1.10:80", "ftp://h", "http://", "http://h/bucket", "http://u:p@h"} {
		if _, _, err := parseClusterURL(bad); err == nil {
			t.Errorf("parseClusterURL(%q) accepted", bad)
		}
	}
}

// cluster add from a URL, with the secret read from stdin: shunt stores it and infers the rest.
func TestClusterAddFromURL(t *testing.T) {
	rg := newAPIRig(t)
	rg.ctl.SecretsDir = filepath.Join(t.TempDir(), "secrets")
	out, stderr, err := runIn(t, "cluster-secret\n", "cluster", "add", "vast01", "http://"+rg.ep01, "--access-key", "AK", "--api", rg.url)
	if err != nil || !strings.Contains(out, "cluster vast01: aws http://"+rg.ep01+" region us-east-1") {
		// gofakes3 answers as AmazonS3, so the inferred type is aws.
		t.Fatalf("cluster add: %v\n%s%s", err, out, stderr)
	}
	c, _ := rg.dir.Snapshot().Cluster("vast01")
	ref := strings.TrimPrefix(c.Credentials.SecretRef, "file:")
	if b, rerr := os.ReadFile(ref); rerr != nil || string(b) != "cluster-secret" || !strings.HasPrefix(ref, rg.ctl.SecretsDir) {
		t.Fatalf("stored secret %q at %q: %v", b, ref, rerr)
	}
	if _, _, err := runIn(t, "", "cluster", "add", "vast02", "http://"+rg.ep02, "--access-key", "AK", "--api", rg.url); err == nil || !strings.Contains(err.Error(), "no secret key given") {
		t.Fatalf("an empty secret: %v", err)
	}
	if _, _, err := runIn(t, "s\n", "cluster", "add", "vast02", "--access-key", "AK", "--api", rg.url); err == nil || !strings.Contains(err.Error(), "give the cluster's URL") {
		t.Fatalf("no URL: %v", err)
	}
	if _, _, err := runIn(t, "s\n", "cluster", "add", "vast02", "http://"+rg.ep02, "--api", rg.url); err == nil || !strings.Contains(err.Error(), "--access-key is required") {
		t.Fatalf("no access key: %v", err)
	}
	// The bare bucket name is the default tenant's bucket.
	if err := rg.vast01.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	if out := rg.must(t, "adopt", "vast01", "data01"); !strings.Contains(out, "data01: ACTIVE on vast01/data01") {
		t.Fatalf("adopt: %s", out)
	}
	if p, ok := rg.dir.Snapshot().Lookup("default", "data01"); !ok || p.Primary != "vast01" {
		t.Fatalf("placement: %+v %v", p, ok)
	}
}

// serve --plaintext with no config builds its state directory once, with a client key the proxy
// accepts and `shunt client show` prints; later starts reuse it.
func TestLabStateDirectory(t *testing.T) {
	state := filepath.Join(t.TempDir(), "shunt-data")
	var notes bytes.Buffer
	cfg, err := labConfig(state, "127.0.0.1:18008", "127.0.0.1:19900", &notes)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Listener.Plaintext || cfg.Auth.Mode != "resign" || cfg.Directory.SecretsDir != filepath.Join(state, "secrets") || cfg.Admin.Address != "127.0.0.1:19900" {
		t.Fatalf("config: %+v", cfg)
	}
	for _, f := range []string{"directory.yaml", "credentials.yaml", "secrets"} {
		st, serr := os.Stat(filepath.Join(state, f))
		if serr != nil || st.Mode().Perm()&0o077 != 0 {
			t.Fatalf("%s: %v %v", f, st, serr)
		}
	}
	store, err := auth.Load(cfg.Auth.CredentialsFile)
	if err != nil {
		t.Fatal(err)
	}
	out, _, err := run(t, "client", "show", "--state-dir", state)
	if err != nil {
		t.Fatal(err)
	}
	var ak, secret string
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, "access_key="); ok {
			ak = v
		}
		if v, ok := strings.CutPrefix(line, "secret="); ok {
			secret = v
		}
	}
	c, err := store.Lookup(context.Background(), ak)
	if err != nil || c.Secret != secret || c.Tenant != "default" || !strings.Contains(out, "tenant=default") {
		t.Fatalf("client show %q against the store: %+v %v", out, c, err)
	}
	if strings.Contains(notes.String(), secret) || !strings.Contains(notes.String(), ak) {
		t.Fatalf("first-start note must name the key and never print the secret: %q", notes.String())
	}
	notes.Reset()
	if _, err := labConfig(state, "127.0.0.1:18008", "127.0.0.1:19900", &notes); err != nil || notes.Len() != 0 {
		t.Fatalf("a second start must reuse the state quietly: %v %q", err, notes.String())
	}
	if again, _ := auth.Load(cfg.Auth.CredentialsFile); again == nil {
		t.Fatal("credentials unreadable after a second start")
	} else if c2, _ := again.Lookup(context.Background(), ak); c2.AccessKey != ak || c2.Secret != secret || c2.Tenant != "default" {
		t.Fatalf("the client key changed on a second start: %+v", c2)
	}
}

func TestServeFlagRules(t *testing.T) {
	if _, _, err := run(t, "serve"); err == nil || !strings.Contains(err.Error(), "--plaintext for a lab") {
		t.Fatalf("serve with nothing: %v", err)
	}
	if _, _, err := run(t, "serve", "-c", "shunt.yaml", "--plaintext"); err == nil || !strings.Contains(err.Error(), "--plaintext applies only without --config") {
		t.Fatalf("serve -c with --plaintext: %v", err)
	}
	if _, _, err := run(t, "client", "show", "--state-dir", filepath.Join(t.TempDir(), "none")); err == nil || !strings.Contains(err.Error(), "shunt serve --plaintext") {
		t.Fatalf("client show without state: %v", err)
	}
}

func TestShownTextHidesTheDefaultTenant(t *testing.T) {
	for in, want := range map[string]string{
		`cluster "a" is still referenced by tenants.default.default_cluster, placements.default/data01`: `cluster "a" is still referenced by the default cluster for new buckets, bucket data01`,
		"tenants.acme.default_cluster, placements.acme/data01":                                          "tenants.acme.default_cluster, placements.acme/data01",
		"default/data01 is MIGRATING; purge-source runs on a placement in CUTOVER":                      "data01 is MIGRATING; purge-source runs on a placement in CUTOVER",
		"no mover has reported on default/data01 to this proxy; run `shunt migrate run default/data01`": "no mover has reported on data01 to this proxy; run `shunt migrate run data01`",
	} {
		if got := shownText(in); got != want {
			t.Errorf("shownText(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}
