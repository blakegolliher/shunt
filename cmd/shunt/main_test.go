package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Tests run from the repository root so config samples resolve their relative paths (the
// directory file, certificates) the way `shunt` does when started from a checkout.
const testdata = "internal/config/testdata"

func TestMain(m *testing.M) {
	if err := os.Chdir("../.."); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestCheckConfigInvalidDirectoryNamesKey(t *testing.T) {
	dir := t.TempDir()
	dfile := filepath.Join(dir, "directory.yaml")
	cfile := filepath.Join(dir, "shunt.yaml")
	if err := os.WriteFile(dfile, []byte("version: 1\nclusters:\n  garage: { type: s3, scheme: http, region: garage, endpoints: [\"127.0.0.1:3900\"], credentials: { access_key: GK, secret_ref: env:S } }\n"+
		"tenants: { acme: { default_cluster: garage } }\nplacements:\n  acme/data: { state: MOVING, primary: garage, names: { garage: data } }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := "listener: { address: \":8443\", tls: { cert: c.pem, key: k.pem } }\nauth: { mode: resign, credentials_file: creds.yaml }\ndirectory: { file: " + dfile + " }\n"
	if err := os.WriteFile(cfile, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	_, stderr, err := run(t, "check-config", cfile)
	if err == nil || !strings.Contains(stderr, "placements.acme/data.state") {
		t.Fatalf("err=%v stderr=%s", err, stderr)
	}
	if err := os.Remove(dfile); err != nil {
		t.Fatal(err)
	}
	if _, stderr, err = run(t, "check-config", cfile); err == nil || !strings.Contains(stderr, "no such file") {
		t.Fatalf("missing directory file accepted: err=%v stderr=%s", err, stderr)
	}
}

func run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	return runIn(t, "", args...)
}

func TestVersion(t *testing.T) {
	out, _, err := run(t, "version")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "shunt dev (commit unknown") {
		t.Errorf("unexpected output: %q", out)
	}
}

func TestCheckConfigValid(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join(testdata, "valid", "*.yaml"))
	if len(files) == 0 {
		t.Fatal("no valid samples")
	}
	for _, f := range files {
		out, _, err := run(t, "check-config", f)
		if err != nil {
			t.Errorf("%s: %v", f, err)
		}
		if !strings.Contains(out, ": ok (") {
			t.Errorf("%s: unexpected stdout %q", f, out)
		}
	}
}

// Every invalid sample must fail and the stderr must name the key from the sample's `# expect:` line.
func TestCheckConfigInvalidNamesKey(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join(testdata, "invalid", "*.yaml"))
	if len(files) == 0 {
		t.Fatal("no invalid samples")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			want := firstLineExpect(t, f)
			out, stderr, err := run(t, "check-config", f)
			if err == nil {
				t.Fatalf("expected failure; stdout=%q", out)
			}
			if !strings.Contains(stderr, want) {
				t.Fatalf("stderr does not name %q:\n%s", want, stderr)
			}
		})
	}
}

func TestCheckConfigMissingFile(t *testing.T) {
	if _, _, err := run(t, "check-config", "/nonexistent/shunt.yaml"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCheckConfigArgCount(t *testing.T) {
	if _, _, err := run(t, "check-config"); err == nil {
		t.Fatal("expected usage error")
	}
}

func firstLineExpect(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	sc := bufio.NewScanner(f)
	sc.Scan()
	return strings.TrimPrefix(sc.Text(), "# expect: ")
}
