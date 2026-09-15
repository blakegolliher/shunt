package main

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testdata = "../../internal/config/testdata"

func run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errb bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(&errb)
	root.SetArgs(args)
	err = root.Execute()
	return out.String(), errb.String(), err
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
