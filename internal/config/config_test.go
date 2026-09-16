package config

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// expectedKey reads the `# expect: <key>` header of an invalid sample.
func expectedKey(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		t.Fatalf("%s: empty", path)
	}
	line := sc.Text()
	const prefix = "# expect: "
	if !strings.HasPrefix(line, prefix) {
		t.Fatalf("%s: first line must be %q<key>, got %q", path, prefix, line)
	}
	return strings.TrimPrefix(line, prefix)
}

func TestValidSamples(t *testing.T) {
	files, err := filepath.Glob("testdata/valid/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no valid samples: %v", err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			if _, err := Load(f); err != nil {
				t.Fatalf("expected valid, got:\n%v", err)
			}
		})
	}
}

func TestInvalidSamplesNameTheKey(t *testing.T) {
	files, err := filepath.Glob("testdata/invalid/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no invalid samples: %v", err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			want := expectedKey(t, f)
			_, err := Load(f)
			if err == nil {
				t.Fatalf("expected an error naming %q, got nil", want)
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error does not name %q:\n%v", want, err)
			}
		})
	}
}

func TestDefaults(t *testing.T) {
	c, err := Load("testdata/valid/poc.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Proxy.DrainTimeout != defaultDrainTimeout {
		t.Errorf("drain_timeout default: got %v", c.Proxy.DrainTimeout)
	}
	if c.Auth.ClockSkew != defaultClockSkew {
		t.Errorf("clock_skew default: got %v", c.Auth.ClockSkew)
	}
	if c.Listener.TLS.MinVersion != "1.2" {
		t.Errorf("min_version default: got %q", c.Listener.TLS.MinVersion)
	}
	if c.Clusters["garage"].EndpointMode != "static" {
		t.Errorf("endpoint_mode default: got %q", c.Clusters["garage"].EndpointMode)
	}
	if c.Telemetry.Slow.Threshold != 500*time.Millisecond {
		t.Errorf("explicit threshold lost: got %v", c.Telemetry.Slow.Threshold)
	}
}

func TestMixedSampleShape(t *testing.T) {
	c, err := Load("testdata/valid/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(c.Clusters); got != 6 {
		t.Errorf("clusters: got %d", got)
	}
	if c.Directory.File != "internal/directory/testdata/valid/mixed.yaml" || c.Directory.PollInterval != 2*time.Second {
		t.Errorf("directory: %+v", c.Directory)
	}
	if c.Clusters["aws-use1"].EndpointMode != "dns" {
		t.Errorf("aws-use1 endpoint_mode: %q", c.Clusters["aws-use1"].EndpointMode)
	}
}

// A kill switch defaults to the normal behavior: unset means the rewriter runs.
func TestKillSwitchDefaults(t *testing.T) {
	c, err := Load("testdata/valid/poc.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.KillSwitches.XMLRewriteDisable {
		t.Error("xml_rewrite_disable must default to false, so rewriting is on unless it is switched off")
	}
	if c.Directory.PollInterval != 0 {
		t.Errorf("poll_interval defaulted without a directory file: %v", c.Directory.PollInterval)
	}
	on, err := Parse([]byte("listener: { address: \":1\", tls: { cert: c, key: k } }\nauth: { mode: passthrough }\nproxy: { cluster: x }\n" +
		"kill_switches: { xml_rewrite_disable: true }\n" +
		"clusters:\n  x: { type: s3, scheme: http, region: r, endpoints: [\"h:1\"], credentials: { access_key: a, secret_ref: env:S } }\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !on.KillSwitches.XMLRewriteDisable {
		t.Error("kill_switches.xml_rewrite_disable: true not parsed")
	}
}

func TestEmpty(t *testing.T) {
	for _, in := range []string{"", "   \n", "# only a comment\n"} {
		if _, err := Parse([]byte(in)); !errors.Is(err, errEmpty) {
			t.Errorf("Parse(%q): want ErrEmpty, got %v", in, err)
		}
	}
}

func TestAllErrorsReported(t *testing.T) {
	// A cluster missing all three required fields reports all three in one pass.
	in := []byte(`
listener: { address: ":1", tls: { cert: c, key: k } }
auth: { mode: passthrough }
clusters:
  x: { endpoints: ["h:1"], credentials: { access_key: a, secret_ref: env:S } }
`)
	_, err := Parse(in)
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"clusters.x.type", "clusters.x.scheme", "clusters.x.region"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("missing %s in:\n%v", key, err)
		}
	}
}

func TestErrorType(t *testing.T) {
	_, err := Parse([]byte("listener: { address: \":1\", tls: { cert: c, key: k } }\nauth: { mode: passthrough }\nproxy: { cluster: x }\n"))
	var ve *Error
	if !errors.As(err, &ve) {
		t.Fatalf("want *Error in chain, got %T: %v", err, err)
	}
	if ve.Key != "clusters" {
		t.Errorf("key: got %q", ve.Key)
	}
}
