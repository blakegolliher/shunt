package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveSecret(t *testing.T) {
	t.Setenv("SHUNT_TEST_SECRET", "s3cr3t")
	dir := t.TempDir()
	f := filepath.Join(dir, "sec")
	if err := os.WriteFile(f, []byte("fromfile\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	_ = os.WriteFile(empty, nil, 0o600)
	cases := []struct {
		ref, want string
		ok        bool
	}{
		{"env:SHUNT_TEST_SECRET", "s3cr3t", true},
		{"file:" + f, "fromfile", true},
		{"env:SHUNT_TEST_UNSET_XYZ", "", false},
		{"file:" + empty, "", false},
		{"file:/nonexistent/x", "", false},
		{"hunter2", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		got, err := ResolveSecret(c.ref)
		if c.ok && (err != nil || got != c.want) {
			t.Errorf("%q: got %q %v", c.ref, got, err)
		}
		if !c.ok && (err == nil || !errors.Is(err, ErrSecretRef)) {
			t.Errorf("%q: expected ErrSecretRef, got %v", c.ref, err)
		}
	}
}

func TestCapabilitiesDefaults(t *testing.T) {
	var c Capabilities
	if !c.EnforcesSHA256Or(true) || !c.UnsignedTrailerOr(true) {
		t.Fatal("unset capabilities must take the default")
	}
	f := false
	c.EnforcesSHA256 = &f
	if c.EnforcesSHA256Or(true) {
		t.Fatal("explicit false ignored")
	}
}
