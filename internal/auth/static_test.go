package auth

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/sigv4"
)

const good = `
credentials:
  - access_key: SHUNTAKONE
    secret: one-secret
    tenant: acme
    buckets: [data]
  - access_key: SHUNTAKTWO
    secret_ref: env:SHUNT_TEST_TWO
    tenant: zed
`

func TestParseAndLookup(t *testing.T) {
	t.Setenv("SHUNT_TEST_TWO", "two-secret")
	s, err := parse([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if s.Len() != 2 {
		t.Fatalf("len %d", s.Len())
	}
	c, err := s.Lookup(context.Background(), "SHUNTAKTWO")
	if err != nil || c.Secret != "two-secret" || c.Tenant != "zed" {
		t.Fatalf("%+v %v", c, err)
	}
	c, _ = s.Lookup(context.Background(), "SHUNTAKONE")
	if len(c.Buckets) != 1 || c.Buckets[0] != "data" {
		t.Fatalf("buckets %v", c.Buckets)
	}
	if _, err := s.Lookup(context.Background(), "nope"); !errors.Is(err, sigv4.ErrUnknownAccessKey) {
		t.Fatalf("want ErrUnknownAccessKey, got %v", err)
	}
	if strings.Contains(fmt.Sprintf("%+v", c), "one-secret") {
		t.Fatal("secret printed")
	}
}

// A key that names no tenant belongs to the default tenant (ADR-0010).
func TestNoTenantIsTheDefaultTenant(t *testing.T) {
	s, err := parse([]byte("credentials:\n  - access_key: a\n    secret: s\n"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Lookup(context.Background(), "a")
	if err != nil || c.Tenant != "default" {
		t.Fatalf("tenant %q, err %v", c.Tenant, err)
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]string{
		"unknown key":   "credentials:\n  - access_key: a\n    secret: s\n    tenant: t\n    password: x\n",
		"no access key": "credentials:\n  - secret: s\n    tenant: t\n",
		"no secret":     "credentials:\n  - access_key: a\n    tenant: t\n",
		"both secrets":  "credentials:\n  - access_key: a\n    secret: s\n    secret_ref: env:X\n    tenant: t\n",
		"duplicate":     "credentials:\n  - {access_key: a, secret: s, tenant: t}\n  - {access_key: a, secret: s2, tenant: t}\n",
		"bad ref":       "credentials:\n  - {access_key: a, secret_ref: env:SHUNT_UNSET_ZZZ, tenant: t}\n",
		"not yaml":      "credentials: [",
	}
	for name, in := range cases {
		if _, err := parse([]byte(in)); err == nil {
			t.Errorf("%s: expected error", name)
		} else if strings.Contains(err.Error(), "s2") {
			t.Errorf("%s: error leaks a secret: %v", name, err)
		}
	}
}

func TestLoadPermissions(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "creds.yaml")
	if err := os.WriteFile(p, []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SHUNT_TEST_TWO", "x")
	if _, err := Load(p); !errors.Is(err, errPermissions) {
		t.Fatalf("0644 accepted: %v", err)
	}
	if err := os.Chmod(p, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("missing file accepted")
	}
}

func BenchmarkLookup(b *testing.B) {
	s, _ := parse([]byte("credentials:\n  - {access_key: a, secret: s, tenant: t}\n"))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.Lookup(context.Background(), "a"); err != nil {
			b.Fatal(err)
		}
	}
}
