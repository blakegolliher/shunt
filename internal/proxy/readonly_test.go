package proxy

import (
	"context"
	"net/http"
	"testing"
)

func TestReadOnlyPlacementAndClusterRefuseWrites(t *testing.T) {
	m := newMixedRig(t, nil)
	ctx := context.Background()
	if err := m.dir.SetPlacementReadOnly(ctx, "acme", "data", true, false, "operator"); err != nil {
		t.Fatal(err)
	}
	if r := m.acme(t, http.MethodPut, "/data/k", []byte("no")); r.StatusCode != http.StatusServiceUnavailable || r.Header.Get("Retry-After") != "1" {
		t.Fatalf("retryable placement read-only: %d retry=%q %s", r.StatusCode, r.Header.Get("Retry-After"), r.body)
	}
	if r := m.acme(t, http.MethodGet, "/data/k", nil); r.StatusCode == http.StatusServiceUnavailable {
		t.Fatalf("read-only blocked a read: %d %s", r.StatusCode, r.body)
	}
	if err := m.dir.SetPlacementReadOnly(ctx, "acme", "data", false, false, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := m.dir.SetClusterReadOnly(ctx, "garage", true, true, "operator"); err != nil {
		t.Fatal(err)
	}
	if r := m.acme(t, http.MethodDelete, "/data/k", nil); r.StatusCode != http.StatusForbidden || r.Header.Get("Retry-After") != "" {
		t.Fatalf("fail-fast cluster read-only: %d retry=%q %s", r.StatusCode, r.Header.Get("Retry-After"), r.body)
	}
}
