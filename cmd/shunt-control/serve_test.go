package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/admin"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/cp"
	"github.com/blakegolliher/shunt/internal/cp/cptest"
	"github.com/blakegolliher/shunt/internal/telemetry"
)

// The control plane's own routes as shunt-control mounts them: the bare /v1/control (ADR-0017),
// its trailing-slash form and /v1/control/status all answer the status, with the join line for a
// new node rendered from this node's flags.
func TestControlRouteBare(t *testing.T) {
	node := cptest.StartNode(t)
	c, err := cp.NewCipher(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	store := cp.New(node.Client(), c, log)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	metrics := telemetry.NewMetrics()
	fleet := cp.NewFleet(node.Client(), 3*time.Second)
	ctl := &control.Server{Dir: store, Fleet: fleet, Keys: store, Log: log, Metrics: metrics}
	o := &nodeOptions{name: "t1", api: "127.0.0.1:9901", tokenRef: "file:/etc/shunt/control.token", plaintext: true, leaseTTL: 3 * time.Second}
	api := &cp.API{Node: node, Store: store, Fleet: fleet, Cipher: c, Version: "test", Join: joinLine(o)}
	adm := admin.New(metrics.Registry, telemetry.NewSlowRing(1, time.Hour))
	mountControl(adm, ctl, api, store)
	srv := httptest.NewServer(adm)
	t.Cleanup(srv.Close)

	want := "shunt-control join --name <name> --data-dir <data-dir> --peer-url http://<host>:2380 --api <host>:9901 " +
		"--token-ref file:/etc/shunt/control.token --plaintext --lease-ttl 3s --existing http://127.0.0.1:9901"
	if got := joinLine(o); got != want {
		t.Fatalf("join line:\n got %s\nwant %s", got, want)
	}
	if got := joinLine(&nodeOptions{api: "127.0.0.1:9901", leaseTTL: 10 * time.Second}); strings.Contains(got, "--token-ref") || strings.Contains(got, "--plaintext") || strings.Contains(got, "--lease-ttl") {
		t.Fatalf("a loopback lab node's join line carries flags it does not run with: %s", got)
	}
	for _, path := range []string{"/v1/control", "/v1/control/", "/v1/control/status"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var ans cp.StatusAnswer
		derr := json.NewDecoder(resp.Body).Decode(&ans)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || derr != nil {
			t.Fatalf("GET %s: HTTP %d (%v)", path, resp.StatusCode, derr)
		}
		if ans.Join != want || ans.LastCompaction != nil || ans.Compaction.Mode != "periodic" || ans.Compaction.Retention != "24h" || ans.Node != "t1" || !ans.DirectoryLoaded {
			t.Fatalf("GET %s: %+v", path, ans)
		}
	}
	resp, err := http.Get(srv.URL + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/status through the mounts: HTTP %d", resp.StatusCode)
	}
}
