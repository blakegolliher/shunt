// Package cptest starts embedded etcd members for tests in other packages (the fleet property
// test in internal/proxy). It is imported by test files only.
package cptest

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/cp"
)

// StartNode runs a one-member cluster on a free loopback port in a temp directory, stopped when
// the test ends.
func StartNode(t testing.TB) *cp.Node {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	node, err := cp.Start(ctx, cp.NodeConfig{Name: "t1", DataDir: t.TempDir(), PeerURL: fmt.Sprintf("http://127.0.0.1:%d", port), Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Close)
	return node
}
