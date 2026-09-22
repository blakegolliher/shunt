package cp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testCluster starts n embedded members on free loopback ports, the first alone and the rest
// joining it, and stops them all when the test ends.
type testCluster struct {
	t     testing.TB
	nodes []*Node
	dirs  []string
	ports []int
}

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startCluster(t testing.TB, n int) *testCluster {
	t.Helper()
	tc := &testCluster{t: t}
	base := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	log := slog.New(slog.DiscardHandler)
	for i := range n {
		name := fmt.Sprintf("c%d", i+1)
		dir := filepath.Join(base, name)
		port := freePort(t)
		peer := fmt.Sprintf("http://127.0.0.1:%d", port)
		cfg := NodeConfig{Name: name, DataDir: dir, PeerURL: peer, Log: log}
		if i > 0 {
			initial, err := tc.nodes[0].MemberAdd(ctx, name, peer)
			if err != nil {
				t.Fatal(err)
			}
			cfg.InitialCluster, cfg.Existing = initial, true
		}
		node, err := Start(ctx, cfg)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		tc.nodes, tc.dirs, tc.ports = append(tc.nodes, node), append(tc.dirs, dir), append(tc.ports, port)
		if err := node.WaitReady(ctx); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	t.Cleanup(func() {
		for _, nd := range tc.nodes {
			if nd != nil {
				nd.Close()
			}
		}
	})
	return tc
}

// restart stops node i and starts it again on the same data directory and port.
func (tc *testCluster) restart(i int) {
	tc.t.Helper()
	tc.nodes[i].Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	name := fmt.Sprintf("c%d", i+1)
	node, err := Start(ctx, NodeConfig{Name: name, DataDir: tc.dirs[i], PeerURL: fmt.Sprintf("http://127.0.0.1:%d", tc.ports[i]), Existing: i > 0, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		tc.t.Fatalf("restart %s: %v", name, err)
	}
	tc.nodes[i] = node
}

func TestClusterFormsJoinsAndReports(t *testing.T) {
	tc := startCluster(t, 3)
	ctx := context.Background()
	st, err := tc.nodes[0].Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Members) != 3 || st.Started != 3 || st.Quorum != 2 || !st.HasQuorum || st.Leader == "" {
		t.Fatalf("status: %+v", st)
	}
	for _, nd := range tc.nodes[1:] {
		if s2, err := nd.Status(ctx); err != nil || s2.Leader != st.Leader {
			t.Errorf("%s sees leader %q, want %q (%v)", nd.Name(), s2.Leader, st.Leader, err)
		}
	}
	if _, err := tc.nodes[0].MemberAdd(ctx, "c2", "http://127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("adding a member with a taken name: %v", err)
	}

	// A member leaves; the two left keep quorum. The last member cannot be removed.
	if err := tc.nodes[0].MemberRemove(ctx, "c3"); err != nil {
		t.Fatal(err)
	}
	tc.nodes[2].Close()
	tc.nodes[2] = nil
	st, err = tc.nodes[0].Status(ctx)
	if err != nil || len(st.Members) != 2 || !st.HasQuorum {
		t.Fatalf("after removing c3: %+v %v", st, err)
	}
	if err := tc.nodes[0].MemberRemove(ctx, "c2"); err != nil {
		t.Fatal(err)
	}
	tc.nodes[1].Close()
	tc.nodes[1] = nil
	if err := tc.nodes[0].MemberRemove(ctx, "c1"); err == nil || !strings.Contains(err.Error(), "only member") {
		t.Errorf("removing the only member: %v", err)
	}
}

func TestSnapshotSaveAndRestore(t *testing.T) {
	tc := startCluster(t, 1)
	ctx := context.Background()
	cli := tc.nodes[0].Client()
	if _, err := cli.Put(ctx, "/shunt/v1/version", "7"); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(t.TempDir(), "c1.snap")
	f, err := os.Create(snap)
	if err != nil {
		t.Fatal(err)
	}
	n, err := tc.nodes[0].Snapshot(ctx, f)
	f.Close()
	if err != nil || n == 0 {
		t.Fatalf("snapshot: %d bytes, %v", n, err)
	}
	if err := tc.nodes[0].Defrag(ctx); err != nil {
		t.Fatal(err)
	}
	tc.nodes[0].Close()
	tc.nodes[0] = nil

	// Restore into a fresh directory as a one-member cluster and read the value back.
	dir := filepath.Join(t.TempDir(), "restored")
	port := freePort(t)
	peer := fmt.Sprintf("http://127.0.0.1:%d", port)
	if err := Restore(snap, dir, "r1", peer); err != nil {
		t.Fatal(err)
	}
	if err := Restore(snap, dir, "r1", peer); err == nil {
		t.Error("restored over an existing data directory")
	}
	node, err := Start(ctx, NodeConfig{Name: "r1", DataDir: dir, PeerURL: peer, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	got, err := node.Client().Get(ctx, "/shunt/v1/version")
	if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "7" {
		t.Fatalf("after restore: %v %v", got, err)
	}
}
