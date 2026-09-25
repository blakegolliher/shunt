package cp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.uber.org/zap"

	"github.com/blakegolliher/shunt/internal/control"
)

// The embedded etcd member (ADR-0015). shunt-control runs one per control node. There is no
// network client port: shunt-control talks to its own member in process, and a joining node asks
// an existing node's control API to add it. Only the peer port is open.

// NodeConfig is what starts a control node's etcd member.
type NodeConfig struct {
	Name    string // this member's name, unique in the cluster
	DataDir string // etcd's data plus the data-encryption key; local disk, since Raft fsyncs every write
	PeerURL string // this member's peer URL, http://host:2380, reachable from the other control nodes
	// InitialCluster is "name=peer-url,..." for every member at the time this one starts: this one
	// alone for the first node, the whole cluster for a joining node (from the join answer). A
	// member that has started before ignores it.
	InitialCluster string
	// Existing: joining a running cluster, rather than forming a new one.
	Existing bool
	Log      *slog.Logger
}

// Node is a running etcd member with an in-process client.
type Node struct {
	cfg    NodeConfig
	e      *embed.Etcd
	cli    *clientv3.Client
	socket string // the client listener's unix:// URL
	rmSock func()
	// sock is a client over the member's unix socket, opened on first use: the in-process client
	// has no backend for maintenance calls that touch the database file (defragment).
	sock *clientv3.Client
}

// socketClient returns the unix-socket client.
func (n *Node) socketClient() (*clientv3.Client, error) {
	if n.sock != nil {
		return n.sock, nil
	}
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{n.socket}, DialTimeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("etcd client socket: %w", err)
	}
	n.sock = c
	return c, nil
}

// clientSocket is the member's client listener: a unix socket in a 0700 directory, which no other
// host and no other local user can reach. The in-process client does not even use it; embed
// requires one, and defragment goes through it. A unix socket path is limited to about 100 bytes,
// so a deep data directory gets a short private directory under the temp dir instead.
func clientSocket(dataDir string) (path string, cleanup func(), err error) {
	p := filepath.Join(dataDir, "client.sock")
	if len(p) < 90 {
		return "unix://" + p, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "shunt-control-")
	if err != nil {
		return "", nil, err
	}
	return "unix://" + filepath.Join(dir, "client.sock"), func() { _ = os.RemoveAll(dir) }, nil
}

// Start runs the member and waits until it has joined a quorum, or ctx ends.
func Start(ctx context.Context, cfg NodeConfig) (*Node, error) {
	if cfg.Name == "" || cfg.DataDir == "" || cfg.PeerURL == "" {
		return nil, errors.New("shunt-control: name, data dir and peer URL are required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	pu, err := url.Parse(cfg.PeerURL)
	if err != nil || pu.Host == "" || (pu.Scheme != "http" && pu.Scheme != "https") {
		return nil, fmt.Errorf("peer URL %q: want http://host:port", cfg.PeerURL)
	}
	_, err = os.Stat(filepath.Join(cfg.DataDir, "member"))
	restart := err == nil // the data directory already holds a member
	socket, rmSock, err := clientSocket(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	cu, _ := url.Parse(socket)
	ec := embed.NewConfig()
	ec.Name, ec.Dir = cfg.Name, cfg.DataDir
	ec.ListenPeerUrls, ec.AdvertisePeerUrls = []url.URL{*pu}, []url.URL{*pu}
	ec.ListenClientUrls, ec.AdvertiseClientUrls = []url.URL{*cu}, []url.URL{*cu}
	ec.InitialCluster = cfg.InitialCluster
	if ec.InitialCluster == "" {
		ec.InitialCluster = cfg.Name + "=" + cfg.PeerURL
	}
	ec.ClusterState = embed.ClusterStateFlagNew
	if cfg.Existing {
		ec.ClusterState = embed.ClusterStateFlagExisting
	}
	ec.AutoCompactionMode, ec.AutoCompactionRetention = "periodic", "24h" // §12.7
	ec.LogLevel = "warn"
	ec.LogOutputs = []string{"stderr"}
	// A member joining right after it was added can find the cluster's member list not yet
	// settled ("incompatible with current running cluster"); it is a matter of seconds. It joins as
	// a learner (AddLearner), so the members it asks keep their quorum and can answer it.
	var e *embed.Etcd
	deadline := time.Now().Add(20 * time.Second)
	for {
		e, err = embed.StartEtcd(ec)
		if err == nil || !cfg.Existing || !strings.Contains(err.Error(), "incompatible with current running cluster") || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			rmSock()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err != nil {
		rmSock()
		return nil, fmt.Errorf("etcd: %w", err)
	}
	// A member restarting without a quorum around it (the first node back after an outage) is
	// not "ready" until a leader exists, which needs the others. The node still has to run, so
	// an operator can see it waiting: for a restart, readiness is bounded here and watched by
	// WaitReady. A new cluster or a fresh join is ready on its own and waits for it.
	bound := make(<-chan time.Time)
	if restart {
		bound = time.After(5 * time.Second)
	}
	select {
	case <-e.Server.ReadyNotify():
	case err := <-e.Err():
		e.Close()
		rmSock()
		return nil, fmt.Errorf("etcd: %w", err)
	case <-ctx.Done():
		e.Close()
		rmSock()
		return nil, ctx.Err()
	case <-bound:
		if cfg.Log != nil {
			cfg.Log.Warn("etcd member started but has no quorum yet: waiting for the other control nodes", "name", cfg.Name)
		}
	}
	n := &Node{cfg: cfg, e: e, cli: v3client.New(e.Server), socket: socket, rmSock: rmSock}
	if cfg.Existing {
		if err := n.promote(ctx); err != nil {
			if !restart {
				n.Close()
				return nil, err
			}
			// A learner restarting (it stopped between its join and its promotion) may find no
			// leader to promote it yet; it runs as a learner, and its next start tries again.
			if cfg.Log != nil {
				cfg.Log.Warn("etcd member is still a learner: not promoted to a voting member yet", "name", cfg.Name, "err", err)
			}
		}
	}
	if cfg.Log != nil {
		cfg.Log.Info("etcd member ready", "name", cfg.Name, "peer", cfg.PeerURL, "data_dir", cfg.DataDir, "members", len(e.Server.Cluster().Members()))
	}
	return n, nil
}

// Client is the in-process etcd client.
func (n *Node) Client() *clientv3.Client { return n.cli }

// Compaction is the member's automatic compaction: its mode and retention (§12.7).
func (n *Node) Compaction() (mode, retention string) {
	c := n.e.Config()
	return c.AutoCompactionMode, c.AutoCompactionRetention
}

// Name is the member's name.
func (n *Node) Name() string { return n.cfg.Name }

// Close stops the member. A control node that stops leaves the cluster's quorum as it was.
func (n *Node) Close() {
	if n.sock != nil {
		_ = n.sock.Close()
	}
	n.e.Close()
	n.rmSock()
}

// Err reports the member's fatal errors, if any.
func (n *Node) Err() <-chan error { return n.e.Err() }

// ListMembers is the member list, read linearizably: a membership change needs quorum, and a join
// reconciles against it (ADR-0021 D4).
func (n *Node) ListMembers(ctx context.Context) ([]control.EtcdMember, error) {
	ml, err := n.cli.MemberList(ctx)
	if err != nil {
		return nil, fmt.Errorf("etcd member list: %w", err)
	}
	out := make([]control.EtcdMember, 0, len(ml.Members))
	for _, m := range ml.Members {
		out = append(out, control.EtcdMember{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, Learner: m.IsLearner})
	}
	return out, nil
}

// AddLearner adds a non-voting member at peerURL and returns its ID. A learner does not vote, so
// adding it leaves the quorum as it was: added as a voter, a second member would make the first
// alone short of quorum, unable to answer the new member's own startup checks. The member promotes
// itself once it has caught up (promote). etcd refuses a second member with the same peer URL, which
// is what lets a join retry an add whose answer was lost.
func (n *Node) AddLearner(ctx context.Context, peerURL string) (uint64, error) {
	// etcd refuses a membership change while a member it already has is not yet caught up
	// ("unhealthy cluster"), which is the normal state for a few seconds after the previous join.
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := n.cli.MemberAddAsLearner(ctx, []string{peerURL})
		if err == nil {
			return resp.Member.ID, nil
		}
		if !strings.Contains(err.Error(), "unhealthy cluster") || time.Now().After(deadline) {
			return 0, fmt.Errorf("etcd member add: %w", err)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// MemberInfo is one control node as `shunt-control member list` shows it.
type MemberInfo struct {
	Name     string   `json:"name"`
	ID       string   `json:"id"`
	PeerURLs []string `json:"peer_urls"`
	Leader   bool     `json:"leader"`
	Learner  bool     `json:"learner,omitempty"`
	Started  bool     `json:"started"` // false: added but not yet running
}

// ClusterStatus is the answer to `shunt-control status`.
type ClusterStatus struct {
	Members   []MemberInfo `json:"members"`
	Quorum    int          `json:"quorum"` // members needed for writes
	Started   int          `json:"started"`
	HasQuorum bool         `json:"has_quorum"`
	Revision  int64        `json:"revision"`
	DBBytes   int64        `json:"db_bytes"`
	DBInUse   int64        `json:"db_in_use_bytes"`
	QuotaByte int64        `json:"quota_bytes"`
	Leader    string       `json:"leader"`
}

// Status describes the cluster from this member's point of view.
func (n *Node) Status(ctx context.Context) (ClusterStatus, error) {
	st, err := n.cli.Status(ctx, n.socket)
	if err != nil {
		return ClusterStatus{}, fmt.Errorf("etcd status: %w", err)
	}
	out := ClusterStatus{Revision: st.Header.Revision, DBBytes: st.DbSize, DBInUse: st.DbSizeInUse, QuotaByte: n.e.Config().QuotaBackendBytes}
	if out.QuotaByte == 0 {
		out.QuotaByte = 2 * 1024 * 1024 * 1024 // etcd's default
	}
	ml, err := n.cli.MemberList(ctx)
	if err != nil {
		return ClusterStatus{}, fmt.Errorf("etcd member list: %w", err)
	}
	for _, m := range ml.Members {
		mi := MemberInfo{Name: m.Name, ID: fmt.Sprintf("%x", m.ID), PeerURLs: m.PeerURLs, Leader: m.ID == st.Leader, Learner: m.IsLearner, Started: m.Name != ""}
		if mi.Leader {
			out.Leader = m.Name
		}
		if mi.Started {
			out.Started++
		}
		out.Members = append(out.Members, mi)
	}
	out.Quorum = len(out.Members)/2 + 1
	out.HasQuorum = st.Leader != 0
	return out, nil
}

// MemberRemove removes the member named name. Removing the last member is refused.
func (n *Node) MemberRemove(ctx context.Context, name string) error {
	ml, err := n.cli.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("etcd member list: %w", err)
	}
	if len(ml.Members) == 1 {
		return errors.New("refusing to remove the only member; stop shunt-control instead")
	}
	for _, m := range ml.Members {
		if m.Name == name {
			deadline := time.Now().Add(30 * time.Second)
			for {
				_, err := n.cli.MemberRemove(ctx, m.ID)
				if err == nil {
					return nil
				}
				if !strings.Contains(err.Error(), "unhealthy cluster") || time.Now().After(deadline) {
					return fmt.Errorf("etcd member remove: %w", err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(500 * time.Millisecond):
				}
			}
		}
	}
	return fmt.Errorf("no member named %s", name)
}

// Snapshot streams a point-in-time snapshot of the store to w: `shunt-control snapshot save`.
func (n *Node) Snapshot(ctx context.Context, w io.Writer) (int64, error) {
	rd, err := n.cli.Snapshot(ctx)
	if err != nil {
		return 0, fmt.Errorf("etcd snapshot: %w", err)
	}
	defer rd.Close() //nolint:errcheck // read-only
	return io.Copy(w, rd)
}

// Defrag compacts this member's database file in place: `shunt-control defrag`. It blocks the
// member's reads and writes while it runs, so one member at a time.
func (n *Node) Defrag(ctx context.Context) error {
	c, err := n.socketClient()
	if err != nil {
		return err
	}
	if _, err := c.Defragment(ctx, n.socket); err != nil {
		return fmt.Errorf("etcd defragment: %w", err)
	}
	return nil
}

// Restore rebuilds a data directory from a snapshot, offline, for a one-member cluster that other
// nodes then join: `shunt-control snapshot restore`. The data directory must not exist.
func Restore(snapshotPath, dataDir, name, peerURL string) error {
	if _, err := os.Stat(dataDir); err == nil {
		return fmt.Errorf("%s exists; restore into a new data directory", dataDir)
	}
	sp := snapshot.NewV3(zap.NewNop())
	if err := sp.Restore(snapshot.RestoreConfig{SnapshotPath: snapshotPath, Name: name, OutputDataDir: dataDir,
		PeerURLs: []string{peerURL}, InitialCluster: name + "=" + peerURL, SkipHashCheck: false}); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}

// promote makes this member a voter once it has caught up with the leader, which etcd requires
// ("can only promote a learner member which is in sync with leader"); a member that votes already
// is left as it is. The request goes to the leader through this member's own server.
func (n *Node) promote(ctx context.Context) error {
	id := n.e.Server.MemberID()
	deadline := time.Now().Add(60 * time.Second)
	for {
		learner := false
		for _, m := range n.e.Server.Cluster().Members() {
			if m.ID == id {
				learner = m.IsLearner
			}
		}
		if !learner {
			return nil
		}
		_, err := n.cli.MemberPromote(ctx, uint64(id))
		if err == nil {
			if n.cfg.Log != nil {
				n.cfg.Log.Info("etcd member promoted to a voting member", "name", n.cfg.Name)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("etcd: promoting %s to a voting member: %w", n.cfg.Name, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// WaitReady blocks until the member has a leader, or ctx ends: after a restart, before serving.
func (n *Node) WaitReady(ctx context.Context) error {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		if n.e.Server.Leader() != 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
