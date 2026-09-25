package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/cp"
)

// A joining node's side of a join (ADR-0021 D4, H3a). The node keeps a record of its join in its
// data directory, join.json, written with fsync before each step that depends on it, so a join
// interrupted anywhere is resumed by running the same command again: prepared (the request id is
// chosen and saved before the cluster is asked), requested (the cluster has the join's operation
// and its learner), bootstrap_saved (the member list and the key are on disk; etcd starts from
// them). The cluster side is an operation record that follows the node until it votes.

const joinFile = "join.json"

// Local join phases.
const (
	joinPrepared       = "prepared"
	joinRequested      = "requested"
	joinBootstrapSaved = "bootstrap_saved"
)

// joinState is join.json. The data-encryption key is never in it: it has its own 0600 file.
type joinState struct {
	Version        int    `json:"version"`
	Name           string `json:"name"`
	PeerURL        string `json:"peer_url"`
	APIURL         string `json:"api_url,omitempty"`
	Existing       string `json:"existing"`
	RequestID      string `json:"request_id,omitempty"` // the join request's Idempotency-Key
	Operation      string `json:"operation,omitempty"`
	MemberID       string `json:"member_id,omitempty"`
	InitialCluster string `json:"initial_cluster,omitempty"`
	Phase          string `json:"phase"`
}

func loadJoin(dataDir string) (*joinState, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, joinFile)) //nolint:gosec // the node's own data directory
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st joinState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("%s: %w; it records an interrupted join, so remove the data directory to start over", filepath.Join(dataDir, joinFile), err)
	}
	if st.Version != 1 {
		return nil, fmt.Errorf("%s: version %d, want 1", filepath.Join(dataDir, joinFile), st.Version)
	}
	return &st, nil
}

func saveJoin(dataDir string, st *joinState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return cp.WritePrivate(filepath.Join(dataDir, joinFile), append(data, '\n'))
}

// advertisedAPI is the control API URL this node announces for the other nodes' health probes: the
// --api address, or, when it listens on every address, the peer URL's host with the API's port.
func advertisedAPI(api, peerURL string) string {
	host, port, err := net.SplitHostPort(api)
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		u, err := url.Parse(peerURL)
		if err != nil {
			return ""
		}
		host = u.Hostname()
	}
	return "http://" + net.JoinHostPort(host, port)
}

// preflightJoin checks what can be checked before the cluster is asked to add anything: an empty,
// writable data directory, the peer URL's one spelling, and a cluster that answers and has no
// member with this name or peer URL. It cannot rule out a later disk or network failure; every
// step after it resumes.
func preflightJoin(ctx context.Context, c *apiClient, dataDir, name, peer string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("%s is not empty and holds no member and no join: join into an empty data directory", dataDir)
	}
	probe := filepath.Join(dataDir, ".write-check")
	if err := cp.WritePrivate(probe, []byte("ok\n")); err != nil {
		return fmt.Errorf("%s is not writable: %w", dataDir, err)
	}
	if err := os.Remove(probe); err != nil {
		return err
	}
	var st cp.StatusAnswer
	if err := c.call(ctx, http.MethodGet, "/v1/control", nil, &st); err != nil {
		return err
	}
	for _, m := range st.Cluster.Members {
		if m.Name == name {
			return fmt.Errorf("the cluster already has a member named %s (%s); remove it first (`shunt-control member remove %s`), or choose another name", name, m.ID, name)
		}
		if slices.ContainsFunc(m.PeerURLs, func(u string) bool { n, err := control.NormalizePeerURL(u); return err == nil && n == peer }) {
			return fmt.Errorf("the cluster already has a member at peer URL %s (%s %s)", peer, m.Name, m.ID)
		}
	}
	return nil
}

// joinCluster runs a join from wherever join.json leaves it, and returns the key and the initial
// cluster etcd starts with.
func joinCluster(ctx context.Context, o *nodeOptions, existing, resume, token string, log *slog.Logger) (key []byte, initial string, err error) {
	peer, err := control.NormalizePeerURL(o.peerURL)
	if err != nil {
		return nil, "", err
	}
	st, err := loadJoin(o.dataDir)
	if err != nil {
		return nil, "", err
	}
	switch {
	case st != nil && (st.Name != o.name || st.PeerURL != peer):
		return nil, "", fmt.Errorf("%s records a join as %s at %s, not %s at %s: run it with the same --name and --peer-url, or join into a fresh data directory",
			filepath.Join(o.dataDir, joinFile), st.Name, st.PeerURL, o.name, peer)
	case st != nil && resume != "" && st.Operation != "" && st.Operation != resume:
		return nil, "", fmt.Errorf("%s records join %s, not %s", filepath.Join(o.dataDir, joinFile), st.Operation, resume)
	case st != nil && existing != "":
		st.Existing = existing // any running control node can carry the join on
	case st == nil && existing == "":
		return nil, "", errors.New("--existing is required the first time a node joins: the API address of a running control node")
	}
	if st == nil {
		c := &apiClient{base: strings.TrimRight(existing, "/"), token: token}
		if resume == "" {
			if perr := preflightJoin(ctx, c, o.dataDir, o.name, peer); perr != nil {
				return nil, "", perr
			}
		}
		var rid [8]byte
		if _, err = rand.Read(rid[:]); err != nil {
			return nil, "", err
		}
		st = &joinState{Version: 1, Name: o.name, PeerURL: peer, APIURL: advertisedAPI(o.api, peer), Existing: existing,
			RequestID: "join-" + hex.EncodeToString(rid[:]), Phase: joinPrepared}
		if resume != "" {
			st.RequestID, st.Operation, st.Phase = "", resume, joinRequested
		}
		if serr := saveJoin(o.dataDir, st); serr != nil {
			return nil, "", serr
		}
	}
	c := &apiClient{base: strings.TrimRight(st.Existing, "/"), token: token, idem: st.RequestID}
	if st.Phase == joinPrepared {
		op, rerr := requestJoin(ctx, c, st, log)
		if rerr != nil {
			return nil, "", fmt.Errorf("join via %s: %w; run the same command again to resume it", st.Existing, rerr)
		}
		if op.Status == control.StatusFailed {
			msg := "failed"
			if op.Error != nil {
				msg = op.Error.Message
			}
			return nil, "", fmt.Errorf("join %s: %s; remove %s to start a new join", op.ID, msg, o.dataDir)
		}
		st.Operation, st.Phase = op.ID, joinRequested
		if serr := saveJoin(o.dataDir, st); serr != nil {
			return nil, "", serr
		}
		log.Info("join requested", "operation", op.ID, "existing", st.Existing, "phase", op.Phase)
	}
	if st.Phase == joinRequested {
		b, berr := fetchBootstrap(ctx, c, st)
		if berr != nil {
			return nil, "", berr
		}
		if werr := cp.WriteKey(o.dataDir, b.EncryptionKey); werr != nil {
			return nil, "", werr
		}
		st.MemberID, st.InitialCluster, st.Phase = b.MemberID, b.InitialCluster, joinBootstrapSaved
		if serr := saveJoin(o.dataDir, st); serr != nil {
			return nil, "", serr
		}
		log.Info("join bootstrap saved", "operation", st.Operation, "member", st.MemberID)
	}
	key, err = cp.LoadOrCreateKey(o.dataDir, false)
	if err != nil {
		return nil, "", err
	}
	log.Info("joining", "operation", st.Operation, "member", st.MemberID, "initial_cluster", st.InitialCluster)
	return key, st.InitialCluster, nil
}

// joinWait bounds how long a join waits for another membership change to finish.
const joinWait = 5 * time.Minute

// requestJoin asks the cluster for the join. The control plane runs one membership change at a time
// (ADR-0021 D4), and refuses a second with operation_conflict rather than queue it; a node started
// right after another, which is still being promoted, waits for it here. The refused request left no
// record, so the retry carries the same Idempotency-Key.
func requestJoin(ctx context.Context, c *apiClient, st *joinState, log *slog.Logger) (control.Operation, error) {
	deadline := time.Now().Add(joinWait)
	for {
		var op control.Operation
		err := c.call(ctx, http.MethodPost, "/v1/control/members", control.JoinRequest{Name: st.Name, PeerURL: st.PeerURL, APIURL: st.APIURL}, &op)
		var ae *apiError
		if err == nil || !errors.As(err, &ae) || ae.code != control.CodeOperationConflict || time.Now().After(deadline) {
			return op, err
		}
		log.Info("waiting for another membership change to finish", "reason", ae.msg)
		select {
		case <-ctx.Done():
			return op, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// fetchBootstrap asks for the join's bootstrap, waiting while the learner is still being added.
func fetchBootstrap(ctx context.Context, c *apiClient, st *joinState) (control.Bootstrap, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		var b control.Bootstrap
		err := c.call(ctx, http.MethodPost, "/v1/control/joins/"+url.PathEscape(st.Operation)+"/bootstrap", control.BootstrapRequest{PeerURL: st.PeerURL}, &b)
		var ae *apiError
		switch {
		case err == nil && (len(b.EncryptionKey) != 32 || b.InitialCluster == "" || b.MemberID == ""):
			return b, fmt.Errorf("join %s: the bootstrap carried no key, member or member list", st.Operation)
		case err == nil:
			return b, nil
		case errors.As(err, &ae) && (ae.code == "not_ready" || ae.status == http.StatusServiceUnavailable) && time.Now().Before(deadline):
		default:
			return b, fmt.Errorf("join %s bootstrap via %s: %w; run the same command again to resume it", st.Operation, st.Existing, err)
		}
		select {
		case <-ctx.Done():
			return b, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
