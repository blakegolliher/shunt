package cp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
)

// The control plane's own routes, beside the control API (ADR-0015): the etcd lifecycle wrapped
// so an operator never sees an etcd flag. Mounted by shunt-control under /v1/control/ on the
// same listener and token as /v1/.

// JoinRequest is `shunt-control join`: add a member and hand it what it needs to start.
type JoinRequest struct {
	Name    string `json:"name"`
	PeerURL string `json:"peer_url"`
}

// JoinAnswer is what a joining node starts with. The data-encryption key crosses the control
// channel here, which is why the channel must be marked plaintext until TLS lands.
type JoinAnswer struct {
	InitialCluster string `json:"initial_cluster"`
	EncryptionKey  []byte `json:"encryption_key"`
}

// StatusAnswer is `shunt-control status` and GET /v1/control: the etcd cluster and the fleet in
// one answer, with what a new node needs to join (ADR-0017).
type StatusAnswer struct {
	Node    string           `json:"node"`
	Version string           `json:"version"` // shunt-control's build
	Cluster ClusterStatus    `json:"cluster"`
	Fleet   []control.Member `json:"fleet"`
	// FleetError: the fleet could not be read (no quorum); Fleet is then empty.
	FleetError string `json:"fleet_error,omitempty"`
	// Directory is the version this node has installed; 0 with DirectoryLoaded false while the
	// node waits for quorum to load it.
	Directory       int64 `json:"directory"`
	DirectoryLoaded bool  `json:"directory_loaded"`
	// LastCompaction is when etcd last compacted its history, from its own gauge; null until a
	// compaction has run since this node started.
	LastCompaction *time.Time `json:"last_compaction"`
	Compaction     Compaction `json:"compaction"`
	// Join is the command a new control node runs to join this one, with the parts only the
	// operator knows in angle brackets.
	Join string `json:"join"`
}

// Compaction is etcd's automatic compaction as this node runs it.
type Compaction struct {
	Mode      string `json:"mode"`
	Retention string `json:"retention"`
}

// API is the handler for /v1/control.
type API struct {
	Node    *Node
	Store   *Store
	Fleet   *Fleet
	Cipher  *Cipher
	Version string
	// Join is the join command line shunt-control renders from its own flags.
	Join string
}

// Handler returns the routes. Authentication is the control API's, applied by the caller.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/control", a.status)
	mux.HandleFunc("GET /v1/control/{$}", a.status)
	mux.HandleFunc("GET /v1/control/status", a.status)
	mux.HandleFunc("POST /v1/control/members", a.join)
	mux.HandleFunc("DELETE /v1/control/members/{name}", a.removeMember)
	mux.HandleFunc("GET /v1/control/snapshot", a.snapshot)
	mux.HandleFunc("POST /v1/control/defrag", a.defrag)
	return mux
}

// Routes is the control plane's own route table, for docs/reference/control-routes.json: the
// `shunt-control` verbs that reach each route.
func Routes() []control.Route {
	status := []string{"status", "member list"}
	return []control.Route{
		{Method: "GET", Pattern: "/v1/control", Verbs: status},
		{Method: "GET", Pattern: "/v1/control/{$}", Verbs: status},
		{Method: "GET", Pattern: "/v1/control/status", Verbs: status},
		{Method: "POST", Pattern: "/v1/control/members", Verbs: []string{"join"}, Mutation: true},
		{Method: "DELETE", Pattern: "/v1/control/members/{name}", Verbs: []string{"member remove"}, Mutation: true},
		{Method: "GET", Pattern: "/v1/control/snapshot", Verbs: []string{"snapshot save"}},
		{Method: "POST", Pattern: "/v1/control/defrag", Verbs: []string{"defrag"}, Mutation: true},
	}
}

// compactionGauge is etcd's own record of its last compaction: unix seconds, 0 since start.
const compactionGauge = "etcd_debugging_mvcc_db_compaction_last"

// lastCompaction reads when etcd last compacted, from the gauge it registers on the default
// Prometheus registry; nil until a compaction has run since this process started.
func lastCompaction() *time.Time {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil
	}
	for _, f := range families {
		if f.GetName() != compactionGauge {
			continue
		}
		for _, m := range f.GetMetric() {
			if v := m.GetGauge().GetValue(); v > 0 {
				t := time.Unix(int64(v), 0).UTC()
				return &t
			}
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, control.Error{Code: code, Message: msg})
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	cs, err := a.Node.Status(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	ans := StatusAnswer{Node: a.Node.Name(), Version: a.Version, Cluster: cs, Fleet: []control.Member{}, Directory: a.Store.Version(), DirectoryLoaded: a.Store.Ready(),
		LastCompaction: lastCompaction(), Join: a.Join}
	ans.Compaction.Mode, ans.Compaction.Retention = a.Node.Compaction()
	fctx, fcancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer fcancel()
	if fleet, err := a.Fleet.Members(fctx); err != nil {
		ans.FleetError = "the fleet cannot be read: " + err.Error()
	} else if fleet != nil {
		ans.Fleet = fleet
	}
	writeJSON(w, http.StatusOK, ans)
}

func (a *API) join(w http.ResponseWriter, r *http.Request) {
	var req JoinRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "request body: "+err.Error())
		return
	}
	if !config.ValidProxyID(req.Name) {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("member name %q: want 1-64 letters, digits, '.', '_' or '-'", req.Name))
		return
	}
	if u, err := url.Parse(req.PeerURL); err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("peer URL %q: want http://host:port", req.PeerURL))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	initial, err := a.Node.MemberAdd(ctx, req.Name, req.PeerURL)
	if err != nil {
		writeError(w, http.StatusConflict, "refused", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, JoinAnswer{InitialCluster: initial, EncryptionKey: a.Cipher.Key()})
}

func (a *API) removeMember(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if err := a.Node.MemberRemove(ctx, r.PathValue("name")); err != nil {
		writeError(w, http.StatusConflict, "refused", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"removed": r.PathValue("name")})
}

func (a *API) snapshot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	if _, err := a.Node.Snapshot(r.Context(), w); err != nil {
		// Headers are out; the client sees a short body and the error in its log.
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

func (a *API) defrag(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	if err := a.Node.Defrag(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"defragmented": a.Node.Name()})
}
