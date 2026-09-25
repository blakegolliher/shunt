package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/admin"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/cp"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
	shuntweb "github.com/blakegolliher/shunt/web"
)

// nodeOptions are the flags init and join share: what one control node is.
type nodeOptions struct {
	name      string
	dataDir   string
	peerURL   string
	api       string
	tokenRef  string
	plaintext bool
	leaseTTL  time.Duration
	logFormat string
	capacity  int
}

func addNodeFlags(cmd *cobra.Command, o *nodeOptions) {
	host, _ := os.Hostname()
	f := cmd.Flags()
	f.StringVar(&o.name, "name", host, "this node's name, unique in the cluster")
	f.StringVar(&o.dataDir, "data-dir", "", "where this node keeps etcd's data and its data-encryption key: a local disk, since every write is fsynced (required)")
	f.StringVar(&o.peerURL, "peer-url", "", "this node's peer URL, http://host:2380, reachable from the other control nodes (required)")
	f.StringVar(&o.api, "api", "127.0.0.1:9901", "the control API listener: operators' --api and proxies' control.endpoints")
	f.StringVar(&o.tokenRef, "token-ref", "", "env:NAME or file:/path holding the bearer token the API requires; required unless --api is loopback")
	f.BoolVar(&o.plaintext, "plaintext", false, "serve the API over plain http on a non-loopback address: secrets and client keys then cross the network in the clear (TLS is deferred, ADR-0015); required unless --api is loopback")
	f.DurationVar(&o.leaseTTL, "lease-ttl", 10*time.Second, "how long a proxy's lease lasts without a heartbeat before it refuses writes on moving buckets")
	f.StringVar(&o.logFormat, "log-format", "auto", "json | console | auto")
	f.IntVar(&o.capacity, "operation-capacity", control.DefaultCapacity, "how many operator operations may be unfinished at once; past it a new one answers 429 operation_capacity, while status stays available (ADR-0021)")
	_ = cmd.MarkFlagRequired("data-dir")
	_ = cmd.MarkFlagRequired("peer-url")
}

func (o *nodeOptions) check() error {
	if !config.ValidProxyID(o.name) {
		return fmt.Errorf("--name %q: want 1-64 letters, digits, '.', '_' or '-'", o.name)
	}
	if o.capacity < 1 {
		return fmt.Errorf("--operation-capacity %d: want at least 1", o.capacity)
	}
	host, _, err := net.SplitHostPort(o.api)
	if err != nil {
		return fmt.Errorf("--api %q: want host:port", o.api)
	}
	if !isLoopbackHost(host) {
		if o.tokenRef == "" {
			return errTokenRequired
		}
		if !o.plaintext {
			return errors.New("--api is not loopback and the API is plain http: pass --plaintext to state that secrets and client keys cross the network in the clear (TLS for the control channel is deferred, ADR-0015)")
		}
	}
	if o.tokenRef != "" && !strings.HasPrefix(o.tokenRef, "env:") && !strings.HasPrefix(o.tokenRef, "file:") {
		return errors.New("--token-ref must be env:NAME or file:/path; a token is never on the command line")
	}
	if o.leaseTTL < 3*time.Second {
		return errors.New("--lease-ttl must be at least 3s: one lost heartbeat must not make a proxy stale")
	}
	return nil
}

func newInit() *cobra.Command {
	var o nodeOptions
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Start the first control node, forming a new cluster (also how it is restarted)",
		Long: "Forms a new cluster of one and runs it. The data-encryption key for secrets at rest is created in\n" +
			"--data-dir. On a data directory that already holds a member, init just starts it again, so the same\n" +
			"command line is the node's service definition. Other nodes join with `shunt-control join`.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runNode(cmd, &o, "", "")
		},
	}
	addNodeFlags(cmd, &o)
	return cmd
}

func newJoin() *cobra.Command {
	var o nodeOptions
	var existing, resume string
	cmd := &cobra.Command{
		Use:   "join",
		Short: "Start a control node that joins a running cluster (also how it is restarted)",
		Long: "Asks the node at --existing to add this one as a learner, fetches the cluster's members and its\n" +
			"data-encryption key, and runs; the node promotes itself to a voter once it has caught up. The join is\n" +
			"an operation on the cluster (`shunt operation show <id>`), and this node records its progress in\n" +
			"--data-dir/join.json: a join interrupted anywhere resumes when the same command runs again. --resume\n" +
			"<operation> carries on a join whose join.json was lost. On a data directory that already holds a\n" +
			"member, join just starts it again. Replacing a failed node is `shunt-control member remove <name>` on\n" +
			"a live node, then join with the same name on a fresh data directory.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runNode(cmd, &o, existing, resume)
		},
	}
	addNodeFlags(cmd, &o)
	cmd.Flags().StringVar(&existing, "existing", "", "a running control node's API, http://host:9901, that adds this node")
	cmd.Flags().StringVar(&resume, "resume", "", "the operation id of a join to carry on, when this data directory has lost its join.json")
	return cmd
}

// initialized reports whether dataDir already holds an etcd member.
func initialized(dataDir string) bool {
	_, err := os.Stat(filepath.Join(dataDir, "member"))
	return err == nil
}

// runNode starts etcd, opens the store, serves the API, and blocks until a signal.
func runNode(cmd *cobra.Command, o *nodeOptions, existing, resume string) error {
	if err := o.check(); err != nil {
		return err
	}
	log := newLogger(o.logFormat, o.plaintext, cmd.ErrOrStderr())
	ctx, cancel := signalContext(cmd.Context())
	defer cancel()
	token := ""
	if o.tokenRef != "" {
		t, err := config.ResolveSecret(o.tokenRef)
		if err != nil {
			return fmt.Errorf("--token-ref: %w", err)
		}
		token = t
	}
	if err := os.MkdirAll(o.dataDir, 0o700); err != nil {
		return err
	}
	abs, _ := filepath.Abs(o.dataDir)
	log.Info("shunt-control starting", "version", version, "name", o.name, "data_dir", abs, "peer", o.peerURL, "api", o.api)

	ncfg := cp.NodeConfig{Name: o.name, DataDir: o.dataDir, PeerURL: o.peerURL, Log: log}
	joined, err := loadJoin(o.dataDir)
	if err != nil {
		return err
	}
	var key []byte
	switch {
	case initialized(o.dataDir):
		// A restart: the member is on disk, and so is the key. A node that joined may still be a
		// learner, and promotes itself.
		k, kerr := cp.LoadOrCreateKey(o.dataDir, false)
		if kerr != nil {
			return kerr
		}
		key, ncfg.Existing = k, existing != "" || joined != nil
		log.Info("restarting an existing member", "name", o.name)
	case existing != "" || resume != "" || joined != nil:
		// A join, new or interrupted (ADR-0021 D4): join.json says where it stands.
		k, initial, jerr := joinCluster(ctx, o, existing, resume, token, log)
		if jerr != nil {
			return jerr
		}
		key, ncfg.InitialCluster, ncfg.Existing = k, initial, true
	case cmd.Name() == "join":
		return errors.New("--existing is required the first time a node joins: the API address of a running control node")
	default:
		k, kerr := cp.LoadOrCreateKey(o.dataDir, true)
		if kerr != nil {
			return kerr
		}
		key = k
		log.Info("forming a new cluster", "name", o.name)
	}
	cipher, err := cp.NewCipher(key)
	if err != nil {
		return err
	}
	node, err := cp.Start(ctx, ncfg)
	if err != nil {
		return err
	}
	defer node.Close()

	metrics := telemetry.NewMetrics()
	store := cp.New(node.Client(), cipher, log)
	registry := upstream.NewRegistry(upstream.Options{}, store.Resolve)
	defer registry.Close()
	// Every installed version and every operation record change is published on GET /v1/events
	// (ADR-0017); the store's first install is the stream's baseline.
	events := control.NewEvents(0)
	store.OnInstall = events.Directory
	ops := cp.NewOperations(node.Client())
	ops.OnChange = events.Fence
	ops.Node = o.name // its liveness key: another node ends this node's operations if it is lost
	ops.Capacity = o.capacity
	fleet := cp.NewFleet(node.Client(), o.leaseTTL)
	telemetryStore := telemetry.NewStore(o.leaseTTL)
	moverDir := filepath.Join(o.dataDir, "mover")
	worker := control.MoverWorker{Snapshot: func() *directory.File { return store.Snapshot().File() }, Secrets: store.ClusterSecrets,
		CursorDir: filepath.Join(moverDir, "cursor"), LedgerDir: filepath.Join(moverDir, "ledger")}
	ctl := &control.Server{Dir: store, Clusters: registry, Metrics: metrics, Log: log, Keys: store, Fleet: fleet, LeaseTTL: o.leaseTTL, ClusterSecrets: store.ClusterSecrets, Token: token,
		Ops: ops, Node: o.name, Events: events, Telemetry: telemetryStore, Ctx: ctx, ConfirmKey: cipher.Derive("confirm"),
		Mover: worker.Run, MoverLedger: worker.Ledger}
	store.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, aerr := registry.PrepareWith(f.Clusters, resolve, secretGeneration(f))
		if aerr != nil {
			return nil, aerr
		}
		return func() {
			added, removed := cand.Commit()
			for _, name := range added {
				log.Info("cluster ready", "cluster", name)
			}
			for _, name := range removed {
				log.Info("cluster removed", "cluster", name)
			}
		}, nil
	}
	// The store loads once there is quorum; until then /v1/ answers 503 and status says so. A
	// node restarting alone after an outage must serve its API for the operator to see that. The
	// operation records follow the directory, and the records this node was running when it last
	// stopped are closed as failed.
	go func() {
		loaded := false
		for ctx.Err() == nil {
			sctx, scancel := context.WithTimeout(ctx, 15*time.Second)
			var err error
			if !loaded {
				if err = store.Start(sctx); err == nil {
					loaded = true
					log.Info("directory loaded", "version", store.Version())
				}
			}
			if loaded {
				if err = ops.Start(sctx); err == nil {
					if ferr := ctl.FailOrphans(sctx); ferr != nil {
						log.Warn("operation records left running by the last stop could not all be closed", "err", ferr.Error())
					}
					if rerr := ctl.ResumeOwn(sctx); rerr != nil {
						log.Warn("barrier operations left running by the last stop could not all be resumed", "err", rerr.Error())
					}
				}
			}
			scancel()
			if err == nil {
				return
			}
			if !loaded {
				log.Warn("directory not loaded yet (waiting for quorum?)", "err", err.Error())
			} else {
				log.Warn("operation records not loaded yet", "err", err.Error())
			}
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}()
	defer store.Close()
	defer ops.Close()
	ctl.Members = &control.Membership{List: node.ListMembers, AddLearner: node.AddLearner, Key: cipher.Key}
	api := &cp.API{Node: node, Store: store, Fleet: fleet, Cipher: cipher, Version: version, Join: joinLine(o), Control: ctl}

	adm := admin.New(metrics.Registry, telemetry.NewSlowRing(1, time.Hour))
	mountControl(adm, ctl, api, store)
	srv := adm.Listen(o.api)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("shunt-control serving", "api", o.api, "directory_version", store.Version(), "lease_ttl", o.leaseTTL.String())

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("signal received, stopping")
			return shutdown(srv)
		case err := <-errCh:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		case err := <-node.Err():
			return fmt.Errorf("etcd: %w", err)
		case <-tick.C:
			if err := ctl.PublishFleet(ctx); err != nil && ctx.Err() == nil {
				log.Warn("fleet unreadable", "err", err.Error())
			}
			if store.Ready() {
				if err := ctl.Sweep(ctx); err != nil && ctx.Err() == nil {
					log.Warn("operation records unreadable", "err", err.Error())
				}
			}
		}
	}
}

// mountControl serves the control plane's own routes and the control API on the admin mux. The
// bare /v1/control is mounted on its own: a subtree pattern alone would redirect it to a path the
// inner mux does not serve.
func mountControl(adm *admin.Server, ctl *control.Server, api *cp.API, store *cp.Store) {
	own := ctl.Authorize(api.Handler())
	adm.Mount("/v1/control", own)
	adm.Mount("/v1/control/", own)
	adm.Mount("/v1/", gate(store, ctl.Handler()))
	adm.Mount("/", shuntweb.Handler())
}

// joinLine is the command a new control node runs to join this one, as GET /v1/control shows it:
// this node's own flags filled in, the parts only the operator knows in angle brackets.
func joinLine(o *nodeOptions) string {
	_, port, err := net.SplitHostPort(o.api)
	if err != nil {
		port = "9901"
	}
	parts := []string{"shunt-control join --name <name> --data-dir <data-dir> --peer-url http://<host>:2380 --api <host>:" + port}
	if o.tokenRef != "" {
		parts = append(parts, "--token-ref "+o.tokenRef)
	}
	if o.plaintext {
		parts = append(parts, "--plaintext")
	}
	if o.leaseTTL != 10*time.Second && o.leaseTTL > 0 {
		parts = append(parts, "--lease-ttl "+o.leaseTTL.String())
	}
	return strings.Join(append(parts, "--existing http://"+o.api), " ")
}

// gate answers 503 until the store has loaded the directory.
func gate(store *cp.Store, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !store.Ready() {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"unavailable","message":"this control node has not loaded the directory yet: it is waiting for quorum (shunt-control status)"}` + "\n"))
			return
		}
		h.ServeHTTP(w, r)
	})
}

func shutdown(srv *http.Server) error {
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(dctx)
}

func newLogger(format string, plaintext bool, stderr io.Writer) *slog.Logger {
	if format == "auto" {
		format = "json"
		if f, ok := stderr.(*os.File); ok {
			if st, err := f.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
				format = "console"
			}
		}
	}
	if format == "console" {
		o := telemetry.ConsoleOptions{}
		if plaintext {
			o.Tag = "PLAINTEXT"
		}
		return slog.New(telemetry.NewConsoleHandler(stderr, o))
	}
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	if plaintext {
		log = log.With("control_api", "PLAINTEXT http (--plaintext; secrets and client keys cross the network in the clear)")
	}
	return log
}

// secretGeneration reads each cluster's secret generation from a directory version, for the
// signers the registry builds from it (ADR-0021 D1).
func secretGeneration(f *directory.File) func(string) int64 {
	return func(name string) int64 { return f.Generation(directory.SecretResource(name)) }
}
