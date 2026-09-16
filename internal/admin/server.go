package admin

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/blakegolliher/shunt/internal/telemetry"
)

// Server is the admin endpoint set. Draining flips /-/healthz to 503 so an L4 balancer stops
// sending new connections before the proxy stops accepting.
type Server struct {
	mux      *http.ServeMux
	draining atomic.Bool
	slow     *telemetry.SlowRing
}

// New builds the admin handler over the metrics registry and slow ring.
func New(reg *prometheus.Registry, slow *telemetry.SlowRing) *Server {
	s := &Server{mux: http.NewServeMux(), slow: slow}
	s.mux.HandleFunc("GET /-/healthz", s.healthz)
	s.mux.Handle("GET /-/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /-/slow", s.slowHandler)
	s.mux.HandleFunc("POST /-/drain", s.drainHandler)
	s.mux.HandleFunc("/debug/pprof/", pprof.Index)
	s.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	s.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	s.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return s
}

// Mount serves h for every path under prefix: the control API under /v1/ in resign mode.
func (s *Server) Mount(prefix string, h http.Handler) { s.mux.Handle(prefix, h) }

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Drain marks the process as draining: healthz answers 503 from now on.
func (s *Server) Drain() { s.draining.Store(true) }

// Draining reports whether Drain was called.
func (s *Server) Draining() bool { return s.draining.Load() }

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if s.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("draining\n"))
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) drainHandler(w http.ResponseWriter, _ *http.Request) {
	s.Drain()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("draining\n"))
}

func (s *Server) slowHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	out := struct {
		Threshold string                `json:"threshold"`
		Entries   []telemetry.SlowEntry `json:"entries"`
	}{Threshold: s.slow.Threshold().String(), Entries: s.slow.Snapshot()}
	if out.Entries == nil {
		out.Entries = []telemetry.SlowEntry{}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// Listen serves the admin handler on addr with conservative timeouts.
func (s *Server) Listen(addr string) *http.Server {
	return &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second}
}
