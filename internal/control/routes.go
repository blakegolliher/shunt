package control

import "net/http"

// Route is one entry of the control API's route table: the mux is built from it, and
// docs/reference/control-routes.json is generated from it for the web UI's parity test
// (ADR-0017). Verbs names the CLI verbs that reach the route; none means a member proxy's or a
// read model's route.
type Route struct {
	Method   string   `json:"method"`
	Pattern  string   `json:"pattern"`
	Verbs    []string `json:"verbs,omitempty"`
	Mutation bool     `json:"mutation"`

	log string // the operation name serve's log shows a refusal under; "" for reads
	h   http.HandlerFunc
}

func (s *Server) routes() []Route {
	read := func(pattern string, h http.HandlerFunc, verbs ...string) Route {
		return Route{Method: "GET", Pattern: pattern, Verbs: verbs, h: h}
	}
	mut := func(method, pattern, log string, h http.HandlerFunc, verbs ...string) Route {
		return Route{Method: method, Pattern: pattern, Verbs: verbs, Mutation: true, log: log, h: h}
	}
	// Every mutation of a placement or a cluster runs under an operation record that reserves its
	// scope (ADR-0021): the fenced actions through launch, the short ones through recorded.
	placement := s.placementScope
	return []Route{
		read("/v1/status", s.status, "status"),
		read("/v1/directory", s.directoryHandler),
		read("/v1/events", s.events),
		read("/v1/telemetry/series", s.telemetrySeries),
		read("/v1/telemetry/latest", s.telemetryLatest),
		read("/v1/audit", s.audit),
		read("/v1/fleet", s.fleetList, "proxy list"),
		read("/v1/fleet/{id}", s.proxyDiagnostics, "proxy show"),
		read("/v1/operations", s.listOperations, "operation list"),
		read("/v1/operations/{id}", s.getOperation, "ramp", "migrate start", "cutover", "purge-source", "migrate finish", "cluster remove", "operation show", "operation wait"),
		read("/v1/clusters/{name}/view", s.clusterView),
		read("/v1/placements/{tenant}/{bucket}", s.placement, "migrate run"),
		read("/v1/placements/{tenant}/{bucket}/view", s.placementView),
		read("/v1/placements/{tenant}/{bucket}/mover-ledger", s.moverLedger),
		read("/v1/tenants/{tenant}/step-out", s.stepOut, "step-out"),
		mut("POST", "/v1/operations", "operation", s.startOperation, "ramp", "migrate start", "cutover", "purge-source", "migrate finish", "cluster remove"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/create", "create", s.recorded(OpCreate, placement, s.createPlacement)),
		mut("POST", "/v1/placements/{tenant}/{bucket}/create-backend", "create", s.recorded(OpCreate, placement, s.createBackendPlacement), "create"),
		mut("DELETE", "/v1/placements/{tenant}/{bucket}", "delete", s.recorded(OpDelete, placement, s.deletePlacement)),
		mut("POST", "/v1/clusters", "cluster add", s.recorded(OpClusterAdd, s.clusterAddScope, s.putCluster), "cluster add"),
		mut("POST", "/v1/clusters/probe", "", s.probeCluster, "cluster add"),
		mut("POST", "/v1/clusters/{name}/credentials", "cluster credentials", s.recorded(OpClusterRotate, s.clusterPathScope, s.rotateCredentials), "cluster credentials"),
		mut("DELETE", "/v1/clusters/{name}", "cluster remove", s.removeCluster, "cluster remove"),
		mut("POST", "/v1/clusters/{name}/read-only", "cluster read-only", s.clusterReadOnly, "cluster readonly"),
		mut("POST", "/v1/tenants/{tenant}/default-cluster", "tenant set-default", s.setTenantDefault, "tenant set-default"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/adopt", "adopt", s.recorded(OpAdopt, placement, s.adopt), "adopt"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/expand", "expand", s.recorded(OpExpand, placement, s.expand), "expand"),
		mut("DELETE", "/v1/placements/{tenant}/{bucket}/target", "expand clear", s.recorded(OpClearTarget, placement, s.clearTarget), "expand"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/prefixes", "expand carve", s.recorded(OpCarve, placement, s.carve), "expand"),
		mut("DELETE", "/v1/placements/{tenant}/{bucket}/prefixes", "expand merge", s.recorded(OpMerge, placement, s.merge), "expand"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/read-only", "placement read-only", s.placementReadOnly, "readonly"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/watch", "watch", s.recorded(OpWatch, placement, s.placementWatch), "watch"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/ramp", "ramp", s.ramp, "ramp"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/migrate", "migrate start", s.migrateStart, "migrate start"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/mover-progress", "mover report", s.moverProgress, "migrate run"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/cutover", "cutover", s.cutover, "cutover"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/purge-source", "purge-source", s.purgeSource, "purge-source"),
		mut("POST", "/v1/placements/{tenant}/{bucket}/finish", "migrate finish", s.finish, "migrate finish"),
		mut("POST", "/v1/tenants/{tenant}/client-keys", "client key import", s.importKey, "adopt", "client add"),
		mut("DELETE", "/v1/tenants/{tenant}/client-keys/{access_key}", "client key remove", s.removeKey, "client remove"),
		mut("POST", "/v1/fleet/{id}/heartbeat", "", s.heartbeat),
		mut("POST", "/v1/fleet/{id}/retire", "proxy retire", s.retireProxy, "proxy retire"),
		mut("POST", "/v1/fleet/{id}/resolve", "proxy resolve", s.resolveProxy, "proxy resolve"),
		mut("DELETE", "/v1/fleet/{id}", "proxy forget", s.forgetProxy, "proxy forget"),
	}
}

// Routes is the route table without its handlers.
func Routes() []Route {
	rs := (&Server{}).routes()
	out := make([]Route, 0, len(rs))
	for _, r := range rs {
		out = append(out, Route{Method: r.Method, Pattern: r.Pattern, Verbs: r.Verbs, Mutation: r.Mutation})
	}
	return out
}
