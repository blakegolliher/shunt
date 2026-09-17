
Phase 3c: the distributed control plane. Read docs/design/distributed.md and docs/adr/0015-embedded-etcd.md first; move ADR-0015 from proposed to accepted in this phase. Plan mode; show me the plan before code.

Build:
1. cmd/shunt-control: separate binary embedding etcd via go.etcd.io/etcd/server/v3/embed. Subcommands init, join, member list|remove, snapshot save|restore, defrag, status, each wrapping the etcd API; no raw etcd flags exposed. Internal CA generated at init; control-to-control and proxy-to-control TLS from it. Pin etcd to one minor version; record it in docs/deps.md with the license and NOTICE handling.
2. Key layout exactly as §12.5. All writes are etcd transactions with compare-and-swap on the record's revision; any control node can write. Audit records under /changes/<rev>, compacted to object storage on a schedule.
3. Control API (the POC-5 REST surface, unchanged for callers) served by every control node: long-poll GET /v1/directory?since=<rev> returning deltas from the node's watch cache; heartbeat endpoint that renews the proxy's lease and records applied revision, health, and counters; every mutation endpoint from POC-5.
4. Proxy side: a `control` Directory backend (the second implementation of the seam). Long-poll, atomic snapshot swap, ack, local snapshot cache on disk, bootstrap from the object-storage snapshot at a stamped revision. Stale mode per §12.6 with the readiness endpoint reporting degraded. Proxy config reduces to control endpoints, identity cert, listener.
5. Version fence: change records carry pending/committed; commit when every leased proxy reports applied ≥ rev; CLI prints waiting-on names. Two-phase ramp per §12.6 with reads widened at R1 and writes moved at R2; cutover single-phase; purge-source gated on cutover committed.
6. Read-only flags on placements and clusters, applied through the fence; 503 + Retry-After by default, 403 with --reject.
7. Dev path unchanged: the file backend and in-proxy control API remain for single-node labs; the walkthrough runs on both backends.
8. Tests: unit tests per §12.9 with an in-process embedded etcd (embed makes this a plain test); a three-node control cluster in test/e2e with two proxies; chaos: kill one control node, kill two (quorum loss), partition one proxy, restart a proxy with the control plane down. Property test gains the stale-proxy and two-phase-ramp invariants.
9. docs: docs/how-to/run-shunt-control.md (init, join, replace a node, snapshot and restore rehearsal, disk and clock requirements), docs/reference/control-api.md updated, docs/explanation/why-no-objects-table.md, runbooks for quorum loss and a lagging proxy.

Acceptance: walkthrough green on the etcd backend with two proxies; the §12.9 property and chaos runs green; quorum loss under a 256-connection ACTIVE workload shows zero data-path errors; a proxy restarted with the control plane down serves ACTIVE placements from its cache; bin/shunt links no etcd packages (make licenses proves it).


Amendment — design the control API for a browser client now:
- Operation records: POST /v1/operations returns an id; GET /v1/operations/<id> shows phase, waiting-on proxies, and outcome; the CLI's --wait polls it.
- Push: GET /v1/events as server-sent events from the watch cache — directory changes, fence progress, heartbeat aggregates.
- Read models: fleet (proxies, applied revision, stale ones), migration (ramp split, fallback reads, per-range mover progress, fence state), cluster (health, measured capabilities, dependent placements), audit tail.
- Identity: OIDC login with viewer/operator/admin roles; the audit actor is the authenticated identity. Bearer tokens remain for the CLI and automation, scoped to a role.
- Safety in the API, not the client: purge-source and cluster remove take a dry-run that returns what would happen and a confirmation token from that dry-run to proceed.
