# Shunt web UI — build prompts

Branch `distributed`. Seven prompts, one per session, in order. Kickoff first each session. G1–G4 at the end of UI-6; G4 must now cover npm dependencies too.

Supersedes item 1 of `docs/prompts/P3e.md` (htmx, no Node). ADR-0017, written in UI-0, records the stack below and states that it overrides the "a web UI" entry in docs/DESIGN.md §3 (non-goals for v1) on the `distributed` branch; the §3 entry is amended in place with the ADR reference, as §9 item 14 was for ADR-0014.

## Decisions (put in docs/adr/0017-web-ui.md in UI-0)

- **Stack:** Vite + React + TypeScript + Tailwind CSS; Recharts for graphs. All MIT: preserve copyright notices; `web/THIRD_PARTY_NOTICES` generated from `package.json` at build and merged into the repo's `THIRD_PARTY_NOTICES`.
- **Delivery:** `make ui` builds `web/dist`; `shunt-control` embeds it with `embed.FS` and serves it on the control API port. Node is a build-time dependency only. A committed stub `web/dist/index.html` ("UI not built — run make ui") keeps `go build` working without Node. `web/dist` is otherwise gitignored; CI runs `make ui` before `make build`.
- **Auth for the demo:** bearer token pasted once, held in `sessionStorage`, sent as `Authorization: Bearer`. OIDC is the P3c amendment's later item; the API surface does not change when it lands.
- **Percentiles:** emitted, never computed in the browser. Proxies keep HdrHistogram (`github.com/HdrHistogram/hdrhistogram-go`, MIT) sketches per 10 s window and ship them compressed in heartbeats; control nodes merge and serve p50/p90/p99/p99.9. Fleet/cluster/proxy views are merges of the same sketches, never averages of percentiles.
- **Theme:** burnt orange on black. Tailwind palette: `ember-500 #CC5500` (primary), `ember-400 #E06A1F`, `ember-600 #A64400`, `ember-300 #F08A4B`; `ink-950 #0B0B0B` (background), `ink-900 #141414`, `ink-800 #1F1F1F`, `ink-700 #2A2A2A`; text `#F2ECE6`; muted `#9A928B`. Destructive actions use `red-600`, not orange, so purge never looks like a normal button. Contrast for all text ≥ 4.5:1.

## Demo steps → UI actions

| Your step | UI screen | API |
|---|---|---|
| Bring up a (backend) cluster | Clusters → Add | `POST /v1/clusters` with typed secret or secret_ref; probe runs; capabilities shown as measured |
| Form a control-plane quorum | Control plane | members, leader, quorum, fleet; "Add node" shows the `shunt-control join` line to run on the host |
| Add a bucket | Buckets → Adopt / Create | `adopt` or `CreateBucket` through a proxy; placement appears via SSE |
| Add another bucket (target) | Bucket → Expand | `expand --to <cluster>`; generated backend name shown |
| Spread traffic 50/50 | Bucket → Migrate → Ramp | ramp to 0.5; two-phase fence shown as pending → applied; live write split |
| Drain traffic | Ramp to 1.0, then Mover → Start | all writes to target; mover progress per range |
| Cut off the old bucket | Cutover | fence + fallback-reads window; refuses on in-flight multipart |
| Forget it | Purge → Forget → Remove cluster | dry-run → confirmation token → purge; CUTOVER → ACTIVE; cluster remove when unreferenced |

---

## UI-0 — API readiness

```
UI-0: make the control API browser-ready. Read docs/reference/control-api.md, docs/design/distributed.md §12.6, and docs/prompts/P3c.md (the amendment at the end). Plan mode. Write docs/adr/0017-web-ui.md first from the Decisions block in docs/prompts/webui.md.

Build, adding only what is missing (check each against the current API first):
1. Operation records: POST /v1/operations for every long-running action (ramp, migrate start, cutover, purge-source, cluster remove) returning an id; GET /v1/operations/<id> with phase, waiting-on proxy names, progress, outcome, error; GET /v1/operations?placement=… listing recent ones. The CLI's --wait polls the record.
2. GET /v1/events: server-sent events from the control node's watch cache. Event types: directory (record changed, with the record), fence (operation id, phase, waiting-on), fleet (proxy joined/left/stale/applied), telemetry (a merged 10 s window is ready). Reconnect with Last-Event-ID replays from the cache.
3. Read models over the cache: GET /v1/fleet (proxies: id, host, applied revision, stale, last heartbeat, version), GET /v1/clusters/<name>/view (health, measured|assumed capabilities, dependent placements, endpoints), GET /v1/placements/<tenant>/<bucket>/view (state, ramp rule and phase, fence status, fallback reads, mover ranges and cursors, in-flight multipart count on source), GET /v1/audit?limit=… (change records with actor).
4. Dry-run and confirmation: purge-source and cluster remove accept dry_run=true and return what would happen plus a confirmation token bound to that result; the real call must present the token. The CLI uses the same path.
5. Control-plane view: GET /v1/control (members, leader, quorum, DB size, last compaction, join command template for a new node).
6. CORS is not enabled; the UI is same-origin on the control port. Every new endpoint is in docs/reference/control-api.md with an example.

Tests: operation records through a full ramp → cutover → purge on the make fleet stack; SSE replay after a dropped connection; dry-run token rejected when the state changed underneath it; route table exported as JSON for the UI parity test in UI-2.

Acceptance: walkthrough.sh green using operation records for --wait; an SSE client sees every directory change made by the CLI within one heartbeat.
```

## UI-1 — Telemetry emission

```
UI-1: emitted percentiles. Read docs/telemetry-catalog.md, docs/design/distributed.md §12.6 (fleet decisions), ADR-0017. Plan mode.

Build:
1. Proxy: HdrHistogram sketches (hdrhistogram-go, MIT; add to deps.md and THIRD_PARTY_NOTICES) with 1 µs–120 s range, 3 significant figures, one per (window, series, op_class, cluster). Series: client_total (first byte in to last byte out), upstream_ttfb, upstream_total, proxy_overhead (client_total − upstream_total). Op classes: read, write, list, delete, multipart, other. Windows are 10 s, rotated on the clock. Counters per window: requests, bytes_in, bytes_out, errors by status class.
2. Heartbeat carries the last completed window: sketches compressed (HdrHistogram's own encoding), counters plain. Bound the payload: report a size test at 6 op classes × 4 series × N clusters and document the ceiling.
3. Control node: merge sketches per window into fleet, per-cluster, and per-proxy series; keep a 60-minute ring of 10 s windows in memory; emit p50, p90, p99, p99.9, max, and count per series. GET /v1/telemetry/series?scope=fleet|cluster:<name>|proxy:<id>&series=…&op=…&from=…&to=… returns the ring slice; GET /v1/telemetry/latest returns the newest window for every scope. SSE telemetry events announce a merged window.
4. The catalog gets rows for the heartbeat metrics and the control node's own telemetry_merge_seconds; the proxy's existing Prometheus metrics are unchanged.
5. Tests: merge of two proxies' sketches equals the sketch of the union (property test with random latencies); percentile error within HdrHistogram's stated bound; ring eviction; payload size; the walkthrough's verify tool cross-checks its own observed p99 against the API's within tolerance.

Acceptance: during make fleet, /v1/telemetry/latest shows two proxies, both clusters, and fleet p99 that matches the verify tool within 10 %.
```

## UI-2 — Scaffold, theme, live data

```
UI-2: the web app skeleton. Read ADR-0017, docs/reference/control-api.md, and the route table JSON from UI-0. Plan mode.

Build:
1. web/ with Vite + React + TypeScript + Tailwind; Recharts. package.json pinned; pnpm or npm lockfile committed. Tailwind config with the ember/ink palette from ADR-0017 and a dark-only theme. make ui, make ui-dev (Vite dev server proxying /v1 to a running control node), make ui-lint, make ui-test. web/THIRD_PARTY_NOTICES generated at build from package.json (production deps) and merged into the repo's THIRD_PARTY_NOTICES by make licenses.
2. Embedding: shunt-control serves web/dist at / with embed.FS; the committed stub index.html keeps go build working; SPA fallback for client routes; API stays under /v1.
3. App shell: left nav (Control plane, Clusters, Buckets, Migrations, Telemetry, Audit), top bar with control node name, quorum state, fleet count, and stale-proxy badge; token entry once, kept in sessionStorage; a global SSE client feeding a small store (Zustand or plain React context, no larger state library); toast for operation outcomes.
4. Typed API client generated from docs/reference/control-api.md or written by hand against the route table; a parity test in Go that loads the route table and the client's route list and fails on any UI route the CLI does not have, or any API route the UI reaches that the CLI cannot.
5. Components: Card, Stat, StateBadge (ACTIVE / RAMPING / MIGRATING / CUTOVER with distinct ember shades), FenceStatus (pending with waiting-on names vs applied), ConfirmDrawer (dry-run result, then confirm), CopyLine (for join commands), Sparkline.
6. Accessibility: keyboard reachable, focus rings in ember-300, contrast ≥ 4.5:1 checked by a test.

Acceptance: make ui && make build produces one shunt-control binary that serves the app; the app connects to make fleet, shows quorum and two proxies live within one heartbeat; parity test green; ui-lint clean.
```

## UI-3 — Control plane, clusters, buckets

```
UI-3: the operational screens. Read ADR-0017 and UI-0's read models. Plan mode.

Build:
1. Control plane: members with role and health, leader, quorum yes/no with the majority math shown, DB size vs quota, fleet table (proxy id, host, applied revision vs current, stale badge, last heartbeat, version). "Add node" opens a drawer that renders the exact shunt-control join line for a new name and peer URL with a copy button, and highlights the member when it appears.
2. Clusters: list with type, scheme, region, health, measured|assumed capabilities, dependent placement count. Add drawer: endpoint, scheme, region, secret as typed value or secret_ref; runs the probe and shows the profile as measured before enabling Save; assumed profiles show a warning. Cluster detail: capability table, endpoints, health history sparkline from telemetry, dependents. Remove: dry-run → confirmation, refused with the reason when referenced. Read-only toggle through the fence.
3. Buckets: list by tenant with state badges and cluster; Adopt drawer (cluster, bucket, tenant, keys via secret_ref); Create; bucket detail shows placement, names per cluster, read-only toggle, and an Expand drawer (target cluster; shows the generated backend name and creates it). Expand leads to the Migrations screen for that bucket.
4. Every action creates an operation record and shows its outcome as a toast; refusals show the API's reason text verbatim.

Acceptance: from a fresh make fleet, an operator completes steps 1–4 of the demo table entirely in the UI; each action appears in the audit tail with the token's actor.
```

## UI-4 — Migration wizard (the demo flow)

```
UI-4: the migration screen. Read docs/design/distributed.md §12.6 (two-phase ramp, fence, stale mode), docs/migrating.md, and UI-0's placement view. Plan mode.

Build a single Migrations screen per bucket, top to bottom in the order of the demo:
1. Header: source → target, state badge, fence status component always visible (pending phase with waiting-on names, or applied at revision N).
2. Ramp: a slider with presets 1 %, 25 %, 50 %, 100 %, and a prefix-rule editor; Apply creates the operation and the slider shows pending until both fence phases are applied. Below it a live write-split bar (source vs target writes over the last window, from telemetry) and a read-outcome bar (target hit, fallback to source, miss). The 50/50 preset is the demo's step 5; 100 % is the drain in step 6.
3. Mover: Start (refused with the reason on an assumed profile or a non-conditional target unless "accept lost-write window" is checked, with the ADR-0004 text shown), per-range progress bars, copied / already-there / vanished / failed counts, ledger tail; convergence shown as "pass N copied 0" and fallback reads trending to zero on a sparkline.
4. Cutover: window selector, in-flight multipart count on the source shown before the button, refused reasons verbatim; fence progress until applied.
5. Purge: dry-run drawer listing object count and bytes to delete and the bucket name, confirmation token, then progress; refused before cutover with the reason.
6. Forget: CUTOVER → ACTIVE (drops the source), and a "Remove cluster" shortcut when the source cluster has no other dependents.
7. A stale proxy during any step shows a banner naming it and that writes on this bucket answer 503 from it.

Acceptance: the eight demo steps completed in the UI on make fleet with the verify tool running throughout at 0 errors; every refusal in docs/migrating.md is reachable and shows the API's message; a Playwright test drives the whole flow.
```

## UI-5 — Telemetry dashboard

```
UI-5: telemetry. Read UI-1's series API and ADR-0017. Plan mode.

Build:
1. Scope selector: fleet, cluster, proxy; window selector: 5, 15, 60 minutes; op-class filter.
2. Panels, each a Recharts line or area chart over the 10 s windows: request rate by op class; throughput bytes in and out; latency with p50, p90, p99, p99.9 as separate lines for client_total, upstream_ttfb, upstream_total, proxy_overhead (four small multiples, shared x-axis); error rate by status class. Values are rendered exactly as received; the UI performs no percentile math, and a test asserts the chart data equals the API payload.
3. Cluster comparison panel: the same series for two clusters side by side, which is the ramp-hold judgment made visible (source vs target on the same op mix).
4. Stat tiles at the top: current p99 client_total, upstream p99 per cluster, proxy overhead p99, requests/s, bytes/s.
5. Live update from SSE telemetry events; no polling.

Acceptance: during a make fleet walkthrough, the dashboard shows the 50/50 split appear in the cluster comparison and the p99 lines within 10 % of the verify tool's report; a screenshot is checked into docs/.
```

## UI-6 — Demo polish and e2e

```
UI-6: make it demoable by someone else. Plan mode.

Build:
1. make demo-ui: brings up make fleet, builds and serves the UI, prints the URL and token, and starts the verify workload; make demo-ui-down.
2. Playwright e2e in web/e2e: the full demo table through the UI against make fleet, asserting state badges, fence transitions, split bars, and the final ACTIVE-on-target with the source cluster removed; runs in CI with the fleet job.
3. Empty and error states for every screen (no control plane, no token, quorum lost, stale proxy) designed, not defaulted.
4. docs/demo-ui.md: the demo script as talking points per screen, with what the audience should notice at each step (fence pending → applied, split bar, fallback reads to zero, purge dry-run).
5. G1 on web/ (unused components, dead routes), G4 covering npm production dependencies with their licenses in THIRD_PARTY_NOTICES, and a check that no dependency is copyleft.

Acceptance: a person who has not seen shunt runs make demo-ui and completes the demo from docs/demo-ui.md without asking a question; Playwright green in CI.
```
