# shunt web

`make ui` builds this Vite/React application into `dist`; `shunt-control` embeds that directory and
serves it at `/`. `make ui-dev` runs Vite on `127.0.0.1:5173` and proxies `/v1` to
`SHUNT_CONTROL_URL` (default `http://127.0.0.1:9901`). The bearer token stays in `sessionStorage`
and the SSE client uses authenticated `fetch`, never a token-bearing URL.

`src/api/routes.json` is the reviewed list of API routes reached by the browser. The Go parity test
requires every entry to exist in `docs/reference/control-routes.json` and normally to name a CLI
verb. These browser read models deliberately have no one-to-one CLI verb and are named in the
inventory with their rationale:

- `GET /v1/events` — the authenticated live stream has no CLI verb; it announces changes to read
  models that CLI reads already expose.
- `GET /v1/telemetry/series` and `GET /v1/telemetry/latest` — chart data and newest per-scope
  windows; `shunt verify` uses latest internally, but there is no general telemetry-read verb.
- `GET /v1/audit` — the audit tail over changes made by existing mutating verbs.
- `GET /v1/operations` — the general operation list; individual CLI actions expose and poll their
  own records instead.
- `GET /v1/clusters/{name}/view` and `GET /v1/placements/{tenant}/{bucket}/view` — secret-free
  composites of state that CLI commands render through their task-specific output.
- `GET /v1/placements/{tenant}/{bucket}/mover-ledger` — a bounded browser tail of the local
  append-only mover ledger; the CLI writes that ledger but has no tail verb.

Member heartbeat routes are not browser routes. Later UI phases add their read models and operator
actions to the inventory as those screens begin to call them.
