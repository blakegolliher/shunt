> **Superseded by `docs/prompts/webui.md` (ADR-0017, branch `distributed`, 2026-09-22).** Item 1 (htmx, no Node) is replaced by the Vite + React stack decided there; items 2 to 5 are carried into its prompts UI-2 to UI-4. OIDC identity (the P3c amendment) stays deferred and is not in webui.md.

Phase 3e: web UI. Plan mode. Read docs/design/distributed.md §12.6 and docs/reference/control-api.md.

Build:
1. Go html/template + htmx over the SSE stream, embedded in shunt-control via embed.FS and served from every control node on the same TLS. No Node toolchain, no separate web server, no build step beyond go build. htmx vendored under web/vendor/ with its BSD-2-Clause notice carried into THIRD_PARTY_NOTICES; uPlot (MIT) only if a chart is genuinely needed, same treatment.
2. Screens: fleet (proxies, applied revision, stale in red); clusters (health, measured capabilities, dependents); buckets (placements by tenant, state, read-only); migration (ramp split, fallback reads trending, fence state with waiting-on names, per-range mover progress); audit tail; the walkthrough as guided steps that call the same operations.
3. Every action calls the control API exactly as the CLI does. A test enumerates the API routes and proves the UI reaches none the CLI cannot and holds no state of its own.
4. Fence state is never hidden: a pending phase shows as pending with the names it is waiting on, distinct from an applied ratio.
5. Destructive actions use the API's dry-run and confirmation token; the UI shows the dry-run result before enabling confirm.

Acceptance: the walkthrough completed end to end from the UI on the three-node e2e control cluster; the route-parity test green; a stale proxy visible within one heartbeat; make licenses shows htmx's notice.
