
Phase 3d: scale the mover and the decisions. Read docs/design/distributed.md §12.6 and ADR-0004. Plan mode.

Build:
1. shunt-mover as its own binary (ADR-0009's planned resolution): migrations split into prefix ranges recorded under /migrations/<id>/ranges; movers claim ranges with leases, checkpoint cursors in etcd, append ledgers to object storage; the mover contract (conditional PUT, re-HEAD, part layout, ratio-1 gate, capability refusal) unchanged. Any number of movers; takeover of an expired claim within one TTL.
2. Fleet decisions: cutover window, ramp hold, and convergence computed from heartbeat counters across all live proxies by the control node handling the request; Prometheus is not consulted.
3. Ramp hold policy from §11 implemented on fleet numbers; hold is a directory record so every control node sees it.
4. Tests: two movers on one migration under the property test; a mover killed mid-range; cutover refused while any proxy still reports fallback reads; hold engages on a synthetic target-latency regression across the fleet.

Acceptance: evacuate.sh with three movers finishes faster than with one and verifies identically; the killed-mover test converges; bin/shunt no longer links the AWS SDK.
