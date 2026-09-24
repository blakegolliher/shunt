# Telemetry catalog

Every metric shunt exports is listed here before it is implemented; CI fails on a metric not in the catalog (docs/DESIGN.md §2.7). Label cardinality is bounded: `op`, `status_class`, `cluster`, `cluster_type`, `endpoint`, `tenant`, `mode`, `reason`. `bucket` only for buckets in a non-ACTIVE state or on an allowlist. Object keys never.

Prefix: `shunt_`. Phase column is the full-design phase; the POC column says which POC session implements it.

| Name | Type | Labels | Unit | Buckets | Description | Phase | POC |
|---|---|---|---|---|---|---|---|
| `shunt_requests_total` | counter | `op`, `status_class`, `cluster`, `cluster_type` | requests | — | Requests completed, by S3 operation, response status class, and the cluster that served them. `cluster` and `cluster_type` are `none` when shunt answered without an upstream request (synthesized ListBuckets, a refusal, an auth failure) | P1 | POC-1 (labels POC-3) |
| `shunt_request_duration_seconds` | histogram | `op` | seconds | 1ms … 60s, log-spaced, 16 buckets | Client-observed request duration, first byte in to last byte out | P1 | POC-1 |
| `shunt_upstream_ttfb_seconds` | histogram | `op`, `cluster` | seconds | 1ms … 60s, log-spaced, 16 buckets | Time from upstream request sent to first response header byte | P1 | POC-1 |
| `shunt_bytes_in_total` | counter | `op` | bytes | — | Request body bytes received from clients | P1 | POC-1 |
| `shunt_bytes_out_total` | counter | `op` | bytes | — | Response body bytes sent to clients | P1 | POC-1 |
| `shunt_inflight` | gauge | `op` | requests | — | Requests currently in flight | P1 | POC-1 |
| `shunt_auth_failures_total` | counter | `reason` ∈ {`missing`, `malformed`, `unknown_key`, `signature`, `skew`, `expired`, `sigv4a`, `token`, `missing_sha256`, `unsigned_headers`, `chunk_signature`, `trailer`} | failures | — | Client requests rejected by shunt's own verifier in resign mode, by cause | P2 | POC-2 |
| `shunt_auth_duration_seconds` | histogram | `mode` ∈ {`header`, `presigned`} | seconds | 10µs … 100ms, log-spaced, 16 buckets | Time to parse and verify the client signature (excludes chunk verification, which is per body byte) | P2 | POC-2 |
| `shunt_compensation_total` | counter | `reason` ∈ {`sha256`, `trailer`}, `outcome` ∈ {`logged`, `deleted`, `failed`} | events | — | Late payload-check failures discovered after the upstream write (ADR-0002); the POC only ever records `logged` | P2 | POC-2 |
| `shunt_route_state` | gauge | `bucket`, `state` ∈ {`ACTIVE`, `RAMPING`, `MIGRATING`, `CUTOVER`} | — | — | 1 for the placement's current state, 0 for the others. `bucket` is bounded: only buckets that are not ACTIVE are exported | P5 | POC-4 |
| `shunt_ramp_ratio` | gauge | `bucket` | ratio | — | The placement's current ramp ratio, 0 to 1. Exported only while RAMPING | P5 | POC-4 |
| `shunt_ramp_writes_total` | counter | `bucket`, `side` ∈ {`primary`, `source`} | writes | — | Writes during RAMPING, by the side the key's hash or prefix rule sent them to | P5 | POC-4 |
| `shunt_migration_fallback_reads_total` | counter | `bucket` | reads | — | Reads served from the source because the primary answered 404 during MIGRATING. Flattening to zero is what convergence looks like | P5 | POC-4 |
| `shunt_migration_dual_delete_total` | counter | `bucket`, `outcome` ∈ {`both`, `source_missing`, `source_failed`, `primary_failed`} | deletes | — | Deletes sent to both clusters during RAMPING, MIGRATING and (since POC-5) CUTOVER, source first, then primary (ADR-0004 race 1). `source_failed`: the source leg failed and the primary leg was still sent; the object may come back when the mover copies it. `primary_failed`: the primary leg failed, whatever the source leg did; the client got the error and the object may be gone from the source while still on the primary (docs/migrating.md). Alert on both | P5 | POC-4 |
| `shunt_listing_merge_seconds` | histogram | `bucket` | seconds | 1ms … 60s, log-spaced, 16 buckets | Time to serve one merged ListObjectsV2 page from both clusters | P5 | POC-4 |
| `shunt_migration_refused_writes_total` | counter | `bucket`, `reason` ∈ {`hold`, `stale`} | writes | — | Writes and deletes shunt answered with 503 + Retry-After instead of routing (ADR-0016). `hold`: the key is inside a ramp step that has not reached every proxy yet; a few seconds per step on a fleet, never on a single proxy. `stale`: this proxy has lost its lease with the control node and will not route a write on a bucket that is moving. Alert on sustained `stale` | P5 | POC-6 |
| `shunt_fleet_members` | gauge | `state` ∈ {`live`, `silent`} | proxies | — | Control plane (shunt-control) only: members that have registered, by whether their heartbeat is within the lease (ADR-0016). A `silent` member blocks the first step of any bucket's migration until it returns or is forgotten | P3c | POC-6 |
| `shunt_fleet_stale` | gauge | — | — | — | Members only: 1 while this proxy's lease has run out (it runs from the last granted heartbeat's send time for the shorter of the control plane's grant and `control.lease_ttl`), so writes on moving buckets are refused; 0 otherwise (ADR-0016) | P3c | POC-6 |
| `shunt_fleet_fence_wait_seconds` | histogram | — | seconds | 1ms … 60s, log-spaced, 16 buckets | Control plane (shunt-control) only: time from a fenced change being written to every live member reporting it installed, per fence round (ADR-0016) | P3c | POC-6 |
| `shunt_directory_install_total` | counter | `result` ∈ {`installed`, `older`, `refused`, `lineage`} | versions | — | A directory version a member or control node was handed: installed; dropped as no newer than the installed one; refused (validation or preparation failed, the last good version stays); or from another lineage (ADR-0021 D1). Proposed, catalogued before code | H1 | — |
| `shunt_directory_install_seconds` | histogram | — | seconds | 1ms … 60s, log-spaced, 16 buckets | Time to validate, prepare and publish one directory version, cache write excluded (ADR-0021 D1). Proposed | H1 | — |
| `shunt_directory_cache_failures_total` | counter | `stage` ∈ {`write`, `fsync`, `rename`, `dir_fsync`} | failures | — | Members only: a restart-cache write that failed at a stage; the in-memory version stays and the cache is not durable for it (ADR-0021 D1). Proposed | H1 | — |
| `shunt_barrier_duration_seconds` | histogram | `phase` ∈ {`hold`, `drain`, `commit`, `settle`} | seconds | 10ms … 1h, log-spaced, 16 buckets | Control plane only: time a drain barrier spent in each phase (ADR-0021 D2). Proposed | H2 | — |
| `shunt_barrier_blockers` | gauge | `code` ∈ {`old_requests`, `backend_outcome_unknown`, `proxy_missing`, `incarnation_unresolved`, `cache_not_durable`, `install_backpressure`, `multipart_open`, `worker_unresolved`, `capability_missing`, `quorum_unavailable`} | blockers | — | Control plane only: blockers now holding open barriers, by code; alert on any held past the barrier's budget (ADR-0021 D2). Proposed | H2 | — |
| `shunt_lease_grant_errors_total` | counter | `reason` ∈ {`sequence`, `no_grant`, `behind`, `late`, `lineage`, `unreachable`} | heartbeats | — | Members only: a heartbeat whose answer granted no lease, by why (ADR-0021 D2, T07). Proposed | H2 | — |
| `shunt_fleet_unresolved_incarnations` | gauge | — | incarnations | — | Control plane only: proxy incarnations that ended without a clean retirement and still block barriers (ADR-0021 D2). Proposed | H2 | — |
| `shunt_operations` | gauge | `status` ∈ {`pending`, `running`, `blocked`}, `effect` ∈ {`none`, `committed`, `uncertain`} | operations | — | Control plane only: unfinished operations, and ended ones whose effect is uncertain (`status` then the terminal one); alert on a blocked or uncertain one (ADR-0021 H0). Proposed | H2 | — |
| `shunt_recovery_phase` | gauge | `phase` ∈ {`none`, `recovering`, `planned`, `reconciling`, `activating`, `active`} | — | — | Control plane only: 1 for the current recovery phase, 0 for the others; alert on any phase but `none` and `active` held past a window (ADR-0021 D3). Proposed | H4 | — |
| `shunt_control_join_phase` | gauge | `phase` ∈ {`prepared`, `learner_added`, `bootstrap_saved`, `started`, `catching_up`, `promoted`, `succeeded`} | — | — | Control plane only: 1 for the phase of an unfinished membership change, all 0 when none (ADR-0021 D4). Proposed | H3 | — |
| `shunt_control_health_observation_age_seconds` | gauge | `member` (control members, bounded by the voter and learner count) | seconds | — | Control plane only: age of the last health observation of each control member; past 15 s its health reads `unknown` (ADR-0021 D4). Proposed | H3 | — |
| `shunt_telemetry_merge_seconds` | histogram | — | seconds | 10µs … 1s, log-spaced, 16 buckets | Control node only: time to decode and merge one completed 10 s fleet telemetry window into fleet, cluster and proxy summaries | P4 | UI-1 |

The `bucket` label appears only on the migration metrics, and only while a bucket is not ACTIVE: a placement that returns to ACTIVE stops exporting them, so cardinality is bounded by the number of migrations in flight, not by the number of buckets.

## Heartbeat-carried window series

These are not Prometheus metrics and do not change the proxy's existing Prometheus surface. A
proxy ships compressed HdrHistogram sketches for its last completed 10-second window; every
control node merges them and emits the percentiles below through `/v1/telemetry`. `op_class` is
one of `read`, `write`, `list`, `delete`, `multipart`, or `other`; `cluster` is the serving
cluster (`none` when shunt answered without one). Every summary carries p50, p90, p99, p99.9,
maximum and count in microseconds.

| Series | Dimensions | Unit | Meaning |
|---|---|---|---|
| `client_total` | `op_class`, `cluster` | microseconds | First client request byte received to the last response byte written (the access log's total duration) |
| `upstream_ttfb` | `op_class`, `cluster` | microseconds | Upstream request headers written to upstream response headers received |
| `upstream_total` | `op_class`, `cluster` | microseconds | Upstream request headers written to the last upstream response body byte read |
| `proxy_overhead` | `op_class`, `cluster` | microseconds | `client_total - upstream_total`, clamped at zero; proxy parsing, authentication, routing, signing, and client-side delivery around the upstream exchange |

Each window also carries exact counters by `op_class` and `cluster`: requests, request and
response bytes, and non-success responses by status key (a fixed set of codes, other 4xx, other
5xx, no response, and a read answered 404; docs/reference/control-api.md), and the same counters
by backend cluster for each bucket spread over legs, moving, or watched (at most 32 per proxy
window, the rest summed as `(other)`). The control node sums them for the same fleet,
cluster and proxy scopes as the latency summaries. `/v1/telemetry/series` exposes these retained
window counters as `requests_per_second`, `bytes_in_per_second`, `bytes_out_per_second`, and
`errors_{0,4xx,5xx}_per_second`, `not_found_per_second` and `status_per_second`; the control node divides by the actual window duration and the UI
renders the returned value without further aggregation.

Deferred to P4: the full catalog (connection, auth, TLS, routing, TCP, runtime signals). Those rows are added when their phase begins, not before.
