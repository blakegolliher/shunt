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

The `bucket` label appears only on the migration metrics, and only while a bucket is not ACTIVE: a placement that returns to ACTIVE stops exporting them, so cardinality is bounded by the number of migrations in flight, not by the number of buckets.

Deferred to P4: the full catalog (connection, auth, TLS, routing, TCP, runtime signals). Those rows are added when their phase begins, not before.
