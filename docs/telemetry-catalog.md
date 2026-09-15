# Telemetry catalog

Every metric shunt exports is listed here before it is implemented; CI fails on a metric not in the catalog (docs/DESIGN.md §2.7). Label cardinality is bounded: `op`, `status_class`, `cluster`, `cluster_type`, `endpoint`, `tenant`, `mode`, `reason`. `bucket` only for buckets in a non-ACTIVE state or on an allowlist. Object keys never.

Prefix: `shunt_`. Phase column is the full-design phase; the POC column says which POC session implements it.

| Name | Type | Labels | Unit | Buckets | Description | Phase | POC |
|---|---|---|---|---|---|---|---|
| `shunt_requests_total` | counter | `op`, `status_class` (+ `cluster`, `cluster_type` from POC-3 / P3b) | requests | — | Requests completed, by S3 operation and response status class | P1 | POC-1 |
| `shunt_request_duration_seconds` | histogram | `op` | seconds | 1ms … 60s, log-spaced, 16 buckets | Client-observed request duration, first byte in to last byte out | P1 | POC-1 |
| `shunt_upstream_ttfb_seconds` | histogram | `op`, `cluster` | seconds | 1ms … 60s, log-spaced, 16 buckets | Time from upstream request sent to first response header byte | P1 | POC-1 |
| `shunt_bytes_in_total` | counter | `op` | bytes | — | Request body bytes received from clients | P1 | POC-1 |
| `shunt_bytes_out_total` | counter | `op` | bytes | — | Response body bytes sent to clients | P1 | POC-1 |
| `shunt_inflight` | gauge | `op` | requests | — | Requests currently in flight | P1 | POC-1 |
| `shunt_auth_failures_total` | counter | `reason` ∈ {`missing`, `malformed`, `unknown_key`, `signature`, `skew`, `expired`, `sigv4a`, `token`, `missing_sha256`, `unsigned_headers`, `chunk_signature`, `trailer`} | failures | — | Client requests rejected by shunt's own verifier in resign mode, by cause | P2 | POC-2 |
| `shunt_auth_duration_seconds` | histogram | `mode` ∈ {`header`, `presigned`} | seconds | 10µs … 100ms, log-spaced, 16 buckets | Time to parse and verify the client signature (excludes chunk verification, which is per body byte) | P2 | POC-2 |
| `shunt_compensation_total` | counter | `reason` ∈ {`sha256`, `trailer`}, `outcome` ∈ {`logged`, `deleted`, `failed`} | events | — | Late payload-check failures discovered after the upstream write (ADR-0002); the POC only ever records `logged` | P2 | POC-2 |

Deferred to P4 (full catalog: connection, auth, TLS, routing, TCP, runtime signals) and P5 (`shunt_ramp_writes_total{side}`, `shunt_migration_fallback_reads_total`, which POC-4's demo reads). Those rows are added when their phase begins, not before.
