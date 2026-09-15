# STATUS

Current phase: **POC-1 complete, POC-2 next.** Track: docs/POC.md. Full design: docs/DESIGN.md.

## POC track

| Session | Gate | State |
|---|---|---|
| POC-0 | `make all` green on clean checkout; `check-config` rejects every invalid sample naming the key; `make e2e-up` leaves Garage and MinIO healthy | done 2026-09-14 |
| POC-1 | s3diff clean on Garage and MinIO (passthrough); unauthorized 5 GiB PUT fails before body bytes; mid-GET backend kill never a silent short read; bench in docs/bench/poc1.md | done 2026-09-15 |
| POC-2 | s3diff clean across signing modes on every backend or a documented gap each; secret-leak test green; fuzz 30s clean; resign overhead in docs/bench/poc2.md | not started |
| POC-3 | mixed-backend s3diff clean; same bucket name under two tenants isolated; ListBuckets spans clusters; no backend name leaks; uploadId round-trip | not started |
| POC-4 | demo.sh green Garage → MinIO and VAST → MinIO/AWS; property test green; docs/bench/poc4.md | not started |

After POC-4: G1 (simplicity) and G4 (licenses) once, then resume the full order at P0's skipped items (docs/POC.md "After the POC").

## What exists

- `cmd/shunt serve`: TLS listener with SNI map, passthrough proxy to one static cluster, admin listener, drain on SIGTERM.
- `internal/s3`: request model (path and virtual-host), ordered classifier table (75 ops + Unknown/Preflight), own error table.
- `internal/proxy`: one handler; pooled 256 KiB `io.CopyBuffer`; hop-by-hop hygiene; chained `Expect: 100-continue`; idle-progress watchdog for data ops, total deadline for metadata ops; abort on short reads (ADR-0003); Content-Type sniffing suppressed.
- `internal/telemetry`: six catalog metrics on a private registry with a catalog-conformance test, JSON access log (docs/reference/access-log.md), 100-entry slow ring.
- `internal/upstream`: one cluster, HTTP/1.1 transport, round-robin. `internal/listener`, `internal/admin`.
- `test/s3diff`: 202-case direct-vs-via matrix, clean on Garage 2.3.0 and MinIO. `test/bench/s3bench`: direct-vs-via bench; results in docs/bench/poc1.md.

- `cmd/shunt`: `version`, `check-config` (with `proxy.cluster`).
- `internal/config`: schema, validator, 2 valid and 39 invalid samples, fuzz target, benchmarks.
- `internal/s3`: `errors.go`, shunt's own S3 error table (code, status, AWS message) and XML renderer.
- `internal/s3/s3response`: S3 XML structs lifted from versitygw (THIRD_PARTY_NOTICES, docs/deps.md) with local types replacing the SDK; tested against gofakes3 in-process.
- R0 reuse audit done: docs/reuse-findings.md. Chunk readers are copied at the start of POC-2; the eBPF example at P4.
- Skeleton packages with doc.go only: sigv4, auth, directory, migrate.
- Makefile targets: build, test, race, lint, fuzz, bench, bench-compare, e2e-up, e2e-down, all.
- test/e2e: Garage + MinIO compose, wildcard cert script, health wait.

## Known gaps carried forward

- Passthrough preserves `Host` including shunt's port, and backends echo it into `<Location>` of CompleteMultipartUpload (MinIO) with the backend's own scheme. Inherent to passthrough; resign mode (POC-2) rewrites Host. s3diff normalises the port.
- Backends that send no `Content-Type` (Garage) are relayed without one; net/http's sniffing is disabled per response. Found by s3diff, fixed in POC-1.
- The bench's 1 GiB tier runs at 1 connection only (disk on the dev box).

- docs/CONTEXT.md VAST rows are `unknown`; required before POC-2.
- go.yaml.in/yaml/v4 is at a release candidate (v4.0.0-rc.6); bump when v4.0.0 ships.
- ADR-0002 compensation is log-and-alert in the POC (POC-2 cut).
