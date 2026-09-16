# Shunt POC track

The POC proves three claims, on the product's skeleton, so nothing is thrown away:

1. Verify-and-resign SigV4 works with real clients against real backends.
2. One endpoint hides which backend a bucket lives on, at measured overhead.
3. A bucket moves between backends with no per-object state and no client change.

Five sessions. Same repo, same layout, same `CLAUDE.md` rules as the full design; each prompt is a subset of a full phase with named cuts. Put this file in the repo as `docs/POC.md` so Claude Code can see what is deliberately out of scope.

**Routine per session:** Kickoff prompt (from the design, §7) → the POC prompt → G2 (validation) → commit and tag `poc-<n>`. Run G1 (simplicity) and G4 (licenses) once, after POC-4.

---

## POC-0 — Bootstrap-lite (subset of P0)

```
POC phase 0. Read docs/DESIGN.md (§1–§4, §2.10) and docs/CONTEXT.md, then docs/POC.md. This is a proof of concept built on the product's skeleton: same layout, same rules, fewer features. Anything docs/POC.md marks as deferred is out of scope even if it looks easy.

Build:
1. Go module github.com/blakegolliher/shunt; layout per §4 but only the packages the POC touches: listener, s3, sigv4, auth, directory, upstream, proxy, migrate, telemetry, admin, config. YAML via go.yaml.in/yaml/v4.
2. cmd/shunt with `version` and `check-config` (unknown keys are errors; cluster type/scheme/region required).
3. CLAUDE.md: §2.10 verbatim plus "no per-object state, ever" and "docs/POC.md lists what not to build".
4. Makefile: build, test, race, lint, fuzz (30s per target), bench, bench-compare. .golangci.yml with the P0 linter set. GitHub Actions: ci only. LICENSE Apache-2.0, NOTICE.
5. docs/: adr/0001-auth-modes.md and adr/0002-late-failure-compensation.md (short), telemetry-catalog.md with the six P1 metrics, reference/, STATUS.md, bench/.
6. test/e2e/docker-compose.yml with Garage and MinIO, a script that generates a self-signed wildcard cert for the domain in CONTEXT.md, and a make target that brings both up and waits for health.

Skip (deferred): CodeQL, Semgrep, nightly fuzz, Dependabot, the Diátaxis skeleton, the failure-modes red-team.

Acceptance: `make all` green on a clean checkout; check-config rejects each invalid sample naming the offending key; `make e2e-up` leaves Garage and MinIO healthy.
```

## POC-1 — Streaming proxy with minimal telemetry (subset of P1)

```
POC phase 1. Read docs/DESIGN.md §1.4, §2.1, §2.8, §2.9. Build P1 items 1, 2, 3, 4, 5, 7, 8 with these cuts.

Listener: TLS from files, SNI map, TLS 1.2 minimum. No hot reload, no mTLS, no SO_REUSEPORT, no PROXY protocol.
Proxy: one static cluster, round-robin across endpoints, Host preserved (passthrough), io.CopyBuffer through a pooled 256 KiB buffer, Expect: 100-continue chained end to end, idle-progress timeouts for data ops, a simple graceful drain.
Admin: /-/healthz, /-/metrics, /debug/pprof, and /-/slow backed by a 100-entry slow ring with the timing breakdown from §2.7.
Telemetry: JSON access log with the fixed schema, the six catalog metrics, the slow ring. Nothing else.
s3diff v0: exactly P1 item 7, against Garage and MinIO, passthrough mode.
Bench: 4 KiB, 1 MiB, 1 GiB at 1 and 64 connections, direct vs via shunt; report added p50/p99 and proxy CPU-seconds per GiB in docs/bench/poc1.md.

Acceptance: s3diff clean on both backends; an unauthorized 5 GiB PUT fails before the client sends body bytes; a mid-GET backend kill never yields a silent short read; bench recorded.
```

## POC-2 — Resign auth: the phase that decides it (P2 nearly whole)

```
POC phase 2. This phase decides whether the concept works; do not cut the hard parts. Read docs/DESIGN.md §2.2, ADR-0001, ADR-0002. Use plan mode; show me the plan first.

Build P2 items 1, 2, 3, 5, 6, 7, 8 in full: SigV4 canonicalization with the S3 rules, header and presigned verification, upstream signing with the cluster's region, aws-chunked signed and unsigned with trailers, SigV4A rejected cleanly, static YAML credential store, the secret-leak test, fuzz targets for canonicalization and the chunk decoder, s3diff's signing-mode dimension.

Cuts: item 4 compensation is log-and-alert only (no compensating delete; record the gap in ADR-0002). Credential store has no hot reload. `shunt probe` reports only: sha256 enforcement, unsigned-trailer support, If-None-Match: * on PUT, checksum algorithms honored, unsigned GET / status.

Run s3diff in resign mode against Garage, MinIO, and every real endpoint in docs/CONTEXT.md (VAST; AWS if credentials are present). Every gap goes into docs/reference/backend-compat.md with the probe output that proves it.

Acceptance: s3diff clean across all signing modes on every available backend, or a documented gap per failure with the backend named; secret-leak test green; fuzz 30s clean; resign overhead measured against the POC-1 baseline in docs/bench/poc2.md.
```

## POC-3 — One endpoint over mixed backends (subset of P3b)

```
POC phase 3. Read docs/DESIGN.md §0 decisions 13–14, §2.3, §2.4. Build P3b items 1, 2, 3, 5, 7 on the file directory backend, with these cuts.

Directory: placements keyed by (tenant, bucket) with backend bucket names per cluster; one tenant is enough for the demo but key by tenant anyway. Clusters carry type, scheme, region, credentials, and a hand-written capability profile copied from probe output (no auto-emit). States ACTIVE / RAMPING / MIGRATING / CUTOVER with validated transitions and the versioning refusal; `shunt directory get|set-state|validate`.
Request path: resolve placement; rewrite bucket name in path, Host, and every XML response that echoes it; sign with the cluster's region; synthesize ListBuckets; CreateBucket to the tenant's default cluster with a generated backend name.
Upstream: static endpoint mode only (AWS is a one-entry list for the POC). Round-robin plus skip-on-connect-error. No P2C/EWMA, no ejection, no dns mode, no 301 region handling beyond failing loudly.
uploadId codec with the tolerance rule.
Metrics: shunt_requests_total gains cluster and cluster_type labels; nothing else new.

Acceptance: s3diff clean with buckets spread across Garage, MinIO, and the real backends from CONTEXT.md in one run; the same bucket name under two tenants proven isolated by a test; ListBuckets spans clusters; no backend name leaks (s3diff body diff catches it); uploadId round-trip.
```

## POC-4 — Migration demo (subset of P5)

```
POC phase 4. Read docs/DESIGN.md §2.5 and write a short docs/adr/0004-migration-races.md first (delete/copy race, ETag drift across backend types, versioning exclusion, monotonic ramp). Use plan mode.

Build P5 items 1, 2, 3, 4, 7 with these cuts: listing merge for ListObjectsV2 only (v1 and ListMultipartUploads deferred); the property test (item 5) runs 2 minutes in CI and once for 10 minutes locally with the result recorded in STATUS.md; docs are a single how-to.
test/mover exactly as specified: If-None-Match: *, re-HEAD after copy, part-layout preservation, cursor file, JSONL ledger to a bucket, refuses to start below ratio 1.

Then write test/e2e/demo.sh. It must, using only aws-cli pointed at shunt with one unchanged config:
1. create a bucket that lands on cluster A; write 1,000 objects including multipart ones;
2. keep a writer running while the operator ramps 0.01 → 0.25 → 1.0 (hash), showing shunt_ramp_writes_total{side} moving;
3. start the mover; wait for shunt_migration_fallback_reads_total to stop increasing;
4. cut over; verify every object by GET and ETag; show the listing diff is empty;
5. print the directory's change log with timestamps (the file backend's equivalent of directory_changes) and the first lines of the ledger;
6. print the added p50/p99 during the migration from the bench harness.

Acceptance: demo.sh green on Garage → MinIO, and on VAST → MinIO (or → AWS) using CONTEXT.md endpoints; property test green; docs/bench/poc4.md filled.
```

## POC-5 — Operator walkthrough (subset of P3c's API, P5's operator verbs)

The request, condensed; the approved plan and its decisions are recorded in ADR-0008, ADR-0009 and the amendments to ADR-0004 and ADR-0006.

```
Goal: an operator runs this sequence with copy-paste commands and verifies it, against two VAST clusters over http,
and it runs unattended in CI against Garage and MinIO:
1. bucket data01 on vast01 (http)  2. shunt on :8008  3. a client at http://shunt:8008, bucket name unchanged
4. reads and writes continuously from here to the end  5. bucket data01-001 on vast02 (or shunt creates it)
6. add vast02 live, no restart  7. ramp writes 50/50 by key hash  8. show the ~50/50 split and reads succeeding on both
sides; then ramp to 1.0  9. mover until converged; cutover  10. purge data01 from vast01; remove vast01
Throughout: every write and every read succeeds, wherever the object lived.

Build: clusters in the directory; a REST control API on the admin listener (bearer token or loopback), one handler per
operation, every mutation through the FileDir write path, secrets only as secret_ref; CLI verbs on the API with --json;
test/mover promoted to `shunt migrate run`; purge-source (CUTOVER only, empty listing diff, zero fallback reads over a
window) and cluster remove refused while referenced; `shunt verify` with a debug route header shunt emits only for
X-Shunt-Debug: 1; listener.plaintext for labs, warned on every banner line; docs/walkthrough.md and
test/e2e/walkthrough.sh (verify from step 4 to 10, failing on any error), green on Garage → MinIO in CI; STATUS.md.
```

Acceptance: `make walkthrough` green with verify at 0 errors and the ratio-0.5 split within 40–60%; the walkthrough job in CI; docs/walkthrough.md transcribed from a real run. VAST → VAST is documented, not yet run.

---

## What the POC proves, and what it doesn't

| Claim | Proven by |
|---|---|
| Transparent streaming proxy at measured overhead | POC-1 s3diff + bench |
| Resign SigV4 works with real clients and real backends | POC-2 s3diff across signing modes on every backend |
| One endpoint, backend hidden, mixed backend types | POC-3 mixed-backend s3diff, ListBuckets, name-leak check |
| Migration without per-object state or client change | POC-4 demo.sh + property test |
| An operator can run a migration from copy-paste commands, live, and verify it | POC-5 walkthrough.sh + `shunt verify` |

Not proven, on purpose: production resilience (ejection, hot reload, drain under load), scale of the directory (Postgres), tiering, packaging, telemetry beyond RED + slow ring, chaos. Each has a home in the full design.

## Deferred items and where they live

| Deferred in POC | Picks up in |
|---|---|
| Cert hot reload, mTLS, SO_REUSEPORT, PROXY protocol | P1, P3a |
| P2C/EWMA, ejection, retry table, dns endpoint mode, AWS 301 handling | P3a, P3b |
| Compensating delete, credential hot reload, STS | P2 |
| Capability profile auto-emit from probe | P3b |
| Postgres, shunt-control, snapshots, audit table | P3c |
| Full metric catalog, traces, metering, eBPF/TCP_INFO | P4 |
| ListObjects v1 and ListMultipartUploads merge, dual-delete metrics | P5 |
| The mover as its own `shunt-mover` binary, taking the AWS SDK out of `bin/shunt` (ADR-0009) | P5 |
| Tiering native and emulated, RestoreObject | P6 |
| Profiling, buffer sweep, runtime tuning | P7 |
| Packaging for systemd and RKE2, doctor, chaos, nightly fuzz, release | P8a, P8b |

## After the POC

If POC-2 passes on VAST, MinIO, and AWS, the concept is sound and the remaining risk is operational. Resume the full order at P0's skipped items, then P1's, then P3a, and continue as §7 prescribes. If POC-2 fails on a backend and the gap cannot be closed with compensation, that backend needs passthrough mode and its own credentials — a product decision, not a code one.
