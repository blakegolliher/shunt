# STATUS

Current phase: **POC-4 built and green on Garage + MinIO; POC-3 and POC-4 both untagged.** The tags wait on one thing: `make check-tls-verify` must pass, which needs a VAST certificate covering its hostname (carried gap below). VAST joins the mixed s3diff run and the VAST → MinIO demo at that point, and only then are `poc-3` and `poc-4` tagged.

## POC track

| Session | Gate | State |
|---|---|---|
| POC-0 | `make all` green on clean checkout; `check-config` rejects every invalid sample naming the key; `make e2e-up` leaves Garage and MinIO healthy | done 2026-09-14 |
| POC-1 | s3diff clean on Garage and MinIO (passthrough); unauthorized 5 GiB PUT fails before body bytes; mid-GET backend kill never a silent short read; bench in docs/bench/poc1.md | done 2026-09-15 |
| POC-2 | s3diff clean across signing modes on every backend or a documented gap each; secret-leak test green; fuzz 30s clean; resign overhead in docs/bench/poc2.md | done 2026-09-15, tagged `poc-2` with carried gaps |
| POC-3 | mixed-backend s3diff clean; same bucket name under two tenants isolated; ListBuckets spans clusters; no backend name leaks; uploadId round-trip | gate met on Garage + MinIO 2026-09-15; **tag held until the TLS gate passes and VAST joins the run** |
| POC-4 | demo.sh green Garage → MinIO and VAST → MinIO/AWS; property test green; docs/bench/poc4.md | gate met on Garage ↔ MinIO 2026-09-15; **VAST → MinIO held behind the same TLS gate** |

After POC-4: G1 (simplicity) and G4 (licenses) once, then resume the full order at P0's skipped items (docs/POC.md "After the POC").

## What exists

- **POC-4: migration without per-object state.** `internal/migrate` (routing table + uploadId codec): every S3 op is classified, and a class plus a placement decides the cluster, whether reads fall back, whether deletes are doubled, and whether listings merge. A completeness test fails the build when an op has no class. `internal/proxy/merge.go` merges `ListObjectsV2` from both clusters, one page per side at a time, with a composite continuation token, a missing side treated as empty, and key encoding normalised across backends. The delete body is buffered within a 1 MiB cap so it can be replayed to the second cluster (ADR-0004). `test/mover` implements the mover contract: conditional write where the backend has it and a HEAD-then-commit guard where it does not, source re-`HEAD` after every copy, part-layout preservation, a cursor, and a JSONL ledger. Operator verbs: `shunt ramp`, `migrate start|finish|status`, `cutover`, all of which take `--from <cluster>` to act on every bucket a cluster holds, plus `directory set-default`. Six migration metrics, republished on every directory reload. `test/e2e/demo.sh` and `test/e2e/evacuate.sh` drive both with aws-cli alone.
- **POC-3: one endpoint over mixed backends.** `internal/directory` (the second named interface seam) with the file backend of ADR-0005: an immutable snapshot behind an atomic pointer, a 1 s stat poll plus SIGHUP reload that refuses a file which does not parse, validate, or carry a higher version, and writes through an flock, a re-read, a version bump, and a temp-file rename, each appending to `<file>.changes.jsonl`. Tenants and placements moved out of the config into that file; `check-config` validates both. `internal/s3/xmlrw`, the streaming XML rewriter of ADR-0006, and `internal/migrate`, the uploadId codec. The proxy resolves `(tenant, bucket)` to a placement, renames the bucket in the path, signs with that cluster's credentials and region, and rewrites every response echo of a backend name, endpoint, or uploadId. ListBuckets is synthesized from the directory; CreateBucket claims its placement row and then creates the backend bucket under a generated name; DeleteBucket removes the row once the backend bucket is gone. `shunt directory get|set-state|validate` with the versioning refusal. `shunt_requests_total` gains `cluster` and `cluster_type`; the access log gains `tenant`, `cluster_type`, and `backend_bucket`. The rewriter can be switched off by `kill_switches.xml_rewrite_disable`, which defaults to false: it is a kill switch, not a feature flag, and CLAUDE.md now states the difference.
- **POC-2 steps 5–8:** `shunt probe` (sha256 enforcement, unsigned trailers, five checksum algorithms, conditional PUT, unsigned `GET /`), run against Garage, MinIO, and VAST (docs/reference/backend-compat.md); s3diff signing-mode dimension (15 payload modes × 2 sizes via a raw client on shunt's signer and encoders), resign mode, existing-bucket mode for VAST, order-insensitive ListBuckets; resign bench (docs/bench/poc2.md: ~150–220 µs CPU per request, per-GiB unchanged).
- **POC-2 steps 1–4:** `internal/sigv4` (canonicalization from the raw request target, header and presigned verification, upstream signing; SDK-oracle round trips over the tricky-key matrix; fuzz on canonicalization and Authorization parsing), `internal/sigv4/chunked` (versitygw readers lifted at 4dc0debf with two hardenings from fuzzing; shunt's unsigned-trailer encoder with precomputed Content-Length; a client-side signed-chunk encoder that, with the lifted decoder, reproduces AWS's published streaming vectors), `internal/auth` (static YAML store, 0600, inline secrets), the resign branch in `internal/proxy`, three catalog metrics, `serve` wiring, e2e resign configs generated by up.sh.
- `cmd/shunt serve`: TLS listener with SNI map, passthrough proxy to one static cluster, resign proxy over every configured cluster, admin listener, drain on SIGTERM, directory reload on SIGHUP.
- `internal/s3`: request model (path and virtual-host), ordered classifier table (75 ops + Unknown/Preflight), own error table, S3 bucket-name rules.
- `internal/proxy`: one handler; pooled 256 KiB `io.CopyBuffer`; hop-by-hop hygiene; chained `Expect: 100-continue`; idle-progress watchdog for data ops, total deadline for metadata ops; abort on short reads (ADR-0003); Content-Type sniffing suppressed; round-robin with skip-on-connect-error.
- `internal/telemetry`: nine catalog metrics on a private registry with a catalog-conformance test, JSON access log (docs/reference/access-log.md), 100-entry slow ring.
- `internal/upstream`: every cluster with its transport, credentials, capability profile, and opaque id. `internal/listener`, `internal/admin`.
- `test/s3diff`: 258-case direct-vs-via matrix per backend, plus mixed-backend mode (`-mixed`) with the four leak and isolation assertions. `test/bench/s3bench`: direct-vs-via bench.
- `internal/config`: schema, validator, samples, fuzz target, benchmarks. `internal/directory`: samples, transition matrix, concurrency and reload tests, fuzz target, benchmarks.

## POC-3 results (2026-09-15)

Mixed-backend run, one shunt over Garage and MinIO, two tenants, four placements (`make run-mixed` + `make s3diff-mixed`):

| Check | Result |
|---|---|
| Direct-vs-via matrix | 332 cases, 0 diffs |
| Backend names and cluster endpoints in via responses | 0 |
| `<Location>` on CompleteMultipartUpload | client-facing URL on every bucket, asserted explicitly |
| uploadIds | every via uploadId carries a 6-hex cluster prefix |
| ListBuckets | equals each tenant's directory set; both tenants span both clusters |
| Cross-tenant access | each tenant reaches only its own buckets |

The same client bucket name (`archive`, and the run's generated bucket) is owned by both tenants and served from different clusters in the same run.

Single-backend runs, the POC-1 and POC-2 gates re-run on the POC-3 code:

| Backend | Mode | Cases | Diffs | Backend gaps |
|---|---|---|---|---|
| Garage 2.3.0 | passthrough | 258 | 0 | 0 |
| MinIO RELEASE.2025-07-23 | passthrough | 258 | 0 | 0 |
| Garage 2.3.0 | resign | 258 | 0 | 10 |
| MinIO RELEASE.2025-07-23 | resign | 258 | 0 | 0 |

MinIO's 11 resign diffs from POC-2 are gone: they were the `<Location>` and path-style `<Resource>` echoes this phase rewrites. Garage's 10 backend gaps are unchanged, the signed-trailer rejection in docs/reference/backend-compat.md. Three cases now compare on status alone in resign mode, because shunt answers them itself instead of forwarding them: ListBuckets, synthesized from the directory; a HEAD of a bucket the directory does not name, answered with shunt's own error body and no backend headers; and `<Location>`, whose contents are asserted directly instead, since each backend builds its own differently.

## POC-4 results (2026-09-15)

A bucket moves between clusters while clients keep using it, with no client change: `RAMPING` splits writes by a stable key hash or prefix, `MIGRATING` sends every write to the new primary while reads fall back to the source, deletes go to both and listings merge, `CUTOVER` lets go of the source, and `ACTIVE` forgets it. `internal/migrate` holds the routing table, `internal/proxy/merge.go` the merged `ListObjectsV2`, `test/mover` the mover contract, and `shunt ramp | migrate start|finish|status | cutover | directory set-default` the operator's side. The races and their windows are ADR-0004; the how-to is docs/migrating.md.

**`test/e2e/demo.sh --from garage --to minio --objects 1000`** — the POC.md acceptance direction, aws-cli only, one unchanged endpoint:

| Check | Result |
|---|---|
| Objects | 1,000 single-part + 4 multipart, plus 108 written by a writer that ran throughout |
| Ramp | ratios 0.01 → 0.25 → 1.0; 152 writes to the new primary, 11 to the source |
| Mover | 1,016 copied / 40.0 MiB, then a second pass copying nothing |
| Fallback reads | rose to 12, then flat: the convergence signal |
| Listing diff across the migration | empty |
| Objects verified by GET and ETag after cutover | 1,004, multipart ETags included |
| Objects written during the migration and still present | 108 of 108 |

**`test/e2e/evacuate.sh --from minio --to garage`** — the same machinery aimed at a whole cluster, which is what retiring a vendor looks like:

| Check | Result |
|---|---|
| Buckets moved | 3, across two tenants, one command per step (`--from minio`) |
| Mover | 321 objects / 9.4 MiB with the HEAD-then-commit guard (Garage ignores `If-None-Match`), second pass copying nothing |
| Keys with spaces, `+` and `%` | moved and verified byte-for-byte |
| Tenant defaults | `migrate finish --from` reported the tenant still defaulting to MinIO; `directory set-default` repointed it |
| Directory afterwards | no placement and no tenant names MinIO |
| Objects verified by ETag through the same endpoint and credentials | 304 |
| **MinIO container stopped** | both tenants still list their buckets, read objects, and take new writes |

**Property test** (`make property`, `internal/proxy/property_test.go`): eight client goroutines write, delete and read 120 keys against an authoritative model while the operator ramps, migrates, runs two mover passes and cuts over. A successful write must be immediately readable with its own bytes; a delete must stay deleted; at rest the listing must name exactly what the client has. Result: clean at the CI duration (2 minutes, 437,677 operations) and at the local soak duration (10 minutes, 1,922,051 operations), `make property PROPERTY_TIME=10m`.

  **One unexplained failure, carried as open.** An earlier 10-minute soak reported 15 violations in 1,857,740 operations. The per-violation detail was lost: the run was piped through `tail`, so only the summary survived. It has not reproduced in the two runs since (one 2-minute, one 10-minute, both clean). The likeliest candidate is the delete/copy window of ADR-0004 race 1 — the test allows a resurrected object 5 seconds to disappear while a mover pass is in flight, and a slower pass would exceed it — but that is a hypothesis, not a finding. `make property` keeps the full output; the next failure will name each violation.

Bench: docs/bench/poc4.md. Migration overhead on GET and PUT is below this box's single-run noise; the one path that measurably costs more is the merged listing, at 86 ms per page over two clusters holding ~1,000 keys each, and it exists only while a bucket is `MIGRATING`.

## Found and fixed in POC-4

- **A merged listing returned every key twice, on real backends only.** Garage 2.3.0 percent-encodes `/` in a listing key when asked for `encoding-type=url`; MinIO returns the key verbatim. The merge compared the two spellings as strings, so an object held by both clusters merged as two objects. Unit tests missed it because the two fake backends agreed with each other, and the first live run that caught it happened to use keys without slashes. shunt now asks both sides for `encoding-type=url`, decodes the keys itself, merges the decoded names, and re-encodes for the client according to what the client asked for. Regression test: the fakes now disagree the way the backends do.
- **Multipart objects lost their ETag in the mover.** MinIO does not send `x-amz-mp-parts-count` on a plain `HEAD`, so the mover saw one part where there were two and re-uploaded the object whole: the bytes were right and the ETag was not. It now takes the `-N` ETag suffix as the trigger and a `HEAD` with `partNumber=1` as the authority, and the ledger flags any object whose ETag changed, with a non-zero exit.
- **The mover's cursor survived a completed pass**, so a second pass resumed after the last key of the first and silently skipped everything written behind it under a lower-sorting prefix. A finished pass now clears its cursor; a resume is only ever a resume.
- **A copy from a bucket that is mid-migration** was decided by a helper that still described POC-3 routing. Such a copy is now refused with a clear 501: the backend performing it can read only its own cluster, and half the objects may be on the other one. The helper is gone.
- **`make run-mixed` ran with a stale config.** It reset the directory but not the generated run config, so `capabilities.conditional_write`, added after `make e2e-up` last ran, was missing, and the mover chose the optimistic guard against a backend that ignores it. The target now regenerates the config from its template, and the demo refuses to run against a config without the key.

## Found and fixed in POC-3

- **Two data races in the proxy, found by `-race` during POC-3, both older than POC-3.** `progressReader.n` counts the client request body; net/http's transport write loop updates it on its own goroutine while the handler read it after `RoundTrip` returned. The `httptrace` timings are written by the connection's read loop when the response headers arrive, and `finish()` read them from the handler goroutine. The counter dates from POC-2, the timings from POC-1. Neither could corrupt a response: both are bookkeeping, and the worst case is a wrong byte count or latency in one log line. Fixed: the counter is atomic, the timings sit behind a small mutex.

  **Why POC-1 and POC-2 passed `-race` anyway.** On a request that runs to completion there is a happens-before edge between the transport and the handler: `RoundTrip` returns only after the body has been written and the response read, so reading that state afterwards is ordered and the detector has nothing to report. The window opens only when the transport abandons a request early: the backend answers before reading the body, the client goes away mid-body, or the response headers arrive after the handler has already given up. POC-1 and POC-2 did open it, in the mid-stream rejection tests, but a handful of times per run, and `-race` reports a race only when it actually observes both accesses interleave. POC-3 added about twenty tests and two fake clusters to the same package, and the failures appeared under `-count=2` and higher, never under `-count=1`. So the answer to "does `go test -race ./...` only catch these under concurrent load" is yes for this class, and the gate needed something that guarantees the load.

  **The gate now guarantees it.** `internal/proxy/stress_test.go` holds each window open about a hundred times per run, with no e2e stack and under a second each. `TestConcurrentAbortedRequestsAreRaceFree` drives bodies that the backend never finishes reading and clients that vanish mid-body; `TestLateResponseHeadersAreRaceFree` makes every response arrive after its handler has given up. Verified by reverting each fix on its own: with the counter reverted the first test reports 10 to 14 races on every run, with the timings mutex reverted the second reports 1 to 2 on every run, and with both fixes in place five runs of both are clean.
- **The XML rewriter's identity property failed at a chunk boundary** before release: the text cap counted the bytes of a partly received end tag, so the same document could be rewritten when written whole and forwarded verbatim when split. The cap now counts element text only, with a separate bound on tag bytes. Found by the fuzz target, which asserts that output does not depend on where the input is split.

## Known gaps carried forward

- **CARRIED GAP from POC-2: TLS verification is off for VAST**, and POC-3 is built on Garage and MinIO only because of it. Three switches disable it:
  - `test/e2e/shunt-vast-resign.yaml`, `tls: { insecure_skip_verify: true }` on the cluster record (shunt logs a warning at startup)
  - `Makefile`, `-direct-insecure` in the `BACKEND=vast` s3diff and bench arguments
  - `Makefile`, `--insecure` in the `BACKEND=vast` probe arguments

  **Blocker: the lab certificate.** VAST serves its factory certificate, CN `vms.example.com`, whose SAN is `*.example.com`, `vms.example.com`, and 33 IP addresses. A wildcard matches exactly one label, so `*.example.com` cannot match the four-label host `vast02.example.com`, and the host's address (10.0.0.2 on 2026-09-15) is not in the IP list. The lab wildcard on the dev box (`/path/to/lab-wildcard.crt`, `*.lab.example.com`) covers only one label below `lab.example.com` and expired 2026-09-02. The fix is a certificate whose SAN includes `vast02.example.com` or `*.vast02.example.com`, then `tls.ca` if it is not publicly issued.

  **This is the POC-3 exit condition.** `make check-tls-verify` fails while any committed config or make target disables verification and prints each location; `make all` warns on every run until it passes. When it passes: add the VAST cluster to `test/e2e/shunt-mixed.yaml` and a placement to `test/e2e/directory-mixed.yaml`, re-run `make probe BACKEND=vast` and `make s3diff-mixed` with verification on, then tag `poc-3`.
- **Copying from a bucket that is mid-migration is refused with 501.** Its objects are split across two clusters and the backend doing the copy can read only one of them. It clears as soon as the bucket is `CUTOVER`.
- **The mover is a test fixture** (`test/mover`), not a subcommand: `shunt` has the verbs that move a placement, and the fixture moves the bytes. A production mover has the same obligations (ADR-0004) and a home in P5.
- **Versioned buckets cannot be migrated at all** (DESIGN §9 item 4): entering `RAMPING` or `MIGRATING` is refused if either side has ever had versioning enabled, and the check fails closed.
- **Copying between clusters is refused with 501** (ADR-0006, DESIGN §9 item 14). A client copying between two of its own buckets whose placements name different clusters gets a hard failure, because the backend cannot read the other cluster. A later phase decides whether shunt streams the copy itself.
- **Bucket logging, replication, inventory, analytics, and notification answer 501** in resign mode: their configurations name buckets as ARNs and carry per-backend account ids.
- **Bucket policy passes through unrewritten**, so an ARN in a policy body can name the backend bucket (docs/reference/backend-compat.md).
- **Vendor identity is deliberately not hidden** (decision of 2026-09-15): `Server`, `X-Minio-*`, `X-Vast-*`, `<HostId>`, Garage's `<Region>`, GetBucketLocation, and backend owner ids reach clients. shunt hides which cluster and which bucket name, not which kind of backend.
- **The directory file must be writable for CreateBucket and DeleteBucket.** On Kubernetes a ConfigMap is mounted read-only, so those answer 503 until P3c. Several proxies converge only within `directory.poll_interval` (default 1 s), so a bucket created on one is `NoSuchBucket` on another until then; ADR-0004 in POC-4 accounts for it.
- Credentials file inline secrets; encrypted at rest is P3c.
- Garage 2.3.0 rejects signed trailers directly; through shunt they work because shunt verifies them and forwards an unsigned trailer.
- Garage ignores `If-None-Match: *` on PUT (docs/reference/backend-compat.md). POC-4 closes this with the HEAD-then-commit guard of ADR-0004, chosen per cluster from `capabilities.conditional_write`; migrations into Garage are exercised by `test/e2e/evacuate.sh`.
- ADR-0002 compensation is log-and-alert only (`sha256`, `trailer` reasons); the compensating delete is P2.
- Passthrough preserves `Host` including shunt's port, and backends echo it into `<Location>` of CompleteMultipartUpload (MinIO) with the backend's own scheme. Inherent to passthrough; resign mode rewrites it. s3diff normalises the port.
- Backends that send no `Content-Type` (Garage) are relayed without one; net/http's sniffing is disabled per response. Found by s3diff, fixed in POC-1.
- The bench's 1 GiB tier runs at 1 connection only (disk on the dev box).
- go.yaml.in/yaml/v4 is at a release candidate (v4.0.0-rc.6); bump when v4.0.0 ships.

## Closed in POC-3

- **The backend address leak in `<Location>`** (POC-2 carried gap): the rewriter replaces it with the client-facing URL. A unit test asserts the exact value for path-style and virtual-host clients, and the mixed s3diff run fails if any endpoint appears in a response.
- **Path-style `<Resource>` echoes on virtual-host requests** (ADR-0001 amendment): rewritten to the client's own path.
- **The s3diff blind spot on VAST**: the mixed run no longer relies on a direct-vs-via difference to see a leak. It scans every via response for backend names and endpoints and asserts `<Location>` contents directly, which works even when both sides address the same host.
