# STATUS

Current phase: **POC-5 (operator walkthrough) done, and tested on VAST.** A bucket has moved VAST → VAST over http by hand, and inside a 15-minute warp load test, with 0 client errors (below, and docs/bench/poc5-load.md); `make walkthrough` is green on Garage → MinIO. **POC-3, POC-4 and POC-5 are untagged.** The tags wait on one thing: `make check-tls-verify` must pass, which needs a VAST cluster serving a certificate for its hostname (carried gap below). VAST then joins the https mixed s3diff run and the VAST → MinIO demo, and `poc-3` and `poc-4` are tagged.

## POC track

| Session | Gate | State |
|---|---|---|
| POC-0 | `make all` green on clean checkout; `check-config` rejects every invalid sample naming the key; `make e2e-up` leaves Garage and MinIO healthy | done 2026-09-14 |
| POC-1 | s3diff clean on Garage and MinIO (passthrough); unauthorized 5 GiB PUT fails before body bytes; mid-GET backend kill never a silent short read; bench in docs/bench/poc1.md | done 2026-09-15 |
| POC-2 | s3diff clean across signing modes on every backend or a documented gap each; secret-leak test green; fuzz 30s clean; resign overhead in docs/bench/poc2.md | done 2026-09-15, tagged `poc-2` with carried gaps |
| POC-3 | mixed-backend s3diff clean; same bucket name under two tenants isolated; ListBuckets spans clusters; no backend name leaks; uploadId round-trip | gate met on Garage + MinIO 2026-09-15; VAST probed and s3diff'd on its own (POC-2); **tag held until the TLS gate passes and VAST joins the mixed https run** |
| POC-4 | demo.sh green Garage → MinIO and VAST → MinIO/AWS; property test green; docs/bench/poc4.md | gate met on Garage ↔ MinIO 2026-09-15; the migration mechanics have since run VAST → VAST over http (POC-5); **VAST → MinIO demo.sh held behind the same TLS gate** |
| POC-5 | the ten-step walkthrough runs from copy-paste commands with `shunt verify` at 0 errors throughout; `make walkthrough` green on Garage → MinIO and in CI; docs/walkthrough.md for VAST → VAST | `make walkthrough` green locally 2026-09-16; **VAST → VAST run by hand (2026-09-16) and under warp load (2026-09-17), 0 client errors**; CI job added, not yet run |

After POC-4: G1 (simplicity) and G4 (licenses) once, then resume the full order at P0's skipped items (docs/POC.md "After the POC").

## The `distributed` branch

This branch carries everything past POC-5; `master` keeps what is done and tagged. Two bodies of
work were merged into one path on 2026-09-17, and P3c was pulled ahead of POC-6 items 4–5 on
2026-09-21 because multi-host is what is required and nothing in shunt may rely on a shared
filesystem:

- `docs/POC-6.md` — migration correctness hardening. Items 1, 2 and 3 done in code (ADR-0013
  conditional writes, ADR-0014 cross-cluster copy, ADR-0016 the version fence); items 4, 5 and the
  trailing items specified.
- **P3c-1 done (ADR-0015 accepted, 2026-09-22):** `shunt-control`, the second binary, embeds etcd;
  three nodes form a cluster (`init`, `join`, `member`, `snapshot`, `defrag`, `status`); the
  directory, client keys and cluster secrets (sealed at rest) live in it, every change one
  compare-and-swap transaction; proxies are members (`control.endpoints`) that take everything
  from it, cache it locally, forward bucket creation, heartbeat, and share nothing with each other.
  The fence of ADR-0016 runs on leased etcd keys. TLS for the control channel is deferred and the
  channel says `plaintext` for it. `bin/shunt` links no etcd (`make build` proves it). Docs:
  docs/fleet.md, docs/how-to/run-shunt-control.md, docs/runbooks/, docs/explanation/why-no-objects-table.md.
- **UI-0 done (2026-09-22, ADR-0017 proposed):** the control API is browser-ready. Every
  long-running action (ramp, migrate start, cutover, purge-source, finish, cluster remove) runs
  under an operation record (`POST /v1/operations`, `GET /v1/operations/{id}`; the routes answer
  as before plus `operation`, and the CLI's `--wait` polls the record); `GET /v1/events` streams
  directory, fence and fleet changes with replay by `Last-Event-ID`; read models with no secret
  in them (`/v1/clusters/{name}/view`, `/v1/placements/{t}/{b}/view`, `/v1/audit` over the change
  records both backends already wrote); `purge-source` and `cluster remove` have a dry run and a
  confirmation token; `GET /v1/control` carries `last_compaction` and a join line; the route table
  is `docs/reference/control-routes.json`, generated from code. Operations run on the server's
  context, so a client that disconnects mid-step does not stop it.
- **UI-1 done (2026-09-22):** proxies emit lazy HdrHistogram sketches and exact counters in
  clock-aligned 10-second windows, carry the last completed window in their heartbeat, and control
  nodes merge fleet, cluster and proxy p50/p90/p99/p99.9/max summaries into a 60-minute ring.
  `GET /v1/telemetry/series` and `/v1/telemetry/latest` expose it and `GET /v1/events` announces
  merged windows. Dense payload testing sets the documented ceiling at twelve clusters per proxy
  (1,025,921 bytes below the 1 MiB heartbeat cap); the fleet-scale read cost is documented. `make
  fleet` saw both proxies and both clusters and matched the verifier's client p99 within 6.51%
  (26,559 us versus 24,831 us, under the 10% gate); build, lint, test and race are green.
- **UI-2 done (2026-09-22):** `web/` is the pinned Vite, React, TypeScript, Tailwind and Recharts
  application in ADR-0017's dark ember/ink theme. `shunt-control` embeds it at `/` with SPA
  fallback; a committed stub keeps a Node-free Go build working, while CI runs `make ui` first.
  The shell keeps its bearer token in `sessionStorage`, follows authenticated SSE with replay,
  shows the control node, quorum and live/stale proxies, and includes the shared cards, stats,
  state/fence, confirmation, copy and sparkline components. The Go parity test checks the UI's
  reviewed API inventory against the generated route table, with every read-only no-CLI route
  named and justified rather than silently exempted. `make ui && make build`, UI lint, 10 browser
  tests (including theme contrast and a two-live-proxy shell), Go test and Go lint are green.
  `make fleet` is green on the embedded build, and a live same-origin smoke served the real bundle
  beside a healthy control API; production npm notices cover 44 packages and the generator rejects
  copyleft licenses.
- **UI-3 done (2026-09-22):** the operational UI now has control-plane membership, leader/quorum,
  database quota and fleet views; cluster preflight/add/detail/remove/read-only flows; and bucket
  adopt/create/detail/expand/read-only flows, with exact join guidance and Migrations handoff.
  Successful short mutations now create operation records too, bearer-authenticated audit rows use
  a stable non-secret `token:<fingerprint>` actor, and read-only is a fleet-fenced directory flag
  enforced by every proxy (retryable 503 by default, optional 403). Thirteen browser tests drive
  the first four demo actions and outcome/refusal toasts. Build, UI, lint, Go test and race gates
  are green; a fresh `make fleet` proved preflight, operation and audit records, read-only across
  two proxies, telemetry within 5.70%, quorum loss, cache restart, migration, cutover and purge.
- **UI-4 done (2026-09-22; browser acceptance is manual by operator choice):** the migration
  screen now carries the complete ramp → mover → cutover → purge/forget flow with an always-visible
  fence, presets and prefix rules, completed-window write/read outcome bars, stale-member warning,
  mover range/count/convergence/ledger views, exact API refusals, the ADR-0004 lost-write
  acceptance, multipart count, purge dry run and unused-source removal. The browser mover invokes
  the same guarded engine as the CLI as a control-node operation; no object state enters etcd.
  `make ui`, UI lint and all 16 UI unit/accessibility tests are green; build, Go lint/test/race,
  walkthrough (66,403 verifier operations, 0 errors), README demo and fleet are green. Fleet ran
  the browser mover operation against Garage → MinIO, saw it converge, read its ledger, and then
  cut over and purged. At the operator's request no Playwright dependency or automated browser run
  is committed; the full visual workflow is the next manual acceptance pass.
- **UI-5 done (2026-09-22; visual/screenshot acceptance is manual by operator choice):** the
  telemetry screen selects fleet, cluster or proxy scope, 5/15/60-minute windows and an operation
  class; it shows current p99/rate/throughput tiles, request and byte rates, four emitted-percentile
  small multiples, status-class errors, and a two-cluster p99 comparison. The control node now
  exposes retained exact counters as per-second series; percentile and scalar chart-data tests prove
  the browser plots API values unchanged, and refresh is driven only by telemetry SSE events. UI
  build/lint and all 18 unit/accessibility tests, Go build/lint/test/race and fleet are green. Fleet
  exposed non-empty request-rate histories for Garage and MinIO and matched the verifier's p99 by
  6.43% (28,367 us versus 26,543 us). The dashboard screenshot is intentionally left for the
  operator's manual end-to-end pass rather than adding a browser runner on this host.
- **UI-6 done (2026-09-22; browser acceptance is manual by operator choice; ADR-0017 accepted):**
  `make demo-ui` builds the real embedded application and leaves a seeded three-control/two-proxy
  Garage-to-MinIO fixture running, prints every URL and credential needed by the walkthrough, and
  starts consecutive checked workloads after the source is adopted; `make demo-ui-down` stops only
  its recorded process groups, including the verifier. The Audit screen is live, global control
  errors and quorum loss are explicit, and the migration finish supplies the tenant-default change
  needed before removing the old cluster. `docs/demo-ui.md` is the complete eight-step handoff.
  The fixture produced 68,945 verifier operations with 0 errors and teardown left no process or
  demo port; UI build/lint and all 20 unit/accessibility tests, Go build/lint/test/race, shellcheck,
  and the production-license gate are green (44 npm packages, no copyleft; both Go binaries fully
  noticed). The final fleet run was green, including 13,118 operations with 0 errors through quorum
  loss, cache restart, mover convergence, cutover and purge; telemetry p99 differed by 3.24%. At the
  operator's request Playwright is not added: the visual flow and UI-5 screenshot are the remaining
  manual acceptance pass, not an automated browser gate.
- **UI-7 (2026-09-23):** fixes from the first hands-on browser demo (commit 74bddd4); `distributed`
  was then fast-forwarded into `master`.

## The `1-to-n-bucket-support` branch (ADR-0018, proposed)

One client bucket over 1 to N backend buckets ("legs", capped at 32), phased N1–N4.

- **N1 done (2026-09-23): placement schema v2 is read everywhere, written nowhere.** A placement may
  be written as `legs`, `owners` (hash ranges that partition the key space, 16-hex-digit bounds)
  and one `move`; every reader converts it into the v1 fields on decode (`internal/directory/legs.go`):
  the directory file, the control plane's etcd store, `GET /v1/directory`, the member cache and the
  change records. A v2 placement this build cannot route (several owners, two legs on one cluster,
  a partial move, more than 32 legs) is refused with the reason, never guessed at. The writer is
  unchanged, so no byte on disk, in etcd or on the wire moved. Gate: `mixed-v2.yaml` loads to
  exactly `mixed.yaml`'s placements; every v1 shape round-trips v1 → v2 → v1 through YAML and JSON;
  17 invalid v2 samples name their key; the etcd store and a member (long poll and cache) read v2,
  and both tests fail with the conversion removed; `FuzzParse` and `FuzzParseV2` clean for 30 s
  each (about 2 M inputs); race suite and lint green; `make walkthrough` (62,441 operations, 0
  errors) and `make readme-demo` green. `make fleet` was not run: its ports are held by the running
  UI demo.
- **N2 done (2026-09-23): buckets spread over legs, at rest.** `create-backend` with `legs` (and
  Create → Spread across clusters in the UI) makes a bucket whose keys are split by hash over one new
  bucket per cluster. The proxy narrows every object request to the leg that owns its key and merges
  listings across legs (v2 and v1) with a fixed-size token (ADR-0019); other bucket-level requests
  answer `NotImplemented`. Nothing moves a spread bucket yet (N3). Gate: proxy tests route 60 keys
  and multipart uploads to their owners and nowhere else, page merged listings with and without a
  delimiter and hide a stray on the wrong leg (fails with the filter removed), fail a listing with a
  leg missing, and copy both ways; control tests cover the create's refusals and the guards;
  `FuzzSpreadToken` clean; `BenchmarkSpreadListingPage` about 2× a plain bucket, flat from 2 to 32
  legs; race suite, lint and 26 UI tests green. **Live on two MinIO clusters (2026-09-23):** a bucket
  spread over them took two concurrent `shunt verify` runs, one through each proxy, at 0 errors in
  119,177 operations, and a 12 MiB three-part upload read back identical through the other proxy.
  Listed directly, the legs held 204 and 209 keys with none on both; through either proxy, paged by
  1000 or 37, v2 or v1, the listing was exactly their 413; the delimited listing gave each of 4
  prefixes once; GetBucketVersioning answered `NotImplemented`.
- **N3a done (2026-09-23): moving part of a bucket between clusters.** A first ramp or migrate step
  with a `range` moves only those keys to another leg (a plain bucket becomes spread this way); the
  move runs through ramp, migrate, the mover, cutover and purge as a migration of its range, and
  ends with the destination owning it. Purge deletes only the range; finish is refused while the
  source leg keeps other keys. Gate: fleet property test on a half-bucket move, 6 runs, 0 violations
  in 54,855 operations, negative control failing every run; proxy and control tests with negative
  controls; race suite, lint, 29 UI tests. **Live (2026-09-23):** a plain bucket on minio01 moved the
  lower half of its key space to a new leg on minio02 (ramp 25/60/100% held, migrate, mover converged
  on pass 2, 5 s cutover, purge of the range's 83 objects) under two concurrent `shunt verify` runs,
  one per proxy: 231,172 operations, and the only errors were the 311 hold 503s of ADR-0016, which
  the proxies counted exactly (163 and 148; verify does not retry them). Afterwards minio01 held
  only upper-half keys (266) and minio02 only lower-half keys (271), none on both, the listing was
  their union, and every seeded object read back. Next: N3b (two legs on one cluster), N3c
  (consolidation).
- `docs/design/distributed.md` (§12 of the design) + `docs/prompts/P3d.md`, `P3e.md` — what is
  left of the fleet-scale form: movers as workers, fleet decisions, the web UI (its seven build
  prompts: `docs/prompts/webui.md`), and the P3c-2 deferrals (deltas, object-storage bootstrap
  and audit export, TLS, the browser-client API).

**Guards on this branch.** `make readme-demo` runs the README's hand-run demo, steps 1 to 14,
unattended; `make walkthrough` the operator walkthrough; `make fleet` three control nodes and two
proxies with quorum loss and a cache restart; `internal/proxy/fleet_property_test.go` the client's
view across a lagging, then partitioned, member over a real control plane, with a negative control
that must fail. All run in CI. The README demo's output is unchanged: a single proxy has no
control plane, no members, and nothing held or waited on.

## What exists

- **POC-5: the operator walkthrough.** Clusters are directory state (ADR-0008): `clusters:` moved from the resign config into the directory file, and `upstream.Registry` swaps the live cluster set before any snapshot that names a new cluster is installed, so a cluster is added or removed without a restart and one shunt cannot sign for is refused. `internal/control` serves the control API under `/v1/` on the admin listener (bearer token from `admin.control_token_ref`, or loopback only), one handler per operation, every mutation through the directory write path (docs/reference/control-api.md). The operator verbs are its clients: `shunt cluster add|remove`, `tenant set-default`, `adopt`, `expand` (target bucket, versioning check, canary), `ramp`, `migrate start|run|finish`, `cutover` (converged mover and a flat fallback-read window, recorded on the placement), `purge-source` (CUTOVER, evidence, empty source − primary diff; then deletes the source bucket), and `status` (state, ratio, writes per side, fallback reads, mover progress), each with `--json`. `shunt migrate run` is the mover, promoted from `test/mover` with the same contract and refusals (ADR-0009: the AWS SDK now ships in `bin/shunt`). `CUTOVER` deletes go to both clusters so the source only loses keys (ADR-0004 amendment). `shunt verify` (`internal/verify`, shared with the property test) drives a checked read/write/delete workload and, with `features.debug_route_header`, tallies the side and cluster that served each request (ADR-0006 amendment). `listener.plaintext` serves plain http for labs and marks every log line. `test/e2e/walkthrough.sh` runs docs/walkthrough.md unattended; `make walkthrough` and a CI job run it on Garage → MinIO.
- **POC-4: migration without per-object state.** `internal/migrate` (routing table + uploadId codec): every S3 op is classified, and a class plus a placement decides the cluster, whether reads fall back, whether deletes are doubled, and whether listings merge. A completeness test fails the build when an op has no class. `internal/proxy/merge.go` merges `ListObjectsV2` from both clusters, one page per side at a time, with a composite continuation token, a missing side treated as empty, and key encoding normalised across backends. The delete body is buffered within a 1 MiB cap so it can be replayed to the second cluster, and a merged listing holds one page per side (ADR-0007, written at the POC close-out). `test/mover` implements the mover contract: conditional write where the backend has it and a HEAD-then-commit guard where it does not, source re-`HEAD` after every copy, part-layout preservation, a cursor, and a JSONL ledger. Operator verbs: `shunt ramp`, `migrate start|finish|status`, `cutover`, all of which take `--from <cluster>` to act on every bucket a cluster holds, plus `directory set-default`. Six migration metrics, republished on every directory reload. `test/e2e/demo.sh` and `test/e2e/evacuate.sh` drive both with aws-cli alone.
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

**Property test** (`make property`, `internal/proxy/property_test.go`): eight client goroutines write, delete and read against an authoritative model while the operator ramps, migrates, runs two mover passes and cuts over. A successful write must be immediately readable with its own bytes; a delete must stay deleted; at rest the listing must name exactly what the client has. Every run records its seed, topology and mover counters in `test/property/runs/<timestamp>/run.json`, and a failing run also records every event, every violation and a report (ADR-0004, "The POC-4 property failure").

**Correction (2026-09-16).** The results first recorded here were a clean 2-minute run (437,677 operations) and a clean 10-minute run (1,922,051 operations). **Neither exercised the mover's copy path.** By the time the bucket reached `MIGRATING`, the clients had deleted every key from the source, so both mover passes found nothing to copy. Those runs showed routing, fallback reads and dual deletes holding under load. They said nothing about the delete/copy race or the overwrite race. The unexplained 15-violation failure from the same period was a **harness stall**, not a migration race: the test rig's in-memory access log and the fake backends' request history grew until the whole process paused past the proxy's 2-second idle watchdog, and GETs in flight ended in EOF. A 60-minute rerun reproduced that class 10 times, reached 48 GB RSS, and was stopped. The original run's own detail was lost, so its cause is inferred, not proven.

The harness now keeps no per-request state. 120 of the 240 keys are kept on the source until the mover has had them, and while the mover copies, the clients overwrite and delete the kept key it is on. 60-minute runs on that harness, with the fixes below:

| Run | Target behaves like | Operations | Mover, pass 1 over kept keys | Violations | Harness stalls |
|---|---|---|---|---|---|
| `20260916T143028Z`, seed 1789569030034891932 | MinIO: honors `If-None-Match: *`, ignores `If-Match` on DELETE | 29,330,793 | 106 read; **29 PUTs refused with 412** (contended); **11 re-HEADs found the source gone**, 7 own copies withdrawn, 4 already gone | **0** | 0 |
| `20260916T153050Z`, seed 1789572651921399394 | Garage: ignores both | 17,326,931 | 94 read; 32 already on the target at HEAD (contended); 62 PUTs; **9 re-HEADs found the source gone**, 7 own copies withdrawn, 2 already gone | **2 distinct lost writes** in the known window (`obj/0157`, `obj/0211`), 15,483 stale reads of the first, 0 violations outside it | 0 |

The guarded run's losses are the documented HEAD-then-commit window (ADR-0004 race 2), measured. On `obj/0157` the mover's HEAD found the key absent, a client's PUT committed 0.25 ms later, and the mover's PUT of the older source bytes committed 0.53 ms after that. The client had already been told 200. Every later read of the key until the second pass ended counted as a violation. `obj/0211` lost a write the same way (client PUT 0.98 ms after the mover's HEAD, mover PUT 0.60 ms after it), but a later client write replaced the stale bytes before anyone read them. Only the commit history shows that loss. That is **2 lost writes in 62 guarded copies aimed at contended keys**.

The guarded variant is now judged on that history (`internal/proxy/property_loss_test.go`). A client PUT that commits between the mover's HEAD and the mover's PUT on the same key is a known-window loss. The headline is distinct lost writes, not reads. The run passes if every violation is a stale read of such a loss and fails on anything else. Replaying the guarded run's event log through it: 2 losses, 15,483 of 15,483 violations inside the window, 0 outside, so under the new rule that run passes. The conditional variant still tolerates no violation. `shunt migrate start` now refuses a target whose profile says `conditional_write: false` unless it is given `--accept-lost-write-window`.

Bench: docs/bench/poc4.md. Migration overhead on GET and PUT is below this box's single-run noise; the one path that measurably costs more is the merged listing, at 86 ms per page over two clusters holding ~1,000 keys each, and it exists only while a bucket is `MIGRATING`.

## POC-5 results (2026-09-16)

`make walkthrough` (Garage playing vast01, MinIO playing vast02, shunt on 127.0.0.1:8008 plaintext) on the final binary, twice in a row; full logs kept outside the repo:

| Run | verify, steps 4–10 | Split at ratio 0.5 (separate 20 s verify, 1,000 keys) | Mover | Purge |
|---|---|---|---|---|
| 1 | 43,729 ops (87 s), **0 errors**; 112 surviving keys read back after it stopped | writes 51 / 49 %, reads 53 / 47 % across vast02 / vast01 | 100 copied, second pass copied 0: converged | diff empty; 100 objects and the bucket deleted |
| 2 | 42,132 ops (87 s), **0 errors**; 116 surviving keys read back | writes 51 / 49 %, reads 52 / 48 % | 100 copied, second pass copied 0: converged | diff empty; 100 objects and the bucket deleted |

Refusals observed live as designed: `purge-source` in `MIGRATING`, `cluster remove vast01` while the placement and then the tenant default named it. `cutover --window 15s` passed with verify still reading. All 100 objects written to vast01 before shunt existed read back byte for byte from vast02 through shunt after the purge.

## Under load (2026-09-17, docs/bench/poc5-load.md)

The whole demo was run inside a 15-minute `warp mixed` benchmark, 16 clients, through shunt.

**VAST → VAST** (VAST 5.5 → VAST 5.4 over http, rate-capped at 160 requests/s, 96 MiB/s):
- **Errors:** 0 in 144,016 requests.
- **Demo steps:** every one succeeded on its first attempt.
- **Latency:** shunt adds about **1.5–2.5 ms at p50 and 3 ms at p99** on GET, STAT and DELETE, and nothing measurable on PUT.
- **During the move:** dual deletes raise DELETE p50 by about 4 ms (p99 to about 40 ms); fallback reads add about 1 ms p50 to reads.
- **CPU:** about half a core at that rate, mostly body relay and syscalls. The client write path (17%), a synchronous access log (4%) and per-request signing-key derivation (1.5%) are the first latency candidates.

**MinIO → MinIO:**
- **1 MiB objects:** 0 errors in 453,336 requests.
- **40 MiB multipart objects:** 0 errors in 24,282 requests.

**Garage → MinIO** errors came from Garage-issued version ids and a full local disk (backend-compat.md).

**Two findings:**
1. **Spurious purge refusal under deletes (fixed 2026-09-17).** `purge-source` refused once on MinIO while warp deleted concurrently. The diff now confirms each key the listings disagree on with a HEAD of the primary, then of the source, before counting it (see the entry below).
2. **Version ids invented by Garage (open).** Version ids invented by a backend (Garage) break version-echoing clients after a move.

## Operator ergonomics (ADR-0010, 2026-09-16)

After the two manual VAST runs, the lab path became all commands:
- **`shunt serve --plaintext` needs no config:** it creates a `shunt-data/` state directory with the directory file, a `secrets/` folder, and a generated client key that `shunt client show` prints.
- **`shunt cluster add <name> http://host --access-key AK`** prompts for the secret. shunt stores it in `secrets/` (0600, referenced as `file:`), checks the pair against the cluster, and reads the type from the cluster's `Server` header.
- **`shunt expand` measures conditional PUT and DELETE** on the target when the cluster doesn't state them.
- **A client key without a tenant belongs to the default tenant,** whose buckets every verb takes by bare name. The CLI never shows the default tenant.

README.md is that demo, rehearsed as written on Garage → MinIO: the clients hold cluster A's own key (`adopt --keys`, ADR-0012), 52 objects read back with 0 mismatches throughout, then cutover, purge, cluster removal, and a step-out that hands the clients to cluster B and stops shunt — the last 52 reads go straight to the cluster, same bucket name. `test/e2e/walkthrough.sh` and docs/walkthrough.md use the default tenant as well (re-run green 2026-09-17).

## POC-5 by hand on VAST (2026-09-16)

An operator ran the migration by hand between two VAST clusters over http, using only `shunt` and aws-cli commands. The source was vast01 (VAST 5.5), the target vast02 (VAST 5.4), and shunt ran on the operator's host on `:8008`. Neither key could create buckets, so both buckets already existed and were emptied first. A later run repeated it with the ADR-0010 commands (no config file, no tenant), with the same result.

| Plan step | Result |
|---|---|
| 2 files straight to vast01; shunt started empty; `cluster add` + `adopt` | both read back byte for byte through shunt |
| 10 more through shunt | all 12 on vast01 |
| `cluster add vast02` live + `expand` onto the existing target bucket | canary write/read/delete ok, no restart |
| `ramp --ratio 0.5`; 20 more | 13 on vast02 and 7 on vast01, exactly the hash's prediction; all 32 read back |
| `ramp --ratio 1.0`; 20 more | all 20 on vast02; all 52 read back from both clusters |
| reader loop (a full `aws s3 cp --recursive` and byte compare every ~4 s) through `migrate start`, `migrate run --until-converged`, `cutover --window 60s`, `purge-source`, `tenant set-default`, `cluster remove` | 98+ passes, **0 mismatches, 0 failed requests**; the mover copied 19, then 0; the cutover window held with the reader running; purge found an empty diff and deleted vast01's bucket |

The one failure was operator-side, and the product made it hard to see. The mover's first run failed with `SignatureDoesNotMatch`: its terminal still held an old vast01 secret, while `shunt serve` and aws-cli had the new one. Earlier in the run the same mix-up had produced a 403 on `adopt`. It is fixed below.



- **The ramp split was not 50/50.** At ratio 0.5 the first live run measured 80/20. `InRange` used FNV-1a-64 directly, whose high bits barely move for keys differing only in their last bytes, so sequential keys (`a/0000…a/0999`) all fell on one side. The hash now goes through murmur3's `fmix64`; `TestInRangeSplitsSequentialKeys` holds five key shapes within ±4 % at 0.1, 0.5 and 0.9. Because changing the function would re-split a running ramp, a ramp now names its hash (`ramp.hash: fnv1a-fmix64-v1`, written at RAMPING start, pinned by `TestRampHashIsPinned`). A proxy that does not implement the named hash refuses the requests that hash would route (503) instead of re-splitting them (ADR-0004 amendment).
- **shunt's signer signed a header it did not send.** A bodyless DELETE with `http.NoBody` listed `content-length` in `SignedHeaders`, but net/http sends no `Content-Length` on DELETE, and Garage refused it (`signed header content-length is not present`); MinIO and gofakes3 accepted it, so no earlier test saw it. It hit `purge-source` on Garage. `sigv4.Sign` now lists `content-length` only when the client sends it; `TestSignListsOnlyHeadersTheClientSends` round-trips every method and body shape through a real server.
- **A credential mix-up surfaced late and unexplained (found on VAST by hand).** `cluster add` saved an access key from one credential set that shunt then paired with a secret from another, and nothing checked the pair until `adopt` got a 403. The mover's secret is read from its own terminal, so a stale variable there failed with a raw SDK error. Now `cluster add` signs one `ListBuckets` with the pair and refuses a wrong secret or an unknown key, naming the `secret_ref` and where it is read from. `migrate run` checks each side the same way before copying anything. Garage reports both faults as `AccessDenied` with the reason in the message, so the check reads code and message (`s3.ClassifyCredentialError`, docs/reference/backend-compat.md).
- **The serve log was hard to read at a terminal.** `telemetry.log_format` (`auto`, the default, picks `console` on a terminal and JSON otherwise) writes `19:25:55 INFO  cluster added  cluster=g type=s3 …`, with a `[PLAINTEXT]` tag in place of the long attribute. Cluster lines now name the event: `cluster ready`, `cluster added`, `cluster updated`.
- **Operator actions were invisible in serve's log (found on the second VAST run).** Only cluster changes were logged; adopt, expand, ramps, migrate, mover passes, cutover and purge showed up only in the terminal that ran them. Every control API mutation now logs one INFO line on success and one WARN line (`<operation> refused|failed`, with the reason) otherwise. A cluster or bucket name that doesn't exist is reported as such, not as `no such placement`.
- **`shunt status` columns misaligned** when an endpoint name was long. The table now sizes columns to their contents, and the fallback column is labelled `FALLBACK READS`: it counts GET and HEAD requests, not objects.
- **A refusal was indistinguishable from a failure in the CLI.** The API's `refused` code did not reach the terminal; the CLI now prints `refused: <reason>`, and an in-use cluster and an illegal transition answer `refused` rather than `conflict`.
- **A copy between clusters was refused (2026-09-17, POC-6 item 2, ADR-0014).** `CopyObject` is server-side, so it needs one backend that can see both buckets. Across clusters, or out of a bucket mid-migration, there is none, and shunt answered 501 — which broke S3A's commit, a rename (copy, then delete), for the whole length of a ramp. shunt now makes the copy itself: it reads the object from the cluster that holds it (primary first, falling back like a GET) and streams it to the destination, keeps a multipart source's part layout so the ETag's shape survives, evaluates the copy-source conditions where the source is, applies ADR-0013 to conditions on the destination, and refuses a single object over 5 GiB with `EntityTooLarge`. `UploadPartCopy` crosses clusters too. Live on Garage → MinIO: a 1 MiB copy, a 20 MiB copy that aws-cli turned into a multipart copy and that landed with the same `…-4` ETag, and a rename inside a ramping bucket. Tags are still not carried across a cross-cluster copy.
- **Conditional writes were answered by one cluster (2026-09-17, POC-6 item 1, ADR-0013).** While a bucket was `RAMPING` or `MIGRATING`, `If-None-Match: *` and `If-Match` went to whichever cluster the write landed on, which often did not hold the key: create-once answered 200 and made a second copy of an object that already existed on the other cluster, and update-if-current answered 412 over a version that was current. Both silently. shunt now `HEAD`s the other cluster first: a create-once whose object is there is refused with 412 and never sent; an update whose current version is there has its `If-Match` dropped and goes out as `If-None-Match: *` so a racing create still loses; a check that cannot be made answers 503. A cluster whose profile says it ignores `If-None-Match: *` (Garage 2.3.0) cannot judge create-once at all, so shunt judges that one itself in **every** state: the property test's guarded variant found 5,682 violations in the phase after cutover, where a create-once answered 200 having overwritten. Only `PutObject` and `CompleteMultipartUpload` pay a `HEAD`, and a bucket on one cluster that honours the header pays nothing. The property test grew create-once and update-if-current clients: without the fix a 15-second run reports 18 violations, and both 2-minute variants are green with it.
- **Clients had to take shunt's keys (2026-09-17, ADR-0012).** Resign mode verified signatures against keys shunt issued, so a brownfield insertion changed every client's credentials, and `step-out` then found a key no cluster knows. `shunt adopt --keys <file>` imports the keys a cluster already issued (checked against it first, the whole adopt refused if one fails), `shunt client add` imports one later, and `shunt client remove` drops one, all through the control API against a running shunt. `auth.Static` holds its map behind an atomic pointer, so `Lookup` on the request path stays lock-free while a key is added, and the credentials file is rewritten atomically at 0600 with `secret_ref`s kept as refs. Proven live on MinIO: a bucket clients already used with MinIO's own key, adopted with `--keys`, the generated key removed, the same client reading and writing through shunt unchanged, `step-out` READY, and with shunt stopped all 20 objects read straight from MinIO with the same key and bucket name.
- **Stepping out was not a supported path (2026-09-17, ADR-0011).** Nothing checked whether clients could go back to talking to their cluster directly. `shunt step-out [tenant]` (`GET /v1/tenants/{tenant}/step-out`) now reads the directory and asks the cluster: every bucket `ACTIVE` on one cluster under the name clients use, nothing mid-upload, and every client key accepted by that cluster with every bucket reachable — signing `ListBuckets` and a `HEAD` per bucket **as that key**. It changes nothing, exits non-zero while anything blocks, and prints the DNS flip and the stop when nothing does. `expand` now notes when a target's bucket name differs from the client's, which is the choice that decides this later. Proven live on Garage → MinIO: blocked while the bucket was on Garage (the client key was MinIO's), READY after the move with `--name`, and with shunt stopped all 40 objects read back byte for byte straight from MinIO with the same key and bucket name.
- **`purge-source` refused spuriously under concurrent deletes (found by the warp load test, 2026-09-17).** The diff walks both buckets' listings a page at a time. A client delete (source, then primary) that landed after the source's page was read and before the primary's made the key look missing from the primary. Every key the listings disagree on is now confirmed before it counts: a HEAD of the primary, and only if that answers 404, a HEAD of the source. Deletes reach the source first and nothing writes to the source after `MIGRATING`, so a source that still holds the key after the primary's 404 means the primary really lacked it; a concurrent delete can no longer produce a refusal. The check runs inline, so unconfirmed keys do not use up the 20-key report and cut the walk short. `TestPurgeDiffIgnoresConcurrentDeletes` deletes 25 keys between the two listings' second pages: before the fix it reported 20 of them and missed the one key really absent from the primary; now it reports only that key.
- **`expand` refused spuriously about 1 time in 20 on MinIO (found by `make walkthrough`, 2026-09-17).** After answering the conditional-write probe's `If-None-Match: *` PUT with 412, MinIO sometimes closes the connection without saying so. The next request, the `If-Match` DELETE, went out on that dead connection, and Go replays only requests it knows are idempotent, so it failed with a bare `EOF`. Every control-plane request to a backend is safe to send twice, so each is now marked idempotent (a zero-length `Idempotency-Key`, never sent or signed) and the transport replays it on a fresh connection. `TestBackendReplaysOnAStaleConnection` fails with `EOF` without the fix. Against the e2e MinIO, 2,000 canary-and-probe rounds went from 96 failures to 0.



- **A delete could be undone for good by a mover copy (ADR-0004 race 1).** shunt sent a migration delete to the primary first and the source second. A mover copy that landed on the primary between the two legs re-checked the source, still found the object there, and kept its copy. That copy outlived the delete, every later pass, and `CUTOVER`. Found by reading the code while investigating the property failure, never observed in a run. shunt now deletes on the source first. Regression test: `TestDeleteRacingAMoverCopyStaysDeleted` runs the five steps between the two legs and fails against the old order.
- **The mover's withdrawal of its copy could delete a client's newer write.** It now removes only its own copy: `If-Match` on its PUT's ETag where the target honors conditional deletes, otherwise a HEAD that must show that ETag and a `Last-Modified` no later than the PUT's `Date`. `shunt probe` now measures `If-Match` on DELETE. Neither Garage 2.3.0 nor MinIO honors it, so `capabilities.conditional_delete` defaults to false and the HEAD path, with its one-round-trip window, is what runs today. Regression test: `TestMoverWithdrawLeavesANewerClientWrite`.
- **A delete can half-succeed**: source leg done, primary leg refused. It is metered as `shunt_migration_dual_delete_total{outcome="primary_failed"}` (replacing `primary_only`, which described the old order), logged, and explained for operators in docs/migrating.md.
- **The property test did not test the mover** and its harness stalled on its own memory; see the correction above.

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

  **Blocker: the certificate.** VAST serves its self-signed factory certificate, whose names cover neither the cluster's S3 hostname nor its address (a wildcard matches exactly one label). The fix is a certificate whose SAN includes the S3 hostname, then `tls.ca` if it is not publicly issued. Plain http needs none, which is how POC-5 ran on VAST.

  **This is the POC-3 exit condition.** `make check-tls-verify` fails while any committed config or make target disables verification and prints each location; `make all` warns on every run until it passes. When it passes: add the VAST cluster to `test/e2e/shunt-mixed.yaml` and a placement to `test/e2e/directory-mixed.yaml`, re-run `make probe BACKEND=vast` and `make s3diff-mixed` with verification on, then tag `poc-3`.
- **Copying from a bucket that is mid-migration is refused with 501.** Its objects are split across two clusters and the backend doing the copy can read only one of them. It clears as soon as the bucket is `CUTOVER`.
- **Resolved on `distributed` (ADR-0016): cutover evidence is fleet-wide.** Kept for `master`:
- **Cutover evidence is per proxy.** `shunt cutover` reads the fallback-read counter of the proxy that serves the API, and the mover's progress is held in that proxy's memory (lost on restart; run the mover again). With several proxies, hold the window on each or check the fleet's `shunt_migration_fallback_reads_total` first; P4 aggregates it.
- **The AWS SDK ships in `bin/shunt`** for `shunt migrate run` (ADR-0009). P5 moves the mover to a separate `shunt-mover` binary and the SDK leaves `bin/shunt` with it.
- **P3c replaces the file directory behind the same API.** `shunt-control` serves `/v1/` from Postgres; the CLI verbs, docs/walkthrough.md and walkthrough.sh do not change. Until then the directory file must be writable by `shunt serve` for every operator verb, not only CreateBucket.
- **On VAST, `test/e2e/walkthrough.sh` has not run.** The migration has (by hand and under load, above), but the script creates buckets and the keys used could not. docs/walkthrough.md's output is still transcribed from the Garage → MinIO run with VAST names.
- **The CI walkthrough job has not run yet**: it was added without a push.
- **Versioned buckets cannot be migrated at all** (DESIGN §9 item 4): entering `RAMPING` or `MIGRATING` is refused if either side has ever had versioning enabled, and the check fails closed.
- **Bucket logging, replication, inventory, analytics, and notification answer 501** in resign mode: their configurations name buckets as ARNs and carry per-backend account ids.
- **Bucket policy passes through unrewritten**, so an ARN in a policy body can name the backend bucket (docs/reference/backend-compat.md).
- **Vendor identity is deliberately not hidden** (decision of 2026-09-15): `Server`, `X-Minio-*`, `X-Vast-*`, `<HostId>`, Garage's `<Region>`, GetBucketLocation, and backend owner ids reach clients. shunt hides which cluster and which bucket name, not which kind of backend.
- **Resolved on `distributed` (ADR-0015): a fleet member has no directory file; CreateBucket and DeleteBucket go to the control plane.** Kept for `master`:
- **The directory file must be writable for CreateBucket and DeleteBucket.** On Kubernetes a ConfigMap is mounted read-only, so those answer 503 until P3c. Several proxies converge only within `directory.poll_interval` (default 1 s), so a bucket created on one is `NoSuchBucket` on another until then; ADR-0004 in POC-4 accounts for it.
- Credentials file inline secrets; encrypted at rest is P3c.
- Garage 2.3.0 rejects signed trailers directly; through shunt they work because shunt verifies them and forwards an unsigned trailer.
- Garage ignores `If-None-Match: *` on PUT (docs/reference/backend-compat.md). POC-4 closes this with the HEAD-then-commit guard of ADR-0004, chosen per cluster from `capabilities.conditional_write`; migrations into Garage are exercised by `test/e2e/evacuate.sh`.
- ADR-0002 compensation is log-and-alert only (`sha256`, `trailer` reasons); the compensating delete is P2.
- Passthrough preserves `Host` including shunt's port, and backends echo it into `<Location>` of CompleteMultipartUpload (MinIO) with the backend's own scheme. Inherent to passthrough; resign mode rewrites it. s3diff normalises the port.
- Backends that send no `Content-Type` (Garage) are relayed without one; net/http's sniffing is disabled per response. Found by s3diff, fixed in POC-1.
- The bench's 1 GiB tier runs at 1 connection only (disk on the dev box).
- go.yaml.in/yaml/v4 is at a release candidate (v4.0.0-rc.6); bump when v4.0.0 ships.

## Closed in POC-5

- **The mover gap** (carried from POC-4: "the mover is a test fixture"). `test/mover` is deleted; `shunt migrate run` is the mover, with the same contract, both refusals, cursor and ledger paths as flags, `--until-converged`, and progress reported to the control API that `cutover` checks.
- **Clusters needed a restart to change.** They are directory state, added and removed live.

## Closed in POC-3

- **The backend address leak in `<Location>`** (POC-2 carried gap): the rewriter replaces it with the client-facing URL. A unit test asserts the exact value for path-style and virtual-host clients, and the mixed s3diff run fails if any endpoint appears in a response.
- **Path-style `<Resource>` echoes on virtual-host requests** (ADR-0001 amendment): rewritten to the client's own path.
- **The s3diff blind spot on VAST**: the mixed run no longer relies on a direct-vs-via difference to see a leak. It scans every via response for backend names and endpoints and asserts `<Location>` contents directly, which works even when both sides address the same host.
