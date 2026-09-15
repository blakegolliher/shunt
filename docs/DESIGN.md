# Shunt — S3 front-end proxy: design + Claude Code build prompts (v3)

Name: **shunt** — to divert traffic onto another track without stopping it. Binary `shunt`, module `github.com/blakegolliher/shunt`, metric prefix `shunt_`.

v2 changes from v1: TLS is userspace `crypto/tls` on both sides; no kTLS, no NIC offload, no sockmap, no hardware assumptions anywhere. See `docs/validation-report.md` for every assumption checked and every correction made.

v3 changes from v2: shunt is a **global endpoint over heterogeneous backends** — VAST, MinIO, AWS S3, or any S3 endpoint, mixed at the same time. The namespace is tenant-scoped, `resign` auth is mandatory for it, the directory moves to **Postgres behind a control service** (§1.5), migrations gain a deterministic **RAMPING** state (§2.5), and the rule that shunt never stores per-object location is now enforced by the schema, not just by policy.

**What it is.** A TLS-terminating, streaming S3 reverse proxy in Go that owns the client-facing endpoint so the storage behind it can change without clients noticing: migrate a bucket between clusters, add clusters, balance across nodes, tier to a cold cluster, and present S3 Glacier storage-class semantics. Telemetry is a product output, not an afterthought.

**What it is not.** Not an object store, not a POSIX translator, not a cache with its own on-disk state. It holds no per-object state. Everything it needs is in the request, in a small bucket-level directory, or in the backends themselves. That single constraint is what keeps it low-overhead and horizontally scalable.

Section 7 is the thing you asked for: copy-paste prompts for Claude Code, one per phase, plus guardrail prompts that run at the end of every phase. Sections 1–6 are the design those prompts reference. Put this file in the repo as `docs/DESIGN.md`; every prompt starts by reading it.

---

## 0. Decisions to confirm (defaults chosen — reply "change N")

1. **Auth model:** two modes. `passthrough` (Phase 1: Host preserved, backend verifies signatures) is a single-cluster and dev mode. `resign` (Phase 2: proxy verifies client SigV4 against shunt-issued credentials, re-signs upstream with each cluster's own credentials) is **mandatory for the global endpoint**: customers hold one set of shunt keys (new tenants) or keep their existing keys, which shunt imports (adopted buckets — §11); AWS, MinIO, and VAST each keep their own. It is also the only mode that lets a bucket cross credential domains during a migration.
2. **Project license:** Apache-2.0. Permissive so partners can run and modify it. Consequence: no code copied from MinIO (AGPL-3.0) — using MinIO tools (warp, mint) is fine, copying their SigV4 code is not.
3. **Backends:** any S3 endpoint, and several kinds at once — VAST, MinIO, AWS S3, others — behind one client-facing endpoint. Each cluster record carries a `type` and a **capability profile** (probe-filled: sha256 enforcement, unsigned trailers, conditional writes, checksum algorithms, native storage classes, region behavior, endpoint mode). VAST is the reference target; Garage, versitygw, and MinIO are the CI backends; an AWS scratch bucket is a CI target when credentials are present.
4. **"Glacier API" means** S3 storage classes (`GLACIER`, `GLACIER_IR`, `DEEP_ARCHIVE`), `RestoreObject`, `x-amz-restore` headers, and lifecycle `Transition` rules — not the legacy Glacier vault REST API (`/-/vaults/…`).
5. **HTTP/1.1 only.** AWS S3 endpoints do not speak HTTP/2 (verified); no S3 client needs it. Not building it removes a class of bugs.
6. **Tiering granularity v1:** bucket/prefix policy with hot-then-cold lookup. No per-object location index. An index is a v2 decision, taken only if the metered fallback cost justifies it.
7. **Directory state:** a hot-reloaded file for dev and single-site; **Postgres behind a small control service** for the product (§1.5). Proxies never open a database connection on the request path — they serve from an in-memory snapshot refreshed by version, and keep serving last-known-good if Postgres is down. No etcd, no Raft, no control bucket.
8. **Data mover is out of process.** The proxy provides migration routing semantics and the invariants a mover must respect; the mover is a separate tool.
9. **TLS and the data path:** userspace `crypto/tls`, `net/http`, `io.CopyBuffer` with pooled buffers. There is no kernel zero-copy path through `net/http` (see §1.4), so proxy CPU scales with bytes and capacity is planned as CPU-per-GiB × throughput, scaled horizontally. eBPF is used for one thing: `sock_ops` TCP telemetry, with a `TCP_INFO` fallback. XDP/DSR is an L4 tier in front of shunt — documented, not built.
10. **Telemetry stack:** prometheus/client_golang for metrics (leanest hot path), OpenTelemetry for traces, `slog` JSON access logs, and an in-process slow-request ring buffer (the s3slower pattern).
11. **YAML library:** `go.yaml.in/yaml/v4` (YAML org fork). `gopkg.in/yaml.v3` is archived and unmaintained.
12. **Versioned buckets cannot enter `RAMPING` or `MIGRATING` in v1.** Version IDs are backend-generated and cannot be reproduced through the S3 API, so a versioned history cannot be moved by a mover. The proxy refuses the transition; migrating a versioned bucket is a v2 problem with a different mechanism.
13. **Namespace is tenant-scoped.** The directory key is `(tenant, bucket)`; the tenant comes from the verified access key. Two customers can both own a bucket called `data`. Each placement maps the client-facing name to a **backend bucket name per cluster** (`t7f3-data` on AWS, `data` on VAST), so AWS's global namespace and per-tenant collisions are both handled in one field.
14. **`ListBuckets` is synthesized from the directory**, never proxied — a tenant's buckets span clusters. `CreateBucket` lands on the tenant's default cluster unless a placement policy says otherwise.
15. **RAMPING is deterministic per key, monotonic, and mover-free.** Writes shift to the target by `hash(key) < ratio` or by prefix rule, never by request percentage (that breaks overwrites); the ratio only rises; the mover starts only at ratio 1. §2.5 has the reasoning.
16. **Postgres never holds a per-object row.** Tenants, credentials, clusters, placements, migration jobs with a resumable cursor, restore jobs, hourly usage rollups, and an audit log — all bounded by tenants × buckets, not by objects. Per-object facts (where an object is) come from the lookup order; per-object history (when the mover copied it) is a JSONL ledger in object storage, never on the request path.
17. **Tiering has two modes per placement:** `native` — the backend implements storage classes itself (AWS), so `x-amz-storage-class`, `RestoreObject`, and lifecycle rules pass straight through; `emulated` — shunt's hot/cold clusters (§2.6) for backends without archive classes. Mixed backends make both necessary.

---

## 1. Architecture

### 1.1 Request lifecycle

```
client ──TLS──▶ listener ─▶ parse(bucket,key,op) ─▶ auth(verify) ─▶ directory(bucket→placement,state)
        ─▶ policy(migration / tier decision) ─▶ upstream pool(pick endpoint) ─▶ sign ─▶ backend
        ◀────────────── stream response; headers rewritten only where the design says so ◀────────
                           telemetry taps at every arrow · body bytes are never buffered
```

### 1.2 Components

| Component | Responsibility | Phase |
|---|---|---|
| `listener` | TCP accept (SO_REUSEPORT), TLS termination, cert hot-reload, SNI, optional client mTLS | 1 |
| `s3` | Request model: bucket/key extraction (virtual-host and path style), operation classifier (~60 ops), error XML | 1 |
| `sigv4` | Canonicalization, header + presigned verification, aws-chunked decode, upstream signing | 2 |
| `auth` | `CredentialStore` (static file → pluggable), tenant label | 2 |
| `directory` | `(tenant, bucket)` → placement (clusters, backend names, state, ramp rules, tier policy); file backend for dev; snapshot cache over the control service for the product | 3a, 3b, 3c |
| `control` | separate binary `shunt-control`: the only writer to Postgres; admin/CLI API; serves versioned directory snapshots to proxies; owns migration jobs and their cursors | 3c |
| `upstream` | per-endpoint pools, health, load balancing, retries, timeouts | 3 |
| `proxy` | the pipeline; one handler, no middleware framework | 1 |
| `migrate` | state-dependent routing, listing merge, uploadId encoding | 5 |
| `tier` | lifecycle rules, hot/cold routing, restore jobs, `x-amz-restore` synthesis | 6 |
| `telemetry` | metric catalog, traces, access log, slow ring, metering counters | 1, 4 |
| `ebpf` | `sock_ops` TCP stats; `TCP_INFO` fallback | 4 |
| `admin` | `/-/healthz`, `/-/ready`, `/-/metrics`, `/-/routes`, `/-/upstreams`, `/-/slow`, `/-/drain`, `/debug/pprof` on a separate listener | 1, 4 |

### 1.3 State model

- **Per object:** none. Ever. Any proposal that adds it needs an ADR.
- **Per tenant:** default cluster, credentials (encrypted at rest), quotas. Bounded by customers.
- **Per bucket:** one placement row (schema in 2.3): clusters, backend bucket names, state, ramp rules, tier policy, lifecycle config, and a small set of restore jobs — bucket-scoped rows keyed by object, explicit and rare. Bounded by tenants × buckets.
- **Per cluster:** type, capability profile, endpoint list or DNS name, region, credentials, health.
- **Per migration job:** source, target, state, a resumable cursor (last key per prefix range), counts. The per-object ledger of what was copied and when is JSONL in object storage, not a table.
- **Per process:** connection pools, slow ring, metrics. Losing a proxy loses nothing.
- **Per in-flight multipart upload:** none — the cluster is encoded in the uploadId (2.4).

### 1.4 The data path, honestly

- TLS terminates in Go's `crypto/tls`. Its AES-GCM and ChaCha20 implementations are assembly-optimized and Go orders cipher suites by what the CPU supports; do not override the ordering.
- Bodies move with `io.CopyBuffer` through pooled buffers, client ↔ upstream, in both directions. `net/http` offers no kernel zero-copy path here: `net.TCPConn.ReadFrom` only uses `splice(2)` when its source is another `*net.TCPConn`, and inside `net/http` the bodies on both sides are `*http.body` wrappers. So every byte is copied through userspace and encrypted/decrypted in userspace — on both sides when upstream is TLS, on one side when upstream is plaintext on a trusted fabric.
- Consequence for design: the proxy is CPU-bound on large objects and allocation-bound on small ones. The levers are copy-buffer size, per-request allocations, GC settings, cores, and process count behind SO_REUSEPORT. Phase 7 measures each. Capacity planning is CPU-seconds per GiB (measured) × target throughput, scaled horizontally behind an L4 tier.
- eBPF's one job: a `sock_ops` program attached to the proxy's cgroup records RTT and retransmit events per upstream connection without kprobes on the hot path. Kernel feature (CAP_BPF + BTF), feature-flagged, with `getsockopt(TCP_INFO)` sampling as the fallback on hosts without it.
- Shunt must be DSR-friendly (VIP on `lo`, no reliance on source NAT, client IP visible) so an L4 tier in front can return proxy responses directly. That costs nothing and is all the L4 story shunt owns.

### 1.5 Control plane: Postgres behind a control service

- **Two binaries, one repo.** `shunt` (the proxy) and `shunt-control` (the control plane). Proxies are stateless and never hold a database connection. `shunt-control` is the only writer to Postgres and the only reader on behalf of proxies; the CLI (`shunt directory …`, `shunt tenant …`, `shunt migration …`) and the admin UI-to-be talk to it over an authenticated HTTP API.
- **Snapshot, not queries.** The directory has a monotonically increasing `version`. Each proxy polls `GET /v1/directory?since=<version>` every few seconds (or subscribes to a change stream) and swaps in a new immutable in-memory snapshot. A request never waits on Postgres. Up to a few hundred thousand placements fit in memory as a full snapshot; past that, proxies switch to a per-bucket LRU with negative caching, filled from the control service — same interface, same version semantics.
- **Outage behavior is defined.** Postgres or `shunt-control` down: proxies keep serving last-known-good; `CreateBucket`, state transitions, credential changes, and migration control return `503 ServiceUnavailable` with a clear message; a `shunt_directory_snapshot_age_seconds` gauge makes the staleness visible. Nothing already flowing stops.
- **Schema, at the level that matters:** `tenants`, `credentials` (access key → encrypted secret, tenant, status), `clusters` (type, capability profile, endpoints/DNS, region, scheme, credential ref), `placements` (tenant, bucket, state, primary, source, backend names per cluster, ramp rules, tier mode/policy, lifecycle XML, version), `migrations` (placement, state, cursor, counts, started/finished), `restore_jobs`, `usage_hourly` (tenant, bucket, cluster, bytes/requests by class), `directory_changes` (who changed what, when, from what to what — the audit trail that answers "when did writes move"). Every table is bounded by tenants, buckets, clusters, or explicit jobs. **There is no objects table and there will not be one.**
- **HA is the deployer's choice**, not shunt's: Patroni or CloudNativePG on RKE2, RDS elsewhere. Read replicas per site are fine because proxies only need eventual, versioned consistency. Writes go to the primary through `shunt-control`.
- **Migrations of the schema** are plain SQL files embedded in `shunt-control` with a tiny versioned runner; no ORM. Postgres access via `pgx` (MIT).

---

## 2. The decisions that shape everything

### 2.1 Streaming, never buffering

Bodies flow through `io.CopyBuffer` in both directions. Consequence: some failures are discovered only after the upstream operation has completed — a `x-amz-content-sha256` mismatch, a trailer checksum mismatch. The design handles these by **compensation**: let the upstream write complete, then on mismatch issue `DeleteObject` (or `AbortMultipartUpload`/drop the part) and return the S3 error the client expected. Caveat: on a versioned bucket the compensating delete creates a delete marker rather than erasing the version. This is documented in ADR-0002, and Phase 2 probes each backend for which checks it enforces itself so compensation is the exception.

### 2.2 Auth modes

**passthrough.** Request forwarded with `Host` preserved; the backend verifies. Requires the backend to accept the proxy's hostname as a virtual-host domain and share the credential database. Zero crypto in the proxy, cannot cross credential domains, cannot re-route presigned URLs across clusters. Correct for "one cluster, add load balancing and telemetry".

**resign.** Proxy parses `Authorization: AWS4-HMAC-SHA256 …` or presigned query params, looks up the secret in `CredentialStore`, recomputes the signature, checks `x-amz-date` skew (±15 min), then signs the upstream request with the target cluster's credentials. SigV4A (`AWS4-ECDSA-P256-SHA256`) is rejected with a clear error; it is out of scope. Payload handling:

| Client `x-amz-content-sha256` | Proxy action |
|---|---|
| `UNSIGNED-PAYLOAD` | forward as-is |
| hex SHA-256 | forward the header unchanged; the backend hashes the body and enforces the mismatch itself. Phase 2 probes whether each backend actually enforces it; if not, proxy verifies while streaming and compensates (2.1) |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | verify each chunk signature against the seed with the client's secret, decode, forward as `UNSIGNED-PAYLOAD` with `Content-Length = x-amz-decoded-content-length` |
| `…-PAYLOAD-TRAILER` (signed chunks + checksum trailer) | as above, but forward as `STREAMING-UNSIGNED-PAYLOAD-TRAILER` so the backend validates the checksum; if the backend doesn't support unsigned trailers, proxy verifies the checksum and compensates |
| `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | pass through |

Presigned URLs are verified with the client's secret and re-issued upstream as header-signed requests — which is what makes presigned-URL consumers migratable. STS session tokens (`x-amz-security-token`) are out of v1 unless the credential store can validate them; the seam exists.

### 2.3 Directory and routing

```yaml
clusters:
  vast-a:   { type: vast,  scheme: https, endpoints: [10.1.0.11:443, 10.1.0.12:443, ...], tls: {ca: /etc/shunt/vast-a.pem}, region: us-east-1, credentials: {access_key: ..., secret_ref: env:VAST_A_SECRET}, storage_classes: emulated }
  vast-fab: { type: vast,  scheme: http,  endpoints: [10.9.0.11:80, ...], region: us-east-1, credentials: {...} }   # plaintext is a per-site decision: scheme is required, never defaulted
  minio-1:  { type: minio, scheme: https, endpoints: [minio-1.lab:9000, ...], tls: {ca: ...}, region: us-east-1, credentials: {...} }
  aws-use1: { type: aws,   scheme: https, endpoint_mode: dns, endpoint: s3.us-east-1.amazonaws.com, region: us-east-1, credentials: {...}, storage_classes: native }
  vast-cold: { type: vast, scheme: https, endpoints: [...], storage_class: GLACIER_IR, access: instant }            # emulated cold: instant with GLACIER_IR or any non-archive class
  vault:     { type: vast, scheme: https, endpoints: [...], storage_class: DEEP_ARCHIVE, access: restore-required } # emulated cold: restore-required only with GLACIER or DEEP_ARCHIVE
tenants:
  acme: { default_cluster: vast-a }
  zed:  { default_cluster: minio-1 }
placements:                      # key is (tenant, bucket); backend names are per cluster
  acme/training-sets: { state: ACTIVE,    primary: vast-a,   names: {vast-a: training-sets} }
  acme/checkpoints:   { state: RAMPING,   primary: vast-b,   source: vast-a, ramp: {ratio: 0.05}, names: {vast-a: checkpoints, vast-b: checkpoints} }
  acme/runs:          { state: RAMPING,   primary: aws-use1, source: vast-a, ramp: {prefixes: ["2026-09/"]}, names: {vast-a: runs, aws-use1: acme-7f3a-runs} }
  zed/data:           { state: MIGRATING, primary: minio-1,  source: aws-use1, names: {aws-use1: zed-91c0-data, minio-1: data} }
  acme/archive:       { state: ACTIVE,    primary: vast-a,   cold: vast-cold, tier: emulated, lifecycle: <S3 lifecycle XML>, names: {...} }
  zed/logs:           { state: ACTIVE,    primary: aws-use1, tier: native, names: {aws-use1: zed-91c0-logs} }   # storage classes and RestoreObject pass through to AWS
```

The tenant is the verified access key's tenant. Unknown `(tenant, bucket)` → `NoSuchBucket`. `CreateBucket` lands on the tenant's default cluster with a generated backend name and writes a placement row. `ListBuckets` is answered from the directory. State transitions are validated (`ACTIVE → RAMPING → MIGRATING → CUTOVER → ACTIVE`, with `RAMPING` skippable); the proxy refuses illegal jumps and refuses `RAMPING`/`MIGRATING` for a source bucket with versioning enabled (decision 12). `check-config` enforces the storage-class/access-mode pairing and that every cluster has a `type`, `scheme`, and `region`.

Backends: the file form above is for dev and single-site. The product form is the same model in Postgres behind `shunt-control` (§1.5); proxies consume identical snapshots from either, through the one `Directory` interface.

### 2.4 Stateless IDs

`InitiateMultipartUpload` responses are rewritten so `UploadId = <clusterID>~<backendUploadId>`; the prefix is stripped on every subsequent `uploadId=` request and used for routing. `ListMultipartUploads` and `ListParts` responses get the same rewrite. `~` is RFC 3986 unreserved so the value survives URL encoding, and the split is on the first `~` only so backend IDs may contain anything. Multipart uploads therefore survive a bucket's state changing mid-upload with no lookup table. Three XML rewrites, always on, no state-dependent branches. Tolerance rule: an incoming uploadId with no `~` prefix routes to the bucket's primary, so uploads in flight across the upgrade that introduces the codec still complete.

### 2.5 Migration semantics (per bucket, `source → primary`, unversioned buckets only)

The process is: shift writes first, serve old data from the old place, backfill, cut over. `RAMPING` exists so the write shift can be gradual; everything in it is deterministic per key.

**Why not a percentage of traffic.** If 5 % of *requests* go to the target, a key can land there once and on the source the next time, and a target-first read returns the stale copy. The split must be a function of the key: `hash(key) < ratio` (64-bit hash of the key, fixed seed) or a prefix rule. Then every write of a key lands on one side, and reads stay correct. Prefix rules match how AI data is laid out (`runs/2026-09/`), so a ramp can go by dataset instead of by hash. Two rules follow: the ratio and prefix set only grow (shrinking moves keys back to the source side while their newest version is on the target — reconcile that range target→source first), and the mover does not start until the ratio is 1, because a key copied to the target but still written to the source would read stale from the target.

| Op class | ACTIVE | RAMPING (key in target range / not) | MIGRATING | CUTOVER |
|---|---|---|---|---|
| PUT / multipart / tagging / ACL writes | primary | primary / source | primary | primary |
| GET / HEAD | primary | primary, on 404 → source / source only (target can't have it) | primary, on 404 → source | primary |
| DELETE | primary | primary **and** source | primary **and** source (prevents mover resurrection) | primary |
| LIST (ListObjects, ListObjectsV2, ListMultipartUploads) | primary | sorted merge of both, primary wins on collision; continuation token = base64 `{p:…, s:…, last:…}` | same merge | primary |
| Bucket-level config ops | primary | primary | primary | primary |

Each ramp step and state change is a row in `directory_changes` with actor and timestamp: that is the record of when writes moved. The ramp is judged on the dashboard by comparing the target cluster to the source on the same op mix (errors, p99, TTFB); "comfortable" is a number. Convergence during `MIGRATING` shows two ways: the migration job's remaining count, and `shunt_migration_fallback_reads_total` trending to zero. `CUTOVER` requires an empty listing diff and zero fallback reads over the operator's comfort window.

Mover contract (the proxy enforces nothing here; the mover must): copy `source → primary` with `If-None-Match: *` so a client write is never overwritten; after each copy, re-`HEAD` source and if it vanished, delete the copy (closes the delete/copy race to a small window); **reproduce the multipart part layout of the source object, or accept that its ETag changes** — the proxy cannot mask ETag drift, and clients holding cached ETags for `If-Match` will see `412` after cutover; this matters most across backend types, whose part-size defaults differ; keep a resumable cursor in the migration job row and append a JSONL ledger (key, size, ETag, copied_at, verified) to object storage; report convergence. `CUTOVER` is entered by an operator after convergence; `source` becomes read-only quarantine for a retention window, then the row returns to `ACTIVE` on `primary`, then the source bucket is deleted. Backend-native replication (VAST↔VAST, AWS same-account) may replace the mover for the bulk copy only if it is proven never to overwrite a target object newer than the source's. The remaining races are listed in ADR-0004 with the window size for each.

### 2.6 Tiering and Glacier semantics

- **Two modes per placement.** `native`: the primary backend implements storage classes (AWS): `x-amz-storage-class`, `RestoreObject`, `x-amz-restore`, and `?lifecycle` pass straight through and shunt only meters them. `emulated`: everything below, for backends without archive classes. A placement is one or the other; a migration between a native and an emulated backend is a migration of the policy too, and the runbook says so.
- Policy language is **S3 lifecycle XML** (`PUT/GET/DELETE ?lifecycle`), stored in the placement record. No invented DSL. v1 supports `Transition` (by `Days` or `Date`, to a `StorageClass`) and optionally `Expiration`.
- `PUT` with `x-amz-storage-class` matching the cold cluster's class writes directly to cold.
- `GET`/`HEAD`: hot first, on 404 → cold. If the cold cluster is `access: restore-required` (only legal with `GLACIER`/`DEEP_ARCHIVE`), a cold hit returns `403 InvalidObjectState` with the XML body AWS returns, and the object's `StorageClass` reports the cold class. `GLACIER_IR` is `instant` by definition, matching AWS.
- `POST ?restore` (RestoreObject): `Days` required, `Tier` accepted and ignored. `202` for a new job, `200` if the object is already restored (and `Days` extends the expiry), `409 RestoreAlreadyInProgress` while a job is running. A worker copies cold → hot with `x-amz-meta-shunt-restore-expiry`; `HEAD`/`GET` synthesize `x-amz-restore: ongoing-request="true"` then `ongoing-request="false", expiry-date="…"`. A sweeper deletes expired hot copies. Restore jobs persist through the Directory (Postgres via `shunt-control` in the product, the file backend in dev); the worker takes a lease on a job row.
- Demotion is `shunt tier run`: list hot, apply rules, conditional copy to cold, verify ETag, delete hot. Listing of a tiered bucket reuses the migration merge (2.5) with `StorageClass` rewritten per source cluster.
- Optional read-through promotion (copy to hot asynchronously on a cold hit) is what turns this into the caching layer; it is a flag, default off.

### 2.7 Telemetry

Rules before signals:

1. The catalog (`docs/telemetry-catalog.md`) is written before the code, and CI fails on a metric not in it.
2. Bounded cardinality: labels are `op`, `status_class`, `cluster`, `endpoint`, `tenant`, `mode`, `reason`. `bucket` is a label only for buckets in a non-ACTIVE state (bounded by definition) or on an explicit allowlist. Object keys never.
3. Every request carries two IDs: the upstream's `x-amz-request-id` and shunt's own `x-shunt-request-id`, both in the access log and the trace.
4. Metering-grade counters: `shunt_tenant_bytes_total{tenant,direction,cluster}` and `shunt_tenant_requests_total{tenant,op}` are monotonic, never sampled, and exported as usage records (JSON lines, hourly) for billing.

Signals: RED per op and per endpoint (`requests_total`, `request_duration_seconds`, `upstream_ttfb_seconds`, `bytes_in/out_total`, `inflight`), connection stats (`upstream_cx_active`, `cx_connect_seconds`, `cx_errors_total{reason}`), auth (`auth_failures_total{reason}`), TLS (`tls_handshake_seconds{version,cipher,resumed}`), routing (`route_state{bucket,state}`, `migration_fallback_reads_total`, `tier_fallback_reads_total`, `listing_merge_seconds`, `restore_jobs{state}`), TCP (`tcp_srtt_seconds{endpoint}`, `tcp_retransmits_total{endpoint}` from eBPF or TCP_INFO), runtime (`copy_buffer_bytes`, GC and heap from the Go collector). Traces: one span per request with children `tls`, `parse`, `auth`, `route`, `upstream.connect`, `upstream.headers`, `upstream.ttfb`, `upstream.body`, `response`; `traceparent` propagated upstream. Slow ring: last N requests over a threshold with full timing breakdown, on `/-/slow`.

### 2.8 Load balancing and resilience

Two endpoint modes per cluster. `static` (VAST VIP pools, MinIO nodes): power-of-two-choices on in-flight count, tie-broken by EWMA latency, across the listed endpoints. `dns` (AWS regional endpoints, anything behind someone else's balancer): one logical endpoint, DNS re-resolved on a short TTL, health and ejection tracked per resolved address; shunt does not try to out-balance AWS. AWS-specific: honor `x-amz-bucket-region` on a 301 once, then update the cluster's region and never loop. Active health: unsigned `GET /` every N seconds; any HTTP response counts as alive (AWS returns 403, other backends vary). Passive: consecutive connect failures or 5xx eject an endpoint with exponential backoff, capped so at least `ceil(N/2)` endpoints of a cluster stay eligible. Retries: `GET`/`HEAD`/`LIST`/`DELETE` retry once on connect error or 503 received before any body byte was forwarded; `PUT` and parts are never retried by the proxy — the client SDK's retry is the right layer. Timeouts: metadata ops get a total deadline; data ops get an idle-progress deadline (no bytes for N seconds), never a total one. `Expect: 100-continue` is chained end to end so an unauthorized 5 GiB PUT fails before the client sends a body byte.

### 2.9 TLS

Cert hot-reload via `GetCertificate` reading an atomically-swapped pair; SNI map for multiple domains; wildcard cert required for virtual-host bucket addressing; TLS 1.2 minimum, 1.3 preferred; Go's default cipher-suite ordering (CPU-aware) left alone; session ticket keys rotated with `SetSessionTicketKeys`; optional client mTLS with tenant mapping from the cert. Upstream: `scheme` is required per cluster. `https` verifies against the configured CA; `http` is allowed only when stated explicitly (a per-site decision, logged at startup and shown on `/-/upstreams`) and removes one of the two crypto passes per byte. There is no default; `check-config` rejects a cluster without a scheme.

### 2.10 Simplicity rules (these go into `CLAUDE.md` verbatim)

- Two binaries. `shunt` (the proxy) with subcommands `serve`, `adopt`, `expand`, `ramp`, `migrate`, `cutover`, `tier run`, `restore worker`, `directory`, `probe`, `check-config`, `doctor`, `version`; `shunt-control` (the control plane, Phase 3c).
- Standard library first. Every dependency has one line in `docs/deps.md` saying why the stdlib wasn't enough.
- No interface with a single implementation, except two named seams: `CredentialStore` and `Directory`.
- No middleware framework, no DI container, no plugin system. One handler, one pipeline, explicit calls.
- Any buffering of a request or response body requires an ADR and a benchmark. (`io.CopyBuffer` through a pooled fixed-size buffer is streaming, not buffering.)
- Every package has tests and at least one benchmark; `-race` in CI; fuzz targets for every parser (op classifier, SigV4 canonicalization, aws-chunked decoder, XML rewriters, lifecycle XML, continuation tokens).
- Feature flags default off, live in one file, and each has a removal criterion.
- Config is validated at startup and by `check-config`; unknown keys are errors.
- ADRs in `docs/adr/` for every decision in Section 2 and any that overrides it.
- Nothing in the code or docs assumes a NIC, a CPU model, or a kernel feature beyond what `doctor` checks and a fallback covers.
- A phase is done when its acceptance gate (Section 8) passes, not when the code compiles.

---

## 3. Non-goals for v1

Object-level location index; migrating versioned buckets; erasure/replication across clusters; write-back caching with local disk; HTTP/2; SigV4A; S3 Select; legacy Glacier vault API; being the data mover; being the L4 balancer; kernel or NIC crypto offload; a web UI.

## 4. Repo layout

```
cmd/shunt/            proxy: main + subcommands
cmd/shunt-control/    control plane: Postgres, admin API, directory snapshots (Phase 3c)
internal/control/     control-service handlers, schema migrations, snapshot builder
internal/listener/      TLS, accept
internal/s3/            request model, op classifier, virtual-host, error XML
internal/sigv4/         canonicalize, verify, sign, aws-chunked
internal/auth/          CredentialStore + static file impl
internal/directory/     placement model, states, ramp rules; file backend + control-snapshot backend
internal/upstream/      pools, health, LB, retries, timeouts
internal/proxy/         the pipeline handler
internal/migrate/       state routing, listing merge, uploadId codec
internal/tier/          lifecycle XML, hot/cold routing, restore jobs, sweeper
internal/telemetry/     catalog, metrics, tracing, access log, slow ring, metering
internal/ebpf/          sock_ops program (bpf2go), loader, TCP_INFO fallback
internal/admin/         admin listener
internal/config/        schema, validation, hot reload
test/s3diff/            differential harness: direct vs via-proxy
test/e2e/               docker-compose backends (Garage, versitygw), scenarios
test/bench/             benchmark harness + baselines
docs/                   Diátaxis: tutorials/ how-to/ reference/ explanation/  + adr/  + prompts/
```

## 5. Dependencies and licenses

Shunt's own license: Apache-2.0. Generate `THIRD_PARTY_NOTICES` with `go-licenses` (Apache-2.0) in CI; run `govulncheck` in CI.

| Dependency | License | Obligation when shipped |
|---|---|---|
| `github.com/cilium/ebpf` | MIT | preserve copyright + license notice |
| `github.com/prometheus/client_golang` | Apache-2.0 | preserve LICENSE/NOTICE; mark modified files if forked |
| `go.opentelemetry.io/otel/*` | Apache-2.0 | same |
| `golang.org/x/sys` | BSD-3-Clause | preserve notice |
| `github.com/jackc/pgx/v5` (control service only) | MIT | preserve notice; proxies do not link it |
| `go.yaml.in/yaml/v4` (YAML org fork; `gopkg.in/yaml.v3` is archived) | MIT (libyaml-derived files) + Apache-2.0 (rest) | preserve both notices |
| `github.com/aws/aws-sdk-go-v2` (tests only) | Apache-2.0 | preserve notices in test tree |
| versitygw (reference reading, CI backend) | Apache-2.0 | if any code is copied: preserve notices **and mark the file as modified** |
| Garage (CI backend, tool only) | AGPL-3.0 | run it; never copy code into shunt |
| MinIO server / warp / mint (tools only) | AGPL-3.0 | run them; never copy code into shunt |
| ceph/s3-tests (conformance tool) | MIT (verify) | tool only |

## 6. Validation strategy

- **`s3diff` is the primary tool.** It runs every request in a matrix (op × object size × addressing style × payload signing mode × tricky keys) against the backend directly and via shunt, and diffs status, headers (minus an allowlist: `Server`, `Date`, request IDs), and body hash. The proxy's core correctness property is transparency, and this measures it directly.
- **Conformance suites** against the proxy with a known-good backend: `ceph/s3-tests` subset, MinIO `mint`, aws-cli smoke script, and the checksum/part-number/GetObjectAttributes probe scripts already built for VAST compat work.
- **Property tests** for migration: random client ops interleaved with a naive mover; invariants checked after CUTOVER.
- **Fuzzing** of every parser, 60 s per target in CI, hours nightly.
- **Chaos**: backend killed mid-PUT and mid-GET, slow backend, cert rotation under load, config reload under load, memory limit.
- **Benchmarks** with a fixed method (Section 8); a >5 % regression fails the phase.

---

## 7. Claude Code prompts

**How to use them.** One phase per session (or `/clear` between phases). Commit this file as `docs/DESIGN.md`, the answered `docs/CONTEXT.md`, and each prompt as `docs/prompts/P<n>.md` so the build history is reproducible. **Order for the first milestone (one cluster, LB + telemetry, deployable on bare metal and RKE2): P0 → P1 → P3a → P4 → P8a.** Then P7 as soon as real traffic exists, then P2 → P3b → P3c → P5 → P6 → P8b. The global endpoint (mixed backends, tenants, Postgres) is everything from P2 onward; M1 is deliberately a single cluster. Section 10 explains the sequencing. Use plan mode for P2, P5, and P6 — they are where the design has the most edges. At the end of every phase run G1–G4 in order, then the phase-close prompt. Do not start the next phase until the gate in Section 8 passes.

### Kickoff (paste at the start of every session)

```
Read CLAUDE.md, docs/DESIGN.md, docs/CONTEXT.md, docs/STATUS.md, and every file in docs/adr/ before doing anything else. Treat docs/CONTEXT.md as ground truth about the environment, clients, and backends; do not re-derive anything it already answers.
Then tell me in at most 10 lines: what is built, which phase we are in, what the acceptance gate for this phase is, and what you would do first.
Do not write or modify code until I say go.
```

### P0 — Bootstrap and guardrails

```
Phase 0: repository bootstrap. Read docs/DESIGN.md fully first. Goal: a repo where every later phase is forced into the design's constraints by tooling, not by memory.

Build:
1. Go module github.com/blakegolliher/shunt on the latest stable Go. Directory layout exactly as docs/DESIGN.md §4; empty packages get a doc.go stating their responsibility from §1.2.
2. cmd/shunt with subcommands `version` (version, commit, build date via -ldflags) and `check-config` (parse + validate a YAML config against internal/config; unknown keys are errors). Only these two commands work in this phase.
3. internal/config: schema mirroring §2.3 (clusters, defaults, buckets, listener TLS, admin, telemetry, feature flags). Validation with specific error messages, including the storage-class/access-mode pairing rule from §2.3 and refusal of MIGRATING on a versioned source (this check runs at transition time; the config validator only checks structure). Table-driven tests covering every invalid-config case you can think of. YAML via go.yaml.in/yaml/v4 — not gopkg.in/yaml.v3, which is archived.
4. Makefile: build, test, race, lint, fuzz (60s per target), bench, bench-compare (against test/bench/baseline.txt using benchstat), licenses (go-licenses → THIRD_PARTY_NOTICES), vuln (govulncheck), all.
5. .golangci.yml with: govet, staticcheck, errcheck, gosec, revive, gocritic, bodyclose, noctx, prealloc, unconvert, misspell. No disabled checks without a comment.
6. GitHub Actions: ci (make all on push/PR), codeql, semgrep, nightly fuzz (long run). Dependabot for gomod + actions.
7. LICENSE (Apache-2.0), NOTICE, CLAUDE.md containing §2.10 verbatim plus: "read docs/DESIGN.md before every task; every metric must be in docs/telemetry-catalog.md; every new dependency gets a line in docs/deps.md; every body buffer needs an ADR; nothing assumes hardware".
8. docs/: Diátaxis skeleton (tutorials, how-to, reference, explanation), adr/0000-template.md, adr/0001-auth-modes.md and adr/0002-late-failure-compensation.md written from §2.1–2.2, telemetry-catalog.md (empty table with columns: name, type, labels, unit, buckets, description, added-in-phase), deps.md, STATUS.md (phase checklist from §8), prompts/ (copy the prompts I give you, one file each).
9. test/bench/README.md describing the benchmark method from docs/DESIGN.md §8 (nothing runs yet).

Constraints: no proxy logic, no third-party dependency beyond the YAML library and the lint/tool binaries. Explain any exception before adding it.

Acceptance: `make all` is green on a clean checkout; `go run ./cmd/shunt version` and `check-config` on a sample config both work; `check-config` on each invalid sample fails with a message that names the offending key.

When done, run the red-team prompt: list the 10 most likely ways this system fails in production based on docs/DESIGN.md, and for each say whether the design already addresses it, and where. Put the result in docs/explanation/failure-modes.md.
```

### P1 — Core streaming proxy (passthrough auth)

```
Phase 1: the streaming data path with passthrough auth. Read docs/DESIGN.md §1, §2.1, §2.8, §2.9, and docs/STATUS.md.

Build:
1. internal/listener: TLS-terminating listener on crypto/tls. Config-driven certs with hot reload (GetCertificate over an atomically swapped pair, triggered by SIGHUP and by fsnotify on the cert paths), SNI map, min TLS 1.2, Go's default cipher ordering untouched, session ticket key rotation, optional client mTLS. SO_REUSEPORT via net.ListenConfig.Control so N processes can share a port.
2. internal/s3: RequestInfo{Bucket, Key, Op, Style, Query} extraction for virtual-host and path style; an Op enum covering every S3 operation you can enumerate from the AWS API reference (method × query keys × key presence × special headers), plus OpUnknown which is still proxied but metered separately. The classifier is a table, not an if-ladder, and has a test per row. S3 error XML rendering (Code, Message, Resource, RequestId).
3. internal/proxy: one handler. Forward to a single static cluster (round-robin over endpoints for now, real LB is Phase 3a) with Host preserved. Stream bodies both directions with io.CopyBuffer through a sync.Pool of fixed-size buffers (size in config, default 256 KiB); this is the only body-copy primitive in the codebase. Chain Expect: 100-continue end to end (send upstream headers, wait for upstream 100, then read the client body). Hop-by-hop header hygiene; add Via and x-shunt-request-id; pass x-amz-request-id back unchanged. Idle-progress timeouts for data ops, total deadline for metadata ops (§2.8). Graceful drain on SIGTERM: stop accepting, finish in-flight, bounded by a deadline.
4. internal/admin: separate listener with /-/healthz, /-/ready, /-/metrics, /-/drain, /debug/pprof.
5. internal/telemetry skeleton: request-id, slog JSON access log with a fixed field schema (document it in docs/reference/access-log.md), and these catalog entries only: shunt_requests_total, shunt_request_duration_seconds, shunt_upstream_ttfb_seconds, shunt_bytes_in_total, shunt_bytes_out_total, shunt_inflight. Add them to docs/telemetry-catalog.md first, then implement.
6. test/e2e: docker-compose with Garage (and versitygw posix as a second backend), a script that generates a self-signed wildcard cert, and a smoke test using aws-cli through the proxy: mb, put (small and 100 MiB multipart), get with range, head, ls, rm, rb.
7. test/s3diff v0: a Go program using aws-sdk-go-v2 that runs an op matrix (put/get/head/list/delete/multipart × sizes 0, 1 B, 4 KiB, 1 MiB, 64 MiB × path and virtual-host style × keys with spaces, unicode, '+', '%', '//', trailing '/', 1024-byte key) against the backend directly and via shunt, and diffs status, headers minus an allowlist, and body SHA-256. Report as a table. It must pass clean in passthrough mode.
8. test/bench: a Go client (or warp if simpler; it is AGPL and used only as a tool) that measures direct vs via-proxy for 4 KiB, 1 MiB, 64 MiB, 1 GiB objects at 1, 16, 256 connections, reporting throughput ratio, added p50/p99 latency, and proxy CPU-seconds per GiB. Record results in docs/bench/phase1.md and the Go benchmarks in test/bench/baseline.txt.

Constraints: net/http server and http.Transport; no fasthttp; no middleware framework; no third-party HTTP router. Any allocation on the per-request path above what net/http itself does must be justified in a comment. Do not attempt any kernel zero-copy path; docs/DESIGN.md §1.4 explains why none exists through net/http.

Acceptance: s3diff clean; smoke test green; an unauthorized 5 GiB PUT (bad credentials on the backend) fails in under 100 ms with zero body bytes sent by the client; killing the backend mid-GET results in the client seeing an error, never a silent short read (document exactly how for Content-Length responses); cert rotation under load drops zero requests; `make all` green.
```

### P2 — SigV4 verify-and-resign, aws-chunked, presigned, credential store

```
Phase 2: resign auth mode. Read docs/DESIGN.md §2.2, ADR-0001, ADR-0002. Use plan mode; show me the plan before coding.

Build:
1. internal/sigv4: canonical request per the AWS SigV4 spec with the S3-specific rules (single URI-encoding of the path, sorted query, header trimming/folding, SignedHeaders order, x-amz-content-sha256 handling). Verify header auth and presigned (X-Amz-Algorithm / X-Amz-Credential / X-Amz-Date / X-Amz-Expires / X-Amz-SignedHeaders / X-Amz-Signature) auth. Clock skew ±15 min. Reject SigV4A (AWS4-ECDSA-P256-SHA256) with a clear NotImplemented error. Sign upstream requests. Implement it yourself — do not copy from MinIO (AGPL) and do not import the AWS SDK signer into the runtime path; the SDK is allowed in tests only.
2. aws-chunked: decoder for STREAMING-AWS4-HMAC-SHA256-PAYLOAD and -PAYLOAD-TRAILER that verifies each chunk signature against the seed with the client's secret while streaming, and STREAMING-UNSIGNED-PAYLOAD-TRAILER passthrough. Forwarding rules exactly as the table in §2.2. Constant memory regardless of body size.
3. internal/auth: CredentialStore interface {Lookup(ctx, accessKey) (Credential, error)} and a static YAML implementation with hot reload; Credential carries secret, tenant label, optional bucket allowlist. Secrets never appear in logs, traces, or error messages — add a test that greps the access log and trace export for every secret used in tests.
4. Compensation path from ADR-0002 for the cases where the backend does not enforce sha256/trailer checks.
5. `shunt probe <endpoint>`: a subcommand that reports, for a backend: enforces x-amz-content-sha256 mismatch (yes/no), accepts STREAMING-UNSIGNED-PAYLOAD-TRAILER (yes/no), which checksum algorithms are honored (CRC32, CRC32C, SHA1, SHA256, CRC64NVME), supports If-None-Match: * on PUT (yes/no), what an unsigned GET / returns, virtual-host style support, GetObjectAttributes and part-number behaviors. Port the existing shell probes for these where they exist in my other repos rather than reinventing them; check their licenses and note any attribution in docs/deps.md.
6. Tests: round-trip tests where aws-sdk-go-v2 signs and shunt verifies (and vice versa) for the tricky-key matrix from s3diff — this is the primary oracle; add the AWS SigV4 test-suite vectors if they are still published, otherwise skip them; presigned URL tests including expiry; skew tests; fuzz targets for canonicalization and the chunk decoder.
7. s3diff gains a signing-mode dimension: unsigned, sha256, streaming-signed, streaming-signed-with-trailer (each checksum algorithm), presigned. Must pass clean in resign mode against Garage and versitygw; record any backend-specific gaps in docs/reference/backend-compat.md.
8. Catalog entries: shunt_auth_failures_total{reason}, shunt_auth_duration_seconds, shunt_compensation_total{reason,outcome}.

Constraints: no body buffering; verification overhead is measured (bench-compare vs Phase 1 baseline) and reported in docs/bench/phase2.md.

Acceptance: s3diff clean in both auth modes; bench shows the resign overhead as an explicit number; the secret-leak test passes; fuzz targets run 60 s clean; ADR-0002 updated with what each CI backend actually enforces.
```

### P3a — Single-cluster load balancing and resilience (first milestone)

```
Phase 3a: load balancing and resilience on one cluster. Read docs/DESIGN.md §2.3, §2.8, §10, and docs/CONTEXT.md. This is the core of the first milestone; multi-cluster state is Phase 3b and must not leak in here.

Build:
1. internal/config + internal/directory (minimal): clusters with endpoints, a REQUIRED per-cluster `scheme: https|http` (check-config fails if omitted — plaintext upstream is a per-site decision and never a default), TLS CA, credential refs; `defaults.cluster`; an optional static bucket → cluster map with no states. Define the Directory interface {Lookup(bucket) Placement; Watch()} now so 3b adds states without touching callers. File backend with atomic hot reload from either a file (bare metal: SIGHUP) or a mounted directory (k8s ConfigMap: watch the directory and handle the ..data symlink swap); invalid content is rejected and the last good state kept, with a log line and a metric.
2. internal/upstream: per-endpoint http.Transport with explicit MaxConnsPerHost, MaxIdleConnsPerHost, idle timeout, dial timeout, TCP keepalive, TLS config from the cluster record (CA verify for https; http logged loudly at startup). Power-of-two-choices on in-flight count with EWMA latency tie-break. Active health check (unsigned GET / every N s; any HTTP response is alive). Passive outlier ejection with exponential backoff and a floor so at least ceil(N/2) endpoints of a cluster stay eligible. Retry policy table by Op exactly as §2.8 — PUTs and parts are never retried. Timeouts by op class.
3. Optional PROXY protocol v2 on the listener (flag, default off) for L4 tiers that cannot preserve the client IP; parse it yourself (it is a small fixed header) and record the decision in deps.md.
4. Admin: /-/routes (secrets redacted), /-/upstreams (per-endpoint health, in-flight, EWMA, ejection state, scheme).
5. Catalog: shunt_upstream_cx_active, shunt_upstream_cx_connect_seconds, shunt_upstream_cx_errors_total{reason}, shunt_upstream_ejected{endpoint}, shunt_retries_total{op,reason}, shunt_directory_reload_total{result}.
6. Tests: LB distribution under uniform and skewed load; ejection and recovery timing; retry only for idempotent ops (a PUT that hits a connect error must fail, not retry); mid-stream backend death; reload from a file and from a simulated ConfigMap symlink swap, including an invalid payload; check-config rejects a cluster with no scheme; PROXY protocol on/off.

Acceptance: s3diff clean with a 3-endpoint cluster in both schemes; a chaos test that kills one endpoint during a 256-connection mixed workload shows no client errors after the ejection window; bench-compare within 5 % of the previous phase's baseline.
```

### P3b — Tenant-scoped directory, mixed backends, stateless IDs

```
Phase 3b: the global namespace over mixed backends, still on the file directory backend. Read docs/DESIGN.md §0 (decisions 13–17), §1.3, §2.3, §2.4, §2.8, and docs/CONTEXT.md. Prerequisite: P2 — the tenant comes from the verified access key.

Build:
1. Directory model: placements keyed by (tenant, bucket) with per-cluster backend bucket names; tenants with a default cluster; clusters with type (vast | minio | aws | generic), scheme, region, endpoint_mode (static | dns), credential ref, storage_classes (native | emulated), and a capability profile (sha256 enforcement, unsigned trailers, conditional writes, checksum algorithms). `shunt probe` can emit a capability profile in the exact config shape so profiles are measured, not typed.
2. Placement states ACTIVE / RAMPING / MIGRATING / CUTOVER with validated transitions (RAMPING skippable); refuse RAMPING/MIGRATING when the source bucket has versioning enabled (GetBucketVersioning at transition time). `shunt directory get|set-state|validate`. Brownfield commands from §11: `shunt adopt <cluster> <bucket> --tenant <t>` (verify the bucket exists, import the tenant's existing keys into the credential store if a key file is given, write an ACTIVE placement with the backend name equal to the client name) and `shunt expand <tenant>/<bucket> --to <cluster>` (probe the target against the source's capability profile, refuse if versioning is on, create the backend bucket using the name policy `<bucket>-<NNN>`, copy CORS/lifecycle/policy where the target supports them, run a canary write/read/delete, record the target on the placement, leave the state ACTIVE).
3. Request path: resolve (tenant, bucket) → placement; rewrite the bucket to the backend name for the chosen cluster in the path, the Host header for virtual-host style, and every XML response that echoes a bucket name (ListBuckets, ListObjects/V2 Name, CopyObject sources, ListMultipartUploads); sign with the cluster's region. Synthesize ListBuckets from the directory. CreateBucket → tenant default cluster with a generated backend name; DeleteBucket → backend delete then row removal.
4. Endpoint modes in internal/upstream: static (3a behavior) and dns (single logical endpoint, TTL re-resolve, per-address health); AWS 301 x-amz-bucket-region handling without loops.
5. uploadId codec (§2.4): rewrite InitiateMultipartUpload, ListMultipartUploads, ListParts; strip on every uploadId= request; streaming XML rewrite; tolerance rule for unprefixed IDs.
6. Catalog: shunt_route_lookups_total{result}, shunt_route_state{bucket,state}, shunt_requests_total gains a cluster_type label.
7. Tests: s3diff across three clusters of different types in one run (Garage, versitygw, MinIO; AWS when credentials are present) with the same logical bucket name owned by two tenants; backend-name rewriting in every response that carries a bucket name; ListBuckets spans clusters; uploadId round-trip across a directory reload; unprefixed uploadId tolerance; refusal on a versioned bucket; every illegal state jump rejected; AWS region redirect handled once.

Acceptance: mixed-backend s3diff clean; two tenants with the same bucket name never see each other's data (a test proves it); a multipart upload started before the codec was deployed completes after; bench-compare within 5 % of the previous phase's baseline.
```

### P3c — Control service on Postgres

```
Phase 3c: the product directory. Read docs/DESIGN.md §1.5 and §2.3. The file backend stays for dev; this phase adds the second implementation of the Directory interface and the service behind it. Use plan mode.

Build:
1. cmd/shunt-control: separate binary. Postgres via pgx (MIT); schema as SQL files embedded in the binary with a tiny versioned migration runner (no ORM, no third-party migration tool unless you can justify it in deps.md). Tables exactly as §1.5; a CI test fails if any table has an object-key column that is not scoped to an explicit job (restore_jobs is the only allowed one).
2. API (authenticated, mTLS or bearer): GET /v1/directory?since=<version> returning a full or incremental snapshot; tenant, credential, cluster, placement, and migration CRUD; every mutation writes directory_changes with actor, before, after, timestamp; migration jobs with a resumable cursor and counters that the mover updates.
3. Proxy side: a `control` Directory backend that polls the snapshot endpoint on an interval, swaps immutable snapshots atomically, and exposes shunt_directory_snapshot_age_seconds and shunt_directory_version. Serve last-known-good on any control-plane failure; return 503 ServiceUnavailable with a clear body for operations that need a write. A per-bucket LRU mode with negative caching for stores past the full-snapshot threshold (config value, documented).
4. CLI: `shunt directory`, `shunt tenant`, `shunt cluster`, `shunt migration` subcommands talk to shunt-control; the file backend commands keep working for dev.
5. Credentials: secrets encrypted at rest with a key from env/KMS ref; the credential store implementation for resign mode reads from the snapshot, never from Postgres directly.
6. Tests: snapshot/version semantics; control-plane outage under load (proxies keep serving, writes 503, no goroutine or connection leak); 100k placements snapshot load time and memory; LRU mode correctness; migration cursor resumability after a mover crash; audit rows for every mutation.
7. docs/how-to/run-shunt-control.md (Postgres HA is the deployer's choice — name Patroni/CloudNativePG/RDS as examples, require none), docs/reference/control-api.md, docs/explanation/why-no-objects-table.md.

Acceptance: the same s3diff run passes with the file backend and with the control backend; a Postgres outage during a 256-connection workload produces zero data-path errors; 100k placements load under a documented bound; schema test green.
```

### P4 — Telemetry everywhere (including eBPF sock_ops)

```
Phase 4: telemetry as a product. Read docs/DESIGN.md §2.7 and the current docs/telemetry-catalog.md.

Do this in order:
1. Finish the catalog first. Every signal in §2.7, with type, labels, unit, histogram buckets (seconds and bytes buckets chosen for object storage: latency 1 ms → 60 s, bytes 1 KiB → 16 GiB), and description. Ask me to review the catalog before implementing.
2. Add a CI test that renders /-/metrics and fails on any metric family not in the catalog, and a cardinality-guard test that drives 500 buckets and 200 tenants through the proxy and asserts the series count stays under a documented bound.
3. Implement the catalog with prometheus/client_golang. Pre-create label combinations where possible so the hot path is a counter add, not a map lookup with string building.
4. Traces with OpenTelemetry: one span per request, children as listed in §2.7, attributes for op, cluster, endpoint, bytes, status, both request IDs, tenant; bucket only per the cardinality rule. Head sampling rate in config; OTLP exporter. Propagate traceparent upstream. In-memory exporter in tests asserting the span tree for a GET, a multipart PUT, and a failed auth.
5. Slow ring: a fixed-size ring of the last N requests over a configurable threshold with the full timing breakdown (tls, parse, auth, route, connect, ttfb, body, total) and both request IDs; /-/slow renders it. Model it on s3slower.
6. Metering: per-tenant monotonic counters (§2.7 rule 4) and an hourly usage-record writer (JSON lines to a path or an S3 bucket) with a test that proves bytes are never double-counted across retries or compensation.
7. internal/ebpf: a sock_ops program built with cilium/ebpf + bpf2go (CO-RE, needs BTF) attached to the proxy's cgroup, recording srtt and retransmit events per 4-tuple into a map; userspace joins 4-tuple → endpoint label and exports shunt_tcp_srtt_seconds{endpoint} and shunt_tcp_retransmits_total{endpoint}. Feature flag, default off. If CAP_BPF/BTF are missing, degrade to the fallback: sample getsockopt(TCP_INFO) on pooled upstream connections every N seconds (reuse the tcpx approach). Both paths feed the same metrics. Nothing else in the proxy depends on eBPF. In containers the TCP_INFO fallback is the default; document the k8s securityContext (capabilities BPF and NET_ADMIN, host BTF, /sys/fs/cgroup mounted) that enables the eBPF path, and make doctor report which path is active.
8. docs/how-to/dashboards.md with Grafana panels and PromQL for: per-cluster RED, per-endpoint health, migration fallback rate, tenant metering, TCP health vs request latency, CPU-seconds per GiB. docs/explanation/telemetry.md explaining the cardinality rules.

Constraints: the per-request telemetry cost is measured; report it in docs/bench/phase4.md. No log line on the hot path above debug level except the access log.

Acceptance: catalog test and cardinality test green; span-tree tests green; eBPF path verified on a kernel with BTF and the fallback verified on one without CAP_BPF; bench-compare within 5 % of Phase 3a; a dashboard screenshot or JSON checked into docs/.
```

### P5 — Migration semantics

```
Phase 5: RAMPING and MIGRATING for unversioned buckets across mixed backends. Read docs/DESIGN.md §2.5 and write docs/adr/0004-migration-races.md as the first deliverable — it must already list the delete/copy race, the ETag-drift issue for multipart objects (worse across backend types), the versioning exclusion, and why ramp rules are monotonic. Use plan mode.

Build:
1. internal/migrate: state-dependent routing per the table in §2.5 for every Op (map each Op to an op class; unmapped ops fail the build via a completeness test). RAMPING: a key is in the target range if hash64(key, fixed seed) < ratio or it matches a prefix rule; writes and reads follow the table (keys outside the range never touch the target). Ramp updates are rejected unless the ratio and prefix set are supersets of the current ones; a `--force-shrink` path exists only after a documented target→source reconcile. Read fallback primary → source on 404 with shunt_migration_fallback_reads_total. DELETE to both, with the source delete failure logged and metered but not surfaced to the client if the primary delete succeeded (document why).
2. Listing merge for ListObjects, ListObjectsV2, and ListMultipartUploads: sorted merge of two paginated streams, primary wins on collision, composite continuation token (base64 of {p, s, last}), correct handling of delimiter/CommonPrefixes, MaxKeys, StartAfter, and IsTruncated. Streaming XML output; bounded memory independent of bucket size. ListObjectVersions is not merged (versioned buckets cannot be MIGRATING); it routes to primary.
3. State transitions through the Directory only; `shunt ramp <tenant>/<bucket> --ratio 0.05 --require-healthy` (enters RAMPING from ACTIVE using the target recorded by `expand`; refuses unless the target's active health check has been green for a configurable window and the expand canary passed), `... --ratio 0.25`, `... --prefix runs/2026-09/`, `shunt migrate start` (requires ratio 1 or an all-prefix rule the operator confirms), `shunt cutover`, `shunt directory set-state ... ACTIVE`, each validated. Hold policy: a background comparison of target vs source on the same op mix (error rate, p99, TTFB) over a window; when the target breaches configured thresholds the ramp is held (no further increase accepted without `--override`), a metric and an alert fire; the ratio is never lowered automatically because lowering requires a reconcile (§2.5).
4. test/mover: a deliberately simple mover that implements the mover contract in §2.5 (If-None-Match: *, re-HEAD after copy, multipart part-layout preservation for objects with multipart ETags, resumable cursor in the migration job, JSONL ledger to an S3 bucket, convergence report) against Garage→MinIO and MinIO→Garage. It is a test fixture, not the product. It must refuse to start while the placement's ratio is below 1.
5. Property test: N goroutines run random client ops (put, overwrite, delete, multipart, list, conditional GET with If-Match) through the proxy while an operator goroutine ramps 0.01 → 0.25 → 1.0 (hash and prefix variants), then the mover runs, then the operator cuts over; after CUTOVER assert: every key written and not deleted is readable with the last-written content and its pre-migration ETag; every deleted key is absent; listing equals the model; no multipart upload started before a transition fails to complete. Run under -race. Any invariant that cannot be guaranteed goes into ADR-0004 with its window.
6. docs/how-to/migrate-a-bucket.md and docs/explanation/migration-consistency.md, including what a mover that does not preserve part layout breaks.
7. Catalog: shunt_migration_fallback_reads_total, shunt_migration_dual_delete_total{outcome}, shunt_listing_merge_seconds, shunt_route_state{bucket,state}, shunt_ramp_ratio{bucket}, shunt_ramp_writes_total{bucket,side}.

Acceptance: property test green over 10 minutes; s3diff clean in ACTIVE state (no regression); listing merge benchmark for two 1M-key buckets shows bounded memory; ADR-0004 lists every known race with its mitigation and window.
```

### P6 — Tiering and Glacier storage-class semantics

```
Phase 6: tiering and restore. Read docs/DESIGN.md §2.6. Use plan mode.

Build:
0. Native mode: for placements with tier: native, pass x-amz-storage-class, ?restore, x-amz-restore, and ?lifecycle straight through to the backend and meter them; a test proves shunt adds nothing and strips nothing on that path. Everything below is emulated mode.
1. Lifecycle: GET/PUT/DELETE ?lifecycle stored in the placement record; parse the AWS lifecycle XML subset (Rule, Filter with Prefix/Tag/And, Status, Transition{Days|Date,StorageClass}, Expiration{Days|Date}); reject anything outside the subset with the same error AWS would return.
2. Routing: PUT with x-amz-storage-class equal to a configured cold cluster's class → cold; GET/HEAD hot → cold on 404 with shunt_tier_fallback_reads_total; cold hit under access: restore-required → 403 InvalidObjectState with AWS's XML body; StorageClass reported per source cluster in HEAD, GET, and listings (reuse the Phase 5 merge for listing tiered buckets). check-config enforces the pairing rule: restore-required only with GLACIER or DEEP_ARCHIVE; GLACIER_IR is always instant.
3. RestoreObject: POST ?restore with Days (required) and Tier (accepted, ignored); 202 on a new job, 200 if already restored (Days extends the expiry), 409 RestoreAlreadyInProgress while a job runs; job persisted through the Directory backend; `shunt restore worker` takes a lease with a conditional write, copies cold → hot with x-amz-meta-shunt-restore-expiry, verifies ETag; HEAD/GET synthesize x-amz-restore exactly as AWS does; a sweeper deletes expired hot copies.
4. `shunt tier run`: demotion job — list hot, evaluate rules, conditional copy to cold, verify ETag, delete hot; idempotent; resumable; rate-limited; dry-run mode that prints what it would do.
5. Optional read-through promotion flag (default off): on a cold hit in instant mode, enqueue an async copy to hot.
6. Tests: golden tests for every header and XML body against the AWS documentation for RestoreObject (200/202/409), x-amz-restore, InvalidObjectState; job state machine tests; sweeper tests with a fake clock; a two-cluster e2e that writes, demotes, reads (403), restores, reads (200), expires, reads (403).
7. docs/how-to/tier-a-bucket.md, docs/reference/glacier-compat.md listing exactly which S3 Glacier behaviors are emulated and which are not.
8. Catalog: shunt_tier_fallback_reads_total, shunt_restore_jobs{state}, shunt_restore_duration_seconds, shunt_tier_run_objects_total{action,outcome}.

Acceptance: e2e scenario green on Garage×2; aws-cli `s3api restore-object` / `head-object` behave as they do against AWS for the emulated subset; bench-compare within 5 % for buckets with no tier policy (the common case must not pay).
```

### P7 — Performance in userspace

```
Phase 7: data-path performance without any hardware or kernel-offload assumptions. Read docs/DESIGN.md §1.4 and every docs/bench/*.md. Numbers before code.

Do this in order:
1. Profile: CPU and alloc profiles under the bench harness at 4 KiB, 1 MiB, and 1 GiB with 64 connections, in resign mode, TLS on both sides. Write docs/bench/phase7-profile.md with the top 10 CPU and top 10 alloc sites and what each costs per GiB (or per request at 4 KiB). Fix any allocation regression against the per-request rule in CLAUDE.md before anything else.
2. Copy-buffer sweep: measure 64 KiB, 256 KiB, 1 MiB, 4 MiB pooled buffers at each object size; pick the default from the data and document why; keep it configurable.
3. Small-object path: at 4 KiB the cost is header parsing, signing, and allocations, not copying. Reduce per-request allocations to a documented floor; measure requests per core-second before and after.
4. TLS cost: report userspace crypto cost per GiB at each object size with upstream TLS on and with upstream plaintext-on-fabric, so the trusted-fabric option is a measured trade-off, not an assumption. Do not change cipher ordering.
5. Runtime sweep: GOGC, GOMEMLIMIT, GOMAXPROCS, and the number of SO_REUSEPORT processes per host, at the 1 MiB and 1 GiB sizes. Document the recommended settings in docs/how-to/tuning.md along with the CPU-seconds per GiB figure operators need for capacity planning.
6. Connection pool sizing: MaxConnsPerHost and idle settings against a 3-endpoint cluster under 256 client connections; look for head-of-line effects in the p99.

Constraints: every optimization is a flag or a config value, default to the measured best, until its bench file shows the gain and s3diff shows no regression. No optimization changes any header or body observed by s3diff. No build tags for platform features; nothing that a plain Linux host with a generic NIC cannot run.

Acceptance: docs/bench/phase7.md with the before/after table; s3diff clean with every optimization on; fuzz and race green; at least one production-shaped number: proxy CPU-seconds per GiB at 1 GiB objects for direct, proxy with TLS both sides, and proxy with plaintext upstream.
```

### P8a — Packaging for bare metal and RKE2 (first milestone)

```
Phase 8a: make the one-cluster build deployable on both targets from the same artifact. Read docs/DESIGN.md §10 and docs/CONTEXT.md.

Build:
1. Static binary (CGO_ENABLED=0; cilium/ebpf needs no cgo — confirm), multi-arch amd64/arm64, distroless container image, Makefile target that pushes to a registry given as a variable (the internal Harbor in CONTEXT.md).
2. Kubernetes: kustomize base + overlays (say why over Helm, or the reverse). Deployment with replicas and resource requests/limits (Go 1.25+ sizes GOMAXPROCS from the cgroup CPU limit — verify the pinned toolchain and note it), readiness /-/ready, liveness /-/healthz, preStop hook calling /-/drain with terminationGracePeriodSeconds matched to the drain deadline; Service type LoadBalancer with externalTrafficPolicy: Local to preserve client IP; cert-manager Certificate for the wildcard domain mounted as a Secret (hot reload via the directory watcher from 3a); ConfigMap for config; ServiceMonitor; PodDisruptionBudget; default securityContext non-privileged with a separate overlay that adds the eBPF capabilities.
3. systemd: template unit shunt@.service so N instances share the port via SO_REUSEPORT; hardening (ProtectSystem=strict, PrivateTmp, NoNewPrivileges, CapabilityBoundingSet=CAP_NET_BIND_SERVICE, with a drop-in adding CAP_BPF CAP_NET_ADMIN when eBPF is on); ExecReload sends SIGHUP; JSON logs to journald/stdout.
4. `shunt doctor` first cut: cert chain and expiry, config validation, backend probes (reuse probe), clock skew per backend, BTF/cgroup v2/CAP_BPF report with which TCP-stats path is active.
5. docs/tutorials/one-cluster-on-systemd.md and docs/tutorials/one-cluster-on-rke2.md, each under 30 minutes; runbooks: endpoint ejected, cert expiry, CPU-per-GiB rising, config reload rejected.

Acceptance: the same image and config run on the RKE2 cluster and under systemd; a rolling restart on k8s and `systemctl reload` on bare metal both show zero client errors under a 256-connection workload; a cert-manager renewal is picked up without a restart; a context-free reader completes each tutorial.
```

### P8b — Hardening and operations

```
Phase 8b: make it operable by someone who did not build it. Read docs/DESIGN.md and docs/STATUS.md.

Build:
1. Limits and abuse: max header size, max connections, per-connection request timeout for headers, slowloris protection, request-smuggling tests (CL+TE ambiguity must be rejected), body size limits per op class, optional per-tenant QoS (token buckets for req/s and bytes/s, flag default off, catalog entries).
2. Chaos suite in test/e2e/chaos: backend killed mid-PUT and mid-GET, backend that accepts connections and never responds, backend that responds at 1 KiB/s, cert rotation under load, config and directory reload under load, GOMEMLIMIT pressure. Each scenario states the expected client-visible behavior and asserts it.
3. Fuzz: every parser has a target; nightly workflow runs each for 30 minutes and uploads crashers.
4. `shunt doctor` completed: everything from 8a plus per-cluster scheme and CA checks, listener cert/SNI coverage of the configured domains, and a summary exit code for automation. No NIC or CPU checks.
5. Release: goreleaser config producing the 8a artifacts, SBOM, THIRD_PARTY_NOTICES regenerated on release, image signing if the registry supports it.
6. Docs: reference for config generated from struct tags (a test fails if the reference is stale); runbooks added to the 8a set for: migration stuck, restore backlog; a tutorial that goes from zero to a migrated bucket on two Garage instances in under 30 minutes.
7. Release checklist in docs/how-to/release.md.

Acceptance: chaos suite green; `shunt doctor` output checked into docs as an example; a fresh reader completes the tutorial without asking questions (test it by giving the tutorial to a sub-agent with no other context and having it report where it got stuck).
```

### Guardrail prompts (run all four at the end of every phase, in order)

**G1 — Simplicity review**

```
Simplicity review for the phase just completed.
1. List every exported type, interface, and function added this phase. For each, name its second caller. Delete or unexport anything with only one, except the two named seams in CLAUDE.md.
2. List every dependency added this phase with its docs/deps.md line and its license. Remove any that the standard library could replace in under 100 lines.
3. Search for any place a request or response body is buffered (bytes.Buffer, io.ReadAll, []byte body copies). Each one needs an ADR or must go. io.CopyBuffer through the pool is not buffering.
4. Find every feature flag; confirm each is default off and has a removal criterion.
5. Search for anything that assumes a NIC, CPU model, or kernel feature without a doctor check and a fallback. Remove it.
6. Report LOC added, LOC deleted, and test-to-code ratio. If the ratio dropped, explain why.
Make the deletions, then show me the diff summary.
```

**G2 — Validation**

```
Validation for the phase just completed. Run, in order, and paste the real output — do not summarize, do not skip, do not mark anything as flaky:
make lint; go vet ./...; go test -race ./...; make fuzz (60 s per target); test/s3diff against Garage and versitygw in both auth modes; make bench-compare against the previous phase's baseline; the e2e scenarios for this phase.
For each failure: fix the code, not the test, unless the test is wrong, in which case show me why before changing it. A benchmark regression over 5 % blocks the phase.
```

**G3 — Telemetry and docs check**

```
Telemetry and documentation check.
1. Every new code path added this phase: list the metric, span, and access-log field that observes it. Anything unobserved gets instrumented from the catalog; anything needing a new metric gets a catalog entry first, then code.
2. Confirm the catalog test and cardinality test still pass.
3. Update docs/reference for every config key and subcommand added; update docs/STATUS.md; write or update the ADR for every decision made this phase that is not already in docs/DESIGN.md.
4. If any decision this phase contradicted docs/DESIGN.md, say so explicitly and propose the edit to DESIGN.md — do not silently diverge.
```

**G4 — Dependency and license audit**

```
Dependency and license audit.
Run make licenses and make vuln. Regenerate THIRD_PARTY_NOTICES. For each dependency: license, whether it is MIT/BSD (preserve notice), Apache-2.0 (preserve LICENSE and NOTICE, mark modified files if forked), or copyleft (stop and tell me). Confirm no code was copied from an AGPL project (MinIO, Garage). Confirm every copied snippet from any repo, including my own, has its origin and license in docs/deps.md. Confirm the YAML dependency is go.yaml.in/yaml and not the archived gopkg.in/yaml.v3, including transitively. Report the list and any action needed.
```

### Phase-close prompt

```
Close the phase: update docs/STATUS.md with what shipped, what was cut and why, the bench numbers, and the gate result. Write the PR description: summary, design decisions with ADR links, how to verify, known gaps. Tag the commit phase-<n>. Then tell me what you would change in docs/DESIGN.md based on what you learned building this phase.
```

---

## 8. Acceptance gates

| Phase | Gate |
|---|---|
| 0 | `make all` green on clean checkout; `check-config` rejects every invalid sample with a named key; failure-modes doc written |
| 1 | s3diff clean (passthrough); 5 GiB unauthorized PUT fails < 100 ms with 0 body bytes; mid-GET backend death never yields a silent short read; cert rotation drops 0 requests; baseline bench recorded |
| 2 | s3diff clean in both modes across all signing modes; resign overhead is a published number; secret-leak test green; fuzz clean |
| 3a | s3diff clean on a 3-endpoint cluster in both schemes; chaos kill shows 0 client errors after ejection window; PUT never retried; reload from file and ConfigMap swap; bench within 5 % |
| 3b | mixed-backend s3diff clean; same bucket name under two tenants isolated; ListBuckets spans clusters; unprefixed uploadId tolerance; versioned-bucket transition refused; bench within 5 % |
| 3c | s3diff identical on file and control backends; Postgres outage causes 0 data-path errors; 100k placements load within bound; no-objects-table schema test green |
| 4 | catalog + cardinality tests green; span-tree tests green; eBPF path and TCP_INFO fallback both verified; dashboard checked in; bench within 5 % |
| 5 | 10-minute property test green through a hash ramp and a prefix ramp, including ETag preservation; listing merge memory bounded on 1M×2 keys; mover refuses to start below ratio 1; ADR-0004 complete |
| 6 | two-cluster tier/restore/expire e2e green; aws-cli restore-object/head-object match AWS for the emulated subset; untiered buckets pay no cost |
| 7 | before/after bench table; s3diff clean with every optimization on; CPU-seconds per GiB published for direct vs TLS-both-sides vs plaintext-upstream |
| 8a | same artifact runs on RKE2 and systemd; rolling restart and `systemctl reload` drop 0 requests; cert-manager renewal picked up live; both tutorials pass a context-free reader |
| 8b | chaos suite green; full doctor output in docs; migration tutorial passes a context-free reader |
| **M1** | **First milestone = gates 0, 1, 3a, 4, 8a all green on the real cluster named in CONTEXT.md** |

**Benchmark method (used by every `docs/bench/*.md`).** Object sizes 4 KiB, 1 MiB, 64 MiB, 1 GiB; connections 1, 16, 256; workload GET-only, PUT-only, 70/30 mixed; three runs, report median; direct-to-backend numbers on the same run as the baseline; metrics: throughput ratio (proxy/direct), added p50 and p99 latency, proxy CPU-seconds per GiB, RSS, allocs per request. Disclose the hardware, kernel, NIC, Go version, and backend build every time — as disclosure, never as a requirement. A number without its configuration is not a result.

## 9. Open questions and risks

1. **CPU scales with bytes.** With userspace TLS and no kernel zero-copy path, a proxy node's ceiling is set by CPU-seconds per GiB, which Phase 7 publishes. The answer is horizontal scale behind an L4 tier; the risk is planning without the number. Plaintext-on-fabric upstream is the one lever that halves crypto cost, and it is a topology decision, not a code one.
2. **Backend enforcement gaps.** Whether a backend enforces `x-amz-content-sha256`, accepts unsigned trailers, and supports `If-None-Match: *` determines how often compensation runs and whether the mover contract holds. `shunt probe` answers this per backend; `docs/reference/backend-compat.md` is a living document.
3. **ETag drift.** A mover that does not reproduce multipart part layout changes ETags; the proxy cannot hide it. This is a hard mover requirement and belongs in every migration runbook.
4. **Versioned buckets** cannot be migrated in v1. Whether v2 does this via backend-native replication or accepts new version IDs is a decision for later, with data on how many buckets it affects.
5. **Listing merge cost** during migration on very large buckets: two backend list streams per client page. Bounded memory is guaranteed by design; latency is not. Measure in Phase 5.
6. **`restore-required` semantics vs clients.** Some clients treat 403 as fatal rather than "call RestoreObject". Instant-access cold is the default for a reason; restore-required is opt-in per cluster and restricted to the archive classes.
7. **STS / session tokens** in resign mode need a validating credential store. The seam exists; the implementation is v2.
8. **DSR** happens in front of shunt, not inside it. If an L4 tier is needed it is a separate small project; shunt's only obligation is not to depend on source NAT.
9. **AWS egress and request costs.** With AWS as a backend, every byte a client reads flows AWS → shunt → client and is billed as egress; fallback HEADs during migration and cold-tier lookups are billed requests. Per-tenant metering by cluster type (§2.7) attributes it; placing a proxy tier inside AWS for AWS-backed buckets is a topology decision the metering data should drive.
10. **Postgres as a dependency.** Shunt's data path never touches it, but the control plane does; a long outage freezes bucket creation and migrations. The snapshot-age gauge and the 503 contract make that visible and bounded; HA is the deployer's job and the runbook says which options are known to work.
11. **Backend-name rewriting surface.** Every response that echoes a bucket name must be rewritten; missing one leaks a backend name to a client. s3diff's header and body diff is the catch; the op classifier's table is where the list lives.
12. **Capability drift.** A backend upgrade can change what it enforces. Re-run `shunt probe` on a schedule and alert when a profile changes.
14. **Copying between clusters is refused.** In resign mode `x-amz-copy-source` is resolved through the directory; if the source bucket lives on another cluster the backend cannot read it, so shunt answers `501 NotImplemented` (POC-3). A client copying between two of its own buckets therefore gets a hard failure whenever their placements differ. Whether shunt streams such a copy itself, or refuses it as a documented limit, is a later phase's decision.

13. **Garage rejects signed-trailer uploads.** Garage 2.3.0 answers `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` with "Invalid payload signature" for every checksum algorithm. Through shunt in resign mode these uploads succeed, because shunt verifies the chunk and trailer signatures itself and forwards an unsigned trailer Garage accepts. The same client pointed at Garage directly, or at shunt in passthrough mode, fails; a migration off shunt or a switch to passthrough breaks those clients (docs/reference/backend-compat.md).

---

## 10. Sequencing and site context (from your answers)

**First milestone: LB + telemetry on one cluster.** One cluster needs no cross-credential routing, so passthrough auth is enough and P2 (resign) waits. Migration state, the uploadId codec, tenants, and Postgres are not on the path. The milestone is P0 → P1 → P3a → P4 → P8a, gated as **M1** in Section 8. P7 runs as soon as M1 carries real traffic, because a profile from the real workload is worth more than one from the bench harness. P2 → P3b → P3c → P5 → P6 → P8b follow.

**The global endpoint.** One client-facing endpoint over VAST, MinIO, AWS S3, or any mix, with shunt deciding where each tenant's bucket lives. What that forces: `resign` auth (customers hold shunt keys, backends keep theirs), a tenant-scoped namespace with backend-name mapping, `ListBuckets` from the directory, capability profiles per cluster, two tiering modes, and a real control plane. What it must not force: any per-object state. The directory answers "where is this bucket"; the lookup order answers "where is this object"; the mover's ledger answers "when did it move". Postgres holds the first, nothing holds the second, object storage holds the third.

**Both targets from one artifact.** Bare metal runs N `shunt@.service` instances sharing the port with SO_REUSEPORT; RKE2 runs N replicas and lets Go size GOMAXPROCS from the cgroup limit. What has to be the same: config and cert reload work from a file *and* from a mounted directory (ConfigMap/Secret symlink swaps), logs are JSON on stdout, health and drain endpoints are the only lifecycle interface, and the eBPF TCP path is a flag that defaults on for bare metal and off in containers with `doctor` saying which is active. Client IP reaches the proxy via `externalTrafficPolicy: Local` on k8s or via the optional PROXY protocol flag when an L4 tier in front cannot preserve it.

**Plaintext upstream depends on site.** So it is never a default: every cluster record states `scheme: https` or `scheme: http`, `check-config` rejects omission, startup logs it, `/-/upstreams` shows it, and P7 publishes CPU-seconds per GiB for both so the site decision has a number attached.

**What still has to come from you** is in `docs/CONTEXT.md` (template alongside this document): the lab endpoint and scratch bucket, the client list, your existing VAST compat findings, a workload shape, scale bounds, the wildcard domain and cert source, tenancy and metering fields, and conventions. The kickoff prompt reads it as ground truth.

---

## 11. Brownfield insertion: vast01 / data01 → vast02 / data01-001

The reference scenario: clients already use bucket `data01` on vast01 through `s3.dc01.cooking.com`. Shunt is inserted with no client change, then the bucket is expanded to vast02 and traffic is ramped.

**What "no client change" requires.** Four invariants, only one of which DNS provides:

1. *The endpoint name resolves to shunt.* The zone that owns `s3.dc01.cooking.com` changes its A records (or a CNAME to `shunt.dc02.cooking.com`; cross-DC is fine) to shunt's VIP(s). Lower the TTL to 60 s a day ahead so flip and rollback are fast. Delegation (NS for a sub-zone) is not needed for this; it is the right tool later for the global endpoint, where `s3.cooking.com` is handed to a health-aware DNS and the per-DC names become CNAMEs to it.
2. *TLS.* Shunt presents a cert for `s3.dc01.cooking.com` and `*.s3.dc01.cooking.com`, issued by the CA the clients already trust for VAST.
3. *Credentials.* `shunt adopt` imports the tenant's existing access keys; clients keep their keys and a DNS rollback still works because VAST accepts the same keys.
4. *Names and addressing.* `data01` stays `data01`; `data01-001` is the vast02 backend name in the placement's name map. Path-style and virtual-host style both keep working because shunt reaches vast01 by its own name, path-style, and re-signs.

**If clients use IP addresses** (hardcoded VIP lists, `/etc/hosts`), DNS is irrelevant: move VAST's VIP pool to a new range and announce the old range from shunt (keepalived or BGP). Same outcome, more coordination.

**Sequence.**

| Step | Command | State after | What it checks |
|---|---|---|---|
| 1 | DNS TTL lowered; cert issued; shunt configured with cluster `vast01` and listener domain `s3.dc01.cooking.com` | — | `shunt doctor`: cert covers the domain and wildcard; vast01 reachable; clock skew |
| 2 | `shunt adopt vast01 data01 --tenant acme --keys keys.yaml` | ACTIVE on vast01 | bucket exists; keys verified against vast01 with a signed HEAD |
| 3 | DNS flip → shunt VIP | ACTIVE, traffic flowing through shunt | s3diff run through the new name; dashboards show vast01 only |
| 4 | `shunt expand acme/data01 --to vast02` | ACTIVE, target recorded | vast02 probe ≥ vast01 profile; versioning off; `data01-001` created; canary write/read/delete |
| 5 | `shunt ramp acme/data01 --ratio 0.05 --require-healthy` | RAMPING 0.05 | vast02 health green for the window; keys in range write to vast02, read vast02-then-vast01 |
| 6 | `--ratio 0.25`, `--ratio 1.0` as the hold policy allows | RAMPING → ratio 1 | target vs source error rate, p99, TTFB on the same op mix; hold on breach |
| 7 | `shunt migrate start acme/data01` | MIGRATING | ratio is 1; mover (test/mover) copies vast01 → vast02 under the §2.5 contract |
| 8 | `shunt cutover acme/data01` | CUTOVER → ACTIVE on vast02 | listing diff empty; fallback reads zero for the comfort window |

Rollback at steps 2–3 is a DNS flip back. From step 5 on, rollback is a reconcile (target → source for the ramped key range), never just lowering the ratio.

**Cross-DC note.** With shunt in dc01 and vast02 in dc02, the ramped 5 % crosses the inter-DC link; the ramp comparison will show that latency honestly, and the hold thresholds should be set with it in mind rather than tuned to hide it.
