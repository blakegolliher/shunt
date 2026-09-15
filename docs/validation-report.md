# Shunt design — validation report

Scope: every assumption in the v1 design, checked against the S3 API reference, Go runtime behavior, kernel/eBPF facts, and dependency licenses. Per your direction, everything that pins hardware (kTLS, NIC TLS offload, sockmap) is removed and TLS terminates in userspace `crypto/tls`. The revised design is `docs/DESIGN.md`.

Status key: **OK** verified · **FIXED** corrected in v2 · **REMOVED** cut per your direction · **PROBE** cannot be settled from the spec, settled per backend by `shunt probe`.

## 1. Data path and TLS

| # | Assumption in v1 | Status | Finding / change |
|---|---|---|---|
| 1 | kTLS moves record crypto to the kernel; NIC offload on ConnectX-6 Dx/7 | REMOVED | Hardware pin. TLS is userspace `crypto/tls` on both sides. |
| 2 | With kTLS/plaintext, `io.Copy` uses `splice(2)` for zero-copy forwarding | **FIXED** — the claim was wrong even without kTLS | `net.TCPConn.ReadFrom` only splices when the source is a `*net.TCPConn`/`*net.UnixConn` (or `sendfile` for `*os.File`). Inside `net/http` the upstream response body and the client request body are `*http.body` wrappers, so the proxy never gets a kernel zero-copy path, plaintext or not. Removed from P1 and P7. Proxy CPU therefore scales with bytes; the levers are copy buffer size, allocation rate, GC, and cores. |
| 3 | sockmap/`sk_msg` could redirect bodies in-kernel | REMOVED | With userspace TLS on the client side, every byte must pass through the process; sockmap cannot apply. Cut entirely, not even as an experiment. |
| 4 | eBPF `sock_ops` gives per-connection RTT/retransmits without hot-path kprobes | OK | Kernel feature, not hardware: `BPF_SOCK_OPS_RTT_CB` / `RETRANS_CB` via a cgroup-attached program; needs CAP_BPF (+CAP_NET_ADMIN) and BTF for CO-RE. Kept, feature-flagged, with the `getsockopt(TCP_INFO)` fallback. |
| 5 | Go's cipher ordering should prefer AES-GCM (for kTLS eligibility) | FIXED | Rationale gone. Go's default suite ordering is already CPU-aware (AES-GCM with AES-NI, ChaCha20 otherwise); v2 says leave it alone. |
| 6 | `GetCertificate` callback enables cert hot-reload; `SetSessionTicketKeys` rotates ticket keys; `SO_REUSEPORT` via `net.ListenConfig.Control` | OK | Standard library, no hardware. |
| 7 | AWS S3 is HTTP/1.1 only, so no HTTP/2 | OK | Confirmed via AWS re:Post and the aws-sdk-java-v2 issue where forcing HTTP/2 to S3 fails. If AWS ever adds h2 it doesn't matter — clients negotiate via ALPN and shunt simply doesn't advertise it. |
| 8 | `net/http` server sends `100 Continue` when the handler first reads the body; `http.Transport` waits for `100` via `ExpectContinueTimeout` | OK | Chaining works by not touching the client body until the upstream responds. |
| 9 | Go native fuzzing, `slog`, `benchstat`, `govulncheck` available | OK | |

## 2. S3 API semantics

| # | Assumption | Status | Finding / change |
|---|---|---|---|
| 10 | SigV4 for S3 URI-encodes the path once (not twice like other services) | OK | S3 is the documented exception. |
| 11 | Clock skew tolerance ±15 min (`RequestTimeTooSkewed`) | OK | |
| 12 | Payload modes: `UNSIGNED-PAYLOAD`, hex SHA-256, `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, `…-PAYLOAD-TRAILER`, `STREAMING-UNSIGNED-PAYLOAD-TRAILER`; `x-amz-decoded-content-length` carries the decoded size | OK | v2 adds: reject `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD` (SigV4A) with a clear error; it is out of scope. |
| 13 | Chunk signatures chain from the seed (header) signature | OK | |
| 14 | Backend enforces a hex `x-amz-content-sha256` mismatch itself | PROBE | AWS does. Per-backend behavior is what `shunt probe` reports; compensation covers the gap. |
| 15 | Backend accepts `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | PROBE | Same. |
| 16 | Checksum algorithms: CRC32, CRC32C, SHA1, SHA256, CRC64NVME | OK | |
| 17 | Presigned URL params `X-Amz-Algorithm/Credential/Date/Expires/SignedHeaders/Signature`; payload is unsigned | OK | |
| 18 | `RestoreObject` is `POST ?restore`; 202 new job, 409 `RestoreAlreadyInProgress` | FIXED (incomplete) | Also **200** when the object is already restored (and `Days` extends expiry). Added. |
| 19 | `x-amz-restore: ongoing-request="true"` then `ongoing-request="false", expiry-date="…"` on HEAD/GET | OK | |
| 20 | Reading an un-restored archive object returns `403 InvalidObjectState` | OK | |
| 21 | Cold cluster default class `GLACIER_IR` with `access: instant` | FIXED (consistency) | On AWS, `GLACIER_IR` never needs `RestoreObject`; `GLACIER`/`DEEP_ARCHIVE` always do. v2 makes `check-config` enforce: `restore-required` only with `GLACIER`/`DEEP_ARCHIVE`; `instant` with `GLACIER_IR` (or any non-archive class). |
| 22 | Lifecycle subset: `Rule`, `Filter{Prefix,Tag,And}`, `Status`, `Transition{Days|Date,StorageClass}`, `Expiration{Days|Date}` | OK | |
| 23 | `UploadId` appears in InitiateMultipartUpload, ListMultipartUploads, ListParts responses and in `uploadId=` on every later request | OK | `~` is RFC 3986 unreserved, so the prefixed ID survives URL encoding; split on the first `~` only, so backend IDs may contain anything. |
| 24 | Unsigned `GET /` returns 403 on a live backend | FIXED (loosened) | AWS returns 403; other backends may return 400/403. v2: any HTTP response counts as alive. |
| 25 | `If-None-Match: *` conditional PUT is available for the mover | OK | AWS since 2024; VAST already used by vamoose. Per-backend in the compat matrix. |
| 26 | ListObjectVersions can be merged across two clusters during migration | **FIXED — removed** | Version IDs are backend-generated; a mover using the S3 API cannot reproduce them, so a versioned bucket's history cannot move. v2: `MIGRATING` is refused when versioning is enabled on the source; `ListObjectVersions` merge is out of v1. |
| 27 | Migration is transparent to clients after cutover | FIXED (caveat added) | Multipart-uploaded objects have part-layout-dependent ETags. Unless the mover reproduces the part layout, ETags change, and clients using `If-Match` with cached ETags get 412 after cutover. The proxy cannot mask this; it's a mover requirement and an ADR-0004 entry. |
| 28 | The AWS SigV4 test-suite vectors are available for tests | PROBE | Historically published in the SigV4 docs; may have moved. v2 makes SDK round-trip tests the primary oracle and the vectors optional. |

## 3. Licenses and dependencies

| # | Assumption | Status | Finding / change |
|---|---|---|---|
| 29 | `gopkg.in/yaml.v3` is the YAML library | **FIXED** | go-yaml/yaml was archived as unmaintained in April 2025. The YAML org fork is `go.yaml.in/yaml` (v3 frozen, security-only; v4 active). License MIT (libyaml-derived files) + Apache-2.0 (the rest) — preserve both notices. v2 uses `go.yaml.in/yaml/v4`. |
| 30 | `cilium/ebpf` MIT; `prometheus/client_golang`, OpenTelemetry-Go, `aws-sdk-go-v2` Apache-2.0; `golang.org/x/sys` BSD-3 | OK | |
| 31 | `waipu-oss/go-ktls` MIT; `northernside/ktls` license unverified | REMOVED | Not used. |
| 32 | versitygw Apache-2.0 | OK | Verified last session. |
| 33 | MinIO server / warp / mint AGPL-3.0 — tools only | OK | Garage is also AGPL-3.0 — added to the table as a tool. |
| 34 | ceph/s3-tests MIT | PROBE | Still marked verify-before-use in v2. |
| 35 | Project license Apache-2.0; no MinIO code copied | OK (decision) | G4 audits it every phase. |

## 4. Design logic

| # | Assumption | Status | Finding / change |
|---|---|---|---|
| 36 | Passthrough mode needs the backend to accept the proxy's hostname as a virtual-host domain and share credentials | OK | Stated as a conditional; it is exactly what makes passthrough a single-cluster mode. |
| 37 | Zero per-object state is sufficient for migration, tiering, restore | OK with caveat | Restore jobs and lifecycle configs are per-bucket-scoped records keyed by object; they are small and explicit, and they live in the directory backend, not in the proxy. Tiering's hot-then-cold fallback is the price of no index; it is metered so the v2 index decision is data-driven. |
| 38 | Compensating delete is an acceptable late-failure strategy | OK with caveat | On versioned buckets it leaves a delete marker plus a version. ADR-0002 keeps this explicit; probes keep it rare. |
| 39 | P2C + EWMA load balancing; ejection floor `ceil(N/2)` | OK (design choice) | |
| 40 | `PUT` and parts are never retried by the proxy | OK | Bodies are not replayable without buffering; the client's SDK retries are the right layer. |
| 41 | Prometheus client for metrics, OTel for traces | OK (decision) | Two SDKs, chosen for hot-path cost; ADR notes the alternative (OTel metrics with Prometheus exporter) if the team prefers one SDK. |
| 42 | Directory v2 in a control bucket with ETag CAS | OK | Same pattern as vamoose's claim protocol; needs conditional writes on the control cluster (compat matrix). |

## 5. What changed structurally in v2

- Section 1.4 rewritten: no kernel data path; the honest performance model is CPU-per-GiB × throughput, scaled horizontally behind an L4 tier.
- Phase 7 rewritten as a userspace performance phase: profile, allocation, copy-buffer sweep, GC and process-count tuning, plaintext-upstream as a measured option. Same gating rules.
- `shunt doctor` no longer checks `tls` module or ethtool offload; still checks BTF, cgroup v2, CAP_BPF, cert chain, backend probes, clock skew.
- Catalog: `shunt_ktls_offload_total` removed.
- Migration: versioning refusal, ETag-drift requirement, `ListObjectVersions` merge dropped.
- Tiering: storage-class/access-mode consistency rule; RestoreObject 200/202/409.
- Dependencies: `go.yaml.in/yaml/v4`; Garage added as an AGPL tool; kTLS rows gone.
