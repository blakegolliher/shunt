# Shunt reuse audit — what to lift, what to run, what to write

Sources examined: clyso/chorus (v0.7.10, Sep 2026, Apache-2.0), versity/versitygw (2,715 commits, Apache-2.0 with NOTICE), seaweedfs, cilium/ebpf, aws-sdk-go-v2, gofakes3. MinIO server code is AGPL-3.0 since April 2021 and is excluded everywhere below.

Two rules before any copy:

1. **Copy leaf packages, don't import the module.** Importing versitygw pulls Fiber and a large tree; importing Chorus pulls minio-go, asynq, and Redis. Both break stdlib-first. Copy the specific files with their headers.
2. **Lineage matters more than the LICENSE file.** Several Go S3 verifiers are function-for-function shaped like MinIO's Apache-era code (pre-2021). That lineage is legal only when the file carries the MinIO copyright header. A file with the same shape and no header cannot be shown to be clean, and shunt does not copy it.

## The map

| Shunt component | Verdict | Source | License | Take | Obligation |
|---|---|---|---|---|---|
| aws-chunked decoding: signed chunks, `STREAMING-UNSIGNED-PAYLOAD-TRAILER`, size and trailer validation | **Lift** | versitygw `s3api/utils` chunk readers | Apache-2.0 | The chunk-reader files. Actively fixed through 2025–26: decoded-length validation, chunk-size validation, malformed-encoding handling, trailer checksum calculation. | Keep header; add "modified by" line; entry in NOTICE and deps.md with commit SHA |
| S3 error catalog (code → HTTP status → message, XML rendering) | **Write ours** (decided after R0) | versitygw `s3err` as a completeness reference | — | `internal/s3/errors.go`: the table of AWS wire values (codes, statuses, messages are AWS facts) and our own XML renderer. Nothing copied; versitygw's three-field struct coincides with MinIO's Apache-era shape and the table-only route removes the question | None |
| S3 response XML structs (ListObjectsV2, ListParts, InitiateMultipartUpload, ListBuckets…) | **Lift** | versitygw `s3response` | Apache-2.0 | The struct definitions with xml tags — feeds the uploadId rewrites, ListBuckets synthesis, and listing-merge output | Same |
| S3 operation enumeration (method × query × key → op) | **Reference, then write** | versitygw `s3api` router; Chorus `service/proxy/router` | Apache-2.0 | Use as the completeness checklist for shunt's Op table. Both are framework-bound (Fiber / their middleware); the table is what transfers, not the code | None if nothing is copied |
| SigV4 verify (header, presigned), canonicalization, signing key | **Write ours; use theirs as oracles** | Chorus `service/proxy/auth/signature_v4.go`, `signature_v4_presign.go` | Apache-2.0, Clyso copyright | Nothing. 174 lines, Clyso header only, but the function set mirrors MinIO's verifier with no MinIO attribution, and it does not verify streaming chunks. Our own ~200 lines with SDK round-trip tests is cheaper than the provenance question | — |
| Upstream signing | **Write ours** | aws-sdk-go-v2 `aws/signer/v4` exists (Apache-2.0) | — | Verify and sign share canonicalization; one implementation, ours. SDK signer stays in tests as the oracle | — |
| Mover (copy engine, checkpointing, metadata/ACL/tag sync, rate limits, diff check) | **Not a mover; at most a bulk-copy comparison** | Chorus worker (`service/worker/copy`, `service/worker/handler`) | Apache-2.0 | R0 audit (docs/reuse-findings.md) found v0.7.10 fails all four §2.5 mover-contract items: no conditional PUT (`If-None-Match: *`), no post-copy source re-check, no part-layout preservation (single streaming PutObject, minio-go picks part size, ETags change), no create-paused gate (only queue pause after enqueue). It does not use rclone; it needs Redis and keeps per-object state of its own. `test/mover` (P5) is the reference mover. Chorus may be run unmodified as a P5 bulk-copy comparison on a ratio-1 bucket if the operator accepts the open delete/copy race and ETag drift; its (key, size, ETag) diff is still a useful CUTOVER cross-check | Running a tool imposes nothing; document the version used |
| Mover contract items Chorus lacks (`If-None-Match: *`, re-HEAD after copy, part-layout preservation, ratio-1 gate) | **Verified: all four missing** | — | — | R0 checked the worker's copy path (docs/reuse-findings.md, Chorus section, with file:line evidence). test/mover in P5 is the reference mover; Chorus is a bulk-copy option, not the contract | — |
| eBPF `sock_ops` RTT/retransmit sampler | **Lift** | cilium/ebpf `examples/tcprtt_sockops` | MIT | The C program and Go loader; adapt the map to key by 4-tuple and add the retransmit callback | Keep the MIT copyright and permission notice; entry in NOTICE and deps.md |
| TCP_INFO fallback sampler | **Own code** | tcpx | Yours | Port directly | Relicense as author; confirm no outside contributions in the file |
| Slow-request ring | **Own code** | s3slower | Yours | Port the ring and the breakdown schema | Same |
| Backend probes (checksums, part-number, GetObjectAttributes, sha256 enforcement) | **Own code** | vast-s3-compatibility scripts | Yours | Port into `shunt probe` | Same |
| In-process fake S3 for unit tests | **Use** | gofakes3 (Chorus uses it in `test/`) | MIT | Import as a test dependency only; keeps unit tests free of Docker | Notice preserved in test tree |
| Embedded gateway as a second in-process backend | **Optional** | versitygw `embedgw` | Apache-2.0 | Only if gofakes3 proves too shallow (no multipart edge cases) | Notice preserved in test tree |
| Conformance and signing-mode test cases | **Run as tools** | versitygw `tests/` (aws-cli, s3cmd, curl system tests; streaming-payload generation script with signed/unsigned trailer options), ceph/s3-tests | Apache-2.0; MIT (verify) | Point them at shunt; mine the streaming-payload cases for s3diff's signing-mode dimension | None |
| Bench harness | **Run as tools** | Chorus `tools/bench` (separate module); warp | Apache-2.0; AGPL-3.0 | Either as a tool. Warp never as code | None |
| Listing merge, uploadId codec, RAMPING routing, RestoreObject emulation, lifecycle subset, Postgres control plane | **Write** | Nothing liftable in Go exists | — | NooBaa merges listings but in JavaScript; Zenko keeps a per-object index; nothing has the ramp | — |
| Rate limiting (P8 QoS) | **Library** | `golang.org/x/time/rate` | BSD-3 | Standard choice; Chorus `pkg/ratelimit` is a thin wrapper over the same idea | Notice |
| Postgres | **Library** | `jackc/pgx/v5` | MIT | Control service only | Notice |

## What this changes in the phase plan

- **POC-2 / P2:** the aws-chunked decoder (the hardest part of resign) becomes a lift-and-adapt from versitygw instead of a from-scratch write. Canonicalization and signing stay ours. This is the single biggest schedule effect.
- **POC-1 / P1:** `s3response` is lifted and `internal/s3/errors.go` is ours; XML shapes and error bodies are in place before the first s3diff run.
- **POC-4 / P5:** the mover is `test/mover`. Chorus does not honor the contract (R0); it is at most a bulk-copy comparison run.
- **P4:** the `sock_ops` sampler starts from a working example rather than a blank C file.
- **Unit tests everywhere:** gofakes3 in-process; Docker backends only for e2e.

## Lift procedure (goes in CLAUDE.md)

1. Copy the file(s) into the target package. Keep the original license header verbatim.
2. Directly under the header add: `// Modified by <name> for github.com/blakegolliher/shunt, <date>: <one line on what changed>.` Apache-2.0 §4(b) requires the modification notice; MIT doesn't, but do it anyway.
3. Append the project's LICENSE and NOTICE text to `THIRD_PARTY_NOTICES` (Apache-2.0 requires NOTICE contents carried forward when the source has one — versitygw does).
4. Add a line to `docs/deps.md`: source repo, path, commit SHA, license, why the stdlib wasn't enough.
5. Never copy a file whose only license signal is the repo LICENSE while its content matches a known AGPL or unattributed lineage.
6. G4 checks all of the above every phase.

## R0 — Reuse audit prompt (run once, before POC-1)

```
Reuse audit. Read docs/DESIGN.md §5, docs/CONTEXT.md, and docs/reuse.md (this document). Clone read-only into /tmp: github.com/versity/versitygw, github.com/clyso/chorus, github.com/cilium/ebpf, github.com/johannesboyne/gofakes3. Do not copy anything into the repo yet.

For each "Lift" row in docs/reuse.md:
1. Locate the exact files. Print their license headers verbatim. If a file has no header, or a header whose copyright holder differs from the repo's LICENSE, say so and stop for that file.
2. List every import of the file. Flag any that would pull a web framework, Redis, minio-go, or the AWS SDK into the proxy runtime. Estimate the lines needed to sever each.
3. Compare the file's function set against MinIO's Apache-era signature verifier and chunked reader (you may recall their shape; do not fetch AGPL-era MinIO code). Report any file that mirrors MinIO's structure without a MinIO copyright line.
4. Record for each file: repo, path, commit SHA, license, header status, import severance cost, recommendation (lift as-is / lift with edits / reference only).

For the Chorus worker: read service/worker/copy and pkg/replication and answer, with file and line references: does a copy use If-None-Match: * or any conditional PUT; does it re-check the source after copying; does it preserve multipart part layout or re-upload with its own part size; can it be told not to start until an external gate. Answer each yes/no with evidence.

For cilium/ebpf examples/tcprtt_sockops: confirm the license file for the examples directory, list what the program records today, and estimate the changes to key by 4-tuple and add BPF_SOCK_OPS_RETRANS_CB.

Write the findings to docs/reuse-findings.md. Then, and only then, propose the exact copy list with destination paths and the header text for each file. Wait for my approval before copying.
```
