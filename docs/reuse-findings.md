# Reuse audit findings (R0)

Run 2026-09-14 against read-only clones. Nothing was copied into the repo. Source of the map and the lift procedure: docs/reuse.md; the procedure itself is in CLAUDE.md.

| Repo | Commit | Date | Tag | License file |
|---|---|---|---|---|
| github.com/versity/versitygw | `4dc0debf8f79e0e0b099b9766a66b40b316dee7e` | 2026-09-14 | none (4dc0deb) | Apache-2.0, with NOTICE: "versitygw - Versity S3 Gateway / Copyright 2023 Versity Software" |
| github.com/clyso/chorus | `8b680455f4ce5f5a9d934ada4f9422842844b367` | 2026-09-10 | v0.7.10 | Apache-2.0 |
| github.com/cilium/ebpf | `60e81073fdc61c98149c44896135441cbae583a4` | 2026-09-10 | none (60e8107) | MIT, "Copyright (c) 2017 Nathan Sweet"; no separate LICENSE under examples/; `examples/headers/` carries its own `LICENSE.BSD-2-Clause` (libbpf headers) |
| github.com/johannesboyne/gofakes3 | `2dd1a84479724dc2f89d758452bee5bcea99e7f8` | 2026-07-09 | v1.2.0 | MIT, "Copyright (c) 2016 Johannes Boyne" |

## Lift row 1: aws-chunked decoding (versitygw `s3api/utils`)

### Files

| File | Lines | Header | Status |
|---|---|---|---|
| `s3api/utils/chunk-reader.go` | 227 | `// Copyright 2024 Versity Software` + Apache-2.0 boilerplate | clean |
| `s3api/utils/signed-chunk-reader.go` | 616 | same, 2024 | clean |
| `s3api/utils/unsigned-chunk-reader.go` | 519 | same, 2024 | clean |
| `s3api/utils/chunk-reader_test.go` | 43 | same, 2024 | clean (1 test) |
| `s3api/utils/unsigned-chunk-reader_test.go` | 177 | same, 2024 | clean (6 tests) |
| `s3api/utils/csum-reader.go` | 442 | same, 2023 | **not needed**: the chunk readers do not reference `HashReader`; it serves versitygw's full-body checksum path |
| `s3api/utils/crc.go` | 180 | **zlib license, Copyright (C) 1995-2017 Jean-loup Gailly and Mark Adler** | not needed (CRC combine for composite multipart checksums); if ever lifted it carries the zlib notice, not Apache |

All three reader files carry the full Versity Apache-2.0 header verbatim. Copyright holder matches the repo LICENSE and NOTICE. No file lacks a header.

### Imports and severance (proxy-runtime pulls in bold)

| Import | Where | Pulls in | Severance |
|---|---|---|---|
| **`github.com/gofiber/fiber/v3`** | chunk-reader.go: `ExtractChecksumType(ctx fiber.Ctx)`, `ParseDecodedContentLength(ctx fiber.Ctx)`, `NewChunkReader(ctx fiber.Ctx, …)` | Fiber + fasthttp | Replace `fiber.Ctx` with `http.Header` / `*http.Request` in three signatures; ~20 lines |
| **`github.com/versity/versitygw/debuglogger`** | 6 + 37 + 37 call sites (`Logf`, `Infof`) | debuglogger imports Fiber, Fiber's logger middleware, and fasthttp | Delete the calls or route to a package-local `func logf(string, ...any)` that is a no-op; ~80 one-line edits, mechanical |
| **`github.com/versity/versitygw/internal/sigv4auth`** | signed-chunk-reader.go lines 239, 266: `sigv4auth.SecureCompare` | that package imports Fiber in ctx.go, query.go, verify.go | Replace with `crypto/subtle.ConstantTimeCompare` or lift `compare.go` (33 lines, Versity 2026 header, stdlib only); ~2 lines |
| `github.com/versity/versitygw/s3err` | 6 + 15 + 22 call sites, all `s3err.GetAPIError(s3err.ErrX)` | s3err (row 2, itself liftable) | None if row 2 is lifted; otherwise map ~8 error codes to shunt's own catalog |
| **`github.com/aws/aws-sdk-go-v2/service/s3/types`** | 3 sites: `types.ChecksumAlgorithm(strings.ToUpper(...))` passed to `s3err.GetChecksumBadDigestErr` | the S3 service package tree (smithy, endpoints, …) — DESIGN §5 says the SDK is tests-only | Change the three helpers to take `string`; ~3 lines here plus ~10 in s3err |
| `github.com/cespare/xxhash/v2`, `github.com/zeebo/xxh3` | unsigned-chunk-reader.go `getHasher`, three cases for `x-amz-checksum-xxhash64/xxhash3/xxhash128` | two small MIT libs | These are Versity extensions, not AWS checksum algorithms; drop the three cases and both imports; ~10 lines |
| `hash/crc64`, `hash/crc32`, `crypto/*` | stdlib | — | — |

`AuthData` (constructor parameter) is a type alias for `sigv4auth.AuthData`; the readers use only `.Signature`, `.Access`, `.Region`. Shunt passes its own struct with those three fields.

Estimated severance total: ~130 mechanical lines, no logic changes.

### MinIO-lineage comparison

MinIO's Apache-era streaming verifier (`cmd/streaming-signature-v4.go`, pre-April-2021) had the shape: `s3ChunkedReader` over a `*bufio.Reader`, `readChunkLine` / `parseHexUint` / `parseChunkSignature`, `getChunkSignature`, `calculateSeedSignature`, and a state machine reading one chunk at a time into an internal buffer.

versitygw's signed reader is structurally different: it parses headers **out of the caller's `Read(p)` buffer in place** (`parseAndRemoveChunkInfo` → `parseChunkHeaderBytes` → `stashAndSkipHeader` / `handleRdrErr` with a `stash` for split headers), computes string-to-sign via `getStringToSignPrefix` / `getChunkStringToSign` / `getTrailerChunkStringToSign`, and tracks `chunkSizes` for size-order validation. No function names, control flow, or helper decomposition match MinIO's. The trailer and checksum handling (2024–26 additions) postdate MinIO's Apache era entirely.

versitygw's unsigned reader (`UnsignedChunkReader`, 2024) uses `bufio.ReadSlice('\r')` in `readChunkSizeLine` with a `maxHeaderSize` bound, `extractChunkSize`, `readTrailer`, `validateChecksum`, `handleExcessChunkData`. MinIO had no unsigned-trailer reader in its Apache era, so there is no Apache-era lineage to mirror; the AGPL-era one was not fetched or consulted.

**Verdict:** no MinIO structure mirrored; no missing attribution. Both readers are Versity-original.

### Recommendation

**Lift with edits** (POC-2 / P2), the three reader files plus both test files, into `internal/sigv4/chunked/` (or `internal/sigv4` directly). Edits limited to the severance table. Keep the Versity header, add the Modified-by line, carry the Versity NOTICE into THIRD_PARTY_NOTICES.

## Lift row 2: S3 error catalog (versitygw `s3err`)

### Files

32 files, 3,532 lines. Headers: 3 files `// Copyright 2023 Versity Software`, 29 files `// Copyright 2026 Versity Software`, all with the Apache-2.0 boilerplate. No file lacks a header. `s3err.go` (the catalog: `ErrorCode` enum, `errorCodeResponse` map of ~200 entries, `APIError{Code, Description, HTTPStatusCode}`, `XMLBody`, `GetAPIError`) is the payload; the other 31 files are typed error variants (`invalid-argument.go`, `sigv4.go`, `presigned-urls.go`, …) that carry extra XML fields for specific S3 errors.

### Imports

Stdlib only except one: `github.com/aws/aws-sdk-go-v2/service/s3/types` in `s3err.go` at four helper functions (`GetChecksumTypeMismatchErr`, `GetChecksumBadDigestErr`, `GetChecksumSchemaMismatchErr`, `GetChecksumTypeMismatchOnMpErr`) and `ObjectDeleteError` which returns `types.Error`. **No Fiber dependency.** `html` and `net/http` are used for `HTMLBody` (static-website hosting error pages), which shunt does not need.

Severance: change the four helpers to take `string`, replace `types.Error` with a local three-field struct or delete `ObjectDeleteError`; ~20 lines. Optionally delete `HTMLBody`/`encodeHTMLResponse` (~40 lines) and the `html` import.

### MinIO-lineage comparison

MinIO's Apache-era `cmd/api-errors.go` had `APIError{Code, Description, HTTPStatusCode}` and a `map[APIErrorCode]APIError` named `errorCodes`. versitygw's `APIError` struct has the same three fields with the same names, and the same map shape. **This is the one place the shape coincides.** Assessment: the three-field struct is the minimal representation of an S3 error XML (`Code`, `Message`, HTTP status) and the map-from-enum is the obvious Go idiom; the field names are the S3 wire names. The 2026 files, the typed-variant design, `XMLBody(requestID, hostID)`, and the `S3Error` interface have no MinIO counterpart. I judge this convergent, not copied, but it is the weakest provenance in the audit and the user should make the call. If in doubt: lift only the *table contents* (code → status → message, which are AWS facts) into a shunt-written file, and write the 60-line wrapper ourselves. That removes the question at a cost of an hour.

### Recommendation

**Lift with edits** (POC-1 / P1) into `internal/s3/s3err/`, or **table-only** if the user prefers zero provenance risk. Either way: Versity headers kept, Modified-by line, NOTICE carried.

## Lift row 3: S3 response XML structs (versitygw `s3response`)

### Files

`s3response/s3response.go` (~1,000 lines, 60 types, `// Copyright 2023 Versity Software`), `s3response/website.go` (+ test, 2026 header) which is static-website routing and not wanted. Headers present, holder matches.

### Imports

`s3response.go` imports `github.com/aws/aws-sdk-go-v2/service/s3/types` at ~20 field sites (`types.StorageClass`, `types.ObjectPart`, `types.CommonPrefix`, `types.Owner`, `types.RestoreStatus`, `types.Checksum`, `types.ObjectIdentifier`, `types.ChecksumAlgorithm`, `types.ChecksumType`, `types.EncodingType`, `types.ObjectStorageClass`), plus `s3err` (3 sites) and `debuglogger` (0 sites in s3response.go; 1 in website.go). **No Fiber.**

Severance: the SDK types are used as *field types inside XML-encoded structs*, so replacing them means defining local structs with the same field names (`ObjectPart{PartNumber, Size, ETag, ChecksumCRC32, …}`, `Owner{ID, DisplayName}`, `CommonPrefix{Prefix}`, `RestoreStatus`, `Checksum`, `ObjectIdentifier{Key, VersionId}`) and string aliases for the enums. ~80–100 lines, mechanical, and it produces byte-identical XML because `encoding/xml` names elements by field name either way.

### MinIO-lineage comparison

XML response structs are dictated by the S3 wire format; every implementation has `ListBucketResult{Name, Prefix, Contents[]…}`. versitygw adds custom `MarshalXML` for `Part`, `Object`, `Upload`, `ListAllMyBucketsEntry`, `CopyObjectResult`, `CopyPartResult`, `ObjectVersion`, and an `AmzDate` type — none of which MinIO's Apache-era `cmd/api-response.go` had. No concern.

### Recommendation

**Lift with edits** (POC-1 / P1): `s3response.go` only, into `internal/s3/s3response/`; skip `website.go`. Same header obligations.

## Lift row 4: eBPF `sock_ops` sampler (cilium/ebpf `examples/tcprtt_sockops`)

### License

`examples/` has no LICENSE of its own; the repo-root MIT license (Nathan Sweet, 2017) covers it. `tcprtt_sockops.c` has no per-file header; its BPF license string is `"Dual MIT/GPL"`. `examples/headers/` (bpf_helpers.h, bpf_endian.h, common.h, bpf_tracing.h) ships `LICENSE.BSD-2-Clause`, so a lift carries **two** notices: MIT for the example, BSD-2-Clause for the headers.

### What it records today

- `map_estab_sk`: `BPF_MAP_TYPE_HASH`, 65,535 entries, **already keyed by the IPv4 4-tuple** (`struct sk_key{local_ip4, remote_ip4, local_port, remote_port}`), value `sk_info{sk_key, sk_type ∈ {ACTIVE, PASSIVE}}`.
- On `BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB` / `PASSIVE_ESTABLISHED_CB`: inserts the tuple and sets `BPF_SOCK_OPS_RTT_CB_FLAG | BPF_SOCK_OPS_STATE_CB_FLAG`.
- On `BPF_SOCK_OPS_RTT_CB`: emits `rtt_event{sport, dport, saddr, daddr, srtt}` (srtt_us >> 3, then /1000 → ms) to a 16 MiB ring buffer.
- On `BPF_SOCK_OPS_STATE_CB`: deletes the map entry when leaving ESTABLISHED.
- Go side (`main.go`, 150 lines): `rlimit.RemoveMemlock`, `link.AttachCgroup(AttachCGroupSockOps)` on `/sys/fs/cgroup` (v2 detection via statfs), `ringbuf.NewReader`, prints events. Generated with `go tool bpf2go`, both endians checked in.

### Estimated changes for shunt

The reuse.md note "adapt the map to key by 4-tuple" is already satisfied. Remaining:

| Change | Lines |
|---|---|
| Add `BPF_SOCK_OPS_RETRANS_CB_FLAG` to `bpf_sock_ops_cb_flags_set` and a `case BPF_SOCK_OPS_RETRANS_CB:` that emits a second event type (or a `kind` field) with `skops->args` (retrans segs) | ~25 C |
| Keep srtt in microseconds (drop the `/1000`) and add `rcv_rtt`/`bytes_retrans` if wanted from `bpf_sock_ops` fields | ~5 C |
| IPv6 (`family == AF_INET6`, `local_ip6[4]`) — shunt's upstream endpoints may be v6 | ~30 C, key struct grows |
| Go: replace the print loop with a consumer that aggregates per upstream endpoint into the two catalog metrics (`tcp_srtt_seconds{endpoint}`, `tcp_retransmits_total{endpoint}`), match events to `net.Conn` local addr, feature-flag + `doctor` check + `TCP_INFO` fallback | ~150 Go, written by us |

This is P4 work. Nothing to copy now.

### Recommendation

**Lift as-is at P4** (`tcprtt_sockops.c` + `bpf_sockops.h` into `internal/ebpf/`, with the four `examples/headers/*.h`), then modify. Two notices in THIRD_PARTY_NOTICES.

## Chorus worker (run-as-tool candidate, not a lift)

Read: `service/worker/copy/copy.go` (the single S3 copy implementation, `S3CopySvc.CopyObject`, Clyso 2025 Apache header), `service/worker/handler/migration_obj_copy_handler.go`, `service/worker/handler/object_handlers.go`, `service/worker/handler/diff_handlers.go`, `pkg/meta/version_keys.go`, `pkg/store/*.go`, `pkg/tasks/queue_service.go`, `pkg/policy/replication.go`. Verified by direct reads of copy.go 249–386 and the copy handler 58–118; the rest by grep with file:line.

| Question | Answer | Evidence |
|---|---|---|
| Uses `If-None-Match: *` or any conditional PUT? | **No** | `copy.go:318-342` builds `minio.PutObjectOptions{UserMetadata, ContentType, UserTags, Internal.*, DisableContentSha256}` and nothing conditional; `copy.go:370` `PutObject(ctx, to.Bucket, to.Name, fromObject, stat.Size, putObjectOpts)`. The only conditional-PUT attempt in the repo is commented out in the Swift path: `service/worker/handler/swift/object_content.go:156-160` (`// toReq.IfNoneMatch = "*"`). What exists instead is a **pre-copy** short-circuit, `copy.go:293`: skip if destination exists with equal ETag and size — a TOCTOU window, not a conditional write. |
| Re-checks the source after copying, deletes the copy if it vanished? | **No** | `copy.go:370-384`: after `PutObject` the only work is recording `info.VersionID` and optional `CopyACLs`, then `return nil`. The single source HEAD is *before* the GET (`copy.go:259`). A reverse-direction check exists only for delete propagation: `object_handlers.go:112-116` re-HEADs the source before deleting the destination. |
| Preserves multipart part layout? | **No** | GET → stream → single `PutObject` with total size (`copy.go:351`, `:358`, `:370`); `PutObjectOptions.PartSize` is never set, so minio-go's internal `optimalPartInfo` chooses part boundaries. No `GetObjectAttributes`, `ListParts`, or part-number HEAD anywhere in `pkg/` or `service/worker/`. Consequence: multipart objects get a new ETag at the destination, which Chorus's own ETag-comparing diff then reports as inconsistent unless `ignore_etags` is set. |
| Can it be gated until an external signal? | **Partly** | Per-replication asynq queue pause/resume: `pkg/policy/replication.go:51-85` (`PauseReplication` / `ResumeReplication` over `migr_list_obj:<id>`, `migr_copy_obj:<id>`, `event:<id>` from `pkg/tasks/queue.go:81-88`), backed by `pkg/tasks/queue_service.go:146-160` (`inspector.PauseQueue`). Per-storage RPM rate limit checked at the top of every copy: `migration_obj_copy_handler.go:50-58`, `pkg/ratelimit/service.go:105-108`. **No** "create paused" or explicit start: `pkg/api/policy_handlers.go:229-238` enqueues the first migration task inside `AddReplication`, so a ratio-1 gate means pre-pausing the queue names or pausing immediately after creation (racy). |

Also found:

- **S3 library:** minio-go v7.3.0 via a fork (`go.mod:8` replaces it with `github.com/aiivashchenko/minio-go/v7`); aws-sdk-go v1 only for ACLs (`copy.go:410`, `:441`); **rclone is not used anywhere** (zero hits in the repo). docs/reuse.md's "uses rclone" note is wrong for v0.7.10.
- **Per-object state:** yes, extensive. Per-object version counters in a Redis hash per (replication, bucket) with fields `f:<obj>:<kind>` / `t:<obj>:<kind>` (`pkg/meta/version_keys.go:75-77`); per-object locks `lk:object:<storage>:<bucket>:<name>:<version>` (`pkg/store/stores.go:117-133`); per-object version lists `p:repl:objectversion:…` (`stores.go:190-197`); a destination-side marker in user metadata `x-amz-meta-chorus-source-version-id` (`copy.go:46`, `:328`). This is the worker's own bookkeeping and stays inside Chorus; it does not touch shunt's no-per-object-state rule as long as Chorus is run as a tool. It does mean Chorus needs Redis to run at all.
- **Diff / consistency check:** `service/worker/handler/diff_handlers.go` + `pkg/store/diff.go`, gRPC `Diff` service (`proto/chorus/diff.proto`). Compares (key, version index, size, ETag) by Redis set cancellation (`pkg/store/diff.go:164-169`, `:193-199`); toggles `ignore_etags`, `ignore_sizes`, `check_only_last_versions`. Does not compare user metadata, ACLs, tags, content-type, or timestamps. `Fix` re-enqueues copies that bypass the version check (`migration_obj_copy_handler.go:78-85`).

**Verdict for the mover question (docs/reuse.md "Verify or wrap"):** Chorus v0.7.10 satisfies none of the four §2.5 mover-contract items. It is a bulk-copy tool with a useful (key, size, ETag) diff, not the contract. `test/mover` in POC-4 / P5 is the reference mover, as reuse.md anticipated. Chorus can still be run for bulk copy on a bucket that is in `MIGRATING` at ratio 1 **if** the operator accepts that (a) the delete/copy race is open for the whole copy, (b) multipart ETags change, and (c) it needs its own Redis. The POC-4 demo should use test/mover; Chorus is an optional P5 comparison run, not a dependency.

## gofakes3 (test dependency)

MIT, holder matches. `go.mod` requires aws-sdk-go-v2 (service/s3 v1.97.3), afero, bbolt, goskiplist, testify. It is imported **only in `_test.go` files**, so none of that reaches the proxy binary; `go build ./cmd/shunt` stays clean. It supports `If-None-Match` on GET (`gofakes3.go:541`) and has a `conditional_put_test.go` (6 tests) so `If-None-Match: *` on PUT is exercised. Multipart is routed (`routing.go:178-180`) via `listMultipartUploadParts` / `putMultipartUploadPart`. `chunk.go` handles aws-chunked bodies. Good enough for POC-1 unit tests; whether it is deep enough for POC-2 signing-mode cases is decided when s3diff runs against it.

## Not audited (not Lift rows)

- Chorus `service/proxy/auth/signature_v4*.go`: the map already says "write ours; use as oracle". Not read for structure, to keep the provenance question closed.
- versitygw `s3api` router and Chorus `service/proxy/router`: "reference, then write" — to be used as the op-table checklist in POC-1, nothing copied.
- versitygw `tests/`: `tests/test_rest_chunked.sh` and `tests/rest_scripts/put_object_openssl_chunked_example.sh` exist and generate aws-chunked payloads with openssl; they are the source for s3diff's signing-mode cases in POC-2 (run as tools).

## Copy list

Approved 2026-09-14 with changes: **s3err is not copied**; `internal/s3/errors.go` is written by shunt with the AWS wire values and its own renderer. Items 1–2 below are therefore void. Item 3 and gofakes3 were done in the same commit as this note; items 4–7 are copied at the start of POC-2 after re-checking the SHA; item 8 at P4 with the BSD-2-Clause headers notice.

Original proposal follows.

Two lifts are due now for POC-1; the third is POC-2; the fourth is P4. Each file keeps its original header verbatim and gets this line directly beneath it:

```
// Modified by Blake Golliher for github.com/blakegolliher/shunt, 2026-09-14: <what changed>.
```

| # | Source (versitygw @ 4dc0debf) | Destination | Modified-by note | Phase |
|---|---|---|---|---|
| 1 | `s3err/s3err.go` | `internal/s3/s3err/s3err.go` | "removed aws-sdk-go-v2 types (four checksum helpers take string; ObjectDeleteError dropped), removed HTML error rendering" | POC-1 |
| 2 | `s3err/*.go` (the 31 typed-variant files) | `internal/s3/s3err/` same names | "package path only" (or none if unchanged) | POC-1 |
| 3 | `s3response/s3response.go` | `internal/s3/s3response/s3response.go` | "replaced aws-sdk-go-v2 types with local structs of the same field names; XML output unchanged" | POC-1 |
| 4 | `s3api/utils/chunk-reader.go` | `internal/sigv4/chunked/chunk-reader.go` | "fiber.Ctx replaced by http.Header; debuglogger calls removed" | POC-2 |
| 5 | `s3api/utils/signed-chunk-reader.go` | `internal/sigv4/chunked/signed-chunk-reader.go` | "debuglogger removed; sigv4auth.SecureCompare replaced by crypto/subtle; aws types replaced by string" | POC-2 |
| 6 | `s3api/utils/unsigned-chunk-reader.go` | `internal/sigv4/chunked/unsigned-chunk-reader.go` | "debuglogger removed; xxhash checksum types dropped; aws types replaced by string" | POC-2 |
| 7 | `s3api/utils/chunk-reader_test.go`, `unsigned-chunk-reader_test.go` | `internal/sigv4/chunked/` | "package path only" | POC-2 |
| 8 | cilium/ebpf `examples/tcprtt_sockops/tcprtt_sockops.c`, `bpf_sockops.h`; `examples/headers/{common.h,bpf_helpers.h,bpf_helper_defs.h,bpf_endian.h,bpf_tracing.h}` | `internal/ebpf/` | C: "added BPF_SOCK_OPS_RETRANS_CB, srtt kept in µs"; headers: unchanged | P4 |

Not copied, by decision: `s3api/utils/csum-reader.go` and `crc.go` (not needed; crc.go is zlib-licensed), `s3response/website.go`, anything from Chorus, anything from `internal/sigv4auth` beyond the two-line SecureCompare replacement.

Bookkeeping per lift (CLAUDE.md procedure): THIRD_PARTY_NOTICES gets versitygw's LICENSE text plus its NOTICE ("versitygw - Versity S3 Gateway / Copyright 2023 Versity Software") at the first versitygw lift, and cilium/ebpf's MIT notice plus `examples/headers/LICENSE.BSD-2-Clause` at the P4 lift. docs/deps.md gets one row per source path with the commit SHA. gofakes3 is added to go.mod as a test-only import at POC-1 with its own deps.md row.

**Open decision for the user:** item 1, lift `s3err.go` whole (Versity header, one three-field struct that coincides with MinIO's Apache-era shape) or take only the table contents into a shunt-written file. The audit's judgment is that the whole-file lift is clean; the table-only option costs about an hour and removes the question entirely.
