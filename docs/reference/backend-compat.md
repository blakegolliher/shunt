# Backend compatibility

What each backend enforces, measured with `shunt probe` (`make probe BACKEND=…`), and every gap s3diff found in resign mode. This is the source for each cluster's `capabilities` block (docs/reference/config.md) and for ADR-0002's compensation cases. Re-run the probe after any backend upgrade (docs/DESIGN.md §9 item 12).

Probe run 2026-09-15 with shunt at POC-2.

## Summary

| Check | Garage 2.3.0 | MinIO RELEASE.2025-07-23 | VAST 5.x (vast02) |
|---|---|---|---|
| Rejects a wrong hex `x-amz-content-sha256` | yes, `400 InvalidDigest` | yes, `400 XAmzContentSHA256Mismatch` | yes, `400 XAmzContentSHA256Mismatch` |
| Accepts `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | yes | yes | yes |
| Validates the trailer checksum | yes, `400 InvalidDigest` | yes, `400 XAmzContentChecksumMismatch` | yes, `400 BadDigest` |
| `x-amz-checksum-crc32` header validated | yes | yes | yes |
| `x-amz-checksum-crc32c` header validated | yes | yes | yes |
| `x-amz-checksum-sha1` header validated | yes | yes | yes |
| `x-amz-checksum-sha256` header validated | yes | yes | yes |
| `x-amz-checksum-crc64nvme` header validated | yes | yes | yes |
| `If-None-Match: *` on PUT (`capabilities.conditional_write`) | **no: Garage 2.3.0 overwrites with 200.** Measured on 2.3.0 only; re-probe after any upgrade | yes, 412 | yes, 412 |
| `encoding-type=url` on a listing (DESIGN §2.5: the merge normalizes key encoding before comparing) | percent-encodes `/` in keys | **leaves keys verbatim** | not measured |
| `x-amz-mp-parts-count` on a plain HEAD (DESIGN §2.5: the mover falls back to `GetObjectAttributes` or a `?partNumber=1` HEAD) | not sent | **not sent** (needs `partNumber`) | not measured |
| `If-Match` on PUT | **no, mismatch accepted** | yes, 412 on mismatch | **no, mismatch accepted** |
| `If-Match` on DELETE (`capabilities.conditional_delete`; probed 2026-09-16) | **no, a mismatch deletes (204)** | **no, a mismatch deletes (204)** | not measured |
| Unsigned `GET /` | 403 | 403 | **200**, empty `ListAllMyBucketsResult` owned by `Anonymous` |
| Enforces the signing region | yes (`garage`) | yes (`us-east-1`) | **no**: a signature scoped to `nowhere-1` is accepted |
| TLS certificate | n/a, plaintext in e2e | n/a, plaintext in e2e | **self-signed factory certificate** (CN `vms.example.com`, SAN `*.example.com`), does not cover the endpoint name; probed and tested with verification disabled |

## What shunt hides, and what it does not

shunt hides **which cluster serves a bucket and under which name**: bucket names, cluster endpoints, and uploadIds are rewritten in every response (ADR-0006), and s3diff's mixed mode fails the run if one appears. shunt does **not** hide **which kind of backend** is serving: `Server`, `X-Minio-*`, `X-Vast-*`, MinIO's `<HostId>`, Garage's `<Region>`, GetBucketLocation, backend owner ids, and backend uploadId formats pass through, so vendor trace ids stay usable for support (decision of 2026-09-15). Bucket policy bodies pass through unrewritten as well: an ARN in a policy may name the backend bucket, which a client can then read.

## Consequences for shunt

- **Capability profile.** All three enforce the hex SHA-256 and accept and validate unsigned trailers, so `enforces_sha256: true` and `unsigned_trailer: true` (the defaults) are correct for all three. With these profiles shunt's ADR-0002 log-and-alert path is never reached on these backends; it stays for backends that fail the probe.
- **Error codes differ by backend** for the same failure. shunt relays the backend's code unchanged in resign mode. Clients that match on `BadDigest` versus `XAmzContentChecksumMismatch` will see whatever the backend emits.
- **Bad credentials, as each backend reports them (2026-09-16).** VAST and MinIO answer a wrong secret with `SignatureDoesNotMatch` and an unknown access key with `InvalidAccessKeyId`. Garage 2.3.0 answers `AccessDenied` for both, and names the fault in the message: `Forbidden: Invalid signature` or `Forbidden: No such key: <key>`. VAST answers `InvalidSecurity` to a valid key that isn't allowed the operation, CreateBucket in the lab. `s3.ClassifyCredentialError` reads all of these, and `shunt cluster add` and `shunt migrate run` use it to tell a credentials mistake from a permission denial.
- **Conditional PUT and DELETE, in one place.** **Garage 2.3.0 ignores `If-None-Match: *` on PUT.** That is a fact about 2.3.0: re-run `make probe BACKEND=garage` after any Garage upgrade and update `capabilities.conditional_write`, because a later release may honor it. **Neither Garage 2.3.0 nor MinIO RELEASE.2025-07-23 honors `If-Match` on DELETE.** Both delete on a mismatch. The consequences are the lost-write window on migrations into Garage, which `shunt migrate start` refuses without `--accept-lost-write-window` (docs/migrating.md), and the mover's re-HEAD withdrawal on both backends (ADR-0004 races 1 and 2).
- **Mover contract (docs/DESIGN.md §2.5, POC-4).** The mover prefers `If-None-Match: *` on PUT. Garage 2.3.0 ignores it, so a migration **into** Garage uses the HEAD-then-commit guard of ADR-0004 instead; MinIO and VAST honor it. The choice is made per cluster from `capabilities.conditional_write`, not from the backend's name. Neither Garage nor VAST honors `If-Match` on PUT; the mover does not use it.
- **Listing key encoding is not portable (found by the POC-4 demo, 2026-09-15; a design rule in docs/DESIGN.md §2.5 since 2026-09-16).** With `encoding-type=url`, Garage 2.3.0 percent-encodes `/` in a key and MinIO returns it verbatim, so the same object arrives from the two clusters under two different spellings. A merged listing that compared them as strings reported every key twice. shunt now asks both sides for `encoding-type=url`, decodes the keys itself, merges the decoded names, and re-encodes on the way out according to what the **client** asked for (`internal/proxy/merge.go`). The one shape this cannot distinguish is a key that really contains a percent escape on a backend that ignores `encoding-type`; that is the backend's non-compliance, and shunt decodes it.
- **A part count needs a part number (found by the POC-4 demo, 2026-09-15; part of the mover contract in docs/DESIGN.md §2.5 since 2026-09-16).** MinIO does not send `x-amz-mp-parts-count` on a plain `HEAD`, so a mover that trusts it copies a multipart object as a single part and its ETag changes. The mover takes the multipart `-N` ETag suffix as the trigger and a `HEAD` with `partNumber=1` as the authority (`shunt migrate run`, `cmd/shunt/mover.go`).
- **Conditional delete (probed 2026-09-16).** Neither Garage 2.3.0 nor MinIO RELEASE.2025-07-23 honors `If-Match` on `DeleteObject`: a mismatched ETag gets 204 and the object is deleted. The mover therefore withdraws its own copy after a delete/copy race with a `HEAD` that checks ETag and `Last-Modified`, not with `If-Match` (ADR-0004 race 1, window A). `capabilities.conditional_delete` defaults to false for this reason: assuming the header works on a backend that ignores it deletes a client's newer write every time.
- **Health checks (§2.8).** "Any HTTP response counts as alive" holds for all three. VAST answering 200 anonymously is not an error but means an unsigned `GET /` proves nothing about credentials.
- **Region.** VAST accepts any scope region, so the cluster record's `region` only matters for backends that enforce it. The resign path signs with the cluster's configured region regardless (ADR-0001 amendment).
- **TLS on VAST is TEMPORARY-insecure.** `tls.insecure_skip_verify: true` in `test/e2e/shunt-vast-resign.yaml`, `--insecure` on the probe, `-direct-insecure` on s3diff. Remove all three once the cluster has a certificate for `vast02.example.com` and set `tls.ca`.

## VAST specifics

- **Signing region is not enforced.** A request signed for `nowhere-1` is accepted as readily as `us-east-1`. The cluster record's `region` is still what shunt signs with upstream.
- **Unsigned `GET /` answers 200** with an empty `ListAllMyBucketsResult` owned by `Anonymous`, not 403. Health checks that treat any response as alive work; nothing about credentials can be inferred from it.
- **TLS (checked 2026-09-15).** The cluster serves VAST's self-signed factory certificate: CN `vms.example.com`, SAN `*.example.com`, `vms.example.com`, and 33 IPs (10.0.0.3, 100.64.0.1–16, 100.64.1.1–16), valid to 2036. It covers neither `vast02.example.com` nor the address that name resolves to. A wildcard matches one label, so `*.example.com` cannot match it. The lab wildcard `*.lab.example.com` on the dev box would not match it either, and it expired 2026-09-02. Verification stays off until the cluster serves a certificate for its hostname.

## Raw probe output

### Garage

```
endpoint                            http://127.0.0.1:3900 (region garage)
unsigned GET /                      403
enforces x-amz-content-sha256       yes (400 InvalidDigest)
STREAMING-UNSIGNED-PAYLOAD-TRAILER  yes, checksum validated (400 InvalidDigest on mismatch)
x-amz-checksum-crc32                validated (400 InvalidDigest)
x-amz-checksum-crc32c               validated (400 InvalidDigest)
x-amz-checksum-sha1                 validated (400 InvalidDigest)
x-amz-checksum-sha256               validated (400 InvalidDigest)
x-amz-checksum-crc64nvme            validated (400 InvalidDigest)
If-None-Match: * on PUT             NO (overwrote with 200)
If-Match on PUT                     NO (mismatch accepted)
note                                scratch bucket probe-garage was created by the probe and deleted afterwards
```

### MinIO

```
endpoint                            http://127.0.0.1:9000 (region us-east-1)
unsigned GET /                      403
enforces x-amz-content-sha256       yes
STREAMING-UNSIGNED-PAYLOAD-TRAILER  yes, checksum validated (400 XAmzContentChecksumMismatch on mismatch)
x-amz-checksum-crc32                validated (400 XAmzContentChecksumMismatch)
x-amz-checksum-crc32c               validated (400 XAmzContentChecksumMismatch)
x-amz-checksum-sha1                 validated (400 XAmzContentChecksumMismatch)
x-amz-checksum-sha256               validated (400 XAmzContentChecksumMismatch)
x-amz-checksum-crc64nvme            validated (400 XAmzContentChecksumMismatch)
If-None-Match: * on PUT             yes (412)
If-Match on PUT                     yes (412 on mismatch, 200 on match)
note                                scratch bucket probe-minio was created by the probe and deleted afterwards
```

### Garage and MinIO, conditional delete (2026-09-16)

`shunt probe` gained one row. The rest of each table matched the runs above.

```
Garage  If-Match on DELETE                  NO (mismatch deleted the object, 204)
MinIO   If-Match on DELETE                  NO (mismatch deleted the object, 204)
```

### VAST (TLS verification disabled)

```
endpoint                            https://vast02.example.com:443 (region us-east-1)
unsigned GET /                      200
enforces x-amz-content-sha256       yes
STREAMING-UNSIGNED-PAYLOAD-TRAILER  yes, checksum validated (400 BadDigest on mismatch)
x-amz-checksum-crc32                validated (400 BadDigest)
x-amz-checksum-crc32c               validated (400 BadDigest)
x-amz-checksum-sha1                 validated (400 BadDigest)
x-amz-checksum-sha256               validated (400 BadDigest)
x-amz-checksum-crc64nvme            validated (400 BadDigest)
If-None-Match: * on PUT             yes (412)
If-Match on PUT                     NO (mismatch accepted)
note                                TLS certificate verification was DISABLED (--insecure)
```

The same probe signed for region `nowhere-1` also succeeded on VAST.

## s3diff in resign mode

Every client payload mode was sent both directly to the backend (signed with the backend's key) and through shunt (signed with a shunt-issued key for region `us-east-1`, re-signed upstream with the cluster's key and region). Findings, each with its cause:

| Finding | Backend | Cause | Owner |
|---|---|---|---|
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` rejected directly with `400 InvalidRequest "Invalid payload signature"`, all five checksum algorithms; the same bytes through shunt succeed | Garage 2.3.0 | Garage does not verify signed trailers. The client encoding is valid: MinIO and VAST accept it directly, and it reproduces AWS's published streaming vectors. shunt decodes and verifies the signed trailer itself, then forwards an unsigned trailer Garage accepts | **Backend gap** (s3diff `GAP(backend)`) |
| `<Location>` in CompleteMultipartUpload shows the upstream endpoint (`http://127.0.0.1/…`) instead of the client-facing host | MinIO | Resign mode sends upstream path-style to the endpoint address, and MinIO echoes the request Host into `<Location>`. **Closed in POC-3:** the rewriter replaces `<Location>` with the client-facing URL, and both a unit test and the mixed s3diff run assert its contents. It returns only if `kill_switches.xml_rewrite_disable` is set, which `shunt serve` warns about | closed (ADR-0006) |
| Error bodies for virtual-host requests carry a path-style `<Resource>` (`/bucket/key`), and a HEAD error's `Content-Length` changes with it | Garage, MinIO | Same path-style upstream (ADR-0001 amendment) | closed in POC-3: `<Resource>` is rewritten to the client's own path, path-style or virtual-host (ADR-0006) |
| 204 responses: backend sends `Content-Length: 0`, shunt's response has none | VAST | RFC 9110 §8.6 forbids `Content-Length` on 204; Go's server enforces it. Not a defect; s3diff skips the header on 204 | none |
| ListBuckets returns the same buckets in a different order on consecutive calls | Garage | Garage's listing order is not stable; seen in passthrough too. s3diff now compares bucket entries order-insensitively | none |
| `<Location>` on CompleteMultipartUpload comes back with a doubled dot: `https://bucket..shunt.example.com/key` | Garage 2.3.0 | Garage builds it as `<bucket>.<root_domain>`, and `root_domain` in `garage.toml` is written with its leading dot (`.shunt.example.com`), as Garage's own documentation shows. The two dots meet. Harmless through shunt, which replaces `<Location>` with the client-facing URL; a client talking to Garage directly gets a URL whose host does not resolve | none (backend cosmetic) |
| `X-Vast-Rcf-Id` differs on every response | VAST | Per-request trace id, like `x-amz-request-id`; allowlisted | none |

Final per-backend counts are in docs/STATUS.md.
