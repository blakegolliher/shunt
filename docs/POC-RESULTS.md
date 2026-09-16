# POC results

2026-09-16. The four claims of docs/POC.md, on Garage 2.3.0 and MinIO RELEASE.2025-07-23 (VAST 5.x where stated), with shunt, clients and backends on one host (docs/bench/poc1.md, "Disclosure"). **All four hold.** The `poc-3` and `poc-4` tags stay held until the VAST lab cluster serves a certificate for its hostname and VAST joins the mixed run and the demo.

## The claims, and the runs that proved them

| Claim | Proven by | Result |
|---|---|---|
| **A transparent streaming proxy at measured overhead** | POC-1: s3diff passthrough; bench (docs/bench/poc1.md) | s3diff: 0 diffs on Garage and MinIO (202 cases; 258 on re-run at POC-3). An unauthorized 5 GiB PUT answered 403 in 9 ms with 0 body bytes sent. 1 GiB: 0.70–1.19× direct throughput at 1.8–3.1 CPU-seconds per GiB. 4 KiB: about 1 ms added p50 at one connection, 1.2–1.5 ms of proxy CPU per request |
| **Verify-and-resign SigV4 works with real clients and backends** | POC-2: s3diff in resign mode across 15 payload modes (docs/bench/poc2.md) | VAST: 156 cases, 0 diffs. MinIO: 258, 0 diffs once POC-3 rewrote echoes. Garage: 258, 0 diffs, plus 10 backend gaps: Garage rejects signed trailers when called directly, and they succeed through shunt. Resign costs +25 to +219 µs of CPU per small request (one cell measured −104, run-to-run noise) |
| **One endpoint hides which backend a bucket is on, over mixed backend types** | POC-3: mixed s3diff, one shunt over Garage and MinIO | 332 cases, 0 diffs. 0 backend names or cluster endpoints in any response. Two tenants own the same bucket name on different clusters and each sees only its own. ListBuckets spans clusters. Every uploadId carries its cluster prefix |
| **A bucket moves between backends with no per-object state and no client change** | POC-4: `demo.sh`, `evacuate.sh`, 60-minute property runs, all on 2026-09-16 | Garage → MinIO: 1,004 objects verified by GET and ETag, multipart included; 102 of 102 writes made during the move survived; listing diff empty; fallback reads flat at 12. MinIO → Garage, whole cluster: 3 buckets across 2 tenants, 304 objects verified by ETag, MinIO switched off and both tenants kept reading and writing. Property test, MinIO-like target: 29,330,793 operations, 0 violations, with 29 mover copies refused as contended and 11 copies whose source was deleted mid-copy. GET and PUT cost during migration is below this box's run-to-run noise; a merged listing costs 86 ms per page |

## Four bugs only real backends found

The fake backends agreed with each other, so unit tests passed. Each bug surfaced on the first live run that exercised it (POC-4):

1. **Merged listings returned every key twice.** Garage percent-encodes `/` in listing keys under `encoding-type=url`; MinIO does not. The merge now decodes keys before comparing (DESIGN §2.5).
2. **Multipart objects lost their ETag in the mover.** MinIO omits `x-amz-mp-parts-count` on a plain HEAD, so the mover copied them as one part. Part layout now comes from a `?partNumber=1` HEAD (DESIGN §2.5 mover contract).
3. **The mover's cursor outlived a completed pass**, so a second pass silently skipped keys written behind it.
4. **A copy from a bucket mid-migration** was routed by a stale POC-3 helper; it is now refused with 501.

## The lost-write window, measured, and the stance on it

On a target that ignores `If-None-Match: *` (Garage 2.3.0 today), the mover's guard is a HEAD, then its PUT. A client write that commits between the two is overwritten with the source's older bytes, and the client has already been told 200. The guarded 60-minute run (17,326,931 operations) aimed client writes at the key the mover was copying. **2 writes were lost, in 62 copies of contended keys**, with windows of 0.78 ms and 1.58 ms from the mover's HEAD to its PUT. Every one of the 15,483 violations was a read of those losses; none fell outside the window. On MinIO and VAST, which honor the header, there is no window.

**Stance:** the window is documented, not hidden, and starting into it takes a decision. `shunt migrate start` and the mover both refuse a target whose `conditional_write` is false, naming the window, unless given `--accept-lost-write-window`. The operator quiesces writers for the mover run, or ramps to ratio 1 and runs the mover at low write volume (docs/migrating.md). A second, narrower window has not been observed under load and is covered by tests: neither Garage nor MinIO honors `If-Match` on DELETE, so when the mover withdraws its copy of a deleted object it HEADs first, and a client write in that one round trip is lost (ADR-0004 race 1, window A).

## Carried gaps

| Gap | Picked up by |
|---|---|
| TLS verification is off for VAST; `poc-3`/`poc-4` tags and VAST in the mixed run and demo wait on a certificate | Lab certificate, then the tags |
| The mover is a test fixture | P5 (production mover, same contract) |
| ListObjects v1 and ListMultipartUploads are not merged; a merged ListObjectsV2 buffers one page per side (ADR-0007) | P5 (streaming merge, 1M×2-key memory bound) |
| Property test covers the hash ramp only: no prefix ramp, multipart, or ETag-preservation assertion | P5 acceptance gate |
| Compensation is log-and-alert only; an unsigned-trailer client is passed straight to a backend that rejects unsigned trailers | P2 |
| Directory writes need a writable file (a read-only ConfigMap answers 503); credentials are inline | P3c |
| Capability profiles are hand-written from `shunt probe` output | P3b |
| P2C/EWMA, ejection, retries, dns endpoints, AWS 301 handling; cert hot reload, mTLS, SO_REUSEPORT | P3a, P3b, P1 |
| Metrics beyond RED and the slow ring; traces; metering; eBPF/TCP_INFO | P4 |
| Tiering and RestoreObject | P6 |
| Profiling and tuning; the 1 GiB bench tier runs at 1 connection only | P7 |
| Packaging, doctor, chaos, nightly fuzz, release | P8a, P8b |
| Cross-cluster copy answers 501; bucket logging, replication, inventory, analytics and notification answer 501; bucket policy ARNs pass through unrewritten | Undecided (DESIGN §9 item 14) |
| Versioned buckets cannot migrate | v2 (DESIGN §3) |
| `xmlrw.Editor` and `chunked.Trailer` are interfaces with one production implementation each (G1) | Decision pending |
