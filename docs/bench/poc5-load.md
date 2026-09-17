# POC-5 under load: the whole migration demo inside a warp run

Date: 2026-09-17.

**Question:** what does a client see, in errors and latency, when a bucket is moved between clusters through shunt while it is under steady mixed load?

**Method:** warp's `mixed` benchmark (v1.7.0, built from source and run as a separate tool; nothing of it is in this repo) ran through shunt for 15 minutes. The README demo was driven inside that run:
1. `cluster add`, `expand`;
2. `ramp 0.5`, then `ramp 1.0`;
3. `migrate start`, `migrate run --until-converged`;
4. `cutover --window 60s`, `purge-source`;
5. `tenant set-default`, `cluster remove`.

Every step was timestamped, and latency and errors were cut into 30-second segments from warp's `--full` data. Each run also has 2-minute direct baselines and a 2-minute run through shunt with the bucket `ACTIVE`. shunt ran in lab form (plaintext listener, resign mode) from commit `1f1c6a6`.

The scripts and the segment tool (`warp-segments.py`, `phase-latency.py`) are not committed; the tables below are their output.

---

## 1. VAST → VAST: the result that counts

**Setup:**
- **Clusters:** vast01 (VAST 5.5.0.1) → vast02 (VAST 5.4.6.0), both over http on port 80.
- **Host:** shunt and warp on the dev box (AMD EPYC 7402P, 16 vCPUs, kernel 5.14.0-570.17.1.el9_6, go1.27.1).
- **Load:** 16 clients, 1 MiB objects, 1,000 prepared, warp's default mix (45% GET, 30% STAT, 15% PUT, 10% DELETE), **rate-limited to 160 requests/s (96 MiB/s)** because both clusters were shared with other work. Uncapped, the same load reached 1.1–1.2 GiB/s direct to either cluster.

### Errors: none

**0 errors in 144,016 requests** over the 15-minute run through shunt, and 0 in both baselines and the `ACTIVE` run. Every demo step succeeded on its first attempt.

| Time | Step | Result |
|---|---|---|
| 06:54:38 | load running, `ACTIVE` on vast01 | |
| 06:55:38 | add vast02, expand | conditional writes measured: PUT yes, DELETE yes |
| 06:56:43 | ramp 0.5 | |
| 06:58:13 | ramp 1.0 | |
| 06:59:13–06:59:59 | migrate start, then the mover | 1,280 objects (1.2 GiB) copied, 93 vanished mid-copy (deleted by warp), then a pass copying 0; If-Match withdrawal |
| 06:59:59–07:00:59 | cutover, 60 s window | passed |
| 07:00:59 | purge-source | listing diff empty; 827 objects and the bucket deleted |
| 07:01:06 | set-default, remove vast01 | |

### Latency added by shunt

Direct baselines and the `ACTIVE` run through shunt, p50 / p99 in ms:

| | GET | PUT | STAT | DELETE |
|---|---|---|---|---|
| vast01 direct | 5.7 / 15.5 | 14.9 / 50.9 | 3.1 / 8.4 | 6.3 / 22.8 |
| vast02 direct | 4.8 / 13.2 | 12.6 / 33.4 | 2.5 / 6.2 | 4.9 / 9.9 |
| through shunt, `ACTIVE` on vast01 | 7.6 / 18.3 | 12.5 / 32.1 | 4.7 / 11.4 | 8.6 / 23.0 |
| **added vs vast01 direct** | **+1.9 / +2.8** | none measurable | **+1.6 / +3.0** | **+2.3 / +0.2** |

Through shunt, by migration phase, p50 / p99 in ms. **Client** is warp's view; **shunt** is shunt's own access-log `duration_ms` for the same requests:

| Phase | GET client | GET shunt | PUT client | PUT shunt | STAT client | STAT shunt | DELETE client | DELETE shunt |
|---|---|---|---|---|---|---|---|---|
| ACTIVE on vast01 | 7.5 / 18.1 | 6.4 / 16.0 | 12.1 / 29.2 | 10.6 / 27.2 | 4.7 / 12.0 | 3.2 / 9.0 | 8.0 / 24.3 | 6.6 / 22.9 |
| ramp 0.5 | 8.2 / 21.7 | 7.1 / 19.5 | 10.9 / 31.6 | 9.4 / 30.5 | 4.8 / 14.4 | 3.4 / 12.2 | 10.0 / 34.4 | 8.7 / 30.7 |
| ramp 1.0 | 8.9 / 23.6 | 7.8 / 22.2 | 10.3 / 21.6 | 8.9 / 18.6 | 5.7 / 17.1 | 4.4 / 15.3 | 10.6 / 41.8 | 9.2 / 38.8 |
| MIGRATING (mover running) | 8.5 / 24.8 | 7.2 / 23.0 | 11.4 / 25.9 | 9.8 / 22.9 | 4.8 / 15.8 | 3.3 / 12.5 | 11.9 / 37.6 | 10.2 / 36.6 |
| CUTOVER window | 7.1 / 17.2 | 6.0 / 14.9 | 10.7 / 23.6 | 9.2 / 21.8 | 4.2 / 10.8 | 2.8 / 7.8 | 10.5 / 24.4 | 9.0 / 21.0 |
| after, vast02 only | 6.9 / 16.6 | 5.8 / 14.1 | 10.5 / 24.9 | 9.0 / 22.6 | 4.0 / 10.3 | 2.6 / 7.4 | 6.0 / 15.2 | 4.6 / 13.3 |

**Reading the tables:**
- **Steady-state cost:** shunt adds **about 1.5–2.5 ms at p50 and about 3 ms at p99** on requests without a large body. PUT shows no measurable penalty.
- **Costs of the move itself:**
  - **Deletes during the move:** from `ramp 0.5` until `purge-source`, a DELETE goes to both clusters (source first, ADR-0004 race 1). Its p50 rises from 6–8 ms to 10–12 ms and its p99 to 34–42 ms.
  - **Reads while ramping:** reads rise by about 1 ms at p50 and 4–6 ms at p99, from fallback reads to the source and listings merged from two clusters.
  - **The mover:** copying 1.2 GiB in 46 s did not raise latency beyond that.
- **The ~1.1–1.5 ms before shunt's timer:** client latency is consistently above shunt's own duration by this much, for every operation, STAT included. It is not connection setup, since connections were reused (165,320 requests over 53 connections). It is time before shunt's `duration_ms` starts: the client hop, request-header reads, and scheduling on a host that also runs warp. This is not yet broken down.

### Where shunt spends CPU

Two 30-second CPU profiles (`/debug/pprof/profile` on the admin listener), taken during `ACTIVE` and during MIGRATING with the mover running. shunt used **0.43 and 0.48 of one core** at 160 requests/s (96 MiB/s). Cumulative shares:

| Share | Where |
|---|---|
| 46–49% | body relay, `io.copyBuffer` (includes the syscalls below) |
| 28–30% | `syscall.write` to client and backend sockets |
| 19–20% | `syscall.read` from them |
| **17%** | **`bufio.Writer.Write` on the client response path**, via `proxy.deadlineWriter` |
| 9–10% | aws-chunked per-chunk signature verification (`chunked.ChunkReader`): warp signs streaming PUTs over http |
| 4% | `proxy.prepareResign`: credential lookup, directory, routing |
| **4%** | **access log, one synchronous JSON write per request** |
| 3% | client signature verification (`sigv4.Verify`) |
| **1.5%** | **the SigV4 signing key derived again on every request** (`sigv4.SigningKey`, four HMACs) |
| 1–1.4% | upstream signing (`sigv4.Sign`) |

### Latency improvement candidates, in order of expected payoff

1. **The client write path.** About 17% of CPU goes through `bufio.Writer.Write` behind `deadlineWriter`, and all response bytes cross a user-space copy. Measure whether a larger response write size, or letting the response writer's `ReadFrom` take the copy, cuts syscalls per MiB.
2. **Dual-delete cost during a move.** DELETE p50 +4 ms and p99 +20 ms while two clusters are involved. The source-first order is what closes ADR-0004 race 1, so the legs cannot simply run in parallel. Any change needs that analysis redone first.
3. **The ~1.3 ms before shunt's timer.** Add a timer from the first byte read, or from accept, so this is measured, not inferred.
4. **Synchronous access log (4%).** Buffered, asynchronous writes with a bounded queue.
5. **Signing-key derivation (1.5%).** Cache per (secret, date, region, service); the key only changes daily.
6. **aws-chunked verification (9–10%).** Inherent to resign mode for streaming uploads, but the per-chunk string-to-sign and HMAC allocations are worth profiling.

---

## 2. MinIO → MinIO (local containers), 1 MiB objects

Two identical MinIO RELEASE.2025-07-23 containers on the dev box, same load uncapped (16 clients, 1 MiB).

- **Result:** **0 errors in 453,336 requests.** All steps succeeded; the mover copied 1,578 objects (1.5 GiB) in 3.5 min.
- **Latency:** shunt added about +6 ms p50 on GET/STAT/DELETE, and p99 roughly doubled while the mover ran. warp, shunt and both MinIOs shared 16 cores, so these numbers are CPU contention as much as shunt. The VAST run above is the representative one.
- **One false refusal:** the first `purge-source` was refused: "the listing diff is not empty … first 1: pSIWAbR4/337…". The retry 6 s later passed. warp deleted that key from both clusters between purge's reads of the source and target listing pages. Purge can only refuse wrongly this way, never delete wrongly. Re-checking each reported key on the source with a HEAD before refusing would remove the noise.

## 3. MinIO → MinIO, 40 MiB objects (multipart)

- **Setup:** same two containers. Objects were 40 MiB, so each PUT was a 3-part upload (ETags `…-3` on both clusters, part layout kept by the mover), with PUT and DELETE weighted equally.
- **Result:** **0 errors in 24,282 requests,** and a separate 2-minute `warp multipart` run with 5 MiB parts through shunt: 674 MiB/s, part upload p50 54 ms, 0 errors.
- **Latency:** shunt cost about 20–25% in throughput and latency at ~700 MiB/s on the shared host.
- **A stall:** for 3½ minutes right after `purge-source` and `cluster remove`, GET p99 reached 28 s. It recovered unaided, with no errors. It coincided with MinIO freeing gigabytes of deleted data on the same, nearly full disk (`/home`, 85%). Without `iostat` this is not proven.

## 4. Garage → MinIO (local containers), 1 MiB objects

**22,083 errors, from two causes that are not the migration mechanics:**

- **10,656 PUT errors, from 03:14:48:** "Storage backend has reached its minimum free drive threshold." MinIO's disk guard, two minutes after the migration had finished. Garage was still holding 27 GB of deleted data on the same disk.
- **11,427 GET and DELETE errors, starting exactly at `ramp 0.5`:** "Invalid version id specified". Garage returns an `x-amz-version-id` on every PUT into an unversioned bucket, and warp sends it back. Once those requests reach MinIO, MinIO rejects the id with a 400, so shunt does not fall back. See docs/reference/backend-compat.md. MinIO and VAST return no version ids, and the runs above had none of these errors.

Four PUTs through shunt to Garage took 41–60 s, while Garage was the primary. They were not reproduced against MinIO or VAST, and are unexplained.
