# ADR-0004: The races in a live migration, and the window on each

Status: accepted (POC-4). Source: docs/DESIGN.md §2.5, §9 items 3 and 4; docs/POC.md POC-4.

## Context

A bucket moves from one cluster to another while clients keep reading and writing it, and without changing anything on the client. shunt shifts writes first (`RAMPING`), then serves reads from both while a mover backfills (`MIGRATING`), then cuts over. No per-object state exists anywhere (CLAUDE.md), so every guarantee has to come from the order of operations and from what the backends themselves enforce.

This ADR lists what can go wrong, what closes it, and for what cannot be closed, how wide the window is. A migration is only as safe as the widest window here.

## The races

### 1. Delete/copy: the mover resurrects a deleted object

A client deletes key K. The mover, mid-copy of K from the source, finishes writing it to the target. The object comes back from the dead.

**Closed by three things together:**

1. `DELETE` goes to **both** clusters in `RAMPING` and `MIGRATING` (§2.5), **source first, then primary** (`internal/proxy/handler.go`).
2. After each copy, the mover re-`HEAD`s the **source**. If K has gone from the source while the copy was in flight, the copy is a resurrection and the mover withdraws it.
3. The mover withdraws **only its own copy**, never a newer client write. With `capabilities.conditional_delete` it sends `DELETE` with `If-Match` on the ETag its `PUT` returned. Without it, it `HEAD`s the target and deletes only if the ETag is its `PUT`'s and `Last-Modified` is no later than the `Date` of that `PUT`'s response.

**Why the order of the dual delete matters (found 2026-09-16, fixed the same day).** Until then shunt deleted on the primary first. The mover's re-`HEAD` only catches a delete that has already reached the source, so this five-step interleaving left a copy on the primary for good:

1. the mover reads K from the source;
2. the client's `DELETE` removes K from the primary;
3. the mover's `PUT` lands on the now-empty primary (`If-None-Match: *` passes, since nothing is there);
4. the mover re-`HEAD`s the source: K is still there, so the copy stands;
5. shunt's second delete removes K from the source, and the client gets 204.

No later pass removed it, since the source no longer held K, and it survived `CUTOVER`. With the source deleted first, a copy that lands between the two legs finds the source gone at step 4 and is withdrawn. A copy that lands before the source leg is removed by the primary leg that follows it. `TestDeleteRacingAMoverCopyStaysDeleted` drives the five steps between shunt's two legs, and fails against the old order.

**Why the withdrawal is conditional.** An unconditional delete of "our copy" also removes whatever a client wrote to K after the copy landed. `TestMoverWithdrawLeavesANewerClientWrite` puts a client write between the mover's `PUT` and its withdrawal, and fails against an unconditional withdrawal.

**Remaining windows.**

| Window | When | Width | Effect |
|---|---|---|---|
| A. Withdrawal by re-`HEAD` (every backend measured so far) | a client writes K between the mover's `HEAD` of the target and its unconditional `DELETE` | one round trip | the client's write is deleted; the client already has 200 |
| B. Identical bytes in the same second | a client re-creates K with the source's exact bytes (same ETag) within the second of the mover's `PUT`, by the backend's one-second `Last-Modified` clock | one second, identical content only | the mover takes the client's object for its own and deletes it. With `If-Match`, same ETag means the same bytes, so the client loses only a write identical to the object it had just deleted |
| C. Partial dual delete | the source leg succeeds and the primary leg fails | until the client retries | the client sees an error; K is gone from the source but still on the primary. Metered as `shunt_migration_dual_delete_total{outcome="primary_failed"}` and logged; docs/migrating.md |
| D. Source leg fails | the source leg fails and the primary leg succeeds | until the mover's next pass | the client sees 204, but the source still holds K and the next pass copies it back. `outcome="source_failed"` |

`capabilities.conditional_delete` defaults to **false**. Garage 2.3.0 and MinIO RELEASE.2025-07-23 both ignore `If-Match` on `DELETE` and delete on a mismatch (`shunt probe`, 2026-09-16), and a backend that ignores it while the mover relies on it deletes the newer write every time. So window A applies to every migration on the backends measured so far.

### 2. Overwrite: the mover clobbers a newer client write

At ratio 1 every client write goes to the target and the source is frozen. The mover must copy only keys the target does not already have, or it overwrites a newer object with an older one.

**Closed by `If-None-Match: *`** on the mover's PUT, where the target honors it. The write fails with 412 and the mover moves on.

**Not closed on a target that ignores the header.** Garage 2.3.0 accepts the PUT and returns 200 (measured, docs/reference/backend-compat.md). shunt therefore carries `capabilities.conditional_write` per cluster, filled from `shunt probe`, and the mover consults it:

- `conditional_write: true` → `If-None-Match: *`, no window.
- `conditional_write: false` → `HEAD` the target first and skip if present, which leaves a window between that `HEAD` and the moment the object becomes visible on the target.

The fallback's window is kept small on purpose:

| Copy shape | Window | Why |
|---|---|---|
| Single PUT (small object) | one `HEAD` + one `PUT` round trip | nothing shortens it |
| Multipart (large object) | `HEAD` → `CompleteMultipartUpload` | parts are staged first and the object appears only at Complete, so the window does not grow with object size |

The mover always uploads the parts first and issues the guard `HEAD` immediately before `Complete`. A client write to the same key inside that window is lost: it is overwritten with the source's older bytes, the client's `PUT` has already returned 200, and nothing reports the loss. **The mover contract therefore degrades on a target without conditional `PUT`** from "never overwrites a client write" to "never overwrites a client write older than one round trip". Neither the property test nor any e2e run has measured this window yet (see the property failure below). It is reported: the mover counts `guarded` copies separately from `conditional` ones, and the run summary states which mode it used.

**Operational advice:** migrating *into* a backend without conditional writes is safe for a bucket nobody is overwriting during the run, and carries the window above for one that is. Migrating *out of* such a backend is unaffected, since the source is only read.

### 3. ETag drift: the copy has a different ETag

A multipart object's ETag is a digest of its part digests plus the part count. Copy it with different part boundaries and the ETag changes, even though the bytes are identical. Across backend types this is the normal case, because default part sizes differ.

**Mitigated by:** the mover reproduces the source's part layout. An object whose ETag is a multipart ETag (`…-N`) is copied as a multipart upload with the same part boundaries, read from the source with ranged GETs.

**Not closed in general.** A backend may compute a multipart ETag differently, and a client holding a cached ETag for `If-Match` will see a 412 after cutover. shunt cannot mask this: rewriting ETags would be per-object state. Every migration runbook says so, and this is why the property test asserts the pre-migration ETag survives only when the layout is reproducible.

### 4. Versioned buckets

Version ids are minted by the backend and cannot be carried across. A migrated bucket would silently lose its version history.

**Closed by refusal.** `shunt directory set-state` reads `GetBucketVersioning` on both the source and the target before entering `RAMPING` or `MIGRATING` and refuses if either is `Enabled` *or* `Suspended`, since a suspended bucket still holds versions. It fails closed: an error reading the status is also a refusal. The proxy additionally refuses `PutBucketVersioning` on a non-ACTIVE placement, so a bucket cannot become versioned mid-migration.

### 5. A ramp that goes backwards

The ramp decides, per key, which side a write goes to. Shrink the ratio or remove a prefix and keys that were being written to the target start being written to the source again, while their newest copy sits on the target. Reads then return the stale copy.

**Closed by monotonicity.** A ramp update is accepted only if the ratio does not decrease and the prefix set only grows. Shrinking requires a documented target→source reconcile first and is not a state transition; POC-4 has no `--force-shrink` path at all.

### 6. Reads during the backfill

A key already written to the target reads from the target. A key not yet copied is absent there.

**Closed by fallback.** In `MIGRATING`, `GET`/`HEAD` try the primary and fall back to the source on 404, counted by `shunt_migration_fallback_reads_total`. Convergence is visible as that counter flattening. `CUTOVER` drops the fallback, which is why it is a separate, operator-driven state: entering it early turns a not-yet-copied object into a 404.

### 7. Listings during the backfill

Neither cluster has the whole bucket.

**Closed by merging.** `ListObjectsV2` is served as a sorted merge of both clusters' streams, primary winning on collision, with a composite continuation token. Memory is bounded by the page size, not by the bucket. ListObjects v1 and ListMultipartUploads are not merged in POC-4 (docs/POC.md cut) and route to the primary, so a v1 lister can see a partial bucket mid-migration.

## Consequences

- Every window above is a function of one round trip, except three: the ETag question, which is a property of the data rather than of timing; race 1's identical-bytes window, which is one second of the backend's clock; and race 1's partial dual delete, which lasts until the client retries.
- A bucket with no concurrent overwrites migrates with no exposure at all, in either direction.
- The mover was a test fixture (`test/mover`) through POC-4. Since POC-5 it is `shunt migrate run`, with the same contract and refusals (ADR-0009); any other mover has the same obligations, listed in §2.5.
- `capabilities.conditional_write` and `capabilities.conditional_delete` are measured, not assumed: re-run `shunt probe` after a backend upgrade, because a backend gaining the header silently upgrades the guarantee, and losing it silently downgrades it.

## The POC-4 property failure (15 violations in 1,857,740 operations)

**Investigated 2026-09-16.** The detail of the original 10-minute failure was lost, so the test now writes its own record (`test/property/runs/<timestamp>/`, see `internal/proxy/property_record_test.go`). It was re-run for 60 minutes on the same topology: seed 0 (the original run's workers), a fake Garage source, and a fake MinIO target that honors `If-None-Match: *`. The run was stopped at 58 min 10 s, when the host ran low on memory, after every operator transition had happened.

**Observed:** 10 violations, all `get-transport`: a GET through shunt that the client saw end in `EOF` with no response. Each one ended a **harness stall**, an interval in which neither the clients nor either fake backend committed anything: 3.35 s (4 violations), 1.76 s just after a 5.57 s stall (4), and 62.4 s (2). Every write and delete in flight across a stall succeeded. The GETs were cut off by shunt's idle-progress watchdog (2 s in the test rig, ADR-0003), which is the designed behavior: an error, never a silent short read.

**Cause of the stalls: the test harness, not shunt.** A heap profile of a 3-minute run with the same seed puts 99% of retained memory in test fixtures. 1 GB is the rig's in-memory access log, a `bytes.Buffer` behind a mutex that every request's `finish` writes to, so the whole proxy waits while it grows. Most of the rest is the fake backends keeping every request's headers (`fakeS3.headers`, `fakeS3.seen`). shunt itself holds no per-object state. The 60-minute run reached 48 GB RSS.

**Correlation with the non-conditional copy path (the first hypothesis): none, and there could be none.** In this topology the target honors `If-None-Match: *` and the test's mover always sends it, so the HEAD-then-commit path is never taken. The 60-minute run made 0 guarded copies, and none of the 10 violating keys had a mover copy at all.

**Correlation with the delete/copy window (race 1): none, because the mover copied nothing.** Both passes read all 120 keys from the source and found every one absent. By `MIGRATING` the clients had already emptied the source: deletes go to both clusters during `RAMPING`, and at ratio 1 every write goes to the new primary. With about five deletes a second per key, no key keeps its source copy for more than a few seconds. The original 10-minute run, with a 75-second ratio-1 phase, had the same shape.

**Conclusion.** The rerun's violations come from the harness, not from a migration race. The original 15 match that class in rate and shape, since that run passed the same access-log growth, but their detail is gone and this cannot be proven. Just as important: **at soak durations the property test does not test the mover's copy path.** Its clean runs are no evidence for races 1 and 2. The harness needs bounded fixture memory, and keys the clients leave alone during the ramp, before its runs mean anything for the mover.

**Follow-up, 2026-09-16.** Four changes to the harness. It keeps no per-request state (access log discarded, no request history in the fake backends). 120 kept keys stay on the source until the mover has had them. While the mover copies, the clients overwrite and delete the kept key it is on, so both races happen on purpose. A GET that ends in EOF across a stall of more than 1 second (no event from any party) is tagged `harness` in violations.jsonl and still counts. With the race 1 fix above, two 60-minute runs:

- **MinIO-like target** (`If-None-Match: *` honored, `If-Match` on DELETE ignored), run `20260916T143028Z`: 29,330,793 operations, **0 violations**, 0 stalls, peak RSS about 80 MB. In pass 1 over the kept keys, 29 PUTs were refused with 412 and 11 re-HEADs found the source gone: 7 own copies withdrawn after the re-HEAD matched, 4 already removed by the primary leg of the delete.
- **Garage-like target** (both ignored), run `20260916T153050Z`: 17,326,931 operations, 0 stalls, and **2 distinct writes lost in race 2's window**. On `obj/0157`, in the target's commit order: the mover's HEAD finds the key absent; 0.25 ms later a client's PUT commits and returns 200; 0.53 ms after that the mover's PUT of the older source bytes commits over it. All 15,483 violations are reads of that one loss, repeated until the kept key was released after pass 2. On `obj/0211` the same happened with gaps of 0.98 ms and 0.60 ms, but a later client write replaced the stale bytes before any read, so only the commit history shows it. That is 2 losses in 62 guarded copies aimed at contended keys. In the same pass 9 re-HEADs found the source gone: 7 own copies withdrawn, 2 already gone.

**How the guarded variant is judged.** The property test classifies each violation from the target's commit stream. A client PUT that commits on a key between the mover's HEAD and the mover's PUT of that key is a loss in this window. A violation is a known-window read only when the model holds exactly that lost client body and the read returned exactly the mover's bytes. The guarded variant reports distinct lost writes as its headline and passes only if every violation is such a read. The conditional variant fails on any violation. A replay of `20260916T153050Z` classified 15,483 of 15,483 violations as known-window reads, 0 outside. `shunt migrate start` refuses a target whose `conditional_write` is false without `--accept-lost-write-window`.

Neither run reached window A, B or C of race 1: no withdrawal found a newer client write on the target, and no primary leg failed. Those windows are covered by `TestMoverWithdrawLeavesANewerClientWrite` and `TestDualDeletePrimaryFailure`, not measured under load.

## Amendment (POC-5, 2026-09-16): cutover evidence, purging the source, and the ramp hash

**CUTOVER keeps the dual delete.** Through POC-4 a `CUTOVER` delete went to the primary only, so the source kept every key a client deleted after cutover. POC-5 deletes the source bucket, and before it does, it must show that the source holds nothing the primary lacks. With primary-only deletes that check fails for every post-cutover delete. A `CUTOVER` delete now goes to the source, then the primary, as in `MIGRATING`. Nothing else writes to the source after `MIGRATING` begins, so from then on the source only ever loses keys: **source ⊆ primary** holds from the mover's converged pass to the purge.

**`shunt cutover` records its evidence.** `MIGRATING → CUTOVER` is refused unless:
- the mover's latest report for the placement says a whole pass copied nothing and failed nothing;
- this proxy's `shunt_migration_fallback_reads_total` for the bucket did not move during `--window` (default 60 s). The call waits out the window.

The window, the time and the counter are written to the placement as `cutover:`. Two limits:
- Both signals are per proxy and in memory. The mover report is lost on a restart; run the mover again.
- A fleet must hold the window on every proxy until P4 aggregates the counter. This is stated in docs/migrating.md.

**`shunt purge-source` is the only path that deletes a source bucket.** It is refused unless:
- the placement is in `CUTOVER` with that evidence;
- a listing of both buckets, walked in step with memory bounded by one page per side (ADR-0007), finds no source key the primary lacks. The first 20 such keys are returned with the refusal.

  A key the listings disagree on counts only once HEADs confirm it: the primary answers 404, then the source answers 200. The listings are read a page at a time, so a client delete landing between a source page and the matching primary page would otherwise look like a missing key (seen once under warp load, docs/bench/poc5-load.md). The order is what makes the check exact: a delete reaches the source before the primary (race 1) and nothing writes to the source after `MIGRATING`, so a key still on the source after the primary's 404 was absent from the primary at that moment. Amended 2026-09-17.

It then aborts the source's in-progress multipart uploads, deletes every object and the bucket, and applies `CUTOVER → ACTIVE`, which drops the source. A client write cannot land on the source during the purge: no state after `MIGRATING` routes a write there.

**Conditional writes are judged across both clusters (ADR-0013, POC-6).** A backend judges `If-None-Match` and `If-Match` against the copy it holds, and mid-migration the key may be on the other cluster: create-once answered 200 and made a second copy, update-if-current answered 412 over a current version. shunt now checks the other cluster before the write and converts or refuses accordingly, failing closed on 503.

**Race 5, again: the ramp hash changed.** The POC-5 walkthrough measured an 80/20 write split at a ratio of 0.5. `InRange` used FNV-1a-64 directly, and FNV-1a barely moves its high bits for keys that differ only in their last bytes. `a/0000` … `a/0999` all fell under 0.5. The hash is now FNV-1a finished with murmur3's `fmix64`. A test holds sequential key shapes within ±4 % of 0.1, 0.5 and 0.9.

Changing the function changes which keys are in range. A proxy that routed a running ramp with a different function than the one that started it would move keys back to the source, which is race 5's stale read, and two builds in one fleet would disagree about the same key. **So a ramp names its hash.** RAMPING start writes `ramp.hash: fnv1a-fmix64-v1` onto the placement, and the name stays for the life of the ramp. The name is a promise about exact values: `TestRampHashIsPinned` holds five keys to their hash outputs, so any change to the function needs a new name.

- **On load**, a `RAMPING` placement must carry a well-formed `ramp.hash`. A name this build does not implement still loads, so one placement written by a newer build does not stop the whole directory, and `serve` logs an error naming the placement and both hashes.
- **On the request path**, a write or read whose side the hash would decide is refused with 503 `ServiceUnavailable`, logged with the hash name, rather than routed by another function. Requests the hash does not decide still work: a key a prefix already moved, a ratio of 0 or 1, deletes, listings, bucket configuration and uploads pinned by their id.
- **On the control path**, extending a ramp split by an unknown hash is refused. Moving it on to `MIGRATING` is allowed, since every write then goes to the primary whatever the hash says.

A fleet mid-upgrade therefore never re-splits a ramp: an old proxy seeing a new hash refuses, and a new proxy seeing the old name routes by the old function or refuses. This replaces the earlier rule that proxies be upgraded only with no placement in `RAMPING`.

