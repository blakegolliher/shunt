# ADR-0004: The races in a live migration, and the window on each

Status: accepted (POC-4). Source: docs/DESIGN.md §2.5, §9 items 3 and 4; docs/POC.md POC-4.

## Context

A bucket moves from one cluster to another while clients keep reading and writing it, and without changing anything on the client. shunt shifts writes first (`RAMPING`), then serves reads from both while a mover backfills (`MIGRATING`), then cuts over. No per-object state exists anywhere (CLAUDE.md), so every guarantee has to come from the order of operations and from what the backends themselves enforce.

This ADR lists what can go wrong, what closes it, and for what cannot be closed, how wide the window is. A migration is only as safe as the widest window here.

## The races

### 1. Delete/copy: the mover resurrects a deleted object

A client deletes key K. The mover, mid-copy of K from the source, finishes writing it to the target. The object comes back from the dead.

**Closed by:** `DELETE` goes to **both** clusters in `RAMPING` and `MIGRATING` (§2.5), and the mover re-`HEAD`s the **source** after each copy: if K vanished from the source while the copy was in flight, the mover deletes its own copy on the target.

**Residual window:** between the mover's re-`HEAD` and its compensating delete. A client that re-creates K in that sliver has its new object deleted by the mover. The window is one round trip, and it needs the client to delete and re-create the same key within it. Metered as `shunt_migration_dual_delete_total{outcome}`.

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

The mover always uploads the parts first and issues the guard `HEAD` immediately before `Complete`. A client write to the same key inside that window is lost. It is reported: the mover counts `guarded` copies separately from `conditional` ones, and the run summary states which mode it used.

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

- Every window above is a function of one round trip, except the ETag question, which is a property of the data rather than of timing.
- A bucket with no concurrent overwrites migrates with no exposure at all, in either direction.
- The mover is a test fixture (`test/mover`), not the product. It exists to prove the contract; a production mover has the same obligations, listed in §2.5.
- `capabilities.conditional_write` is measured, not assumed: re-run `shunt probe` after a backend upgrade, because a backend gaining the header silently upgrades the guarantee, and losing it silently downgrades it.
