# Moving a bucket to another cluster, and removing a vendor

This is the POC-4 how-to: how to move a bucket's data from one S3 cluster to another while clients
keep using it, and how to empty and retire a cluster entirely. Clients are not reconfigured, not
restarted, and not told. They keep the same endpoint, the same credentials and the same bucket name
throughout.

Everything below is one `shunt` binary plus the mover fixture in `test/mover`. The design is
docs/DESIGN.md §2.5; the races and their windows are ADR-0004; what each backend actually does is
docs/reference/backend-compat.md.

## What the states mean

A placement moves `ACTIVE → RAMPING → MIGRATING → CUTOVER → ACTIVE`. `RAMPING` may be skipped.

| State | Writes | Reads | Deletes | Listings |
|---|---|---|---|---|
| `ACTIVE` | primary | primary | primary | primary |
| `RAMPING` | split by key: hash ratio or prefix | the side that key's rule chose | both | primary |
| `MIGRATING` | new primary | new primary, falling back to the source on 404 | both | merged from both |
| `CUTOVER` | new primary | new primary | new primary | new primary |
| `ACTIVE` (after) | new primary | new primary | new primary | new primary |

The ramp only grows. Shrinking is a reconcile, not a state change, so `--ratio` below the current
one is refused: a key that has already moved must not start being written on the old cluster again.

## Moving one bucket

```sh
shunt migrate status                      # what is moving, and where
shunt ramp acme/data --to garage --create --ratio 0.01
shunt ramp acme/data --ratio 0.25
shunt ramp acme/data --ratio 1
shunt migrate start acme/data             # all writes on the new cluster; reads still fall back
go run ./test/mover -config /etc/shunt/shunt.yaml -bucket acme/data
shunt cutover acme/data                   # stop reading the old cluster
shunt migrate finish acme/data            # forget it
```

`--create` makes the backend bucket on the target; without it the bucket must already exist, so a
typo is a refusal rather than a slow trickle of 404s. `--name` overrides the generated backend name.

**Watch, don't guess.** On the metrics listener:

- `shunt_ramp_writes_total{side}` — writes landing on each side during the ramp. If `primary` is
  not climbing, the ramp is not doing anything.
- `shunt_migration_fallback_reads_total` — reads the primary could not serve, answered from the
  source. This is the "how much is left" signal: **cut over when it stops increasing** under real
  read traffic, not when the mover prints a total.
- `shunt_migration_dual_delete_total{outcome}` — `both`, `source_missing`, `source_failed`,
  `primary_failed`. A migration delete goes to the source first, then the primary. Alert on
  `source_failed` (the mover will copy the object back) and `primary_failed` (see "A delete can
  half-succeed" below).
- `shunt_listing_merge_seconds` — what merged listings cost while `MIGRATING`.
- `shunt_route_state{bucket,state}` and `shunt_ramp_ratio{bucket}` — where each bucket is.

Every state change is appended to `<directory file>.changes.jsonl` with the actor, the timestamp and
the before and after placement.

## The mover

The mover copies what the source still holds to the new primary. It is safe to run only in
`MIGRATING`, or in `RAMPING` at ratio 1 — anywhere else some writes are still going to the source
and a copied key could go stale. It refuses to start otherwise.

```sh
go run ./test/mover -config /etc/shunt/shunt.yaml -bucket acme/data   # one placement
go run ./test/mover -config /etc/shunt/shunt.yaml -from minio         # everything moving off minio
go run ./test/mover -config … -bucket acme/data -dry-run              # list, do not copy
```

What it guarantees, and how (ADR-0004):

- **It does not overwrite a newer client write.** Where the target honors `If-None-Match: *` it
  uses it and there is no window. Where it does not (Garage 2.3.0), it falls back to a
  HEAD-then-commit guard, taken from the cluster's `capabilities.conditional_write`, which has a
  window: see the section below. For multipart objects the guard `HEAD` is issued immediately
  before `CompleteMultipartUpload`, so the window is one round trip whatever the object weighs.
- **It does not resurrect a deleted object, and it removes only its own copy.** shunt sends a
  migration delete to the source first and then the primary. After each copy the mover re-`HEAD`s
  the source, and if the object has gone it withdraws its copy. Where the target honors `If-Match`
  on `DELETE` (`capabilities.conditional_delete`), the withdrawal is conditional on the ETag of the
  mover's own `PUT`. Otherwise the mover `HEAD`s the target first and deletes only if the ETag and
  `Last-Modified` still say it is its copy. Neither Garage 2.3.0 nor MinIO honors `If-Match` on
  `DELETE`, so today every migration uses the `HEAD`: a client write landing between that `HEAD` and
  the `DELETE`, one round trip, is lost (ADR-0004 race 1, window A).
- **ETags survive.** A multipart object is re-uploaded with the source's own part layout, read back
  with `HEAD partNumber=N`. The ledger flags any object whose ETag changed, and the run exits
  non-zero if one did.
- **It resumes.** A cursor file per placement in `-state-dir`; a completed pass clears it, so a
  re-run is a full pass and not a continuation from the last key.
- **It leaves a record.** One JSONL line per object — size, both ETags, part count, which guard,
  the outcome — in `-state-dir`, optionally uploaded to a bucket with `-ledger-bucket`.

Run it more than once. The last pass should copy nothing.

### Into a cluster without conditional writes, the mover can lose a client write

If the target cluster's `capabilities.conditional_write` is `false` (Garage 2.3.0 today; re-run
`shunt probe` to check yours), the mover cannot ask the backend to refuse an overwrite. It HEADs
the target, and if the key is absent it writes the source's copy. **A client write to the same key
between that HEAD and the mover's PUT (or `CompleteMultipartUpload`) is overwritten with the older
source bytes.** The client already got 200 for its write, and nothing reports the loss. The window is
one round trip per object, repeated for every object the mover copies.

`shunt migrate start` refuses such a target, naming the window and this section, unless it is given
`--accept-lost-write-window`. The flag changes nothing about the mover. It is the
operator saying the next point has been dealt with. The refusal is keyed on the profile, so a
cluster whose `conditional_write` is unset is taken at its default, true: fill the profile from
`shunt probe`. It guards `migrate start` only. A mover run while the placement is `RAMPING` at ratio 1
has the same window and no such check.

What to do:

- **Quiesce writers** to the bucket for as long as the mover runs, or
- **ramp to ratio 1 and run the mover when write volume is low**, so few writes can land inside a
  window, and
- check the mover's run summary: it names the guard it used (`HEAD-then-commit` versus
  `If-None-Match`).

Migrating *out of* such a cluster is unaffected: the source is only read.

The same advice covers the mover's withdrawal on a target without `conditional_delete`, which is
every backend measured so far. It only matters for keys that clients delete and then write again
while the mover is copying them.

### A delete can half-succeed

A delete during `RAMPING` or `MIGRATING` goes to the source first and the primary second. If the
source accepts it and the primary refuses it (5xx, timeout), **the client gets the primary's error,
the object is gone from the source, and it is still on the primary**. Reads still return it, since
the primary answers first. This is visible as
`shunt_migration_dual_delete_total{outcome="primary_failed"}` and an error log line naming the
bucket and request id. What to do: nothing if clients retry failed deletes, as every S3 SDK does,
because the retry removes it from the primary. Otherwise, re-issue the delete for the logged request
before `CUTOVER`.

## Removing a vendor entirely

The same steps, once, for every bucket a cluster still holds:

```sh
shunt migrate start --from minio --to garage --create   # refused: garage ignores If-None-Match: * (see below)
shunt migrate start --from minio --to garage --create --accept-lost-write-window   # writers quiesced
go run ./test/mover -config /etc/shunt/shunt.yaml -from minio
shunt migrate status                                    # fallback reads flat? then:
shunt cutover --from minio
shunt migrate finish --from minio
```

`migrate finish --from` reports what still points at the cluster. Moving every bucket is not the
same as being rid of it: a tenant whose `default_cluster` is still the old cluster would put its
next new bucket back on the machine you are about to switch off. Repoint those tenants in the
directory file, then delete the cluster's block from the config and run `shunt check-config` — it
fails if any placement still names a cluster that is gone.

## What it will refuse, and why

- **A versioned bucket.** If `GetBucketVersioning` is anything but empty on either side, entering
  `RAMPING` or `MIGRATING` is refused: version history is not carried and would be silently lost.
  The check fails closed — an unreadable bucket is also a refusal.
- **A destination change mid-move.** `--to` may be repeated on a later ramp step, but only naming
  the same target.
- **`DeleteBucket` and `PutBucketVersioning`** while the bucket is not `ACTIVE`: 409.
- **A cross-cluster `x-amz-copy-source`** — a client copying between two of its own buckets that
  now live on different clusters gets a hard failure (docs/DESIGN.md §9). It is a carried gap, not
  a migration bug: it can appear as soon as one tenant spans two clusters.

## Seeing it end to end

`test/e2e/demo.sh` does the whole thing against the e2e Garage and MinIO with nothing but aws-cli:
1,000 objects including multipart ones, a writer that never stops, the ramp, the mover, the cutover,
then every object verified by GET and ETag with an empty listing diff, the change log, the ledger,
and the latency the client saw before, during and after.

```sh
make e2e-up
make run-mixed                              # in another terminal
test/e2e/demo.sh --from minio --to garage   # or --from garage --to minio
```
