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
- `shunt_migration_dual_delete_total{outcome}` — `both`, `source_missing`, `source_failed`.
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

- **It never overwrites a newer client write.** Where the target honors `If-None-Match: *` it uses
  it; where it does not — Garage 2.3.0 — it falls back to a HEAD-then-commit guard, taken from the
  cluster's `capabilities.conditional_write`. For multipart objects the guard `HEAD` is issued
  immediately before `CompleteMultipartUpload`, so the window is one round trip whatever the object
  weighs.
- **It never resurrects a deleted object.** After each copy it re-`HEAD`s the source; if the object
  vanished mid-copy it removes its own copy.
- **ETags survive.** A multipart object is re-uploaded with the source's own part layout, read back
  with `HEAD partNumber=N`. The ledger flags any object whose ETag changed, and the run exits
  non-zero if one did.
- **It resumes.** A cursor file per placement in `-state-dir`; a completed pass clears it, so a
  re-run is a full pass and not a continuation from the last key.
- **It leaves a record.** One JSONL line per object — size, both ETags, part count, which guard,
  the outcome — in `-state-dir`, optionally uploaded to a bucket with `-ledger-bucket`.

Run it more than once. The last pass should copy nothing.

## Removing a vendor entirely

The same steps, once, for every bucket a cluster still holds:

```sh
shunt migrate start --from minio --to garage --create   # every bucket on minio starts moving
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
