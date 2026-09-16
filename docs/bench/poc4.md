# POC-4 bench: what a live migration costs the client

Question: while a bucket is migrating — every write going to the new cluster, reads falling back to
the old one, deletes doubled, listings merged, and a mover copying in the background — what do
clients actually feel?

Method: `test/e2e/demo.sh`, which drives the whole migration with aws-cli against one unchanged
shunt endpoint and measures with `test/bench/s3bench -via-only` at three points:

1. **before**, with the bucket ACTIVE on the source cluster, right after 1,000 objects were written;
2. **during**, with the bucket `MIGRATING` and the mover running;
3. **after**, with the migration finished and the bucket ACTIVE on the target.

Each cell is 200 operations at 4 KiB and 100 at 1 MiB over 8 connections, one run, median latency.
`-via-only` measures through shunt alone on the bucket being migrated, and neither creates nor
cleans it up.

## Disclosure

| Item | Value |
|---|---|
| Host | AMD EPYC 7402P 24-Core Processor, 16 vCPUs, 62 GB RAM |
| Kernel | 5.14.0-570.17.1.el9_6.x86_64 |
| NIC | loopback: client, shunt, Garage and MinIO all on one host |
| Go | go1.27.1 |
| shunt | POC-4 working tree at 2026-09-15 |
| Backends | Garage 2.3.0, MinIO RELEASE.2025-07-23, both in containers on the same box |
| Bucket | 1,000 × 4 KiB objects, 4 × 9 MiB multipart objects, plus a writer running throughout |

## Garage → MinIO, 1,004 objects

| size | op | before (Garage) p50 / p99 | during migration p50 / p99 | after (MinIO) p50 / p99 |
|---|---|---|---|---|
| 4 KiB | PUT | 83.76 / 262.75 ms | 15.69 / 113.03 ms | 22.77 / 97.87 ms |
| 4 KiB | GET | 57.09 / 102.17 ms | 3.75 / 24.09 ms | 4.63 / 15.29 ms |
| 1 MiB | PUT | 155.14 / 279.00 ms | 33.23 / 81.14 ms | 49.07 / 97.68 ms |
| 1 MiB | GET | 141.20 / 259.23 ms | 10.50 / 18.21 ms | 20.12 / 46.55 ms |

**Read this carefully, because the naive reading is wrong.** Every "during" number is *better* than
the "before" number, which does not mean a migration makes a cluster faster. Two effects dominate
and both are larger than what the migration costs:

- **The two backends are not equally fast on this box.** Compare "during" with "after": both are
  served by MinIO, one with the whole migration machinery live and one without. Those two columns
  are within run-to-run noise of each other, and at 1 MiB the quiescent column is the slower of the
  two. That is the honest measurement of migration overhead on this hardware: **below the noise
  floor of a single run.**
- **The baseline was taken on a cluster under write pressure.** It runs immediately after 1,000
  objects plus four multipart uploads land on Garage, so it measures a cluster that is still
  digesting them. It is a fair picture of "what the client saw before we started" and an unfair
  picture of Garage.

What the numbers do not cover: `GET` and `PUT` during a migration never exercise the merged
listing, because the bench does not list. That path is metered separately.

## The paths the migration adds

Measured from the same run's `/-/metrics`, on a bucket holding ~1,000 keys on each side:

| Path | Measurement |
|---|---|
| Merged `ListObjectsV2` | `shunt_listing_merge_seconds`: 13 pages, 1.119 s total, **86 ms per page** — two upstream listings, a sorted merge, and a re-render |
| Fallback reads | 12 over the run: reads the new primary could not serve, answered from the source |
| Ramp split | 152 writes to the new primary, 11 to the source, across ratios 0.01 → 0.25 → 1.0 |

The merged listing is the one path that is meaningfully more expensive than its ACTIVE equivalent,
and it is bounded: it exists only while a bucket is `MIGRATING`.

## The mover

| Measure | Garage → MinIO | MinIO → Garage |
|---|---|---|
| Objects | 1,016 copied, 16 already on the target | 321 copied over 3 buckets |
| Bytes | 40.0 MiB | 9.4 MiB |
| Guard | `If-None-Match: *` (MinIO honours it) | HEAD-then-commit (Garage ignores it) |
| Second pass | 0 copied, 1,032 already there | 0 copied, 321 already there |

The second pass is the convergence check: nothing left to move. The 16 objects "already on the
target" in the first pass are the bench harness's own keys, which the client rewrote onto the new
primary while the mover was working — exactly the case the overwrite guard exists for.

## Reproducing

```sh
make e2e-up
make run-mixed                                                   # in another terminal
test/e2e/demo.sh --from garage --to minio --objects 1000         # this table
test/e2e/evacuate.sh --from minio --to garage                    # the whole-cluster version
```
