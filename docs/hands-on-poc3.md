# Hands-on: one endpoint, two clusters, a bucket that moves (POC-3)

Every command below was run once, in order, on the dev box against the live Garage and MinIO from `test/e2e`, and the output is transcribed from that run (2026-09-15, shunt `poc-2-2-g8f5ee94`). Nothing here is illustrative.

**What this shows:** one TLS endpoint in front of two clusters; two tenants that cannot see each other; buckets that land on different clusters; 200 MiB multipart IO; and a bucket moving from Garage to MinIO while the client's endpoint, bucket name, and keys stay the same.

**What it does not show:** shunt moving the data. POC-3 has no mover, and `RAMPING`/`MIGRATING` route every request to the *source* on purpose, so step 18 copies the objects by hand with the backends' own credentials. That step is where POC-4's mover goes, along with dual writes, fallback reads, and a listing merge. Until then a migration is only safe on a bucket nobody is writing to.

## Setup

Backends up, proxy running, directory reset:

```bash
make e2e-up                       # Garage on :3900, MinIO on :9000 (skip if already up)
make build
make run-mixed                    # resets test/e2e/data/directory-mixed.yaml, serves on 127.0.0.1:8443
```

In the shell you work from:

```bash
set -a; . test/e2e/data/garage.env; set +a
export PYTHONWARNINGS=ignore AWS_DEFAULT_REGION=us-east-1

# Two tenants, one endpoint. TLS verification is off only because the lab
# certificate is for *.shunt.example.com and we address 127.0.0.1.
s3a() { AWS_ACCESS_KEY_ID=$SHUNT_A_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$SHUNT_A_SECRET \
        aws --endpoint-url https://127.0.0.1:8443 --no-verify-ssl --no-paginate "$@"; }
s3b() { AWS_ACCESS_KEY_ID=$SHUNT_B_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$SHUNT_B_SECRET \
        aws --endpoint-url https://127.0.0.1:8443 --no-verify-ssl --no-paginate "$@"; }

# The backends themselves, to check where bytes really are. Clients never get these.
garage() { AWS_ACCESS_KEY_ID=$GARAGE_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$GARAGE_SECRET \
           AWS_DEFAULT_REGION=garage aws --endpoint-url http://127.0.0.1:3900 "$@"; }
minio()  { AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$MINIO_SECRET \
           aws --endpoint-url http://127.0.0.1:9000 "$@"; }

CFG="-c test/e2e/data/shunt-mixed.yaml"
```

`e2e-a` creates on Garage by default, `e2e-b` on MinIO. Each starts with one seeded bucket named `archive` on the *other* cluster, so both tenants already span clusters.

## 1. The same bucket name, two tenants, two clusters

```console
$ s3a s3 mb s3://alpha && s3b s3 mb s3://alpha
make_bucket: alpha
make_bucket: alpha

$ ./bin/shunt directory get e2e-a/alpha $CFG
state: ACTIVE
primary: garage
names:
  garage: e2e-a-5428-alpha

$ ./bin/shunt directory get e2e-b/alpha $CFG
state: ACTIVE
primary: minio
names:
  minio: e2e-b-bc10-alpha
```

The client said `alpha`. shunt generated `<tenant>-<4 hex>-<bucket>` per tenant and put each on that tenant's default cluster.

## 2. IO, including multipart

```console
$ s3a s3 cp small.txt s3://alpha/small.txt        # 44 bytes
$ s3a s3 cp big.bin  s3://alpha/big.bin           # 200 MiB, 3.2s, ~86 MiB/s, 25 parts
$ s3b s3 cp small-b.txt s3://alpha/small.txt      # same key, other tenant

$ s3a s3 ls s3://alpha/
2026-09-15 17:40:59  209715200 big.bin
2026-09-15 17:40:58         44 small.txt
$ s3b s3 ls s3://alpha/
2026-09-15 17:41:02         24 small.txt

$ s3a s3 cp s3://alpha/small.txt - ; s3b s3 cp s3://alpha/small.txt -
hello from the client, 2026-09-16T00:40:17Z
hello from tenant e2e-b
```

Round-tripping the 200 MiB object gives back the same sha256 it went up with.

## 3. Where the bytes physically are

```console
$ garage s3 ls | grep -E 'alpha|archive'
2026-09-15 17:40:56 e2e-a-5428-alpha
2026-09-15 16:08:29 e2e-b-seed-archive
$ minio s3 ls | grep -E 'alpha|archive'
2026-09-15 16:04:10 e2e-a-seed-archive
2026-09-15 17:41:02 e2e-b-bc10-alpha

$ garage s3 ls s3://e2e-a-5428-alpha/
2026-09-15 17:40:59  209715200 big.bin
2026-09-15 17:40:58         44 small.txt
```

## 4. Isolation

```console
$ s3b s3 ls s3://e2e-a-5428-alpha/
An error occurred (NoSuchBucket) when calling the ListObjectsV2 operation: The specified bucket does not exist.
```

A tenant asking for another tenant's backend bucket by its real name gets `NoSuchBucket`, answered by shunt with no upstream request at all.

## 5. Spread: one tenant, one endpoint, buckets on both clusters

```console
$ for b in logs runs models; do s3a s3 mb s3://$b; s3a s3 cp small.txt s3://$b/marker.txt; done
$ s3a s3 cp small.txt s3://archive/marker.txt      # archive lives on minio

$ s3a s3 ls
2026-09-15 17:40:56 alpha       # garage  e2e-a-5428-alpha
2026-09-14 17:00:00 archive     # minio   e2e-a-seed-archive
2026-09-15 17:41:50 logs        # garage  e2e-a-daa4-logs
2026-09-15 17:41:53 models      # garage  e2e-a-06ec-models
2026-09-15 17:41:52 runs        # garage  e2e-a-25f0-runs
```

`ListBuckets` is answered from the directory, not from either backend. Where the traffic went:

```console
$ curl -s http://127.0.0.1:9900/-/metrics | grep '^shunt_requests_total' | grep -v 'cluster="none"'
{cluster="garage",cluster_type="s3",op="UploadPart",status_class="2xx"} 25
{cluster="garage",cluster_type="s3",op="PutObject",status_class="2xx"} 4
{cluster="garage",cluster_type="s3",op="CreateBucket",status_class="2xx"} 4
{cluster="minio",cluster_type="minio",op="CreateBucket",status_class="2xx"} 1
{cluster="minio",cluster_type="minio",op="GetObject",status_class="2xx"} 1
…
```

And per request, in `test/e2e/data/shunt-mixed.access.jsonl`:

```json
{"op":"PutObject","bucket":"archive","tenant":"e2e-a","cluster":"minio","cluster_type":"minio","backend_bucket":"e2e-a-seed-archive","status":200}
{"op":"ListBuckets","bucket":"","tenant":"e2e-a","cluster":"none","cluster_type":"none","backend_bucket":"","status":200}
```

`cluster: none` means shunt answered it itself.

## 6. Move `alpha` from Garage to MinIO

The client does nothing in this section. Quiesce the bucket first: POC-3 has no dual writes, so a write during the copy would be lost.

```console
$ ./bin/shunt directory set-state --offline e2e-a/alpha CUTOVER $CFG
shunt: directory: illegal transition ACTIVE -> CUTOVER: allowed from ACTIVE: RAMPING, MIGRATING

$ minio s3 mb s3://e2e-a-moved-alpha
$ ./bin/shunt directory set-state --offline e2e-a/alpha MIGRATING --to minio --name e2e-a-moved-alpha --actor demo $CFG
e2e-a/alpha: ACTIVE -> MIGRATING (directory version 7)
```

`set-state` reads `GetBucketVersioning` on both the source and the target first, signed with each cluster's own credentials, and refuses if either was ever versioned. It fails closed: an error reading it is also a refusal.

While MIGRATING, reads still come from Garage and the bucket is frozen:

```console
$ s3a s3 ls s3://alpha/
2026-09-15 17:40:59  209715200 big.bin
2026-09-15 17:40:58         44 small.txt
$ s3a s3 rb s3://alpha
remove_bucket failed: An error occurred (InvalidBucketState) when calling the DeleteBucket operation:
The bucket is MIGRATING; DeleteBucket is refused until its migration completes.
```

The copy, by hand, backend to backend. **This is the POC-4 mover's job**:

```console
$ garage s3 sync s3://e2e-a-5428-alpha /tmp/move --quiet
$ minio  s3 sync /tmp/move s3://e2e-a-moved-alpha --quiet
  copied 2 objects

$ ./bin/shunt directory set-state --offline e2e-a/alpha CUTOVER --actor demo $CFG
e2e-a/alpha: MIGRATING -> CUTOVER (directory version 8)
$ ./bin/shunt directory set-state --offline e2e-a/alpha ACTIVE --actor demo $CFG
e2e-a/alpha: CUTOVER -> ACTIVE (directory version 9)
```

Same endpoint, same bucket name, now served by MinIO:

```console
$ ./bin/shunt directory get e2e-a/alpha $CFG
state: ACTIVE
primary: minio
names:
  minio: e2e-a-moved-alpha

$ s3a s3 ls s3://alpha/
2026-09-15 17:42:27  209715200 big.bin
2026-09-15 17:42:26         44 small.txt
$ s3a s3 cp s3://alpha/big.bin big-after.bin && sha256sum big.bin big-after.bin
1c1ad4f0329aacfae900e321e2e0264a3c71a0abf3fc7e6b85e03e06d78d4e31  big.bin
1c1ad4f0329aacfae900e321e2e0264a3c71a0abf3fc7e6b85e03e06d78d4e31  big-after.bin
```

The trail, from `test/e2e/data/directory-mixed.yaml.changes.jsonl`:

```
create    proxy:SHUNT8145…   -         -> ACTIVE
set-state demo               ACTIVE    -> MIGRATING
set-state demo               MIGRATING -> CUTOVER
set-state demo               CUTOVER   -> ACTIVE
```

## What this does not prove yet

- **The copy was manual.** POC-4 adds the mover, with `If-None-Match: *` so a client write is never overwritten, a resumable cursor, and a ledger.
- **The bucket had to be quiet.** Dual writes during RAMPING, fallback reads during MIGRATING, and the listing merge are POC-4. Until then, migrate only what nobody is writing.
- **Load is not spread within a bucket.** A bucket lives on one cluster. Spreading here means buckets across clusters; within a cluster it is round-robin across that cluster's endpoints, which this lab cannot show since each backend has one.
- **VAST is not in this run.** It joins the https run when `make check-tls-verify` passes; the migration itself has run on VAST over http (docs/STATUS.md).

## Reset

```bash
make run-mixed      # restores the seed directory and restarts the proxy
make e2e-down       # or tear the backends down entirely
```
