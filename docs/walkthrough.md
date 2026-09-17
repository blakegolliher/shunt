# Walkthrough: moving a bucket from vast01 to vast02, live

**Start with README.md** for the same move with no config file and no tenant: `shunt serve --plaintext`, clusters added from a URL with a prompted secret, and bare bucket names (ADR-0010). This page is the scripted form that `test/e2e/walkthrough.sh` checks, with a config file, `file:` secret refs, and `shunt verify` as the client. It uses the default tenant too: the client key names no tenant, and every command takes the bare bucket name.

Ten steps, each a copy-paste block followed by the output to expect. Bucket `data01` starts on cluster `vast01` and ends on cluster `vast02` as `data01-001`. One shunt serves it throughout on `:8008`. A client keeps reading and writing through that shunt from step 4 to the end, with the same endpoint and bucket name, and never sees an error.

`test/e2e/walkthrough.sh` runs these same steps unattended and asserts each one. `make walkthrough` runs it on the e2e Garage (playing vast01) and MinIO (playing vast02), and CI does the same. **The output below is transcribed from a green `make walkthrough` run on 2026-09-17.** Only the endpoints, cluster types, regions, keys and timestamps were changed to their VAST form. Counts vary from run to run.

**Tested on VAST.** The same migration has run VAST → VAST over http, driven by hand through README.md's commands and inside a warp load test, with 0 errors (docs/STATUS.md, docs/bench/poc5-load.md). `test/e2e/walkthrough.sh` itself creates buckets, so run it against your clusters with keys allowed to (the last section shows how).

Everything here is lab-grade on purpose. The client listener is plain http (`listener.plaintext: true`), and the debug route header is on. Neither belongs in production (docs/reference/config.md).

## Before you start

You need:
- `bin/shunt` (`make build`);
- `aws` (aws-cli v2) on the client host;
- one access key per VAST cluster, with rights to create and delete buckets on vast02 and delete them on vast01;
- the two cluster addresses. Set them once:

```sh
export VAST01=http://vast01.example.com:80      # vast01's S3 VIP, http
export VAST02=http://vast02.example.com:80      # vast02's S3 VIP, http
export SHUNT=http://127.0.0.1:8008              # where clients will reach shunt
export SHUNT_API=http://127.0.0.1:9900          # shunt's admin listener; every operator verb uses it
mkdir -p ~/shunt-lab && cd ~/shunt-lab && umask 077
```

Store the secrets in files. shunt never takes a secret inline: a cluster names a `secret_ref`, and a cluster added to a running shunt can only use a `file:` ref, because an `env:` variable would have had to be set when shunt started.

```sh
printf '%s' '<vast01 secret key>' > vast01.secret
printf '%s' '<vast02 secret key>' > vast02.secret
# the client's own key pair: shunt verifies the client's signature with it (resign mode)
printf '%s' "$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')" > client.secret
export CLIENT_AK=SHUNTLAB0000000000000001
cat > credentials.yaml <<EOF
credentials:
  - access_key: $CLIENT_AK
    secret: $(cat client.secret)
EOF
# aws-cli for this session: path-style addressing, and no default checksums on plain http
printf '[default]\ns3 =\n  addressing_style = path\n' > aws.config
export AWS_CONFIG_FILE=$PWD/aws.config AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
as_vast01() { AWS_ACCESS_KEY_ID='<vast01 access key>' AWS_SECRET_ACCESS_KEY=$(cat vast01.secret) aws --endpoint-url "$VAST01" "$@"; }
as_vast02() { AWS_ACCESS_KEY_ID='<vast02 access key>' AWS_SECRET_ACCESS_KEY=$(cat vast02.secret) aws --endpoint-url "$VAST02" "$@"; }
as_client() { AWS_ACCESS_KEY_ID=$CLIENT_AK AWS_SECRET_ACCESS_KEY=$(cat client.secret) aws --endpoint-url "$SHUNT" "$@"; }
```

## 1. Bucket `data01` on vast01

If `data01` already holds your data, skip the upload. Otherwise put something in it, so that there is data to move:

```sh
as_vast01 s3api create-bucket --bucket data01
mkdir -p seed && for i in $(seq 1 100); do head -c $((1024 + RANDOM)) /dev/urandom > seed/obj-$i; done
as_vast01 s3 cp --recursive --quiet seed s3://data01/seed/
as_vast01 s3api list-objects-v2 --bucket data01 --prefix seed/ --query 'length(Contents)'
```

Expected:

```
100
```

## 2. Start shunt on :8008

It starts with no clusters at all. vast01 is added live, and its bucket adopted under the same name.

```sh
printf 'version: 1\n' > directory.yaml
cat > shunt.yaml <<EOF
listener: { address: ":8008", plaintext: true }
admin: { address: "127.0.0.1:9900" }
auth: { mode: resign, credentials_file: $PWD/credentials.yaml }
directory: { file: $PWD/directory.yaml, poll_interval: 1s }
features: { debug_route_header: true }
EOF
shunt check-config shunt.yaml
nohup shunt serve -c shunt.yaml > shunt.log 2>&1 & echo $! > shunt.pid
sleep 1; cat shunt.log
```

Expected. The log goes to a file here, so it is JSON; run `shunt serve` straight in a terminal and it prints one short line per event instead (`telemetry.log_format: auto`), each tagged `[PLAINTEXT]`. Every startup line carries the plaintext marker:

```
shunt.yaml: ok (auth resign; directory directory.yaml version 1: 0 clusters, 0 tenants, 0 placements)
{"level":"WARN","msg":"the client listener is PLAINTEXT http: signatures, credentials in presigned URLs, and object bytes cross the network unencrypted; listener.plaintext is for labs","client_listener":"PLAINTEXT http (listener.plaintext: true; lab use only)"}
{"level":"WARN","msg":"features.debug_route_header is on: any client sending X-Shunt-Debug: 1 learns which cluster served it (ADR-0006 amendment); for labs","client_listener":"PLAINTEXT http (listener.plaintext: true; lab use only)"}
{"level":"INFO","msg":"resign mode","client_listener":"PLAINTEXT http (listener.plaintext: true; lab use only)","credentials":1,"clusters":[],…}
{"level":"INFO","msg":"shunt serving","client_listener":"PLAINTEXT http (listener.plaintext: true; lab use only)","listen":":8008","admin":"127.0.0.1:9900","auth":"resign",…}
```

```sh
shunt cluster add vast01 --type vast --scheme http --region us-east-1 --endpoint "${VAST01#http://}" \
  --access-key '<vast01 access key>' --secret-ref "file:$PWD/vast01.secret"
shunt adopt vast01 data01
shunt status data01
```

Expected:

```
cluster vast01: vast http://vast01.example.com:80 region us-east-1 (conditional_write true, conditional_delete false)
data01: ACTIVE on vast01/data01
directory version 3

CLUSTER  TYPE  SCHEME  ENDPOINTS              COND.PUT  USED BY
vast01   vast  http    vast01.example.com:80  true      the default cluster for new buckets, bucket data01

BUCKET  STATE   RATIO  PRIMARY        SOURCE  WRITES P/S  FALLBACK READS  MOVER
data01  ACTIVE  -      vast01/data01  -       -           0               -
```

`adopt` checked that the bucket exists and was never versioned, made vast01 the default cluster for new buckets, and wrote the placement. The cluster's conditional-write and conditional-delete support is measured on the target by `expand` in step 6, unless you state it with `--conditional-write`/`--conditional-delete` (docs/reference/backend-compat.md).

## 3. Point a client at shunt, with the bucket name unchanged

```sh
as_client s3 ls s3://data01/seed/ | wc -l
as_client s3 cp --quiet s3://data01/seed/obj-1 obj-1.via-shunt && cmp seed/obj-1 obj-1.via-shunt && echo identical
```

Expected:

```
100
identical
```

## 4. Reads and writes, continuously, from here to the end

`shunt verify` is the client for the rest of the walkthrough. It writes, deletes and reads random keys under `verify/<run>/` and checks every answer against a model of what it did:
- a write must read back with its bytes;
- a delete must stay deleted;
- every request must succeed.

With `--debug-route`, each response also says which side and cluster served it. Leave it running in its own terminal, or in the background as shown:

```sh
nohup shunt verify --endpoint "$SHUNT" --bucket data01 --access-key "$CLIENT_AK" --secret-ref "file:$PWD/client.secret" \
  --debug-route --interval 15s --json-out verify.json > verify.log 2>&1 & echo $! > verify.pid
sleep 16; cat verify.log
```

Expected, with counts that vary (in the recorded run the script ramped before the first progress line, so this line is its shape, not a transcript):

```
verify: http://127.0.0.1:8008/data01, 8 workers over 200 keys
verify: 15s <ops> ops, 0 errors; writes primary vast01 <n> (100%); reads primary vast01 <n> (100%)
```

Every progress line from here on must say `0 errors`.

## 5. Bucket `data01-001` on vast02

```sh
as_vast02 s3api create-bucket --bucket data01-001
```

Or skip this step and give `--create` to `shunt expand` in step 6.

## 6. Add vast02 to the running shunt

```sh
shunt cluster add vast02 --type vast --scheme http --region us-east-1 --endpoint "${VAST02#http://}" \
  --access-key '<vast02 access key>' --secret-ref "file:$PWD/vast02.secret"
shunt expand data01 --to vast02
kill -0 "$(cat shunt.pid)" && echo "same shunt process, no restart"
```

Expected:

```
cluster vast02: vast http://vast02.example.com:80 region us-east-1 (conditional_write true, conditional_delete false)
data01: target vast02/data01-001 (exists); canary write, read, delete ok; conditional_write true, conditional_delete true (measured; directory version 6)
same shunt process, no restart
```

Before the cluster was written to the directory, shunt built it and resolved its secret, so a typo in the secret path is refused here. `expand` did three things:
- checked that `data01-001` exists and was never versioned;
- wrote, read back and deleted a canary object there;
- measured conditional PUT and DELETE on vast02, and recorded vast02 as the bucket's target.

The name defaults to `<bucket>-NNN`, the lowest unused number.

## 7. Ramp writes 50/50 by key hash

```sh
shunt ramp data01 --ratio 0.5
```

Expected:

```
data01: ACTIVE -> RAMPING (directory version 7)
writes for 50% of keys now land on vast02; reads fall back to vast01
```

The ramp records the hash that splits its keys. A shunt that doesn't implement that hash refuses the ramp's requests rather than splitting keys differently (ADR-0004):

```sh
curl -s "$SHUNT_API/v1/placements/default/data01" | jq -c .placement.ramp
```

```
{"hash":"fnv1a-fmix64-v1","ratio":0.5}
```

Half the keys, chosen by a stable hash of the key, now write to vast02. A key always lands on the same side, so an overwrite never splits across clusters. A read tries vast02 first and falls back to vast01 on 404.

## 8. Show the split, then send every write to vast02

A short second verify run over 1,000 keys measures the split by itself:

```sh
shunt verify --endpoint "$SHUNT" --bucket data01 --access-key "$CLIENT_AK" --secret-ref "file:$PWD/client.secret" \
  --debug-route --keys 1000 --duration 20s --cleanup
shunt status data01
```

Expected:

```
verify: http://127.0.0.1:8008/data01, 8 workers over 1000 keys

verify report: http://127.0.0.1:8008/data01 prefix verify/1789655639807397108/ seed 1789655639807396907
  4729 operations in 29s: 1644 PUT, 2245 GET, 840 DELETE
  keys present at the end: 609, 606 read back after the workload stopped
  errors: 0
  writes by side: primary 794 (48%), source 850 (52%)
  reads by side: primary 620 (48%), source 685 (52%)
  writes by route: primary vast02 794 (48%), source vast01 850 (52%)
  reads by route: primary vast02 620 (48%), source vast01 685 (52%)
directory version 7

CLUSTER  TYPE  SCHEME  ENDPOINTS              COND.PUT  USED BY
vast01   vast  http    vast01.example.com:80  true      the default cluster for new buckets, bucket data01
vast02   vast  http    vast02.example.com:80  true      bucket data01

BUCKET  STATE    RATIO  PRIMARY            SOURCE         WRITES P/S       FALLBACK READS  MOVER
data01  RAMPING  0.50   vast02/data01-001  vast01/data01  3209/2985 (52%)  36              -
```

Reading the output:
- Writes are ~50/50, and reads succeed from both clusters with 0 errors.
- `WRITES P/S` is shunt's own count (`shunt_ramp_writes_total`) of writes to the new primary and to the source.
- `FALLBACK READS` counts read requests (GET and HEAD) the primary answered 404 and vast01 then served. It counts requests, not objects: `aws s3 cp` of one object sends a HEAD and a GET.
- The walkthrough script fails if the write split is outside 40–60%.

Now send every write to vast02:

```sh
shunt ramp data01 --ratio 1.0
shunt verify --endpoint "$SHUNT" --bucket data01 --access-key "$CLIENT_AK" --secret-ref "file:$PWD/client.secret" \
  --debug-route --keys 1000 --duration 10s --cleanup
```

Expected:

```
data01: RAMPING -> RAMPING (directory version 8)
writes for 100% of keys now land on vast02; reads fall back to vast01
…
  errors: 0
  writes by side: primary 907 (100%)
  reads by side: primary 773 (100%)
  writes by route: primary vast02 907 (100%)
  reads by route: primary vast02 773 (100%)
```

## 9. Move the rest with the mover, then cut over

```sh
shunt migrate start data01
shunt purge-source data01               # refused: not cut over yet
shunt cluster remove vast01             # refused: still in use
shunt migrate run data01 --until-converged --cursor-dir "$PWD" --ledger-dir "$PWD"
shunt status data01
shunt cutover data01 --window 60s
```

Expected:

```
data01: RAMPING -> MIGRATING (directory version 9)
all writes now land on vast02; run `shunt migrate run data01` to copy what is still on vast01
shunt: refused: data01 is MIGRATING; purge-source runs on a placement in CUTOVER
shunt: refused: directory: cluster is still in use: cluster "vast01" is still referenced by the default cluster for new buckets, bucket data01
== data01: vast01/data01 → vast02/data01-001 (If-None-Match guard, re-HEAD withdrawal)
   pass 1
   100 copied, 0 already there, 0 vanished, 0 failed, 1.6 MiB
   pass 2
   0 copied, 100 already there, 0 vanished, 0 failed, 0 B
   converged: the last pass copied nothing

mover: 100 copied, 100 already on the target, 0 vanished mid-copy, 0 failed, 1.6 MiB moved
directory version 9

CLUSTER  TYPE  SCHEME  ENDPOINTS              COND.PUT  USED BY
vast01   vast  http    vast01.example.com:80  true      the default cluster for new buckets, bucket data01
vast02   vast  http    vast02.example.com:80  true      bucket data01

BUCKET  STATE      RATIO  PRIMARY            SOURCE         WRITES P/S       FALLBACK READS  MOVER
data01  MIGRATING  -      vast02/data01-001  vast01/data01  6573/2988 (69%)  85              pass 2: 0 copied, 100 already there, 0 failed, converged
data01: waiting 1m0s for fallback reads to stay flat
data01: MIGRATING -> CUTOVER (directory version 10)
vast01 is no longer read; `shunt purge-source data01` deletes it once you are satisfied
```

What each command did:
- **`migrate start`:** every write now goes to vast02. Reads still fall back to vast01, deletes go to both clusters, and listings merge both.
- **`migrate run`:** runs in this CLI process. It copies what vast01 still holds without overwriting anything a client wrote since (`If-None-Match: *`), and repeats passes until one copies nothing. It reports each pass to shunt.
- **`cutover`:** refused unless that report says converged and no read fell back to vast01 during the window. It waits out the window, then records the evidence on the placement.

The script uses a 15s window. Use one that covers how often your clients read old keys.

## 10. Purge `data01` from vast01, and remove vast01

```sh
shunt purge-source data01
shunt cluster remove vast01             # refused: new buckets still land on vast01
shunt tenant set-default vast02
shunt cluster remove vast01
shunt status data01
as_vast01 s3api head-bucket --bucket data01 || echo "data01 is gone from vast01"
rm -rf readback && as_client s3 cp --recursive --quiet s3://data01/seed/ readback/ && diff -r seed readback && echo "seed objects identical, now served from vast02"
```

Expected:

```
data01: listing diff empty; deleted 100 objects and aborted 0 uploads from vast01/data01, deleted the bucket; ACTIVE on its primary (directory version 11)
shunt: refused: directory: cluster is still in use: cluster "vast01" is still referenced by the default cluster for new buckets
New buckets now land on vast02 (directory version 12)
cluster vast01 removed; shunt no longer holds a connection to it
directory version 13

CLUSTER  TYPE  SCHEME  ENDPOINTS              COND.PUT  USED BY
vast02   vast  http    vast02.example.com:80  true      the default cluster for new buckets, bucket data01

BUCKET  STATE   RATIO  PRIMARY            SOURCE  WRITES P/S       FALLBACK READS  MOVER
data01  ACTIVE  -      vast02/data01-001  -       6573/2988 (69%)  85              -

An error occurred (404) when calling the HeadBucket operation: Not Found
data01 is gone from vast01
seed objects identical, now served from vast02
```

`purge-source` refuses unless the placement is in `CUTOVER` with recorded evidence, and a full listing of both buckets finds no key on vast01 that vast02 lacks. It then aborts in-progress uploads, deletes the objects and the bucket, and drops vast01 from the placement. The `WRITES P/S` and `FALLBACK READS` columns are this shunt process's counters since it started, so they keep their totals.

Stop the client and read its report:

```sh
kill -INT "$(cat verify.pid)"; sleep 2; sed -n '/^verify report/,$p' verify.log
```

Expected:

```
verify report: http://127.0.0.1:8008/data01 prefix verify/1789655634239839728/ seed 1789655634239839557
  33135 operations in 119s: 13190 PUT, 13316 GET, 6629 DELETE
  keys present at the end: 138, 134 read back after the workload stopped
  errors: 0
  writes by side: primary 11053 (84%), source 2137 (16%)
  reads by side: primary 7221 (83%), source 1465 (17%)
  writes by route: primary vast01 1286 (10%), primary vast02 9767 (74%), source vast01 2137 (16%)
  reads by route: primary vast01 721 (8%), primary vast02 6500 (75%), source vast01 1465 (17%)
```

**`errors: 0`** is the claim: every write and every read succeeded, wherever the object lived at that moment. `verify` exits non-zero on any error. In the routes, `primary vast01` is before the ramp, `source vast01` is keys still on vast01 during the move, and `primary vast02` is after.

When you are done: `kill -TERM "$(cat shunt.pid)"`.

## The same, unattended

```sh
printf 'access_key=%s\nsecret=%s\n' '<vast01 access key>' '<vast01 secret>' > vast01.creds
printf 'access_key=%s\nsecret=%s\n' '<vast02 access key>' '<vast02 secret>' > vast02.creds
test/e2e/walkthrough.sh --src "$VAST01" --src-creds vast01.creds --dst "$VAST02" --dst-creds vast02.creds --window 60s
```

The script:
- runs steps 1 to 10 with shunt on `127.0.0.1:8008` and its admin listener on `127.0.0.1:9900`, from a fresh config and directory in a temp dir;
- asserts every expected state and refusal above;
- fails on any error `verify` reports, and on a write split outside 40–60%;
- ends with `WALKTHROUGH GREEN`.

`--reset` deletes `data01` on vast01 and `data01-001` on vast02 first. `--listen`, `--admin`, `--src-type`, `--src-region` and `--dst-*` adjust the rest. `make walkthrough` is this script on the e2e Garage and MinIO.

## When something is refused

| Refusal | Meaning | Do |
|---|---|---|
| `cluster add`: the proxy cannot use this cluster | shunt could not build it or read its `secret_ref` | check the file path and permissions from shunt's host |
| `adopt`/`expand`: versioning Enabled/Suspended | version history cannot be moved | not migratable in v1 |
| `migrate start`: target ignores `If-None-Match: *` | the mover could overwrite a client write | see docs/migrating.md before using `--accept-lost-write-window` |
| `cutover`: has not converged / no mover has reported | the mover's last pass copied something, or shunt restarted | `shunt migrate run … --until-converged` again |
| `cutover`: reads still fall back | a client read an object only vast01 has during the window | run the mover again, then retry |
| `purge-source`: keys on the source the primary lacks | the diff is not empty (it lists up to 20 keys) | run the mover while still in MIGRATING, or investigate those keys |
| `cluster remove`: still referenced by … | a placement or tenant default still names it | finish or purge those placements; `shunt tenant set-default <cluster>` |
