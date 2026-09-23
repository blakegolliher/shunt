# shunt

shunt is an S3 front-end proxy. Clients talk to one endpoint and one bucket name, and shunt decides which backend cluster actually holds each bucket. That lets you move a bucket from one cluster to another **while clients keep reading and writing it**, with no client change, no downtime and no per-object bookkeeping.

When the move is done, shunt can get out of the way: `shunt step-out` checks whether your clients could talk to the cluster directly again, and tells you what stands in the way if they can't. Coming in costs no client change, and so does leaving (ADR-0011).

**Status: proof of concept.** It has run on Garage, MinIO and VAST. The design is in [docs/DESIGN.md](docs/DESIGN.md), progress and known gaps in [docs/STATUS.md](docs/STATUS.md), and the POC track in [docs/POC.md](docs/POC.md). It is not production software yet.

## Build

You need Go 1.27 and, for the demo, aws-cli.

```sh
git clone git@github.com:blakegolliher/shunt.git && cd shunt
make build          # → bin/shunt
make all            # build, lint, tests, -race, fuzz
```

For the browser workflow, `make demo-ui` builds the UI and leaves a three-control/two-proxy local
fleet running; follow [docs/demo-ui.md](docs/demo-ui.md), then stop it with `make demo-ui-down`.

## Commands at a glance

| Command | What it does |
|---|---|
| `shunt serve --plaintext` | Run a lab shunt with no config: state in `./shunt-data`, clients on `127.0.0.1:8008`, commands on `127.0.0.1:9900` |
| `shunt client show` | Print the client access key and secret your S3 client uses with shunt |
| `shunt cluster add <name> <url> --access-key <key>` | Add a cluster; prompts for the secret key and checks it; reads the type from the cluster |
| `shunt cluster remove <name>` | Drop a cluster nothing uses any more |
| `shunt adopt <cluster> <bucket> [--keys <file>]` | Serve an existing bucket through shunt under the same name, with the cluster's own client keys |
| `shunt client add <access-key> --check <cluster>` | Import a key clients already use, so their credentials don't change |
| `shunt client remove <access-key>` | Drop a key shunt holds (the generated lab key, once real ones are in) |
| `shunt expand <bucket> --to <cluster> [--name <bucket>]` | Prepare a target bucket on another cluster: checks, canary, measured capabilities |
| `shunt ramp <bucket> --ratio <0..1>` | Send that share of writes (by key hash) to the target |
| `shunt migrate start <bucket>` | All writes to the target; reads fall back to the source |
| `shunt migrate run <bucket> --until-converged` | Copy what is still only on the source |
| `shunt cutover <bucket> --window 60s` | Stop reading the source once nothing needs it |
| `shunt purge-source <bucket>` | Delete the source copy and bucket, after checking the target has everything |
| `shunt tenant set-default <cluster>` | Where new buckets are created |
| `shunt status [bucket]` | Clusters, moving buckets, write split, fallback reads, mover progress |
| `shunt step-out` | Check whether clients could use their cluster directly again, with shunt gone |
| `shunt proxy list` / `forget <id>` | With several proxies: which have each change, which are silent, and forgetting one that is gone |
| `shunt-control init` / `join` / `status` | The control plane a fleet of proxies runs on ([docs/how-to/run-shunt-control.md](docs/how-to/run-shunt-control.md)) |

Every command answers `--help`. A bucket is named bare (`demo-source`). `tenant/bucket` is only needed when one shunt serves several tenants. For production (TLS, tokens), run `shunt serve --config shunt.yaml` ([docs/reference/config.md](docs/reference/config.md)). Several proxies on several hosts take their directory from `shunt-control`, the control plane, and share nothing with each other: [docs/fleet.md](docs/fleet.md).

---

## Demo: move a live bucket between two clusters, by hand

A bucket starts on cluster **A**. Clients reach it through shunt, **with the key cluster A already gave them**. Half the writes, then all of them, move to cluster **B**. The mover copies what's left, shunt cuts over and deletes the old copy, cluster A is dropped, and shunt hands the clients to cluster B and gets out of the way. A reader loop checks every object byte for byte throughout.

Every step is a command: no config file to write, nothing to edit. The transcripts below come from a run of exactly these commands; the same sequence has also been run by hand between two VAST clusters.

**You need:**
- **Two S3 clusters,** with an access key and secret key for each.
- **An empty bucket named `demo-source` on each.** The same name on both is what lets shunt hand the clients back at the end (step 14); S3 cannot rename a bucket. If your keys can create buckets, `aws s3 mb` does it; otherwise ask an admin.
- **aws-cli.**
- **Plain http.** Everything below runs over http, fine for a lab; see [docs/reference/config.md](docs/reference/config.md) for TLS.

Open **three terminals** on the same host, and in each one run:

```sh
mkdir -p ~/shunt-lab && cd ~/shunt-lab
export PATH=~/shunt/bin:$PATH            # wherever you cloned and built shunt
```

| Terminal | Used for |
|---|---|
| **T1** | runs `shunt serve` (its log window) |
| **T2** | where you type the commands |
| **T3** | the reader loop |

### Step 1: start shunt (T1)

```sh
shunt serve --plaintext
```

That's all it needs. On first start it creates `./shunt-data/`, where it keeps the clusters and buckets you add, their secrets, and a client key. It then listens for S3 clients on `127.0.0.1:8008`, with its commands on `127.0.0.1:9900`. Leave T1 running: every change and every refusal from here on shows up in its log.

```
shunt: created a client key SHUNT3F0C… in …/shunt-data/credentials.yaml; `shunt client show --state-dir shunt-data` prints its secret
21:02:11 INFO  [PLAINTEXT] shunt serving  listen=127.0.0.1:8008 admin=127.0.0.1:9900 auth=resign
```

### Step 2: point aws-cli at the clusters and at shunt (T2, once)

```sh
aws configure --profile clustera       # cluster A's access key and secret key; region us-east-1
aws configure set endpoint_url http://A-HOST --profile clustera
aws configure --profile clusterb       # cluster B's keys
aws configure set endpoint_url http://B-HOST --profile clusterb
for p in clustera clusterb; do aws configure set s3.addressing_style path --profile $p; done

aws --profile clustera s3 ls s3://demo-source/ --summarize
aws --profile clusterb s3 ls s3://demo-source/ --summarize
```

**`clustera`** and **`clusterb`** talk to the clusters directly, to check where objects really are. The client profile comes in step 4, once shunt holds cluster A's key. Both listings should end in `Total Objects: 0`.

### Step 3: two files straight to cluster A (T2)

```sh
mkdir -p files
for i in 01 02; do head -c 1048576 /dev/urandom > files/file-$i; aws --profile clustera s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
aws --profile clustera s3 ls s3://demo-source/demo/
```

Expect `file-01` and `file-02`, each 1048576 bytes. Every file is also kept in `files/`, so later reads can be compared byte for byte.

### Step 4: put shunt in front of the bucket, with your clients' own key (T2)

```sh
shunt cluster add clustera http://A-HOST --access-key A_ACCESS_KEY      # prompts for A's secret key

umask 077; cat > client-keys.yaml <<EOF                                # the key your clients already use
credentials:
  - access_key: A_ACCESS_KEY
    secret: A_SECRET_KEY
EOF
shunt adopt clustera demo-source --keys client-keys.yaml

shunt client show                      # shows the key shunt generated for itself at startup
shunt client remove SHUNT…             # that key; your clients' key is the only one left

aws configure --profile shunt          # cluster A's key and secret again; region us-east-1
aws configure set endpoint_url http://127.0.0.1:8008 --profile shunt
aws configure set s3.addressing_style path --profile shunt
aws --profile shunt s3 ls s3://demo-source/demo/
```

```
cluster clustera: s3 http://A-HOST region us-east-1 (conditional_write true, conditional_delete false)
client key A_ACCESS_KEY imported: clustera accepts it, and shunt now verifies clients with it
demo-source: ACTIVE on clustera/demo-source
client key SHUNT… removed; 1 key(s) left for these clients
2026-09-17 10:59:55    1048576 file-01
2026-09-17 10:59:56    1048576 file-02
```

- **`cluster add`** takes the cluster's URL and access key, and asks for the secret key. Before it saves anything, it signs a request with that pair and refuses a wrong secret. It reads the cluster type from the cluster itself and keeps the secret in `shunt-data/secrets/`.
- **`adopt --keys`** serves the existing bucket under the same name, **and imports the client keys cluster A already issued**. shunt checks each one against the cluster before storing it, and from then on verifies client signatures with it. So your clients keep the credentials they have: nothing on their side changes when shunt appears, and nothing changes when it leaves (step 14).
- **`client remove`** drops the key shunt generated for itself at startup. Nothing uses it, and a key no cluster knows would block the handover at the end.
- **The listing** comes through shunt, with cluster A's own key, and shows the same two files.

T1 logs `cluster added`, `client key imported`, `bucket adopted` and `client key removed`.

### Step 5: ten more, through shunt (T2)

```sh
for i in $(seq -w 3 12); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l
```

Expect `12`: all on cluster A, the only cluster so far.

### Step 6: add cluster B, live (T2)

```sh
shunt cluster add clusterb http://B-HOST --access-key B_ACCESS_KEY      # prompts for B's secret key
shunt expand demo-source --to clusterb --name demo-source
shunt status demo-source
```

```
cluster clusterb: minio http://B-HOST region us-east-1 (conditional_write true, conditional_delete false)
demo-source: target clusterb/demo-source (exists); canary write, read, delete ok; conditional_write true, conditional_delete false (measured; directory version 6)

CLUSTER   TYPE   SCHEME  ENDPOINTS  COND.PUT  USED BY
clustera  s3     http    A-HOST     true      the default cluster for new buckets, bucket demo-source
clusterb  minio  http    B-HOST     true      bucket demo-source

BUCKET       STATE   RATIO  PRIMARY               SOURCE                         WRITES P/S  FALLBACK READS  MOVER
demo-source  ACTIVE  -      clustera/demo-source  (target clusterb/demo-source)  -           0               -
```

`expand` does four things:
- checks that the bucket exists on B;
- checks that it was never versioned;
- writes, reads back and deletes a canary object;
- measures whether B honours conditional writes, which the mover relies on.

It then records B as the target. Clients see no change yet. **`--name demo-source` keeps the name clients use.** Without it the bucket on B would be `demo-source-001`, which is fine while shunt is in front and is the one thing that cannot be undone later: a bucket cannot be renamed, so clients going direct would have to use that name (step 14). `expand` says so when the names differ.

### Step 7: send 50% of writes to cluster B (T2)

```sh
shunt ramp demo-source --ratio 0.5
```

Each key name is hashed, and half of the names now write to B. A given name always goes to the same side. Reads try B first and fall back to A.

### Step 8: read everything back, then 20 more (T2)

Define a checker once in T2. It downloads everything through shunt and compares it with `files/`:

```sh
check() { rm -rf readback && mkdir readback && aws --profile shunt s3 cp --recursive --quiet s3://demo-source/demo/ readback/;
  bad=0; for f in files/*; do cmp -s "$f" "readback/${f##*/}" || { echo "BAD ${f##*/}"; bad=$((bad+1)); }; done
  echo "read $(ls readback | wc -l), mismatched $bad"; }
check
for i in $(seq 13 32); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
echo "A: $(aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l)  B: $(aws --profile clusterb s3 ls s3://demo-source/demo/ | wc -l)"
check
shunt status demo-source
```

Expect `read 12, mismatched 0`, then `read 32, mismatched 0`.

The 20 new files split by a hash of their names, **not exactly 10/10**. With these names it is 11 to B and 9 to A, so the counts read `A: 21  B: 11` and `WRITES P/S` shows `11/9`. Every file reads back correctly whichever cluster it's on.

### Step 9: all writes to cluster B, 20 more (T2)

```sh
shunt ramp demo-source --ratio 1.0
for i in $(seq 33 52); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
echo "A: $(aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l)  B: $(aws --profile clusterb s3 ls s3://demo-source/demo/ | wc -l)"
check
```

A's count doesn't change: all 20 new files land on B. Expect `read 52, mismatched 0`.

### Step 10: start the reader loop (T3)

```sh
pass=0; while true; do pass=$((pass+1)); rm -rf rb && mkdir rb
  aws --profile shunt s3 cp --recursive --quiet s3://demo-source/demo/ rb/ 2> err.txt; rc=$?
  bad=0; for f in files/*; do cmp -s "$f" "rb/${f##*/}" || bad=$((bad+1)); done
  echo "$(date +%T) pass $pass: read $(ls rb | wc -l), mismatched $bad, aws exit $rc $(tr '\n' ' ' < err.txt)"; sleep 2; done
```

Leave it running. Every line should say `read 52, mismatched 0, aws exit 0` through the rest of the demo.

### Step 11: migrate the rest (T2)

```sh
shunt migrate start demo-source
shunt migrate run demo-source --until-converged
shunt cutover demo-source --window 60s
```

What each command does:
- **`migrate start`:** every write goes to B. Deletes go to both clusters, and listings merge them.
- **`migrate run`:** the mover copies what's still only on A, never overwriting a newer client write, and repeats until a pass copies nothing. It runs in T2 on the same host as shunt, and reads the secrets shunt stored.
- **`cutover`:** waits 60 seconds, and cuts over only if no read needed cluster A in that time. The reader in T3 is exactly what it's watching.

### Step 12: remove the data from cluster A (T2)

```sh
shunt purge-source demo-source
aws --profile clusterb s3 ls s3://demo-source/demo/ | wc -l
aws --profile clustera s3 ls s3://demo-source/
```

- **`purge-source`:** refuses unless B has every object A has. It then deletes A's copies **and the `demo-source` bucket on A**.
- **B:** holds all 52.
- **A:** the listing ends in `NoSuchBucket`.

The client still calls the bucket `demo-source`.

### Step 13: shunt forgets cluster A (T2)

```sh
shunt tenant set-default clusterb      # new buckets now land on B
shunt cluster remove clustera
shunt status
```

`cluster remove` is refused while anything still points at cluster A, and the refusal lists what does. That's why `set-default` comes first. `status` then lists only `clusterb`, and T3 keeps printing `mismatched 0`.

Stop T3 with Ctrl-C. Leave shunt running for the last step.

### Step 14: hand the clients back and stop shunt (T2)

shunt is not a one-way door. `shunt step-out` asks cluster B whether your clients could talk to it directly, and changes nothing:

```sh
shunt step-out
```

```
step-out check for your clients: every bucket is on clusterb (http://B-HOST)
  ok      bucket demo-source: ACTIVE on clusterb under the same name, no uploads in progress
  BLOCKED client key A_ACCESS_KEY: clusterb does not know this access key: clients going direct would need keys clusterb issues, or shunt should hold clusterb's own keys from the start (docs/migrating.md)

1 problem blocks stepping out. Fix them and run shunt step-out again.
shunt: not ready to step out; nothing was changed
```

The bucket is fine: it kept its name (step 6). The key is not, and that is the one real cost of moving between clusters with separate credentials — cluster B never issued cluster A's key. Import B's key, drop A's, and point the client profile at the new key:

```sh
shunt client add B_ACCESS_KEY --check clusterb     # prompts for B's secret key
shunt client remove A_ACCESS_KEY
aws configure --profile shunt                      # cluster B's key and secret now
shunt step-out
```

```
client key B_ACCESS_KEY imported: clusterb accepts it, and shunt now verifies clients with it
client key A_ACCESS_KEY removed; 1 key(s) left for these clients

step-out check for your clients: every bucket is on clusterb (http://B-HOST)
  ok      bucket demo-source: ACTIVE on clusterb under the same name, no uploads in progress
  ok      client key B_ACCESS_KEY: clusterb accepts it and it reaches every bucket

READY: clients can use clusterb directly, with the keys and bucket names they use now. To step out:
  1. Point the S3 name your clients use at clusterb (B-HOST) instead of shunt: its DNS records (lower the TTL a day ahead) or the VIP.
  2. Wait out the TTL and watch shunt_requests_total on shunt's admin listener stop rising.
  3. Stop shunt. Until then, pointing the name back at shunt undoes the step-out: nothing in shunt changed.
```

In a real deployment step 1 is a DNS change, and clients never notice. Here, do it by hand: stop shunt in T1 with Ctrl-C, point the client profile straight at cluster B, and read everything one more time.

```sh
aws configure set endpoint_url http://B-HOST --profile shunt
check
```

```
read 52, mismatched 0
```

All 52 files, the two written before shunt existed included, read back byte for byte from cluster B with no shunt in the path and the bucket name clients started with. If cluster B can be given the same key pair as A, even the key change goes away and clients notice nothing at all. To start again from scratch, delete `shunt-data/`.

### When something goes wrong

| You see | It means |
|---|---|
| `refused: … the secret key you entered is not the secret of access key …` | Re-run `shunt cluster add` with the right secret |
| `refused: … does not know access key …` | The access key is wrong, or belongs to the other cluster |
| `refused: … does not know this access key` on `adopt --keys` or `client add` | That client key is not one this cluster issued. Nothing was imported or adopted |
| `refused: … cannot reach cluster …` | The URL is wrong, or the cluster isn't reachable from this host |
| `refused: … still referenced by the default cluster for new buckets` | Run `shunt tenant set-default <cluster>` first |
| `refused: … ignores If-None-Match: * on PUT` on `migrate start` | Cluster B can't refuse an overwrite, so the mover could clobber a client write. VAST and MinIO don't hit this. Read [docs/migrating.md](docs/migrating.md), then add `--accept-lost-write-window` to both `migrate start` and `migrate run` if you accept that |
| `refused: … cutover … reads still fall back` | Something wasn't copied yet. Run `shunt migrate run demo-source --until-converged` again |
| a line in T3 with `mismatched` above 0 | A real problem. Stop and keep T1's log |

A signing region other than `us-east-1` (AWS outside it, Garage) takes `--region` on `cluster add`. A cluster that doesn't answer with its own `Server` header can be named with `--type`.

---

## More

- [docs/walkthrough.md](docs/walkthrough.md): the same migration, scripted, with a workload generator (`shunt verify`) that checks every read and write. `make walkthrough` runs it against local Garage and MinIO.
- [docs/migrating.md](docs/migrating.md): the operator how-to, and what each state does.
- [docs/reference/control-api.md](docs/reference/control-api.md): the control API behind the `shunt` commands.
- [docs/reference/config.md](docs/reference/config.md): every config key.
- [docs/adr/](docs/adr/): the design decisions, including the migration races and their windows (ADR-0004).

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE) and [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
