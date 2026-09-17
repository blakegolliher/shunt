# shunt

shunt is an S3 front-end proxy. Clients talk to one endpoint and one bucket name, and shunt decides which backend cluster actually holds each bucket. That lets you move a bucket from one cluster to another **while clients keep reading and writing it**, with no client change, no downtime and no per-object bookkeeping.

**Status: proof of concept.** It has run on Garage, MinIO and VAST. The design is in [docs/DESIGN.md](docs/DESIGN.md), progress and known gaps in [docs/STATUS.md](docs/STATUS.md), and the POC track in [docs/POC.md](docs/POC.md). It is not production software yet.

## Build

You need Go 1.27 and, for the demo, aws-cli.

```sh
git clone git@github.com:blakegolliher/shunt.git && cd shunt
make build          # → bin/shunt
make all            # build, lint, tests, -race, fuzz
```

## Commands at a glance

| Command | What it does |
|---|---|
| `shunt serve --plaintext` | Run a lab shunt with no config: state in `./shunt-data`, clients on `127.0.0.1:8008`, commands on `127.0.0.1:9900` |
| `shunt client show` | Print the client access key and secret your S3 client uses with shunt |
| `shunt cluster add <name> <url> --access-key <key>` | Add a cluster; prompts for the secret key and checks it; reads the type from the cluster |
| `shunt cluster remove <name>` | Drop a cluster nothing uses any more |
| `shunt adopt <cluster> <bucket>` | Serve an existing bucket through shunt under the same name |
| `shunt expand <bucket> --to <cluster> [--name <bucket>]` | Prepare a target bucket on another cluster: checks, canary, measured capabilities |
| `shunt ramp <bucket> --ratio <0..1>` | Send that share of writes (by key hash) to the target |
| `shunt migrate start <bucket>` | All writes to the target; reads fall back to the source |
| `shunt migrate run <bucket> --until-converged` | Copy what is still only on the source |
| `shunt cutover <bucket> --window 60s` | Stop reading the source once nothing needs it |
| `shunt purge-source <bucket>` | Delete the source copy and bucket, after checking the target has everything |
| `shunt tenant set-default <cluster>` | Where new buckets are created |
| `shunt status [bucket]` | Clusters, moving buckets, write split, fallback reads, mover progress |

Every command answers `--help`. A bucket is named bare (`demo-source`). `tenant/bucket` is only needed when one shunt serves several tenants. For production (TLS, tokens), run `shunt serve --config shunt.yaml` ([docs/reference/config.md](docs/reference/config.md)).

---

## Demo: move a live bucket between two clusters, by hand

This is the demo as it was run by hand on two lab VAST clusters. A bucket starts on cluster **A**. Clients reach it through shunt. Half the writes, then all of them, move to cluster **B**. The mover copies what's left, shunt cuts over and deletes the old copy, and cluster A is dropped. A reader loop checks every object byte for byte throughout.

Every step is a command: no config file to write, nothing to edit.

**You need:**
- **Two S3 clusters,** with an access key and secret key for each.
- **An empty bucket on each:** `demo-source` on A and `demo-dest` on B. If your keys can create buckets, `aws s3 mb` does it; otherwise ask an admin.
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
shunt client show                      # the access_key and secret your S3 client uses with shunt
aws configure --profile shunt          # paste those two; region us-east-1; output format blank
aws configure set endpoint_url http://127.0.0.1:8008 --profile shunt

aws configure --profile clustera       # cluster A's access key and secret key; region us-east-1
aws configure set endpoint_url http://A-HOST --profile clustera
aws configure --profile clusterb       # cluster B's keys
aws configure set endpoint_url http://B-HOST --profile clusterb
for p in shunt clustera clusterb; do aws configure set s3.addressing_style path --profile $p; done

aws --profile clustera s3 ls s3://demo-source/ --summarize
aws --profile clusterb s3 ls s3://demo-dest/ --summarize
```

- **`shunt`** is the client, and talks only to shunt.
- **`clustera`** and **`clusterb`** talk to the clusters directly, to check where objects really are.

Both listings should end in `Total Objects: 0`.

### Step 3: two files straight to cluster A (T2)

```sh
mkdir -p files
for i in 01 02; do head -c 1048576 /dev/urandom > files/file-$i; aws --profile clustera s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
aws --profile clustera s3 ls s3://demo-source/demo/
```

Expect `file-01` and `file-02`, each 1048576 bytes. Every file is also kept in `files/`, so later reads can be compared byte for byte.

### Step 4: put shunt in front of the bucket (T2)

```sh
shunt cluster add clustera http://A-HOST --access-key A_ACCESS_KEY      # prompts for A's secret key
shunt adopt clustera demo-source
aws --profile shunt s3 ls s3://demo-source/demo/
```

- **`cluster add`** takes the cluster's URL and access key, and asks for the secret key. Before it saves anything, it signs a request with that pair and refuses a wrong secret. It reads the cluster type from the cluster itself and keeps the secret in `shunt-data/secrets/`.
- **`adopt`** serves the existing bucket through shunt under the same name.
- **The listing** comes through shunt and shows the same two files.

T1 logs `cluster added` and `bucket adopted`.

### Step 5: ten more, through shunt (T2)

```sh
for i in $(seq -w 3 12); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l
```

Expect `12`: all on cluster A, the only cluster so far.

### Step 6: add cluster B, live (T2)

```sh
shunt cluster add clusterb http://B-HOST --access-key B_ACCESS_KEY      # prompts for B's secret key
shunt expand demo-source --to clusterb --name demo-dest
shunt status demo-source
```

`expand` does four things:
- checks that `demo-dest` exists on B;
- checks that it was never versioned;
- writes, reads back and deletes a canary object;
- measures whether B honours conditional writes, which the mover relies on.

It then records `demo-dest` as the target. Clients see no change yet. The status shows both clusters, and `(target clusterb/demo-dest)`.

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
echo "A: $(aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l)  B: $(aws --profile clusterb s3 ls s3://demo-dest/demo/ | wc -l)"
check
shunt status demo-source
```

Expect `read 12, mismatched 0`, then `read 32, mismatched 0`.

The 20 new files split by a hash of their names, **not exactly 10/10**. With these names it is 11 to B and 9 to A, so the counts read `A: 21  B: 11` and `WRITES P/S` shows `11/9`. Every file reads back correctly whichever cluster it's on.

### Step 9: all writes to cluster B, 20 more (T2)

```sh
shunt ramp demo-source --ratio 1.0
for i in $(seq 33 52); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
echo "A: $(aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l)  B: $(aws --profile clusterb s3 ls s3://demo-dest/demo/ | wc -l)"
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
aws --profile clusterb s3 ls s3://demo-dest/demo/ | wc -l
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

Stop T3, then T1, with Ctrl-C. To start again from scratch, delete `shunt-data/`.

### When something goes wrong

| You see | It means |
|---|---|
| `refused: … the secret key you entered is not the secret of access key …` | Re-run `shunt cluster add` with the right secret |
| `refused: … does not know access key …` | The access key is wrong, or belongs to the other cluster |
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
