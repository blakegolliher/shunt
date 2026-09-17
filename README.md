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

---

## Demo: move a live bucket between two clusters, by hand

This is the demo as it was run by hand on two lab VAST clusters. A bucket starts on cluster **A**. Clients reach it through shunt. Half the writes, then all of them, move to cluster **B**. The mover copies what's left, shunt cuts over and deletes the old copy, and cluster A is dropped. A reader loop checks every object byte for byte throughout.

Every step is a command. Nothing is hand-edited.

**You need:**
- **Two S3 clusters**, with an access key and secret key for each.
- **An empty bucket on each:** `demo-source` on A and `demo-dest` on B. If your keys can create buckets, `aws s3 mb` does it; otherwise ask an admin.
- **Plain http on both clusters.** This demo runs everything over http, which is fine for a lab. For https, see [docs/reference/config.md](docs/reference/config.md).

Open **three terminals**:

| Terminal | Used for |
|---|---|
| **T1** | runs `shunt serve` (its log window) |
| **T2** | where you type the commands |
| **T3** | the reader loop |

### Step 1: set up each terminal (T1, T2, T3)

Paste into **each** of the three terminals, after putting in your endpoints and access keys:

```sh
mkdir -p ~/shunt-lab/files && cd ~/shunt-lab
export PATH=~/shunt/bin:$PATH                       # wherever you cloned and built shunt
export AWS_CONFIG_FILE=~/shunt-lab/aws.config AWS_SHARED_CREDENTIALS_FILE=~/shunt-lab/aws.credentials
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export SHUNT_API=http://127.0.0.1:9901
export A_ENDPOINT=10.0.1.10:80  A_AK=CLUSTER_A_ACCESS_KEY
export B_ENDPOINT=10.0.2.10:80  B_AK=CLUSTER_B_ACCESS_KEY
read -rsp 'cluster A secret key: ' A_SECRET; echo; read -rsp 'cluster B secret key: ' B_SECRET; echo
export A_SECRET B_SECRET
echo "$A_AK ${#A_SECRET}  $B_AK ${#B_SECRET}"      # both lengths should be non-zero, e.g. 40
```

Every terminal needs the **same, current** keys. shunt (T1) and the mover (T2) each read the secret from their own terminal, so a stale secret in one terminal is the most common mistake. shunt checks the credentials when you add a cluster, and the mover checks again before copying. Either one tells you which secret is wrong.

### Step 2: aws-cli profiles (T2, once)

```sh
for c in A B; do
  ep=${c}_ENDPOINT ak=${c}_AK sk=${c}_SECRET p=cluster$(echo $c | tr A-Z a-z)
  aws configure set region us-east-1 --profile $p
  aws configure set endpoint_url http://${!ep} --profile $p
  aws configure set s3.addressing_style path --profile $p
  aws configure set aws_access_key_id "${!ak}" --profile $p
  aws configure set aws_secret_access_key "${!sk}" --profile $p
done
aws configure set region us-east-1 --profile shunt
aws configure set endpoint_url http://127.0.0.1:8008 --profile shunt
aws configure set s3.addressing_style path --profile shunt
aws configure set aws_access_key_id SHUNTDEMOCLIENT01 --profile shunt
aws configure set aws_secret_access_key demoClientSecret0000000000000001 --profile shunt

aws --profile clustera s3 ls s3://demo-source/ --summarize
aws --profile clusterb s3 ls s3://demo-dest/ --summarize
```

Both listings should end in `Total Objects: 0`.

This creates three profiles:
- **`clustera`** and **`clusterb`** talk to the clusters directly.
- **`shunt`** is the client, and talks only to shunt.

### Step 3: two files straight to cluster A (T2)

```sh
for i in 01 02; do head -c 1048576 /dev/urandom > files/file-$i; aws --profile clustera s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
aws --profile clustera s3 ls s3://demo-source/demo/
```

Expect `file-01` and `file-02`, each 1048576 bytes. Every file is also kept in `files/`, so later reads can be compared byte for byte.

### Step 4: start shunt (T1)

```sh
umask 077
printf 'credentials:\n  - access_key: SHUNTDEMOCLIENT01\n    secret: demoClientSecret0000000000000001\n    tenant: acme\n' > credentials.yaml
printf 'version: 1\n' > directory.yaml
printf 'listener: { address: "127.0.0.1:8008", plaintext: true }\nadmin: { address: "127.0.0.1:9901" }\nauth: { mode: resign, credentials_file: %s/credentials.yaml }\ndirectory: { file: %s/directory.yaml, poll_interval: 1s }\n' ~/shunt-lab ~/shunt-lab > shunt.yaml
shunt serve -c shunt.yaml
```

The three files do three jobs:
- **`credentials.yaml`** is the client's key; `acme` is just the tenant name it belongs to.
- **`directory.yaml`** is where shunt keeps clusters and buckets. It starts empty.
- **`shunt.yaml`** holds the ports.

Leave T1 running. Every change and every refusal from here on shows up in its log:

```
19:46:10 INFO  [PLAINTEXT] shunt serving  listen=127.0.0.1:8008 admin=127.0.0.1:9901 auth=resign
```

### Step 5: put shunt in front of the bucket (T2)

```sh
shunt cluster add clustera --type vast --scheme http --region us-east-1 \
  --endpoint "$A_ENDPOINT" --access-key "$A_AK" --secret-ref env:A_SECRET
shunt adopt clustera acme/demo-source
aws --profile shunt s3 ls s3://demo-source/demo/
```

- **`--type`:** use `vast`, `minio`, `aws` or `s3`, whichever matches your cluster.
- **`cluster add`:** signs one request with the key pair and refuses it if the cluster rejects them.
- **`adopt`:** serves the existing bucket through shunt under the same name.
- **The listing:** comes through shunt and shows the same two files.

### Step 6: ten more, through shunt (T2)

```sh
for i in $(seq -w 3 12); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l
```

Expect `12`: all on cluster A, the only cluster so far.

### Step 7: add cluster B, live (T2)

```sh
shunt cluster add clusterb --type vast --scheme http --region us-east-1 \
  --endpoint "$B_ENDPOINT" --access-key "$B_AK" --secret-ref env:B_SECRET
shunt expand acme/demo-source --to clusterb --name demo-dest
shunt status acme/demo-source
```

`expand` does four things:
- checks that `demo-dest` exists on B;
- checks that it was never versioned;
- writes, reads back and deletes a canary object;
- records `demo-dest` as the target.

Clients see no change yet. The status shows both clusters, and `(target clusterb/demo-dest)`.

### Step 8: send 50% of writes to cluster B (T2)

```sh
shunt ramp acme/demo-source --ratio 0.5
```

Each key name is hashed, and half of the names now write to B. A given name always goes to the same side. Reads try B first and fall back to A.

### Step 9: read everything back, then 20 more (T2)

Define a checker once in T2. It downloads everything through shunt and compares it with `files/`:

```sh
check() { rm -rf readback && mkdir readback && aws --profile shunt s3 cp --recursive --quiet s3://demo-source/demo/ readback/;
  bad=0; for f in files/*; do cmp -s "$f" "readback/${f##*/}" || { echo "BAD ${f##*/}"; bad=$((bad+1)); }; done
  echo "read $(ls readback | wc -l), mismatched $bad"; }
check
for i in $(seq 13 32); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
echo "A: $(aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l)  B: $(aws --profile clusterb s3 ls s3://demo-dest/demo/ | wc -l)"
check
shunt status acme/demo-source
```

Expect `read 12, mismatched 0`, then `read 32, mismatched 0`.

The 20 new files split by a hash of their names, **not exactly 10/10**. With these names it is 11 to B and 9 to A, so the counts read `A: 21  B: 11` and `WRITES P/S` shows `11/9`. Every file reads back correctly whichever cluster it's on.

### Step 10: all writes to cluster B, 20 more (T2)

```sh
shunt ramp acme/demo-source --ratio 1.0
for i in $(seq 33 52); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
echo "A: $(aws --profile clustera s3 ls s3://demo-source/demo/ | wc -l)  B: $(aws --profile clusterb s3 ls s3://demo-dest/demo/ | wc -l)"
check
```

A's count doesn't change: all 20 new files land on B. Expect `read 52, mismatched 0`.

### Step 11: start the reader loop (T3)

```sh
pass=0; while true; do pass=$((pass+1)); rm -rf rb && mkdir rb
  aws --profile shunt s3 cp --recursive --quiet s3://demo-source/demo/ rb/ 2> err.txt; rc=$?
  bad=0; for f in files/*; do cmp -s "$f" "rb/${f##*/}" || bad=$((bad+1)); done
  echo "$(date +%T) pass $pass: read $(ls rb | wc -l), mismatched $bad, aws exit $rc $(tr '\n' ' ' < err.txt)"; sleep 2; done
```

Leave it running. Every line should say `read 52, mismatched 0, aws exit 0` through the rest of the demo.

### Step 12: migrate the rest (T2)

```sh
shunt migrate start acme/demo-source
shunt migrate run acme/demo-source --until-converged --cursor-dir ~/shunt-lab --ledger-dir ~/shunt-lab
shunt cutover acme/demo-source --window 60s
```

What each command does:
- **`migrate start`:** every write goes to B. Deletes go to both clusters, and listings merge them.
- **`migrate run`:** the mover copies what's still only on A, never overwriting a newer client write. It repeats until a pass copies nothing.
- **`cutover`:** waits 60 seconds, and cuts over only if no read needed cluster A in that time. The reader in T3 is exactly what it's watching.

### Step 13: remove the data from cluster A (T2)

```sh
shunt purge-source acme/demo-source
aws --profile clusterb s3 ls s3://demo-dest/demo/ | wc -l
aws --profile clustera s3 ls s3://demo-source/
```

- **`purge-source`:** refuses unless B has every object A has. It then deletes A's copies **and the `demo-source` bucket on A**.
- **B:** holds all 52.
- **A:** the listing ends in `NoSuchBucket`.

The client still calls the bucket `demo-source`.

### Step 14: shunt forgets cluster A (T2)

```sh
shunt tenant set-default acme clusterb     # new buckets now land on B
shunt cluster remove clustera
shunt status
```

`cluster remove clustera` is refused while anything still points at it, and the refusal lists what does. That's why `set-default` comes first. `status` then lists only `clusterb`. T3 keeps printing `mismatched 0`.

Stop T3, then T1, with Ctrl-C.

### When something goes wrong

| You see | It means |
|---|---|
| `refused: cluster … rejected the signature` | That terminal's secret doesn't match the access key. Fix the secret in **T1** and restart `shunt serve` |
| `… rejected the mover's signature` | T2's secret is stale. Re-run the `read -rsp` line in T2 |
| `refused: … still referenced by tenants.acme.default_cluster` | Run `shunt tenant set-default` first |
| `refused: … ignores If-None-Match: * on PUT` on `migrate start` | Cluster B can't refuse an overwrite, so the mover could clobber a client write. VAST and MinIO don't hit this. Read [docs/migrating.md](docs/migrating.md), then add `--accept-lost-write-window` to both `migrate start` and `migrate run` if you accept that |
| `refused: … cutover … reads still fall back` | Something wasn't copied yet. Run `shunt migrate run … --until-converged` again |
| a line in T3 with `mismatched` above 0 | A real problem. Stop and keep T1's log |

---

## More

- [docs/walkthrough.md](docs/walkthrough.md): the same migration, scripted, with a workload generator (`shunt verify`) that checks every read and write. `make walkthrough` runs it against local Garage and MinIO.
- [docs/migrating.md](docs/migrating.md): the operator how-to, and what each state does.
- [docs/reference/control-api.md](docs/reference/control-api.md): the control API behind the `shunt` commands.
- [docs/reference/config.md](docs/reference/config.md): every config key.
- [docs/adr/](docs/adr/): the design decisions, including the migration races and their windows (ADR-0004).

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE) and [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
