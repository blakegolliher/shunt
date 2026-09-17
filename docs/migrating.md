# Moving a bucket to another cluster, and removing a vendor

This is the how-to (POC-4, operator verbs since POC-5): how to move a bucket's data from one S3 cluster to another while clients
keep using it, and how to empty and retire a cluster entirely. Clients are not reconfigured, not
restarted, and not told. They keep the same endpoint, the same credentials and the same bucket name
throughout.

For a first try, README.md walks the whole move with nothing but commands: `shunt serve --plaintext`, `shunt cluster add <name> http://<host> --access-key …` (prompts for the secret), and bare bucket names. The examples below use bare bucket names too. A bucket is named `tenant/bucket` only when one shunt serves several tenants (docs/reference/control-api.md).

Everything below is one `shunt` binary. `shunt serve` runs the proxy. The other verbs call its control
API on the admin listener (docs/reference/control-api.md; `--api`, default `http://127.0.0.1:9900`).
Every verb takes `--json`. For the ten-step procedure with expected output, from adopting a bucket to
removing the old cluster, see docs/walkthrough.md.

The design is docs/DESIGN.md §2.5. The races and their windows are ADR-0004. What each backend
actually does is docs/reference/backend-compat.md.

## What the states mean

A placement moves `ACTIVE → RAMPING → MIGRATING → CUTOVER → ACTIVE`. `RAMPING` may be skipped.

| State | Writes | Reads | Deletes | Listings |
|---|---|---|---|---|
| `ACTIVE` | primary | primary | primary | primary |
| `RAMPING` | split by key: hash ratio or prefix | the side that key's rule chose | both | primary |
| `MIGRATING` | new primary | new primary, falling back to the source on 404 | both | merged from both |
| `CUTOVER` | new primary | new primary | both (so the source only loses keys) | new primary |
| `ACTIVE` (after) | new primary | new primary | new primary | new primary |

The ramp only grows. Shrinking is a reconcile, not a state change, so `--ratio` below the current
one is refused: a key that has already moved must not start being written on the old cluster again.

## Moving one bucket

```sh
shunt cluster add garage --type s3 --scheme http --region garage --endpoint 10.0.0.9:3900 \
  --access-key GK… --secret-ref file:/etc/shunt/garage.secret      # live; no restart
shunt status                                  # clusters, what is moving, the write split, the mover
shunt expand data --to garage --create        # target bucket, versioning check, canary
shunt ramp data --ratio 0.01
shunt ramp data --ratio 0.25
shunt ramp data --ratio 1
shunt migrate start data                      # all writes on the new cluster; reads still fall back
shunt migrate run data --until-converged      # copy until a pass copies nothing
shunt cutover data --window 60s               # stop reading the old cluster, once nothing falls back
shunt purge-source data                       # delete the old bucket; or `migrate finish` to keep it
```

`shunt cluster add` checks the credentials before saving the cluster. It signs one request to the cluster with the access key you give and the secret `shunt serve` resolves, and refuses an unknown key or a secret that isn't that key's. `shunt migrate run` makes the same check before copying anything, using the secret *its own* process resolves. An `env:` secret_ref is read from each process's own environment, so a terminal holding a stale secret is caught at this point rather than partway through a copy.

`expand` records the target. Without `--create` the bucket must already exist, so a typo is refused
up front rather than showing up later as a slow trickle of 404s. `--name` overrides the generated
backend name, `<name>-NNN`. You can also skip `expand` and pass `--to`/`--create` to the first `ramp`
or `migrate start`.

**Watch, don't guess.** `shunt status` shows per bucket the state, ratio, writes per side, fallback
reads, and the mover's last pass. Underneath, on the metrics listener:

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
`MIGRATING`, or in `RAMPING` at ratio 1: anywhere else some writes are still going to the source, and
a copied key could go stale. It refuses to start otherwise.

It runs in the `shunt migrate run` process, not in `shunt serve`. It asks the control API for the
placement and the cluster definitions, then resolves their `secret_ref`s itself, so the host running
it needs the same `env:` or `file:` secrets as the proxy. It reports each pass to the API, which is
what `shunt status` shows and what `cutover` checks.

```sh
shunt migrate run data --until-converged                   # passes until one copies nothing (max --max-passes 10)
shunt migrate run --from minio                             # one pass over everything moving off minio
shunt migrate run --from minio --accept-lost-write-window  # when the target lacks conditional PUT
shunt migrate run data --dry-run                           # list, do not copy
shunt migrate run data --cursor-dir /var/lib/shunt --ledger-dir /var/lib/shunt --ledger-bucket audit
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
- **It resumes.** It keeps one cursor file per placement in `--cursor-dir`. A completed pass clears
  the cursor, so the next run is a full pass, not a continuation from the last key.
- **It leaves a record.** It writes one JSONL line per object to `--ledger-dir`: size, both ETags,
  part count, which guard it used, and the outcome. `--ledger-bucket` also uploads the ledger to a
  bucket.

`cutover` requires the mover's last pass to have copied nothing: that is what `--until-converged`
repeats for. The report is held in the proxy's memory, so after a proxy restart, run the mover again
before cutting over.

### Into a cluster without conditional writes, the mover can lose a client write

If the target cluster's `capabilities.conditional_write` is `false` (Garage 2.3.0 today; re-run
`shunt probe` to check yours), the mover cannot ask the backend to refuse an overwrite. It HEADs
the target, and if the key is absent it writes the source's copy. **A client write to the same key
between that HEAD and the mover's PUT (or `CompleteMultipartUpload`) is overwritten with the older
source bytes.** The client already got 200 for its write, and nothing reports the loss. The window is
one round trip per object, repeated for every object the mover copies.

The check lives in two places, and both give the same refusal, naming the window and this section:

- `shunt migrate start` refuses such a target unless given `--accept-lost-write-window`;
- `shunt migrate run` refuses it unless given `--accept-lost-write-window`, whatever state the placement is in.
  That catches a mover run on a placement `RAMPING` at ratio 1, which never went through
  `migrate start`. With `--from`, one refused placement stops the whole run before anything is copied.

Neither flag changes what the mover does. Each is the operator saying the point below has been
dealt with. The refusal is keyed on the profile, so a cluster whose `conditional_write` is unset is
taken at its default, true: fill the profile from `shunt probe`.

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

A delete during `RAMPING`, `MIGRATING` or `CUTOVER` goes to the source first and the primary second. If the
source accepts it and the primary refuses it (5xx, timeout), **the client gets the primary's error,
the object is gone from the source, and it is still on the primary**. Reads still return it, since
the primary answers first. This is visible as
`shunt_migration_dual_delete_total{outcome="primary_failed"}` and an error log line naming the
bucket and request id. What to do: nothing if clients retry failed deletes, as every S3 SDK does,
because the retry removes it from the primary. Otherwise, re-issue the delete for the logged request
before `purge-source`.

## Cutting over, and purging the source

`shunt cutover` moves `MIGRATING → CUTOVER` only when both of these hold:

- the mover's last reported pass copied nothing and failed nothing, and
- this proxy served no fallback read of the bucket during `--window` (default 60s).

The call waits out the window, then records the evidence on the placement (`cutover:` in the
directory). Choose a window that covers your clients' read patterns. A bucket that is read once an
hour needs an hour-long window, or a scripted read of its cold keys during a shorter one.

**With several proxies,** the fallback counter is per proxy: hold the window on each proxy's API, or
check `shunt_migration_fallback_reads_total` across the fleet before cutting over. P4 aggregates
it.

In `CUTOVER`, reads and listings use the new primary alone. Deletes still reach the source, so the
source only ever loses keys. Then either:

- `shunt purge-source data`: refused unless the placement is in `CUTOVER` with recorded
  evidence *and* a full listing of both buckets finds no source key that the primary lacks (each
  candidate is confirmed with a HEAD on both sides, so clients deleting meanwhile do not trip it). It then
  aborts the source's in-progress multipart uploads, deletes every object and the bucket, and returns
  the placement to `ACTIVE` without a source. The refusal names the first 20 missing keys: run the
  mover again, or find out why they are missing, before retrying.
- `shunt migrate finish data`: returns to `ACTIVE` and leaves the source bucket untouched and
  unreferenced, for you to keep or delete yourself.

## Taking shunt out of the path

Nothing about a migration keeps shunt in front of the clients afterwards. When a tenant's buckets
have arrived on one cluster, clients can go back to talking to that cluster directly:

```sh
shunt step-out                                # read-only: what stands in the way, or how to leave
```

It checks, against the cluster itself, that every bucket is `ACTIVE` there under **the name clients
use**, that nothing is in flight, and that every client key shunt holds is one the cluster accepts
and can use on every bucket. It changes nothing and exits non-zero while anything blocks.

Two of those are decided long before you run it:

- **Keys.** In resign mode clients sign with keys shunt issued. If shunt holds the *cluster's* keys
  instead (the brownfield case: clients keep the keys they already had, docs/DESIGN.md §11), they
  keep working when shunt leaves. After a move to another cluster, the same key has to exist on that
  cluster too.
- **Names.** `expand` names the target bucket `<bucket>-001` by default, and a bucket created through
  shunt gets a generated backend name. S3 cannot rename a bucket, so pass `--name <bucket>` at expand
  time if you want the option of leaving later. `expand` prints a note when the names differ.

When the check passes it prints the three steps shunt cannot take for you: point the clients' S3 name
at the cluster (DNS or the VIP; lower the TTL a day ahead, and the cluster needs its own certificate
for that name), wait out the TTL while `shunt_requests_total` stops rising, then stop shunt. Until you
stop it, pointing the name back at shunt undoes the whole thing: step-out changed nothing.

## Removing a vendor entirely

The same steps, once, for every bucket a cluster still holds:

```sh
shunt migrate start --from minio --to garage --create   # refused: garage ignores If-None-Match: * (see above)
shunt migrate start --from minio --to garage --create --accept-lost-write-window   # writers quiesced
shunt migrate run --from minio --until-converged --accept-lost-write-window
shunt status                                            # fallback reads flat? then:
shunt cutover --from minio --window 60s
shunt migrate finish --from minio                       # or purge-source, bucket by bucket
shunt tenant set-default e2e-b garage                   # every tenant still defaulting to minio
shunt cluster remove minio
```

`migrate finish --from` reports what still points at the cluster. Moving every bucket is not the
same as being rid of it: a tenant whose `default_cluster` is still the old cluster would put its next
new bucket back on the machine you are about to switch off. `shunt cluster remove` is refused while
any placement or tenant default still names the cluster, and the refusal lists each one.
`test/e2e/evacuate.sh` does this for a whole cluster.

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

`test/e2e/walkthrough.sh` (`make walkthrough`) runs docs/walkthrough.md unattended, with `shunt
verify` reading and writing throughout and failing the run on any error. `test/e2e/demo.sh` does a
bucket move against the e2e Garage and MinIO with nothing but aws-cli:
1,000 objects including multipart ones, a writer that never stops, the ramp, the mover, the cutover,
then every object verified by GET and ETag with an empty listing diff, the change log, the ledger,
and the latency the client saw before, during and after.

```sh
make e2e-up
make run-mixed                              # in another terminal
test/e2e/demo.sh --from minio --to garage   # or --from garage --to minio
```
