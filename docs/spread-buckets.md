# Spread buckets: one client bucket over several backend buckets

A client bucket can live in up to 32 backend buckets ("legs"), on one cluster or several. Each key
lives in exactly one leg, chosen by a hash of its name; listings merge every leg. Clients see one
bucket under one name and never know. The design is ADR-0018; this page is how to operate it.

Use it to put part of a bucket on another cluster, to move a bucket a piece at a time, to rename
its backend bucket on the same cluster, or to bring a spread bucket back to one bucket so shunt can
step out of its path.

Every command below takes `--api` and `--token-ref` (or `SHUNT_API` and `SHUNT_API_TOKEN_REF`) like
any operator verb. The UI's Buckets screen does the same things; each section names its button.

## What it costs

- **Listings read every leg.** A listing takes as long as the slowest leg and fails if any leg is
  unreachable, since an answer without a leg would be silently incomplete.
- **A leg down makes its share of the keys unavailable**, and every listing.
- **Bucket-level requests** other than listing, HeadBucket and GetBucketLocation (versioning,
  policy, lifecycle, DeleteBucket, ListMultipartUploads) answer `NotImplemented` on a spread
  bucket: they would have to reach every leg.
- **One move at a time per bucket.** Consolidating N legs is N−1 moves in sequence.

## Create one

```sh
shunt adopt cluster-a acme/wide --spread cluster-b          # new bucket "wide" on each cluster
shunt adopt cluster-a acme/wide --name wide-a --spread cluster-b:wide-b
```

`--spread <cluster>[:<name>]`, once per further cluster, makes a new, empty bucket on each and gives
each an equal share of the key space. A spread bucket is created over different clusters; two legs
on one cluster come from a move (below). UI: **Adopt or create → Create new → Spread across
clusters**.

`shunt adopt <cluster> <bucket> --create` makes a plain new bucket, and without either flag `adopt`
takes over one that already exists.

## See it

```sh
shunt status --all            # every bucket; spread ones get a table of their legs
shunt status acme/wide
```

```
acme/wide is spread over 2 backend buckets:
  LEG        CLUSTER    BUCKET  SHARE  HASH RANGES                        MOVING
  cluster-a  cluster-a  wide    50.0%  0000000000000000-7ffffffffffffffe  -
  cluster-b  cluster-b  wide    50.0%  7fffffffffffffff-ffffffffffffffff  -
```

Hash ranges are 64-bit and inclusive. A leg's id is its cluster, or `<cluster>-2`, `-3`, ... when a
cluster holds more than one. The UI shows the same as a bar over the key space, one color per leg,
with the moving range striped.

## Move part of a bucket

A move takes one hash range from the leg that owns it to another leg, through the states of a
migration (docs/migrating.md): ramp, migrate, the mover, cutover, purge. Everything that holds for
a migration holds for a move, including the held ramp steps whose writes answer 503 with
`Retry-After` for a moment (ADR-0016). The rest of the bucket is untouched.

```sh
# the first half of a plain bucket to another cluster: it becomes spread
shunt ramp acme/data --range 0000000000000000-7fffffffffffffff --to cluster-b --name data-b --create --ratio 0.25
shunt ramp acme/data --ratio 1          # later steps name nothing new
shunt migrate start acme/data
shunt migrate run acme/data --until-converged
shunt cutover acme/data
shunt purge-source acme/data            # deletes only the moved range's keys from the source leg
```

`--leg <id>` instead of `--range` moves the first range that leg owns. The destination is the leg
on `--to` holding the bucket `--name` names, or a new leg. A new bucket on the source's own cluster
is a new leg there (the rename case). The ratio is a share of the range.

`purge-source` compares and deletes the range alone, and deletes the source bucket once its leg owns
nothing. `migrate finish` (forget without deleting) is refused while the source leg keeps other
keys: the moved copies left there would be strays.

UI: **Move keys** on a spread bucket (a share of one leg's range, to any cluster or a new bucket
beside it); the first step of an expanded bucket on Migrations can move part of the key space; and
**Expand** can pick the bucket's own cluster, which starts a move to a new bucket there.

## Consolidate

Bring a spread bucket back to one backend bucket by moving every other leg's keys into the one you
keep, one move each:

```sh
shunt ramp acme/wide --leg cluster-b --to cluster-a --name wide --ratio 0.25
# ... ramp to 1, migrate start, migrate run, cutover, purge-source, as above
# repeat for each other leg (a leg owning several ranges takes a move per range)
```

When the last move is purged the bucket is a plain bucket on the kept leg's cluster, and `shunt
status` shows it as one. Keep the leg whose bucket name is the client bucket's name if shunt is to
step out later: clients going direct use the backend name.

UI: **Consolidate** on a spread bucket: pick the leg to keep; it lists the moves left and starts the
next one, which Migrations then takes through its steps.

A first step measures the destination cluster's conditional-write support if nothing has, as
expand does, with a probe object it deletes at once; the mover refuses a destination whose support
is only assumed (ADR-0004).

## Retire an idle leg

A first step cancelled before its commit (`shunt operation cancel <id>`, ADR-0021) releases its
hold and leaves its destination as a leg that owns no keys. A step that waits on a proxy is not
released by itself: it stays blocked until the proxy drains or the step is cancelled. Forget such
legs with:

```sh
shunt expand acme/wide --clear
```

Their buckets stay on their clusters. UI: **Retire idle leg**.

## Step out

`shunt step-out` refuses a spread bucket: clients going direct need all its keys in one bucket.
Consolidate it first; after that it is a plain bucket and step-out handles it as any other
(ADR-0011, ADR-0012).
