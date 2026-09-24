# ADR-0018: one client bucket over 1 to N backend buckets

Status: proposed (2026-09-23); N1, N2 and N3 (a, b, c) built (2026-09-23). Would amend ADR-0004 (migration races), ADR-0013 (conditional writes
across a migration), ADR-0016 (version fence) and docs/DESIGN.md §2.3–§2.5. N1, N2, N3a, N3b and
N3c are built (below); N4 is not.
Branch: `1-to-n-bucket-support`.

## Context

A client bucket today maps to one backend bucket, or to two while it moves: `source → primary`, with
a ramp that splits keys between them by hash or prefix (§2.5). The directory keeps one backend
name per cluster (`names: {cluster: bucket}`), and `expand` refuses the cluster that is already the
primary. So shunt cannot:

- spread one client bucket's writes over more than two backend buckets, for capacity or to place
  parts of a bucket on different clusters;
- move a bucket between two backend buckets on the **same** cluster (a rename, or consolidation);
- start a second move before the first has finished.

The first hands-on demo ran into all three: an operator wanted `garage-bucket` split across three
backend buckets, one of them on the cluster that was already primary.

## Decision (proposed)

### Legs and ownership

- A placement holds **legs**: `legs: {<leg id>: {cluster, bucket}}`. A leg id is stable,
  directory-generated, and unique within the placement, so two legs may share a cluster.
- The key space is a partition of the 64-bit key hash (the ramp's hash, `fnv1a-fmix64-v1`,
  ADR-0004) into **ranges**, each owned by exactly one leg: `owners: [{from, to, leg}]`, covering
  `[0, 2^64)` with no gaps or overlaps. A key's **owner** is the leg whose range holds its hash.
  Nothing per object is stored: a key's location is a function of its name and the placement, as
  it is today (CLAUDE.md). This is not the object-level location index docs/DESIGN.md §3 rules
  out: the owners table is bounded by the leg cap, not by objects.
- **Cap: 32 legs per placement** (`directory.MaxLegs`), enforced by directory validation and by
  `check-config`. Removal criterion: raised only when the listing benchmark below meets its target
  at the new cap. The cap is not a config key; changing it is an ADR amendment.

### At most one move in flight per placement

- A **move** transfers one range from one leg to another. It carries today's states
  (`RAMPING → MIGRATING → CUTOVER`) and today's ramp *within the range* (a ratio of the range, or a
  prefix rule restricted to it). Outside the moving range, routing is ownership only.
- With one move in flight, every key has at most two possible homes: its owner, and, inside the
  moving range, the leg it is moving from. So every mechanism that is proven for two sides applies
  per move unchanged: the §2.5 routing table, the fence and the hold (ADR-0016), the dual delete
  order (ADR-0004), both-sides conditional checks (ADR-0013), the mover contract, and cutover
  evidence. This is the rule the rest of the design leans on; allowing concurrent moves on one
  placement would need a new race analysis and is out of scope.
- Today's migration is the special case of two legs, one move, and the range `[0, 2^64)`. Its
  behavior, CLI and API are unchanged.

### Steady state with several legs

- **Writes, reads, deletes, conditional writes, multipart:** the owner only. There is no fallback
  read outside a move: the invariant is that a key exists only on its owner.
- **Listing** merges every leg's listing, sorted by decoded key (the existing normalization), and
  **drops each entry whose hash that leg does not own**, except inside a moving range, where both
  of the move's legs are kept and the destination wins on collision, as today. Ownership filtering
  makes leftovers from a move invisible and keeps the listing correct without knowing what each leg
  holds. `ListMultipartUploads` merges the same way.
- **Continuation tokens** stay fixed-size: `{last, done}`, where `last` is the last key handed to
  the client and `done` is a bitmap of exhausted legs. A resumed page asks each live leg for
  `start-after=last` (`marker` for ListObjects V1) instead of carrying 32 backend tokens. This
  trades backend token efficiency for a token that does not grow with N.
- **Upload ids** are prefixed with the leg id instead of the cluster id (§2.4). An id prefixed with
  a cluster id still routes when that cluster has exactly one leg in the placement, and is refused
  as `NoSuchUpload` when it has several, so uploads in flight across the upgrade complete.
- **Bucket-level checks** (versioning must stay off, §2.5) apply to every leg.

### Changing legs

- `expand` adds a leg that owns no range; any cluster, including one that already has a leg.
- A move's cutover is followed by **range purge**: delete the moved range's keys from the old leg,
  after the empty-diff check `purge-source` does today, restricted to the range. `migrate finish`
  (forget without deleting) is allowed only when the old leg owns nothing afterwards; with
  ownership filtering its leftovers would be invisible but would still cost storage and listing
  time.
- **Retire** removes a leg that owns no range. A split (one leg's range becomes two ranges) is a
  directory-only change with no data movement, so a leg can hand over half its keys by moving one
  of the halves.

## Consequences

- **Listing is the cost, and it is permanent for multi-leg buckets.** A single-leg bucket lists
  one backend, as today. An N-leg bucket always lists N backends: latency is the slowest leg's,
  request cost is N times, and a listing fails if any leg is unreachable, because a listing
  missing a leg is silently wrong. Listing availability is roughly the product of the legs'.
  Memory is one page per leg per in-flight listing: at 32 legs and 1 000 keys per page, about
  32 000 entries, which needs its own ADR under the bounded-buffer rule (ADR-0007) and a
  benchmark before the cap can go above 32.
- **Blast radius:** a leg down makes about 1/N of the keys unavailable, and every listing.
- **Directory schema v2.** `names` and `source/primary/target` become `legs`, `owners` and an
  optional `move`. Rollout order: proxies that read v2 ship first, then the control plane writes
  v2 and converts v1 placements (primary → one leg owning everything; a v1 migration → two legs and
  one move). A proxy that sees a v2 placement it cannot route refuses it rather than guess, as a
  proxy does today for an unknown ramp hash.
- **Telemetry:** per-leg labels are bounded by the cap (bucket × ≤ 32). Every new or relabelled
  metric goes into docs/telemetry-catalog.md first.
- **UI:** the bucket view shows legs and the ownership bar; expand adds a leg; a move picks a
  range and a destination leg.
- **Tests.** The fleet property test generalizes to three or more legs with random range moves
  across two proxies, and its negative control (no fence) must still fail. Fuzz targets for the
  owners table and the new continuation token. A listing benchmark at 1, 2, 8 and 32 legs.

## Phasing (each is a gate, not a milestone date)

1. **N1, schema v2 with no new behavior.** Legs, owners, one move; v1 conversion; every existing
   test, walkthrough and demo passes unchanged.
2. **N2, several legs at rest.** Ownership routing, the filtered N-way listing merge, the
   `{last, done}` token, leg-prefixed upload ids, the cap at 32, the listing benchmark.
3. **N3, moves between any two legs.** Range moves, range purge, split, retire, and legs on the
   same cluster (the same-cluster rename).
4. **N4, UI and CLI.**

## N1 as built (2026-09-23)

Schema v2 is read everywhere and written nowhere yet: readers ship first, as the rollout order
above requires. `Placement` carries `legs`, `owners`, `hash` and `move` next to the v1 fields; the
YAML decoder checks them as strictly as any other key, and a JSON hook on `Placement` covers every
JSON reader. Either decoder converts a v2 placement into the v1 fields and clears the v2 ones
before anything else sees it, so routing, the API and the writers are untouched. In v2, `target`
and `cold` name legs. A leg converted from v1 is named after its cluster, so v1 → v2 → v1 is
exact.

```yaml
acme/runs:
  state: RAMPING
  legs: { onprem: { cluster: vast-a, bucket: runs }, cloud: { cluster: aws-use1, bucket: acme-7f3a-runs } }
  owners: [ { from: "0000000000000000", to: ffffffffffffffff, leg: onprem } ]
  move: { range: { from: "0000000000000000", to: ffffffffffffffff }, from: onprem, to: cloud,
          ramp: { hash: fnv1a-fmix64-v1, prefixes: ["2026-09/"] } }
```

During a move the owner is the leg the keys are moving **from**: owners change when the move
completes, not when it starts. Hash bounds are inclusive and written as 16 hex digits, so a JSON
reader without 64-bit integers reads them exactly; quote a bound that YAML would read as a number.
This build refuses, with the reason, the v2 placements only later phases route: more than one
owner (N2), two legs on one cluster or a move of part of the key space (N3), and more than
`MaxLegs` legs.

## N2 as built (2026-09-23)

A bucket several legs own is **spread**. It stays in v2 form in memory (`Placement.Spread()`), with
empty `Primary` and `Names`, and N2 routes it only at rest: ACTIVE, no move, no target, no tier, and
one leg per cluster. `Apply` and `SetTarget` refuse it, so nothing moves it until N3.

- **Creation only.** `create-backend` with `legs` (two to 32) makes one new, empty bucket per cluster
  and splits the key hash space evenly (`EvenOwners`); adopting existing buckets into legs would need
  a move. The UI's Create has a **Spread across clusters** choice.
- **Object requests are narrowed to their owner** (`migrate.Narrow`): the proxy turns the placement
  into a plain one-cluster placement on the leg owning the key, so PUT, GET, HEAD, DELETE, multipart,
  conditional writes and both ends of a copy take the paths they always have.
- **Listings** merge every leg (ADR-0019). HeadBucket and GetBucketLocation go to the first leg. Any
  other bucket-level request, DeleteBucket, versioning, policy, lifecycle and ListMultipartUploads
  among them, answers `NotImplemented`: it would have to reach every leg.
- **Upload ids keep their cluster prefix.** With one leg per cluster the prefix names the leg, so the
  leg-prefixed ids planned for N2 wait for N3, which allows two legs on one cluster.
- **Step-out** reports a spread bucket as a blocker: its keys have to be in one leg first (N3).
- **Backend check first:** `start-after` resumes exactly after a key on Garage, MinIO and VAST. After
  a common prefix they disagree (Garage and VAST in opposite ways), so the merge's rule of dropping
  anything at or before its last name is what keeps a listing exact (docs/reference/backend-compat.md,
  ADR-0019).

## N3a as built (2026-09-23): moving part of a bucket between clusters

- **A move is the two-cluster migration of its range.** `Placement.MoveView` is the move as a v1
  migration: its state, the source leg as source, the destination leg as primary, the ramp limited
  to the range (`ramp.range`, the ratio a share of the range). `Apply` applies a move's steps to that
  view and writes the result back, so every rule of a migration holds for it without a second copy:
  the ramp only grows, holds complete or release, MIGRATING and CUTOVER in order. The proxy narrows
  a key in the moving range to the view (`migrate.Narrow`), and the control plane reasons about the
  view in every step, the mover, cutover and purge (`moving(p)`).
- **A first step with `range`** starts a move: from a plain bucket, whose primary becomes the leg
  owning every key, or from a spread one; the range lies inside what one leg owns; the destination
  is the leg on the target cluster or a new one. This is how a plain bucket becomes spread.
- **When a move ends** the destination owns the range (`reassign`), a source leg left owning nothing
  is dropped, and a bucket one leg owns again settles back to its plain form.
- **Purge is limited to the range** and keeps the source bucket while its leg owns other keys.
  **Finish is refused** then: left in place, the range's copies would be strays on a leg that stays,
  and a later move into that leg would take them for the bucket's own. So no leg ever holds keys it
  does not own, and a move into an existing leg needs no empty check.
- **Proof:** the fleet property test and its negative control run the move of half a bucket too:
  six runs each, 0 violations in 54,855 client operations with the fence, a violation in every run
  without it. Proxy tests route a move of half a bucket through the handler (the in-range writes
  stay on the source with the narrowing removed); a control test runs ramp through purge on half a
  bucket and then the other half, and fails with purge's range filter removed.
- **UI:** the first step of an expanded bucket can move a share of its key space; a spread bucket
  has **Move keys** (a share of one leg's range to another cluster); the Migrations screen shows a
  move as the migration it is, with which part moves.

## N3b as built (2026-09-23): two legs on one cluster

- **A move's roles are legs.** `MoveView` names its source and primary by leg id, keys `Names` by
  them, and maps each to its cluster (`LegClusters`, never stored); `Placement.ClusterOf(role)` is
  the cluster, which is the role itself for every stored placement. Everything that picks a bucket
  (the proxy's route, fallback, conditional check and copy source; the mover; the control plane's
  steps, cutover and purge) picks it by role and asks `ClusterOf` for the cluster. The side a proxy
  answered from is recorded as a role (`outcome.fromSource`), since the cluster no longer tells.
- **A new leg on a cluster that has one** gets its own id (`<cluster>-2`, `-3`, ...). A first step
  names the destination bucket; with no name it is the one leg on that cluster. A plain bucket asked
  to move to another bucket on its own cluster moves every key as a move (the rename), since a plain
  migration's two buckets are on two clusters; it settles back to a plain bucket under the new name.
  CreateSpread still spreads a new bucket over different clusters, since one cluster's keys over two
  of its buckets gains nothing.
- **Upload ids.** `<clusterID>~<backendID>` (docs/DESIGN.md §2.4) gains a form for two buckets on one
  cluster: `<clusterID>.<tag>~<backendID>`, the tag being six hex of the bucket name's SHA-256
  (`migrate.BucketTag`). A proxy issues it only while a request's two buckets share a cluster; every
  other id keeps the old form. An incoming id resolves to the role on its cluster whose bucket the
  tag names; untagged, to the one role on that cluster, or, when a move's two legs share it, to the
  source: an untagged id there was issued before the move.
- **Status** reports clusters in `primary` and `source` as before and adds `primary_bucket` and
  `source_bucket`, since `names` cannot hold two buckets of one cluster. The UI's Expand offers the
  bucket's own cluster ("another bucket here"), which starts the move at once, and Move keys offers a
  new bucket beside the source.
- **Proof:** the fleet property test and its negative control run a move to another bucket on
  garage: 3 runs each, 0 violations in 25,287 client operations with the fence, violations in every
  run without it. A proxy test covers writes, fallback reads, the merged listing, dual deletes, and
  uploads begun before the move (untagged, completing on the old bucket) and during it (tagged,
  completing on the new one); it fails with the bucket looked up by cluster, and with no tag. A
  control test moves a bucket to a new bucket on its own cluster from migrate through purge.

## N3c as built (2026-09-23): consolidation, retire, and step-out of a spread bucket

- **A move may name a leg** (`Transition.Leg`, `leg` on ramp and migrate, `--leg` on the CLI) instead
  of a range: it takes the first range that leg owns, and is then the move N3a built, unchanged. No
  per-key rule changes, so the fleet property test's range and same-cluster runs cover it; naming a
  leg is how its range is chosen, not a new kind of move. A leg owning several ranges moves them one
  move at a time.
- **Consolidating** a spread bucket is that move once per other leg, into the leg kept (or a new
  bucket, as any move's destination). Each purge deletes a source leg's bucket once it owns
  nothing, and the last move settles the bucket to plain. **Step-out** then treats it as any plain
  bucket; until then its blocker says to consolidate. Concurrent moves stay deferred, so a
  consolidation of N legs is N−1 moves in sequence.
- **Retire** is `DELETE …/target` (`shunt expand --clear`) on a spread bucket: it forgets the legs
  that own nothing and take part in no move (`directory.IdleLegs`), leaving their buckets. A first
  step released before it routed anything leaves such a leg. A leg can only come to own nothing
  through a move's end, which drops it, or a release, which never routed anything to it, so it holds
  no keys.
- **A first step measures its destination.** The mover refuses a destination whose conditional-write
  profile is assumed (ADR-0004), and only expand measured it, so a move into a leg that expand never
  prepared stalled at the mover (found live, 2026-09-23). A first step now measures an unmeasured
  destination cluster as expand does, with a `.shunt-probe-<random>` object it deletes at once; on
  a live leg a listing can see that object for the few milliseconds it exists.
- **Split** needs no action of its own: a move may take any range inside one leg's ownership, and
  the owners table is cut where the move's range ends.
- **CLI:** `shunt ramp` and `shunt migrate start` take `--range <from>-<to>` and `--leg`; the result
  says which range moves. **UI:** a spread bucket has **Consolidate** (pick the leg to keep; it
  lists the moves left and starts the next) and **Retire idle leg**.
- **Proof:** a directory test consolidates three legs into one and retires a released destination;
  a control test consolidates a two-leg bucket into a new bucket on one of its clusters through
  migrate, mover, cutover and purge, checking step-out before and after; a UI test drives both
  buttons.

## Open questions

- **Prefix rules.** Today a ramp may go by prefix (`runs/2026-09/`). Ownership here is by hash only.
  Either ownership rules become an ordered list of prefix-or-range → leg (more expressive, harder to
  validate as a partition), or prefix rules survive only inside a move's ramp.
- **`start-after` across backends.** Measured on Garage, MinIO and VAST (2026-09-23,
  docs/reference/backend-compat.md); AWS is not measured.
- **The listing target** that would justify raising the cap: a p99 for a 1 000-key page at N legs,
  measured on the lab clusters.
- **Rebalancing policy.** This ADR provides the mechanism (split, move). Whether shunt ever
  proposes moves itself (by capacity or load) is a separate decision.
- **Concurrent moves (deferred, 2026-09-23; one at a time until revisited).** Moves over disjoint
  ranges would keep every key two-sided, since a key's hash lies in exactly one range; so the
  per-key arguments (routing, dual delete order, conditional checks, the mover's guard, the hold)
  would hold unchanged, and consolidating several legs into one (A→D, B→D, C→D) would qualify,
  the owners being a partition. What is not per key would have to become per move first:
  `moves: [...]` in the directory, each with its own state, ramp, hold and cutover evidence;
  fallback reads and mover convergence labelled by source leg, so one move's fallback reads do
  not block another's cutover; and the fleet property test extended to interleaved steps of
  several moves, with its negative control still failing. Fenced steps would stay one at a time
  per placement.
