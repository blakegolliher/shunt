# ADR-0018: one client bucket over 1 to N backend buckets

Status: proposed (2026-09-23). Would amend ADR-0004 (migration races), ADR-0013 (conditional writes
across a migration), ADR-0016 (version fence) and docs/DESIGN.md §2.3–§2.5. Nothing here is built.
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

## Open questions

- **Prefix rules.** Today a ramp may go by prefix (`runs/2026-09/`). Ownership here is by hash only.
  Either ownership rules become an ordered list of prefix-or-range → leg (more expressive, harder to
  validate as a partition), or prefix rules survive only inside a move's ramp.
- **`start-after` across backends.** VAST, MinIO, Garage and AWS must honor `start-after` (and
  `marker`) exactly as a lexicographic resume point on the decoded name; it goes into
  docs/reference/backend-compat.md before N2.
- **The listing target** that would justify raising the cap: a p99 for a 1 000-key page at N legs,
  measured on the lab clusters.
- **Rebalancing policy.** This ADR provides the mechanism (split, move). Whether shunt ever
  proposes moves itself (by capacity or load) is a separate decision.
