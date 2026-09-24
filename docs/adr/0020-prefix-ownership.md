# ADR-0020: prefix ownership in a spread bucket

Status: proposed (2026-09-23); P1 built (2026-09-23). Decisions confirmed 2026-09-23: scopes by
longest match, each with its own table; the caps below; the CLI names below; a move's ramp by
ratio only. Amends ADR-0018 (its "Prefix rules" open question). Branch:
`1-to-n-bucket-support`.

## Context

ADR-0018 decides a key's leg by the hash of its name alone. Keys spread evenly, but an operator
cannot say where a part of a bucket lives: every leg holds a scattered share of `archive/`, of
`2026-09/`, of every prefix. Operators need that placement by meaning:

- age a time period (`2026-08/`) onto a cheaper cluster;
- keep one dataset (`models/`) on the cluster next to the jobs that read it;
- move a bucket one prefix at a time, as a whole-bucket ramp can today (`shunt ramp --prefix`),
  but ending with the prefix living elsewhere rather than the whole bucket.

A side benefit: a listing under a prefix that one leg owns needs to read only that leg.

## Decision (proposed)

### Scopes

- A spread placement may carry **prefix rules**: `prefixes: [{prefix, owners}]`. Each rule is a
  **scope** with its own owners table, which partitions the hash space among legs exactly as the
  placement's own `owners` do (ADR-0018). The placement's own `owners` are the scope of the empty
  prefix.
- A key's **scope** is the rule with the **longest prefix the key starts with**, or the empty
  prefix. Its **owner** is the leg its hash falls to in that scope's table. This is a total
  function: every key has exactly one owner whatever rules exist, so no rule set can leave a gap
  or an overlap, and validation only checks that prefixes are distinct and each table is a
  partition. Nothing per object is stored.
- Legs are shared by every scope. A scope's table can name any leg of the placement.
- **Caps:** at most 64 rules per placement, a prefix at most 1024 bytes (the S3 key limit), and at
  most 32 legs over all scopes (MaxLegs, unchanged). Constants, changed by amending this ADR.

### Carving and merging change no key's owner

- **Carve** adds a rule for a prefix with a **copy of its parent scope's table** (the parent being
  the scope its keys belong to now). Every key keeps its owner, so no data moves.
- **Merge** removes a rule whose table equals its parent's, which again changes no owner. A rule
  whose table differs has to be made equal by moves first.
- Because neither changes any key's owner, a proxy on the version before and one on the version
  after route every key alike: neither needs a fence or a hold. Both need ACTIVE with no move in
  flight. A test holds carve and merge to that: over many keys and rule sets, the owner before
  equals the owner after.

### Moves within a scope

- A move gains a **scope** (default the empty prefix): it moves one hash range of that scope's
  table from one leg to another. A key is in the move when its scope is the move's scope and its
  hash is in the range. Keys of a nested rule (`archive/2025/` inside `archive/`) are outside it.
- With one move in flight a key still has at most two homes: its owner, and, inside the move, the
  destination. So every per-key argument of ADR-0018 holds unchanged: routing, the fence and hold
  (ADR-0016), dual deletes (ADR-0004), both-sides conditional checks (ADR-0013), the mover's guard,
  cutover evidence and range purge. That depends on one code path deciding move membership and
  ownership; P1 routes every caller (the proxy's narrowing, the listing merge, the mover, purge)
  through it.
- **"This prefix on that cluster"** is: carve the prefix, then consolidate that scope into the leg
  on the target cluster (ADR-0018 N3c, one move per other leg of the scope). If the target cluster
  has no leg yet, the first move makes one.
- The mover and purge list the source leg **under the scope's prefix only**, and skip keys of
  nested rules. So moving `archive/` reads `archive/`, not the whole bucket.
- When one leg owns every key in every scope, the bucket settles to plain (ADR-0018), rules and
  all.

### Listing

- A listing with prefix P reads only the legs that can hold keys under P: the legs in the table of
  the scope P falls in, and in every rule nested under P. Each entry is kept only by its owner, as
  in ADR-0018; common prefixes are not filtered and are listed once.
- So after `archive/` is consolidated onto one leg, a listing under `archive/` reads one leg.

### Compatibility

- A placement without `prefixes` routes as today. A proxy that does not know `prefixes` refuses a
  placement that has them rather than guess (ADR-0018's rule for unroutable v2), so proxies are
  upgraded first, as in N1.
- The whole-bucket ramp's `ramp.prefixes` (§2.5) is unrelated and unchanged. P1 decides whether a
  move's ramp may also use prefixes, or refuses them.

## Consequences

- Legs may fill unevenly: prefix ownership places by meaning, not by size. shunt does not
  rebalance (ADR-0018, decided 2026-09-23).
- The owner of a key costs a longest-prefix lookup over at most 64 rules, then a hash. It is done
  per request and per listed entry; a benchmark in P1 bounds it.
- Status, the UI's ownership bar and step-out's blockers become per scope.

## Phasing (each a gate)

1. **P1, scopes at rest.** Schema, the owner function, carve and merge (directory, control API),
   validation and caps, the listing's leg selection. Fuzz the owner function; the carve/merge
   invariant test; proxy tests over nested rules; the owner benchmark.
2. **P2, moves within a scope.** `move.scope`, the scoped mover and purge, consolidation of a scope.
   The fleet property test gains scoped moves beside carved rules; its negative control (no fence)
   must still fail. Live run: carve a prefix and move it to another cluster under two verify runs.
3. **P3, CLI and UI.** Carve and merge verbs, `--scope` on ramp and migrate start, status per
   scope, the ownership bar per scope, docs/spread-buckets.md.

## P1 as built (2026-09-23): scopes at rest

- **Schema.** `prefixes: [{prefix, owners}]` on a v2 placement, sorted by prefix, each prefix once
  and non-empty; `hash` is required with them. A placement with rules stays in v2 form in memory
  (`Spread()`), even when one leg owns every key, until its last rule is merged.
- **One ownership path.** `Placement.Scope(key)` is the longest rule the key starts with;
  `migrate.OwnerOf` reads that scope's table; `migrate.InMove` is the one test of move membership,
  and the proxy's narrowing, the listing merge, the mover's filter and purge's filter all call it.
  Moves are refused in a bucket with rules until P2 (by the transition and by the decoder), so a
  move's scope is the empty prefix for now.
- **Carve and merge.** `directory.File.Carve` and `Merge`, in both stores; `POST` and `DELETE
  /v1/placements/{tenant}/{bucket}/prefixes`; `shunt expand <bucket> --carve <prefix>` and
  `--merge <prefix>` (the CLI came in P1, being two flags). Carving a plain bucket makes it v2 with
  its one leg; merging the last rule of a bucket one leg owns makes it plain again, byte for byte.
  Both refuse unless ACTIVE with no move; carve also refuses a bucket with a target, cold tier or
  lifecycle recorded. Refusals are `directory.ErrRefused` (409 `refused`).
- **Listing.** A listing reads only `directory.ListingLegs(p, prefix)`: the legs of the scope the
  prefix falls in, of every rule nested under it, and a move's two legs.
- **Status and step-out.** Status adds `scopes` (each rule's legs) and a leg's `idle` (no key in any
  scope; the UI's Retire idle leg now reads it, since a leg may own keys only under a rule).
  `shunt status` prints a SCOPE column when a bucket has rules. Step-out tells a bucket one leg
  holds but rules keep spread to merge them.
- **Proof.** A property test: over 300 random rule sets whose tables differ, every carve and merge
  leaves every key's owner unchanged (it fails when carve copies the empty prefix's table instead
  of the parent's). `FuzzOwnerOf` holds OwnerOf and InMove to a brute-force longest match (4.5 M
  inputs). A proxy test routes a rule's keys to its one leg and lists under the prefix without
  reading the other leg; it fails with the scope ignored in OwnerOf, and with every leg listed.
  Control and CLI tests carve, show, refuse a move, and merge back to the plain bucket. Cost:
  `Scope` over 64 rules is 444 ns, `OwnerOf` with 3 rules 82 ns, no allocation.

## Open questions

- **CLI names.** Decided: `shunt expand <bucket> --carve <prefix>` and `--merge <prefix>`, and
  `--scope <prefix>` on ramp and migrate start (P2).
- **A move's ramp** is by ratio only within its scope (decided 2026-09-23).
