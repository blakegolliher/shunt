# ADR-0016: a routing change is in effect only when every proxy has it

Status: accepted (POC-6 item 3, 2026-09-21). Amends ADR-0004 (migration races), ADR-0008 (the control API), docs/DESIGN.md §2.5 and §12.6. Source: POC-6 item 3; docs/design/distributed.md §12.6.

## Context

Several proxies can serve one directory: each polls the file (`directory.poll_interval`, 1 s) and routes by whatever version it last read. Until now a ramp step, `migrate start` or `cutover` took effect the moment it was written, and each proxy picked it up on its own schedule. Three things follow, and none of them shows up with one proxy:

1. **Split writes.** A ramp step moves some keys' writes from the source to the target. For up to a poll interval, one proxy sends key K to the target and another still sends it to the source. A client that writes K through the first and then again through the second gets 200 both times. The target holds the older copy, reads try the target first, and **the second write is lost**. Widening reads does not help: both copies exist and the wrong one wins.
2. **Stale reads.** An out-of-range key during `RAMPING` is read from the source only, because "the target cannot have it" (`migrate.Decide`). That holds for one proxy. It does not hold once another proxy with a wider rule has written the key to the target.
3. **Evidence from one proxy.** `cutover` watched the fallback-read counter of the proxy that served the API call. Reads through the others were invisible to it.

## Decision

### The fleet

- **One control node.** The control API (`internal/control`, ADR-0008) runs on every proxy, but only one of them is the fleet's control node. A proxy whose config names another node (`control.endpoint`) is a **member**. It sends that node a heartbeat and refuses mutations on its own API, naming the node. A proxy with no `control:` block is its own control node with no members, which is what every lab, `shunt serve --plaintext`, and the README demo run.
- **Heartbeat.** Every `control.heartbeat_interval` (1 s), a member calls `POST /v1/fleet/{id}/heartbeat` with its id, its start time, the directory version it has installed, and its `shunt_migration_fallback_reads_total` value for each non-ACTIVE bucket. That keeps the heartbeat's size bounded by migrations in flight, not by buckets. The answer carries the current version and the lease TTL.
- **Membership outlives liveness.** The first heartbeat from an id makes it a **member**. Membership is written to `<directory file>.fleet.yaml`, with ids only, written on join and on forget, so it survives a control-node restart. Liveness stays in memory: a member is **live** while its last heartbeat is younger than `lease_ttl` + 5 s. A member stops being one only through `shunt proxy forget <id>`, an operator's statement that the proxy is gone. §12.5's leased `/proxies/<id>` key needs the same split in P3c. If a lease expiry erased membership, a partitioned proxy would disappear from the one check that has to wait for it (see "Leaving ACTIVE").
- **Stale mode.** A member whose last successful heartbeat was **sent** more than `lease_ttl` (10 s) ago is stale. It answers writes and deletes on every non-ACTIVE bucket with `503 ServiceUnavailable` and `Retry-After: 1`, and serves everything else. Measuring from the send time means the member always stops before the control node's `lease_ttl` + 5 s drop point. `/-/healthz` does **not** go to 503 when a member is stale. §12.6 says readiness should report degraded so the L4 tier drains the proxy, but when the control node is down, every member is stale at once. Draining on stale would turn a control-plane outage into a full data-path outage, which §12.1 rules out. Staleness is reported by `shunt_fleet_stale`, `/-/fleet`, and the log, and it is for alerting only. Two rules close the gaps the fleet property test found, where both produced real violations:
  - **A heartbeat renews the lease only once the member has installed the version the control node answered with.** A member coming back from silence may have missed steps that went ahead without it. If the answer alone made it fresh, it would write a moved key to the source for one poll interval while other proxies write it to the target: a lost write (§12.6 says "until it has reconnected **and applied** the current revision"; the first implementation renewed on the answer alone).
  - **A stale member reads every key of a moving bucket target first, then source.** A step may have gone ahead without it and moved key K, and another proxy may have written K to the target. Reading K from the source only, as its old snapshot says, returns the older copy or a 404. Target-first is correct whatever step the member missed: it writes nothing to a moving bucket, and the target only ever holds the newer copy. This is the one place where read-widening, withdrawn as a per-change rule, belongs.

### The fence

- **Fenced changes** are the ones that move routing for keys that already exist: a ramp step (including the first one), `migrate start`, and `cutover`. Each is written at version V, and the API call answers once every live member has installed V. The CLI prints the names it is still waiting on. If the wait (`wait`, default 30 s) runs out, the answer is `pending` with those names, and nothing claims the change is in effect.
- **Precondition.** A fenced change first waits for every live member to be at the current version. So at most two adjacent routing versions of a bucket are ever live in the fleet, and the next step is always taken from a committed one. The fleet table is all the state this needs.
- **Members only.** The control node installs its own writes synchronously, so it is never waited on. With no members, the fence costs nothing and every command answers exactly as before.

### The hold (closes split writes)

When the fleet has at least one member, a step that moves writes from the source to the target is written in two versions:

1. **V1, the hold.** The step's new ramp is recorded as `ramp.hold` next to the ramp in force. A key inside `hold` but outside the ramp is **held**. Its writes answer `503 ServiceUnavailable` with `Retry-After: 1`, which every SDK retries, and its reads go target first, then source. Nobody writes a held key to the target, so a proxy still on V0 can keep reading it from the source only, and be right.
2. **V2, once V1 is on every live member.** The ramp becomes the step's ramp and `hold` is cleared. Held keys now write to the target. A proxy still on V1 refuses them, so nobody writes them to the source.

A step that starts in ACTIVE is held as `RAMPING` with a ratio of 0 and the step in `hold`. `migrate start` from a ramp below 1 is held with `hold.ratio: 1`, then written as `MIGRATING`. At ratio 1 it moves no writes, so it needs one fence round and no hold. **If V1 does not reach every member within the wait, the step is undone**: the hold is removed, or the bucket goes back to ACTIVE with its target recorded, exactly as it was. That rollback is always safe. While V1 rolls back, some proxies hold the keys and the rest write them to the source, and nobody writes them to the target.

**Cost:** writes to the keys a step moves get two fence rounds of 503s, about 2 s with a 1 s heartbeat. Keys outside the step, and all reads, are never refused. With one proxy nothing is held. A single process only ever moves forward through versions, so a write that starts after another has finished always sees a rule at least as new, and a split write cannot happen.

**Held steps survive an interrupted call.** If the control node stops between writing a hold and completing or releasing it, the hold stays in the directory and its keys keep answering 503. Repeating the step completes it: the call fences the existing hold, then writes the step, instead of refusing because a hold is "already in progress". A different step is refused, naming the step to repeat. The control node warns at startup for every held bucket, and `status` shows `held→<ratio>`. Completing is a compare-and-swap on the hold, checked inside the directory's write lock (`Transition.Complete`). Fenced steps on one bucket are also serialized on the control node. Without both, a call whose hold another call had just released could complete it anyway and move writes that no proxy held.

### Leaving ACTIVE

A stale member refuses writes only on buckets it knows are moving. A member cut off before a bucket's first step still sees that bucket as ACTIVE and keeps writing every key to the source, and nothing on that proxy can tell it otherwise. So **a fence round that takes a bucket out of ACTIVE waits for every member, live or not**. The round is the precondition plus V1 of the hold. A member that does not answer blocks it, by name, until it comes back or until `shunt proxy forget <id>` removes it. Every later round waits only for live members, because a member that falls silent after V1 already holds a non-ACTIVE snapshot and stops itself when its lease runs out.

### Fleet-wide cutover evidence

`cutover` reads the fallback-read count of the control node and every live member at the start and the end of the window. It refuses if the total moved, and also if any live member fails to report after the window starts. A member that falls silent during the window is not evidence of quiet. `purge-source` refuses until the cutover version is on every live member.

## Consequences

- **The README demo and `make walkthrough` do not change.** One proxy means no members, no hold, and no waits. `make readme-demo` and `make walkthrough` check that on every commit.
- **New surface:**
  - config `control: {endpoint, token_ref, proxy_id, heartbeat_interval, lease_ttl}`;
  - API `POST /v1/fleet/{id}/heartbeat`, `GET /v1/fleet`, `DELETE /v1/fleet/{id}`;
  - CLI `shunt proxy list` and `shunt proxy forget <id>`;
  - admin `GET /-/fleet`;
  - directory field `ramp.hold`;
  - metrics `shunt_fleet_members`, `shunt_fleet_stale`, `shunt_fleet_fence_wait_seconds`, `shunt_migration_refused_writes_total`.

  `proxy` is a new top-level verb, added to the subcommand list in §2.10 and CLAUDE.md by this ADR.
- **Hardening from review (2026-09-21):**
  - An unreadable `fleet.yaml` fails closed with 503. It never reads as an empty fleet, which would silently turn the fence off.
  - The member's lease is measured on the monotonic clock, so a wall clock stepped backwards cannot extend it.
  - A member registers with one synchronous heartbeat before it serves anything, so a bucket's first step never runs without waiting for a proxy that has just started. One gap remains: a brand-new member that cannot reach the control node at startup starts stale and unregistered. A restarted member is unaffected, since its id is already a member.
  - The default proxy id is sanitized and shortened to fit 64 characters, and an invalid one fails startup rather than every heartbeat.
  - A stale proxy also reads a moving bucket target-first when it is a copy's source.
  - `shunt_fleet_members` is refreshed on every directory poll, so a silent member can be alerted on.
- **A dead member costs an operator one command,** and only at a bucket's first step. Once a migration is under way, a dead member is dropped when its lease ends.
- **A control-node restart** forgets liveness but not membership, so the first fenced change after a restart waits one heartbeat for members to check back in.
- **The fleet table is per proxy, not per object** (CLAUDE.md). It is bounded by proxies, and each heartbeat is bounded by migrations in flight.
- **The file backend still needs a shared directory file.** Members read it directly. The control node is the one place that knows the fleet, but not the one place that holds the directory. P3c replaces both with etcd behind `shunt-control`. The protocol stays the same, and the fleet table becomes leased keys plus a membership list.
- **Tests:**
  - `internal/control`, with a fake clock and two members: fence, precondition, strict first step, rollback of a hold, fleet cutover evidence, and forget;
  - `internal/migrate`: the held routes;
  - `internal/proxy`: stale mode and held writes;
  - `internal/proxy/fleet_property_test.go`: two proxies over one directory file, B reloading 60 ms behind A and a member through the real `Membership` code, clients owning disjoint keys and writing, deleting and reading each through A or B at random across seven ramp steps and `migrate start`, with B partitioned from the control node for one step. Every read must return the last acknowledged write. Its negative control runs the same steps straight into the directory, without the fence, and must find a violation, and it finds about 20 per run. 10 runs of each: the fenced run held 0 violations in about 264,000 operations, and the unfenced run failed 10 of 10;
  - `test/e2e/fleet.sh`: two real proxies, with one paused by SIGSTOP.

## Amendment (P3c-1, 2026-09-22): the fleet moved onto the control plane

The first implementation kept the fleet table in the control proxy's memory with a membership
file beside the directory file, and members read that directory file directly. That made a
multi-host fleet depend on a shared filesystem, which shunt must never do. P3c-1 (ADR-0015)
moved it:

- **The control node is `shunt-control`**, not a proxy. Members name the control nodes in
  `control.endpoints`; a proxy serves no operator API of its own in a fleet. The lab proxy's
  in-process API remains for single-node use and takes no members (`control.NoFleet`).
- **The fleet table is in etcd:** a persistent record per member (`/shunt/fleet/members/<id>`,
  written by the first heartbeat, removed by `shunt proxy forget`) and a leased key per heartbeat
  (`/shunt/fleet/proxies/<id>`, `lease_ttl` + 5 s). Membership outlives the lease, as this ADR
  requires, and any control node serves any member's heartbeat.
- **A member's directory, client keys and cluster secrets come from `GET /v1/directory`**, long
  polled, and are kept in the member's own `cache_dir` for a restart with the control plane down.
  `CreateBucket` and `DeleteBucket` through a member are forwarded to the control plane.
- **The lease renews only once the member has installed the answered version** and a stale member
  reads moving buckets target-first, as the review of the first implementation found necessary;
  both carried over unchanged into `internal/member`.
- **A fenced change syncs the control node's copy first** (`Store.Sync`): a node whose watch lags
  another node's write takes its decision on the current directory, not its last one.
- **Everything the fleet property test and `make fleet` proved before, they prove again** over a
  real control plane: 0 violations in about 29,000 operations with the fence and a negative
  control that fails without it; three control nodes and two proxies, with quorum lost under a
  256-worker workload on an ACTIVE bucket at zero errors, a proxy restarted with the control plane
  down serving from its cache, and the hold, the strict first step and forget as before.
- **The gap left:** a brand-new member that cannot reach the control plane at startup starts stale
  and unregistered, so a bucket's first step does not wait for it until it registers. A restarted
  member is unaffected, since its id is already a member.
