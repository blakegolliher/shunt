# Runbook: the control plane has lost quorum

**Signal.** `shunt-control status` on any reachable node says `NO QUORUM`; every `shunt` verb
answers `503 unavailable`; `shunt_fleet_stale` is 1 on every proxy within `lease_ttl`.

**What is still working.** Every proxy serves reads and writes on every bucket that is not
moving, from its last installed directory. That is the data path, and it is unaffected; nothing an
operator does here is urgent for clients on ACTIVE buckets.

**What is not.** Writes to buckets that are `RAMPING`, `MIGRATING` or `CUTOVER` answer 503 with
`Retry-After` on every proxy (stale mode, ADR-0016), and no migration step can be taken. Bucket
creation through a proxy answers 503. None of this touches the ACTIVE buckets above.

**Do.**
1. Find which control nodes are down (`status` on each, or `/-/healthz`). A majority must be up:
   two of three, three of five.
2. Start the down nodes with the same `init` or `join` command line they run under (a restart
   changes nothing on a data directory that holds a member). If a node's host is gone for good,
   bring the majority back first with the nodes you have; then `member remove` the lost one and
   `join` its replacement on a fresh data directory (docs/how-to/run-shunt-control.md).
3. Watch `status`: once it says `quorum`, every proxy's next heartbeat is answered, leases return,
   and moving buckets take writes again. Nothing needs re-running.

**Do not** restore from a snapshot while a majority can still be brought back: a restore rebuilds
a cluster of one and the other nodes then join it, which is the path for losing the data
directories, not for losing a host.

**Afterwards.** A step that was running kept its hold throughout; nothing was released or rolled
back. Check `shunt operation list` for unfinished records. One `blocked` on `owner_lost` lost its
control node: resume it from a live node with `shunt operation resume <id>`. A restarted node
resumes its own. Running the same step again answers `operation_conflict` naming the record
([blocked-operation.md](blocked-operation.md)).
