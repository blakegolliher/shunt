# §12. Distributed control plane — 1000 proxies, no external services

Referenced from `docs/DESIGN.md` §12. Supersedes §1.5 where they differ: Postgres is replaced by etcd embedded in `shunt-control`. ADR-0015 records the decision.

## 12.0 Where this sits

This section is the fleet-scale form of work that starts on a single proxy. `docs/POC-6.md` is the
branch's near-term plan; its item 3 (a version fence across proxies) fixes the hazard named in §12.6
for one control plane, and this section generalizes the same semantics onto etcd. The order is
**POC-6 → P3c → P3d → P3e** (`docs/prompts/`). POC-6 items 1 and 2 (conditional writes across a ramp,
cross-cluster copy) are already done on this branch and are prerequisites, not alternatives: the
fence only makes a correct single-proxy rule correct across a fleet.

Two reconciliations were made when this section landed in the repo, both recorded in `docs/POC-6.md`:

- **Fleet liveness is control-plane state, never directory state.** POC-6's sketch put a `proxies:`
  record in the directory file, which would make the file a write target for every proxy through one
  flock. Instead: proxies heartbeat to the control API, which holds the fleet table in memory in the
  single-node form and as a leased key (§12.5) in the etcd form. P3c then swaps the registry's
  storage, not its protocol.
- ~~**Read-widening is permanent, not per-change.**~~ Withdrawn by ADR-0016: widening reads fixes
  a stale read, not a split write. The hold in §12.6 closes both, and with it `Decide`'s
  "source only" read stays correct unchanged.

**P3c-1 (2026-09-22) built §12.3, §12.4, §12.5 and §12.7 in their first form** (ADR-0015 accepted with
amendments: no etcd client port, a directory version key rather than the revision, a membership
record beside the lease, sealed secrets, full snapshots, TLS deferred). What remains of §12 is P3d
(movers as workers, fleet decisions), P3e (the web UI), and a P3c-2 for deltas, the object-storage
bootstrap and audit export, and TLS.

## 12.1 Goals

- Any number of proxies (design target: 1000 in one datacenter) served by one control plane.
- One command adds or removes a cluster, creates or moves a bucket, or sets a bucket or cluster read-only, and the change is applied fleet-wide with a known point at which every proxy has it.
- No service to stand up beyond shunt's own binaries: no etcd install, no Postgres.
- A proxy that cannot reach the control plane keeps serving and cannot corrupt a migration.
- The request path never touches the control plane. Unchanged from §1.3.

## 12.2 What needs strong consistency

Only directory writes: cluster add/remove, tenant and credential changes, placement create/delete, state transitions, ramp rules, read-only flags, migration job records and range claims. These are small records with compare-and-swap semantics on a single ordered log. Everything else is eventually consistent by design: proxies observe directory versions in order; mover ledgers and metering are append-only files in object storage; dashboards are Prometheus.

## 12.3 Substrate: etcd embedded in `shunt-control`

`shunt-control` embeds an etcd member using `go.etcd.io/etcd/server/v3/embed`. Three control nodes form the cluster (five where two simultaneous failures must be tolerated). Each control node also runs the HTTP control API and the directory fan-out for proxies, backed by its local etcd client. There is no separate etcd process and no separate database.

Why not the alternatives:

- **Own Raft** (`go.etcd.io/raft`, Apache-2.0; `hashicorp/raft`, MPL-2.0): the library is the easy part; snapshotting, compaction, membership change, backup/restore, and the failure modes are months of hardening that etcd has already done.
- **Postgres**: correct for the writes, but it is a service to run, and it lacks watch, lease, and revision, each of which would be rebuilt above it.
- **Gossip** (`hashicorp/memberlist`, MPL-2.0): eventual consistency is the wrong guarantee for the thing that decides where writes go.

What etcd gives that the design uses directly: **revision** (the directory version and the fence counter, for free), **watch** (push, so control nodes see changes without polling), **lease** (proxy liveness and mover work claims with TTL), **transactions** (compare-and-swap on revision, so any control node can write; no leader election in shunt's own code).

Licenses: etcd server and client Apache-2.0 (carry etcd's NOTICE into `THIRD_PARTY_NOTICES`); `go.etcd.io/raft` Apache-2.0; `go.etcd.io/bbolt` MIT; `google.golang.org/grpc` Apache-2.0; `go.uber.org/zap` MIT. These live only in `shunt-control`; `bin/shunt` links none of them. Pin etcd to a minor version and upgrade one minor at a time, per etcd's own rule.

## 12.4 Topology

```
 clients ──▶ L4 ──▶ N proxies (shunt)            three config values: control endpoints, identity cert, listener
                       │  long-poll /v1/directory?since=<rev>
                       │  heartbeat: lease keepalive + applied=<rev> + per-migration counters
                       ▼
                3–5 shunt-control nodes           each: embedded etcd member + control API + fan-out cache
                       │  local etcd client: watch, txn, lease
                       ▼
                   embedded etcd (Raft)           3 or 5 voters; quorum = majority
                       ▲
     movers (M) ── claim ranges (lease), cursors ─┘        ledgers → object storage
     operator CLI ── REST ──▶ any control node
```

Proxies never open an etcd connection. Each control node holds one etcd watch and serves a thousand proxies from its cache, so consensus sees three to five clients regardless of fleet size. Proxies connect to any control node; a proxy list or VIP in front of the control nodes is the only discovery needed.

Dev and single-node labs keep the file directory backend and the in-proxy control API from POC-5. The etcd backend is a second implementation of the same `Directory` seam and the same REST API; the CLI does not change.

## 12.5 Key layout in etcd

Per-record keys, never one document (etcd caps value size; per-key writes are what CAS is for):

```
/shunt/v1/clusters/<name>                     type, scheme, endpoints, region, capability profile (measured|assumed), flags (readonly)
/shunt/v1/tenants/<tenant>                    default cluster, quotas
/shunt/v1/credentials/<access-key>            tenant, encrypted secret, status
/shunt/v1/placements/<tenant>/<bucket>        state, primary, source, names, ramp {rule, hash id, phase}, readonly, pending change
/shunt/v1/proxies/<id>                        LEASED (TTL ~10 s): applied revision, health, counters for non-ACTIVE placements
/shunt/v1/migrations/<id>                     job: placement, state, counts
/shunt/v1/migrations/<id>/ranges/<n>          LEASED claim by a mover: prefix range, cursor
/shunt/v1/changes/<rev>                       audit record: actor, before, after, time (compacted to object storage on a schedule)
```

Size: a million placements is roughly 300 MB of etcd state, inside the default quota. Proxies bootstrap from a periodic compressed directory snapshot written to object storage and stamped with its revision, then long-poll forward from that revision; a cold proxy never replays history.

## 12.6 The protocol

**Version fence.** Every change is written at an etcd revision R. Each proxy reports `applied=<rev>` on its leased key when it has swapped in a snapshot at or past R. A change is *committed* when every live proxy (every proxy holding a lease) reports `applied ≥ R`. The CLI reports progress by name ("waiting on proxy-0417, proxy-0912") and never claims a change is in effect before it is.

**Ramp changes are two-phase: reads widen first, writes move second.** The hazard is a key entering the target range while some proxies still hold the old rule: old-rule proxies write it to the source, new-rule proxies to the target, and an old-rule read is source-only. So:

1. Revision R1 sets the read rule to target-then-source for the *union* of the old and new ranges. Writes are unchanged.
2. When R1 is committed (all live proxies acked), revision R2 moves writes to the new range.
3. Optionally, when R2 is committed, R3 narrows reads back to the new range (a wider read rule is always safe, only slower on misses).

**Amended by ADR-0016 (POC-6 item 3): the hold replaces reads-widen-first.** Widening reads at R1
does not stop the split write: while R2 rolls out, one proxy writes a key to the target and another
still writes it to the source, and the target's older copy wins the read. Instead R1 records the
step as `ramp.hold`: the keys it moves answer writes with 503 + `Retry-After` and read
target-then-source on every proxy that has R1. R2, once R1 is committed, moves their writes. No
proxy writes a moving key to the target while another writes it to the source, and a proxy on R0
reading such a key source-only is still right. A hold that does not commit within the wait is
released (R1 undone), so a step happens everywhere or nowhere. With one proxy nothing is held.

*Reconciliation with the code as it stands (superseded by the amendment above; kept for the
record).* This sequence assumes reads narrow to the write rule,
which is true today: `internal/migrate.Decide` routes an out-of-range key during `RAMPING` to the
source with **no fallback**, because "the target cannot have it" — a statement that stops being true
the moment any proxy in the fleet holds a wider rule. Making that read `source → primary`
permanently is phase 1 done once instead of once per change, and it costs a second lookup only on a
genuine miss. With it, a ramp change needs one fence round (the write move) rather than two. Whether
the write move still needs its round is the question the §12.9 stale-proxy property test must answer
before this is written down as settled; the fence stays until it does.

The same sequence applies to ratio increases, prefix additions, and ACTIVE → RAMPING. Cutover only narrows reads (target-then-source → target-only) once all writes already land on the target, so it is single-phase; `purge-source` waits until cutover is committed.

**Stale mode.** A proxy that cannot renew its lease for longer than the TTL enters stale mode: it keeps serving everything on ACTIVE placements from its local snapshot cache, and it refuses writes with 503 (and `Retry-After`) on any placement in RAMPING, MIGRATING, or CUTOVER until it has reconnected and applied the current revision. A partitioned proxy can reduce a migration's availability; it cannot corrupt one. Its readiness endpoint reports degraded so the L4 tier drains it. The same rule applies to a proxy starting while the control plane is unreachable: it serves from its cached snapshot in stale mode.

*Amended by ADR-0016.* (1) Readiness does **not** drain a stale proxy: when the control plane is
down every proxy is stale at once, and draining them all turns a control-plane outage into a
data-path outage, which §12.1 forbids. Staleness is an alert (`shunt_fleet_stale`), not a
routing signal. (2) Stale mode alone does not protect a bucket's **first** step: a proxy
partitioned before it still sees the bucket ACTIVE and writes every key to the source. So a fence
round that takes a bucket out of ACTIVE waits for every **member**, live or not, until it answers
or is forgotten (`shunt proxy forget`), and membership must outlive the lease: in etcd that is a
membership record beside the leased `/proxies/<id>` key, not the leased key alone.

**Fleet-wide decisions.** Cutover windows, ramp holds, and convergence read counters carried on every live proxy's heartbeat, aggregated by the control node that runs the check. The counters exist only for non-ACTIVE placements, so heartbeat size is bounded by migrations in flight, not by buckets. Prometheus remains the dashboard and alerting path; it is never the decision path.

**Read-only.** A placement flag and a cluster flag, applied through the fence like any change. Writes answer 503 with `Retry-After` by default so SDKs back off; `--reject` switches to 403 so they fail fast. `shunt cluster readonly <name>` is the maintenance command for a whole backend.

**Movers are workers.** A migration is split into prefix ranges. Movers claim ranges with leases, advance a cursor per range in etcd, and append ledgers to object storage. Any number of movers can run; a double claim after a lease expiry is harmless because copies are conditional. This is the claim protocol from vamoose on a substrate built for it.

**Control-plane loss.** With quorum: everything works. Without quorum (two of three control nodes down): the data path is unaffected on ACTIVE placements; proxies continue with their last snapshot; leases cannot be renewed, so every proxy enters stale mode for transitional placements; no directory change is possible until quorum returns. The alert is quorum loss, not proxy errors.

## 12.7 Operating the embedded cluster

`shunt-control` exposes etcd's lifecycle as its own subcommands so an operator never sees an etcd flag:

- `shunt-control init --name c1 --peer-url https://c1:2380 --client-url https://c1:2379` starts the first member and creates the internal CA (control-to-control and proxy-to-control TLS are generated at init; k3s is the precedent).
- `shunt-control join --name c2 --peer-url ... --existing https://c1:9901` calls member-add on the existing cluster, receives the initial-cluster string and certificates, and starts.
- `shunt-control member list|remove <name>`; replacing a failed node is `remove` then `join` with the same name.
- `shunt-control snapshot save <path>` and `shunt-control snapshot restore <path>` (restore is a documented, rehearsed runbook, not a button).
- `shunt-control defrag` and automatic compaction (retain 24 h of revisions) on a schedule.
- `shunt-control status`: members, leader, quorum, DB size against quota, last compaction, connected proxies and their applied revisions.

Requirements the runbook states plainly: three nodes on separate failure domains; a local SSD for the etcd data directory, since Raft fsyncs on every write; clocks within a few seconds; the client and peer ports reachable between control nodes and from every proxy.

## 12.8 Limits

- A control domain is one datacenter or one region with sub-10 ms RTT between control nodes. Stretched consensus is fragile. Multiple datacenters are multiple shunt deployments sharing nothing; a bucket lives in exactly one.
- A burst of bucket creations fans out as watch events through the control nodes; sustained thousands per second would need batching, which is a later problem.
- Every proxy holds every tenant secret in memory because it must verify signatures. Secrets are encrypted at rest in etcd with a key from env or KMS held only by control nodes, control-to-proxy is mTLS, and the README states that shunt is the highest-value target in the environment.
- The two-phase ramp makes state changes slower (two fence rounds); at 1000 proxies with a 10 s heartbeat that is under a minute per phase. Correctness over speed.

## 12.9 What the property test must prove

- Two-phase ramp: with two proxies where one lags by a revision, no key is written to one side and read source-only on the other. A model-checked test with a deliberately stale proxy. *(File-backend form done in POC-6: `internal/proxy/fleet_property_test.go`, with a negative control that must fail without the fence; P3c reruns it against etcd.)*
- Stale mode: a proxy whose lease is revoked mid-migration refuses transitional writes and serves ACTIVE placements; on reconnect it acks and resumes. *(Done in POC-6 for the file backend: the property test partitions a member mid-migration; `make fleet` does it with SIGSTOP. A reconnecting proxy renews only once it has installed the current version, ADR-0016.)*
- Fence: a ramp change never advances to phase 2 while any live proxy lags; a proxy whose lease expires is dropped from the fence and lands in stale mode.
- Quorum loss: kill two of three control nodes under load; data-path error rate on ACTIVE placements stays zero; transitional placements see only 503s with `Retry-After`; recovery restores everything with no operator action.
- Mover claims: two movers on the same migration never copy conflicting content; a mover killed mid-range is taken over within one lease TTL.

