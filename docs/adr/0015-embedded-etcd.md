# ADR-0015: Embedded etcd in shunt-control, not own Raft, not Postgres

Status: proposed (branch `distributed`)

Numbering note: this ADR was drafted as 0011 in an out-of-tree doc set. 0011 through 0014 were taken
in the meantime (stepping out, importing client keys, conditional writes across a migration,
cross-cluster copy), so it lands as 0015. Supersedes `docs/DESIGN.md` §1.5 (Postgres) where the two
differ.

## Context

The design targets ~1000 stateless proxies in one datacenter behind one control plane, with no service to stand up beyond shunt's own binaries. Directory writes (clusters, tenants, placements, transitions, ramp rules, read-only flags, migration claims) need a single linearizable log with compare-and-swap. Nothing else does.

## Decision

`shunt-control` embeds an etcd member via `go.etcd.io/etcd/server/v3/embed`. Three (or five) control nodes form the cluster. Each also serves the control API and the directory fan-out to proxies from its local watch cache. Proxies never open an etcd connection; they long-poll a control node and heartbeat a leased key through it.

## Alternatives rejected

- **Own Raft** (`go.etcd.io/raft`, Apache-2.0; `hashicorp/raft`, MPL-2.0): the library is the easy part; snapshotting, compaction, membership change, backup/restore, and failure modes are months of hardening etcd has already done.
- **Postgres**: correct for the writes, but a service to run, and it lacks watch, lease, and revision, each of which would be rebuilt above it.
- **Gossip** (`hashicorp/memberlist`, MPL-2.0): eventual consistency is the wrong guarantee for the thing that decides where writes go.

## Consequences

- The `shunt-control` binary named in the simplicity rules is this one; §1.5's Postgres form is
  withdrawn, and the P3c prompt in `docs/DESIGN.md` §7 is superseded by `docs/prompts/P3c.md`.

- Revision is the directory version and the fence counter; lease is proxy liveness and mover work claims; transactions make any control node a writer with no leader election in shunt's code.
- `shunt-control` gains etcd's operational surface, wrapped as subcommands (`init`, `join`, `member`, `snapshot`, `defrag`, `status`); operators never see an etcd flag.
- `shunt-control` carries the etcd dependency tree (etcd server and client, Apache-2.0 with a NOTICE that must be carried into `THIRD_PARTY_NOTICES`; `go.etcd.io/raft` Apache-2.0; `go.etcd.io/bbolt` MIT; `google.golang.org/grpc` Apache-2.0; `go.uber.org/zap` MIT). `bin/shunt` links none of it; `make licenses` proves that.
- etcd is pinned to one minor version and upgraded one minor at a time.
- A control domain is one datacenter or sub-10 ms region; stretched consensus is out of scope.
- The file directory backend remains for single-node labs; both backends implement the same `Directory` seam and the same REST API.

See `docs/design/distributed.md` for the protocol (version fence, two-phase ramp, stale mode, fleet decisions, movers as workers) and the operating requirements.
