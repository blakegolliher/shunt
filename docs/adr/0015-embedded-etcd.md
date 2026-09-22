# ADR-0015: Embedded etcd in shunt-control, not own Raft, not Postgres

Status: accepted (P3c-1, 2026-09-22), with the amendments at the end.

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
- `shunt-control` carries the etcd dependency tree (etcd server and client, Apache-2.0; the modules ship no NOTICE file, so `THIRD_PARTY_NOTICES` carries their LICENSE and the NOTICEs of what they pull in; `go.etcd.io/raft` Apache-2.0; `go.etcd.io/bbolt` MIT; `google.golang.org/grpc` Apache-2.0; `go.uber.org/zap` MIT). `bin/shunt` links none of it; `make licenses` proves that.
- etcd is pinned to one minor version and upgraded one minor at a time.
- A control domain is one datacenter or sub-10 ms region; stretched consensus is out of scope.
- The file directory backend remains for single-node labs; both backends implement the same `Directory` seam and the same REST API.

See `docs/design/distributed.md` for the protocol (version fence, two-phase ramp, stale mode, fleet decisions, movers as workers) and the operating requirements.

## Amendments at acceptance (P3c-1, 2026-09-22)

What was built differs from the proposal in these ways, each a decision:

- **No etcd client port.** `shunt-control` talks to its own member in process, and a joining node asks a running node's control API (`POST /v1/control/members`) to add it. Only the peer port is open; the client listener is a unix socket in the data directory, which no other host can reach. Operators have one API address per node, not two.
- **The directory version is a key, not the etcd revision.** `/shunt/v1/version` is bumped by every directory write in the same transaction, and every write compare-and-swaps on it. The fleet's leased keys live under `/shunt/fleet/`, so heartbeats do not move the version that proxies install and the fence counts.
- **Membership is a record beside the lease** (`/shunt/fleet/members/<id>`), written by a member's first heartbeat and removed by `shunt proxy forget`; the lease (`/shunt/fleet/proxies/<id>`) is liveness only. ADR-0016 explains why a partitioned member must not vanish when its lease expires.
- **Secrets at rest** are sealed with AES-256-GCM under one data-encryption key, created by `init` in the data directory (0600) and handed to a joining node in the join answer. A cluster whose secret the control plane stores names it as `secret_ref: control:<name>`; client keys are records of their own (`/shunt/v1/credentials/<access-key>`), so `adopt --keys` and `client add` reach every proxy.
- **Full snapshots, not deltas.** `GET /v1/directory?since=` returns the whole directory whenever it is newer. Deltas, the object-storage bootstrap snapshot, and audit export to object storage are deferred; audit records stay in etcd with a retention of 10,000 versions.
- **TLS is deferred.** The control channel is plain http with a bearer token. Cluster secrets, client keys and the data-encryption key cross it in the clear between hosts, and a proxy id is only as trustworthy as the token. Every process on the channel must say so: `shunt-control --plaintext` on a non-loopback API, `control.plaintext: true` on a member. The internal CA and mTLS with proxy ids in certificates are the next step and change none of the protocol.
- **Readiness does not drain a stale proxy** (ADR-0016), and the two-phase ramp is the hold, not reads-widened-first; the prompt's wording of both is superseded.
- **The `Directory` seam has three implementations** (file, etcd, member client) and the operator mutations are `directory.Store` (file, etcd). `control.Fleet` and `control.Keys` are interfaces because the etcd client must not link into `bin/shunt`, which `make build` and `make licenses` prove; each has a second implementation that is not a test double (`NoFleet`, `auth.Static`).
