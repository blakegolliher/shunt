# Running shunt-control

`shunt-control` is shunt's control plane (ADR-0015): the directory, the client keys, the cluster
secrets and the fleet of proxies, in an etcd member embedded in each control node. Three nodes
form a cluster that survives one failure; five survive two; one is enough for a lab. Proxies take
everything from it and share nothing with each other (docs/fleet.md).

## What a node needs

- **A local disk for `--data-dir`.** Every directory write is fsynced by Raft. Not a network
  filesystem, not shared with another node.
- **Two ports.** `--peer-url` (`http://host:2380`), reachable from the other control nodes, and
  `--api` (`host:9901`), reachable from every proxy and from operators. There is no etcd client
  port: a node talks to its own member in process.
- **Clocks within a few seconds** of each other, and control nodes in one datacenter or region
  with a few milliseconds between them. Stretched consensus is out of scope.
- **A token** (`--token-ref env:NAME|file:/path`) whenever `--api` is not loopback, and
  `--plaintext`, which states what is true until TLS for the control channel lands: cluster
  secrets, client keys and the data-encryption key cross the network in the clear. Keep the API
  on a management network.

## Forming a cluster

The first node forms the cluster and creates the data-encryption key (`<data-dir>/encryption.key`,
0600) that seals every secret at rest:

```sh
shunt-control init --name c1 --data-dir /var/lib/shunt-control --peer-url http://c1:2380 \
  --api c1:9901 --token-ref file:/etc/shunt/control.token --plaintext
```

Each further node joins through a running node's API, receives the cluster's member list and the
key, and starts:

```sh
shunt-control join --name c2 --data-dir /var/lib/shunt-control --peer-url http://c2:2380 \
  --api c2:9901 --token-ref file:/etc/shunt/control.token --plaintext --existing http://c1:9901
```

Both commands are also how a node is restarted: on a data directory that already holds a member
they start it again and change nothing, so the same line is the node's service definition
(`ExecStart=` in systemd). Joining takes a few seconds; `shunt-control status` shows the members,
the leader, and whether there is quorum:

```
node c1 (shunt-control v0.x): 3 of 3 members started, 2 needed for writes, quorum
directory version 12; etcd revision 40; database 84.0 KiB of 120.0 KiB in use, quota 2.0 GiB

MEMBER  PEER            ROLE      STATE
c1      http://c1:2380  leader    started
c2      http://c2:2380  follower  started
c3      http://c3:2380  follower  started

PROXY    STATE  APPLIED  LAST HEARTBEAT
proxy-a  live   12       0s ago
proxy-b  live   12       1s ago
```

The `shunt` operator verbs point `--api` (or `SHUNT_API`) at any node; `shunt-control`'s own verbs
take `--api` (or `SHUNT_CONTROL_API`) the same way, and both read the token from `--token-ref` or
`SHUNT_API_TOKEN_REF`.

## Replacing a failed node

Remove it while a majority is up, then join a new node with the same name on a fresh data
directory:

```sh
shunt-control member remove c3
shunt-control join --name c3 --data-dir /var/lib/shunt-control --peer-url http://c3:2380 ... --existing http://c1:9901
```

A node that is only restarting keeps its data directory and needs neither.

## Snapshot and restore

`snapshot save` writes a point-in-time copy of the store; its secrets stay sealed, so keep a copy
of `encryption.key` with it, out of band:

```sh
shunt-control snapshot save /backup/shunt-control-$(date +%F).snap
```

Restore is a rehearsed runbook, not a button: it rebuilds a cluster of **one** from the file, on a
host with no `shunt-control` running, and the other nodes then join it on fresh data directories.

```sh
shunt-control snapshot restore /backup/shunt-control-2026-09-22.snap --data-dir /var/lib/shunt-control --name c1 --peer-url http://c1:2380
cp /backup/encryption.key /var/lib/shunt-control/encryption.key && chmod 600 /var/lib/shunt-control/encryption.key
shunt-control init --name c1 --data-dir /var/lib/shunt-control --peer-url http://c1:2380 ...
```

Rehearse it on a lab before it is needed. Proxies keep serving throughout from their last installed
directory; they re-join once a node answers.

## Housekeeping

Revisions older than 24 hours are compacted automatically. `shunt-control defrag` compacts a
node's database file in place when `status` shows it far larger than what is in use; it blocks
that node briefly, so one node at a time. The audit trail keeps the last 10,000 directory
versions in the store (`/shunt/v1/changes/`); export to object storage is deferred.

Upgrade the control nodes before the proxies. A proxy's heartbeat is decoded with unknown fields
refused, and a newer proxy reports fields (`host`, `version`, ADR-0017) an older control node
does not know, so it would be refused until the node is upgraded.

## Metrics

Every node serves `/-/metrics` on its API listener: `shunt_fleet_members{state="live"|"silent"}`,
`shunt_fleet_fence_wait_seconds`, and the request metrics of the API itself. Alert on
`shunt_fleet_members{state="silent"} > 0` and on quorum loss (`status` says `NO QUORUM`); the
runbooks are docs/runbooks/quorum-loss.md and docs/runbooks/lagging-proxy.md.
