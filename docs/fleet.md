# Running several proxies

One shunt proxy is enough for the README demo and the walkthrough. When clients need more than one
(throughput, or an L4 balancer in front of a pool), any number of proxies can serve one directory:
a control plane holds it, and every proxy takes it from there. Nothing on a proxy is shared with
another host. The design is ADR-0015 and ADR-0016; the control plane itself is
[docs/how-to/run-shunt-control.md](how-to/run-shunt-control.md); the API is
[docs/reference/control-api.md](reference/control-api.md#the-fleet).

## The shape

```
 clients ──▶ L4 ──▶ proxy A ─┐  control.endpoints: the control nodes
                    proxy B ─┼──▶ shunt-control c1, c2, c3   (embedded etcd; one is enough for a lab)
                    proxy C ─┘   long-poll GET /v1/directory; heartbeat every 1 s
 operators ── shunt <verb> --api http://c1:9901 ──▶ any control node
```

- **Control nodes** hold the directory, the client keys and the cluster secrets (sealed at rest),
  and the fleet table. Every operator command goes to any one of them.
- **Members** (`control.endpoints` set) fetch the directory whenever it changes, keep the last one
  on local disk, forward `CreateBucket`/`DeleteBucket` to the control plane, send a heartbeat, and
  serve no operator API of their own.

## A member's config

```yaml
listener: { address: "0.0.0.0:443", tls: { cert: /etc/shunt/tls.pem, key: /etc/shunt/tls.key } }
admin:    { address: "10.0.0.12:9900" }
auth:     { mode: resign }               # no credentials_file: client keys come from the control plane
control:
  endpoints: ["http://c1:9901", "http://c2:9901", "http://c3:9901"]
  token_ref: file:/etc/shunt/control.token
  plaintext: true                        # the control channel is http until TLS for it lands (ADR-0015)
  proxy_id: proxy-b                      # default <hostname>-<admin port>
  cache_dir: /var/lib/shunt/cache        # the last installed directory, for a restart with the control plane down
```

No `directory` block: a member has no directory file. `shunt check-config` validates it. On start
a member loads its cache, registers with one heartbeat before it serves anything, and logs
`fleet member` with its id.

```sh
shunt proxy list --api http://c1:9901     # members, the version each has installed, live or SILENT
```

## Where the state lives

| State | Lives in | Reaches a proxy by |
|---|---|---|
| Directory (clusters, tenants, placements) | etcd, one key per record | `GET /v1/directory`, long-polled |
| Client keys | etcd, secrets sealed | the same answer, in the clear over the control channel |
| Cluster secrets | etcd, sealed; ref `control:<cluster>` | the same answer |
| Fleet membership and liveness | etcd: a record per member, a leased key per heartbeat | — |
| A proxy's last directory | its own `cache_dir` (0700) | — |

`shunt cluster add` with a typed secret, `adopt --keys` and `client add` therefore reach every
proxy from one command. The mover (`shunt migrate run`) gets the cluster secrets from the control
plane too, so it runs on any host that can reach the API.

## What changes while a bucket moves

With one proxy, nothing: every command answers as in the README. With members:

- **Each step waits for every proxy.** `ramp`, `migrate start` and `cutover` answer once every live
  member has installed the change, and say `in effect on every live proxy`. One that has not
  within `--wait` (default 30s) is named (`PENDING: not yet installed on proxy-c`), and the next
  step on that bucket is refused until it has. Each step runs as an operation record on the
  control plane (`POST /v1/operations`, ADR-0017): the CLI polls it every 250 ms and prints the
  outcome, so a step outlives a dropped connection, and a `--wait` the CLI gives up on prints the
  record's id to follow with `GET /v1/operations/<id>` on any control node.
- **The keys a step moves pause their writes for a moment.** Two proxies must never send the same
  key to different clusters, so a step that moves writes is written twice: first as a hold, where
  writes to the keys it moves answer `503` with `Retry-After: 1` (every SDK and aws-cli retries),
  then, once every member has the hold, as the step itself. About two heartbeats per step, only
  for the keys that step moves; reads never pause. If the hold cannot reach every member, it is
  undone and the command is refused: nothing changed.
- **A bucket's first step waits for every member, even a silent one.** A proxy cut off before the
  first step still thinks the bucket is not moving and would write every key to the old cluster.
  Check `shunt proxy list` first. A proxy that is gone for good: `shunt proxy forget <id>` (refused
  while it is live; one that comes back re-joins by itself).
- **Cutover counts every proxy's fallback reads,** and refuses if a member stops reporting during
  the window.

## When a proxy loses the control plane

A member whose heartbeat has not been answered for `lease_ttl` is **stale**. It keeps serving
reads, and writes to buckets that are not moving; it refuses writes and deletes on moving buckets
with `503` + `Retry-After`, and reads those buckets from the new cluster first (it may have missed
a step). It becomes fresh again only once it has installed the version the control plane has. Its
`/-/healthz` stays 200 on purpose: when the control plane is down every member is stale at once,
and draining them all would take the data path down with it. Alert on `shunt_fleet_stale` (members)
and on `shunt_fleet_members{state="silent"}` (control nodes) instead. `/-/fleet` on a member shows
its state.

While the control plane has no quorum, no step can be taken, clients keep working on every bucket
that is not moving, and moving buckets refuse writes until it is back. A proxy that restarts
meanwhile serves ACTIVE buckets from its cache, stale, and re-joins when a node answers.

If a control node stops in the middle of a step, the keys that step moves keep answering 503:
`shunt status` shows `held→<ratio>`, and running the same step again finishes it.

## Limits of this form

- **The control channel is plain http** with a bearer token (TLS deferred, ADR-0015). Directory
  versions, cluster secrets and client keys cross it in the clear, and a proxy id is only as
  trustworthy as the token. Keep it on a management network; every process on it says
  `PLAINTEXT` in its log for that reason.
- **One datacenter per control plane.** Control nodes need a few milliseconds between them.
- **Full snapshots.** Every change sends every member the whole directory; at hundreds of
  buckets that is kilobytes, at a million placements it is not. Deltas are deferred.
