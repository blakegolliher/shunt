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
shunt proxy list --api http://c1:9901     # members, the version each has installed: live, SILENT, retiring, retired, UNRESOLVED(n)
shunt proxy show proxy-b --api http://c1:9901   # one member: lease, incarnation, backend outcomes unknown, what is off
```

Each process of a member is an **incarnation**: a random id written to `cache_dir` before it
serves. Stopping a member with SIGTERM, or `shunt proxy retire <id>`, **retires** it: it stops
accepting requests, waits up to `proxy.drain_timeout` for those in flight, records a clean
retirement with the control plane and exits. A retired member counts out of every step. A process
that ends any other way (a crash, `kill -9`, a lost host), or that retires with a backend outcome
it never learned, is **unresolved**, and every step waits on it until an operator resolves it
([docs/runbooks/crashed-proxy.md](runbooks/crashed-proxy.md)).

## Where the state lives

| State | Lives in | Reaches a proxy by |
|---|---|---|
| Directory (clusters, tenants, placements) | etcd, one key per record | `GET /v1/directory`, long-polled |
| Client keys | etcd, secrets sealed | the same answer, in the clear over the control channel |
| Cluster secrets | etcd, sealed; ref `control:<cluster>` | the same answer |
| Fleet membership, incarnations and liveness | etcd: a record per member, a leased key per heartbeat | — |
| Operation records, with their holds and blockers | etcd, one key per operation | — |
| A proxy's last directory and incarnation marker | its own `cache_dir` (0700) | — |

`shunt cluster add` with a typed secret, `adopt --keys` and `client add` therefore reach every
proxy from one command. The mover (`shunt migrate run`) gets the cluster secrets from the control
plane too, so it runs on any host that can reach the API.

## What changes while a bucket moves

The rules are ADR-0021's (H2). With one proxy they apply to that proxy alone: a step still pauses
the bucket's writes until the requests admitted under the old routing have ended. With members:

- **Every step waits for every member.** `ramp`, `migrate start`, `cutover`, `purge-source` and
  `readonly` each run as an operation record on the control plane (`POST /v1/operations`,
  ADR-0017): the CLI polls it every 250 ms and prints the outcome, so a step outlives a dropped
  connection. A step first waits until every registered member has the current directory version.
  A silent member is not skipped: its process may still route by the old directory, so it is a
  named blocker, `proxy_missing`, for as long as it is silent. Only a member that retired cleanly
  counts out. Check `shunt proxy list` before a step. A second step on a bucket while one is
  unfinished answers `operation_conflict` naming it, from any control node.
- **A step pauses the bucket's writes for a moment.** Two proxies must never send the same key to
  different clusters, and a write admitted under the old routing must end before the new routing
  takes effect. So a step that moves writes is written twice. First a hold: every proxy installs
  it and closes the bucket's mutations, so writes and deletes answer `503` with `Retry-After: 1`
  (every SDK and aws-cli retries). Then, once every proxy reports its gate closed, nothing it
  admitted before still out, no backend outcome unknown, and the hold in its restart cache, the
  step itself. On a healthy fleet that is about two heartbeats plus the longest write in flight;
  reads never pause. The CLI says `the keys this step moves paused their writes until every proxy
  had it`.
- **After the commit** the CLI says `in effect on every live proxy` once every live member has
  installed the step within `--wait` (default 30s). One that has not is named (`PENDING: not yet
  installed on proxy-c`), and one that fell silent after the drain is listed (`silent, not waited
  for: proxy-c`). The step is in force either way: such a member refuses writes to the moving
  bucket on its own until it has it. The next step on the bucket waits for it in its precondition.
- **Cutover watches its quiet window before it holds the bucket.** Writes flow during `--window`.
  A read that falls back in it, or a live member that does not report twice afterwards from the
  same process, refuses the cutover with nothing held. Then cutover holds the bucket like any step,
  and under the closed gate checks that the source has no open multipart upload and that every
  proxy that watched the window reports twice more, from the same process, with the fallback count
  unchanged. If not, it blocks (`multipart_open`, `old_requests`, `proxy_missing`) with the
  bucket's writes paused: cancel it, deal with the cause, and cut over again
  ([docs/runbooks/blocked-operation.md](runbooks/blocked-operation.md)). A cutover resumed after
  its control node was lost watches a new window under the hold, with writes paused.
- **Purge-source pauses the bucket's deletes.** Its hold closes the source: while every proxy drains
  its reads of the source and the listing diff is taken again, a DELETE answers 503 with
  `Retry-After` rather than reach the primary alone.

## When a step is blocked

A wait is how long the CLI watches, not a safety timeout. When the CLI stops watching an
unfinished step it exits 3 and says where the operation stands:

```
shunt: data: operation 1790323451234-3f2a1c is still blocked (phase drain); waiting on proxy_missing proxy-c; it keeps running: `shunt operation wait 1790323451234-3f2a1c` follows it, `shunt operation cancel 1790323451234-3f2a1c` releases its hold and changes nothing
```

The cancel hint appears only while the operation offers `cancel`. `ramp`, `migrate start`,
`readonly`, `cluster readonly` and `purge-source` watch for `--wait` (default 30s); `cutover` for
its window plus `--wait`. Interrupting the CLI does not stop the operation either. Nothing is
released, rolled back or called done when the CLI stops: the hold stays, the record stays `blocked`
on every control node, and the step carries on by itself once its blockers clear. Then, in this
order:

1. **Inspect.** `shunt operation show <id>` lists each blocker by code and proxy, and the actions
   the record allows (`actions: resume, cancel`). `shunt proxy list` and `shunt proxy show <proxy>`
   say what the named member is doing.
2. **Restore the member if you can.** A silent member that comes back acknowledges the hold, drains,
   and the step goes on ([docs/runbooks/lagging-proxy.md](runbooks/lagging-proxy.md)).
3. **Retire a member you can reach** and want out: `shunt proxy retire <id> --wait 2m`.
4. **Resolve a crashed or unreachable member's incarnation** only after its backend effects are
   reconciled: `shunt proxy resolve <id> --incarnation <inc> --attest "<how you know>"`
   ([docs/runbooks/crashed-proxy.md](runbooks/crashed-proxy.md)).
5. **Forget only with retirement proof.** `shunt proxy forget <id>` is refused while the member is
   live, and with `retirement_unproven` while any incarnation of it is unresolved.
6. **Resume an owner-lost operation** on a live control node: `shunt operation resume <id>`.
7. **Cancel only while the record offers `cancel`:** `shunt operation cancel <id>` releases that
   operation's own hold and ends it `cancelled`, with the routing as it was before. Once the change
   is being committed, or purge-source has started deleting, cancel answers `not_cancellable`.

Each blocker code and what clears it: [docs/runbooks/blocked-operation.md](runbooks/blocked-operation.md).

**An uncertain backend outcome is expensive.** A proxy that never learns how a mutation ended (the
backend had the whole request, then the proxy's own idle or metadata deadline expired, or the
connection broke, before an answer came) counts it for the life of its process. Every later step
on that bucket then blocks on `backend_outcome_unknown` until the proxy is retired and its
incarnation resolved with an attestation. A client that disconnects does not cause this: once the
backend has the whole request, the proxy waits for its answer, bounded by its own deadlines
(`proxy.idle_timeout`, `proxy.metadata_timeout`).

## When a proxy loses the control plane

A member whose lease has run out is **stale**: a lease runs from a heartbeat's send time for the
shorter of the control plane's grant and the member's own `lease_ttl`. It keeps serving
reads, and writes to buckets that are not moving; it refuses writes and deletes on moving buckets
with `503` + `Retry-After`, and reads those buckets from the new cluster first (it may have missed
a step). It becomes fresh again only once it has installed the version the control plane has. Its
`/-/healthz` stays 200 on purpose: when the control plane is down every member is stale at once,
and draining them all would take the data path down with it. Alert on `shunt_fleet_stale` (members)
and on `shunt_fleet_members{state="silent"}` (control nodes) instead. `/-/fleet` on a member shows
its state.

While the control plane has no quorum, clients keep working on every bucket that is not moving,
from each proxy's last installed directory, and moving buckets refuse writes until it is back. A
proxy that restarts meanwhile serves ACTIVE buckets from its cache, stale, and re-joins when a node
answers. That is the promise for ACTIVE buckets, and nothing about blocked steps weakens it
([docs/runbooks/quorum-loss.md](runbooks/quorum-loss.md)).

Migration progress is a different matter. No step can be taken without quorum, and no step goes
ahead while a member, or the control node running it, is missing: it blocks, as above.

If a control node stops in the middle of a step, the bucket's writes keep answering 503 (`shunt
status` shows `held→<ratio>` for a ramp step). The node resumes its own operations when it
restarts. If it does not come back, another node marks the record `blocked` on `owner_lost` once the
node's liveness lapses, and `shunt operation resume <id> --api <live node>` carries it on from the
phase it reached. Running the same step again answers `operation_conflict` naming that operation.

A lab proxy with no members keeps its operation records in memory: when it restarts, it releases
the holds its previous process left and fails their records, and the step is repeated.

## Limits of this form

- **The control channel is plain http** with a bearer token (TLS deferred, ADR-0015). Directory
  versions, cluster secrets and client keys cross it in the clear, and a proxy id is only as
  trustworthy as the token. Keep it on a management network; every process on it says
  `PLAINTEXT` in its log for that reason.
- **One datacenter per control plane.** Control nodes need a few milliseconds between them.
- **Full snapshots.** Every change sends every member the whole directory; at hundreds of
  buckets that is kilobytes, at a million placements it is not. Deltas are deferred.
- **Every control node reads every proxy heartbeat record once per second.** UI telemetry adds the
  last completed 10-second window to that record, never a history. The dense payload test is
  1,025,921 bytes at 6 operation classes × 4 series × 12 active clusters; at the 1,000-proxy design
  target that artificial maximum is about 978 MiB/s of JSON read by **each** control node. Real
  deployments must size this path from their active clusters and measured sparse payloads; the
  supported request ceiling is 1 MiB, and the 60-minute history consists only of summaries in
  each control node's memory.
