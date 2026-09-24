# ADR-0021: coherent snapshots, drain barriers and recoverable control operations

Status: **proposed, implementation not started** (2026-09-24).

Design baseline: `distributed` at `74bddd4f3d3a6621d118dbc2a99b6f35a52c6e03`.
This proposal amends [ADR-0015](0015-embedded-etcd.md),
[ADR-0016](0016-version-fence.md), [ADR-0017](0017-web-ui.md), and
[DESIGN §2.5 and §12](../DESIGN.md) when implemented and accepted. It does not
declare the current implementation safe or change its behavior.

The implementation specification is [distributed correctness](../design/distributed-correctness.md).
Its [interface contracts](../design/distributed-correctness-contracts.md) and
[delivery plan](../prompts/distributed-hardening.md) are part of this decision.

## Problem

The current fleet acknowledges an installed directory version, but a request can
still be using the previous snapshot. Moving writes while that request completes
can leave a newer acknowledged value on the old source. Concurrent snapshot
installation, independently mutable credentials, and a local lease duration that
ignores the server's grant make the version acknowledgement weaker still.

An etcd restore can decrease shunt's application version. Existing proxies can
mistake their newer, different snapshot for current state. Joining a control node
adds a voter before its local bootstrap finishes, so an interrupted join can
damage availability. The UI also confuses pending with completed maintenance and
historical membership with current health.

These are protocol and lifecycle defects, not just missing retries. The fixes
must preserve streaming, the absence of per-object state, and an ACTIVE data path
that does not query the control plane on each request.

## Decisions

1. **One immutable runtime bundle per request.** Directory, client keys, upstream
   signers and secret generations are prepared without changing live state, then
   published together. Installation is monotonic within a recovery epoch.
   Failed preparation publishes nothing. Credential identity includes a secret
   generation; transport pools need not be rebuilt for secret-only rotation.
2. **Installed is not drained.** A routing barrier closes mutation admission on
   the affected bucket and waits for every relevant proxy incarnation to stop
   admitting old work and finish old backend effects. The initial implementation
   pauses all mutations on that bucket, not only the newly ramped hash range.
   Other buckets retain their normal request path. Single-proxy operation uses
   the same local drain rule. Destructive cleanup also drains source-dependent
   reads and workers.
3. **Silence is not proof.** Lease expiry, a closed connection, cancellation, and
   a crashed proxy do not prove that an accepted backend mutation has finished.
   Such uncertainty blocks the barrier. Persistent membership and unresolved
   incarnations survive liveness expiry and administrative forget. Generic S3
   provides no fencing token that can make a late write harmless. Recovery from
   an unprovable backend outcome requires externally established quiescence and
   reconciliation; a force flag must not turn uncertainty into success.
4. **Server-granted leases and explicit lineage.** Snapshot identity is
   `(cluster_id, epoch, version)`. Proxies honor the server grant conservatively
   from their monotonic request-send time. Versions from different epochs are
   incomparable. Restore always creates a new epoch in recovery mode. A new
   epoch does not fence a disconnected old ACTIVE proxy: cold recovery must
   isolate the old deployment and account for accepted backend work before
   serving recovered state.
5. **Durable operations own transitions.** Operation ID, epoch, scope generation,
   and expected state are checked in durable compare-and-swap transitions.
   Timeout means pending; it neither implies success nor silently releases a
   hold. Repeated requests identify the same intent. Owner leases coordinate
   workers but do not fence arbitrary S3 side effects. Unfinished operations
   cannot be pruned by a history limit.
6. **Learner-first, resumable membership.** A durable join intent precedes the
   etcd learner RPC. Retries reconcile actual member IDs and peer URLs after
   ambiguous outcomes. Promotion follows etcd's catch-up check. Operators can
   remove an unnamed member by ID. Membership RPCs and application KV writes
   are not treated as one atomic transaction.
7. **Truthful interfaces are acceptance criteria.** API, CLI and GUI share
   operation outcomes and blockers. Health distinguishes observed reachability,
   quorum evidence and observation age. UI events refresh server state without
   overwriting an operator's dirty form. Every new operator action has all three
   surfaces and behavioral tests. Offline filesystem bootstrap remains a local
   CLI action, with an API-backed recovery workflow and GUI handoff.

## Scope and cost

The five fix packages are D1 runtime installation, D2 draining and leases,
D3 restore recovery, D4 membership and health, and D5 operator consistency.
They add only bucket-, proxy-incarnation-, cluster-, and operation-level state.
No object-location table, per-object journal, request body buffer, additional
binary, or external service is introduced. Use existing concrete packages and
the existing Directory/Fleet/Operations seams; do not add an abstraction framework.

Admission accounting is local and bounded by configured placements and active
requests. Durable writes occur for configuration, operations and proxy lifecycle,
never for each S3 request. Heartbeats prioritize safety acknowledgements over
optional telemetry. Snapshot construction and cache persistence stay off the
request path. Every changed package needs tests and a benchmark; the delivery
plan defines performance and fault-injection gates.

This tightens ADR-0016's availability contract: dead members cannot simply fall
out of later fences, and the no-members fast path still drains local requests.
Healthy ramp steps briefly pause more writes than the old range-only hold.
That cost buys a small, testable admission protocol without object locks.
Range-selective admission can be a later measured optimization with the same
proof obligations; it is not a prerequisite for correctness.

## Alternatives rejected

| Alternative | Why it fails |
|---|---|
| A mutex around snapshot installation only | Does not make auth/routing/signing coherent or drain older requests. |
| Wait one lease, then advance | Accepted backend writes can outlive both lease and proxy. |
| Increase etcd revision after restore only | Shunt's application version and cached routing lineage are separate; backend data was not rolled back. |
| Add a voting member, compensate on failure | A lost response may mean success; a failed two-voter expansion may already lack quorum for removal. |
| Rebuild every transport on every version | Discards connection reuse and increases reload cost without fixing protocol ordering. |
| Report success when the desired flag is stored | Confuses requested state with fleet-wide enforcement and backend quiescence. |

## Compatibility and acceptance

Protocol capability checks and the controlled upgrade procedure in the design
are mandatory. Legacy proxies cannot count as drain-capable. Old cache formats
cannot be trusted to acknowledge the new protocol. Mixed-version operation may
retain existing ACTIVE traffic during staging, but fenced mutations are blocked
until the deployment has completed the controlled handover.

Accept this ADR only as its implementation gates pass. Keep the proposal label
and the status checklist honest while individual packages land. Passing unit
tests alone is insufficient: the real etcd membership, restore, fleet partition,
and API/CLI/GUI workflows must also run in a capable environment.

## Primary references

- [etcd 3.7 runtime reconfiguration](https://etcd.io/docs/v3.7/op-guide/runtime-configuration/):
  learner admission, catch-up and promotion; membership changes require quorum.
- [etcd 3.7 disaster recovery](https://etcd.io/docs/v3.7/op-guide/recovery/):
  restoring a snapshot creates a new etcd cluster, and revision rollback affects
  consumers. The application epoch and backend reconciliation here are additional
  shunt requirements.
- [Go net/http](https://pkg.go.dev/net/http): cancellation controls the request
  lifecycle; the API does not provide a guarantee that a remote mutation was undone.
