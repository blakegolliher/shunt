# ADR-0021: coherent snapshots, drain barriers and recoverable control operations

Status: **proposed; the first four regressions landed** (T01, T02, T03, T07 on 2026-09-24, see
"Decisions taken 2026-09-24"), H0a (identity, generations, lineage checks), H0b (the operation
transaction contract and scope reservations), H0c (idempotency keys, expected generations and
operation capacity), H0d (typed refusals, `shunt operation`, the Operations screen, and the
direct-write audit) and H0e (metric catalog rows and benchmarks, docs/bench/h0.md). H0's gate
passed on 2026-09-24. H1 passed on 2026-09-24: H1a (the runtime bundle), H1b (secret generations), H1c (resource lifetimes, the retained-bundle bound), H1d (the restart cache), H1e (rotation and install diagnostics on API, CLI and UI) and H1f (benchmarks, docs/bench/h1.md). H2 passed on 2026-09-25: H2a–H2e, the review fixes, and H2f's acceptance (docs/bench/h2.md; "Decisions taken 2026-09-25"). H3–H5 are pending.

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

## Decisions taken 2026-09-24

1. **No live protocol upgrade.** No protocol-1 fleet is deployed; shunt is in development and
   alpha test. Protocol 2 replaces protocol 1 outright. Capability negotiation, the `426
   upgrade_required` answer, the four-step controlled handover (design §9), scenario T18 and the
   default-off rollout flag are dropped. The H0 capability gate reduces to a schema version that an
   older binary refuses to start against. The heartbeat's `seq` echo (T07) is the first such
   change: a proxy talking to an older control node never takes a lease.
2. **etcd carries the transaction contract.** The file directory backend stays a single-writer lab
   backend in its current format. The one durable envelope for directory, scope reservations and
   unfinished operations (design §2, "Durable intent") is not built for it.
3. **Cheap fixes first.** T01, T02, T03 and T07 landed before H0, each with a regression test that
   fails when its fix is reverted:
   - T01: a member serializes installs and rechecks the version under the lock, so neither the
     installed version nor the cache goes back.
   - T02: `Prepare` hooks get a candidate-local secret resolver and return a commit. The member
     commits only once the version is installed. The file backend commits only once the write is
     on disk. The control node never commits a write's candidate: the version goes live through
     its watch, so a failed compare-and-swap leaves the live registry as it was.
   - T03: the registry's reuse check compares the resolved secret too. A secret-only rotation
     gets a new signer that shares the old transport, and a request in flight keeps the old one.
   - T07: the lease runs from the heartbeat's monotonic send time for
     `min(server grant, control.lease_ttl)`. An answer to another heartbeat, with no grant, behind
     the proxy's version, older than the grant held, or past its own deadline renews nothing.

   Deferred to their slices: conflicting content for one identity (T01) needs H0's epoch; one
   coherent bundle across keys, directory and signers (T01, T02) is H1; rotation status on API,
   CLI and GUI (T03) is H1; epoch mismatch and CLI/UI grant fields (T07) are H0 and H2.

## Decisions taken 2026-09-24 (H2 review fixes)

A review of H2a–H2e found operations that could block forever with no operator exit, and one that
could reopen a source under its own deletes. The fixes settle four rules.

1. **The DELETE rule.** While purge-source's `source` barrier is on a placement, a DELETE (or
   DeleteObjects) that must reach both clusters answers 503 with `Retry-After`
   (`shunt_migration_refused_writes_total{reason="source_closed"}`) instead of skipping its source
   leg. Skipping it, as H2e did, sent the delete to the primary alone and left the source holding a
   key the primary lacks, which purge's re-diff must refuse; under any delete load purge could
   neither complete nor be canceled. Pausing is what "mutations pause" means for every other
   barrier, and a purge's hold is short. Rejected: making the re-diff tolerate keys deleted on the
   primary since the hold. That needs either per-object state (which keys the proxies deleted) or
   a listing comparison that cannot tell a delete from a key the mover never copied, and it would
   turn a correctness check into a guess.
2. **Purge's irreversible point is the first delete.** The re-diff under the drained source
   barrier runs in the drain (a non-empty or failed diff is the `source_diff` blocker; nothing is
   deleted and the operation can be canceled). `dispatch_started` is written to the record
   immediately before the first backend delete, never before a check that can still refuse.
3. **The record orders a cancellation against the owner.** Cancellation ends the record first,
   with the cancellable check repeated inside that compare-and-swap, and only then releases the
   hold, compared on its barrier id. The owner writes its record (phase `commit`, or
   `dispatch_started`) before its commit or first destructive dispatch and checks the answer, so
   exactly one of the two wins. A cancellation that finds the hold gone after the owner's commit
   landed corrects the record to `failed` with effect `committed`. Releasing read-only names its
   barrier and is refused once the commit cleared it. An operation still in its precondition, or a
   barrier whose lost owner never made its hold version durable, can be canceled.
4. **A resumed barrier drains only while its scope carries it.** A record whose commit reached the
   directory before the record said so is reconciled by the commit (`already`), not by a drain
   that waits for an acknowledgement no proxy can send. A dead external mover's session is resolved
   by an operator with an attestation (`resolve-worker`, on API, CLI and GUI), which the record
   keeps.

## Decisions taken 2026-09-25 (H2f acceptance)

The H2 release gate ran `make walkthrough` and `make fleet` on the H2 build and found three
behaviors that the slices' own tests did not. Each fix has a regression test that fails with the
fix reverted.

1. **A mutation dispatched whole runs to the backend's answer.** A client that disconnects no
   longer cuts off the upstream request of a write, delete or other mutation (`migrate.Mutates`):
   the proxy detaches it from the client's context and waits for the backend, bounded by its own
   idle-progress and metadata deadlines, so the outcome is definitive. Before, a client that left
   with its request at the backend made the outcome uncertain, and uncertainty is counted for the
   life of the process (H2a), so one ordinary client timeout (in the walkthrough, `shunt verify
   --duration` ending with two DELETEs in flight) blocked every later barrier on the bucket until
   the proxy was retired and its incarnation resolved. A client that leaves mid-body still stops
   the request: the body is cut short and a short body is never committed. Only the proxy's own
   deadline expiring with the backend silent remains uncertain; that cost stands and the runbooks
   state it. Reads keep following the client's context. (`TestClientDisconnectDoesNotMakeAMutationUncertain`.)
2. **Cutover's quiet window runs before its hold, with writes flowing.** H2e ran the window as the
   barrier's `extra`, behind the closed mutations gate, which answered every write and delete 503
   for the whole window (60 s by default); the walkthrough's verify gave up after its SDK-like
   retries. The gate buys the window nothing: a fallback read is a read, which the gate does not
   stop. The window now runs first, bound to the proxies' incarnations as before; a fallback read
   or a proxy that did not report is a refusal (409) with nothing held. Then cutover holds the
   bucket like any step and, under the closed gate, checks the source has no open multipart upload
   and that every proxy that watched the window reports twice more, from the same process, with the
   fallback count unchanged. A read that fell back after the window blocks the operation
   (`old_requests`) and a new window is watched under the hold; a resumed cutover, whose first
   owner's window is not on the record, watches its window under the hold too.
   (`TestCutoverWindowKeepsWritesFlowing`.)
3. **An operation orphaned before its first write changed nothing, and an ended record offers
   nothing.** A step whose control node died while it was queued, in its precondition, in
   cutover's window or in purge's diff, with no barrier intent on its record, ends `failed` with
   effect `none`, not `uncertain`. Any record ended by the owner-loss sweep or a restart carries no
   blockers and no allowed actions; `make fleet` found such a record still offering `cancel`.
   (`TestOrphanedBeforeFirstWriteChangedNothing`.)
4. **Forget keeps the incarnations it proved ended.** Forget deleted the member record, and with
   it the operator's resolution of a killed process. A new process started on the same cache
   reports the killed one as its unclean previous incarnation, and the fresh record took it for a
   crash, so every step blocked on `incarnation_unresolved` again. Forget now writes
   `/shunt/fleet/forgotten/<id>`, in the transaction that deletes the member, listing its retired
   and resolved incarnations, and a registration does not revive those. A previous incarnation
   forget never proved ended still counts: a forgotten proxy restarted during a control outage
   serves ACTIVE buckets from its cache, and its crash is real evidence.
   (`TestFleetForgottenIncarnationStaysEnded`.)

The CLI's `--wait` is now what the contract says (contracts §2, "CLI completion semantics"): how
long the command watches its operation, after which it exits 3 with the record's id, phase,
blockers and next commands. `ramp` and `migrate start` watched for one minute plus three times
`--wait`, `cutover` for its window plus two minutes plus three times `--wait`, and `readonly` and
`purge-source` until the operation ended. `cutover` now watches for its window plus `--wait`. The
request that creates the record has its own one-minute bound, so a control plane without quorum
still answers with its reason. `shunt operation wait` and the steps' own polling decoded every
answer into one value, so an ended record, which omits its blockers, was printed with the blockers
of an earlier poll; each poll now decodes into a fresh value. SIGINT's exit 130 is not implemented
(H5).

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

There is no mixed-version operation (decision 1 of 2026-09-24): protocol 2
replaces protocol 1, and a binary that cannot read protocol-2 state refuses to
start. Old cache formats cannot be trusted to acknowledge the new protocol.

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
