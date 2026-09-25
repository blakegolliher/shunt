# Distributed correctness: implementation specification

**Proposed, not implemented.** 2026-09-24; based on `distributed` commit
`74bddd4f3d3a6621d118dbc2a99b6f35a52c6e03`. Read
[ADR-0021](../adr/0021-distributed-correctness.md), the
[API/CLI/GUI contracts](distributed-correctness-contracts.md), and the
[delivery and test plan](../prompts/distributed-hardening.md) together.

## 1. Coverage and invariants

The review found the following defects. The code references are investigation
entry points at the baseline, not statements that those files have been fixed.

| Finding | Baseline entry points | Fix package | Required regression |
|---|---|---|---|
| Installed hold acknowledged while an older write is in flight | `internal/member/member.go`, `internal/control/ops.go`, `internal/proxy/resign.go` | D2 | T05, T06 |
| Concurrent fetches can install version 2 after acknowledging version 3 | `internal/member/member.go` | D1 | T01 |
| Lease grant ignored in favor of local TTL | `internal/member/member.go`, `internal/control/fleet.go` | D2 | T07 |
| Restore keeps an older logical version; proxies remain on different routing | `internal/cp/etcd.go`, `internal/control/fleet.go` | D3 | T10, T11 |
| Secret-only rotation reuses a signer with the previous secret | `internal/upstream/registry.go`, `internal/cp/store.go` | D1 | T03 |
| Interrupted voter join is not resumable; unnamed member cannot be removed | `internal/cp/etcd.go`, `cmd/shunt-control/serve.go` | D4 | T12 |
| Pending read-only change displayed as success | `internal/control/readonly.go`, `web/src/screens/Clusters.tsx`, `web/src/screens/Buckets.tsx` | D2, D5 | T08, T15 |
| Named etcd member displayed as currently healthy | `internal/cp/etcd.go`, `web/src/screens/ControlPlane.tsx` | D4, D5 | T13 |
| Telemetry refresh overwrites migration form edits | `web/src/screens/Migrations.tsx`, `web/src/store.tsx` | D5 | T16 |
| Failed runtime preparation changes the live secret resolver | `internal/member/member.go` | D1 | T02 |

Preserve these invariants across both the file/lab and distributed paths:

- A request authenticates, chooses placement and signs upstream using one
  coherent runtime bundle. No published identity regresses within its epoch.
- A routing commit cannot race an old-route mutation that can still affect the
  backend. An installed snapshot, lease expiry or zero local handlers is not by
  itself a drain proof.
- No decision becomes successful just because a caller timed out. All frontends
  report the durable outcome and the same blockers.
- An ACTIVE request performs no control RPC, etcd write or disk sync. Bodies
  remain streamed; no persistent or aggregate per-object state is added.
- Recovery does not treat a restored metadata snapshot as a restored data set.

Non-goals: eliminating the separately documented conditional-copy/backend
limitations in ADR-0004, building backend-enforced fencing for generic S3, adding
an object journal, or implementing the deferred control-channel TLS project.
Those limits must stay visible; this work cannot claim linearizability for every
S3 workload. Existing backend capability checks remain release requirements.

## 2. Shared identity and operation model

### Identity

**Landed (2026-09-24, H0a):** `directory.Identity` (`cluster_id`, `epoch`), drawn by the first
write under schema 2 and stored in the same transaction as the version (`/shunt/v1/identity`, and
in the lab file); a node starting on schema-1 data upgrades it with one ordinary write. Resource
generations are the version of the write that last changed each placement, cluster or tenant
(`directory.Stamp`); a cluster's secret-only change counts. Unchanged resources stay unstamped
(generation 0) so the upgrade is one bounded transaction. The directory poll, the heartbeat, the
member cache and the fence carry and compare the identity (`checkLineage`, `Member.Has`); the
wire keeps the numeric `version` for now, and the string-typed `identity.version` of the contracts
arrives with the typed read models (H0d).

Persist a random `cluster_id` at initial creation and a random `epoch` for each
recovery lineage. Snapshot `version` increases within one epoch. Placement and
cluster `generation` increase on changes to that resource. Secret generations
increase even when the credential reference string is unchanged. Use 128-bit
random opaque IDs; serialize counters and etcd member IDs as strings in the new
wire fields to avoid JavaScript number precision loss.

A resource deleted and recreated under the same logical name must not reuse its
old generation. Allocate resource generations from the epoch's monotonic
directory counter, including recreation; this prevents an old confirmation or
operation from matching a replacement. Secret generations also cannot revert
when a credential reference is reused.

Persist the identity in the same etcd transaction as the directory it describes.
Do not infer it from process start time or a host name. The cache, heartbeats,
operation records, barrier acknowledgements and confirmation tokens carry it.
The file backend creates and persists its own identity; a file import that
replaces an existing lineage requires the recovery path, not a version reset.

### Durable intent

**Landed (2026-09-24, H0b):** `Operations.Create` writes a record and its scope reservation in one
atomic step, comparing the directory identity and the scope's generation; `Update` is a
compare-and-swap on the record's sequence, and a terminal write releases the scope in the same
step. Placement operations name the clusters they touch; a cluster operation conflicts with them
and they with it (on etcd, a reservation revision key closes the race between a cluster
operation's read and its transaction). The etcd store and the lab's in-memory store pass one
contract (`internal/control/opstest`). Unfinished and uncertain records survive the history
limit. Every placement and cluster mutation route runs under a record, replacing the per-node step
mutex. Owners keep a liveness key; a lost owner's record is ended `failed` with effect `uncertain`
and its scope released, which is weaker than the reconciliation below: until H2, repeating the
step is what completes a hold it left. Deferred: owner terms, idempotency keys and capacity (H0c),
multi-scope child operations, and the file-backend durable envelope (dropped, ADR-0021).

**Landed (2026-09-24, H0c):** `Idempotency-Key` is required on every request that creates an
operation record. The key is indexed by epoch, actor and key (`/shunt/ops-idem/`) in the same
transaction as the record; the record stores a keyed digest (HMAC under the shared confirmation
key) of kind, scope and canonical body, which a retry must match, and the API never answers it.
A retry gets the same record or the route's first answer; another intent gets 409
`idempotency_conflict`. Ended keyed records stay seven days past the history limit. An optional
`If-Generation` sets the scope's expected generation; making it required arrives with the draft
work of H5. `--operation-capacity` (default 256) answers 429 `operation_capacity` before any
effect. The CLI numbers its keys from a per-command `--request-id`.

**Landed (2026-09-24, H0d):** refusals carry `retryable`; `shunt operation list|show|wait`
(wait exits 3 at its timeout with the operation unfinished) and the web UI's Operations screen
read the same records; `resume` and `cancel` arrive with the barriers (H2). The direct file writes
`shunt directory set-state` and `set-default` now need `--offline`, since they go around the
records, reservations and fence.

Extend the existing Operations seam rather than add a second job framework.
Each safety-sensitive operation contains:

```
id, kind, actor, request_id, canonical_intent_digest
cluster_id, epoch, scope, expected_generation, resulting_generation
status, phase, sequence, owner, owner_term, owner_lease
barrier_id, target_identity, blockers[], created_at, updated_at
result, error, allowed_actions[], effect_state
```

`scope` is a placement, cluster, or control-membership resource; never an object.
`sequence` is monotonic per operation. `effect_state` is `none`, `committed`, or
`uncertain`. Accepted operations use `pending`, `running`, `blocked`, `succeeded`,
`failed`, or `cancelled`; a refused request that never created an operation is an
HTTP error. Failed operations can still have committed effects, which must be
reported. Never use failure to imply rollback.

Create the operation and scope reservation atomically, comparing the expected
generation and epoch. Compare operation ID, phase, owner term and scope
generation on every state advance. The etcd implementation uses transactions;
the file implementation uses its single writer and an atomically replaced
durable operation/transition record. Extend existing seams with explicit
transactional methods where needed; independent `Put` calls are insufficient.

**Amended 2026-09-24 (ADR-0021):** the file backend stays a single-writer lab
backend in its current format; etcd carries the transaction contract, and the
envelope below is not built. For the file backend, keep directory state, scope reservations and minimal
unfinished-operation state in one authoritative durable envelope, replaced with
fsync/rename under the existing writer lock. History/read models may be derived
separately. Two independent YAML/JSON renames are not one transaction. The
in-memory implementation remains a test fixture, not the durable lab coordinator.

At most one unfinished conflicting operation owns a scope. Cluster maintenance
reserves the cluster and conflicts with placement actions touching it. Acquire
multi-scope reservations in canonical order and validate them together. Persist
the finite affected placement set (bucket metadata, not object state); prevent
new placements from targeting a reserved cluster. Bound transaction batches;
large cluster operations use a parent operation with idempotent placement child
operations and retain the cluster reservation until all children settle.

HTTP retries use `Idempotency-Key`. Bind the key to epoch, authenticated actor,
kind and canonical payload. Reuse with the same intent returns the same record;
different payload returns `409 idempotency_conflict`. Keep an unfinished key for
the full operation lifetime and completed keys at least seven days. Declare the
retention window in responses; clients must not automatically retry an old key
after that window. Never evict an active operation, barrier or unresolved proxy
incarnation because the existing 1,000-record history cap is reached. At a
configured active-operation limit, refuse new work before side effects with
`429 operation_capacity`, while keeping recovery and status available.

A terminal record with uncertain effects or a retained scope reservation is
still safety evidence and cannot be pruned until reconciliation discharges it.

Canonical intent matching for credential requests must not retain plaintext in
operation arguments. Keep any required secret input in the existing sealed store;
use a domain-separated keyed digest internally for retry matching and redact it
from public operation JSON. Public arguments carry only credential generation
and non-secret fields. Resume must not require a browser to resubmit a secret
that was already committed.

On owner loss, reconcile durable state before claiming work. A new owner does
not replay an ambiguous destructive backend call. There is no atomic transaction
spanning etcd and S3: persist intent before external effects, validate ownership
before dispatch, and preserve uncertainty after ambiguous completion. A stale
worker may already have dispatched an RPC. Keep the scope unavailable for reuse
until that old activity is known to have ended. An owner lease is coordination,
not a backend fencing token.

## 3. D1 — atomic runtime installation and credential rotation

**Landed (2026-09-24, on `1-to-n-bucket-support`):** T01, T02 and T03 in their pre-H0 form
(ADR-0021, "Decisions taken 2026-09-24"). Member installs are serialized and monotonic
(`internal/member`, `install`). `Prepare` hooks take a candidate-local resolver and return a
commit that runs only for an installed version (`internal/upstream`, `Registry.Prepare` and
`Candidate.Commit`; `internal/cp`, `Store.prepare`; `internal/directory`, `FileDir`). A
secret-only rotation builds a new signer over the old transport. What remains for H1 is
everything below about one coherent bundle, generations, retained-bundle limits and the cache.

**Landed (2026-09-24, H1a):** the runtime bundle. `internal/runtimecfg` publishes one immutable
`Bundle` (directory snapshot, client key table, cluster set); a resign-mode request loads it once
before authentication and verifies, routes and signs from it alone, and `Publish` refuses an older
version or another lineage. The member publishes one per install from `OnInstall`, under its
install lock; the lab proxy on each directory install and each key-file change. `auth.Table` is an
immutable snapshot of the key store. A test publishes a rotated-secret bundle while a request is
authenticating and shows that request signing with the secret of the bundle it took; its negative
control (reading the clusters again after authentication) signs both requests with the new secret.

**Landed (2026-09-24, H1b):** secret generations. A rotation stamps `secret:<name>` with the
directory version that changed it, beside `cluster:<name>` (an endpoint-only edit moves the
cluster generation, not the secret's); a removed cluster or one no longer holding a control secret
loses its secret generation. The registry reuses a cluster only when its definition, secret and
secret generation are all unchanged, so a re-set to the same secret still counts as a rotation.
Each member reports `secrets` (cluster → generation it signs with) in its heartbeat, and the
cluster view answers `secret`: the generation, which live members have installed it, which are
pending on an older one, and which are silent. Installed is not drained: a request that took the
old bundle may still sign with the old secret until it finishes (H2 is the drain barrier).

**Landed (2026-09-24, H1c):** resource lifetimes and the retained-bundle bound. A request acquires
its bundle (a counted reference) in `ServeHTTP` and releases it when the handler returns, response
body included; the publisher lets go of a bundle only after the next one is current, so acquire
and retirement interlock through the count with no lock on the request path. A committed cluster
`Set` is counted by the registry and by each bundle on it, and each transport by the sets that use
it: the last release closes a retired transport's idle connections, so a secret-only rotation, which
shares the transport, closes nothing, and a request on a replaced set keeps its connections to the
end. Preparing a candidate takes no reference (its new transports never dialed), so a dropped
candidate holds nothing. At most `MaxRetired` (8) replaced bundles may be held; past that an install
becomes the pending bundle (a newer one replaces it) and is published when a held one drains. New
requests keep the served version meanwhile, the member's heartbeat reports that served version and
its secret generations as `applied`, so a fence waits, and `shunt_install_backpressure` and
`shunt_runtime_bundles_retired` show it. Nothing cuts a request short. There are no transport
health loops yet (P3a), so none needs sharing.

**Landed (2026-09-24, H1d):** the restart cache. The member writes an envelope (cache schema,
fleet protocol, proxy ID, identity, version, SHA-256 of the directory as written) to a 0600
temporary file, fsyncs it, renames it and fsyncs the directory, after the install lock is let go.
Writes are serialized and a version older than one already attempted is skipped, so the cache only
moves forward. A failure at any stage keeps the in-memory version and the last durable file (no
temporary file stays), counts `shunt_directory_cache_failures_total{stage}`, and is reported:
`/-/fleet` shows `durable` and `cache_error`, the heartbeat carries `durable`, and the Control
plane screen's proxy table shows Durable beside Applied. A cache that is torn, altered, oversized
(64 MiB, checked before decoding), of another schema or protocol, another proxy's, or without
identity is ignored and the proxy starts empty. T04's fault tests fail each stage and restart.
Incarnation lifecycle metadata, and refusing a restart from a cache older than an acknowledged
barrier, come with incarnations and barriers in H2.

**Landed (2026-09-24, H1e):** rotation and install diagnostics on every interface. `POST
/v1/clusters/{name}/credentials` (`shunt cluster credentials`, the cluster detail's Credentials
form) replaces a cluster's secret, or access key and secret, and nothing else; it shares cluster
add's credential check, so a wrong pair is refused with the old secret in place, runs under a
`cluster-credentials` operation, and answers the secret generation it started. `GET
/v1/fleet/{id}` (`shunt proxy show`, a click on a proxy in the Control plane screen) reports one
proxy's served, installed and durable versions, its secret generations against the control
plane's, and the problems in words; the heartbeat carries `installed` (while backpressured) and
`cache_error` for it. `old_generation_drained` is the D2 barrier's to prove; until H2 the UI and
CLI say that installed is not drained.

**Landed (2026-09-24, H1f); H1 passed:** docs/bench/h1.md. The data path is statistically
unchanged against the last pre-H1 commit; a request's bundle acquire costs 37 ns under sixteen-way
contention. A member install of 1,000 placements is 20 % slower (+2.4 ms), the durable cache's
directory fsync and checksummed envelope, off the request path and outside the install lock. A
100,000-placement install takes 854 ms and 224 MiB of transient allocation. Fifty secret-only
rotations with requests between them open one backend connection; the negative control opens
fifty. `make fleet` and `make walkthrough` passed on the H1 build.

### Runtime bundle and installation

Introduce a concrete immutable runtime bundle (suggested home:
`internal/runtimecfg`) with identity, parsed directory, client credential table,
cluster definitions, per-cluster secret generation, and upstream signer/pool
handles. It contains no etcd dependency. Expose it through the existing directory
and credential integration points; do not invent a general dependency container.

The proxy acquires one bundle before SigV4 credential lookup and retains it
through routing, cross-cluster operations, compensation and response completion.
Copy source and destination lookups use that same bundle. Mutable transport
health belongs to reusable pool handles; it must not change credential meaning.

Serialize the installation pipeline, including all fetch, heartbeat-triggered
refresh, forwarded-mutation refresh and startup/cache paths:

1. Decode with bounded size; validate identity, schema, references and versions.
   A different epoch is a recovery condition, never a numeric comparison.
2. Under the installation sequencer, recheck against the installed identity.
   Discard older or duplicate candidates. Conflicting content for the same
   identity is a protocol error and cannot renew freshness.
3. Prepare the candidate with a **candidate-local secret resolver**. Resolve all
   required secrets and construct immutable key/signing state without modifying
   the installed registry or key table. On failure, release candidate resources.
4. Publish one bundle pointer. If candidate construction is moved outside the
   sequencer for performance, the final identity check and publication remain
   serialized; a stale build cannot publish or update cache/ack state.
5. Enqueue cache persistence and report the exact installed identity. Report
   `durable_identity` separately. Safety acknowledgements follow D2's stricter
   durability and drain rules, not merely this pointer swap.

On the control node, `Store.prepare` must also be side-effect-free. A prospective
directory mutation prepares a candidate, commits its etcd CAS, then installs the
committed result. If CAS fails, discard the candidate. A watch can race that
path, so both use the same monotonic installer. A node that cannot construct a
committed version reports degraded/not-ready for decisions requiring that
version; it cannot silently overwrite the authoritative state with its older
local bundle.

### Rotation and resource lifetime

Split signer identity from transport identity. The former includes credential
generation; the latter includes endpoint, TLS and connection configuration.
Secret-only rotation produces a new immutable signer and reuses compatible
connection pools. No raw secret, secret hash or ciphertext becomes a metric
label, ETag, UI field or log value. Preflight secrets stay local to the candidate.

An old request may finish with its retained signer. Show rotation as
`installed` separately from `old_generation_drained`; use the D2 barrier machinery
if the operator needs a proven revocation boundary. Backend credential revocation
is external and happens only after the required drain. Rotation must document
the need for a backend overlap period; replacing a single backend secret before
old requests drain can fail those requests. Shunt must not claim it manages
backend IAM or automatically revokes a key.

Use explicit reference ownership for shared pools and retained bundles. Acquire
and retirement must interlock, so a publisher cannot close a handle while a
request is acquiring the old bundle. Never hold that small critical section
across network I/O. Close retired idle transports after their last user, and
never stop a shared health loop still used by a newer bundle. Cap outstanding
retired bundles (initial limit eight); backpressure/coalesce installation when
the bound is reached and expose `install_backpressure`. Do not kill in-flight
requests to make a benchmark pass. A barrier can remain pending until resources
become available.

### Restart cache

Persist the coherent bundle, identity, schema/protocol capability, proxy ID and
incarnation lifecycle metadata together. Use a mode-0600 temporary file on the
same filesystem, fsync, atomic rename, and parent-directory fsync; keep existing
secret-at-rest protections and permissions. Reject corrupt, partial, unknown
schema or mismatched-cluster caches. A single bounded writer coalesces ordinary
snapshots; required barrier durability cannot be skipped or reordered behind an
older queued write. Faults retain the in-memory last-known-good bundle, report
`cache_not_durable`, and prevent a drain acknowledgement requiring that cache.

Never restart offline from a snapshot older than an acknowledged barrier. Cache
and lifecycle state must prove that condition or startup stays closed. A brand-new
unregistered proxy cannot serve from a copied cache. A previously registered,
unretired proxy may serve eligible ACTIVE cache traffic during a control outage
only while its unresolved persistent membership continues to block transitions;
it starts stale and cannot renew or acknowledge anything. A retired identity's
cache cannot be used for offline restart.

## 4. D2 — admission, drain barriers and server leases

**Landed (2026-09-25, H2a–H2f); H2 passed:** docs/bench/h2.md. Local admission gates, proxy
incarnations with attested resolution, heartbeat drain proofs and server-granted leases, durable
barriers with resume and precommit cancel, and cutover, purge, movers and credential drain
through them. H2f changed three things this section specified: a mutation sent whole runs to the
backend's answer when its client leaves; cutover watches its quiet window before its hold, with
writes flowing, and re-checks it under the closed gate; forget keeps the incarnations it proved
ended. ADR-0021, "Decisions taken 2026-09-25", has each with its regression test.

### Local admission

Maintain bounded counters/gates per configured placement and in-use generation,
with cluster gates for maintenance. No object-key map. The request acquires its
bundle, resolves all involved scopes, and enters their gates in canonical order.
Gate closure and the check/increment sequence interlock. If a gate was closed
after the bundle was acquired, reject/retry admission; do not route on a newer
bundle using authentication from the older one.

For a write-routing change, close **all mutations on the affected placement**.
Healthy reads continue on its valid precommit read rule. Other placements are
unaffected. Classify every S3 operation, including DELETE, DeleteObjects,
CopyObject, UploadPartCopy, multipart initiation/parts/completion/abort, and
bucket configuration. Unknown potentially mutating operations fail closed on a
held scope. CreateBucket/DeleteBucket use the same reservation rules as operator
changes. Copies acquire a source-dependency token and a destination mutation
token; dual deletes and compensation retain their tokens through all effects.

Keep a mutation token until backend work has a definitive completed outcome,
including response-body parsing where a successful HTTP status can carry an S3
error. A definitive rejection with no outstanding effect can drain. A response
lost after dispatch, partial compensation, or cancellation with an unknown
backend outcome marks that scope/generation `uncertain`. Local handler exit does
not erase it. Request-local classification may distinguish a proven pre-dispatch
failure; if proof is absent, classify conservatively. Persist uncertainty through
the incarnation protocol below, not per-request journal writes.

Multipart IDs pin a backend. Before a transition that would move writes from
that backend, prevent new source initiations and check existing uploads after
old mutation requests drain. If a source upload remains, block with
`multipart_open`. Cancel the precommit transition to let the client finish, or
use the existing backend upload-management workflow to abort it, then retry.
Do not let an encoded uploadId bypass the hold. This deliberately tightens the
old promise that every pre-transition upload can finish across any transition.
Do not store upload IDs in etcd. Record a count and sample-free blocker; list
uploads from the backend on demand using existing streaming pagination.

### Incarnations and missing members

A proxy ID is stable; each process has a random incarnation. Before accepting
requests, durably mark the incarnation unclean locally and register it when the
control plane is available. Membership records outlive leases. An online proxy
that already proved a scope drained can send that scope's barrier proof; a
different incarnation cannot attest that the previous process had no work.

A clean retirement stops admission, drains all backend effects, persists a
non-restartable local marker, then records retirement in the control plane.
Lost replies are retried idempotently. A crash or unknown outcome leaves the old
incarnation unresolved. Store current and unresolved incarnations, compacting
clean history; cap unresolved records per proxy and refuse additional registrations
at the bound rather than silently discard evidence. Use a compact global
uncertainty marker when a crashed incarnation's affected scopes are unknown.

Every barrier considers all unretired/unresolved incarnations that could have
served its prechange state, **including silent members in later ramp steps**.
Lease expiry never supplies a drain proof. `proxy forget` cannot discharge this
obligation; it refuses with blockers until retirement or the D3 quiescence
workflow resolves them. A restarted process can resume service where safe, but
cannot clear its predecessor's uncertainty merely by reporting zero counters.

### Barrier state machine

| Phase | Durable action and invariant | Failure/retry behavior |
|---|---|---|
| Precondition | Reserve scope; sync authoritative state; validate epoch/generation, capabilities and existing holds. Capture membership revision. | Conflict returns existing operation or `409`; no side effects. |
| Hold | Atomically record unique barrier ID, proposed change and current route. Proxies close admission and durably install the hold. | Owner loss leaves the hold, with the same operation ID. |
| Drain | Require matching proofs from every relevant incarnation: hold installed durably, closed admission, old effects complete, uncertainty absent. | Absent, stalled, multipart or ambiguous work is an explicit blocker, not an expiring success condition. |
| Commit | CAS barrier ID, expected generation/epoch, operation phase and required evidence. Write new route plus operation phase. | Unknown transaction result is resolved by reading state; never blindly repeat a different step. |
| Settle | Proxies install committed route. Old hold proxies still refuse writes. Record enforcement coverage separately from the routing commit. | Timeout leaves pending/blocked; no route rollback. |
| Done | Required install/drain predicates are satisfied; persist result and release scope reservation. | Repeated requests return the same result. |

Membership changes must not escape the captured set. New registrations during a
barrier either install its current hold before admission or remain unready; the
registration transaction checks active barriers/membership generation. A delayed
first heartbeat cannot introduce an old serving process behind an already
completed fence. This registration rule also applies to local/lab server startup.

The local proxy participates even when there are no remote members. A control
node with no data listener has no artificial request count; actual local movers
and other backend workers do participate. Audit every direct directory-state
mutation path so CLI aliases and file-backend commands cannot bypass a barrier.

Cancellation is a durable request. Before commit, CAS the original barrier and
unchanged generation back to the original routing, then settle the release before
marking cancelled. It does not clear uncertainty or undo backend writes. After
commit, cancellation returns `409 not_cancellable`; do not shrink a committed
ramp. HTTP wait expiry does not perform cancellation. Resume schedules reconciliation
of the existing operation, not a new operation with guessed arguments.

### Cutover, purge, workers and maintenance

Cutover still needs the existing mover and fallback evidence, now bound to epoch,
placement generation and an observation interval with complete membership
coverage. Counters reset on incarnation changes; subtracting unrelated counters
must not manufacture a quiet window. Unknown/stale telemetry is no evidence.

Before source destruction, close new source-dependent admission and drain old
GET/LIST/copy-source requests, delayed dual deletes, and mover activity. A retained
snapshot can otherwise access a deleted source. Keep the source reserved through
cleanup; do not reuse/recreate its backend bucket while an old worker's effects
are uncertain. External CLI movers must register generation-bound work sessions
through the control API and honor holds; browser movers use the same path.
Expired worker sessions block destructive cleanup until reconciled, just like
proxy incarnations. Unmanaged direct-backend writers remain an explicit operator
precondition for migration, maintenance and recovery, not something a proxy fence
can stop.

Read-only activation closes mutation admission and drains accepted mutations
for its placement or all placements referencing its cluster. A stored flag is
`desired`; it becomes `effective` only on completion of the relevant barrier.
Read-only deactivation is generation-checked and settled through the same
operation record. Scope interaction and errors must be identical on API, CLI and
GUI. Credential revocation barriers also drain all requests using the old signer,
including reads, while ordinary rotation can report installation progress first.

### Lease algorithm

**Landed (2026-09-24):** T07 without identity. The lease runs from the monotonic send time for
`min(server grant, local lease_ttl)`, the answer echoes `seq`, and a zero grant, an older or
mismatched answer, a version behind the proxy's, or a late answer renews nothing
(`internal/member`, `renew`). Epoch checks wait for H0.

Each heartbeat has a monotonically increasing sequence and carries incarnation,
identity and protocol capability. At monotonic send time `t0`, record the request.
Validate a successful response's cluster/epoch, sequence and positive bounded
`lease_ttl_ms`. A candidate deadline is `t0 + min(server_grant, local_max_ttl)`.
Never use receive time or a wall-clock timestamp. An older response cannot
replace a newer grant; a delayed response past its deadline cannot renew it.

Renew freshness only after the answered identity is coherently installed; the
deadline still starts at `t0`. A lower server version in the same epoch is a
protocol fault requiring authoritative resync, not proof that the proxy is ahead.
A different epoch enters recovery-required mode. Only a response based on a
current authoritative control read can grant freshness. Expired/invalid grants
refuse non-ACTIVE mutations; ACTIVE outage behavior remains as documented subject
to registration/recovery rules. This lease is an admission rule, not permission
for the control plane to ignore unfinished requests.

Persisting a barrier acknowledgement requires its closed gate and cache to be
restart-safe before sending it. Ordinary telemetry cannot prevent lease or drain
messages: split safety metadata from optional telemetry or cap/truncate telemetry
first. Preserve the existing total body cap; oversized safety state is paged with
a barrier generation and only a complete set counts as proof. No missing page
may be interpreted as zero blockers.

## 5. D3 — snapshot restore and cold recovery

### Supported recovery contract

Restoring etcd rolls back metadata, not backend writes. Keep automatic in-place
recovery with old proxies serving **unsupported** for generic S3. A restore is a
cold recovery with explicit data reconciliation. A new epoch prevents accidental
cache equivalence; it cannot force an unreachable old ACTIVE proxy to stop.

1. Isolate the old deployment: stop client ingress, stop proxies and movers, and
   deny their backend access. Inventory all deployment instances using the
   orchestrator/service inventory, not the backup's older membership list.
2. Establish that accepted backend mutations have finished using backend-specific
   drain/isolation evidence. A stopped process, revoked key, quiet counter or
   elapsed lease alone does not prove this. Record evidence references and scope;
   never upload secrets as evidence. If the backend offers no sufficient proof,
   recovery remains blocked pending a safe backend/operator recovery procedure.
3. Restore to new empty directories with the matching encryption key. Validate
   snapshot integrity, schema and manifest; never overwrite a running data dir.
   Create a new epoch in `recovering` mode before exposing the normal API. All
   restored nodes use one recovery ID/epoch, not independently generated epochs.
   etcd revision bump/compaction may be used as its documentation requires but
   does not replace the application epoch.
4. Mark old operations interrupted and old confirmation tokens invalid. Start
   no destructive worker. Compare restored placements against external audit,
   operator records and backend contents; the snapshot is only a starting point.
5. Submit a recovery manifest and validated per-placement decisions. Missing
   post-backup buckets/tenants, changed credentials, dual copies, and lost deletes
   are explicit conflicts. Never choose a copy by newest timestamp or primary-first
   fallback and call that reconciliation. Where backend history cannot determine
   the acknowledged value, report possible data loss and require an explicit
   authoritative data-source decision. No algorithm in this plan reconstructs
   history that was never retained.
6. Prepare new runtime caches and enroll new proxy incarnations in recovery mode
   with data listeners closed. Reconcile required scopes; execute a generation-
   checked activation operation only when the inventory, quiescence, reconciliation
   and capability predicates are satisfied. Install the same epoch on the serving
   fleet before reopening ingress. Retain the recovery manifest and audit.

Recovery manifests contain recovery ID, snapshot digest, old/new lineage,
inventory/evidence references, manifest revision, and placement-level outcomes.
Object-level evidence stays in the existing external audit/ledger location, not
etcd. Limit and paginate placement batches, hash the canonical manifest, and bind
activation confirmation to that hash and revision. Evidence is an operator
attestation where automated verification is unavailable: label it honestly in
the API and UI. A text checkbox is not an automatic proof. Document the supported
backend-specific procedure before accepting that backend's recovery gate.

### Detection and cache behavior

`GET /v1/directory` includes the caller's epoch; mismatch never returns 304.
Return `409 epoch_mismatch` plus non-secret recovery identity. Heartbeat mismatch
cannot renew freshness. A proxy that observes an epoch change stops data service
and requires re-enrollment; the cluster should already have been externally
quiesced. Reject endpoints belonging to another cluster. Serialize recovery
installation so a late old-epoch fetch cannot publish after enrollment.

A normal control restart keeps the epoch. Snapshot export records application
identity, schema and checksum in a companion manifest and verifies a complete
download before atomically publishing the backup. CLI restore validates it;
legacy snapshots require explicit cold migration and cannot silently synthesize
safe fleet state. The GUI guides offline restore and displays recovery state;
it never accepts an arbitrary server filesystem path for remote execution.

Use an immutable export ID to bind the streamed snapshot and manifest. The
snapshot response carries that ID; the client retrieves the manifest for exactly
that export, verifies checksum/length, and publishes the backup only after both
are complete. Bound export retention and concurrent exporters; an expired or
interrupted export is retried as a new export, never paired with a latest-state
manifest. The manifest identity must be read from the exported snapshot itself,
not from the live store before or after the streaming snapshot call.

## 6. D4 — recoverable membership and observed health

### Join and remove

**Landed (2026-09-23, on `1-to-n-bucket-support`):** a joining node is now added
with `MemberAddAsLearner` and promotes itself with `MemberPromote` once etcd
reports it caught up, retrying while it is not (`internal/cp/etcd.go`, `promote`);
a learner that restarts before promotion tries again and runs as a learner with a
warning if it cannot. This removed the intermittent `internal/cp` test failure
("incompatible with current running cluster") and a ~7 s stall on every join. What
remains for D4/T12 is everything durable below: the persisted join intent and its
phases, reconciling a lost add response, resume and cancel, and removal by ID.

Use the existing etcd client learner API; do not expose etcd client ports to
proxies or browsers. Before a membership mutation, require quorum and capability
checks. Serialize membership intents durably; at most one unfinished membership
change, accounting for the configured etcd learner limit. A local mutex or a
lease that disappears is insufficient.

The join workflow is `prepared → learner_added → bootstrap_saved → started →
catching_up → promoted → succeeded`. Local bootstrap preflight checks an empty
writable data directory, atomic secret-file creation, normalized peer/API URLs,
unique name, and connectivity before asking the cluster to add a learner.
Preflight cannot eliminate later disk/network failures, so every phase resumes.

Persist the intent before `MemberAddAsLearner`. If its response is lost, reconcile
the member list using the normalized peer URL and saved intent; capture the
stable member ID before any subsequent action. Never add a second member just
because a request timed out. Member names may be empty until the process starts.
Bootstrap retrieval is authenticated, explicitly sensitive, `no-store`, and
idempotent for the same intent/member. Keep the encryption key out of operation
JSON, SSE, CLI normal output, URLs, audit and GUI state. Save local bootstrap with
fsync before starting etcd. Reject a retry whose peer URL, identity or data dir
conflicts with the saved join state.

Promotion checks current learner identity and relies on etcd's caught-up
validation; a lagging learner remains pending with a reason. Never disable
strict reconfiguration checks to make promotion pass. Expose explicit resume
and cancel/remove actions. Cancellation before promotion can remove the exact
learner ID after verifying the current phase. If a membership RPC outcome is
unknown, reconcile it before permitting a conflicting action. After promotion,
removal is a separate quorum-checked operation; cancellation cannot silently
remove a voter.

Remove by member ID, with name as an optional unique-resolution convenience.
Resolve once and bind confirmation to ID plus membership revision. A late worker
must never remove a replacement that reused a name. Persist intent, issue RPC,
then reconcile by ID after a lost response. Never auto-compensate an ambiguous
join by removing a member. The only voter cannot be removed. A single voter can
admit a learner without increasing quorum; promoting to two voters explicitly
warns about the resulting two-vote quorum and recommends completing a three-voter
deployment promptly. No design here makes two voters tolerant of one failure.

### Health model

Retain `started` only as historical metadata. Report each member's role,
ID/name, joined state, observed `healthy|unreachable|unknown`, observation time,
observer and reason. A member that never started is not healthy. A named member
that is stopped is not healthy. Expired cached data becomes `unknown`, never a
fresh green badge.

Each node has an authenticated, local-only status handler on the existing
control listener. It reports local etcd status without probing peers. A bounded
background sampler probes registered control API endpoints and separately runs
a bounded linearizable etcd read to establish current quorum reachability.
Configured peer URLs are not assumed to be API URLs; store an advertised API URL
and validate it at join. Do not open a new public etcd port. Status handlers
serve the cached sample, not N probes per browser refresh.

Initial settings: one cycle per 5 seconds with jitter, 3-second deadlines,
maximum five concurrent probes per node, stale after 15 seconds. A cycle cannot
overlap its predecessor indefinitely; cancel it at the deadline. Report
membership last observed using cached/serializable reads while quorum is lost.
The existing default etcd MemberList is linearizable; using it for all status
would make the diagnostic page disappear precisely when it is needed.

`quorum` is `reachable|unavailable|unknown` with evidence and timestamp. Known
leader ID and named-member count are not quorum evidence. Health reads remain
available during quorum loss, clearly marked stale/partial. Mutations still
require authoritative quorum and all normal preconditions. This status model
cannot promise that a future write will succeed.

## 7. D5 — operator state and UI consistency

All mutations return a typed operation or a typed refusal. The UI and CLI read
the same operation; they do not infer success from HTTP 200/202, a changed desired
flag, a toast timer or an SSE notification. Show pending and blocked phases with
named reasons, last observation time, and server-provided allowed actions.

Separate `desired`, `installed`, `effective`, and `backend_quiescent` where they
mean different things. A read-only operation can be installed but not yet safe
for backend maintenance. Audit records tie the request, authoritative commit,
drain proof and final result to the same operation. Do not claim a backend's
direct clients are fenced by shunt.

React forms keep server state and local draft state separately. Initialize a
draft on resource selection; refresh pristine fields only. Telemetry events
update charts, not ramp ratio/prefix inputs. A directory generation change while
dirty produces a conflict banner and an explicit reload/rebase action. Submit
the expected generation. A 409 preserves the draft and displays the current
server state. Ignore/abort late fetches for a previously selected bucket.

SSE events carry epoch, resource identity and sequence. Use them as invalidation
hints and fetch authoritative read models. Duplicates/out-of-order events do not
regress state. A gap, replay expiry, epoch change or server switch forces a full
resync while retaining drafts as conflicted. Resume tokens are scoped to their
origin/epoch; do not pretend independent node-local event IDs are a global log.
Coalesce refreshes by resource with one in-flight request and a dirty bit, so a
telemetry burst cannot generate an unbounded fetch queue.

## 8. Performance, limits and observability

Proposed budgets are acceptance targets, not measured claims:

| Path | Bound / acceptance target |
|---|---|
| S3 admission | Local bundle acquisition and bounded scope counters; no control RPC, disk sync, body copy or per-object map; no lock held during I/O. |
| Snapshot reload | 100,000 placements benchmark; at most one installation build and one coalesced cache write, plus at most eight retained published bundles; reject oversized payload before unbounded allocation. |
| Fleet | 1,000 simulated proxies, 100 concurrent placement barriers; safety payload bounded/paged, total body cap enforced; no per-heartbeat scan of all 100,000 placements. |
| Healthy barrier | With 1-second heartbeat and no existing work, target p99 under 5 seconds on the disclosed test topology; long/uncertain requests are explicit exceptions, never expired to meet this target. |
| Data performance | Against the reviewed baseline on identical hardware: no more than 5% regression in throughput, added p99 and CPU/GiB for unaffected traffic; report allocations and RSS. |
| Control load | Heartbeat/ack processing O(active barriers + reported windows) per proxy; persistent membership writes only on lifecycle changes; lease refresh remains bounded by proxy count. |
| UI/health | One coalesced fetch per resource; health sample independent of viewers; test 30 control members, 1,000 proxies, reconnect storms and telemetry bursts. |

The existing dense telemetry payload already approaches the 1 MiB heartbeat cap.
Do not append barrier arrays to it without reserving space and testing the cap.
Bound worker goroutines, queues, transaction bytes, response sizes and backend
concurrency. Large maintenance/recovery operations page work without ever
interpreting a partial result as complete. Define explicit limit errors in the
contracts; do not silently sample safety evidence.

Before implementation, add metric families to `docs/telemetry-catalog.md` for
install results/duration, cache persistence failures, barrier duration/blocker
counts, lease grant errors, unresolved incarnations, recovery phase, join phase
and health observation age. Use existing families where possible. Labels have
bounded reason/phase enums and established cluster/proxy scope. No operation ID,
epoch, secret, object key or unbounded error string as a metric label. Detailed
IDs belong in authenticated read models and structured logs. Add counters and
alerts for a stuck hold, uncertain backend effects, expired health data and an
incomplete recovery; a stale ACTIVE proxy alone does not make liveness fail.

## 9. Upgrade, rollback and remaining limits

**Amended 2026-09-24 (ADR-0021):** no protocol-1 fleet is deployed, so the mixed-version
machinery below (capabilities in registration, the controlled handover, the rollout flag, T18)
is not built. Protocol 2 replaces protocol 1, and a binary that cannot read protocol-2 state
refuses to start. The paragraphs on rollback after protocol-2 state is written still apply.

Introduce `protocol: 2` plus named capabilities (`atomic_runtime`,
`drain_barrier`, `epoch_identity`, `server_lease`) in registration and status.
Legacy proxies may remain visible while staging, but no safety-sensitive change
may count them as capable. Old control handlers must not remain reachable for
mutations after enforcement is enabled. Use a controlled deployment handover:

1. Freeze migration, purge, maintenance and credential mutation through **all**
   old operator endpoints; finish/reconcile existing operations. Back up metadata.
2. Quiesce affected traffic and old worker/proxy incarnations as required by D2.
   Upgrade all control nodes, establish the application identity, and move admin
   ingress only to nodes enforcing protocol 2. Version detection alone is not a
   guard if an old endpoint can still accept a mutation.
3. Upgrade/enroll every proxy, invalidate unsafe old cache restart paths, and
   verify capability, bundle and drain state. New/unregistered old binaries
   cannot join a protocol-2 deployment. Record handover completion durably.
4. Run canary operations with all three frontends, then unfreeze mutations.
   Do not advertise zero-downtime protocol upgrade before this is proven.

Use a shared minimum-protocol gate, not a user-toggleable safety bypass. The
default-off rollout flag, if required by repository policy, permits staged code
deployment only while dangerous operations remain refused; its removal criterion
is a completed fleet handover. There is no `--force` path that skips drain,
quiescence, generation or epoch checks.

Once protocol-2 state has been written, a binary that cannot interpret it must
refuse startup. Downgrade requires cold recovery to a compatible deployment;
restoring an older backup is not a safe live rollback. A failed runtime candidate
before publication is safely discarded, and a precommit hold can be cancelled
under D2. These are the bounded rollback paths.

This plan intentionally chooses blocked progress over unverifiable data safety.
For opaque backends, some crashed/ambiguous mutations require an operator-assisted
outage and reconciliation. Reducing that limitation requires backend-enforced
fencing or stronger backend recovery evidence, a separately designed capability.
Control transport TLS and the known backend conditional-operation limitations
remain outside these five fixes and must not disappear from release reporting.
