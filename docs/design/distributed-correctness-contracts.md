# Distributed correctness: API, CLI and GUI contracts

**Proposed interface specification, not the shipped API.** Part of
[ADR-0021](../adr/0021-distributed-correctness.md). Read the
[protocol design](distributed-correctness.md) for safety predicates and the
[delivery plan](../prompts/distributed-hardening.md) for tests. Names below are
implementation targets; changes require updating this contract and its parity
tests together. Existing `docs/reference/control-routes.json` continues to
describe only implemented routes until each slice lands.

## 1. Shared rules

- Existing authentication applies to every route, including status, operations,
  bootstrap and recovery. Preserve the existing actor derivation; never trust a
  browser-supplied actor. Secrets appear only in explicitly sensitive request or
  bootstrap bodies, never ordinary operation/read-model JSON, events or logs.
- New protocol fields use `cluster_id`, `epoch` and decimal-string `version` /
  `generation`. These are nested under `identity` so legacy numeric fields do
  not silently change type. Deprecate legacy fields after the controlled upgrade.
  Member IDs are opaque strings. Durations use named integer millisecond fields.
- New mutation requests require `Idempotency-Key` and an expected scope
  generation; destructive confirmations also bind epoch, intent digest, resource
  IDs, evidence revision and expiry. Backend effects require a durable operation.
  A stale or reused confirmation never executes a changed action.
- `202 Accepted` means a durable operation exists, not that the effect is complete.
  Set `Location: /v1/operations/<id>`. `200` may return an existing completed
  operation on an idempotent retry. The envelope always contains `operation`.
  A bounded wait may return the terminal record; wait timeout returns the current
  record as 202, never a successful empty body.
- `GET /v1/operations/{id}` returns 200 for every known state, including failed or
  blocked. HTTP transport success is distinct from operation success. GET status
  reads are non-mutating; reload/reconnect must not restart work.
- Structured refusals have `{code, message, retryable, blockers, current_identity,
  current_generation, operation_id}` with inapplicable fields omitted. Errors
  must not include request bodies or secrets. The same code is visible in CLI
  JSON and GUI details.
- Cursor-paginated lists declare whether the result is complete. Operation details
  return a bounded blocker summary plus a paginated blockers link/count. Missing
  pages never mean missing blockers. Initial maximum page size is 500; server
  validation rejects invalid limits rather than silently creating huge responses.

### Operation example

Illustrative protocol-2 response; identifiers are placeholders and secrets are
absent. `observed_at` is for display, not a lease clock.

```json
{
  "operation": {
    "id": "op-example",
    "kind": "placement-read-only",
    "identity": {"cluster_id": "cluster-example", "epoch": "epoch-example", "version": "43"},
    "scope": {"tenant": "team", "bucket": "data", "generation": "8"},
    "status": "blocked",
    "phase": "drain",
    "sequence": "4",
    "effect_state": "none",
    "desired": {"read_only": true},
    "effective": false,
    "barrier_id": "barrier-example",
    "blockers": [{"code": "old_requests", "proxy_id": "proxy-b", "incarnation": "incarnation-example", "count": 1}],
    "blocker_count": 1,
    "allowed_actions": ["resume", "cancel"],
    "observed_at": "2026-09-24T00:00:00Z"
  }
}
```

`effect_state: none` here means the requested read-only effect has not committed;
the operation's hold can already be installed. `effective: true` is specific to
the requested predicate, not an assertion about direct backend clients. For
backend maintenance, expose `backend_quiescent` and its evidence independently.

### Common outcomes

| HTTP / code | Meaning | Frontend behavior |
|---|---|---|
| 400 `invalid_request` | Invalid shape, enum, identity or unknown field | Preserve input and show field errors. |
| 401/403 | Authentication/authorization failed | Reauthenticate or refuse; never retry the mutation anonymously. |
| 409 `generation_conflict` | Resource changed since the operator's view | Fetch latest state, preserve draft, require explicit rebase. |
| 409 `operation_conflict` | A conflicting durable intent already owns the scope | Link to that operation; do not create another. |
| 409 `idempotency_conflict` | Same key, different canonical intent | Correct the client; never silently generate a key and retry. |
| 409 `epoch_mismatch` | Different recovery lineage | Stop automatic mutations and refresh recovery state. |
| 409 `not_cancellable` | Requested cancellation is past the safe point | Show committed effect and available follow-up actions. |
| 409 `retirement_unproven` | Forget would erase unresolved work | Show incarnation/evidence blockers and recovery handoff. |
| 426 `upgrade_required` | A required protocol capability is missing | List incompatible members and the upgrade procedure. |
| 429 `operation_capacity` | Bounded active-operation capacity reached | Retry with backoff; existing operations/status remain available. |
| 503 `control_unavailable` | Cannot validate/commit authoritative control state | Keep last observation marked stale; retry the same idempotency key. |
| 503 `recovery_required` | Deployment is recovering | Show recovery workflow; disable normal mutations. |

A nonterminal record with blockers is not an HTTP error. Initial blocker enums:
`old_requests`, `backend_outcome_unknown`, `proxy_missing`, `incarnation_unresolved`,
`cache_not_durable`, `install_backpressure`, `multipart_open`, `worker_unresolved`,
`capability_missing`, `quorum_unavailable`, `learner_not_caught_up`,
`recovery_evidence_missing`, `reconciliation_required`. Counts and scope details
are bounded; no object keys or credentials in blocker records.

## 2. Operator parity matrix

The CLI verbs below are proposed additions or extensions. Existing spellings
remain aliases where safe. Every row requires an API integration test, CLI
behavior test and GUI workflow test; this is behavioral parity, not just a list
of routes that the UI imports.

| Capability | API contract | CLI contract | GUI contract | Tests |
|---|---|---|---|---|
| Inspect coherent snapshot/cache/install health (D1) | Extend `GET /v1/fleet` and add `GET /v1/fleet/{id}` with installed/durable identities, protocol, last install error, secret generations only | `shunt proxy list`, `shunt proxy inspect <id> --json` | Fleet detail: installed vs durable, age and install blockers | T01–T04, T17 |
| Rotate upstream credentials (D1) | `POST /v1/clusters/{name}/credentials`; sensitive request; operation result gives new generation and installation/drain coverage | `shunt cluster credentials set <name> --credentials-file <path> [--wait]`; read a mode-0600 file or stdin, never secret arguments | Cluster credential form; clear secrets on submit/close; rotation progress and overlap/revocation guidance | T03, T04, T17 |
| Inspect/resume/cancel operation (D2/D5) | Existing `GET /v1/operations[/{id}]`; add `GET /v1/operations/{id}/blockers`, `POST /v1/operations/{id}/resume`, `POST /v1/operations/{id}/cancel` | `shunt operation list\|show\|wait\|resume\|cancel` | Shared operation drawer with phases, blockers, outcome and allowed actions | T08, T09, T15, T17 |
| Ramp and migrate start (D2) | Existing `POST /v1/operations` kinds and placement aliases; aliases enter the same durable barrier | Existing `shunt ramp`, `shunt migrate start`; expected generation and request ID in JSON/output | Migrations submit from a dirty-safe draft; operation drawer displays hold/drain/settle | T05–T09, T16, T17 |
| Cutover, purge, finish (D2) | Existing operations and placement aliases, confirmation bound to generation and drain evidence | Existing `shunt cutover`, `purge-source`, `migrate finish`; same wait semantics | Existing migration workflow; source-read/worker blockers and confirmation evidence shown | T06, T09, T14, T17 |
| Register/run/resume mover (D2) | Existing mover operation gains durable generation-bound worker session; `POST /v1/operations/{id}/worker-heartbeat` carries session/sequence; status in operation record | Existing `shunt migrate run` enrolls before backend work; resume names existing operation | Existing browser mover shares server engine and session read model | T06, T09, T14, T17 |
| Placement/cluster read-only (D2) | Existing `POST .../read-only` routes create operations with desired/effective/drained fields | Existing `shunt readonly`, `shunt cluster readonly`; print pending/blockers until complete | Buckets/Clusters show requested and effective state; no success toast while pending | T08, T15, T17 |
| Retire/forget proxy (D2) | Add `POST /v1/fleet/{id}/retire`; existing DELETE refuses unresolved incarnations; status available in fleet detail/operation | `shunt proxy retire <id> [--wait]`; `shunt proxy forget <id>` obeys proof gate | Fleet retirement flow with drain progress; forget only when allowed | T06, T08, T17 |
| Inspect server lease (D2) | Fleet detail: granted TTL, sequence, staleness/reason and observation time, never a portable monotonic timestamp | `shunt proxy inspect <id>` | Fleet detail explains stale vs unresolved, including current grant | T07, T17 |
| Save/inspect snapshot (D3) | Existing `GET /v1/control/snapshot` plus `GET /v1/control/snapshot/manifest`; bind both to the same export ID and checksum | Existing `shunt-control snapshot save <path>` saves/verifies manifest atomically; add `snapshot inspect <path>` offline | Control Plane backup download/metadata; show completed checksum verification, never a partial backup as success | T10, T17 |
| Restore local etcd files (D3) | No arbitrary server-file execution endpoint; shared offline manifest validator, online recovery APIs below | Existing `shunt-control snapshot restore <path> --manifest <path> --recovery-id <id>` into new empty dirs | Recovery wizard explains/copies exact node-local command, lists prerequisites and observes resulting recovery ID | T10, T11, T17 |
| Inspect/plan recovery (D3) | `GET /v1/control/recovery`; `POST /v1/control/recovery/plan` validates revisioned inventory, quiescence evidence and placement decisions without activation | `shunt-control recovery status\|plan --manifest <path> --json` | Recovery wizard: inventory, evidence provenance, conflicts, placement decisions, planned changes | T10, T11, T17 |
| Execute reconciliation/activate recovery (D3) | `POST /v1/control/recovery/reconcile`, `POST /v1/control/recovery/activate`; durable operations, manifest digest and confirmation required | `shunt-control recovery reconcile\|activate --recovery-id <id> --manifest-revision <n> [--wait]` | Recovery wizard reviews authoritative decisions and blockers; activation disabled until predicates pass | T11, T17 |
| Join/resume/cancel control member (D4) | Existing `POST /v1/control/members` becomes idempotent learner intent; operation actions above; sensitive `POST /v1/control/joins/{id}/bootstrap` for node bootstrap only | Existing `shunt-control join` persists join ID locally; `join --resume <id>`; `shunt-control operation show\|wait\|resume\|cancel` uses shared operation client | Control Plane join wizard creates/views intent, copies bootstrap command and follows learner progress; no DEK in browser | T12, T17 |
| Remove member including unnamed learner (D4) | Add unambiguous `DELETE /v1/control/members/by-id/{id}`; existing name path resolves once or returns conflict; confirmation binds ID/revision | `shunt-control member remove --id <id>`; existing name argument resolves uniquely | Member row keyed by ID with role, phase, quorum impact and exact removal confirmation | T12, T17 |
| Observe control health/quorum (D4) | Extend `GET /v1/control` / `/status`; add authenticated `GET /v1/control/local-status` for bounded sampler; aggregate includes evidence/age | Existing `shunt-control status`, `member list`, `--json`; exit/status distinguishes unknown | Control Plane separates voter/learner membership, observed health and recent quorum evidence | T13, T17 |
| Keep drafts across telemetry/reconnect (D5) | Existing placement/cluster views gain identity/generation; existing events gain scoped identity/sequence and replay-reset signal | Mutating CLI passes `--if-generation` or reads state once and retains that generation during retries | Migrations dirty forms, conflict resolution, reconnect/stale indicators | T16, T17 |

`shunt-control operation` and `shunt operation` share a concrete client helper;
they are not separate implementations of operation semantics. Human output may
be concise, but JSON contains the same status, IDs, reasons and evidence as the
API. Every accepted mutation prints its operation ID so a broken terminal does
not strand work.

### CLI completion semantics

For accepted mutations without `--wait`, exit 0 means **accepted**, and output
must say that explicitly. With `--wait`, exit 0 means succeeded; exit 3 means the
wait deadline elapsed with nonterminal work and prints the resume/status command;
exit 1 means failed/refused/cancelled; exit 2 means usage/validation error. SIGINT
stops local waiting, exits 130 and prints the operation ID if known; it does not
cancel the server operation. `--json` emits a single final envelope, not mixed
progress text; human progress goes to stderr. No automatic retry without the
same persisted request ID after a transport error.

## 3. Proxy wire protocol

Extend heartbeat requests with `protocol`, `capabilities`, `identity`,
`durable_identity`, `incarnation`, `sequence` and bounded barrier acknowledgements.
Each ack carries barrier ID, scope generation, installed hold identity,
`admission_closed`, drained counter classes, and uncertainty state. The server
validates exact identities; a larger version alone is not a proof for a different
barrier. Historical incarnation proofs cannot be replayed for a new process.

Replies include authoritative identity, accepted heartbeat sequence,
`lease_ttl_ms`, requested barrier pages and recovery mode. Page acknowledgements
include a manifest generation and total count; incomplete sets never satisfy the
barrier. Invalid sequence/identity/capability cannot refresh liveness evidence
used by a fence. Ordinary presence telemetry may still record that the process
was observed, separately from a valid lease grant.

`GET /v1/directory?cluster_id=...&epoch=...&since=...&wait=...` only returns 304
for an exact lineage and version. A client ahead in the same epoch triggers
resync/error; another epoch returns 409. Responses specify schema/protocol and a
content identity to detect conflicting duplicate versions. Snapshot identity and
all materialized credentials are produced from one committed directory state.
Use transport authentication and secret redaction from the existing control
channel; do not advertise the plaintext baseline as a production-secure channel.

The local `/-/fleet` read model mirrors installed/durable identity, grant age,
staleness and bounded drain summary without exposing credentials. Data-path
rejections retain S3 XML `ServiceUnavailable` with `Retry-After: 1`; authenticated
admin models provide detailed reasons. No backend identifiers or operation
secrets are added to public S3 error bodies.

Worker heartbeats similarly carry operation ID, worker session ID, generation,
epoch, sequence, in-flight counts and uncertainty. They never authorize starting
work for a different generation. On owner loss, prevent new work and preserve
unknown dispatched effects for reconciliation. Finishing a CLI process does not
automatically mark an external worker session clean.

## 4. Health and recovery read models

`GET /v1/control` retains existing compatibility fields but adds:

```
identity, mode, observed_at, partial, observation_error
membership_revision, members[] {id, name, role, started,
  api_url, health, observed_at, observer, reason}
quorum {state, observed_at, observer, method, error_code}
active_membership_operation, recovery {id, phase, blockers, manifest_revision}
```

Deprecated `started` never drives health color or quorum math. An unavailable
linearizable read yields a diagnostic response with `quorum.state=unavailable`
and a timestamp, not a silently retained green state. No observation yet is
`unknown`. Read failures expose codes, not raw errors containing credentials.

Recovery plan output distinguishes `automatically_verified`,
`operator_attested`, `missing`, and `conflicting` evidence. Every reconciliation
decision names its placement generation, chosen authoritative data source,
validation result and retained audit reference. Activation includes the exact
manifest digest and blocks on unsatisfied predicates. UI and CLI must explicitly
present possible data loss where an acknowledged history cannot be reconstructed;
there is no misleading global “all safe” checkbox.

The restore and join GUI handoffs are deliberate locality boundaries: a browser
can manage intent, validation, progress and cluster changes through APIs, while
the node-local CLI writes etcd data directories and the encryption key. Tests
must drive both halves of the workflow. “CLI only” cannot be used to omit GUI
monitoring or logical control actions.

## 5. Compatibility and route inventory

During implementation, register every new control route through the repository's
route inventory mechanism, regenerate `docs/reference/control-routes.json`,
update `web/src/api/routes.json` and typed `web/src/api/client.ts`, and update the
live reference and CLI help. Protocol-only heartbeat/bootstrap/local-status
routes need explicit GUI/CLI rationale in the inventory, plus tests of their
operator-facing read models. Do not expose secrets just to satisfy route parity.

Legacy route aliases call the same coordinator; they cannot bypass operation
records, idempotency, expected generations or protocol checks. A legacy mutation
without necessary identity/capability gets a typed upgrade response after the
handover. Preserve ordinary read compatibility where it remains truthful.

Keep the current manual visual acceptance policy from ADR-0017 unless explicitly
changed. Component tests and Go/CLI integration tests are mandatory and must
assert behavior; the route inventory is an additional completeness check, not a
substitute for them. No new browser-test dependency is required by this proposal.
