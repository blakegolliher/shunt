# Distributed hardening: implementation and acceptance plan

Status: **design only; every implementation slice below is pending**.
2026-09-24, against `distributed` at
`74bddd4f3d3a6621d118dbc2a99b6f35a52c6e03`.

Read `CLAUDE.md`, [DESIGN](../DESIGN.md), [CONTEXT](../CONTEXT.md),
[STATUS](../STATUS.md), [ADR-0021](../adr/0021-distributed-correctness.md), the
[protocol specification](../design/distributed-correctness.md), and the
[interface contract](../design/distributed-correctness-contracts.md).

Goal: implement the five reviewed fix packages without treating compilation,
route presence, or a happy-path demo as a correctness proof. Preserve two
binaries, bounded state and streamed bodies. No per-object state or extra
dependency is authorized by this plan. Every slice closes its tests, API, CLI,
GUI, metrics and documentation together before being marked shipped.

## 1. Implementation order

| Slice | Scope | Depends on | Ship gate |
|---|---|---|---|
| H0 | Shared identities, operation transaction contract, protocol capability gate and negative regressions | Existing distributed baseline | Old capability cannot advance a new barrier; operation/identity contract tests and staging UI pass. |
| H1 | D1 coherent bundles, serialized install, secret generations and restart cache | H0 | T01–T04, relevant T17–T20; real signature and cache fault tests, rotation API/CLI/GUI, benchmark comparison. |
| H2 | D2 local admission, incarnations, server leases and durable barriers | H0, H1 | T05–T09, T14, relevant T17–T20; property negative control, file and etcd paths, maintenance API/CLI/GUI. |
| H3 | D4 learner join, ID removal and truthful health | H0 | T12, T13, relevant T17–T20; real etcd one-to-three-node and interrupted-join workflows. |
| H4 | D3 cold restore, epoch enrollment and recovery workflow | H1, H2, H3 | T10, T11, relevant T17–T20; real restore rehearsal with external backend state and all recovery surfaces. |
| H5 | D5 final UI/event consistency and release rehearsal | H1–H4 | T15–T20 and all end-to-end gates; no parity gaps, protocol handover/rollback runbook exercised. |

H3 can be developed independently after H0, but must not bypass the durable
operation semantics. Implement each slice's UI and CLI while that slice lands;
H5 is the combined release gate, not permission to defer all frontend work.
Small frontend bug fixes (dirty drafts, honest pending toasts) may land earlier
with accurate current-schema tests, then move to the typed contract as it lands.

### H0 — protocol foundation

1. Add epoch/identity/generation fields and strict validation to the directory,
   cache and wire models. Document the controlled upgrade and old-schema refusal.
2. Extend Operations/Directory/Fleet concrete implementations with atomic
   reservation and transition methods. Write a transaction contract shared by
   the file and etcd tests. Do not make several independent writes look atomic.
3. Add idempotency and explicit nonterminal outcomes, sequence, effect state and
   allowed actions; protect unfinished records from history pruning. Add the
   shared typed API/CLI/GUI operation model and status views.
4. Add capability gating before exposing any new transition path. Audit all
   aliases and direct directory mutation commands. Register catalog entries
   before metric code. The new tests must demonstrate why version-only fencing
   is insufficient before the new protocol passes them.

Code entry points: `internal/directory`, `internal/control/operation.go`,
`internal/control/ops.go`, `internal/control/fleet.go`, `internal/cp/ops.go`,
`internal/cp/store.go`, `cmd/shunt/api.go`, `web/src/api/client.ts`.

### H1 — coherent installation

1. Add the concrete bundle; make auth, route selection and upstream signing
   retain one bundle through a request. Cover cross-cluster copy and compensation.
2. Replace candidate writes into live secrets/keys/registry with isolated
   preparation. Serialize all install sources; compare again at publication.
3. Split signer generation from pool configuration, account resource lifetimes,
   and bound retained bundles. Make control-store preparation transactional too.
4. Implement restart-safe atomic cache persistence and distinct installed/durable
   state. Add credential rotation and install diagnostics across all interfaces.
5. Benchmark request admission/acquisition, large snapshot reload and repeated
   rotations with long-running requests. Demonstrate transport reuse rather than
   just asserting pointer equality in a unit test.

Code entry points: `internal/member/member.go`, `internal/auth`,
`internal/upstream/registry.go`, `internal/proxy/resign.go`,
`internal/proxy/copy.go`, `internal/cp/store.go`, `cmd/shunt/serve.go`.

### H2 — drain and maintenance

1. Implement gate/counter accounting for every S3 mutation and source-dependent
   read, with a completeness test against the operation classifier. Interlock
   admission and gate closure. No global lock held across upstream work.
2. Separate definitive completion from uncertain effects. Track incarnation
   lifecycle and cache durability; clean shutdown/retirement is a protocol.
3. Honor heartbeat TTL/sequence/identity from send time. Prioritize safety
   metadata over telemetry and enforce complete paged acknowledgements.
4. Implement the durable barrier state machine, owner reconciliation and safe
   precommit cancellation. Include local/lab proxies and external mover sessions.
5. Put readonly, rotation drain, cutover and purge through their appropriate
   gates. Add API/CLI/GUI blockers and retirement flows. Unknown backend effects
   remain blocked; do not insert a timeout that quietly clears them.
6. Update fleet and maintenance runbooks, especially the old promise that a
   dead member can simply be forgotten. Preserve the ACTIVE control-outage test
   separately from migration-progress tests.

Code entry points: `internal/proxy/resign.go`, `internal/proxy`,
`internal/member/member.go`, `internal/control/ops.go`,
`internal/control/readonly.go`, `internal/control/mover.go`,
`internal/cp/fleet.go`, `cmd/shunt/moverun.go`, `cmd/shunt/proxycmd.go`,
`web/src/components/FenceStatus.tsx` and the Buckets/Clusters/Migrations screens.

### H3 — membership and health

1. Add durable membership intents and explicit advertised API URLs. Validate
   local bootstrap paths before contacting the existing cluster.
2. Use `MemberAddAsLearner`; reconcile by normalized peer URL/member ID after
   every ambiguous outcome. Persist bootstrap safely, resume startup, promote
   only through etcd's readiness checks. Keep secrets out of operations/events.
3. Add member-ID removal and confirmation, including unnamed learners. Verify
   quorum rules and protect against name reuse and competing workers.
4. Implement bounded background health sampling and the separate quorum probe;
   keep partial status usable during loss of quorum. Mark expired samples unknown.
5. Wire join progress/resume/cancel/remove/status into CLI and Control Plane UI.

Code entry points: `internal/cp/etcd.go`, `internal/cp/api.go`,
`cmd/shunt-control/serve.go`, `cmd/shunt-control/verbs.go`,
`web/src/screens/ControlPlane.tsx`, `web/src/store.tsx`.

### H4 — recovery

1. Export and verify a snapshot/identity manifest. Share strict parsing between
   CLI inspect/restore and recovery API; fuzz it. Reject partial exports and
   nonempty restore destinations.
2. Create the new application epoch in recovery mode before the normal API can
   serve; invalidate old operations, confirmations and enrollment. All restored
   nodes must agree on the same recovery ID and epoch.
3. Implement revisioned recovery plans, evidence provenance, placement conflicts,
   reconciliation operations and activation CAS. No object facts in etcd.
4. Enroll replacement proxies with data admission closed; activation and ingress
   reopening are explicit phases. Reject late old-epoch fetches and heartbeats.
5. Add the API/CLI/GUI recovery workflow and a backend-specific quiescence
   runbook. Rehearse it against writes and routing changes made after the backup.
   Where history is insufficient, prove that the workflow blocks or explicitly
   reports a data-loss decision rather than silently guessing.

Code entry points: `internal/cp/etcd.go`, `internal/cp/store.go`,
`internal/cp/api.go`, `internal/control/fleet.go`, `internal/member/member.go`,
`cmd/shunt-control/verbs.go`, `docs/how-to/run-shunt-control.md`,
`docs/runbooks/quorum-loss.md`, Control Plane UI.

### H5 — operator workflow and release

1. Replace untyped mutation responses with typed operation envelopes everywhere.
   Remove success paths based only on a stored flag or successful HTTP transport.
2. Separate drafts from refreshed state; preserve dirty values on SSE and 409,
   discard late responses for old selections, and make generation conflicts
   actionable. Test telemetry bursts and reconnect gaps without timer sleeps.
3. Add scoped event sequencing/reset handling and bounded refresh coalescing.
   UI and CLI status must show observation age, partial data and backend uncertainty.
4. Regenerate live route/reference inventory and verify every parity-matrix row.
   Execute the manual visual checklist and API/CLI workflows on the same fleet.
5. Rehearse controlled protocol handover, owner crashes and cold restore before
   enabling production mutations. Update ADR/status only with actual results.

## 2. Required regression matrix

Use deterministic channels/latches, fake clocks and injected failures. Do not
rely on sleeps to create a race. Test real observable behavior (backend content,
signature acceptance, durable operations and UI output), not only internal
method call counts. Test-only object maps are an oracle, not product state.

| ID | Scenario and fault injection | Required result / level |
|---|---|---|
| T01 | Pause preparation of v2, install v3 via concurrent heartbeat refresh, then release v2; duplicate identity with conflicting payload | Published/acknowledged/cache identity never regresses; conflict rejected. Member/control installer tests under `-race`. |
| T02 | Fail candidate preparation after resolving new secrets; fail control CAS after candidate build | Old credentials, client keys, routes and pools still serve; rejected secrets never leak into the resolver or cache. Unit + authenticated proxy request. |
| T03 | Rotate only a secret with unchanged `control:name`; keep old request open | New requests sign with new secret; retained old request stays coherent; compatible connections reused; rotation outcome accurate in API/CLI/UI. |
| T04 | Fail write/fsync/rename/parent fsync; restart at each cache/lifecycle boundary; flood rotations with a held request | Corrupt/old cache cannot acknowledge a newer barrier; install vs durable displayed; retained bundle bound/backpressure and no pool/goroutine leak. |
| T05 | Fake S3 accepts old-route PUT body but delays commit. Request a ramp/migrate step, copy old source to target, then release PUT | Step cannot advance/copy under new authority before drain; final read returns last acknowledged value. Negative control using version-only fence MUST fail the oracle. Single proxy and fleet. |
| T06 | Drain race at check/increment; delayed DELETE/dual delete/compensation/CopyObject/UploadPartCopy/CompleteMultipartUpload; delayed source GET; proxy/worker crash after dispatch | No gate bypass or early ack; uncertain backend effects block; purge never removes an in-use source; a new incarnation cannot erase old work. Handler + model + e2e. |
| T07 | Client max TTL 60 s, server grant 1 s; delayed/out-of-order replies, backwards wall clock, zero TTL, stale endpoint version, new epoch | Freshness expires from monotonic send time using server grant; cannot renew on wrong identity/sequence or before install. Fake-clock tests; CLI/UI grant fields. |
| T08 | Readonly barrier with missing proxy; wait deadline; server owner exits; client disconnects/retries; concurrent cluster/placement action | Durable pending/blocked outcome, no false success or silent release; same idempotent operation; all interfaces agree on desired/effective/quiescent. |
| T09 | Two control nodes compete; crash before/after hold and commit; lose CAS reply; attempt cancel on newer hold; worker side effect reply lost; history cap exhausted | At most one authoritative intent per scope; reconciliation by ID/generation; safe precommit cancel only; no duplicate destructive dispatch or eviction of live evidence. Real etcd + file contract. |
| T10 | Backup v1, advance live metadata/cache to v3, restore backup; send old fetch/heartbeat/confirmation; boot two restored nodes | New shared epoch, recovery mode before normal service, mismatch never 304/fresh; late response cannot publish; old confirmation rejected. Real etcd restore. |
| T11 | Old proxy remains ACTIVE across restore; member added after backup; backend accepted write still executing; missing post-backup delete history | Recovery cannot activate on old membership list/lease timeout alone; external quiescence and reconciliation required; ambiguous history explicitly unresolved/data-loss decision. API/CLI/GUI and backend rehearsal. |
| T12 | Start one voter; add learner; fail local disk/startup, lose add response, retry intent, crash before promotion, remove unnamed learner; reuse a member name | Original voter stays writable before promotion; no duplicate member; bootstrap resumes; secrets redacted; remove exact ID only; one-to-three-node flow succeeds. Real etcd and all operator surfaces. |
| T13 | Stop a named member, partition quorum, delay/drop probe responses, never-started learner, stale cache, many browser viewers | Historical membership never reports healthy; quorum has bounded fresh evidence; partial status available; probe work independent of viewers. Fake clock + real cluster/UI. |
| T14 | Seeded fleet model: repeated keys, acknowledged sequential writes, DELETE, multipart, copy, ramp steps, cutover/purge; partitions and crashes at every phase | No acknowledged update lost through early routing advancement; no deleted source used by retained work; replay seed on failure; version-only negative control catches violation. Preserve ADR-0004 limits explicitly. |
| T15 | API returns pending/blocked readonly, later success/failure/cancellation, then reconnect with missed event | No early success toast; desired/effective accurate; operation discoverable and same status in CLI JSON/UI. Component + API/CLI tests. |
| T16 | Edit ratio/prefix, emit telemetry, refresh directory to a newer generation, switch buckets with fetches in flight, replay events out of order | Draft survives; conflict shown; submit carries expected generation; stale response cannot overwrite selection; bounded fetch count. React tests. |
| T17 | Drive every row of the operator parity matrix, including errors, retries and sensitive bootstrap paths | API, CLI and GUI expose the same effect/blockers; route inventory complete; read models/operations/SSE/logs contain no secrets. Negative auth and confirmation tests included. |
| T18 | Legacy controller reachable, old proxy registers, old cache restarts, unsupported schema/protocol, attempt downgrade after new state | Unsafe operation/startup refused; capability/upgrade reason visible; no alias bypass. Controlled upgrade rehearsal. |
| T19 | 100k placements, 1k proxies, 100 barriers, dense telemetry near 1 MiB, health across 30 members, slow clients/reload storm | Memory/queues/goroutines and request sizes bounded; safety evidence never truncated into success; latency/CPU/throughput budget measured. Bench + soak. |
| T20 | Parsers receive malformed IDs, overlong counters, unknown enums, truncated cache/manifest, invalid URLs and repeated paging tokens | Strict bounded validation, no panic, no partial state publication, no secret echo; fuzz and `check-config` tests. |

The initial review had eight failing diagnostic tests confirming snapshot races,
lease/restore behavior, secret handling, inflight writes and UI regressions.
Reconstruct those scenarios as maintained tests above; do not depend on an
untracked review patch for the release gate. Interrupted etcd joins and health
also require real-cluster tests, not only mocked RPC success.

## 3. Performance and resilience gates

- Capture the baseline before changes. Follow DESIGN §8's object sizes,
  concurrency and mixed-workload method; disclose Go/backend/host configuration.
  Compare throughput, p99, CPU/GiB, allocations and RSS with at least three runs.
  A regression above 5% blocks shipment until resolved or the design/budget is
  explicitly revised with evidence. No silent budget increase.
- Benchmark local admission, bundle publication/refcounting, signer rotation,
  snapshot decode/prepare/cache, heartbeat parsing and barrier evaluation. Every
  new package has a meaningful benchmark, not an empty benchmark for policy.
- Measure the 100k-placement full snapshot and 1k-proxy simulation. Report
  snapshot bytes, load/serialize duration and peak RSS; steady-state memory must
  plateau after churn. Record bytes per proxy/barrier and safety/telemetry split.
- Run a 256-worker ACTIVE workload during loss of control quorum. Existing
  registered proxies continue permitted ACTIVE requests without control-induced
  errors. Separately prove that routing changes and non-ACTIVE stale mutations
  are refused. Do not count refused migrations as data-plane errors or hide them.
- Under a held/uncertain bucket, benchmark an unrelated ACTIVE bucket. There
  must be no global serialization or repeated full-directory scans on requests.
- Under a healthy 1-second heartbeat topology with no old work, measure barrier
  p99 against the proposed 5-second target. Report hold duration with large
  in-flight uploads separately; never cancel backend work and call it drained.
- Run partition, owner-crash and reconnect scenarios for at least 10 minutes
  under the model workload, retain seeds/traces and prove the unsafe negative
  control fails. No goroutine, transport or retained-bundle growth after recovery.

## 4. Validation commands and evidence

Use the repo's pinned toolchain and normal Makefile targets. Preserve full
build/test output in logs; never pipe runs through filters. Run focused tests
while developing; broaden for the slice's concrete risks and final release gate.

At the combined release gate run the existing lint, vet, race, fuzz, build and
UI gates; the new regression suite; file/etcd parity; and the real fleet,
walkthrough, backend s3diff and recovery scenarios. `make build` must still prove
the proxy binary does not link etcd. `make ui` must embed the working UI, not only
the committed Node-free placeholder. Update generated route inventory only from
the implemented route tables. Preserve the existing manual visual acceptance
policy; record the completed checklist for pending, blocked, stale, recovering
and success states, including keyboard use and error focus.

Real embedded-etcd tests need a runner that permits local Unix-domain sockets
and the multi-process/TCP fixtures. The review environment rejected Unix socket
creation, so it could not verify those scenarios. A constrained environment is
not a waiver: arrange a capable CI/dev runner and record the real results.
Do not turn required tests into `t.Skip` or substitute a mock for the release gate.

For each slice record commit, commands, exit codes, backend builds, results and
benchmark comparison in `docs/bench/` or its acceptance report; link from STATUS.
Keep design targets distinct from measurements and proposed API distinct from
implemented reference. Do not mark a known pending gate green.

## 5. Definition of done

- [x] H0 protocol/operation foundation complete; controlled upgrade documented. *Passed
      2026-09-24 (ADR-0021; docs/bench/h0.md): `make fleet` and `make walkthrough` green on the H0
      code, and a manual acceptance pass on `make demo-ui` over 0.0.0.0, the Operations screen
      included, that took a bucket from two MinIO legs through a third backend and consolidation to
      removing both MinIOs. There is no controlled upgrade: no fleet is deployed, and a newer
      schema is refused. Deferred by design: resume and cancel actions and owner terms (H2),
      required If-Generation (H5), string-typed identity versions (read models, H5).*
- [ ] H1 atomic snapshot/credential/cache paths pass T01–T04 and parity gates.
- [ ] H2 drain/lease/maintenance paths pass T05–T09 and worker/fleet gates.
- [ ] H3 resumable membership/observed health pass T12–T13 on real etcd.
- [ ] H4 restore/reconciliation passes T10–T11 with data newer than backup.
- [ ] H5 truthful operations/drafts/events pass T15–T18 and visual acceptance.
- [ ] T14 model/property negative control and crash/partition runs retained.
- [ ] T19 performance budgets and bounded-state soak pass; numbers published.
- [ ] T20 parser fuzz, schema validation and secret-leak tests pass.
- [ ] Every parity row has API, CLI, GUI and behavior tests; no placeholder action.
- [ ] Catalog, live reference, help, route inventory, ADRs and runbooks updated.
- [ ] Known backend/TLS limitations still disclosed; no unsupported recovery claim.
- [ ] Release/handover rehearsal completed; STATUS reflects evidence, not plans.

Do not tag the hardening phase complete until every required box is satisfied.
