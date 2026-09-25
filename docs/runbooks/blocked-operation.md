# Runbook: an operation is blocked

**Signal.** A step exits 3 with `operation <id> is still blocked (phase …); waiting on …`;
`shunt operation list` shows `blocked`; `shunt_operations{status="blocked"} > 0` or
`shunt_barrier_blockers{code} > 0`; a bucket's writes answer 503 longer than a step takes
(`shunt_migration_refused_writes_total{reason="barrier"}` or `{reason="source_closed"}` rising).

**What it means.** The operation waits on the blockers its record names. Its hold stays: the
bucket's writes pause (for purge-source, its deletes). It does not succeed, fail, roll back or
release anything because a wait ran out, and every control node shows the same record. It carries
on by itself once its blockers clear. Buckets outside its scope are not affected.

**Do.**
1. `shunt operation show <id>`: its `phase`, one `blocker` line per blocker, and `actions`, the
   actions it allows now.
2. Clear each blocker:

| Blocker | What it means | What clears it |
|---|---|---|
| `proxy_missing` | a member is silent, has no running process, or is on another directory lineage | bring it back ([lagging-proxy.md](lagging-proxy.md)); or retire it, or resolve and forget it ([crashed-proxy.md](crashed-proxy.md)); re-enroll a member on another lineage with an empty `control.cache_dir` |
| `install_pending` | a live member has not installed the version or the hold yet | a heartbeat, normally; else [lagging-proxy.md](lagging-proxy.md) |
| `install_backpressure` | the member installed the version, but long requests still hold older runtime bundles | those requests ending |
| `cache_not_durable` | the member's restart cache is behind the hold | the member's `control.cache_dir` (disk full, permissions: `shunt proxy show`) |
| `old_requests` | requests admitted before the hold are still out; for cutover, a read fell back after its quiet window | the requests ending; for cutover, see below |
| `backend_outcome_unknown` | a member never learned how a mutation of this bucket ended | retire and resolve it ([crashed-proxy.md](crashed-proxy.md)) |
| `incarnation_unresolved` | an earlier process of a member ended without retiring | resolve it ([crashed-proxy.md](crashed-proxy.md)) |
| `multipart_open` | cutover: the source still has multipart uploads | cancel the cutover so they can complete, or abort them on the source; then repeat |
| `source_diff` | purge-source: after the drain, the source holds keys the primary lacks, or the diff failed | see below |
| `worker_unresolved` | an external mover's session expired with its work unresolved | see below |
| `owner_lost` | the control node running the operation stopped | see below |
| `quorum_unavailable` | the control plane cannot be read | [quorum-loss.md](quorum-loss.md); the operation carries on after |

3. To undo the step instead, and only while `actions` lists `cancel`: `shunt operation cancel
   <id>` answers `operation <id> canceled; nothing changed`. The record ends `cancelled` first,
   then the operation's own hold is released, compared on its barrier id, so a newer hold is never
   touched, and the routing is as it was before. If it answers that releasing the hold failed,
   repeat the cancel.

**When cancel is offered.** While the operation waits in its precondition (nothing written yet),
and from the moment its hold is durable until it starts to commit. Not while the hold is being
written (retry), not while a live owner is in phase `commit`, not after the commit, not once
purge-source has started deleting, and never for a mover. A refused cancel answers
`not_cancellable`. A cancel that reaches an operation whose commit won the race answers 409 and
corrects the record to `failed` with effect `committed`: the change is in force, and a further
change is a new operation.

**Owner lost.** A record `blocked` on `owner_lost` with `actions: resume` belongs to a control node
that stopped. A restarted `shunt-control` resumes its own operations. From any live node:
`shunt operation resume <id> --api http://c2:9901` continues from the recorded phase
(`operation <id> resumed on c2 (running, phase drain)`); it never repeats a step. Refused while the
owner is live. An operation lost before it wrote its hold, or one without a hold (such as
`migrate finish`), ends `failed` instead: with effect `none` if it was still waiting in its
precondition, quiet window or diff, and nothing to check; with effect `uncertain` if it may have
written, so check `shunt status`. Then repeat it.

**Cutover that does not finish.** Cutover watched its quiet window before the hold; under the hold
it blocks on `multipart_open` (the source has uploads in progress), `old_requests` (a read fell back
after the window) or `proxy_missing` (a proxy that watched the window has not reported since, or
restarted), with the bucket's writes paused. Cancel it, deal with the cause (let the uploads finish
or abort them; run the mover until a pass copies nothing; bring the proxy back), and cut over again.

**Purge-source on `source_diff`.** Nothing is deleted yet, and the bucket's DELETEs answer 503
until the purge ends. Cancel it: the source reopens and DELETEs flow again. The blocker names the
first 20 keys the primary lacks. The mover cannot fix them: it runs only on a MIGRATING placement,
or RAMPING at ratio 1, and a placement in CUTOVER does not go back. Find out why those keys are
missing: a delete whose source leg failed
(`shunt_migration_dual_delete_total{outcome="source_failed"}`) leaves its key on the source alone.
Delete such keys from the source, or copy a key that belongs on the primary, then repeat
`shunt purge-source <bucket>` (a new dry run and token). Or keep the source with
`shunt migrate finish <bucket>`. A diff that could not be taken (a backend did not answer) is taken
again on the next poll; the blocker's message says why it failed.

**Dead external mover.** `worker_unresolved` means a `shunt migrate run` process stopped without
ending its session, and its last copy may still land. `shunt operation show <id>` prints
`worker: <session> unresolved, sequence N, X in flight, Y uncertain` and `actions: resolve-worker`.
Establish that the process has ended and its requests with it (the process is gone and its host
holds no connection to either cluster), then:

```sh
shunt operation resolve-worker <id> --session <session> --attest "mover host rebooted 09:14; no connections to either cluster"
```

It answers `worker session <session> of operation <id> resolved; the operation ends and frees its
bucket`. The operation ends `failed`, its copy unfinished, with the attestation on its record.
Refused while the worker still heartbeats. Then run `shunt migrate run <bucket> --until-converged`
again.

**Do not** cancel a step to get past a missing proxy unless you want it undone: the next step
blocks on the same proxy. **Do not** attest what you have not established.
