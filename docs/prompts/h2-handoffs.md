# H2 handoffs, summarized (record)

H2 of `docs/prompts/distributed-hardening.md` (ADR-0021 D2) was carried between sessions by two
work orders, kept untracked while they were live and summarized here once H2 passed (2026-09-25,
merged to master in PR #4). The outcomes are in ADR-0021's dated decision sections, STATUS and
`docs/bench/h2.md`; this file records what each handoff asked for and how it ended.

## 1. The review handoff (2026-09-24, after H2a–H2e)

A review of the five H2 commits (`4900907` H2a through `e306ceb` H2e) found the gate and token
accounting sound, and seven defects at the crash and cancel edges that T08 and T09 cover. Four of
them left an operation blocked forever with its scope reserved and no operator exit.

| # | Defect | Fix (commit `9761192`, regression tests named in ADR-0021) |
|---|---|---|
| 1 | A resumed barrier drained forever after a commit whose record write was lost | drain only while the scope still carries the barrier; otherwise the commit reconciles `already` |
| 2 | Cancel could release a hold while purge was deleting | end the record first, re-checking `DispatchStarted` and `Committed` in its CAS; then release by barrier id |
| 3 | Read-only off was not compared on the barrier id | release requires the scope to carry exactly that barrier |
| 4 | Purge marked dispatch before its re-checks, and DELETEs during the drain broke the re-diff | `dispatch_started` just before the first delete; the DELETE rule: 503 + `Retry-After` while the source is closed |
| 5 | A clean retirement reported from the proxy's marker was recorded unclean | apply `Previous` before replacing `Current` |
| 6 | An operation blocked in its precondition could not be cancelled | cancel allowed with no hold to release |
| 7 | A dead external mover had no operator exit | `resolve-worker` with an attestation, on API, CLI and UI |

It also listed should-fix items, all done in the same pass: ack generation `>=` with the id match;
skip `extra` while fleet blockers exist; no directory clone in `statsLocked`; barrier derivation
once per snapshot; set `shunt_fleet_unresolved_incarnations`; sweep and resume see unfinished
records past the history limit; stop heartbeating once retired; one exported `BlockerText`.
Left by design: uncertain outcomes count for the life of a process (the runbooks state the cost).

## 2. The H2f handoff (2026-09-24, before acceptance)

With the review fixes in, H2f was release closure: correct the operator docs (no timed-out hold
is released, a dead member cannot simply be forgotten), add runbooks for a crashed proxy and a
blocked operation, rewrite `test/e2e/fleet.sh` to prove the H2 protocol, publish
`docs/bench/h2.md` (T19-scale measurements, healthy-barrier p99 against 5 s, an unrelated bucket
under a held one, a ten-minute partition and owner-crash soak), run every gate, and close the
records only on evidence. It restated the safety rules that must not regress: every registered
member is in every barrier; no timeout turns a missing member or an unknown backend outcome into
success; a wait is an observation budget; precommit cancel releases exactly its own hold; owner
loss is resumable; purge never removes a source with work in flight.

How it ended (2026-09-25, `25f5ee8`, `aa2ce3c`, `d84971c`):

- All of the above done: `make fleet` with an H2 section and a barrier-latency section,
  `make walkthrough`, `make soak` (47 cycles, 1.28 M operations, 0 violations), property, fuzz,
  race, lint, the UI gates and a manual UI pass on `make demo-ui`.
- The gates found faults the slices' tests had not, each fixed with a regression test: a client
  disconnect made a mutation's outcome uncertain; cutover refused writes for its whole quiet
  window; a step orphaned before its first write was recorded uncertain and kept offering cancel;
  forget lost a killed process's resolution; `operation wait` printed stale blockers; the
  heartbeat size check left out the fallback counts; hot paths deep-copied the directory. `--wait`
  became the CLI's deadline.
- Found alongside: DeleteObjects on a spread bucket (ADR-0018 amended), and MinIO's public images
  withdrawn, so the e2e MinIO is built from source.
- Carried: two costs at 1,000 proxies (T19), and SIGINT's exit 130 and folding Audit into
  Operations (H5).
