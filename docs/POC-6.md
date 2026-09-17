# POC-6: migration correctness hardening (branch `distributed`)

This work lives on the branch `distributed` until it is finished: plans, designs and the ADRs they
produce. `master` carries only what is done and green.

The theme the branch is named for: **a migration is correct only if every proxy and every cluster
agrees on what the client was promised.** Items 1 and 2 are about two clusters holding one bucket,
item 3 about several proxies holding one directory, items 4 and 5 about the state and the facts a
cutover relies on.

## Where each item stands

| # | Item | State |
|---|---|---|
| 1 | Conditional PUT across the ramp | **done** on this branch: ADR-0013, `internal/proxy/conditional.go`, property-test clients |
| 2 | Cross-cluster CopyObject | **done** on this branch: ADR-0014, `internal/proxy/crosscopy.go`, live on Garage → MinIO |
| 3 | Version fence across proxies | not started; cutover evidence is per proxy (STATUS carried gap) |
| 4 | Cutover refuses in-progress multipart uploads | not started; `cutover` checks state, convergence, fallback reads only |
| 5 | Capability profiles measured vs assumed | partial: `expand` measures and reports `measured`, but provenance is not stored, `cluster add` does not probe, and nothing refuses an assumed profile |
| — | Secrets never on argv | **already true**: prompt or stdin, `--secret-ref env:/file:`, `--keys <file>` |
| — | `shunt-data` at an explicit path, absolute path logged | partial: `--state-dir` defaults to a relative `shunt-data`; the path is absolutized and logged |
| — | `purge-source` dry run | not started; only the mover has `--dry-run` |
| — | README and walkthrough | updated per item as each lands |

**ADR numbering:** 0010–0012 are taken (operator ergonomics, stepping out, importing client keys).
POC-6 starts at **0013** (item 1, written), so items 2–5 take 0014 and up.

## Item 1 — conditional writes (done, ADR-0013)

A write's `If-None-Match`/`If-Match` is judged against **both** clusters while a bucket is `RAMPING`
or `MIGRATING`, and create-once is judged by shunt itself on any cluster whose profile says it
ignores `If-None-Match: *`. Refusals are 412; a check that cannot be made is 503.

Found along the way, and worth carrying into item 5: shunt trusts the capability profile completely.
The property test's guarded variant had a target that ignored the header while its profile claimed
support, and 5,682 client-visible violations followed in two minutes. A wrong profile is a
correctness bug, not a tuning mistake.

## Item 2 — cross-cluster CopyObject (done, ADR-0014)

shunt makes the copy itself whenever no backend can see both sides: another cluster, or a source
bucket whose objects are split by a migration. It streams, keeps a multipart source's part layout,
evaluates the copy-source conditions on the cluster that holds the source, and applies ADR-0013 to
conditions on the destination. `UploadPartCopy` crosses clusters the same way.

The open questions, as answered:
- **Size ceiling:** a single object over 5 GiB is refused with `EntityTooLarge`, because one PUT
  cannot carry it; a multipart source of any size is copied part by part, so the ceiling only bites
  an object that is genuinely one 5 GiB+ part.
- **shunt-driven multipart above a threshold:** not needed. A multipart source already goes part by
  part, and clients use `UploadPartCopy` for large copies, which now works.
- **Halfway failure:** a streamed copy is one PUT and leaves nothing; a part-by-part copy aborts its
  own upload on the destination.
- **Tags are not carried** across a cross-cluster copy (`x-amz-tagging-directive`), written down in
  ADR-0014 rather than left to be discovered.

## Item 3 — version fence across proxies

**Today:** each proxy polls the directory file (1 s) and serves whatever version it has read. A ramp
change or a cutover applies the moment it is written, so for up to a poll interval two proxies can
route the same key differently, and `cutover` reads the fallback-read counter of the one proxy that
served the API call. With several proxies that evidence is not evidence.

**Design sketch.** Proxies register in the directory (`proxies:` with an id, a start time, and the
version each serves, refreshed on every poll and expired after a few missed intervals). A ramp step
or a cutover is applied only when every live proxy acknowledges the version that precedes it, and
the CLI waits, naming the proxies it is waiting for. The cutover window aggregates the fallback-read
counters of all live proxies rather than one.

**Open questions:** registration is per-proxy state in the directory — allowed (it is not per-object)
but it makes the file a write target for every proxy, which the file backend serializes through one
flock; what a dead proxy costs (expiry window vs a cutover that waits forever); whether the fleet
counter belongs in the directory or is scraped from each proxy's metrics.

**Test:** two proxies over one directory and a deliberately stale one: a ramp step and a cutover both
refuse to apply until the stale proxy catches up or expires.

## Item 4 — cutover and in-progress multipart uploads

**Today:** an upload started on the source before the ramp keeps its source-issued upload id, so its
`CompleteMultipartUpload` lands on the source **after** cutover, where nothing reads any more, and
`purge-source` then deletes it. `purge-source` aborts uploads while emptying the bucket, and
`step-out` reports them, but `cutover` does not look.

**Design sketch.** `cutover` lists `ListMultipartUploads` on the source and refuses while any exist,
naming them; `--abort-uploads` aborts them first. The refusal is the same shape as the other cutover
refusals.

**Test:** an upload started before the ramp and completed after the mover's last pass: cutover
refuses, `--abort-uploads` clears it, and the client's completion then fails with `NoSuchUpload`
rather than writing an object nobody will read.

## Item 5 — measured vs assumed capabilities

**Today:** `capabilities` are `*bool` on the cluster record: set or unset, with no record of where a
value came from. `expand` measures conditional write and delete and reports `measured` in its result,
but the provenance is not stored. `cluster add` does not probe. `migrate start` refuses only
`conditional_write: false`, never an assumed profile. The type read from the `Server` header is not
purely cosmetic: `internal/proxy/buckets.go:190` uses `type == "aws"` for the CreateBucket location
constraint.

**Design sketch.** Each capability carries `measured` or `assumed` with the timestamp of the probe.
`cluster add` runs the probe and stores measured values; `migrate start` and the mover refuse an
assumed profile, with the command to fix it. The `Server` header sets a display type only, and
anything behavioural keys off a capability instead.

**Open question:** re-probing policy after a backend upgrade (backend-compat.md says re-probe; the
profile should probably carry the probe's date and shunt should say when it is old).

## The trailing items

- **`shunt-data` explicit:** require `--state-dir` rather than defaulting to a relative path, and log
  the absolute path at startup (it is already absolutized internally).
- **`purge-source` dry run:** print what would be deleted (counts and the first keys) and require a
  second call, or `--yes`, to delete. The listing diff it already computes is most of the work.
- **README and walkthrough** are updated with each item that changes a command.
