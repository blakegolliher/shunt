# CLAUDE.md — rules for working in this repo

Read docs/DESIGN.md before every task. docs/CONTEXT.md is ground truth about the environment; do not re-derive what it answers. docs/STATUS.md says which phase we are in.

## Simplicity rules (docs/DESIGN.md §2.10, verbatim)

- Two binaries. `shunt` (the proxy) with subcommands `serve`, `cluster`, `tenant`, `adopt`, `expand`, `ramp`, `migrate`, `cutover`, `purge-source`, `readonly`, `status`, `verify`, `client`, `step-out`, `proxy`, `operation`, `tier run`, `restore worker`, `directory`, `probe`, `check-config`, `doctor`, `version` (the operator verbs call the control API, ADR-0008); `shunt-control` (the control plane, Phase 3c).
- Standard library first. Every dependency has one line in `docs/deps.md` saying why the stdlib wasn't enough.
- No interface with a single implementation, except two named seams: `CredentialStore` and `Directory`.
- No middleware framework, no DI container, no plugin system. One handler, one pipeline, explicit calls.
- Any buffering of a request or response body requires an ADR and a benchmark. (`io.CopyBuffer` through a pooled fixed-size buffer is streaming, not buffering.)
- Every package has tests and at least one benchmark; `-race` in CI; fuzz targets for every parser (op classifier, SigV4 canonicalization, aws-chunked decoder, XML rewriters, lifecycle XML, continuation tokens).
- Feature flags default off, live in one file, and each has a removal criterion.
- Kill switches are not feature flags: they turn off behavior that is normally on, so they default to the normal behavior (`false`) and each documents what breaks when it is flipped. They live in the same file and are removed when the behavior they guard no longer needs a switch. A flag whose safe value is "on" is a kill switch stated backwards; name it so the default is `false`.
- Config is validated at startup and by `check-config`; unknown keys are errors.
- ADRs in `docs/adr/` for every decision in Section 2 and any that overrides it.
- Nothing in the code or docs assumes a NIC, a CPU model, or a kernel feature beyond what `doctor` checks and a fallback covers.
- A phase is done when its acceptance gate (Section 8) passes, not when the code compiles.

## Additional rules

- **No per-object state, ever.** Not in memory, not in the directory, not in Postgres, not on disk. Anything shunt needs is in the request, in a bucket-level placement, or in the backend. A proposal that adds per-object state needs an ADR before code.
- **docs/POC.md lists what not to build.** Anything it marks as deferred is out of scope for the POC sessions even if it looks easy. Its "Deferred items" table says which full-design phase picks each one up.
- Every metric must be in docs/telemetry-catalog.md before it is implemented.
- Every new dependency gets a line in docs/deps.md.
- Every body buffer needs an ADR.
- Nothing assumes hardware.
- **Never pipe a build, test, or property run through `head`, `tail`, `grep`, or any other filter.** Redirect the full output to a file (`> run.log 2>&1`), then read the file. Filtering has destroyed evidence three times: twice a stale binary survived a build killed by `tail`, and once a property run's 15 violations were reduced to a summary line.
- Commit messages carry no tool attribution: no Co-Authored-By, no Generated-with, no Claude-Session trailer. Check `git log -1 --format=%B` after every commit.

## Lift procedure (copying third-party code in; from docs/reuse.md)

1. Copy the file(s) into the target package. Keep the original license header verbatim.
2. Directly under the header add: `// Modified by <name> for github.com/blakegolliher/shunt, <date>: <one line on what changed>.` Apache-2.0 §4(b) requires the modification notice; MIT doesn't, but do it anyway.
3. Append the project's LICENSE and NOTICE text to `THIRD_PARTY_NOTICES` (Apache-2.0 requires NOTICE contents carried forward when the source has one — versitygw does).
4. Add a line to `docs/deps.md`: source repo, path, commit SHA, license, why the stdlib wasn't enough.
5. Never copy a file whose only license signal is the repo LICENSE while its content matches a known AGPL or unattributed lineage.
6. G4 checks all of the above every phase.

Never copy code from MinIO, Garage, or warp (AGPL-3.0). Running them as tools is fine.
