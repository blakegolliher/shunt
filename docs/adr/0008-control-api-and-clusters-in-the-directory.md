# ADR-0008: Clusters are directory state, changed live through a control API

Status: accepted (POC-5, 2026-09-16). Source: docs/DESIGN.md §1.5 (clusters, tenants and placements are control-plane state), §4 (`internal/control`), §2.10; ADR-0005; docs/POC.md POC-5.

## Context

Through POC-4, clusters lived in the static config and every operator verb opened the directory file from the CLI host. Adding a cluster meant a restart; running a verb meant being on the box that holds the file, with that config and its secrets. POC-5 asks for an operator to add `vast02` to a running shunt, walk `data01` across, and remove `vast01`, all without a restart and with commands that verify themselves.

§1.5 already says where clusters belong: in the control plane, next to the tenants and placements that name them. P3c puts that state in Postgres behind `shunt-control`. The POC needs the same shape on the file backend of ADR-0005, and it needs the operator surface to survive the swap.

## Decision

**Clusters move into the directory file** (`clusters:` next to `tenants:` and `placements:`), validated by the same `config.ValidateClusters` rules as before. In resign mode the config refuses `clusters:`; passthrough keeps its single config cluster, because it has no directory. A directory may start with no clusters at all.

**Secrets are never in the directory.** A cluster names its secret only as a `secret_ref` (`env:NAME` or `file:/path`), resolved by the process that uses it. (Amended by ADR-0010: `cluster add` may also send the secret itself, which shunt stores in a 0600 file and references as `file:`.) For a cluster added live, `file:` is the practical form: an `env:` var must already be in `shunt serve`'s environment.

**The live cluster set is a registry.** `upstream.Registry` holds the current `*Set` behind an atomic pointer; a request loads it once, next to its directory snapshot. `FileDir` gains two hooks. `Prepare` runs on every new file before it is installed, on reload and on a write alike, and builds the next set: unchanged clusters keep their `*Cluster` and connection pool, new or changed ones are built and their secrets resolved. If a secret cannot be resolved the file is rejected, and on the write path the API call fails with the reason, so a placement can never reference a cluster the proxy cannot sign for. `OnInstall` republishes the route gauges after every install, including the proxy's own writes.

**The control API lives on the admin listener** under `/v1/` (`internal/control`), one handler per operation, JSON in and out, errors as `{code, message}` with 409 for a refusal. Every mutation goes through `FileDir`'s write path (flock, re-read, version bump, rename, `changes.jsonl`) with actor `api:<peer>`. The routes are listed in docs/reference/control-api.md. The CLI verbs (`cluster`, `tenant`, `adopt`, `expand`, `ramp`, `migrate`, `cutover`, `purge-source`, `status`) are thin clients of it; `shunt directory` stays as an offline file tool for development and recovery.

**Authentication** is a bearer token named by `admin.control_token_ref`, compared in constant time. Without one, the API answers loopback peers only, and `serve` warns if the admin address is not loopback.

**Refusals the API owns**: `cluster remove` while any placement (primary, source, target, names) or tenant default references the cluster; `purge-source` outside CUTOVER, without recorded cutover evidence, or with any source key the primary lacks; `cutover` without a converged mover report or with fallback reads during the window (ADR-0004 amendment, POC-5).

## Consequences

- P3c replaces `FileDir` behind the same API: `shunt-control` serves `/v1/` from Postgres, and the CLI does not change.
- Two pieces of state are per proxy and in memory: mover progress (lost when the proxy restarts; the mover re-reports on its next pass) and the fallback-read counter the cutover window reads. With several proxies, cutover must be run against each or wait for P4's fleet aggregation. Documented in docs/migrating.md and STATUS.md.
- A multipart upload started on a cluster that is later removed answers `NoSuchUpload`; `cluster remove` is refused while any placement still names the cluster, so this only hits uploads abandoned across a whole migration.
- The file directory carries cluster definitions, so a hand edit to a cluster is picked up on the next poll, through the same `Prepare` check.
- Tests: `internal/directory` (cluster lifecycle, hooks), `internal/upstream` (registry swaps under concurrent load), `internal/control` (the walkthrough through the API, every refusal, authorization), `cmd/shunt` (the verbs against an in-process API), and `test/e2e/walkthrough.sh` live.
