# ADR-0005: The directory is its own file, written through a lock and read from a snapshot

Status: accepted (POC-3). Source: docs/DESIGN.md §2.3, §1.5, decisions 13–14; docs/POC.md POC-3.

## Context

From POC-3 a request is routed by its placement: `(tenant, bucket)` decides which cluster serves it and under which backend bucket name. The placement table must be readable on every request without a lock, and writable by the proxy itself (`CreateBucket`, `DeleteBucket`) and by the operator (`shunt directory set-state`), possibly from several processes at once. In POC-2 tenants and placements lived in the main config file, which is operator-owned and read once at startup.

The product form is Postgres behind `shunt-control` (§1.5). The POC and single-site deployments need something with the same shape and no new service.

## Decision

**The directory is a separate YAML file**, named by `directory.file`, required in resign mode and forbidden in passthrough (where the client's signature covers the bucket name, so nothing can be renamed). `tenants` and `placements` move out of the config: shunt rewrites the directory, and it must never rewrite the operator's config. Both files are validated by `check-config`, the directory against the configured clusters.

`Directory` is one of the two interface seams named in CLAUDE.md. `FileDir` is its file backend:

- **Reads** take an immutable `*Snapshot` from an `atomic.Pointer`. `Lookup` is one map read with no allocation, so routing costs nothing per request.
- **Reloads** happen on a `directory.poll_interval` stat check (inode, mtime, size) and on SIGHUP. A new file must parse, validate against the running clusters, and carry a **higher `version`**; anything else keeps the last good snapshot and is reported once per change. A ConfigMap's `..data` symlink swap is picked up because the file is opened by path.
- **Writes**, from the proxy and the CLI alike, take an exclusive `flock` on `<file>.lock` (5 s, else `ErrLockTimeout` → 503), re-read the file, apply the change to what is on disk, increment `version`, validate, and replace the file with a temp file, fsync, rename, and directory fsync. Each write appends one record to `<file>.changes.jsonl`: actor, operation, before, after, version, timestamp. That file is the `directory_changes` log of §2.5, which POC-4's demo prints.
- **Errors are typed**: `ErrExists`, `ErrNotFound`, `ErrConflict`, `ErrReadOnly` (EROFS or EACCES, e.g. a read-only ConfigMap), `ErrLockTimeout`, `ErrStaleVersion`.
- **Validation** owns the rules that keep tenants apart: placement keys and backend names must be valid S3 bucket names, and **two placements may never share a backend bucket on one cluster**, which would make two tenants' buckets the same bucket.

**CreateBucket claims the row first, then creates the backend bucket.** The reverse order, which the POC-3 plan proposed, has a failure mode: a backend that answers a re-create by the same credentials with 200 (AWS in us-east-1) or that leaves the bucket in place makes the compensating delete ambiguous, and a request that lost a race could delete the winner's bucket. Claiming the row first means the only compensating action is removing shunt's own row. The order also gives adoption a safe meaning: if the backend says `BucketAlreadyOwnedByYou` for a name no placement uses, that bucket is an orphan from an earlier half-finished create, and it is adopted with a warning.

## Consequences

- Every write rewrites the whole file: O(placements) per write, fine for the POC and a single site, replaced by Postgres in P3c.
- The lock needs a local filesystem. NFS `flock` semantics are not relied on.
- On Kubernetes a ConfigMap is mounted read-only, so `CreateBucket` answers 503 until P3c. `shunt directory set-state` needs a writable copy.
- Several proxies converge only within `poll_interval`; a bucket created on one is `NoSuchBucket` on another for up to that long. ADR-0004 (POC-4) accounts for it.
- Hand edits must increment `version`, and a rewrite drops YAML comments.
- Tests: `internal/directory` covers every transition pair, concurrent creates from two instances and a second process, reload on rename and on a symlink swap, a rejected version regression, a read-only directory, the change log, and a fuzz target over the parser.
