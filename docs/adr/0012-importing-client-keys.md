# ADR-0012: shunt holds the keys clients already have

Status: accepted (POC-5 follow-up, 2026-09-17). Source: docs/DESIGN.md §11 invariant 3 ("`shunt adopt` imports the tenant's existing access keys"), ADR-0001 (resign mode), ADR-0011 (step-out). Amends ADR-0010's generated lab key.

## Context

Resign mode verifies a client's signature against a key **shunt** holds and re-signs upstream with the cluster's own credentials (ADR-0001). Where that key comes from decides two things that look unrelated and are the same thing:

- **Going in.** A brownfield insertion is supposed to cost clients nothing (§11): DNS moves, the bucket keeps its name, and the client keeps its key. That last one only works if shunt can verify signatures made with the key the cluster issued, which means shunt needs that key's secret.
- **Coming out.** `shunt step-out` (ADR-0011) asks the cluster whether it would accept the keys shunt holds. A key shunt generated is a key no cluster knows, so the check blocks and the operator learns, at the end, that leaving costs every client a credential change.

Until now the only way in was a credentials file written by hand before `shunt serve` started, and the file was read once at startup (`auth.Load`). A key could not be added to a running shunt at all, and the lab path (ADR-0010) generated a key that exists nowhere else.

## Decision

**`shunt adopt <cluster> <bucket> --keys <file>`** imports the keys that cluster already issued, in the same schema as `auth.credentials_file` (`access_key` and `secret` per entry, `buckets` optional), so an operator can hand over a file they already have. Each key is checked against the cluster **before** the placement is written: shunt signs `ListBuckets` with it, and an unknown key or a wrong secret refuses the whole adopt, in the words `cluster add` already uses (`s3.ClassifyCredentialError`). A `secret_ref` is refused here with the reason: shunt verifies signatures itself, so it needs the secret, not a reference the file's own loader would resolve later.

**`shunt client add <access-key> [--check <cluster>] [--tenant] [--buckets]`** imports one key later — after a move onto a cluster whose keys are different, or to rotate one. It prompts for the secret without echo, like `cluster add`, and checks against `--check`, or the tenant's default cluster when that is unset.

**`shunt client remove <access-key>`** is the other half: it drops a key shunt holds. A lab shunt's generated key is the first thing to remove once the clients' own keys are in, and it is what stands between an otherwise-ready deployment and `step-out`.

**Both go through the control API** (`POST` and `DELETE /v1/tenants/{tenant}/client-keys[/{access_key}]`), so they work against a running shunt from any host the admin listener trusts, and P3c serves the same routes. The secret crosses that listener once, as `cluster add`'s already does (ADR-0010), and is never logged, never returned, and never in a `GET`.

**The credential store becomes updatable.** `auth.Static` keeps its map behind an `atomic.Pointer`, so `Lookup` on the request path stays lock-free, and a write swaps a cloned map after rewriting the credentials file atomically at 0600 (temp file, `rename`). Writes are serialized by a mutex. The file is rewritten from the entries shunt parsed, so a `secret_ref` stays a ref and an inline secret stays inline, but comments do not survive: the file is a secret store, not a config file.

**The store keeps its single-method seam.** `sigv4.CredentialStore` still has only `Lookup`; the control server reaches the rest through function fields (`TenantKeys`, `AddKey`, `RemoveKey`), which P3c wires to its own store.

## Consequences

- **The entrance and the exit are the same property.** With the clients' own keys imported, `shunt step-out` passes on the day shunt is inserted, not only after a migration. Proven live: a bucket clients already used on MinIO with MinIO's key, adopted with `--keys`, the generated lab key removed, the same client reading and writing through shunt unchanged, `step-out` READY, and after `shunt serve` stopped, all 20 objects read straight from MinIO with the same key and bucket name.
- **A migration across credential domains still costs clients a key change**, and step-out now says so before the move rather than after: the key must exist on the cluster the bucket ends on. Where a backend can be given a key pair, importing the same pair on both sides keeps clients unchanged end to end.
- **Credentials change without a restart**, which ADR-0010's file did not. This is the credential hot reload docs/POC.md defers to P2, arriving early because step-out needs it; the deferred parts (rotation policy, STS, encryption at rest) stay deferred.
- **A key is only as checked as the cluster it was checked against.** `client add` without `--check` and without a tenant default stores the key unchecked, and says so in its output.
- **The credentials file is now written by shunt**, so it must be writable by `shunt serve` (the directory file already is, ADR-0005) and comments in it are lost on the first import. `check-config` still validates it at startup.
- **Tests:** `internal/auth` (add, remove, atomic rewrite, 0600, a ref kept as a ref, a reload seeing both), `internal/control` (import refused against the cluster, stored, duplicate, removal, and step-out turning ready), `cmd/shunt` (`adopt --keys`, `client add` from stdin with the secret never printed, `client remove`), plus the live run above.
