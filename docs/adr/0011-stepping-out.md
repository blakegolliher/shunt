# ADR-0011: shunt can be taken out of the path again

Status: accepted (POC-5 follow-up, 2026-09-17). Source: docs/DESIGN.md §11 (brownfield insertion, the four "no client change" invariants), ADR-0008 (the control API), ADR-0010 (the default tenant). Adds the operator verb `step-out` to DESIGN §2.10.

## Context

Everything about shunt is built so that inserting it costs clients nothing: the endpoint name moves to shunt, shunt holds the clients' own keys, and a bucket keeps the name clients use (§11). Nothing said whether the reverse is true. An operator who adopts a bucket, moves it and reaches a good final state has no way to ask "could my clients talk to the cluster directly now, and what would break if I stopped shunt?"

That question decides whether shunt is infrastructure a team chooses or infrastructure a team is stuck with. A proxy that cannot be removed is a lock-in, whatever its documentation says, and a migration tool people cannot leave is a migration tool fewer people start.

Four things make the answer "no" in practice, and none of them is visible from a `status` table:
1. the bucket's name on the cluster is not the name clients use (`expand` names a target `<bucket>-NNN` by default, and a bucket created through shunt gets `<tenant>-<4hex>-<bucket>`), and S3 cannot rename a bucket;
2. the clients' keys are shunt's, not the cluster's, so the cluster refuses them;
3. the tenant's buckets ended up on more than one cluster, so no single endpoint serves them;
4. work in flight only means something through shunt: multipart uploads whose ids shunt rewrote.

## Decision

**`GET /v1/tenants/{tenant}/step-out`, and `shunt step-out [tenant]`.** A read-only check that changes nothing and reports whether the tenant's clients could use one cluster directly, with shunt gone.

**What it checks**, per tenant:
- **One cluster.** Every placement of the tenant names the same primary. More than one is a blocker naming them.
- **Every bucket ACTIVE**, with no move under way. A recorded but unstarted target is a note, not a blocker: stepping out simply abandons it.
- **The name clients use.** The bucket's name on the cluster equals the client's bucket name, or it is a blocker that says so and names `--name` as the fix.
- **Nothing in flight.** No multipart upload is in progress on the bucket, because their upload ids are shunt's (ADR-0006).
- **The clients' own keys.** For each of the tenant's client keys, shunt signs `ListBuckets` at the cluster **with that key** and a `HEAD` of every bucket. An unknown key, a key whose secret differs, or a bucket the key cannot reach is a blocker, in the words `cluster add` already uses (`s3.ClassifyCredentialError`).
- **Notes, not blockers:** buckets the key would see directly that shunt does not show, and a key whose shunt-side bucket allowlist the cluster will not enforce.

**When nothing blocks**, the CLI prints the three steps it cannot take itself: point the clients' S3 name at the cluster, wait out the TTL while watching `shunt_requests_total`, then stop shunt. Until shunt stops, pointing the name back is the rollback, because step-out changed nothing.

**`expand` says so at the time it matters.** When the target bucket's name differs from the client's, `expand` prints one note that this is what would stand between clients and the cluster later, and that `--name <bucket>` keeps the name. The default name policy does not change: a default that silently takes an existing bucket of the same name on the target would be worse than a note.

**The client keys reach the check through a function, not the seam.** `control.Server.TenantKeys func(tenant string) []sigv4.Credential`, wired to the static store's new `Tenant` method. `CredentialStore` (a named seam, CLAUDE.md) keeps its single `Lookup` method, and P3c wires the same function to its own store.

## Consequences

- **The exit is a supported path, tested like the entrance.** `TestStepOut` covers each blocker and the ready case, `TestStepOutCommand` covers the operator's output and its non-zero exit, and a live run on Garage → MinIO went from blocked, through the move, to READY, then read every object straight from MinIO with shunt stopped and the same key and bucket name (docs/STATUS.md).
- **It reports, it does not fix.** Renaming a bucket means moving it again; issuing cluster keys to clients is the cluster's business. shunt says which, in the operator's terms.
- **A check is not a guarantee about the network.** DNS, VIPs and certificates are outside shunt; the printed steps name them, and `shunt doctor` is where cert checks live (DESIGN §2.10).
- **Keys clients keep are worth setting up early.** The check is most useful when shunt holds the cluster's own keys from the start (§11 invariant 3). Importing keys at `adopt` time (`--keys`) is still design, not code.
- **Single-proxy, like cutover evidence.** The check reads this proxy's directory and asks the cluster; with several proxies, the directory is the same but each proxy must be stopped.
