# Control API

shunt's operator verbs (`shunt cluster`, `tenant`, `adopt`, `expand`, `ramp`, `migrate`, `cutover`, `purge-source`, `status`, `proxy`) are clients of this API. A fleet's control plane, `shunt-control`, serves it on every control node (`--api`, default `127.0.0.1:9901`; ADR-0015); a single-node lab proxy serves it itself under `/v1/` on its **admin listener** (`admin.address`, default `127.0.0.1:9900`), next to `/-/metrics` (ADR-0008). The routes and the CLI are the same on both.

## Authentication

| `admin.control_token_ref` | Who is served |
|---|---|
| set (`env:NAME` or `file:/path`) | any peer sending `Authorization: Bearer <token>` |
| unset | loopback peers only, and `serve` warns if the admin address is not loopback |

Anyone else gets 401 `unauthorized`.

The CLI takes `--api` (env `SHUNT_API`, default `http://127.0.0.1:9900`) and `--token-ref` (env `SHUNT_API_TOKEN_REF`).

## Conventions

- Bodies are JSON, and unknown request fields are rejected. That includes a cluster's inline `secret`: clusters carry only a `secret_ref`.
- Errors come back as `{"code": "...", "message": "..."}`:

| HTTP | code | Meaning |
|---|---|---|
| 400 | `bad_request` | malformed body or argument |
| 400 | `invalid` | the change would make the directory invalid (message names the field) |
| 401 | `unauthorized` | no valid bearer token, or a non-loopback peer without a configured token |
| 404 | `not_found` | no such placement or cluster |
| 409 | `refused` | the rules forbid this now; the message says why and what to do. The CLI prints `refused: <message>` |
| 409 | `conflict` | already exists, or a concurrent change won |
| 502 | `backend` | a cluster call failed (HEAD, create, canary, listing, delete) |
| 503 | `unavailable` | the directory is read-only or its lock timed out |

- `202` (`accepted`) is not an error: `POST /v1/operations` wrote an operation record and the action runs; poll the record ([Operations](#operations)).
- `purge-source` and `DELETE /v1/clusters/{name}` take a `token` their dry run issued (ADR-0017); without one they are refused.
- CORS is not enabled: the web UI is served from the control port itself, same-origin, and every other client is the CLI.
- The route table is `docs/reference/control-routes.json`, generated from the code by a test, so a route cannot exist in one and not the other.
- Every mutation goes through the directory write path (lock, re-read, version bump, rename) and appends one record to `<directory>.changes.jsonl` with actor `api:<peer address>`. `GET /v1/audit` reads those records back.
- Every mutation is logged by `shunt serve`, with its actor: one INFO line on success (`bucket adopted`, `target recorded`, `ramp`, `migrate start`, `mover pass`, `cutover window started`, `cutover`, `source purged`, `migrate finish`, `tenant default changed`, plus `cluster added|updated|removed`), and one WARN line for any refusal or failure (`<operation> refused` or `<operation> failed`, with the code and reason). An operator watching serve's log sees every change, whichever host ran the command.
- `{tenant}/{bucket}` is the client's view: the tenant of the access key and the bucket name the client uses. A client key with no tenant belongs to tenant `default`; the CLI takes a bare bucket name for it and never shows it (ADR-0010).

## Routes

### `GET /v1/status[?bucket=t/b][&all=1]`

Returns every cluster, plus every placement that is not plain `ACTIVE` (moving, or with a recorded target). `?all=1` lists every placement; `?bucket=` returns just that one.

```json
{
  "version": 12,
  "clusters": [
    {"name": "vast02", "type": "vast", "scheme": "http", "region": "us-east-1", "endpoints": ["10.0.0.2:80"],
     "access_key": "AKIA…", "secret_ref": "file:/etc/shunt/vast02.secret",
     "conditional_write": true, "conditional_delete": false,
     "read_only": false, "reject_writes": false,
     "references": ["placements.default/data01"]}
  ],
  "placements": [
    {"key": "default/data01", "state": "RAMPING", "primary": "vast02", "source": "vast01",
     "names": {"vast01": "data01", "vast02": "data01-001"}, "ratio": 0.5,
     "read_only": false, "reject_writes": false, "client_keys": 1,
     "ramp_writes": {"primary": 4456, "source": 4353}, "fallback_reads": 37, "dual_deletes": {"both": 812},
     "mover": {"source": "vast01", "primary": "vast02", "pass": 2, "copied": 0, "skipped": 5210, "vanished": 3,
               "failed": 0, "bytes": 0, "done": true, "converged": true, "updated_at": "2026-09-16T20:31:02Z"},
     "cutover": {"at": "…", "window": 60000000000, "fallback_reads": 37}}
  ]
}
```

A bucket spread over legs (ADR-0018 N2) has empty `primary` and `names` and a `legs` list instead: `{"id", "cluster", "bucket", "share", "ranges"}` per leg in key-space order, `share` being its fraction of the key hash space and `ranges` the hash ranges it owns (a new leg of a move owns none yet). While part of it moves (N3), `primary` and `source` are the move's two clusters and `move` says which part.

`client_keys` counts the tenant's client keys whose bucket allowlist admits the bucket. At 0, shunt refuses every request for the bucket until a key is imported; the field is absent when this shunt holds no client keys at all.

`fallback_reads` counts read requests (GET and HEAD) that the source served after the primary answered 404, not distinct objects: `aws s3 cp` of one object sends a HEAD then a GET, so it counts two; a recursive copy sends only GETs. `ramp_writes`, `fallback_reads` and `dual_deletes` are this proxy's counters (`shunt_ramp_writes_total`, `shunt_migration_fallback_reads_total`, `shunt_migration_dual_delete_total`), read without creating series. `mover` is the last progress report, held in memory: it is lost when shunt restarts, and the next mover pass reports again.

### `GET /v1/placements/{tenant}/{bucket}`

Returns `{"key", "placement", "clusters"}`: the placement as stored, plus the definitions of the clusters it names. `shunt migrate run` uses it to find what to copy and where. The definitions include `secret_ref`, never a secret.

### `POST /v1/clusters`

`{"name": "vast02", "cluster": {"type", "scheme", "region", "endpoints": [...], "credentials": {"access_key", "secret_ref"}, "capabilities": {"conditional_write", "conditional_delete"}, "tls": {...}}, "secret": "…"}`

- **Name:** lowercase letters, digits, `-` and `_`.
- **`secret`:** used in place of `credentials.secret_ref`. The server writes it to `directory.secrets_dir` as a 0600 file and points `secret_ref` at it. It is never stored in the directory, logged, or returned. A refused add removes the file; a replaced secret replaces the file; removing the cluster deletes it.
- **`type`:** may be empty. It is then read from the cluster's `Server` header on the credential check: `vast`, `minio`, `aws`, otherwise `s3`.
- **`region`:** may be empty. It is then `us-east-1`, or the region in an `s3.<region>.amazonaws.com` endpoint.
- **`capabilities`:** may be left unset; `expand` measures them.

Adds or replaces a cluster. Before the change is written, the running proxy builds the cluster and resolves `secret_ref` in its own environment. If the secret can't be resolved, the call is refused with the reason, so a cluster shunt cannot sign for is never added. Use `file:` refs for clusters added to a running shunt.

It then signs one `ListBuckets` to the cluster with that access key and secret, and refuses the add when the cluster rejects them:

- **Signature mismatch.** `SignatureDoesNotMatch`, or Garage's `AccessDenied: Invalid signature`: the key exists, but the secret shunt resolves isn't that key's secret. The refusal names the `secret_ref`. For an `env:` ref it adds that the variable is read from `shunt serve`'s environment as it was at startup.
- **Unknown key.** `InvalidAccessKeyId`, or Garage's `AccessDenied: No such key`: the cluster has no such access key.
- **Unreachable.** The cluster can't be reached at all.

A plain `AccessDenied` passes, since a key may be allowed its buckets without being allowed `ListBuckets`. Every refusal is also logged by `shunt serve` as `cluster add refused`.

### `POST /v1/clusters/probe`

Takes the same body as cluster add, but does not store the secret or change the directory. It
validates the definition, builds the client, signs the credential probe, and answers the effective
cluster plus `reachable`, `profile`, and capability values with a `known` bit. `profile` is
`measured` only when both migration capabilities were supplied; otherwise it is `assumed`, and
`expand` measures them against a real target bucket. The web UI requires an unchanged successful
probe before enabling Save.

### `DELETE /v1/clusters/{name}[?dry_run=1]`

Removes a cluster, and its stored secret files with it. Refused while any placement (primary, source, target, a `names` entry) or any tenant's `default_cluster` references it; the message lists each reference.

With `?dry_run=1` nothing is removed; the answer says what would happen and carries the token the real call needs (ADR-0017):

```json
{"allowed": true, "name": "vast01", "references": [], "secret_files": 1,
 "token": "1758542400.k2…", "expires_at": "2026-09-22T12:10:00Z"}
```

`allowed: false` carries the refusal the real call would give as `reason`, with the `references`, and no token. The real call takes the token in an optional body, `{"token": "…"}`, and refuses in this order: the references first, then a missing, malformed, expired or mismatched token (see [`purge-source`](#post-v1placementstenantbucketpurge-source) for the messages). The token is bound to the cluster's definition and its reference set, and lasts 10 minutes. The call runs under an operation record and answers `{"removed": "vast01", "operation": "…"}`. `shunt cluster remove` runs the dry run, prints it, and proceeds with the token; `--dry-run` stops after the summary.

### `POST /v1/clusters/{name}/read-only`

`{"read_only": true, "reject": false, "wait": "30s"}` changes the backend maintenance switch
under an operation record. It fences on every registered proxy before and after the directory
write. A write routed to this cluster answers retryable 503 with `Retry-After: 1`; `reject: true`
selects fail-fast 403. Setting `read_only: false` also clears reject mode. The answer carries
`version`, `operation`, live `proxies`, and any `waiting_on` or `silent` proxy ids.

### `POST /v1/tenants/{tenant}/default-cluster`

`{"cluster": "vast02"}`: the cluster that tenant's new buckets are created on. Existing buckets don't move.

### `POST /v1/placements/{tenant}/{bucket}/adopt`

`{"cluster": "vast01", "name": "data01"}`, where `name` defaults to the client bucket name.

Takes over a bucket that already exists: a signed HEAD checks it is there, then a new `ACTIVE` placement is written. If the tenant doesn't exist yet, it is created with this cluster as its default.

Refused if:
- the bucket is missing;
- it has ever been versioned;
- the placement already exists (`conflict`), checked before anything else: the message names the bucket's state, cluster and backend name, and points at `expand` for adding a cluster to it.

### `POST /v1/placements/{tenant}/{bucket}/create-backend`

The browser's Create action. `{"cluster": "vast01", "name": "data01"}` creates the backend S3
bucket, then records it as an `ACTIVE` placement (creating the tenant when needed). `name` defaults
to the client bucket name. If the directory write fails, the newly created backend bucket is
removed. An optional `"keys": [{"access_key": "…", "secret": "…", "buckets": ["data01"]}]` imports
client keys as adopt's `keys` does: each is checked against the cluster before the bucket is
created, and a refused key creates nothing. With `"legs": [{"cluster": "minio01", "name": "data02"}, {"cluster": "minio02"}]`
(two to 32, one per cluster, `name` defaulting to the client bucket name) it creates a bucket spread over
those legs instead (ADR-0018 N2): every leg's bucket must not exist yet, keys are checked against the
first leg's cluster, the buckets are created, and the placement records the legs owning equal shares
of the key hash space. `cluster` and `name` are then unused. A client bucket name the tenant already uses answers
`conflict` as adopt does, before any backend request or key import; a backend bucket that already
exists is refused (adopt takes over an existing bucket), and the cleanup after a failed directory
write deletes only a bucket this call created. The member-only `/create` route remains the first half of a proxy's signed S3
CreateBucket flow and does not duplicate the backend request.

### `POST /v1/placements/{tenant}/{bucket}/expand`

`{"to": "vast02", "name": "data01-001", "create": false, "accept_existing_objects": false}`, where `name` defaults to `<primary backend name>-NNN`, the lowest unused number.

Prepares the target while the placement stays `ACTIVE`, in this order:
1. Refuses if either bucket was ever versioned.
2. Verifies the target bucket exists, or creates it with `create`. An existing bucket must be empty: a one-key listing that finds an object refuses, naming the key, because the bucket's objects would join the moving bucket (in its listings, in target-first reads of a shared key, and ahead of the mover's `If-None-Match` copy, so a stale object would win at cutover). `accept_existing_objects` states they are this bucket's objects, copied ahead (by backend replication, for instance), and skips the check. A first `ramp` or `migrate` step that names its own target with `to` is checked the same way.
3. Runs a canary PUT, GET and DELETE of `.shunt-canary-<hex>`.
4. If the target cluster's `conditional_write` or `conditional_delete` is unset, measures it on a scratch object: a PUT with `If-None-Match: *` over it must get 412, and a DELETE with a wrong `If-Match` must get 412. It records the results on the cluster (`measured: true` in the answer). A capability set explicitly is left alone.
5. Records `target` and its name on the placement.

Returns `{"key", "target", "name", "created_bucket", "canary", "conditional_write", "conditional_delete", "measured", "version"}`. Later `ramp` and `migrate` calls use the recorded target.

### `DELETE /v1/placements/{tenant}/{bucket}/target`

Forgets the target `expand` recorded, while the placement is still `ACTIVE`, so it can be expanded somewhere else. A recorded target routes nothing, so this needs no fence round. The target's bucket stays on its cluster, since shunt may not have created it. Returns `{"key", "target", "name", "version"}`. Refused once a step has used the target (`… is RAMPING, moving to …`) and when there is none. For a spread bucket (ADR-0018 N3c) it retires the legs that own no keys, such as the destination of a first step released before it routed anything, and returns them as `retired` (`[{"id", "cluster", "bucket", …}]`); their buckets stay where they are, and a bucket one leg owns afterwards returns to its plain form. Refused when every leg owns keys. `shunt expand <bucket> --clear` calls it.

### `POST /v1/placements/{tenant}/{bucket}/read-only`

The same body and operation result as cluster read-only, scoped to one placement. The flag is
independent of migration state and is applied through a strict fleet fence. Reads continue. Writes
and deletes answer 503 plus `Retry-After: 1` by default, or 403 with `reject: true`.

### `POST /v1/placements/{tenant}/{bucket}/ramp`

`{"ratio": 0.5, "prefixes": ["runs/2026-09/"], "to": "", "name": "", "create": false, "wait": "30s"}`

A fenced change (see [The fleet](#the-fleet)). Enters or raises `RAMPING`. A key's writes go to the new primary when its hash is below `ratio` or it starts with one of the `prefixes`. The ratio and prefix set only grow (ADR-0004 race 5). Entering `RAMPING` records the hash as `ramp.hash` (`fnv1a-fmix64-v1`). Raising a ramp whose recorded hash this build does not implement is refused, because it would re-split keys. `to`, `name` and `create` are only needed when `expand` has not recorded a target. Returns a transition result (below).

**Moving part of a bucket (ADR-0018 N3).** A first step (from `ACTIVE`, here or on `migrate`) with `"range": {"from": "0000000000000000", "to": "7fffffffffffffff"}` moves only the keys whose hash falls in the range, from the leg that owns all of them to `to`: the leg already on that cluster, or a new leg named `name` there (a plain bucket's primary becomes its first leg). The ratio is then a share of the range. Every later step, `migrate`, the mover, `cutover` and `purge-source` act on that move, and status reports its two clusters as `source` and `primary` with a `move` entry (`from`, `to`, `range`, `share`). `purge-source` compares and deletes only the range's keys, and deletes the source bucket only if its leg owns nothing else; `finish` is refused while the source leg keeps other keys, since the range's copies there would be strays. When the move ends the destination owns the range, and a bucket one leg owns again returns to its plain form. One move at a time per bucket. A cluster may hold several legs (N3b): a first step naming `to` and a new `name` makes a new leg there, and a plain bucket asked to move to another bucket on its own cluster moves every key that way; status then adds `primary_bucket` and `source_bucket`, since `names` holds one bucket per cluster, and upload ids issued during such a move carry a tag of their bucket (`<clusterID>.<tag>~…`). A first step may name `"leg": "<leg id>"` instead of a range (N3c): the move takes the first range that leg owns. **Consolidating** a spread bucket is that step once per other leg, with `to` and `name` naming the leg kept; when the last move is purged the bucket is plain again and `step-out` can take shunt out of its path (step-out reports a spread bucket as a blocker until then). A later step may repeat the same `leg` or leave it out. `shunt ramp` and `shunt migrate start` take `--leg` and `--range <from>-<to>`.

### `POST /v1/placements/{tenant}/{bucket}/migrate`

`{"accept_lost_write_window": false, "to": "", "name": "", "create": false, "wait": "30s"}`

A fenced change (see [The fleet](#the-fleet)); held when the ramp is below 1. Enters `MIGRATING`. Refused if the target does not honor `If-None-Match: *` (`conditional_write: false`), unless `accept_lost_write_window` is set (ADR-0004 race 2).

### `POST /v1/placements/{tenant}/{bucket}/mover-progress`

`{"source", "primary", "pass", "copied", "skipped", "vanished", "failed", "bytes", "last_key", "done", "converged"}`

Sent by `shunt migrate run` every 1,000 objects and at the end of each pass. Refused if `source` and `primary` do not match the placement. Only counts and the cursor key are stored, in memory.

### `GET /v1/placements/{tenant}/{bucket}/mover-ledger[?limit=20]`

Returns `{"entries": [...]}` from the append-only JSONL ledger on the control node that ran the
browser mover. `limit` is 1 through 500. Entries carry the object key, size, source and destination
ETags, part count, guard mode, outcome and duration. The ledger is local evidence, never directory
or etcd state; another control node may therefore have no tail. The UI treats that as unavailable
and can repeat the guarded mover there after failover.

### `POST /v1/placements/{tenant}/{bucket}/cutover`

`{"window": "60s", "wait": "30s"}`

A fenced change (see [The fleet](#the-fleet)). Enters `CUTOVER`, but only once both of these hold:
- the placement is `MIGRATING` and its latest mover report is `converged` (a whole pass copied nothing and failed nothing);
- the fallback-read counter for the bucket, summed over this proxy and every live member, does not move during `window`. Every member that was live at the start must report twice after the window ends; one that does not is refused by name, since a silent proxy is no evidence of quiet (ADR-0016).

The call blocks for the window. It records `{"at", "window", "fallback_reads"}` on the placement as `cutover`.

### `POST /v1/placements/{tenant}/{bucket}/purge-source`

`{"dry_run": false, "token": "…", "wait": "30s"}`; the body may be empty.

Deletes the source bucket, then returns the placement to `ACTIVE` on its primary. Refused unless all of these hold:
- the placement is `CUTOVER`, and every live member has installed the current directory version (a proxy that has not seen the cutover still reads the source on a miss);
- it carries `cutover` evidence;
- a full listing of both buckets finds no source key that the primary lacks, each candidate confirmed by a HEAD that finds it absent on the primary and then present on the source. The refusal names the first 20 keys it finds.

Once allowed, it aborts the source's in-progress multipart uploads, deletes every object and then the bucket, and applies `CUTOVER → ACTIVE`, which drops the source. It runs under an operation record whose `progress` counts the objects deleted.

Returns `{"key", "source", "bucket", "objects_deleted", "uploads_aborted", "version", "operation"}`.

**The dry run** (`dry_run: true`) makes every check above, walks the source listing once more to count it, and deletes nothing (ADR-0017):

```json
{"allowed": true, "key": "default/data01", "source": "vast01", "bucket": "data01",
 "objects": 5213, "bytes": 1287348211, "uploads_in_flight": 0, "missing": [], "version": 14,
 "token": "1758542400.k2…", "expires_at": "2026-09-22T12:10:00Z"}
```

- `allowed: false` carries as `reason` the refusal the real call would give, with whatever was gathered before it (`missing` when the listing diff was not empty), and no token.
- `objects`, `bytes` and `uploads_in_flight` are information, not what the token is bound to: in `CUTOVER` deletes still reach the source, so the counts move under client traffic. The token is bound to the placement and the source cluster's definition, and lasts 10 minutes.

The real call refuses in this order: the state and the evidence (`… is ACTIVE; purge-source runs on a placement in CUTOVER`); then a missing token (`purge-source needs the confirmation token from its dry run; the token is valid for 10m0s`); then the fence and the listing diff; then a token that is wrong: `the confirmation token is malformed; run the dry run again`, `the confirmation token has expired; run the dry run again`, or `the confirmation token does not match the current state: something changed since the dry run; run it again`. `shunt purge-source` runs the dry run, prints it, and proceeds with the token; `--dry-run` stops after the summary.

### `POST /v1/tenants/{tenant}/client-keys`

Imports one client key: `{"access_key", "secret", "buckets"?, "cluster"?}` (ADR-0012). The key is checked against `cluster`, or the tenant's default cluster when that is empty, by signing `ListBuckets` with it; an unknown key or a wrong secret is refused. It is then stored in shunt's credentials file (0600, rewritten atomically) and used to verify client signatures from the next request on. Answers `{"access_key", "tenant", "checked"}`; the secret is never logged, returned, or readable through the API.

Importing a key shunt already holds, with the same secret and tenant, succeeds: it is the same client key checked against another cluster or given another bucket. Its bucket allowlist becomes the union of both, and no allowlist on either side means every bucket of the tenant. The same access key with a different secret or tenant is refused (`shunt already holds access key … with a different secret`); remove it first to replace it.

`shunt adopt <cluster> <bucket> --keys <file>` sends one of these per entry before the placement is written, and `shunt client add <access-key>` sends one, prompting for the secret.

### `DELETE /v1/tenants/{tenant}/client-keys/{access_key}`

Drops a key shunt holds, for a key that has been replaced or the key `serve --plaintext` generated. Answers `{"access_key", "tenant", "left"}`, `left` being the tenant's remaining keys. `shunt client remove <access-key>`.

### `GET /v1/tenants/{tenant}/step-out`

Read-only: whether this tenant's clients could use their cluster directly, with shunt out of the path (ADR-0011). Changes nothing.

It answers `{"tenant", "cluster", "scheme", "endpoints", "ready", "problems", "notes", "buckets": [{"bucket", "state", "cluster", "name", "problems", "notes"}], "keys": [{"access_key", "problems"}]}`. `ready` is true when no list of problems has an entry. Blockers: buckets on more than one cluster, a placement that is not `ACTIVE`, a bucket whose name on the cluster is not the client's bucket name, multipart uploads in progress, and a client key the cluster does not know, knows with another secret, or cannot use on a bucket. Each client key is checked by signing `ListBuckets` and a `HEAD` of every bucket **as that key** against the cluster. Notes are differences a client would notice without being blocked, such as buckets the key sees directly that shunt does not show.

`shunt step-out [tenant]` prints this and exits non-zero while anything blocks.

### `POST /v1/placements/{tenant}/{bucket}/finish`

`CUTOVER → ACTIVE` without deleting anything: the source bucket stays on its cluster and is no longer referenced. This is the path for a source you want to keep, or delete yourself later.

### Transition result

`ramp`, `migrate`, `cutover` and `finish` return:

```json
{"key": "default/data01", "from": "RAMPING", "to": "MIGRATING", "version": 8, "primary": "vast02", "source": "vast01",
 "ratio": 1, "created_bucket": "", "warning": "", "cutover": null,
 "held": true, "proxies": 2, "waiting_on": [], "silent": ["proxy-c"],
 "operation": "1758542400123-a1b2c3"}
```

`held`, `proxies`, `waiting_on` and `silent` are only present with fleet members (ADR-0016): `held`, the step was written as a hold first; `proxies`, live members that have the change; `waiting_on`, live members that have not installed it yet within `wait` (the change is written, but pending); `silent`, members past their lease that were not waited for. `operation` is the record the change ran under ([Operations](#operations)).

## Operations

Every long-running action (a ramp step, `migrate`, the browser `mover`, `cutover`, `purge-source`, `finish`, and `DELETE /v1/clusters/{name}`) runs under an operation record (ADR-0017): which phase it is in, which proxies it is waiting on, how it ended. The action's own route writes the record, runs, and answers as it always has plus `operation`; `POST /v1/operations` writes the record and answers at once, for a browser that polls or follows [Events](#events). The CLI's `--wait` polls the record. A record is per operation, never per object.

- An operation runs on the control node's lifetime, not the request's: a client that leaves does not stop it, and the record has its outcome.
- `wait` in the args keeps the fence semantics of ADR-0016: a hold that does not reach every member within it is released, and the record ends `refused` with the same message the route gives.
- A record is owned by the node that runs it. A control node that restarts marks its own `running` records `failed` with `the control node running this operation restarted before it finished; repeat the step to complete it`; a held step it left completes when the step is repeated.
- Records are kept in etcd on a fleet (`/shunt/ops/<id>`, the last 1 000, readable from any control node) and in memory on a lab proxy.

### `POST /v1/operations`

`{"kind": "ramp", "placement": "default/data01", "args": {"ratio": 0.5, "wait": "30s"}}`

`kind` is one of `ramp`, `migrate`, `mover`, `cutover`, `purge-source`, `finish`, `placement-read-only`
(a placement, as `tenant/bucket`) or `cluster-remove`, `cluster-read-only` (a `cluster`). `args` is
the body the action's own route takes; unknown fields are refused, and a `purge-source` dry run is
not an operation (call its route). A missing placement or cluster is 404. Answers `202` with the
record:

The short UI mutations `cluster-add`, `adopt`, `create`, and `expand` also produce records when
their own routes are called. Their bodies are never copied into `args`, because they can carry a
cluster or client secret; their secret-free response is recorded as `result`.

```json
{"id": "1758542400123-a1b2c3", "kind": "ramp", "placement": "default/data01", "actor": "api:127.0.0.1", "node": "c1",
 "created": "2026-09-22T12:00:00Z", "updated": "2026-09-22T12:00:00Z",
 "status": "running", "phase": "queued", "args": {"ratio": 0.5, "wait": "30s"}}
```

| Field | Meaning |
|---|---|
| `status` | `running`, then `succeeded`, `failed` (the error's `code` is what the route would have answered) or `refused` |
| `phase` | where it is: `queued` (waiting for the bucket's step lock), `precondition` (waiting for every proxy to have the current version), `hold` (the hold is written; waiting for every proxy to have it), `step` (the step is being written), `settle` (the step is written; waiting for every live proxy to have it), `mover` (copying guarded objects and reporting passes), `window` (cutover's quiet window), `diff` (purge-source's listing diff), `purge` (deleting the source), `done` |
| `waiting_on` | the proxies the current phase waits for, by id, as they change |
| `silent` | members past their lease, not waited for |
| `progress` | `{"done", "total", "unit", "ranges"?}` for a phase with a length: mover objects and cursors, cutover's window in seconds, purge-source's objects |
| `version` | the directory version the step wrote, once it has |
| `args` | the request as given |
| `result` | the answer the route gives, once `succeeded`: a transition result, a purge result, or `{"removed"}` |
| `error` | `{"code", "message"}`, once `failed` or `refused`; the message is the one the route gives |

### `GET /v1/operations/{id}`

The record; 404 `not_found` for an id this control plane does not have (a lab proxy's records die with it).

### `GET /v1/operations[?placement=t/b][&cluster=name][&limit=50]`

`{"operations": [...]}`, newest first, filtered by placement or cluster when given; `limit` defaults to 50 and is capped at 500.

## Events

### `GET /v1/events`

Server-sent events from this control node (ADR-0017): `Content-Type: text/event-stream`, one `id: <epoch>:<seq>`, `event: <type>`, `data: <json>` block per event, and a `: keepalive` comment every 15 s while nothing happens. A change made by the CLI is on the stream within one heartbeat.

| `event` | `data` |
|---|---|
| `directory` | `{"version", "kind": "placement" \| "cluster" \| "tenant", "key", "op": "put" \| "delete", "record"}`, one per record that changed at that version (a cluster record carries `secret_ref`, never a secret), then `{"version", "kind": "version"}` once every record of the version has been sent |
| `fence` | `{"operation", "kind", "placement" \| "cluster", "status", "phase", "waiting_on", "silent", "version"}`, every time an operation record changes, from any control node |
| `fleet` | `{"id", "event": "joined" \| "left" \| "silent" \| "live" \| "applied", "applied", "version"}`, from the fleet table read once a second |
| `telemetry` | a merged 10 s telemetry window is ready: `{"start", "end", "scopes"}` |
| `reset` | `{"reason"}`: the id the client resumed from is not in this node's ring; reload the read models, then follow the stream |

- Ids are `<epoch>:<seq>`, the epoch being this node's start, so an id from another node or an earlier run is recognized as foreign. Each node keeps its last 4 096 events. Resuming with `Last-Event-ID` (the header, or `?last_event_id=` for a client that cannot set one) replays what was missed when the id is in the ring, and sends `reset` first when it is not.
- The bearer token is the only authentication, so a browser reads the stream with `fetch` and the `Authorization` header, not `EventSource`, which cannot send headers; a token never goes in a URL.
- On `shunt-control` the route answers 503 `unavailable` until the node has loaded the directory (waiting for quorum); a client treats it as retry. At most 64 streams per node; the 65th is 503.

## Telemetry

### `GET /v1/telemetry/series`

Query parameters are `scope=fleet|cluster:<name>|proxy:<id>`,
`series=client_total|upstream_ttfb|upstream_total|proxy_overhead` or one of the exact-counter rates
`requests_per_second|bytes_in_per_second|bytes_out_per_second|errors_0_per_second|errors_4xx_per_second|errors_5xx_per_second`,
`op=all|read|write|list|delete|multipart|other` (default `all`), and optional RFC3339 `from` /
`to`. The answer is a chronological slice of the 60-minute, 10-second-window ring:

```json
{"scope":"fleet","series":"client_total","op":"all","points":[
  {"start":"2026-09-22T12:00:00Z","end":"2026-09-22T12:00:10Z","series":"client_total","op":"all",
   "count":1842,"p50_us":940,"p90_us":1810,"p99_us":4220,"p999_us":7310,"max_us":9100}
]}
```

Percentiles are emitted by the control node from merged HdrHistogram sketches; clients never
average percentiles. Values are integer microseconds. An unknown scope may validly have an empty
`points` array; malformed scope, series, operation, or time filters answer 400.

Counter-rate points have the same `start`, `end`, `series` and `op`, plus `value` instead of
percentiles. The control node divides the exact counter by that window's duration; the browser
plots `value` unchanged. `errors_0_per_second` means no response status was produced, normally a
client disconnect before headers.

### `GET /v1/telemetry/latest`

Returns `{"windows": [...]}` with the newest completed window for every available scope: `fleet`,
each `cluster:<name>`, and each `proxy:<id>`. A window has `scope`, `start`, `end`, a `series` array
with the percentile records above, and `counters` by operation class (`requests`, `bytes_in`,
`bytes_out`, and `errors` by status class). It is empty until the first 10-second window closes.
The same read model is filled by member heartbeats on a fleet and directly by the proxy handler in
a single-node lab.

## Views

Read models for a browser: what one screen shows, in one answer, with no secret in it (ADR-0017). `GET /v1/placements/{tenant}/{bucket}` keeps its secrets for the mover; these routes strip them.

### `GET /v1/clusters/{name}/view`

The cluster as `GET /v1/status` lists it, plus:

```json
{"name": "vast02", "type": "vast", "scheme": "http", "region": "us-east-1", "endpoints": ["10.0.0.2:80"],
 "access_key": "AKIA…", "secret_ref": "control:vast02", "conditional_write": true, "conditional_delete": false,
 "references": ["placements.default/data01"],
 "capabilities": {"conditional_write": {"value": true, "known": true}, "conditional_delete": {"value": false, "known": false}},
 "probe": {"reachable": true, "latency_ms": 3.2, "checked_at": "2026-09-22T12:00:00Z"}}
```

- `capabilities`: `known` is true when the definition states the capability (set by the operator, or measured by `expand`); `known: false` is the assumed default, which the UI marks as such.
- `probe`: one signed `ListBuckets` made for this answer, with a 2 s timeout; `error` says why it is not `reachable`.

### `GET /v1/placements/{tenant}/{bucket}/view`

The placement as `GET /v1/status` reports it (state, sides, names, ratio, prefixes, hold, counters, cutover evidence, mover), plus:

```json
{"key": "default/data01", "state": "RAMPING", "primary": "vast02", "source": "vast01", "names": {"vast01": "data01", "vast02": "data01-001"},
 "ratio": 0.5, "ramp_writes": {"primary": 4456, "source": 4353}, "fallback_reads": 37, "dual_deletes": {},
 "fence": {"version": 12, "held": false, "proxies": 2, "waiting_on": [], "silent": []},
 "source_uploads_in_flight": 0, "operations": ["1758542400123-a1b2c3"],
 "migration_window": {"start":"2026-09-22T12:00:00Z","end":"2026-09-22T12:00:10Z","writes":{"source":51,"primary":49},"reads":{"target_hit":70,"fallback_source":3,"miss":1}},
 "clusters": {"vast01": {"name": "vast01", "…": "…"}, "vast02": {"name": "vast02", "…": "…"}}}
```

- `fence`: the fleet read once for this answer; `waiting_on` are live members that have not installed `version`, `silent` members past their lease, `held` a step written as a hold and not yet completed (ADR-0016).
- `source_uploads_in_flight`: multipart uploads in progress on the source bucket, which a cutover would cut off; `null` without a source, or when the source could not be asked (then `source_uploads_error` says why).
- `operations`: the running operation records on this placement.
- `clusters`: the definitions of the clusters the placement names, as `GET /v1/status` lists them, without secrets.
- `migration_window` is the latest completed fleet 10-second window for this placement: exact ramp
  write outcomes (`source`, `primary`) and migrating read outcomes (`target_hit`, `fallback_source`,
  `miss`). It is absent before a matching window closes. These bounded counters ride the same
  heartbeat and merge deadline as latency telemetry.
- `mover` is the last report this node received, held in memory. `ranges` currently contains one
  ordered `all keys` range with its cursor and completed object count; a later split worker can add
  ranges without changing the read model.

### `GET /v1/audit[?limit=50][&before=<version>]`

`{"changes": [...]}`: the change records, newest first, the last `limit` (default 50, at most 500) at or before directory version `before` (default: the current version; the oldest record's `version` minus one is the next page).

```json
{"changes": [
  {"ts": "2026-09-22T12:00:01Z", "actor": "api:127.0.0.1", "op": "set-state", "key": "default/data01", "version": 12,
   "before": {"state": "ACTIVE", "…": "…"}, "after": {"state": "RAMPING", "…": "…"}}
]}
```

`op` is `create`, `delete`, `set-state`, `placement-read-only`, `set-default`, `adopt`, `set-target`,
`cluster-put`, `cluster-read-only`, `cluster-remove`, or on a fleet `key-add` and `key-remove`;
`before` and `after` are the placement, or `cluster_before` and `cluster_after` the cluster
definition (with `secret_ref`, never a secret). A fleet keeps the last 10 000 versions in etcd; a
lab proxy reads the tail of `<directory>.changes.jsonl`. An authenticated request's actor is a
stable, non-secret `token:<12-hex fingerprint>`; a loopback-only API with no token records
`api:<peer address>`. OIDC can replace that label without changing the API.

## The fleet

Several proxies serve one directory when a control plane holds it: `shunt-control`, three nodes (one for a lab) with the directory in embedded etcd (ADR-0015). Every route above is served by every control node; the CLI's `--api` may point at any of them. A proxy whose config names them in `control.endpoints` is a **member**: it takes its directory, client keys and cluster secrets from `GET /v1/directory`, forwards bucket creation, sends a heartbeat, and serves no `/v1/` of its own. A proxy without `control.endpoints` is a single-node lab with the file backend, whose in-process API serves the routes above from the file and takes no members. How to run a fleet: docs/fleet.md; the control nodes: docs/how-to/run-shunt-control.md.

- **Fenced changes** (`ramp`, `migrate`, `cutover`; `finish` and `purge-source` wait too) first wait until every live member has the current version, and are refused with the ids they are waiting on if one does not within `wait`. A bucket's **first** step and read-only changes wait for every member, live or not, because one cut off before them could still write; `DELETE /v1/fleet/{id}` removes a member that is gone for good (ADR-0016).
- **The hold.** With members, a step that moves writes to the new primary is written twice: once as `ramp.hold`, where the keys it moves answer writes with `503` + `Retry-After: 1`, and again as the step itself once every member has the hold. If the hold does not reach every member within `wait`, it is released and the call is refused: nothing changed.
- **Stale mode.** A member whose last acknowledged heartbeat is older than `control.lease_ttl` refuses writes and deletes on buckets that are not `ACTIVE` with `503` + `Retry-After: 1`, reads every key of such a bucket target-first-then-source (it may have missed a step), and serves everything else, ACTIVE buckets included. A heartbeat renews the lease only once the member has installed the version the control plane answered with. Its `/-/healthz` stays 200: a control-plane outage must not drain the fleet. `/-/fleet` on a member reports `{"id", "control_node", "stale", "last_ack", "applied"}`.
- **Quorum loss.** With a minority of control nodes up, no route that writes answers (503 `unavailable`), heartbeats are not answered, so every member goes stale within `lease_ttl`; reads and writes on ACTIVE buckets continue on every proxy. Quorum back, everything resumes with no operator action.

### `GET /v1/directory?since=<version>&wait=<duration>`

The whole directory once it is newer than `since`, or 304 when `wait` runs out first (default 0: answer at once). A member long-polls it. The answer is the directory file's shape (`version`, `clusters`, `tenants`, `placements`) plus `credentials` (`[{"access_key", "secret", "tenant", "buckets"}]`, every client key) and `secrets` (`{"control:<cluster>": "<secret>"}`, the cluster secrets the control plane stores). It carries secrets, which is why the control channel must say `plaintext: true` while TLS for it is deferred (ADR-0015).

### `POST /v1/placements/{tenant}/{bucket}/create`, `DELETE /v1/placements/{tenant}/{bucket}`

A member's S3 `CreateBucket` and `DeleteBucket`, forwarded: `{"cluster", "name", "actor"}` claims the placement row under the backend name (the member then creates the backend bucket itself, as a lab proxy does), and the delete removes an ACTIVE row. Both answer `{"key", "version"}`; the member installs that version before it answers its client.

### `POST /v1/fleet/{id}/heartbeat`

Sent by a member every `control.heartbeat_interval`: `{"started", "seq", "applied", "host", "version", "fallback_reads": {"<tenant>/<bucket>": n}, "telemetry": {...}}`, with fallback counters for buckets that are not `ACTIVE` only; `host` is the member's host name and `version` its build. `telemetry` is the last non-empty completed 10-second window: compressed HdrHistogram sketches and plain counters, re-sent until another window closes. The first heartbeat from an id makes it a member (`/shunt/fleet/members/<id>`, persistent); each one renews its lease (`/shunt/fleet/proxies/<id>`, `lease_ttl` + 5 s). Answers `{"version", "lease_ttl"}`; a member whose version is behind fetches the directory at once. Unknown fields are refused, so control nodes are upgraded before proxies.

The request decoder is capped at 1 MiB. `TestHeartbeatPayloadBound` fills all 6 operation classes ×
4 series with a realistic 10,000-value latency spread: each compressed sketch is 2,592 bytes, the
12-cluster JSON heartbeat is 1,025,921 bytes, and 13 clusters is 1,111,406 bytes. Twelve clusters
active on one proxy within one window is therefore the tested dense ceiling; ordinary sparse
sketches are a few hundred bytes. Plan and load-test a larger per-proxy active-cluster count before
raising the cap.

### `GET /v1/fleet`

`{"version", "members": [{"id", "live", "applied", "seq", "started", "seen", "since_seen", "host", "version", "fallback_reads", "telemetry"}]}`. `live`: the lease has not expired; a member that is not live is what the web UI shows as stale. `shunt proxy list`.

### `DELETE /v1/fleet/{id}`

Forgets a member that is gone for good. Refused while its lease is live: a running proxy re-joins on its next heartbeat. `shunt proxy forget <id>`.

## The control plane's own routes

Served by `shunt-control` only, under the same token, for its own lifecycle; the `shunt-control` verbs are their clients.

| Route | Verb | Does |
|---|---|---|
| `GET /v1/control/status` | `status`, `member list` | `{"node", "version", "cluster": {"members": [{"name", "id", "peer_urls", "leader", "started"}], "quorum", "started", "has_quorum", "revision", "db_bytes", "db_in_use_bytes", "quota_bytes", "leader"}, "fleet": [...], "directory": <version>, "directory_loaded", "last_compaction", "compaction": {"mode", "retention"}, "join"}`. `last_compaction` is when etcd last compacted, `null` until it has since this node started; `compaction` is the schedule it runs on; `join` is the `shunt-control join` line a new node runs, built from this node's flags, with the new node's name, host and data directory left to fill in |
| `GET /v1/control`, `GET /v1/control/` | the web UI | The same answer as `GET /v1/control/status` |
| `POST /v1/control/members` | `join` | `{"name", "peer_url"}` adds a member and answers `{"initial_cluster", "encryption_key"}`: what the new node starts with. The key crosses the channel here |
| `DELETE /v1/control/members/{name}` | `member remove` | Removes a member; the only member is refused |
| `GET /v1/control/snapshot` | `snapshot save` | Streams a point-in-time snapshot of the store (secrets sealed) |
| `POST /v1/control/defrag` | `defrag` | Compacts this node's database file in place |
