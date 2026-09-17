# Control API

shunt's operator verbs (`shunt cluster`, `tenant`, `adopt`, `expand`, `ramp`, `migrate`, `cutover`, `purge-source`, `status`) are clients of this API. It is served under `/v1/` on the **admin listener** (`admin.address`, default `127.0.0.1:9900`), next to `/-/metrics`. ADR-0008 records the decision. P3c serves the same routes from `shunt-control`, so the CLI and anything scripted against it keep working.

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

- Every mutation goes through the directory write path (lock, re-read, version bump, rename) and appends one record to `<directory>.changes.jsonl` with actor `api:<peer address>`.
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
     "references": ["placements.default/data01"]}
  ],
  "placements": [
    {"key": "default/data01", "state": "RAMPING", "primary": "vast02", "source": "vast01",
     "names": {"vast01": "data01", "vast02": "data01-001"}, "ratio": 0.5,
     "ramp_writes": {"primary": 4456, "source": 4353}, "fallback_reads": 37, "dual_deletes": {"both": 812},
     "mover": {"source": "vast01", "primary": "vast02", "pass": 2, "copied": 0, "skipped": 5210, "vanished": 3,
               "failed": 0, "bytes": 0, "done": true, "converged": true, "updated_at": "2026-09-16T20:31:02Z"},
     "cutover": {"at": "…", "window": 60000000000, "fallback_reads": 37}}
  ]
}
```

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

### `DELETE /v1/clusters/{name}`

Removes a cluster. Refused while any placement (primary, source, target, a `names` entry) or any tenant's `default_cluster` references it; the message lists each reference.

### `POST /v1/tenants/{tenant}/default-cluster`

`{"cluster": "vast02"}`: the cluster that tenant's new buckets are created on. Existing buckets don't move.

### `POST /v1/placements/{tenant}/{bucket}/adopt`

`{"cluster": "vast01", "name": "data01"}`, where `name` defaults to the client bucket name.

Takes over a bucket that already exists: a signed HEAD checks it is there, then a new `ACTIVE` placement is written. If the tenant doesn't exist yet, it is created with this cluster as its default.

Refused if:
- the bucket is missing;
- it has ever been versioned;
- the placement already exists (`conflict`).

### `POST /v1/placements/{tenant}/{bucket}/expand`

`{"to": "vast02", "name": "data01-001", "create": false}`, where `name` defaults to `<primary backend name>-NNN`, the lowest unused number.

Prepares the target while the placement stays `ACTIVE`, in this order:
1. Refuses if either bucket was ever versioned.
2. Verifies the target bucket exists, or creates it with `create`.
3. Runs a canary PUT, GET and DELETE of `.shunt-canary-<hex>`.
4. If the target cluster's `conditional_write` or `conditional_delete` is unset, measures it on a scratch object: a PUT with `If-None-Match: *` over it must get 412, and a DELETE with a wrong `If-Match` must get 412. It records the results on the cluster (`measured: true` in the answer). A capability set explicitly is left alone.
5. Records `target` and its name on the placement.

Returns `{"key", "target", "name", "created_bucket", "canary", "conditional_write", "conditional_delete", "measured", "version"}`. Later `ramp` and `migrate` calls use the recorded target.

### `POST /v1/placements/{tenant}/{bucket}/ramp`

`{"ratio": 0.5, "prefixes": ["runs/2026-09/"], "to": "", "name": "", "create": false}`

Enters or raises `RAMPING`. A key's writes go to the new primary when its hash is below `ratio` or it starts with one of the `prefixes`. The ratio and prefix set only grow (ADR-0004 race 5). Entering `RAMPING` records the hash as `ramp.hash` (`fnv1a-fmix64-v1`). Raising a ramp whose recorded hash this build does not implement is refused, because it would re-split keys. `to`, `name` and `create` are only needed when `expand` has not recorded a target. Returns a transition result (below).

### `POST /v1/placements/{tenant}/{bucket}/migrate`

`{"accept_lost_write_window": false, "to": "", "name": "", "create": false}`

Enters `MIGRATING`. Refused if the target does not honor `If-None-Match: *` (`conditional_write: false`), unless `accept_lost_write_window` is set (ADR-0004 race 2).

### `POST /v1/placements/{tenant}/{bucket}/mover-progress`

`{"source", "primary", "pass", "copied", "skipped", "vanished", "failed", "bytes", "last_key", "done", "converged"}`

Sent by `shunt migrate run` every 1,000 objects and at the end of each pass. Refused if `source` and `primary` do not match the placement. Only counts and the cursor key are stored, in memory.

### `POST /v1/placements/{tenant}/{bucket}/cutover`

`{"window": "60s"}`

Enters `CUTOVER`, but only once both of these hold:
- the placement is `MIGRATING` and its latest mover report is `converged` (a whole pass copied nothing and failed nothing);
- this proxy's fallback-read counter for the bucket does not move during `window`.

The call blocks for the window. It records `{"at", "window", "fallback_reads"}` on the placement as `cutover`.

With several proxies, the window has to hold on each of them: this API reads only its own proxy's counter.

### `POST /v1/placements/{tenant}/{bucket}/purge-source`

Deletes the source bucket, then returns the placement to `ACTIVE` on its primary. Refused unless all of these hold:
- the placement is `CUTOVER`;
- it carries `cutover` evidence;
- a full listing of both buckets finds no source key that the primary lacks, each candidate confirmed by a HEAD that finds it absent on the primary and then present on the source. The refusal names the first 20 keys it finds.

Once allowed, it aborts the source's in-progress multipart uploads, deletes every object and then the bucket, and applies `CUTOVER → ACTIVE`, which drops the source.

Returns `{"key", "source", "bucket", "objects_deleted", "uploads_aborted", "version"}`.

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
 "ratio": 1, "created_bucket": "", "warning": "", "cutover": null}
```
