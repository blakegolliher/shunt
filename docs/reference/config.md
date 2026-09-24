# Configuration reference

**No config at all, for a lab:** `shunt serve --plaintext` runs from a state directory (`--state-dir`, default `shunt-data`), which it creates on first start. The directory holds the directory file, a `secrets/` folder, and `credentials.yaml` with one generated client key for the default tenant; `shunt client show` prints that key. The listeners default to `--listen 127.0.0.1:8008` and `--admin 127.0.0.1:9900`. The config file below is for everything else (TLS, tokens, domains), passed with `--config`, and the lab flags are refused alongside it (ADR-0010).

YAML, validated at startup and by `shunt check-config <file>`. Unknown keys are errors; every error names the offending key path. Schema: `internal/config/config.go`; rules: `internal/config/validate.go`; samples: `internal/config/testdata/`.

## `listener`

| Key | Type | Default | Rule |
|---|---|---|---|
| `address` | host:port | `:443` | |
| `domains` | list | | Wildcard domains for virtual-host addressing, e.g. `"*.s3.example.net"`; the bare name is path-style |
| `plaintext` | bool | false | Lab use only: serve plain HTTP instead of TLS. Refused together with any `tls` key. `serve` adds `client_listener="PLAINTEXT http …"` to every log line and warns once: signatures, presigned-URL credentials and object bytes cross the network unencrypted |
| `tls.cert`, `tls.key` | paths | | Default pair; both or neither. Required unless `tls.sni` has an entry or `plaintext: true` |
| `tls.min_version` | `"1.2"` / `"1.3"` | `"1.2"` | |
| `tls.sni.<name>.cert/key` | paths | | Pair selected by SNI; `<name>` may be `*.suffix` |

## `admin`

| Key | Type | Default | Rule |
|---|---|---|---|
| `address` | host:port | `127.0.0.1:9900` | Serves `/-/metrics`, `/-/healthz`, `/-/slow`, `/-/drain`, pprof, and the control API under `/v1/` (docs/reference/control-api.md) |
| `control_token_ref` | `env:NAME` / `file:/path` | | Bearer token the control API requires. Unset: the API answers loopback peers only, and `serve` warns if `address` is not loopback |

## `auth`

| Key | Type | Rule |
|---|---|---|
| `mode` | `passthrough` / `resign` | Required. `resign` requires `credentials_file`; `passthrough` forbids it |
| `credentials_file` | path | Required with `mode: resign`. YAML: `credentials: [{access_key, secret | secret_ref, tenant?, buckets?}]`; a key without `tenant` belongs to the default tenant, whose buckets the CLI addresses by bare name (ADR-0010); inline `secret` is allowed because this file is the secret store (mode must be 0600; encrypted at rest is P3c) |
| `clock_skew` | duration | Default 15m |

## `proxy`

| Key | Type | Default | Rule |
|---|---|---|---|
| `cluster` | name | | Passthrough only, and required there: the config's `clusters:` entry every request is forwarded to. Forbidden in resign mode, where the placement decides the cluster per bucket |
| `copy_buffer_bytes` | int | 262144 | ≥ 4096; the pooled body-copy buffer size |
| `idle_timeout` | duration | 60s | Data ops: abort when no bytes move for this long |
| `metadata_timeout` | duration | 30s | Metadata ops: total deadline |
| `drain_timeout` | duration | 30s | SIGTERM: wait this long for in-flight requests |

## `directory`

The clusters, tenants and placements that route each bucket (docs/DESIGN.md §1.5, §2.3; ADR-0005, ADR-0008). Required in `resign` mode, forbidden in `passthrough`. It is a separate file because shunt writes it (CreateBucket, DeleteBucket, the control API) and must never rewrite the operator's config.

| Key | Type | Default | Rule |
|---|---|---|---|
| `file` | path | | The directory file. Must exist and validate at startup; every cluster it names must build and resolve its `secret_ref` |
| `secrets_dir` | path | `secrets/` next to `file` | Where shunt stores a secret given to `shunt cluster add` (one 0600 file per cluster, referenced as `file:`; ADR-0010) |
| `poll_interval` | duration | 1s | How often `serve` re-checks the file for another writer's changes (SIGHUP reloads immediately) |

## `control`

Makes this proxy a **member** of a fleet run by `shunt-control` (ADR-0015, ADR-0016): it takes its directory, client keys and cluster secrets from the control plane, forwards bucket creation there, sends it a heartbeat, and goes stale (refusing writes on moving buckets) when its lease lapses. Omitted, the proxy is a single-node lab that reads a directory file on this host. A member has no `directory` block and no `auth.credentials_file`; both come from the control plane.

| Key | Type | Default | Rule |
|---|---|---|---|
| `endpoints` | list of URLs | | `shunt-control` API addresses, `http://host:port`; any one is enough, the proxy tries the next on failure. Set, this proxy is a member. Resign mode only |
| `token_ref` | `env:NAME` / `file:/path` | | The control plane's bearer token |
| `plaintext` | bool | false | Required `true` while the endpoints are `http`: the control channel then carries directory versions, cluster secrets and client keys in the clear. TLS for it is deferred (ADR-0015); this key exists so that is stated, as `listener.plaintext` must be |
| `proxy_id` | string | `<hostname>-<admin port>` | This member's id: 1-64 letters, digits, `.`, `_`, `-`. Stable across restarts, so a restarted proxy is the same member |
| `cache_dir` | path | | Required. The last directory this proxy installed, kept 0700, and served after a restart with the control plane down (ACTIVE buckets only; moving ones refuse writes until the lease is back) |
| `heartbeat_interval` | duration | 1s | How often a member reports the directory version it has installed |
| `lease_ttl` | duration | 10s | At least three heartbeats. The longest lease this member takes: a lease runs from a heartbeat's send time for the shorter of this and the control plane's grant. A member whose lease has run out refuses writes on moving buckets |

Directory file schema (validated by `check-config` and `shunt directory validate`):

```yaml
version: 12                       # increments on every write; a reload needs a higher version
schema: 2                         # written by shunt; a newer schema is refused at open (ADR-0021)
identity:                         # drawn by the first write under schema 2, never changed by a write
  cluster_id: 3f0c…               # 32 lowercase hex characters
  epoch: 9a1e…                    # 32 lowercase hex characters; a restore starts a new epoch
generations:                      # written by shunt: the version of the write that last changed each resource
  placement:acme/data: 11         # placement:<tenant>/<bucket> | cluster:<name> | tenant:<name>; absent = unchanged since the upgrade
clusters:                         # same schema as clusters.<name> below; since POC-5 (ADR-0008)
  vast-a:
    type: vast
    scheme: http
    region: us-east-1
    endpoints: ["10.0.0.1:80"]
    credentials: { access_key: AKIA…, secret_ref: file:/etc/shunt/vast-a.secret }
tenants:
  acme: { default_cluster: vast-a }
placements:                       # key is <tenant>/<bucket>, both valid S3 bucket names
  acme/data:
    state: ACTIVE                 # ACTIVE | RAMPING | MIGRATING | CUTOVER
    primary: vast-a
    source: minio-1               # required unless ACTIVE, forbidden in ACTIVE
    ramp: { hash: fnv1a-fmix64-v1, ratio: 0.05, prefixes: ["2026-09/"] }   # RAMPING only; hash written at RAMPING start, never changed (ADR-0004)
    #   ramp.hold: { ratio: 0.25 }  # a step written but not yet on every proxy: its keys' writes answer 503 (ADR-0016)
    target: vast-b                # ACTIVE only: recorded by `shunt expand`, used by the next ramp or migrate
    names: { vast-a: acme-7f3a-data, vast-b: data-001 }   # cluster → backend bucket name
    cutover: { at: 2026-09-16T20:31:00Z, window: 60s, fallback_reads: 37 }   # CUTOVER only: `shunt cutover` evidence
    cold: vault                   # with tier: emulated
    tier: emulated                # native | emulated
    lifecycle: "<LifecycleConfiguration/>"
    created: 2026-09-15T18:00:00Z
    watch: true                   # count this bucket's traffic by backend in telemetry (shunt watch)
```

Two placements may never share a backend bucket on one cluster: that would make two tenants' buckets the same bucket. `shunt` writes `<file>.changes.jsonl` (actor, before, after) and takes `<file>.lock` for every write.

## `clusters.<name>`

In resign mode clusters live in the directory file (above) and are added and removed live with `shunt cluster add|remove`; a `clusters:` key in a resign-mode config is refused. In passthrough mode the config keeps its `clusters:` block for `proxy.cluster`. The schema is the same in both places.

| Key | Type | Rule |
|---|---|---|
| `type` | `vast` / `minio` / `aws` / `s3` | Required |
| `scheme` | `https` / `http` | Required, never defaulted; `http` is logged at startup |
| `region` | string | Required |
| `endpoint_mode` | `static` / `dns` | Default `static`. `static` requires `endpoints`; `dns` requires `endpoint` (treated as a one-entry list until P3b) |
| `endpoints` | list of host:port | No scheme in entries |
| `endpoint` | host | dns mode |
| `tls.ca` | path | Only with `scheme: https` |
| `tls.insecure_skip_verify` | bool | Only with `scheme: https`. Disables upstream certificate verification; logged as a warning at startup. Temporary use only (a VAST cluster with its factory certificate in POC-2, pending a valid certificate) |
| `credentials.access_key` | string | Required |
| `credentials.secret_ref` | `env:NAME` / `file:/path` | Required; secrets are never inlined |
| `storage_classes` | `native` / `emulated` | |
| `storage_class` + `access` | class + `instant` / `restore-required` | Set together; `restore-required` only with `GLACIER` or `DEEP_ARCHIVE`; not on a `storage_classes: native` cluster |
| `capabilities.enforces_sha256` | bool | Default true. False means the backend does not reject a wrong hex `x-amz-content-sha256`; shunt then hashes the body itself and logs a mismatch (ADR-0002, POC: log-and-alert). Fill from `shunt probe` |
| `capabilities.conditional_write` | bool | Default true. False means the backend ignores `If-None-Match: *` on PUT (Garage 2.3.0 does). The mover then falls back to a HEAD-then-commit guard with a race window; see ADR-0004. Fill from `shunt probe` |
| `capabilities.conditional_delete` | bool | **Default false**, unlike the others. True means the backend honors `If-Match` on `DeleteObject`: a mismatched ETag gets 412 and the object stays. The mover then withdraws its own copy with `If-Match` on the ETag its PUT returned; without it, the mover HEADs the target and deletes only if ETag and Last-Modified still match its copy, which leaves a one-round-trip window (ADR-0004 race 1). It defaults to false because a backend that ignores the header deletes unconditionally. Fill from `shunt probe` |
| `capabilities.unsigned_trailer` | bool | Default true. False means the backend rejects `STREAMING-UNSIGNED-PAYLOAD-TRAILER`; shunt then verifies the trailer checksum itself and forwards `UNSIGNED-PAYLOAD`. Fill from `shunt probe` |

`capabilities` are measured facts about a backend, filled from `shunt probe`, not switches over shunt's behavior: the feature-flag and kill-switch rules (CLAUDE.md) do not apply to them. Each default except `conditional_delete` states what a conformant S3 backend does (that one defaults to the choice that cannot delete a client's write); a backend that differs gets a profile, and shunt compensates for the difference (ADR-0002).

## `tenants`, `placements`

Moved out of the config into the directory file in POC-3 (see `directory` above and ADR-0005). Samples: `internal/directory/testdata/`.

## `telemetry`

| Key | Type | Default |
|---|---|---|
| `log_format` | `auto` / `json` / `console` | `auto`: `console` when serve's stderr is a terminal, `json` otherwise (a file, a pipe, journald). `console` writes one line per event for a person watching: `19:25:55 INFO  cluster added  cluster=g type=s3 endpoints=127.0.0.1:3900 …`, with a `[PLAINTEXT]` tag on every line when `listener.plaintext` is on (JSON lines carry `client_listener` instead). The access log is always JSON |
| `access_log.enabled` | bool | false |
| `access_log.path` | path | stdout |
| `slow.ring_size` | int | 100 |
| `slow.threshold` | duration | 500ms |

## `features`

Feature flags: behavior that is off until it is turned on. Each defaults off and carries a removal criterion in `internal/config/features.go`. Any other key here is an error.

| Key | Type | Default | Rule |
|---|---|---|---|
| `debug_route_header` | bool | false | Lab use only. A request carrying `X-Shunt-Debug: 1` gets `X-Shunt-Route: <primary|source> <cluster>` (or `merged <primary>+<source>` on a merged listing) on its response, which `shunt verify --debug-route` tallies. This tells any client that asks which cluster served it, an exception to ADR-0006; `serve` warns at startup. `X-Shunt-Debug` is never forwarded upstream. Removal: when P4 traces carry the route of every request |

## `kill_switches`

Switches that turn off behavior which is normally on, for an operator who hits a bug in it. Each defaults to `false`, the normal behavior, and says what breaks when flipped. They are not feature flags.

| Key | Type | Default | What flipping it does |
|---|---|---|---|
| `xml_rewrite_disable` | bool | false | Stops rewriting backend bucket names, cluster endpoints, and uploadIds in resign-mode responses (ADR-0006). Clients then see a bucket name they cannot address, the cluster's address in `<Location>`, and backend uploadIds that break when a placement moves. `serve` warns at startup and names the clusters whose `<Location>` will leak. |
