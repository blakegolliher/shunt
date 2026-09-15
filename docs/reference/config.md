# Configuration reference

YAML, validated at startup and by `shunt check-config <file>`. Unknown keys are errors; every error names the offending key path. Schema: `internal/config/config.go`; rules: `internal/config/validate.go`; samples: `internal/config/testdata/`.

## `listener`

| Key | Type | Default | Rule |
|---|---|---|---|
| `address` | host:port | `:443` | |
| `domains` | list | | Wildcard domains for virtual-host addressing, e.g. `"*.s3.example.net"`; the bare name is path-style |
| `tls.cert`, `tls.key` | paths | | Default pair; both or neither. Required unless `tls.sni` has an entry |
| `tls.min_version` | `"1.2"` / `"1.3"` | `"1.2"` | |
| `tls.sni.<name>.cert/key` | paths | | Pair selected by SNI; `<name>` may be `*.suffix` |

## `admin`

| Key | Type | Default |
|---|---|---|
| `address` | host:port | `127.0.0.1:9900` |

## `auth`

| Key | Type | Rule |
|---|---|---|
| `mode` | `passthrough` / `resign` | Required. `resign` requires `credentials_file`; `passthrough` forbids it |
| `credentials_file` | path | Required with `mode: resign`. YAML: `credentials: [{access_key, secret | secret_ref, tenant, buckets?}]`; inline `secret` is allowed because this file is the secret store (mode must be 0600; encrypted at rest is P3c) |
| `clock_skew` | duration | Default 15m |

## `proxy`

| Key | Type | Default | Rule |
|---|---|---|---|
| `cluster` | name | | Passthrough only, and required there: the `clusters:` entry every request is forwarded to. Forbidden in resign mode, where the placement decides the cluster per bucket |
| `copy_buffer_bytes` | int | 262144 | ≥ 4096; the pooled body-copy buffer size |
| `idle_timeout` | duration | 60s | Data ops: abort when no bytes move for this long |
| `metadata_timeout` | duration | 30s | Metadata ops: total deadline |
| `drain_timeout` | duration | 30s | SIGTERM: wait this long for in-flight requests |

## `directory`

The tenants and placements that route each bucket (docs/DESIGN.md §2.3, ADR-0005). Required in `resign` mode, forbidden in `passthrough`. It is a separate file because shunt writes it (CreateBucket, DeleteBucket, `shunt directory set-state`) and must never rewrite the operator's config.

| Key | Type | Default | Rule |
|---|---|---|---|
| `file` | path | | The directory file. Must exist and validate against `clusters:` at startup |
| `poll_interval` | duration | 1s | How often `serve` re-checks the file for another writer's changes (SIGHUP reloads immediately) |

Directory file schema (validated by `check-config` and `shunt directory validate`):

```yaml
version: 12                       # increments on every write; a reload needs a higher version
tenants:
  acme: { default_cluster: vast-a }
placements:                       # key is <tenant>/<bucket>, both valid S3 bucket names
  acme/data:
    state: ACTIVE                 # ACTIVE | RAMPING | MIGRATING | CUTOVER
    primary: vast-a
    source: minio-1               # required unless ACTIVE, forbidden in ACTIVE
    ramp: { ratio: 0.05, prefixes: ["2026-09/"] }   # RAMPING only
    names: { vast-a: acme-7f3a-data }               # cluster → backend bucket name
    cold: vault                   # with tier: emulated
    tier: emulated                # native | emulated
    lifecycle: "<LifecycleConfiguration/>"
    created: 2026-09-15T18:00:00Z
```

Two placements may never share a backend bucket on one cluster: that would make two tenants' buckets the same bucket. `shunt` writes `<file>.changes.jsonl` (actor, before, after) and takes `<file>.lock` for every write.

## `clusters.<name>`

| Key | Type | Rule |
|---|---|---|
| `type` | `vast` / `minio` / `aws` / `s3` | Required |
| `scheme` | `https` / `http` | Required, never defaulted; `http` is logged at startup |
| `region` | string | Required |
| `endpoint_mode` | `static` / `dns` | Default `static`. `static` requires `endpoints`; `dns` requires `endpoint` (treated as a one-entry list until P3b) |
| `endpoints` | list of host:port | No scheme in entries |
| `endpoint` | host | dns mode |
| `tls.ca` | path | Only with `scheme: https` |
| `tls.insecure_skip_verify` | bool | Only with `scheme: https`. Disables upstream certificate verification; logged as a warning at startup. Temporary use only (the VAST lab cluster in POC-2, pending a valid certificate) |
| `credentials.access_key` | string | Required |
| `credentials.secret_ref` | `env:NAME` / `file:/path` | Required; secrets are never inlined |
| `storage_classes` | `native` / `emulated` | |
| `storage_class` + `access` | class + `instant` / `restore-required` | Set together; `restore-required` only with `GLACIER` or `DEEP_ARCHIVE`; not on a `storage_classes: native` cluster |
| `capabilities.enforces_sha256` | bool | Default true. False means the backend does not reject a wrong hex `x-amz-content-sha256`; shunt then hashes the body itself and logs a mismatch (ADR-0002, POC: log-and-alert). Fill from `shunt probe` |
| `capabilities.unsigned_trailer` | bool | Default true. False means the backend rejects `STREAMING-UNSIGNED-PAYLOAD-TRAILER`; shunt then verifies the trailer checksum itself and forwards `UNSIGNED-PAYLOAD`. Fill from `shunt probe` |

## `tenants`, `placements`

Moved out of the config into the directory file in POC-3 (see `directory` above and ADR-0005). Samples: `internal/directory/testdata/`.

## `telemetry`

| Key | Type | Default |
|---|---|---|
| `access_log.enabled` | bool | false |
| `access_log.path` | path | stdout |
| `slow.ring_size` | int | 100 |
| `slow.threshold` | duration | 500ms |

## `features`

Every flag carries a removal criterion in `internal/config/features.go`; any other key is an error.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `xml_rewrite` | bool | **true** | Rewrite backend bucket names, cluster endpoints, and uploadIds in resign-mode responses (ADR-0006). The one flag that defaults on: with it off, those names reach clients and `serve` warns at startup naming the clusters whose `<Location>` leaks. |
