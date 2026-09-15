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
| `cluster` | name | | Required: the `clusters:` entry every request is forwarded to (until the directory arrives in POC-3) |
| `copy_buffer_bytes` | int | 262144 | ≥ 4096; the pooled body-copy buffer size |
| `idle_timeout` | duration | 60s | Data ops: abort when no bytes move for this long |
| `metadata_timeout` | duration | 30s | Metadata ops: total deadline |
| `drain_timeout` | duration | 30s | SIGTERM: wait this long for in-flight requests |

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

## `tenants.<name>`, `placements.<tenant>/<bucket>`

Validated for structure and references now (POC-0), used from POC-3. See docs/DESIGN.md §2.3 and `internal/config/testdata/valid/mixed.yaml`.

## `telemetry`

| Key | Type | Default |
|---|---|---|
| `access_log.enabled` | bool | false |
| `access_log.path` | path | stdout |
| `slow.ring_size` | int | 100 |
| `slow.threshold` | duration | 500ms |

## `features`

Empty. Every flag will default off and carry a removal criterion; any key here is an error until one exists.
