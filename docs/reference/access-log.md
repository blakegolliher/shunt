# Access log

One JSON object per request on stdout (or `telemetry.access_log.path`), written by `internal/telemetry.AccessLogger` when `telemetry.access_log.enabled` is true. The field set is fixed; the object key is never logged (docs/DESIGN.md §2.7).

| Field | Type | Meaning |
|---|---|---|
| `ts` | RFC 3339 | Time the record was written (request end) |
| `request_id` | string | shunt's `X-Shunt-Request-Id`, 32 hex chars |
| `upstream_request_id` | string | Backend's `x-amz-request-id`, empty if it sent none |
| `client` | string | Client `ip:port` |
| `method` | string | HTTP method |
| `host` | string | `Host` header as received |
| `style` | `path` / `virtual-host` | How the bucket was addressed |
| `bucket` | string | Bucket name, empty at service level |
| `op` | string | Operation from the classifier (AWS API name, `Unknown`, or `Preflight`) |
| `status` | int | Status sent to the client; `0` when none was produced |
| `bytes_in` | int | Request body bytes forwarded upstream |
| `bytes_out` | int | Response body bytes sent to the client |
| `duration_ms` | float | Request start to record time |
| `ttfb_ms` | float | Upstream headers written to first response byte; `0` if none |
| `upstream` | string | Endpoint `host:port` the request went to |
| `cluster` | string | Cluster name |
| `tls` | string | Client TLS version (`1.2`, `1.3`), empty for plaintext |
| `error` | string | Empty on success; `upstream: …` when no upstream response, `body: …` when a body copy failed (see ADR-0003) |

Example:

```json
{"ts":"2026-09-15T06:11:37.441-07:00","request_id":"dd8d08e6b0f0b17f187cc6cc02214839","upstream_request_id":"","client":"127.0.0.1:49194","method":"GET","host":"127.0.0.1:8443","style":"path","bucket":"smoke","op":"GetObject","status":200,"bytes_in":0,"bytes_out":5000000,"duration_ms":41.2,"ttfb_ms":1.9,"upstream":"127.0.0.1:3900","cluster":"garage","tls":"1.3","error":""}
```
