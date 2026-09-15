# docs/CONTEXT.md — ground truth for Claude Code

Claude Code reads this before every task and treats it as fact. Fill every line; write `unknown` rather than leaving a blank, and `n/a` when it truly does not apply. Keep secrets out — reference env var names only.

## Already decided

| Item | Answer |
|---|---|
| First milestone | LB + telemetry on one cluster (P0 → P1 → P3a → P4 → P8a; gate M1) |
| Deployment targets | Both: bare metal (systemd, `shunt@.service` × N) and RKE2 (kustomize, N replicas) |
| Plaintext upstream | Per-site decision; every cluster states `scheme: https|http`; no default |
| TLS termination | Userspace `crypto/tls` both sides; no kernel or NIC offload |
| Auth mode for M1 | passthrough |
| Product shape | Global endpoint over mixed backends (VAST, MinIO, AWS S3, any S3); tenant-scoped namespace; `resign` auth mandatory from P2 on |
| Control-plane store | Postgres behind `shunt-control` (P3c); file backend for dev; no per-object rows anywhere |
| License | Apache-2.0; no AGPL code (MinIO, Garage) copied |

## Environment

| Item | Answer |
|---|---|
| Dev box distro + kernel (`uname -r`) | RHEL 9.6, `5.14.0-570.17.1.el9_6.x86_64` |
| Go version (`go version`) — pin it in go.mod | `go1.27.1` at /usr/local/go (not on the default PATH; `export PATH=/usr/local/go/bin:$PATH`), `GOTOOLCHAIN=auto`; go.mod says `go 1.27` |
| Container runtime on dev box (docker / podman / none) | podman 5.4.0; `docker` is the podman shim and `docker compose` runs `~/.local/bin/podman-compose`. Makefile `COMPOSE ?= docker compose` |
| Root or CAP_BPF available on dev box? BTF present (`ls /sys/kernel/btf/vmlinux`)? | unknown |
| Egress for `go get` / module proxy? Any GOPROXY/GONOSUMDB settings? | yes; `GOPROXY=https://proxy.golang.org,direct`, no GONOSUMDB, no GOFLAGS |
| GitHub org/repo for the project; CI runner type (hosted / self-hosted) | github.com/blakegolliher/shunt; hosted `ubuntu-latest` (assumed; no remote yet) |
| Internal registry (Harbor URL, project name); env var names for push creds | unknown |
| RKE2 cluster: k8s version, CNI, LB implementation (MetalLB / other), cert-manager issuer name, Prometheus operator present? | unknown |

## Backend mix on day one

| Item | Answer |
|---|---|
| Backend types in the first deployment (VAST / MinIO / AWS S3 / other) and how many of each | unknown (POC uses Garage + MinIO from test/e2e, plus the VAST lab endpoint below) |
| AWS account and scratch bucket available for CI? env var names; region | unknown |
| MinIO version and deployment shape (single node / distributed) for CI | `quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z`, single node, test/e2e/docker-compose.yml |
| Postgres: managed (RDS etc.) or self-hosted (Patroni / CloudNativePG)? version; who runs HA | unknown (P3c) |
| Tenant model: one tenant per customer? per team? how tenants get created and keyed | unknown (POC-3 keys by tenant with one tenant) |
| Backend bucket naming rule you want (e.g. `<tenant>-<hash>-<bucket>`) and any length/charset constraints | unknown |

## Reference backend (the real target)

| Item | Answer |
|---|---|
| VAST lab endpoint(s), scheme, port |  https://vast02.example.com:443 |
| Scratch bucket name Claude Code may create/delete objects in | demo-dest |
| Env var names holding access key / secret | VAST_ACCESS_KEY_ID / VAST_SECRET_ACCESS_KEY |
| VAST version; virtual-host style domain configured? (which) | 5.x; no virtual-host domai |
| Second cluster available for multi-cluster tests later? | https://vast03.example.com:443 |
| Garage / versitygw already installed locally? paths/ports | Garage `docker.io/dxflrs/garage:v2.3.0` via test/e2e compose only: S3 127.0.0.1:3900, admin 127.0.0.1:3903 (`/health` unauthenticated, other endpoints `/v2/*` with bearer token), region `garage`. No versitygw |

## Known VAST compatibility findings (pre-seed backend-compat.md)

| Behavior | Known answer |
|---|---|
| Enforces `x-amz-content-sha256` mismatch? | yes, `400 XAmzContentSHA256Mismatch` (shunt probe, 2026-09-15, vast02) |
| Accepts `STREAMING-UNSIGNED-PAYLOAD-TRAILER`? | yes, and validates the trailer checksum (`400 BadDigest` on mismatch) |
| Checksum headers honored (CRC32 / CRC32C / SHA1 / SHA256 / CRC64NVME) | all five validated (`400 BadDigest` on mismatch) |
| `If-None-Match: *` on PUT? `If-Match` on PUT? | `If-None-Match: *` yes (412); `If-Match` **no**, a mismatched ETag is accepted |
| Unsigned `GET /` returns (status)? | 200, empty `ListAllMyBucketsResult` owned by `Anonymous`. Also: the signing region is not enforced (a `nowhere-1` scope is accepted). TLS: self-signed factory cert (CN vms.example.com) that does not cover the endpoint name; POC-2 runs with verification disabled until a proper cert is installed. Full table: docs/reference/backend-compat.md |
| GetObjectAttributes, HeadObject/GetObject `--part-number` known issues (Jira IDs) | unknown |
| CORS, presigned URL, multipart edge cases already documented | unknown |

## Clients that must work

| Client | Version(s) | Notes (checksum defaults, chunked signing, quirks) |
|---|---|---|
| aws-cli | unknown | unknown |
| boto3 / botocore | unknown | unknown |
| aws-sdk-go-v2 | unknown | unknown |
| s5cmd / rclone / mc | unknown | unknown |
| Java SDK v2 (CRT?) | unknown | unknown |
| PyTorch / NVIDIA data loaders | unknown | unknown |
| Hadoop S3A / Spark | unknown | unknown |
| Other | unknown | unknown |

## Workload shape (for bench and defaults)

| Item | Answer |
|---|---|
| Object size distribution (p50 / p90 / p99, or histogram) | unknown |
| Op mix (GET / PUT / HEAD / LIST / DELETE %) | unknown |
| Concurrency: connections per client, clients per proxy | unknown |
| Target throughput per proxy node (Gbit/s) and node CPU count | unknown |
| Source of these numbers (access log / pcap / estimate) | unknown |

## Scale bounds (become cardinality and pool limits)

| Item | Answer |
|---|---|
| Buckets (now / 2 years) | unknown |
| Tenants / access keys | unknown |
| Endpoints per cluster | unknown |
| Max objects per bucket; typical listing page use | unknown |

## Topology and trust

| Item | Answer |
|---|---|
| Wildcard domain clients use (e.g. `*.s3.example.net`) | `*.shunt.example.com` (cert also covers the bare name for path-style); Makefile `DOMAIN` |
| Cert source: bare metal (internal CA path / ACME) and k8s (cert-manager issuer) | POC: self-signed via test/e2e/gen-cert.sh; production unknown |
| What sits in front: IPVS/keepalived, MetalLB, HAProxy, cloud NLB — preserves client IP? PROXY protocol? | unknown |
| Clients address VIPs or DNS names? | unknown |
| Which sites allow `scheme: http` upstream, and why | unknown |
| Client mTLS required anywhere? | unknown |

## Tenancy and metering

| Item | Answer |
|---|---|
| Where access keys live today (VAST VMS / Keycloak / file) | unknown |
| Same keys on every cluster? | unknown |
| Tenant identity = access key, bucket, or cert? | unknown |
| Usage record fields required for billing; granularity; destination | unknown |

## Conventions

| Item | Answer |
|---|---|
| CLI: stdlib `flag` or cobra | cobra (decided 2026-09-14; the one runtime dependency beyond YAML, see docs/deps.md) |
| Tests: stdlib `testing` or testify | stdlib `testing` |
| Error style (wrap with `%w`; sentinel errors; where to log) | wrap with `%w`; sentinel errors per package; validation errors are typed (`config.Error{Key, Msg}`) and joined with `errors.Join`; log at the edge (cmd/, handler), not in libraries |
| Style-reference repo of yours | unknown |
| Anything CodeRabbit / Semgrep should enforce beyond defaults | unknown |

## Reuse

| Item | Path | License / relicense note |
|---|---|---|
| Checksum / part-number / GetObjectAttributes probe scripts | unknown | unknown |
| s3slower (slow-ring model) | unknown | unknown |
| tcpx (TCP_INFO sampling) | unknown | unknown |
| vamoose (conditional-write lease pattern) | unknown | AGPL — your own code may be relicensed by you; outside contributions may not |
