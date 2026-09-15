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
| Dev box distro + kernel (`uname -r`) | |
| Go version (`go version`) — pin it in go.mod | |
| Container runtime on dev box (docker / podman / none) | |
| Root or CAP_BPF available on dev box? BTF present (`ls /sys/kernel/btf/vmlinux`)? | |
| Egress for `go get` / module proxy? Any GOPROXY/GONOSUMDB settings? | |
| GitHub org/repo for the project; CI runner type (hosted / self-hosted) | |
| Internal registry (Harbor URL, project name); env var names for push creds | |
| RKE2 cluster: k8s version, CNI, LB implementation (MetalLB / other), cert-manager issuer name, Prometheus operator present? | |

## Backend mix on day one

| Item | Answer |
|---|---|
| Backend types in the first deployment (VAST / MinIO / AWS S3 / other) and how many of each | |
| AWS account and scratch bucket available for CI? env var names; region | |
| MinIO version and deployment shape (single node / distributed) for CI | |
| Postgres: managed (RDS etc.) or self-hosted (Patroni / CloudNativePG)? version; who runs HA | |
| Tenant model: one tenant per customer? per team? how tenants get created and keyed | |
| Backend bucket naming rule you want (e.g. `<tenant>-<hash>-<bucket>`) and any length/charset constraints | |

## Reference backend (the real target)

| Item | Answer |
|---|---|
| VAST lab endpoint(s), scheme, port | |
| Scratch bucket name Claude Code may create/delete objects in | |
| Env var names holding access key / secret | |
| VAST version; virtual-host style domain configured? (which) | |
| Second cluster available for multi-cluster tests later? | |
| Garage / versitygw already installed locally? paths/ports | |

## Known VAST compatibility findings (pre-seed backend-compat.md)

| Behavior | Known answer |
|---|---|
| Enforces `x-amz-content-sha256` mismatch? | |
| Accepts `STREAMING-UNSIGNED-PAYLOAD-TRAILER`? | |
| Checksum headers honored (CRC32 / CRC32C / SHA1 / SHA256 / CRC64NVME) | |
| `If-None-Match: *` on PUT? `If-Match` on PUT? | |
| Unsigned `GET /` returns (status)? | |
| GetObjectAttributes, HeadObject/GetObject `--part-number` known issues (Jira IDs) | |
| CORS, presigned URL, multipart edge cases already documented | |

## Clients that must work

| Client | Version(s) | Notes (checksum defaults, chunked signing, quirks) |
|---|---|---|
| aws-cli | | |
| boto3 / botocore | | |
| aws-sdk-go-v2 | | |
| s5cmd / rclone / mc | | |
| Java SDK v2 (CRT?) | | |
| PyTorch / NVIDIA data loaders | | |
| Hadoop S3A / Spark | | |
| Other | | |

## Workload shape (for bench and defaults)

| Item | Answer |
|---|---|
| Object size distribution (p50 / p90 / p99, or histogram) | |
| Op mix (GET / PUT / HEAD / LIST / DELETE %) | |
| Concurrency: connections per client, clients per proxy | |
| Target throughput per proxy node (Gbit/s) and node CPU count | |
| Source of these numbers (access log / pcap / estimate) | |

## Scale bounds (become cardinality and pool limits)

| Item | Answer |
|---|---|
| Buckets (now / 2 years) | |
| Tenants / access keys | |
| Endpoints per cluster | |
| Max objects per bucket; typical listing page use | |

## Topology and trust

| Item | Answer |
|---|---|
| Wildcard domain clients use (e.g. `*.s3.example.net`) | |
| Cert source: bare metal (internal CA path / ACME) and k8s (cert-manager issuer) | |
| What sits in front: IPVS/keepalived, MetalLB, HAProxy, cloud NLB — preserves client IP? PROXY protocol? | |
| Clients address VIPs or DNS names? | |
| Which sites allow `scheme: http` upstream, and why | |
| Client mTLS required anywhere? | |

## Tenancy and metering

| Item | Answer |
|---|---|
| Where access keys live today (VAST VMS / Keycloak / file) | |
| Same keys on every cluster? | |
| Tenant identity = access key, bucket, or cert? | |
| Usage record fields required for billing; granularity; destination | |

## Conventions

| Item | Answer |
|---|---|
| CLI: stdlib `flag` or cobra | |
| Tests: stdlib `testing` or testify | |
| Error style (wrap with `%w`; sentinel errors; where to log) | |
| Style-reference repo of yours | |
| Anything CodeRabbit / Semgrep should enforce beyond defaults | |

## Reuse

| Item | Path | License / relicense note |
|---|---|---|
| Checksum / part-number / GetObjectAttributes probe scripts | | |
| s3slower (slow-ring model) | | |
| tcpx (TCP_INFO sampling) | | |
| vamoose (conditional-write lease pattern) | | AGPL — your own code may be relicensed by you; outside contributions may not |
