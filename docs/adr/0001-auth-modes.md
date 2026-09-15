# ADR-0001: Two auth modes, passthrough and resign

Status: accepted (POC-0). Source: docs/DESIGN.md §2.2, decision 1.

## Context

Shunt owns the client-facing endpoint. A client signs every request with SigV4 over the Host it addressed, so the proxy must either keep that Host intact and let the backend verify, or verify the signature itself and produce a new one for the backend. The choice decides whether a bucket can move between credential domains.

## Decision

Two modes, selected by `auth.mode`:

- **passthrough**: forward with `Host` preserved; the backend verifies. Zero crypto in the proxy. Requires the backend to accept the proxy's hostname as a virtual-host domain and share the credential database. Single-cluster and dev mode only. Cannot cross credential domains, cannot re-route presigned URLs across clusters.
- **resign**: parse `Authorization: AWS4-HMAC-SHA256 …` or presigned query parameters, look up the secret in `CredentialStore`, recompute and compare the signature, check `x-amz-date` skew (±15 min), then sign the upstream request with the target cluster's credentials and region. Mandatory for the global endpoint. Presigned URLs are verified with the client's secret and re-issued upstream as header-signed requests.

Payload handling in resign mode follows the table in §2.2: `UNSIGNED-PAYLOAD` and hex SHA-256 forward unchanged; `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` is verified chunk by chunk and forwarded decoded as `UNSIGNED-PAYLOAD` with `Content-Length = x-amz-decoded-content-length`; the trailer variant is forwarded as `STREAMING-UNSIGNED-PAYLOAD-TRAILER`; `STREAMING-UNSIGNED-PAYLOAD-TRAILER` passes through.

SigV4A (`AWS4-ECDSA-P256-SHA256`) is rejected with a clear error. STS session tokens are out of v1; the seam exists in `CredentialStore`.

## Amendments (POC-2, 2026-09-15)

- **Upstream is always path-style in resign mode.** Host is the cluster endpoint; a virtual-host client request becomes `/<bucket><path>` upstream (docs/DESIGN.md §11: "reaches vast01 by its own name, path-style, and re-signs"). Backends therefore need no wildcard domain for shunt, and the `<Location>` echo of shunt's port disappears. Visible consequence, found by s3diff against Garage: error responses to a virtual-host request carry the path-style `<Resource>` (`/bucket/key` instead of `/key`), and a HEAD error's `Content-Length` changes with it. Clients do not act on `<Resource>`; POC-3 rewrites every XML echo of the bucket name and closes this.
- **The region in the client's credential scope is accepted as-is.** kSigning is derived from the scope the client used; shunt is a global endpoint and the client's idea of "region" is whatever its config says. The upstream request is signed with the cluster's configured region. The e2e Garage cluster carries `region: garage` while clients sign for `us-east-1`; every resign s3diff run is the live test of "client scope region accepted, cluster region used upstream".
- **`x-amz-security-token` is rejected** with `400 InvalidToken` on any client request. STS is v2; the seam is `CredentialStore`.
- **SigV4A** (`AWS4-ECDSA-P256-SHA256`, header or presigned, and the ECDSA streaming payload types) is rejected with `501 NotImplemented`.
- **Verification happens before the upstream round trip.** A bad client signature is answered by shunt with zero body bytes read, which keeps the `Expect: 100-continue` property from POC-1 for every payload mode.
- **Payload table implementation:** the signed-chunk decoder is versitygw's (lifted, `internal/sigv4/chunked`); the signed-trailer row is re-framed as `STREAMING-UNSIGNED-PAYLOAD-TRAILER` with a precomputed `Content-Length` (never chunked transfer encoding) when the cluster's capability profile allows it, else decoded to `UNSIGNED-PAYLOAD` with shunt verifying the trailer checksum. Upstream header rewrite: strip `Authorization`, `x-amz-date`, `x-amz-content-sha256`, `x-amz-security-token`; when decoding, also strip only the `aws-chunked` token from `Content-Encoding`, plus `x-amz-decoded-content-length` and `x-amz-trailer`; keep and sign every other `x-amz-*` header (`internal/proxy/auth.go`, tested per row in `resign_test.go`).
- **Credential store:** static YAML with inline secrets allowed (the file is the secret store, mode 0600 enforced); encrypted at rest is P3c; no hot reload in the POC.

## Consequences

- One canonicalization implementation serves both verify and sign; the AWS SDK signer is the test oracle only (docs/reuse.md).
- Resign adds one HMAC chain per request and, for signed chunks, one per chunk. POC-2 measures the overhead against the POC-1 passthrough baseline.
- Backends that do not enforce checks the proxy delegates to them (sha256 mismatch, trailer checksum) force compensation: see ADR-0002.
- M1 (single cluster) ships passthrough; every phase from P2 on runs resign.
