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

## Consequences

- One canonicalization implementation serves both verify and sign; the AWS SDK signer is the test oracle only (docs/reuse.md).
- Resign adds one HMAC chain per request and, for signed chunks, one per chunk. POC-2 measures the overhead against the POC-1 passthrough baseline.
- Backends that do not enforce checks the proxy delegates to them (sha256 mismatch, trailer checksum) force compensation: see ADR-0002.
- M1 (single cluster) ships passthrough; every phase from P2 on runs resign.
