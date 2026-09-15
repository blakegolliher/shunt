# ADR-0002: Late failures are compensated, never buffered away

Status: accepted (POC-0). Source: docs/DESIGN.md §2.1, §2.2.

## Context

Bodies stream through `io.CopyBuffer`; shunt never holds a request or response body. Some checks can therefore only complete after the upstream write has finished: an `x-amz-content-sha256` mismatch against a hex digest, a trailer checksum mismatch, a signed-chunk signature failure on the final chunk. Buffering the body to check first would break the streaming rule and require an ADR plus a benchmark per CLAUDE.md.

## Decision

Delegate the check to the backend whenever the backend enforces it, and **compensate** when it does not:

1. Let the upstream write complete.
2. On mismatch, issue `DeleteObject` (or `AbortMultipartUpload` / drop the part) against the object just written.
3. Return the S3 error the client expected (`XAmzContentSHA256Mismatch`, `BadDigest`, `SignatureDoesNotMatch`).

Phase 2 probes each backend (`shunt probe`) for which checks it enforces itself and records the result in the cluster's capability profile, so compensation is the exception, not the rule.

## Consequences

- On a versioned bucket the compensating delete creates a delete marker rather than erasing the version. The bad version remains readable by version ID. This is accepted for v1 and documented for operators.
- There is a window between upstream completion and the compensating delete in which a concurrent reader can observe the rejected object.
- **POC gap (POC-2 cut):** the POC implements compensation as log-and-alert only. No compensating delete is issued. The gap is recorded here and closed in P2. Implemented in POC-2: `shunt_compensation_total{reason,outcome="logged"}` plus an error-level log line with request id, bucket, op, and upstream. Two reasons exist: `sha256`, when the cluster's capability profile says `enforces_sha256: false` and shunt's own hash of the streamed body disagrees with the client's hex `x-amz-content-sha256` after the upstream write succeeded; and `trailer`, when a signed-chunk body was fully delivered upstream before the decoder rejected its trailer or a chunk signature. The client receives the backend's success response in the `sha256` case (log-and-alert cannot undo it) and shunt's own 4xx in the `trailer` case. Probe results (docs/reference/backend-compat.md): Garage 2.3.0, MinIO, and VAST 5.x all reject a wrong hex `x-amz-content-sha256` and all accept and validate unsigned trailers, so with their capability profiles at the defaults the log-and-alert path is never reached on the POC backends; it is exercised in unit tests with a non-enforcing profile. Garage does not verify *signed* trailers, which does not matter to shunt: signed-chunk bodies are always verified by shunt's decoder before anything reaches the backend.
- A signed-chunk failure on a non-final chunk aborts the stream before the backend commits the object; only the final-chunk and trailer cases need compensation.
