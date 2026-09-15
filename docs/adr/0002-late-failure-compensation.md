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
- **POC gap (POC-2 cut):** the POC implements compensation as log-and-alert only. No compensating delete is issued. The gap is recorded here and closed in P2.
- A signed-chunk failure on a non-final chunk aborts the stream before the backend commits the object; only the final-chunk and trailer cases need compensation.
