# ADR-0013: a conditional write is judged by the whole bucket, not by one cluster

Status: accepted (POC-6 item 1, 2026-09-17). Amends ADR-0004 (migration races) and docs/DESIGN.md §2.5 (the routing table). Source: POC-6 item 1.

## Context

While a bucket is `RAMPING` or `MIGRATING` its objects are split across two clusters: some keys are on the source, some on the new primary, and a write goes to exactly one of them (§2.5). A backend can only judge a precondition against the copy **it** holds, and until now shunt relayed the client's `If-None-Match` and `If-Match` headers untouched. That answers the wrong question twice:

- **`If-None-Match: *`** (create-once, the lock idiom clients build leader election and "write once" on) was evaluated on a cluster that did not hold the key, so shunt answered **200 and created a second copy** of an object that already existed on the other cluster. The client believes it created the object; a listing merges both copies; after cutover one of them wins.
- **`If-Match: <etag>`** (update-if-current, read-modify-write) was evaluated on a cluster that did not hold the key, so shunt answered **412** although the client's version was the current one. Retrying does not help: the object stays on the other side until the mover copies it.

Both are silent: no error, no metric, no log. The proxy's own tests could not see them because every fake backend held every key.

`CUTOVER` does not have the problem (the source is a subset of the primary by then, ADR-0004 amendment), and `ACTIVE` has one cluster.

## Decision

**In `RAMPING` and `MIGRATING`, shunt evaluates a write's preconditions against both clusters before the write is sent,** and on any cluster whose profile says it ignores `If-None-Match: *` shunt evaluates create-once itself. It applies to the ops that carry them on an object: `PutObject` and `CompleteMultipartUpload`.

- **`If-None-Match`**: the other cluster is asked with a signed `HEAD`. If it holds a matching object (`*`, or a listed ETag), the write is **refused with 412 and never sent upstream**. Otherwise the header goes upstream unchanged and the destination judges its own copy.
- **`If-Match`**: if the destination holds the key, its backend judges as before. If it does not and the other cluster holds a matching version, shunt has made the client's check itself: the `If-Match` header is **dropped** and the write goes out as **`If-None-Match: *`** instead, so a create that races it on the destination still loses. If neither side holds a matching version, it is **412**.
- **A check that cannot be made fails closed**: a `HEAD` that errors or answers anything but 200/404 answers the client `503 ServiceUnavailable`. Guessing either way breaks the promise the header is.
- **A cluster that ignores `If-None-Match: *`** (`capabilities.conditional_write: false`, Garage 2.3.0) cannot judge create-once at all, so shunt judges it: a `HEAD` of the destination, and 412 if the object is there. **This one applies in every state**, not only mid-migration: a backend that answers 200 to a create-once it did not honor breaks the idiom whether or not a migration is running, and shunt knows from the profile that it will. The window between that `HEAD` and the write is the mover's HEAD-then-commit window (ADR-0004), and it is the best a backend without the header allows.
- **An `If-Match` converted onto such a cluster** cannot carry the `If-None-Match: *` guard either, so the write goes out unguarded and shunt logs a WARN naming the cluster. The client's condition was still checked; what is lost is the protection against a create racing the same key there.

**Cost:** one `HEAD` per conditional write (two when `If-Match` finds nothing on the destination). Unconditional writes are untouched; so is a conditional write to a single-cluster bucket on a backend that judges the header itself (MinIO, VAST, AWS), which pays nothing.

## Consequences

- **The window that remains** is between shunt's `HEAD` and the backend's write: a create-once that shunt allowed can still race a create of the same key **on the other cluster**, which no single-cluster check can see either. It is narrower than a request and is the price of two clusters holding one bucket; the alternative, a lock across clusters, is per-object state (CLAUDE.md).
- **Clients see S3's semantics through a migration**, which is what "no client change" has to mean for a client that uses conditional writes.
- **The property test grew conditional clients.** They assert create-once and update-if-current against the model, outside the mover's copy-in-flight window (ADR-0004 race 1 can put an object back for a moment, so a condition there has no single right answer). Without this change a 15-second run reports 18 violations of the two new invariants.
- **`internal/proxy/conditional_test.go`** covers each case on both states: create-once refused from the other cluster's copy, create-once allowed when nobody holds the key, update applied from the source's current version (with the converted header asserted upstream), stale ETags refused, a key neither side holds refused, a target without conditional writes taking the unguarded write with the WARN, and an `ACTIVE` bucket left untouched with no extra round trip.
- **A backend gap became shunt's answer.** Garage 2.3.0 ignores `If-None-Match: *` (docs/reference/backend-compat.md), and until now a create-once against it answered 200 having overwritten. The property test's guarded variant, whose target ignores the header, found this in the phase after cutover: 5,682 violations in two minutes. The variant now also records the capability its fake actually has, which is what a probe would record.
- **Not covered:** `x-amz-copy-source-if-*` on CopyObject, because a cross-cluster copy is refused today (POC-6 item 2 changes that, and inherits this rule).
