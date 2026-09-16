# ADR-0006: Rewriting the responses that echo a backend name, and the uploadId codec

Status: accepted (POC-3). Source: docs/DESIGN.md §2.3, §2.4, §9 item 11; docs/POC.md POC-3; docs/STATUS.md carried gaps from POC-2.

## Context

In resign mode a client's bucket `data` may live on a cluster as `acme-1111-data`, and the request goes upstream path-style to the cluster's endpoint. Backends echo what they were addressed by: `<Name>` in a listing, `<Bucket>` and `<UploadId>` in multipart responses, `<Location>` in CompleteMultipartUpload (which MinIO builds from the request Host, verified in POC-2), and `<Resource>`, `<BucketName>`, and free text in error bodies. Relayed unchanged, they hand the client a bucket name it cannot use and an address it was never meant to have. Multipart uploads add a second problem: an uploadId is meaningful only on the cluster that issued it, and a bucket's placement can change while an upload is in flight.

Bodies are streamed and never buffered (§2.1), so the rewrite has to happen while the bytes move.

## Decision

**A streaming XML rewriter, `internal/s3/xmlrw`.** An `io.Writer` between the pooled copy buffer and the response writer, it is a byte-level scanner rather than a parser: it tracks element nesting only deep enough to recognize the elements its table names, holds that element's character data (at most 64 KiB), asks an `Editor` for the replacement, and forwards every other byte as it arrives.

- **Identity is the primary invariant**, and the primary fuzz property: with an editor that changes nothing, the bytes out equal the bytes in, however the input is split across writes. Anything unexpected degrades to identity, never to an error: mixed content, CDATA or comments inside a target element, text past the cap, a non-XML body, an unknown root. Each such case is counted and logged, because it could mean a name reached a client.
- **The table** (the rewrite inventory) covers `ListBucketResult`, `ListVersionsResult`, `ListMultipartUploadsResult`, `InitiateMultipartUploadResult`, `ListPartsResult`, `CompleteMultipartUploadResult`, and `Error`. It applies to those operations' 2xx bodies and to every response with a status of 300 or more.
- **Framing (amendment 2):** the rewritten body goes into a 64 KiB scratch buffer. A body that fits is sent with an exact `Content-Length`; only a larger one spills to chunked. `CompleteMultipartUpload`, `CreateBucket`, and error bodies therefore keep their `Content-Length`.
- **ADR-0003 still holds:** a short upstream body aborts the response. The rule that tolerates a client-side write error after a complete body now compares bytes read from upstream, not bytes written.
- **`kill_switches.xml_rewrite_disable`** (default false, so rewriting is on) turns it off; `shunt serve` then warns, naming the clusters whose `<Location>` will leak. It is a kill switch, not a feature flag: it exists to get past a bug in the rewriter, and what it costs is stated on the switch (`internal/config/features.go`).

**The uploadId codec, `internal/migrate`** (§2.4): a response's uploadId becomes `<clusterID>~<backendUploadId>`, and every incoming `uploadId=` and `upload-id-marker=` is split on its first `~`. The prefix is the cluster's **opaque id**, the first 6 hex characters of SHA-256 over the cluster name, not the name itself: the point of POC-3 is that a client cannot tell which cluster serves it. A value whose prefix is not six hex characters is treated as unprefixed and routed by the bucket's placement, so uploads started before the codec existed still complete. A prefix naming a cluster that does not hold the bucket is `NoSuchUpload`, answered without an upstream call.

**What is not rewritten** (the user's decision of 2026-09-15): vendor identity. `Server`, `X-Minio-*`, `X-Vast-*`, MinIO's `<HostId>`, Garage's `<Region>`, GetBucketLocation, and backend owner ids pass through. A client can therefore tell which **kind** of backend served it, and keeps the vendor's trace ids for support. What shunt hides is which cluster and which bucket name, stated once in docs/reference/backend-compat.md.

**Operations that are refused** with 501 rather than rewritten: bucket logging, replication, inventory, analytics, and notification. Their configurations name buckets as ARNs and carry per-backend account ids; relaying either direction would leak a name or point the backend at a bucket the client does not know. **Bucket policy passes through unrewritten** (amendment 3): the body is JSON, not XML, and its ARNs may name backend buckets, which backend-compat.md records.

## Consequences

- One scan of the response bytes for the affected operations: about 850 ns for a 1000-key listing whose only rule matches early, ~1 ms for 1000 multipart uploads where every id is rewritten, at 0 allocations per document.
- At most 64 KiB is held per rewritten response, from a pool, and no body is buffered end to end.
- Backend uploadId formats still fingerprint the backend type, as vendor headers already do.
- A backend error message shape that no fixture covers could still carry a backend name in free text. The s3diff mixed-mode leak assertion is the backstop, and the rewriter counts every element it could not rewrite.
- `CreateBucket` ignores the client's `CreateBucketConfiguration`: the directory, not the client, decides where a bucket lives, so a requested region or tag set is dropped.
- Cross-cluster `x-amz-copy-source` is refused with 501 (carried gap in docs/STATUS.md): the backend cannot read the other cluster, and whether shunt streams the copy itself is a later phase's decision.

## Amendment (POC-5, 2026-09-16): a flag-gated exception, the debug route header

The walkthrough has to show an operator that writes split ~50/50 at a ratio of 0.5, and that reads succeed on both sides. From the client's side, that means knowing which cluster served each request, which is exactly what this ADR hides.

`features.debug_route_header` (default off; removal criterion: P4 traces carry the route of every request) opens a narrow exception. With the flag on, a request that carries `X-Shunt-Debug: 1` gets a response header naming its route:
- `X-Shunt-Route: <primary|source> <cluster name>` for a relayed request, set after any fallback read, so it names the side that answered;
- `X-Shunt-Route: merged <primary>+<source>` for a merged listing.

Without the flag, or without the request header, nothing is added. `X-Shunt-Debug` is never forwarded upstream. `serve` warns at startup that any client sending the header learns cluster names. The backend bucket name, the endpoint and the uploadId stay hidden either way.

It is for labs and for `shunt verify --debug-route`. s3diff runs with the flag off, so transparency is still measured without it.
