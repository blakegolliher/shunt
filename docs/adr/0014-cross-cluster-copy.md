# ADR-0014: shunt streams a copy no backend can make

Status: accepted (POC-6 item 2, 2026-09-17). Overrides docs/DESIGN.md §9 item 14 and the 501 of ADR-0006's consequences. Inherits ADR-0013 (conditional writes) and ADR-0004 (the mover's part-layout rule). Source: POC-6 item 2.

## Context

`CopyObject` is server-side: the client names a source, and the **backend** reads it and writes the destination. That works only when one backend can see both. Through shunt two cases break it:

- **The two buckets are on different clusters.** The destination's backend cannot read the source's.
- **The source bucket is mid-migration.** Its objects are split across two clusters, so no single backend can read all of them.

Both answered `501 NotImplemented`. The cost is not theoretical: **S3A commits by renaming**, which is a copy followed by a delete, so a Hadoop or Spark job writing through shunt failed at commit time for the whole length of a ramp — the period when the operator least wants a surprise. `UploadPartCopy`, which clients use for large renames, failed the same way.

## Decision

**shunt performs the copy itself when no backend can.** It reads the object from the cluster that holds it and writes it to the destination cluster, streaming, and answers the client with the `CopyObjectResult` (or `CopyPartResult`) a backend would have sent.

- **Same cluster, settled bucket:** unchanged. The copy-source header is rewritten to the backend bucket name and the backend does the copy, which is faster and is what it is for.
- **Source selection:** the source bucket's placement decides which cluster holds the key, by the §2.5 routing table. A mid-migration source is read primary-first with a fallback to the source cluster on 404, the same rule a `GET` follows, and the fallback moves the whole read to that cluster, bucket name included.
- **A multipart source keeps its part layout.** Its part count comes from the `-N` ETag, each part is read with `?partNumber=`, and the destination is written as a multipart upload with the same parts, so the ETag's shape survives the copy. This is the mover's rule (ADR-0004), for the same reason: clients cache ETags and pass them to `If-Match`.
- **A single object over 5 GiB is refused** with `EntityTooLarge` naming multipart copy, because one PUT cannot carry it. That is S3's own limit, not shunt's.
- **Conditions on the source** (`x-amz-copy-source-if-*`) are evaluated by the cluster that holds the source, as S3 evaluates them on the source object: a failed condition is `412` and nothing is written.
- **Conditions on the destination** (`If-None-Match`, `If-Match` on the copy) go through ADR-0013's cross-cluster rules, so create-once and update-if-current mean the same on a copy as on a `PUT`.
- **Metadata** follows `x-amz-metadata-directive`: `COPY` (the default) carries the source object's content headers and `x-amz-meta-*`; `REPLACE` takes the client's.
- **Nothing is buffered.** Each body streams from one response into the next request, so a copy costs the bytes in flight, not the object (CLAUDE.md: a body buffer needs an ADR; this is not one).
- **A failed multipart copy aborts its own upload** on the destination, so a half-copied object never appears.

## Consequences

- **S3A's rename works during a ramp**, which is the case that made this urgent, and `UploadPartCopy` works across clusters for the large files it exists for.
- **A cross-cluster copy costs the bytes twice on the network** (down to shunt, up to the destination) where a backend-side copy costs none. It is the price of the object not being where the copy was asked for; the alternative is the 501 that was there before.
- **The 501s are gone** from `internal/proxy/resign.go`: both "copying between buckets on different clusters" and "copying from a bucket that is being migrated". DESIGN §9 item 14 and the STATUS gap are updated.
- **Tagging is not carried.** `x-amz-tagging-directive: COPY` would need a `GetObjectTagging` and a `PutObjectTagging` around the copy; today tags are left behind on a cross-cluster copy. Written down here rather than discovered later.
- **Versioned sources are out of scope** as everywhere else: a `versionId` in the copy source is passed through to the cluster that holds it, and migrating a versioned bucket is refused anyway (DESIGN §9 item 4).
- **Tested** in `internal/proxy/crosscopy_test.go`: a cross-cluster copy, a missing source, source conditions honoured and refused, an S3A rename across a ramp, part layout preserved for a three-part object, and `UploadPartCopy` with a byte range. Live on the e2e Garage and MinIO: a 1 MiB copy, a 20 MiB copy that aws-cli turned into a multipart copy and that arrived with the **same `…-4` ETag as the source**, and a rename inside a ramping bucket.
