# ADR-0007: The bodies shunt holds in memory, and the cap on each

Status: accepted (POC close-out, 2026-09-16). Source: CLAUDE.md "any buffering of a request or response body requires an ADR and a benchmark"; docs/DESIGN.md §2.1, §2.5; ADR-0004; ADR-0006. Found by the G1 simplicity review: all three buffers below shipped in POC-3 and POC-4 without this record.

## Context

shunt streams bodies (§2.1). The request path it proxies never holds a body end to end, and the ADR-0006 rewriter holds at most one element's text. Three paths shunt added for migration and for requests it originates cannot stream, because each has to use a body twice or build one from two sources:

1. **A migration delete with a body** (`DeleteObjects`, `POST ?delete`) goes to both clusters (ADR-0004 race 1). The client sends the body once.
2. **A merged listing** (§2.5) reads one page from each cluster, merges the two sorted streams, and renders one page for the client, with a composite continuation token that depends on where the merge stopped.
3. **A backend error on a request shunt originates** (CreateBucket and DeleteBucket through the directory) is read so its backend names can be rewritten before it is relayed.

## Decision

Each is buffered, with a hard cap, and nothing else is.

| Path | What is held | Cap | Beyond the cap | Code |
|---|---|---|---|---|
| Dual delete body | the client's `DeleteObjects` XML | 1 MiB (`maxDeleteBody`). S3 caps the request at 1,000 keys, about 0.1–0.3 MiB | the request is refused, before anything is deleted | `internal/proxy/auth.go`, `resign.go` |
| Merged listing, input | one upstream `ListObjectsV2` page per side | 8 MiB per side. A 1,000-key page with 1 KiB keys is about 1.2 MiB | the page is truncated at the cap and fails to parse; the client gets an error, not a partial listing | `internal/proxy/merge.go` |
| Merged listing, output | the rendered page, then written with an exact `Content-Length` | bounded by `max-keys` (≤ 1,000) | — | `merge.go` `writeListing` |
| Relayed backend error | the backend's error XML on a shunt-originated request | 64 KiB | the rest is dropped; an S3 error body is under 1 KiB | `internal/proxy/buckets.go` |

Memory for all four is bounded by the page size or by S3's own request limits, never by bucket size or object size. None of them is on the path of an object's bytes.

## Benchmarks

`go test ./internal/proxy -bench 'MigratingDeleteObjects1000Keys|MergedListingPage1000Keys' -benchmem`, on the dev box (AMD EPYC 7402P, 16 vCPUs, go1.27.1), three runs each. Both run through the in-process test rig, so they include the fake backends' own work: signature verification, and generating 1,000 keys per side. They are an upper bound on shunt's cost, not a measurement of it alone.

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkMigratingDeleteObjects1000Keys` (61 KiB body, both clusters) | 2.7–3.3 ms | 650–708 KiB | 906–926 |
| `BenchmarkMergedListingPage1000Keys` (1,000 keys each side, one merged page) | 45–48 ms | 21.2 MiB | 146,300 |
| `BenchmarkSmallGET`, for scale (streamed, no buffer) | 0.39–0.42 ms | 27–30 KiB | 107 |

The merged listing is the expensive one, as docs/bench/poc4.md measured live (86 ms per page over Garage and MinIO). It exists only while a bucket is `MIGRATING`.

## Consequences

- A migration makes listings slower and heavier per page, never unbounded. A bucket that stays `MIGRATING` for long under heavy listing traffic pays this on every page, which is one more reason `CUTOVER` should follow convergence promptly.
- The P5 listing-merge work (docs/DESIGN.md §7 P5 item 2: streaming XML output, bounded memory on two 1M-key buckets) replaces the input and output buffers with a streaming merge and its own benchmark. Until then this ADR is the record.
- Any new body buffer needs its own row here, or a new ADR, and a benchmark.
