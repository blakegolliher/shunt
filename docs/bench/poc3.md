# POC-3 bench: directory routing, response rewriting, and the uploadId codec

Question: what do the POC-3 additions cost on the data path? Every request now resolves `(tenant, bucket)` through a directory snapshot, renames the bucket in the upstream path, and, for the operations that echo a name, streams the response through the XML rewriter (ADR-0006).

Method: `make bench-compare`, Go micro-benchmarks, `-benchtime 1s -count 6`, median reported, compared with `benchstat` against `test/bench/baseline.txt` (the POC-2 baseline). The e2e harness (`make bench-e2e`) was **not** re-run for POC-3: the byte path for GET and PUT is unchanged (the rewriter never touches an object body), and what POC-3 adds is per-request work that the micro-benchmarks isolate. The gate is the >5 % rule from docs/bench/README.md.

## Disclosure

| Item | Value |
|---|---|
| Host | AMD EPYC 7402P 24-Core Processor, 16 vCPUs, 62 GB RAM |
| Kernel | 5.14.0-570.17.1.el9_6.x86_64 |
| NIC | loopback: client, shunt, and backends on one host |
| Go | go1.27.1 |
| shunt | poc-2 plus the POC-3 working tree |
| Baseline | `test/bench/baseline.txt`, recorded at POC-2 |

## Result against the POC-2 baseline

`internal/proxy`, the whole package, geomean over its benchmarks:

| Measure | vs POC-2 baseline |
|---|---|
| Time per op | −3.50 % |
| Allocations per op | +0.82 % |
| Throughput (B/s) | +4.65 % |

Inside the gate, and slightly faster rather than slower. The one number that moved for a reason worth stating is `internal/config`, whose geomean dropped about 42 %: its benchmark parses the sample configs, and the placements moved out of them into the directory file (ADR-0005). It measures a smaller document now, not faster code. `internal/directory` has no baseline: the package is new.

## Medians

| Benchmark | Median | Allocs/op | Note |
|---|---|---|---|
| `proxy/SmallGET` (passthrough, 4 KiB) | 409 µs | 107 | unchanged path |
| `proxy/PUT1MiB` (passthrough) | 2.21 ms, 475 MB/s | 179 | unchanged path |
| `proxy/ResignSmallGET` | 561 µs | 240 | verify, route, re-sign |
| `proxy/ResignSignedChunkPUT1MiB` | 5.25 ms, 200 MB/s | 4 733 | per-chunk HMAC dominates |
| `directory/Lookup` (10 000 placements) | 43.0 ns | **0** | one map read per request |
| `directory/Create` | 11.8 ms | 7 367 | fsync, rename, fsync of the directory: write path only |
| `migrate/DecodeUploadID` | 15.2 ns | **0** | per uploadId in a request |
| `xmlrw/Listing1000` (1 000-key ListObjectsV2, ~120 KiB) | 665 ns | **0** | the rule matches early and the scanner stops |
| `xmlrw/Uploads1000` (1 000 uploads, every id rewritten) | 842 µs, 203 MB/s | **0** | whole document scanned |
| `admin/Healthz` | 1.91 µs | 10 | unchanged |

## Reading it

- **Routing is free.** A directory lookup is 43 ns with no allocation, against a resign request of roughly 560 µs: about one ten-thousandth of it. The snapshot is an immutable map behind an atomic pointer, so a reload never blocks a request.
- **The rewriter is paid only where it applies.** It never sees an object body: `GetObject` and `PutObject` are untouched. On a listing it stops scanning once the last rule has matched, which is why a 120 KiB listing costs 665 ns. The worst case in the table, a listing where every element is rewritten, runs at about 200 MB/s of response body, and response bodies are small compared with object bodies.
- **Nothing allocates per request** in the three new packages: the scanner holds one pooled scratch buffer per rewritten response, and the codec and the lookup allocate nothing at all.
- **`directory/Create` is slow on purpose.** It rewrites the whole file, fsyncs it, renames it, and fsyncs the directory, so a bucket creation costs about 12 ms. It is a control-plane write, not a data-path operation, and P3c replaces the file backend with Postgres.

## Carried forward

The `Healthz` variance recorded in docs/bench/poc2.md (about 7 % between runs of unchanged code, attributed to binary layout) is still present and still not worth chasing.
