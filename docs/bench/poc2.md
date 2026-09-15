# POC-2 bench: resign overhead

Question: what does verify-and-resign cost on top of the POC-1 passthrough proxy? Same method, harness, matrix, and host as docs/bench/poc1.md, with shunt in resign mode: the client signs with a shunt-issued key for region `us-east-1`, shunt verifies, strips the client's auth headers, and re-signs upstream with the cluster's own key and region. Payload mode on the via side is `UNSIGNED-PAYLOAD` (what aws-sdk-go-v2 sends over HTTPS with checksums `WhenRequired`), so the delta isolates header verification and signing. The signed-chunk path, which adds one HMAC per 64 KiB chunk plus the re-framing, is measured by Go micro-benchmarks below, not by the e2e harness (the SDK cannot emit signed chunks).

Baseline: docs/bench/poc1.md (passthrough, same host, 2026-09-15). Resign runs: `make bench-e2e BACKEND=garage|minio MODE=resign` with `make run-<backend>-resign`.

## Disclosure

| Item | Value |
|---|---|
| Host | AMD EPYC 7402P 24-Core Processor, 16 vCPUs, 62 GB RAM |
| Kernel | 5.14.0-570.17.1.el9_6.x86_64 |
| NIC | loopback: client, shunt, and backends on one host |
| Go | go1.27.1 |
| shunt | poc-1-2-g9112ec5-dirty plus POC-2 working tree |
| Client | aws-sdk-go-v2 s3 v1.113.1, path-style, `UNSIGNED-PAYLOAD` to shunt |
| Client → shunt | TLS 1.3, ECDSA P-256 wildcard cert |
| shunt → backend | plaintext HTTP/1.1, header-signed `UNSIGNED-PAYLOAD` |
| Garage / MinIO | as in docs/bench/poc1.md |

VAST is not benchmarked in POC-2: the lab cluster is across the network with TLS verification temporarily disabled, so its numbers would measure the network, not the proxy.

## Results: Garage 2.3.0, resign

|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 KiB | 1 | PUT | 12.86 ms | 33.16 ms | 13.79 ms | 33.77 ms | 0.93 ms | 0.61 ms | 0.3 | 0.3 | 0.93 | 425.98 | 1625 |
| 4 KiB | 1 | GET | 43.02 ms | 45.70 ms | 43.85 ms | 46.28 ms | 0.83 ms | 0.58 ms | 0.1 | 0.1 | 0.98 | 432.54 | 1650 |
| 4 KiB | 64 | PUT | 305.32 ms | 603.60 ms | 316.53 ms | 537.07 ms | 11.21 ms | -66.54 ms | 0.8 | 0.8 | 0.97 | 381.75 | 1456 |
| 4 KiB | 64 | GET | 201.52 ms | 595.92 ms | 197.63 ms | 564.64 ms | -3.89 ms | -31.28 ms | 1.1 | 1.1 | 1.04 | 397.31 | 1516 |
| 1 MiB | 1 | PUT | 32.85 ms | 76.25 ms | 33.62 ms | 61.51 ms | 0.77 ms | -14.74 ms | 27.8 | 29.0 | 1.05 | 4.97 | 4850 |
| 1 MiB | 1 | GET | 21.01 ms | 46.69 ms | 22.18 ms | 51.23 ms | 1.17 ms | 4.55 ms | 45.4 | 41.4 | 0.91 | 4.81 | 4700 |
| 1 MiB | 64 | PUT | 761.70 ms | 1439.67 ms | 760.71 ms | 1187.94 ms | -0.98 ms | -251.73 ms | 82.8 | 83.3 | 1.01 | 5.22 | 5094 |
| 1 MiB | 64 | GET | 1122.57 ms | 1468.81 ms | 1116.57 ms | 1464.59 ms | -6.00 ms | -4.21 ms | 57.1 | 56.9 | 1.00 | 7.78 | 7594 |
| 1 GiB | 1 | PUT | 10694.76 ms | 10694.76 ms | 11071.74 ms | 11071.74 ms | 376.98 ms | 376.98 ms | 95.8 | 92.3 | 0.96 | 1.80 | 1803333 |
| 1 GiB | 1 | GET | 10676.30 ms | 10676.30 ms | 11720.52 ms | 11720.52 ms | 1044.23 ms | 1044.23 ms | 99.2 | 89.2 | 0.90 | 3.22 | 3220000 |

## Results: MinIO, resign

|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 KiB | 1 | PUT | 10.53 ms | 23.47 ms | 11.79 ms | 20.37 ms | 1.26 ms | -3.10 ms | 0.3 | 0.3 | 0.90 | 393.22 | 1500 |
| 4 KiB | 1 | GET | 2.35 ms | 5.94 ms | 3.39 ms | 6.72 ms | 1.04 ms | 0.78 ms | 1.5 | 1.1 | 0.70 | 380.11 | 1450 |
| 4 KiB | 64 | PUT | 41.83 ms | 198.14 ms | 44.59 ms | 167.41 ms | 2.75 ms | -30.73 ms | 4.8 | 4.7 | 0.98 | 290.82 | 1109 |
| 4 KiB | 64 | GET | 11.45 ms | 126.75 ms | 19.02 ms | 66.81 ms | 7.58 ms | -59.94 ms | 15.4 | 11.2 | 0.73 | 238.39 | 909 |
| 1 MiB | 1 | PUT | 24.19 ms | 38.58 ms | 25.34 ms | 39.72 ms | 1.14 ms | 1.14 ms | 39.4 | 38.1 | 0.97 | 4.61 | 4500 |
| 1 MiB | 1 | GET | 5.72 ms | 13.17 ms | 7.90 ms | 16.55 ms | 2.18 ms | 3.38 ms | 160.9 | 117.9 | 0.73 | 4.51 | 4400 |
| 1 MiB | 64 | PUT | 110.11 ms | 202.65 ms | 137.42 ms | 268.22 ms | 27.31 ms | 65.56 ms | 530.3 | 415.2 | 0.78 | 4.13 | 4031 |
| 1 MiB | 64 | GET | 39.32 ms | 133.83 ms | 60.54 ms | 140.25 ms | 21.22 ms | 6.42 ms | 1272.8 | 945.6 | 0.74 | 3.68 | 3594 |
| 1 GiB | 1 | PUT | 4835.85 ms | 4835.85 ms | 4274.48 ms | 4274.48 ms | -561.37 ms | -561.37 ms | 204.4 | 227.7 | 1.11 | 1.83 | 1826667 |
| 1 GiB | 1 | GET | 2996.24 ms | 2996.24 ms | 3154.10 ms | 3154.10 ms | 157.86 ms | 157.86 ms | 335.0 | 324.6 | 0.97 | 2.46 | 2460000 |

## Resign overhead against POC-1

Proxy CPU per operation, median of three runs, from the tables above and docs/bench/poc1.md. Small objects are where per-request cost shows; large objects are dominated by per-byte TLS and copy cost, which resign does not change.

| Backend | Cell | Passthrough µs/op | Resign µs/op | Delta |
|---|---|---|---|---|
| Garage | 4 KiB × 1 PUT | 1425 | 1625 | +200 |
| Garage | 4 KiB × 1 GET | 1475 | 1650 | +175 |
| Garage | 4 KiB × 64 PUT | 1294 | 1456 | +162 |
| Garage | 4 KiB × 64 GET | 1297 | 1516 | +219 |
| MinIO | 4 KiB × 1 PUT | 1475 | 1500 | +25 |
| MinIO | 4 KiB × 1 GET | 1250 | 1450 | +200 |
| MinIO | 4 KiB × 64 PUT | 1213 | 1109 | −104 |
| MinIO | 4 KiB × 64 GET | 753 | 909 | +156 |

| Backend | Cell | Passthrough CPU s/GiB | Resign CPU s/GiB |
|---|---|---|---|
| Garage | 1 GiB PUT | 1.76 | 1.80 |
| Garage | 1 GiB GET | 3.11 | 3.22 |
| MinIO | 1 GiB PUT | 1.83 | 1.83 |
| MinIO | 1 GiB GET | 2.46 | 2.46 |

**The resign overhead is about 150–220 µs of proxy CPU per request** for header-signed requests (six of eight small-object cells; the MinIO 4 KiB PUT cells sit inside run-to-run noise, one of them negative). Per-GiB cost is unchanged within noise. Added client latency at one connection stays near 1 ms, as in POC-1. The micro-benchmarks below break the per-request cost down.

## Micro-benchmarks (Go, in-process backend, `make bench`, 6 runs)

| Benchmark | Time/op | Bytes/op | Allocs/op | What it isolates |
|---|---|---|---|---|
| `sigv4.BenchmarkVerifyHeader` | 18.8 µs | 5.9 KiB | 76 | parse + canonicalize + derive kSigning + HMAC, header auth |
| `sigv4.BenchmarkSign` | 15.7 µs | 5.2 KiB | 72 | upstream signing |
| `sigv4.BenchmarkSigningKey` | 4.55 µs | 2.1 KiB | 29 | one kSigning derivation (done once in verify, once in sign) |
| `proxy.BenchmarkSmallGET` (passthrough) | 442 µs | 31 KiB | 106 | whole handler, 4 KiB GET |
| `proxy.BenchmarkResignSmallGET` | 547 µs | 50 KiB | 236 | whole handler in resign mode, 4 KiB GET |
| `proxy.BenchmarkPUT1MiB` (passthrough) | 2.29 ms, 438 MiB/s | 68 KiB | 178 | 1 MiB PUT |
| `proxy.BenchmarkResignSignedChunkPUT1MiB` | 5.56 ms, 180 MiB/s | 1.0 MiB | 4 728 | 1 MiB signed-chunk PUT, 8 KiB chunks, decode + verify every chunk |
| `chunked.BenchmarkEncode1MiB` | 1.05 ms, 957 MiB/s | 1.19 MiB | 43 | unsigned-trailer re-framing |
| `chunked.BenchmarkUnsignedDecode1MiB` | 141 µs, 6.9 GiB/s | 1.7 KiB | 47 | lifted unsigned-trailer decoder |

What this says:

- Header verification plus upstream signing is **~35 µs of CPU and ~150 allocations per request**; the handler-level delta is +105 µs and +130 allocations. The e2e delta (150–220 µs of process CPU) is larger because it also carries the GC cost of those allocations. The two kSigning derivations are 9 µs and 58 allocations of that and are cacheable per (access key, date, region): the first P7 lever for resign.
- **Signed-chunk uploads cost 2.4× a plain PUT in-process** (180 vs 438 MiB/s at 8 KiB chunks, the worst case; SDK clients that sign chunks use 64 KiB). The lifted decoder builds each chunk's string-to-sign with `fmt.Sprintf` and allocates ~37 times per chunk. Memory stays constant; allocation rate is the P7 target.
- The unsigned-trailer encoder allocates one frame per chunk (1.19 MiB per MiB encoded). Bounded by the chunk size, so still streaming, not buffering; a pooled frame is the P7 fix.

## Regression check against the POC-1 baseline

`benchstat test/bench/baseline.txt` (POC-1) against this run, unchanged benchmarks: config parsing +1.6 % bytes and allocations (the schema gained `capabilities`), everything else within noise except `admin.BenchmarkHealthz`, reported +15 % (1.92 µs → 2.21 µs, ±14 %). That handler did not change in POC-2; see the re-run below.

Re-run to separate code from machine noise: the `poc-1` tag and the POC-2 tree benchmarked alternately, two rounds of five each, same machine state. `poc-1` averaged 1783 ns/op, POC-2 1903 ns/op (+6.7 %). `internal/admin` is byte-identical between the two (`git diff poc-1 -- internal/admin` is empty), bytes and allocations per op are identical (1011 B, 10 allocs), and nothing in the benchmark loop touches POC-2 code. **Recorded as binary layout, not a code regression** (decided 2026-09-15): the handler's code, allocations, and bytes are unchanged, and the ~120 ns shift follows the binary, which grew with POC-2's dependency graph, not any change in the measured path.

`test/bench/baseline.txt` is refreshed from this run.
