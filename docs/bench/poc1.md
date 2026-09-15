# POC-1 bench: passthrough proxy overhead

Method: docs/DESIGN.md §8 with the POC-1 cuts (docs/POC.md): sizes 4 KiB, 1 MiB, 1 GiB; connections 1 and 64 (1 GiB at 1 only, dev-box disk); GET-only and PUT-only; three runs, median. Direct-to-backend numbers come from the same run. Harness: `test/bench/s3bench` (`make bench-e2e BACKEND=…`). Proxy CPU is `process_cpu_seconds_total` from `/-/metrics`, delta over the via run, divided by GiB moved and by operations.

## Disclosure

| Item | Value |
|---|---|
| Host | AMD EPYC 7402P 24-Core Processor, 16 vCPUs, 62 GB RAM |
| Kernel | 5.14.0-570.17.1.el9_6.x86_64 |
| NIC | loopback: client, shunt, and backends on one host (127.0.0.1) |
| Go | go1.27.1 |
| shunt | poc-0-4-g40e934b-dirty |
| Client | aws-sdk-go-v2 s3 v1.113.1, unsigned payload, path-style, one HTTP/1.1 keep-alive pool per side |
| Client → shunt | TLS 1.3, Go default suite, ECDSA P-256 wildcard cert |
| shunt → backend | plaintext HTTP/1.1 (`scheme: http`) |
| Garage | docker.io/dxflrs/garage:v2.3.0, single node, sqlite metadata, podman |
| MinIO | quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z, single node, podman |
| Backend storage | container volumes on the host filesystem |

Caveat: everything shares one host, so "added latency" includes the client's TLS work and the backend's, and the loopback has no NIC. The numbers bound the proxy's own cost; they are not a throughput claim for a real fabric. P7 measures on real hardware.

## Results: Garage 2.3.0

| size | conns | op | direct p50 | direct p99 | via p50 | via p99 | added p50 | added p99 | direct MiB/s | via MiB/s | ratio | proxy CPU s/GiB | proxy CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 KiB | 1 | PUT | 10.68 ms | 22.31 ms | 11.26 ms | 21.11 ms | 0.57 ms | -1.20 ms | 0.3 | 0.3 | 0.94 | 373.56 | 1425 |
| 4 KiB | 1 | GET | 43.03 ms | 44.96 ms | 44.05 ms | 46.46 ms | 1.02 ms | 1.51 ms | 0.1 | 0.1 | 0.98 | 386.66 | 1475 |
| 4 KiB | 64 | PUT | 312.90 ms | 468.33 ms | 325.65 ms | 558.34 ms | 12.76 ms | 90.00 ms | 0.8 | 0.7 | 0.95 | 339.15 | 1294 |
| 4 KiB | 64 | GET | 160.08 ms | 459.02 ms | 174.36 ms | 550.43 ms | 14.28 ms | 91.41 ms | 1.4 | 1.3 | 0.90 | 339.97 | 1297 |
| 1 MiB | 1 | PUT | 28.74 ms | 61.05 ms | 29.66 ms | 53.84 ms | 0.91 ms | -7.21 ms | 32.2 | 31.9 | 0.99 | 4.40 | 4300 |
| 1 MiB | 1 | GET | 17.06 ms | 33.41 ms | 20.74 ms | 37.77 ms | 3.68 ms | 4.35 ms | 55.2 | 45.9 | 0.83 | 4.51 | 4400 |
| 1 MiB | 64 | PUT | 467.44 ms | 906.73 ms | 527.39 ms | 966.65 ms | 59.95 ms | 59.92 ms | 123.9 | 120.5 | 0.97 | 4.26 | 4156 |
| 1 MiB | 64 | GET | 1026.77 ms | 2114.66 ms | 957.11 ms | 1397.36 ms | -69.66 ms | -717.30 ms | 57.4 | 64.4 | 1.12 | 7.18 | 7016 |
| 1 GiB | 1 | PUT | 10630.59 ms | 10630.59 ms | 10421.33 ms | 10421.33 ms | -209.26 ms | -209.26 ms | 97.0 | 98.6 | 1.02 | 1.76 | 1756667 |
| 1 GiB | 1 | GET | 10370.46 ms | 10370.46 ms | 11007.65 ms | 11007.65 ms | 637.20 ms | 637.20 ms | 94.7 | 91.1 | 0.96 | 3.11 | 3110000 |

## Results: MinIO

| size | conns | op | direct p50 | direct p99 | via p50 | via p99 | added p50 | added p99 | direct MiB/s | via MiB/s | ratio | proxy CPU s/GiB | proxy CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 KiB | 1 | PUT | 11.21 ms | 23.53 ms | 12.18 ms | 17.86 ms | 0.97 ms | -5.67 ms | 0.3 | 0.3 | 0.96 | 386.66 | 1475 |
| 4 KiB | 1 | GET | 2.36 ms | 6.47 ms | 3.28 ms | 7.44 ms | 0.92 ms | 0.98 ms | 1.5 | 1.1 | 0.74 | 327.68 | 1250 |
| 4 KiB | 64 | PUT | 59.06 ms | 212.39 ms | 75.82 ms | 233.74 ms | 16.76 ms | 21.35 ms | 3.5 | 2.9 | 0.83 | 317.85 | 1213 |
| 4 KiB | 64 | GET | 16.18 ms | 72.83 ms | 18.46 ms | 58.15 ms | 2.28 ms | -14.68 ms | 12.3 | 12.2 | 1.00 | 197.43 | 753 |
| 1 MiB | 1 | PUT | 26.57 ms | 47.87 ms | 25.80 ms | 40.99 ms | -0.77 ms | -6.89 ms | 35.2 | 37.3 | 1.06 | 4.40 | 4300 |
| 1 MiB | 1 | GET | 5.79 ms | 12.97 ms | 9.73 ms | 32.67 ms | 3.95 ms | 19.70 ms | 159.9 | 87.4 | 0.55 | 4.76 | 4650 |
| 1 MiB | 64 | PUT | 123.04 ms | 260.08 ms | 164.20 ms | 323.33 ms | 41.16 ms | 63.25 ms | 466.9 | 362.3 | 0.78 | 4.86 | 4750 |
| 1 MiB | 64 | GET | 67.56 ms | 162.27 ms | 84.71 ms | 208.05 ms | 17.15 ms | 45.78 ms | 870.9 | 665.4 | 0.76 | 4.50 | 4391 |
| 1 GiB | 1 | PUT | 4781.12 ms | 4781.12 ms | 4229.52 ms | 4229.52 ms | -551.60 ms | -551.60 ms | 204.8 | 244.3 | 1.19 | 1.83 | 1833333 |
| 1 GiB | 1 | GET | 2143.06 ms | 2143.06 ms | 3160.86 ms | 3160.86 ms | 1017.79 ms | 1017.79 ms | 463.8 | 324.0 | 0.70 | 2.46 | 2456667 |

## Reading the numbers

- **Large objects: the proxy is close to free on this box.** 1 GiB PUT through shunt runs at 0.98–1.19× direct throughput on both backends; 1 GiB GET at 0.70× (MinIO) and 0.96× (Garage). Proxy CPU is 1.8–3.1 s per GiB moved, with TLS on the client side and plaintext upstream. Per the profile taken during the run, that CPU is 64 % socket syscalls and most of the rest AES-GCM; shunt's own code does not register. That is the userspace-TLS cost docs/DESIGN.md §1.4 predicts, and the number P7 will drive down with buffer size, GOGC, and process count.
- **Small objects: ~1 ms added per request at 1 connection, 1.2–1.5 ms of proxy CPU per op.** With 64 connections the added p50 is 2–17 ms and p99 is noisy in both directions (negative "added" values are run-to-run variance of a single-host setup, not a proxy speedup). Per-request cost, not per-byte cost, is where P7's allocation work applies.
- **Mid-size (1 MiB) GET at 1 connection** is the worst ratio (0.55× MinIO, 0.83× Garage): a single stream pays TLS encryption on every 256 KiB chunk serially. 64 connections recover most of it (0.76×/1.12×).
- Garage's own 4 KiB GET latency (43 ms) and 1 MiB/64-connection tail (2.1 s p99) are the backend, not the proxy; direct and via track each other.
- Cuts vs the §8 method: 64 MiB and 256 connections are not run; 1 GiB only at 1 connection; GET-only and PUT-only, no mixed workload. Three runs each, median reported.

## Go micro-benchmarks (test/bench/baseline.txt, refreshed in this commit)

`BenchmarkSmallGET` drives the handler with an in-process backend: ~450 µs and 106 allocs per 4 KiB GET, which includes net/http's client and server allocations on both sides of the handler. The classifier and parser are allocation-free (`BenchmarkClassify` 0 B/op). P7 separates shunt's own allocations from net/http's and works them down; `make bench-compare` flags a >5 % regression against this baseline from here on.

## Baseline for later phases

POC-2 (resign) is measured against these tables; the metric that matters there is proxy CPU per GiB and per op, since SigV4 verify adds one HMAC chain per request and, for signed chunks, one per 64 KiB chunk.
