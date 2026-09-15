# test/bench

`baseline.txt` is the benchstat baseline for the Go micro-benchmarks (`make bench-compare`). `new.txt` is generated and ignored. The end-to-end bench harness (direct vs via-shunt) arrives in POC-1 with results in `docs/bench/`.

`s3bench/` is the direct-vs-via harness: `make bench-e2e BACKEND=garage|minio` with shunt running in front of that backend. Matrix, runs, and the admin endpoint it reads `process_cpu_seconds_total` from are flags.
