# Benchmarks

Method from docs/DESIGN.md §8, used by every `docs/bench/*.md`:

- Object sizes 4 KiB, 1 MiB, 64 MiB, 1 GiB (POC-1 runs 4 KiB, 1 MiB, 1 GiB).
- Connections 1, 16, 256 (POC-1 runs 1 and 64).
- Workloads GET-only, PUT-only, 70/30 mixed.
- Three runs, report the median. Direct-to-backend numbers from the same run are the baseline.
- Metrics: throughput ratio (proxy/direct), added p50 and p99 latency, proxy CPU-seconds per GiB, RSS, allocs per request.
- Disclose hardware, kernel, NIC, Go version, and backend build every time. A number without its configuration is not a result.
- A >5 % regression against the previous phase fails the phase.

Go micro-benchmarks: `make bench` writes `test/bench/new.txt`; `make bench-compare` runs benchstat against `test/bench/baseline.txt`. Update the baseline deliberately, in its own commit, with the reason in the message.

| File | Phase |
|---|---|
| `poc1.md` | POC-1 passthrough baseline |
| `poc2.md` | POC-2 resign overhead vs poc1 |
| `poc4.md` | POC-4 added latency during migration |
