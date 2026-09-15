# STATUS

Current phase: **POC-0 complete, POC-1 next.** Track: docs/POC.md. Full design: docs/DESIGN.md.

## POC track

| Session | Gate | State |
|---|---|---|
| POC-0 | `make all` green on clean checkout; `check-config` rejects every invalid sample naming the key; `make e2e-up` leaves Garage and MinIO healthy | done 2026-09-14 |
| POC-1 | s3diff clean on Garage and MinIO (passthrough); unauthorized 5 GiB PUT fails before body bytes; mid-GET backend kill never a silent short read; bench in docs/bench/poc1.md | not started |
| POC-2 | s3diff clean across signing modes on every backend or a documented gap each; secret-leak test green; fuzz 30s clean; resign overhead in docs/bench/poc2.md | not started |
| POC-3 | mixed-backend s3diff clean; same bucket name under two tenants isolated; ListBuckets spans clusters; no backend name leaks; uploadId round-trip | not started |
| POC-4 | demo.sh green Garage → MinIO and VAST → MinIO/AWS; property test green; docs/bench/poc4.md | not started |

After POC-4: G1 (simplicity) and G4 (licenses) once, then resume the full order at P0's skipped items (docs/POC.md "After the POC").

## What exists

- `cmd/shunt`: `version`, `check-config`.
- `internal/config`: schema, validator, 2 valid and 39 invalid samples, fuzz target, benchmarks.
- Skeleton packages with doc.go only: listener, s3, sigv4, auth, directory, upstream, proxy, migrate, telemetry, admin.
- Makefile targets: build, test, race, lint, fuzz, bench, bench-compare, e2e-up, e2e-down, all.
- test/e2e: Garage + MinIO compose, wildcard cert script, health wait.

## Known gaps carried forward

- docs/CONTEXT.md VAST rows are `unknown`; required before POC-2.
- go.yaml.in/yaml/v4 is at a release candidate (v4.0.0-rc.6); bump when v4.0.0 ships.
- ADR-0002 compensation is log-and-alert in the POC (POC-2 cut).
