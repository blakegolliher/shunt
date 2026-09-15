POC phase 0. Read docs/DESIGN.md (§1–§4, §2.10) and docs/CONTEXT.md, then docs/POC.md. This is a proof of concept built on the product's skeleton: same layout, same rules, fewer features. Anything docs/POC.md marks as deferred is out of scope even if it looks easy.

Build:
1. Go module github.com/blakegolliher/shunt; layout per §4 but only the packages the POC touches: listener, s3, sigv4, auth, directory, upstream, proxy, migrate, telemetry, admin, config. YAML via go.yaml.in/yaml/v4.
2. cmd/shunt with `version` and `check-config` (unknown keys are errors; cluster type/scheme/region required).
3. CLAUDE.md: §2.10 verbatim plus "no per-object state, ever", "docs/POC.md lists what not to build", and the lift procedure from docs/reuse.md.
4. Makefile: build, test, race, lint, fuzz (30s per target), bench, bench-compare. .golangci.yml with the P0 linter set. GitHub Actions: ci only. LICENSE Apache-2.0, NOTICE.
5. docs/: adr/0001-auth-modes.md and adr/0002-late-failure-compensation.md (short), telemetry-catalog.md with the six P1 metrics, reference/, STATUS.md, bench/.
6. test/e2e/docker-compose.yml with Garage and MinIO, a script that generates a self-signed wildcard cert for the domain in CONTEXT.md, and a make target that brings both up and waits for health.

Skip (deferred): CodeQL, Semgrep, nightly fuzz, Dependabot, the Diátaxis skeleton, the failure-modes red-team.

Acceptance: `make all` green on a clean checkout; check-config rejects each invalid sample naming the offending key; `make e2e-up` leaves Garage and MinIO healthy.

Before writing code, list the files you intend to create and any dependency you intend to add beyond the YAML library, and wait for my go.
