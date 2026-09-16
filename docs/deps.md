# Dependencies

One line per dependency saying why the stdlib wasn't enough (CLAUDE.md). Tool binaries are pinned in the Makefile and never enter go.mod.

## go.mod

| Module | Version | License | Why |
|---|---|---|---|
| `go.yaml.in/yaml/v4` | v4.0.0-rc.6 | MIT (libyaml-derived files) + Apache-2.0 | Config is YAML; stdlib has no YAML. YAML org fork; `gopkg.in/yaml.v3` is archived. Provides `KnownFields` for the unknown-key rule and positional errors for key-path naming. |
| `github.com/spf13/cobra` | v1.10.2 | Apache-2.0 | Subcommand tree with help; stdlib `flag` has no subcommands. Chosen in docs/CONTEXT.md. Pulls `spf13/pflag` (BSD-3) and `inconshreveable/mousetrap` (Apache-2.0). |
| `github.com/prometheus/client_golang` | v1.24.1 | Apache-2.0 | Metrics (docs/DESIGN.md decision 10): the leanest hot-path client; stdlib has no exposition format. Pulls `prometheus/client_model`, `prometheus/common`, `prometheus/procfs`, `protobuf`. `client_model` is a direct require since POC-5: `internal/control` reads counter values for `GET /v1/status` from the registered vectors (`dto.Metric`) rather than scraping its own `/-/metrics`. |
| `github.com/aws/aws-sdk-go-v2` (+ `config`, `credentials`, `service/s3`) | v1.47.0 / s3 v1.113.1 | Apache-2.0 | SigV4-signing S3 client for the differential and bench harnesses (`test/s3diff`, `test/bench/s3bench`), and, **since POC-5, in the shipped binary** for the mover only: `cmd/shunt/mover.go` (`shunt migrate run`). ADR-0009 overrides the earlier test-tree-only rule until P5, when the mover moves to its own `shunt-mover` binary and the SDK leaves `bin/shunt`. Never imported by `internal/`; the request path signs with `internal/sigv4`. |
| `github.com/aws/smithy-go` | v1.28.1 | Apache-2.0 | `cmd/shunt/mover.go` only (ADR-0009). Already pulled in by aws-sdk-go-v2; named directly so the mover can read an S3 error's code (`PreconditionFailed`, `NotFound`) instead of matching on error strings. Never imported by `internal/`. |
| `github.com/johannesboyne/gofakes3` | v1.2.0 | MIT | **Test-only** (imported from `_test.go` files). In-process fake S3 so unit tests need no Docker. Pulls aws-sdk-go-v2, afero, bbolt into the test build only; `go build ./cmd/shunt` does not link them. |

## Tool binaries (Makefile, ./bin)

| Tool | Version | License | Why |
|---|---|---|---|
| `golangci-lint` | v2.13.2 | GPL-3.0 (tool only, not linked) | The P0 linter set. |
| `benchstat` (`golang.org/x/perf`) | v0.0.0-20260908200009-22c9c6c9d4da | BSD-3 | `make bench-compare`. |
| `go-licenses` (`github.com/google/go-licenses/v2`) | v2.0.1 | Apache-2.0 | `make licenses`: which modules `bin/shunt` links and under what license, for THIRD_PARTY_NOTICES (G4). |
| `govulncheck` (`golang.org/x/vuln`) | v1.8.0 | BSD-3 | `make vuln`: known vulnerabilities in the code paths shunt actually calls (G4). |

## Lifted code

| Source repo | Path | Commit | License | Destination | Why the stdlib wasn't enough |
|---|---|---|---|---|---|
| github.com/versity/versitygw | `s3response/s3response.go` | `4dc0debf8f79e0e0b099b9766a66b40b316dee7e` | Apache-2.0 (NOTICE carried in THIRD_PARTY_NOTICES) | `internal/s3/s3response/s3response.go` | The ListObjectsV2 response structs and `Object`'s custom marshaller, matching backend wire shapes for the merged listing. Lifted as 60 structs; the G1 review (2026-09-16) removed the 50-odd shunt never used, since each is one re-lift away at the pinned SHA. Modified: SDK types replaced by `types.go`, s3err/debuglogger removed, S3 Select and admin types dropped, xxhash fields dropped, unused types removed. |

| github.com/versity/versitygw | `s3api/utils/chunk-reader.go`, `signed-chunk-reader.go`, `unsigned-chunk-reader.go` (+ two test files) | `4dc0debf8f79e0e0b099b9766a66b40b316dee7e` (re-checked 2026-09-15, unchanged) | Apache-2.0 | `internal/sigv4/chunked/` | aws-chunked decoding with per-chunk signature verification, trailer signature and checksum validation, chunk-size and decoded-length checks, in-place header parsing with a stash for split headers. Actively hardened upstream through 2025–26; a from-scratch decoder would re-learn those cases. Modified: Fiber → http.Header, debuglogger removed, SecureCompare → crypto/subtle, s3err → shunt's table, SDK enum → string, Versity-only checksum types dropped; two fixes from fuzzing: decoded bytes capped at the declared length, negative chunk sizes rejected (the second is also present upstream and crashed the process through net/http's transport); G1 (2026-09-16) removed the unused payload-type helpers and `NewChunkReader`, and unexported the functions only the package uses. The unsigned reader stays: it is the decode oracle for shunt's own encoder, which production uses. |

Planned (docs/reuse-findings.md): cilium/ebpf `examples/tcprtt_sockops` plus `examples/headers` (MIT + BSD-2-Clause) at P4.
