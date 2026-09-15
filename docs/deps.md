# Dependencies

One line per dependency saying why the stdlib wasn't enough (CLAUDE.md). Tool binaries are pinned in the Makefile and never enter go.mod.

## go.mod

| Module | Version | License | Why |
|---|---|---|---|
| `go.yaml.in/yaml/v4` | v4.0.0-rc.6 | MIT (libyaml-derived files) + Apache-2.0 | Config is YAML; stdlib has no YAML. YAML org fork; `gopkg.in/yaml.v3` is archived. Provides `KnownFields` for the unknown-key rule and positional errors for key-path naming. |
| `github.com/spf13/cobra` | v1.10.2 | Apache-2.0 | Subcommand tree with help; stdlib `flag` has no subcommands. Chosen in docs/CONTEXT.md. Pulls `spf13/pflag` (BSD-3) and `inconshreveable/mousetrap` (Apache-2.0). |
| `github.com/johannesboyne/gofakes3` | v1.2.0 | MIT | **Test-only** (imported from `_test.go` files). In-process fake S3 so unit tests need no Docker. Pulls aws-sdk-go-v2, afero, bbolt into the test build only; `go build ./cmd/shunt` does not link them. |

## Tool binaries (Makefile, ./bin)

| Tool | Version | License | Why |
|---|---|---|---|
| `golangci-lint` | v2.13.2 | GPL-3.0 (tool only, not linked) | The P0 linter set. |
| `benchstat` (`golang.org/x/perf`) | v0.0.0-20260908200009-22c9c6c9d4da | BSD-3 | `make bench-compare`. |

## Lifted code

| Source repo | Path | Commit | License | Destination | Why the stdlib wasn't enough |
|---|---|---|---|---|---|
| github.com/versity/versitygw | `s3response/s3response.go` | `4dc0debf8f79e0e0b099b9766a66b40b316dee7e` | Apache-2.0 (NOTICE carried in THIRD_PARTY_NOTICES) | `internal/s3/s3response/s3response.go` | 60 S3 XML response/request structs with the custom marshallers (Part, Object, Upload, ListAllMyBucketsEntry, CopyObjectResult, ObjectVersion, AmzDate) that match backend wire shapes; writing and validating them from scratch is a week of s3diff churn. Modified: SDK types replaced by `types.go`, s3err/debuglogger removed, S3 Select and admin types dropped, xxhash fields dropped. |

Planned (docs/reuse-findings.md): versitygw `s3api/utils/{chunk-reader,signed-chunk-reader,unsigned-chunk-reader}.go` at POC-2 (re-check the SHA then); cilium/ebpf `examples/tcprtt_sockops` plus `examples/headers` (MIT + BSD-2-Clause) at P4.
