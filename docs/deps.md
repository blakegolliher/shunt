# Dependencies

One line per dependency saying why the stdlib wasn't enough (CLAUDE.md). Tool binaries are pinned in the Makefile and never enter go.mod.

## go.mod

| Module | Version | License | Why |
|---|---|---|---|
| `go.yaml.in/yaml/v4` | v4.0.0-rc.6 | MIT (libyaml-derived files) + Apache-2.0 | Config is YAML; stdlib has no YAML. YAML org fork; `gopkg.in/yaml.v3` is archived. Provides `KnownFields` for the unknown-key rule and positional errors for key-path naming. |
| `github.com/spf13/cobra` | v1.10.2 | Apache-2.0 | Subcommand tree with help; stdlib `flag` has no subcommands. Chosen in docs/CONTEXT.md. Pulls `spf13/pflag` (BSD-3) and `inconshreveable/mousetrap` (Apache-2.0). |

## Tool binaries (Makefile, ./bin)

| Tool | Version | License | Why |
|---|---|---|---|
| `golangci-lint` | v2.8.0 | GPL-3.0 (tool only, not linked) | The P0 linter set. Newest release that builds on Go 1.24. |
| `benchstat` (`golang.org/x/perf`) | v0.0.0-20251208221838-04cf7a2dca90 | BSD-3 | `make bench-compare`. |

## Lifted code

None yet. The lift procedure in CLAUDE.md adds rows here and to THIRD_PARTY_NOTICES.
