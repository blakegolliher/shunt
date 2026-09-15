# Reference

Machine-checked facts about shunt's interfaces. Each file is written by the phase that builds the thing it describes.

| File | Written in | Content |
|---|---|---|
| `config.md` | POC-1 | Every config key, type, default, and validation rule (the schema in `internal/config`) |
| `access-log.md` | POC-1 | The fixed JSON access-log field schema |
| `backend-compat.md` | POC-2 | Per-backend capability findings with the `shunt probe` output that proves each |
| `metrics.md` | P4 | Generated from `docs/telemetry-catalog.md` |

Until then, `internal/config/config.go` is the config reference and `docs/telemetry-catalog.md` is the metrics reference.
