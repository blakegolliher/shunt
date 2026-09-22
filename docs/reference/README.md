# Reference

Machine-checked facts about shunt's interfaces. Each file is written by the phase that builds the thing it describes.

| File | Written in | Content |
|---|---|---|
| `config.md` | POC-1 | Every config key, type, default, and validation rule (the schema in `internal/config`) |
| `access-log.md` | POC-1 | The fixed JSON access-log field schema |
| `backend-compat.md` | POC-2 | Per-backend capability findings with the `shunt probe` output that proves each |
| `control-api.md` | POC-5, P3c, UI-0 | Every control API route: the operator verbs' API, the fleet's, the control plane's own, operations, events and the views (ADR-0008, ADR-0015, ADR-0016, ADR-0017) |
| `control-routes.json` | UI-0 | The route table, generated from `internal/control` and `internal/cp` by a test; the web UI's parity test reads it |
| `metrics.md` | P4 | Generated from `docs/telemetry-catalog.md` |

Until then, `internal/config/config.go` is the config reference and `docs/telemetry-catalog.md` is the metrics reference.
