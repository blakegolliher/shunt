# ADR-0009: The mover ships in `bin/shunt`, and brings the AWS SDK with it

Status: accepted (POC-5, 2026-09-16). Overrides the "test tree only" scope of the `aws-sdk-go-v2` and `smithy-go` rows in docs/deps.md. Source: docs/DESIGN.md §2.10 (`shunt migrate`), CLAUDE.md "standard library first"; ADR-0004; docs/POC.md POC-5.

## Context

Through POC-4 the mover was `test/mover`, a test-tree program on the AWS SDK: listing, HEAD, streaming GET into PUT, multipart copy with the source's part layout, `If-None-Match: *`, `If-Match` on DELETE, and reading typed S3 errors. docs/deps.md kept the SDK out of `internal/` and `cmd/`, so the shipped binary had only shunt's own SigV4 signer and hand-written requests on the proxy path.

POC-5 makes `shunt migrate run` an operator command with the same contract (ADR-0004 guards, both refusals, cursor and ledger). The choice was between rewriting the mover on shunt's signer, or moving the SDK-based mover into `cmd/shunt` unchanged.

## Decision

**For POC-5, the mover moves into `cmd/shunt` as it is, SDK included** (P5 moves it to its own binary; see "Planned resolution"). Its guards were fixed and measured against a 60-minute property run in POC-4; a rewrite of the client underneath them would re-open that evidence for no gain in a POC. The SDK is imported by `cmd/shunt/mover.go` only. Nothing in `internal/`, and nothing on the request path, imports it: the proxy still signs with `internal/sigv4` and streams through its own transport.

The mover runs in the CLI process, not in `shunt serve`. It fetches the placement and cluster definitions from the control API (ADR-0008), resolves `secret_ref`s locally, and reports each pass back.

## Consequences

- `bin/shunt` grows from 14.4 MiB to 20.8 MiB (`-trimpath -ldflags '-s -w'`, linux/amd64, go1.27.1), most of it the SDK's S3 client, endpoint and checksum packages.
- `make vuln` (govulncheck) now reports SDK advisories against the shipped binary, not just the test tree; `make licenses` carries the SDK's notices in `THIRD_PARTY_NOTICES`.
- The mover host needs the same `env:` or `file:` secrets as `shunt serve`.

## Planned resolution (P5)

This ADR is a POC-5 expedient, not the end state. **In P5 the mover becomes a separate binary, `shunt-mover`**, built from the same repo, with the same `migrate run` flags and contract, talking to the same control API. `shunt migrate run` is removed from `bin/shunt` then, and with it the AWS SDK and smithy-go. The proxy binary goes back to shunt's own signer alone, as docs/deps.md stated before POC-5.

What P5 carries over unchanged:
- the ADR-0004 guards and both refusals;
- cursor and ledger paths as flags;
- progress reports to `mover-progress`;
- the property test, which drives the mover contract rather than the binary.

What P5 changes:
- the §2.10 binary list gains `shunt-mover`, with an ADR amending it;
- docs/deps.md scopes the SDK rows to `cmd/shunt-mover`;
- `make licenses` and `make vuln` cover both binaries;
- walkthrough.sh, demo.sh and evacuate.sh call `shunt-mover run`.

A rewrite of the mover onto `internal/sigv4` stays a separate question. It is only worth doing if it first reproduces the property test's evidence.
