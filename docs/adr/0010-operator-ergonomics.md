# ADR-0010: A default tenant, a config-free lab start, and clusters added from a URL

Status: accepted (POC-5 follow-up, 2026-09-16). Amends ADR-0008 (how a cluster's secret reaches shunt). Source: two manual VAST walkthroughs; docs/DESIGN.md decisions 13–14, §3b item 2 (`shunt adopt <cluster> <bucket> --tenant <t>`), §2.10.

## Context

The manual walkthroughs on VAST worked, but the operator had to learn three things that had nothing to do with moving a bucket.

1. **Tenants.** Every bucket was addressed as `acme/demo-source`, and the client key had to name `acme`. Tenants exist so that one shunt can front many customers, each with its own namespace (decision 13). A single team gets nothing from them.
2. **Three files before the first command.** `shunt serve` needed a config file, a credentials file, and an existing directory file.
3. **A cluster was eight flags,** each with its own chance to be wrong: `--type --scheme --region --endpoint --access-key --secret-ref`, plus capabilities typed by hand. The secret was an `env:` ref, read by each process from its own environment. Both walkthroughs lost time to a terminal holding a stale secret while the others had the new one.

## Decision

**A default tenant.**
- **Client keys:** a key that names no tenant belongs to tenant `default`.
- **CLI arguments:** every verb takes a bare bucket name for the default tenant's bucket, and `tenant/bucket` still names a tenant.
- **Output:** the CLI never shows the default tenant, in placement keys, reference lists, or refusals.
- **Multi-tenancy is unchanged:** tenants are still the namespace, the directory key is still `(tenant, bucket)`, and nothing else changes for them.

**`shunt serve --plaintext` needs no config.**
- **State directory:** without `--config`, serve uses a state directory (`--state-dir`, default `shunt-data`), created on first start with mode 0700.
- **What it holds:**
  - the directory file;
  - `secrets/`;
  - `credentials.yaml`, holding one generated client key for the default tenant.
- **Listeners:** `--listen 127.0.0.1:8008`, and `--admin 127.0.0.1:9900` (the CLI's default API address).
- **Plain http stays an explicit opt-in:** without `--config`, `--plaintext` is required.
- **The client secret is never logged or printed by serve** (DESIGN §3 P2 item 3). `shunt client show` reads it from the 0600 file on the host.
- **Production:** keeps `--config` for TLS, tokens and domains; the lab flags are refused alongside it.

**`shunt cluster add <name> <url> --access-key AK`**
- **URL:** gives the scheme, host and port, defaulting to 80 or 443.
- **Type:** read from the cluster's `Server` response header, which is measured, not guessed from the hostname: `vast …` means VAST, `MinIO` means MinIO, `AmazonS3` means AWS, anything else (Garage sends none) is generic `s3`.
- **Region:** `us-east-1` by default, or the region in an `s3.<region>.amazonaws.com` host.
- **Overrides:** `--type` and `--region` still override. `--scheme`/`--endpoint` still work in place of a URL.

**Capabilities are measured by `expand`.** A cluster added without `--conditional-write`/`--conditional-delete` has them unset. `expand` measures them on the target bucket before recording it as the target:
- **conditional write:** a PUT with `If-None-Match: *` over an existing scratch object must get 412;
- **conditional delete:** a DELETE with a mismatched `If-Match` must get 412.

It records the results on the cluster. A capability stated explicitly is left alone.

**Secrets can be given to shunt, which stores them.**
- **Prompt:** when `cluster add` has no `--secret-ref`, it prompts for the secret without echo (`golang.org/x/term`), or reads one line from stdin when stdin is not a terminal.
- **Transport and storage:** the secret travels once, in the `POST /v1/clusters` request, over the admin listener (loopback-only, or bearer-token protected). shunt writes it to `<secrets_dir>/<cluster>-<random>` with mode 0600, and the cluster's `secret_ref` becomes that `file:` path.
- **The rule this amends:** ADR-0008 said secrets arrive only as `secret_ref`. They still never appear in the directory file, in a log, or in an API response.
- **Lifecycle:** a refused add removes the file it wrote. A replaced secret gets a new file name, so the proxy rebuilds the cluster, and the old file is removed. Removing the cluster removes its files.
- **Other processes:** the mover reads the same file, so `migrate run` must run on a host that can read `secrets_dir`, the same host in the lab.
- **Managed secrets:** `--secret-ref env:…|file:…` still works for operators who manage secrets themselves.

## Consequences

- **The lab demo becomes all commands:** `shunt serve --plaintext`, then `cluster add`, `adopt`, `expand`, `ramp`, `migrate`, `cutover`, `purge-source` and `cluster remove`, with no file written by hand. README.md is that demo, rehearsed as written on Garage and MinIO.
- **A secret now crosses the control API once.** With no `admin.control_token_ref` the API answers loopback peers only, so on a lab host the secret never leaves the machine. A remote operator needs the token and, today, trusts the admin listener's transport. That listener is plain http, so over a network use a token and an ssh tunnel until the admin listener gets TLS.
- **Type from `Server` depends on backends keeping that header.** A misidentified cluster changes only `<Location>` warnings and backend-compat notes, not routing, and `--type` fixes it.
- **Measured capabilities replace the profile's defaults** the first time a cluster becomes a target. A backend upgraded afterwards keeps its recorded values until the cluster is re-added or given explicit flags. That is the same re-probe rule as backend-compat.md.
- **`shunt client show`** joins the operator verbs (DESIGN §2.10).
- **New dependency:** `golang.org/x/term` (docs/deps.md).
