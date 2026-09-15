# Getting started on a Mac

Everything in the POC track builds, tests, and runs natively on macOS. The two S3 backends used
by the end-to-end tests (Garage, MinIO) are containers, so you need a container runtime, which on
a Mac is a small Linux VM either way. Only P4's eBPF sampler and the recorded benchmarks need
real Linux; neither is on the path yet.

Every block below is copy-paste. Lines starting with `#` are comments.

## 1. Tools (once)

```bash
# Homebrew, if you don't have it: https://brew.sh
brew install go git make awscli
go version    # needs go1.27 or newer; go.mod says 1.27
```

Container runtime, pick one:

```bash
# Option A (recommended): OrbStack — light, fast, docker-compatible
brew install --cask orbstack
# Option B: Docker Desktop
brew install --cask docker
# Option C: podman
brew install podman && podman machine init && podman machine start
```

Check it works and has a compose command:

```bash
docker compose version || podman compose version
```

## 2. Clone and hook up

```bash
git clone git@github.com:blakegolliher/shunt.git
cd shunt
git config core.hooksPath scripts/githooks   # strips tool attribution from commit messages
make tools                                    # pins golangci-lint into ./bin
```

## 3. Build and run the gate

```bash
make all     # build, lint, test, race, fuzz (30 s per target). ~2 minutes.
```

Green means the tree is healthy. If `lint` fails on a fresh clone, run `make tools` again and check `go version`.

## 4. Bring up the backends

```bash
make e2e-up
```

This generates a self-signed wildcard cert into `test/e2e/certs/`, starts Garage and MinIO,
waits for both to be healthy, applies Garage's single-node layout, creates a Garage API key,
and writes credentials to `test/e2e/data/garage.env`. It is idempotent; run it again any time.

If the Garage image pull fails with "short-name resolution", your runtime is podman and the
image references are already fully qualified; check `podman machine` is running.

## 5. Run shunt in front of Garage

Terminal 1:

```bash
make run-garage        # listens on https://127.0.0.1:8443, admin on http://127.0.0.1:9900
```

Terminal 2, a smoke test with the AWS CLI (path-style, self-signed cert so verification is off):

```bash
source test/e2e/data/garage.env
export AWS_ACCESS_KEY_ID=$GARAGE_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$GARAGE_SECRET AWS_DEFAULT_REGION=garage
alias s3='aws --endpoint-url https://127.0.0.1:8443 --no-verify-ssl s3'
s3 mb s3://hello
echo "hi from a mac" > /tmp/hi.txt
s3 cp /tmp/hi.txt s3://hello/hi.txt
s3 ls s3://hello
s3 cp s3://hello/hi.txt -
s3 rm s3://hello/hi.txt && s3 rb s3://hello
```

What to look at while it runs:

```bash
curl -s http://127.0.0.1:9900/-/healthz
curl -s http://127.0.0.1:9900/-/metrics | grep '^shunt_'
curl -s http://127.0.0.1:9900/-/slow | python3 -m json.tool
tail -f test/e2e/data/shunt-garage.access.jsonl
```

## 6. The differential test

With shunt still running in terminal 1:

```bash
make s3diff BACKEND=garage     # 202 cases direct vs via shunt; expect "0 diffs"
```

Then the same against MinIO: stop terminal 1 (Ctrl-C drains cleanly), `make run-minio`, and

```bash
make s3diff BACKEND=minio
```

## 7. Bench (optional, numbers are not comparable across machines)

```bash
make bench-e2e BACKEND=garage    # prints a markdown table; ~10 minutes
make bench                        # Go micro-benchmarks → test/bench/new.txt
make bench-compare                # against test/bench/baseline.txt (recorded on Linux)
```

Mac numbers are for spotting regressions in your own changes, not for docs/bench/. Recorded
results come from the Linux box in docs/CONTEXT.md.

## 8. Stopping and cleaning up

```bash
make e2e-down     # removes the containers and their data; certs are kept
```

## Before you write code

Read, in order: `CLAUDE.md`, `docs/DESIGN.md`, `docs/CONTEXT.md`, `docs/STATUS.md`,
`docs/POC.md`, `docs/adr/`. Then the current phase's prompt in `docs/POC.md`.

Rules that bite early: unknown config keys are errors; every metric needs a row in
`docs/telemetry-catalog.md` before code; every dependency needs a line in `docs/deps.md`; commit
messages carry no tool attribution (the hook enforces it); the lift procedure in `CLAUDE.md`
applies to any third-party code copied in.

## Known Mac differences

| Thing | On the Linux box | On a Mac |
|---|---|---|
| Container runtime | podman 5 behind a docker shim | OrbStack / Docker Desktop / podman machine; the Makefile detects the compose command |
| `:z` volume labels in compose | SELinux relabel | ignored |
| `openssl` | OpenSSL 3 | LibreSSL; the cert script uses a config file, which both accept |
| Go toolchain | `/usr/local/go`, `GOTOOLCHAIN=auto` | Homebrew Go |
| P4 eBPF `sock_ops` and `TCP_INFO` | works | Linux only; use a Linux VM or devcontainer for that phase |
| Bench results | recorded | local only |
