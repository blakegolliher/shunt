# shunt — build and verification targets.
# Tool binaries are pinned here and installed into ./bin; they never enter go.mod.

SHELL := /bin/bash
.DEFAULT_GOAL := all

MODULE        := github.com/blakegolliher/shunt
BIN           := $(CURDIR)/bin
GO            ?= go
GOFLAGS       ?=
PKGS          := ./...
FUZZ_TIME     ?= 30s
BENCH_TIME    ?= 1s
BENCH_COUNT   ?= 6

# Current releases as of 2026-09-14 (Go 1.27.1 on the dev box; both need Go >= 1.26).
GOLANGCI_LINT_VERSION ?= v2.13.2
BENCHSTAT             ?= golang.org/x/perf/cmd/benchstat@v0.0.0-20260908200009-22c9c6c9d4da
GOLANGCI_LINT         := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)

VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS   := -s -w \
  -X main.version=$(VERSION) \
  -X main.commit=$(COMMIT) \
  -X main.date=$(BUILD_DATE)

# e2e. COMPOSE is detected: docker compose (Docker Desktop, OrbStack, podman's docker shim),
# then podman compose, then podman-compose. Override with COMPOSE="..." if needed.
COMPOSE      ?= $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || \
                  (podman compose version >/dev/null 2>&1 && echo "podman compose" || echo "podman-compose"))
E2E_DIR      := test/e2e
DOMAIN       ?= shunt.example.com

.PHONY: all build test race lint fuzz bench bench-compare tools tidy clean e2e-up e2e-down e2e-cert run-garage run-minio run-garage-resign run-minio-resign run-vast-resign run-mixed s3diff s3diff-mixed bench-e2e probe check-tls-verify help

all: build lint test race fuzz ## build, lint, test, race, fuzz — the CI gate
	@scripts/check-tls-verify.sh >/dev/null 2>&1 || echo "WARNING: TLS verification is disabled in a committed config or make target (make check-tls-verify). POC-3 multi-cluster work must not start until it passes."

check-tls-verify: ## fail if any committed config or make target disables TLS verification (POC-3 preflight)
	@scripts/check-tls-verify.sh

build: ## compile ./cmd/shunt into ./bin/shunt
	@mkdir -p $(BIN)
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/shunt ./cmd/shunt

test: ## unit tests
	$(GO) test $(PKGS)

race: ## unit tests under the race detector
	$(GO) test -race $(PKGS)

lint: $(GOLANGCI_LINT) ## golangci-lint with .golangci.yml
	$(GOLANGCI_LINT) run $(PKGS)
	$(GO) vet $(PKGS)

# Runs every Fuzz* function in the tree for FUZZ_TIME each. Seed corpus only; no corpus is committed.
fuzz: ## run every fuzz target for FUZZ_TIME (default 30s)
	@set -euo pipefail; \
	found=0; \
	for pkg in $$($(GO) list $(PKGS)); do \
	  for fn in $$($(GO) test -list '^Fuzz' $$pkg 2>/dev/null | grep '^Fuzz' || true); do \
	    found=1; echo "fuzz $$pkg $$fn $(FUZZ_TIME)"; \
	    $(GO) test -run '^$$' -fuzz "^$$fn$$" -fuzztime $(FUZZ_TIME) $$pkg; \
	  done; \
	done; \
	test $$found -eq 1 || { echo "no fuzz targets found"; exit 1; }

bench: ## run all benchmarks, write test/bench/new.txt
	@mkdir -p test/bench
	$(GO) test -run '^$$' -bench . -benchmem -benchtime $(BENCH_TIME) -count $(BENCH_COUNT) $(PKGS) | tee test/bench/new.txt

bench-compare: bench ## compare test/bench/new.txt against test/bench/baseline.txt with benchstat
	$(GO) run $(BENCHSTAT) test/bench/baseline.txt test/bench/new.txt

tools: $(GOLANGCI_LINT) ## install pinned tool binaries into ./bin

# The binary is named by version so a pin bump reinstalls it.
$(GOLANGCI_LINT):
	@mkdir -p $(BIN)
	GOBIN=$(BIN) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	mv $(BIN)/golangci-lint $(GOLANGCI_LINT)

tidy: ## go mod tidy and verify nothing changed
	$(GO) mod tidy
	git diff --exit-code go.mod go.sum

clean:
	rm -rf $(BIN) test/bench/new.txt

e2e-cert: ## generate a self-signed wildcard cert for $(DOMAIN) into test/e2e/certs
	DOMAIN=$(DOMAIN) $(E2E_DIR)/gen-cert.sh

e2e-up: e2e-cert ## bring up Garage and MinIO and wait until both are healthy
	COMPOSE="$(COMPOSE)" $(E2E_DIR)/up.sh

run-garage: build ## run shunt in front of the e2e Garage (foreground)
	$(BIN)/shunt serve --config $(E2E_DIR)/shunt-garage.yaml

run-minio: build ## run shunt in front of the e2e MinIO (foreground)
	$(BIN)/shunt serve --config $(E2E_DIR)/shunt-minio.yaml

# s3diff, bench-e2e, and probe take BACKEND=garage|minio|vast and MODE=passthrough|resign.
# garage/minio need `make e2e-up` (credentials in test/e2e/data/garage.env) and shunt running in
# front of that backend (`make run-<backend>` or `make run-<backend>-resign`). vast needs
# VAST_ACCESS_KEY_ID / VAST_SECRET_ACCESS_KEY exported and `make run-vast-resign`.
BACKEND      ?= garage
MODE         ?= passthrough
VAST_ENDPOINT ?= https://vast02.example.com:443
VAST_BUCKET   ?= demo-dest
ifeq ($(BACKEND),garage)
S3_REGION := garage
S3_ADDR   := 127.0.0.1:3900
S3_AK     := $$GARAGE_ACCESS_KEY
S3_SK     := $$GARAGE_SECRET
S3_BACKEND_ARGS := -direct-addr $(S3_ADDR)
PROBE_ARGS := --endpoint http://127.0.0.1:3900 --region garage --bucket probe-garage
else ifeq ($(BACKEND),minio)
S3_REGION := us-east-1
S3_ADDR   := 127.0.0.1:9000
S3_AK     := minioadmin
S3_SK     := minioadmin
S3_BACKEND_ARGS := -direct-addr $(S3_ADDR)
PROBE_ARGS := --endpoint http://127.0.0.1:9000 --region us-east-1 --bucket probe-minio
else ifeq ($(BACKEND),vast)
# TEMPORARY -direct-insecure / --insecure: the VAST lab cluster serves a self-signed factory certificate.
S3_REGION := us-east-1
S3_AK     := $$VAST_ACCESS_KEY_ID
S3_SK     := $$VAST_SECRET_ACCESS_KEY
S3_BACKEND_ARGS := -direct $(VAST_ENDPOINT) -direct-addr "" -direct-insecure -styles path -existing-bucket $(VAST_BUCKET)
PROBE_ARGS := --insecure --endpoint $(VAST_ENDPOINT) --region us-east-1 --bucket $(VAST_BUCKET)
endif

s3diff-mixed: ## POC-3: mixed-backend differential test through one shunt (needs make e2e-up and make run-mixed)
	. $(E2E_DIR)/data/garage.env && $(GO) run ./test/s3diff -mixed -config $(E2E_DIR)/data/shunt-mixed.yaml $(S3DIFF_ARGS)

s3diff: ## differential test direct vs via shunt (BACKEND=garage|minio|vast MODE=passthrough|resign)
	. $(E2E_DIR)/data/garage.env && AWS_ACCESS_KEY_ID=$(S3_AK) AWS_SECRET_ACCESS_KEY=$(S3_SK) \
	  $(GO) run ./test/s3diff -mode $(MODE) -region $(S3_REGION) $(S3_BACKEND_ARGS) $(S3DIFF_ARGS)

bench-e2e: ## direct vs via bench (BACKEND=garage|minio MODE=passthrough|resign), prints a markdown table
	. $(E2E_DIR)/data/garage.env && AWS_ACCESS_KEY_ID=$(S3_AK) AWS_SECRET_ACCESS_KEY=$(S3_SK) \
	  $(GO) run ./test/bench/s3bench -mode $(MODE) -backend $(BACKEND) -region $(S3_REGION) $(S3_BACKEND_ARGS) $(BENCH_ARGS)

probe: build ## shunt probe against BACKEND=garage|minio|vast
	. $(E2E_DIR)/data/garage.env 2>/dev/null; AWS_ACCESS_KEY_ID=$(S3_AK) AWS_SECRET_ACCESS_KEY=$(S3_SK) \
	  $(BIN)/shunt probe $(PROBE_ARGS) $(PROBE_FLAGS)

run-garage-resign: build ## run shunt in resign mode in front of the e2e Garage (foreground; resets its directory)
	cp $(E2E_DIR)/directory-garage.yaml $(E2E_DIR)/data/directory-garage.yaml
	. $(E2E_DIR)/data/garage.env && $(BIN)/shunt serve --config $(E2E_DIR)/data/shunt-garage-resign.yaml

run-minio-resign: build ## run shunt in resign mode in front of the e2e MinIO (foreground; resets its directory)
	cp $(E2E_DIR)/directory-minio.yaml $(E2E_DIR)/data/directory-minio.yaml
	. $(E2E_DIR)/data/garage.env && $(BIN)/shunt serve --config $(E2E_DIR)/data/shunt-minio-resign.yaml

run-mixed: build ## POC-3: one shunt in resign mode over the e2e Garage and MinIO (foreground; resets the mixed directory)
	cp $(E2E_DIR)/directory-mixed.yaml $(E2E_DIR)/data/directory-mixed.yaml
	rm -f $(E2E_DIR)/data/directory-mixed.yaml.changes.jsonl
	. $(E2E_DIR)/data/garage.env && $(BIN)/shunt serve --config $(E2E_DIR)/data/shunt-mixed.yaml

run-vast-resign: build ## run shunt in resign mode in front of the VAST lab cluster (foreground)
	@test -n "$$VAST_ACCESS_KEY_ID" && test -n "$$VAST_SECRET_ACCESS_KEY" || { echo "export VAST_ACCESS_KEY_ID and VAST_SECRET_ACCESS_KEY first"; exit 1; }
	@sed "s/VAST_ACCESS_KEY_SET_BY_ENV/$$VAST_ACCESS_KEY_ID/" $(E2E_DIR)/shunt-vast-resign.yaml > $(E2E_DIR)/data/shunt-vast-resign.yaml
	cp $(E2E_DIR)/directory-vast.yaml $(E2E_DIR)/data/directory-vast.yaml
	$(BIN)/shunt serve --config $(E2E_DIR)/data/shunt-vast-resign.yaml

e2e-down: ## tear down the e2e backends and their data
	cd $(E2E_DIR) && $(COMPOSE) down -v --remove-orphans
	rm -rf $(E2E_DIR)/data

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'
