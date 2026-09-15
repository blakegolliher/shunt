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

.PHONY: all build test race lint fuzz bench bench-compare tools tidy clean e2e-up e2e-down e2e-cert run-garage run-minio run-garage-resign run-minio-resign s3diff bench-e2e help

all: build lint test race fuzz ## build, lint, test, race, fuzz — the CI gate

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

# s3diff and the e2e bench need shunt running (`make run-garage` / `run-minio`) and credentials
# from test/e2e/data/garage.env (written by e2e-up). BACKEND=garage|minio.
BACKEND ?= garage
ifeq ($(BACKEND),garage)
S3_REGION := garage
S3_ADDR   := 127.0.0.1:3900
S3_AK     := $$GARAGE_ACCESS_KEY
S3_SK     := $$GARAGE_SECRET
else
S3_REGION := us-east-1
S3_ADDR   := 127.0.0.1:9000
S3_AK     := minioadmin
S3_SK     := minioadmin
endif

s3diff: ## differential test direct vs via shunt (BACKEND=garage|minio)
	. $(E2E_DIR)/data/garage.env && AWS_ACCESS_KEY_ID=$(S3_AK) AWS_SECRET_ACCESS_KEY=$(S3_SK) \
	  $(GO) run ./test/s3diff -region $(S3_REGION) -direct-addr $(S3_ADDR) $(S3DIFF_ARGS)

bench-e2e: ## direct vs via bench (BACKEND=garage|minio), prints a markdown table
	. $(E2E_DIR)/data/garage.env && AWS_ACCESS_KEY_ID=$(S3_AK) AWS_SECRET_ACCESS_KEY=$(S3_SK) \
	  $(GO) run ./test/bench/s3bench -backend $(BACKEND) -region $(S3_REGION) -direct-addr $(S3_ADDR) $(BENCH_ARGS)

run-garage-resign: build ## run shunt in resign mode in front of the e2e Garage (foreground)
	. $(E2E_DIR)/data/garage.env && $(BIN)/shunt serve --config $(E2E_DIR)/data/shunt-garage-resign.yaml

run-minio-resign: build ## run shunt in resign mode in front of the e2e MinIO (foreground)
	. $(E2E_DIR)/data/garage.env && $(BIN)/shunt serve --config $(E2E_DIR)/data/shunt-minio-resign.yaml

e2e-down: ## tear down the e2e backends and their data
	cd $(E2E_DIR) && $(COMPOSE) down -v --remove-orphans
	rm -rf $(E2E_DIR)/data

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'
