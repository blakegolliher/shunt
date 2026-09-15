#!/usr/bin/env bash
# Fails when any committed cluster config or make target disables TLS certificate verification.
# POC-3 must not start multi-cluster work while this fails (docs/STATUS.md, carried gap from POC-2).
# Run: make check-tls-verify
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

# Uncommented occurrences only; config-validation fixtures under internal/config/testdata may test
# the key on purpose and are not deployable configs.
yaml=$(git ls-files '*.yaml' '*.yml' | grep -v '^internal/config/testdata/' || true)
hits=$(
  { [ -n "$yaml" ] && grep -nHE 'insecure_skip_verify:[[:space:]]*true' $yaml
    grep -nHE '(^|[[:space:]])(--insecure|-direct-insecure)([[:space:]]|$)' Makefile
  } 2>/dev/null | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' || true
)
if [ -n "$hits" ]; then
  echo "TLS certificate verification is disabled at:"
  echo "$hits" | sed 's/^/  /'
  echo "Install a certificate that covers the backend hostname, set tls.ca, and remove these."
  exit 1
fi
echo "TLS verification: no committed config or make target disables it."
