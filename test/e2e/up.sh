#!/usr/bin/env bash
# Bring up Garage and MinIO, wait until both answer their health endpoints, and apply Garage's
# single-node layout (a fresh Garage refuses S3 calls until a layout is applied).
# Usage: COMPOSE="docker compose" test/e2e/up.sh
set -euo pipefail

COMPOSE="${COMPOSE:-docker compose}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMEOUT="${TIMEOUT:-120}"
GARAGE_ADMIN="http://127.0.0.1:3903"
GARAGE_TOKEN="shunt-e2e-admin-token"
MINIO_HEALTH="http://127.0.0.1:9000/minio/health/live"

cd "$DIR"
mkdir -p data/garage/meta data/garage/data data/minio
$COMPOSE up -d

wait_for() { # name url [curl args...]
  local name=$1 url=$2; shift 2
  local deadline=$((SECONDS + TIMEOUT))
  until curl -fsS "$@" "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "e2e-up: $name not healthy after ${TIMEOUT}s" >&2
      $COMPOSE logs --tail=50 >&2
      exit 1
    fi
    sleep 2
  done
  echo "e2e-up: $name healthy ($url)"
}

wait_for minio  "$MINIO_HEALTH"
wait_for garage "$GARAGE_ADMIN/health" -H "Authorization: Bearer $GARAGE_TOKEN"

# Garage layout: assign the single node 1 GiB of capacity in zone "e2e" and apply it, once.
node_id=$($COMPOSE exec -T garage /garage status 2>/dev/null | awk '/^[0-9a-f]{16}/ {print $1; exit}')
if [[ -z "$node_id" ]]; then
  echo "e2e-up: could not read garage node id" >&2; exit 1
fi
if $COMPOSE exec -T garage /garage layout show 2>/dev/null | grep -q "$node_id"; then
  echo "e2e-up: garage layout already applied for $node_id"
else
  $COMPOSE exec -T garage /garage layout assign -z e2e -c 1G "$node_id" >/dev/null 2>&1
  version=$($COMPOSE exec -T garage /garage layout show 2>/dev/null | sed -n 's/.*--version \([0-9]*\).*/\1/p' | head -1)
  $COMPOSE exec -T garage /garage layout apply --version "${version:-1}" >/dev/null 2>&1
  echo "e2e-up: garage layout applied (node $node_id)"
fi

# S3-level readiness: any HTTP status from an unsigned GET / counts (403 is fine, refused is not).
for name in garage:3900 minio:9000; do
  deadline=$((SECONDS + TIMEOUT))
  until code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${name#*:}/") && [[ "$code" != "000" ]]; do
    (( SECONDS < deadline )) || { echo "e2e-up: ${name%%:*} S3 port not answering" >&2; exit 1; }
    sleep 2
  done
  echo "e2e-up: ${name%%:*} S3 answers on :${name#*:} (HTTP $code unsigned)"
done

echo "e2e-up: ready — garage s3 127.0.0.1:3900 (region garage), minio 127.0.0.1:9000 (minioadmin/minioadmin)"
