#!/usr/bin/env bash
# Bring up Garage and MinIO, wait until both answer their health endpoints, and apply Garage's
# single-node layout (a fresh Garage refuses S3 calls until a layout is applied).
# Usage: COMPOSE="docker compose" test/e2e/up.sh
set -euo pipefail

COMPOSE="${COMPOSE:-docker compose}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMEOUT="${TIMEOUT:-120}"
GARAGE_ADMIN="http://127.0.0.1:3903"
MINIO_HEALTH="http://127.0.0.1:9000/minio/health/live"

cd "$DIR"
mkdir -p data/garage/meta data/garage/data data/minio
$COMPOSE up -d

wait_for() { # name url [curl args...]  — waits for HTTP 200
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

wait_for_any() { # name url — waits for any HTTP status (the port answers)
  local name=$1 url=$2 code
  local deadline=$((SECONDS + TIMEOUT))
  until code=$(curl -s -o /dev/null -w '%{http_code}' "$url") && [[ "$code" != "000" ]]; do
    (( SECONDS < deadline )) || { echo "e2e-up: $name not answering after ${TIMEOUT}s" >&2; exit 1; }
    sleep 2
  done
  echo "e2e-up: $name answers ($url, HTTP $code)"
}

wait_for minio "$MINIO_HEALTH"

# Garage 2.x: /health returns 503 until a layout is applied, so wait for the admin port to answer,
# apply the single-node layout, then wait for /health to go 200. (1.x returned 200 with no layout.)
wait_for_any garage-admin "$GARAGE_ADMIN/health"
node_id=$($COMPOSE exec -T garage /garage status 2>/dev/null | awk '/^[0-9a-f]{16}/ {print $1; exit}')
if [[ -z "$node_id" ]]; then
  echo "e2e-up: could not read garage node id" >&2; exit 1
fi
if $COMPOSE exec -T garage /garage layout show 2>/dev/null | sed '/STAGED ROLE CHANGES/q' | grep -q "$node_id"; then
  echo "e2e-up: garage layout already applied for $node_id"
else
  $COMPOSE exec -T garage /garage layout assign -z e2e -c 1G "$node_id" >/dev/null 2>&1
  version=$($COMPOSE exec -T garage /garage layout show 2>/dev/null | sed -n 's/.*layout apply --version \([0-9]*\).*/\1/p' | head -1)
  $COMPOSE exec -T garage /garage layout apply --version "${version:-1}" >/dev/null 2>&1
  echo "e2e-up: garage layout applied (node $node_id)"
fi
wait_for garage "$GARAGE_ADMIN/health"

# Garage credentials for the tests: one key allowed to create buckets. Idempotent; the secret is
# written to data/garage.env for `source`-ing (GARAGE_ACCESS_KEY / GARAGE_SECRET).
if ! $COMPOSE exec -T garage /garage key info shunt-e2e >/dev/null 2>&1; then
  $COMPOSE exec -T garage /garage key create shunt-e2e >/dev/null 2>&1
  $COMPOSE exec -T garage /garage key allow --create-bucket shunt-e2e >/dev/null 2>&1
  echo "e2e-up: garage key shunt-e2e created"
fi
info=$($COMPOSE exec -T garage /garage key info --show-secret shunt-e2e 2>/dev/null)
ak=$(echo "$info" | awk '/^Key ID:/ {print $3}')
sk=$(echo "$info" | awk '/^Secret key:/ {print $3}')
if [[ -z "$ak" || -z "$sk" ]]; then
  echo "e2e-up: could not read garage key (output was: $info)" >&2; exit 1
fi
# shunt-issued client credentials for resign mode: generated once, never committed. Tenant e2e is
# used by the single-backend resign runs; e2e-a and e2e-b are the two tenants of the POC-3 mixed run.
gen_key() { echo "SHUNT$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n' | tr 'a-f' 'A-F')"; }
gen_secret() { head -c 30 /dev/urandom | base64 | tr -d '\n/+='; }
if [[ ! -s data/credentials.yaml ]] || ! grep -q 'tenant: e2e-b$' data/credentials.yaml; then
  {
    echo "credentials:"
    for tenant in e2e e2e-a e2e-b; do
      printf '  - access_key: %s\n    secret: %s\n    tenant: %s\n' "$(gen_key)" "$(gen_secret)" "$tenant"
    done
  } > data/credentials.yaml
  chmod 600 data/credentials.yaml
  echo "e2e-up: shunt client credentials written to test/e2e/data/credentials.yaml"
fi
cred() { # cred <tenant> <access_key|secret>
  awk -v t="$1" -v f="$2" '/access_key:/ {ak=$3} /^ +secret:/ {sk=$2} /tenant:/ { if ($2 == t) { print (f == "access_key" ? ak : sk); exit } }' data/credentials.yaml
}
{
  printf 'export GARAGE_ACCESS_KEY=%s\nexport GARAGE_SECRET=%s\nexport MINIO_ACCESS_KEY=minioadmin\nexport MINIO_SECRET=minioadmin\n' "$ak" "$sk"
  printf 'export SHUNT_ACCESS_KEY=%s\nexport SHUNT_SECRET=%s\n' "$(cred e2e access_key)" "$(cred e2e secret)"
  printf 'export SHUNT_A_ACCESS_KEY=%s\nexport SHUNT_A_SECRET=%s\n' "$(cred e2e-a access_key)" "$(cred e2e-a secret)"
  printf 'export SHUNT_B_ACCESS_KEY=%s\nexport SHUNT_B_SECRET=%s\n' "$(cred e2e-b access_key)" "$(cred e2e-b secret)"
} > data/garage.env
chmod 600 data/garage.env
echo "e2e-up: credentials written to test/e2e/data/garage.env"
cp shunt-garage-resign.yaml shunt-mixed.yaml shunt-minio-resign.yaml data/
for d in directory-garage.yaml directory-mixed.yaml directory-minio.yaml; do
  sed "s/GK_SET_BY_E2E/$ak/" "$d" > "data/$d"
done
echo "e2e-up: resign configs and their directories (clusters included) written to test/e2e/data/"

# S3-level readiness: any HTTP status from an unsigned GET / counts (403 is fine, refused is not).
wait_for_any garage-s3 "http://127.0.0.1:3900/"
wait_for_any minio-s3  "http://127.0.0.1:9000/"

echo "e2e-up: ready — garage s3 127.0.0.1:3900 (region garage), minio 127.0.0.1:9000 (minioadmin/minioadmin)"
