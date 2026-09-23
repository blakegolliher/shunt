#!/usr/bin/env bash
# Persistent local fleet for the browser demo. Unlike fleet.sh this is a fixture, not a test: it
# leaves three control nodes and two proxies running until --down.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
WORK=${SHUNT_DEMO_UI_WORK:-$ROOT/test/e2e/data/demo-ui}
PIDS=$WORK/pids
A_LISTEN=127.0.0.1:8038; A_ADMIN=127.0.0.1:9938
B_LISTEN=127.0.0.1:8048; B_ADMIN=127.0.0.1:9948
C1_API=127.0.0.1:9951; C2_API=127.0.0.1:9952; C3_API=127.0.0.1:9953
C1_PEER=127.0.0.1:9961; C2_PEER=127.0.0.1:9962; C3_PEER=127.0.0.1:9963
# SHUNT_DEMO_UI_BIND=0.0.0.0 (or one IPv4 address) serves the UI/control APIs and the two S3 listeners
# there instead of loopback, for a browser on another host. Peers and admin listeners stay loopback,
# and the local addresses above still reach everything. The control API is plain http: the bearer
# token, cluster secrets and client secrets then cross the network in the clear.
BIND=${SHUNT_DEMO_UI_BIND:-127.0.0.1}
case $BIND in
  127.*) EXPOSED= ;;
  *[!0-9.]*|'') echo "SHUNT_DEMO_UI_BIND must be an IPv4 address or 0.0.0.0, not $BIND" >&2; exit 2 ;;
  *) EXPOSED=1 ;;
esac
bound() { printf '%s:%s' "$BIND" "${1##*:}"; }

case $WORK in
  "$ROOT"/test/e2e/data/*|/tmp/*) ;;
  *) echo "refusing unsafe demo work directory: $WORK" >&2; exit 1 ;;
esac
case $WORK in /|/tmp|"$ROOT"|"") echo "refusing unsafe demo work directory: $WORK" >&2; exit 1 ;; esac

down() {
  [ -f "$PIDS" ] || { echo "demo UI is not running ($PIDS does not exist)"; return 0; }
  while read -r name pid; do
    case $pid in (*[!0-9]*|'') continue ;; esac
    target=$pid; case $name in (*-group) target=-$pid ;; esac
    if kill -0 -- "$target" 2>/dev/null; then echo "stopping $name ($pid)"; kill -TERM -- "$target" 2>/dev/null || true; fi
  done < "$PIDS"
  for _ in $(seq 1 100); do
    live=0
    while read -r name pid; do target=$pid; case $name in (*-group) target=-$pid ;; esac; kill -0 -- "$target" 2>/dev/null && live=$((live+1)) || true; done < "$PIDS"
    [ "$live" = 0 ] && break
    sleep 0.1
  done
  while read -r name pid; do target=$pid; case $name in (*-group) target=-$pid ;; esac; kill -0 -- "$target" 2>/dev/null && kill -KILL -- "$target" 2>/dev/null || true; done < "$PIDS"
  mv "$PIDS" "$PIDS.stopped"
}

if [ "${1:-}" = --down ]; then down; exit 0; fi
if [ "${1:-}" = --verify-watcher ]; then
  SHUNT=$ROOT/bin/shunt
  TOKEN=$(cat "$WORK/secrets/control.token")
  # shellcheck disable=SC1091
  . "$ROOT/test/e2e/data/garage.env"
  child=
  stop_watcher() {
    if [ -n "$child" ]; then kill -TERM "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi
    exit 0
  }
  trap stop_watcher INT TERM
  printf '%s\n' "$$" > "$WORK/verify.pid"
  for _ in $(seq 1 3600); do
    if curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/placements/default/ui-demo/view" >/dev/null; then
      while :; do
        "$SHUNT" verify --endpoint "http://$A_LISTEN" --bucket ui-demo --access-key "$GARAGE_ACCESS_KEY" \
          --secret-ref "file:$WORK/secrets/client.secret" --workers 24 --keys 200 --duration 60s --interval 0 \
          --cleanup --json-out "$WORK/verify.json" &
        child=$!
        wait "$child" || exit
        child=
      done
    fi
    sleep 1
  done
  echo "timed out waiting for default/ui-demo" >&2
  exit 1
fi
[ $# = 0 ] || { echo "usage: $0 [--down]" >&2; exit 2; }

SHUNT=$ROOT/bin/shunt; CONTROL=$ROOT/bin/shunt-control
[ -x "$SHUNT" ] && [ -x "$CONTROL" ] || { echo "run make ui and make build first" >&2; exit 1; }
[ -f "$ROOT/test/e2e/data/garage.env" ] || { echo "run make e2e-up first" >&2; exit 1; }
command -v aws >/dev/null && command -v jq >/dev/null || { echo "demo-ui needs aws-cli and jq" >&2; exit 1; }
# shellcheck disable=SC1091
. "$ROOT/test/e2e/data/garage.env"

if [ -f "$PIDS" ]; then
  existing=0
  while read -r _ pid; do kill -0 "$pid" 2>/dev/null && existing=1 || true; done < "$PIDS"
  [ "$existing" = 0 ] || { echo "demo UI already has a live process; run make demo-ui-down first" >&2; exit 1; }
fi
for addr in $A_LISTEN $A_ADMIN $B_LISTEN $B_ADMIN $C1_API $C2_API $C3_API $C1_PEER $C2_PEER $C3_PEER; do
  if (exec 3<>"/dev/tcp/${addr%:*}/${addr##*:}") 2>/dev/null; then echo "$addr is already in use" >&2; exit 1; fi
done

rm -rf "$WORK"
mkdir -p "$WORK/secrets"
chmod 700 "$WORK" "$WORK/secrets"
umask 077
TOKEN=demo-ui-token-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')
printf '%s' "$TOKEN" > "$WORK/secrets/control.token"
printf '%s' "$GARAGE_SECRET" > "$WORK/secrets/client.secret"
cat > "$WORK/demo.env" <<EOF
SHUNT_UI_URL=http://$C1_API/
SHUNT_PROXY_URL=http://$A_LISTEN
SHUNT_CONTROL_TOKEN=$TOKEN
GARAGE_ENDPOINT=127.0.0.1:3900
GARAGE_ACCESS_KEY=$GARAGE_ACCESS_KEY
GARAGE_SECRET=$GARAGE_SECRET
MINIO_ENDPOINT=127.0.0.1:9000
MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY
MINIO_SECRET=$MINIO_SECRET
EOF
umask 022

export AWS_CONFIG_FILE=$WORK/aws.config AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
printf '[default]\ns3 =\n  addressing_style = path\n' > "$AWS_CONFIG_FILE"
on_garage() { AWS_ACCESS_KEY_ID=$GARAGE_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$GARAGE_SECRET aws --endpoint-url http://127.0.0.1:3900 --region garage "$@"; }
on_minio() { AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$MINIO_SECRET aws --endpoint-url http://127.0.0.1:9000 --region us-east-1 "$@"; }
for side in on_garage on_minio; do
  if $side s3api head-bucket --bucket ui-demo >/dev/null 2>&1; then $side s3 rb s3://ui-demo --force >/dev/null; fi
  if $side s3api head-bucket --bucket ui-demo-001 >/dev/null 2>&1; then $side s3 rb s3://ui-demo-001 --force >/dev/null; fi
done
on_garage s3api create-bucket --bucket ui-demo >/dev/null
for i in $(seq 1 40); do printf 'shunt UI seed %03d\n' "$i" | on_garage s3 cp --quiet - "s3://ui-demo/seed/object-$i.txt"; done

for p in a b; do
  listen=$A_LISTEN; admin=$A_ADMIN; [ "$p" = b ] && { listen=$B_LISTEN; admin=$B_ADMIN; }
  cat > "$WORK/$p.yaml" <<EOF
listener: { address: "$(bound "$listen")", plaintext: true }
admin: { address: "$admin" }
auth: { mode: resign }
features: { debug_route_header: true }
control:
  endpoints: ["http://$C1_API", "http://$C2_API", "http://$C3_API"]
  token_ref: file:$WORK/secrets/control.token
  plaintext: true
  proxy_id: proxy-$p
  cache_dir: $WORK/cache-$p
  heartbeat_interval: 1s
  lease_ttl: 10s
EOF
  "$SHUNT" check-config "$WORK/$p.yaml" >/dev/null
done

: > "$PIDS"
cleanup_error() { rc=$?; echo "demo-ui startup failed; logs are in $WORK" >&2; down; exit "$rc"; }
trap cleanup_error ERR INT TERM
wait_http() { for _ in $(seq 1 200); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
start_control() {
  n=$1; mode=$2
  case $n in 1) api=$C1_API; peer=$C1_PEER ;; 2) api=$C2_API; peer=$C2_PEER ;; 3) api=$C3_API; peer=$C3_PEER ;; esac
  exposure=(); [ -n "$EXPOSED" ] && exposure=(--plaintext)
  if [ "$mode" = init ]; then
    nohup setsid "$CONTROL" init --name "c$n" --data-dir "$WORK/c$n" --peer-url "http://$peer" --api "$(bound "$api")" "${exposure[@]}" --token-ref "file:$WORK/secrets/control.token" --lease-ttl 10s > "$WORK/c$n.log" 2>&1 &
  else
    nohup setsid "$CONTROL" join --name "c$n" --data-dir "$WORK/c$n" --peer-url "http://$peer" --api "$(bound "$api")" "${exposure[@]}" --token-ref "file:$WORK/secrets/control.token" --lease-ttl 10s --existing "http://$C1_API" > "$WORK/c$n.log" 2>&1 &
  fi
  pid=$!; printf 'c%s-group %s\n' "$n" "$pid" >> "$PIDS"
  wait_http "http://$api/-/healthz"
}
start_control 1 init
start_control 2 join
start_control 3 join
for p in a b; do
  nohup setsid "$SHUNT" serve -c "$WORK/$p.yaml" > "$WORK/$p.log" 2>&1 &
  pid=$!; printf 'proxy-%s-group %s\n' "$p" "$pid" >> "$PIDS"
  admin=$A_ADMIN; [ "$p" = b ] && admin=$B_ADMIN
  wait_http "http://$admin/-/healthz"
done

# Run one-minute checked workloads continuously once the UI adopts default/ui-demo and imports
# Garage's key. The watcher writes its post-setsid PID so teardown targets its actual process group
# even on hosts where setsid has to fork.
rm -f "$WORK/verify.pid"
nohup setsid "$0" --verify-watcher > "$WORK/verify.log" 2>&1 &
for _ in $(seq 1 100); do [ -s "$WORK/verify.pid" ] && break; sleep 0.05; done
[ -s "$WORK/verify.pid" ]
printf 'verify-group %s\n' "$(cat "$WORK/verify.pid")" >> "$PIDS"

for _ in $(seq 1 100); do
  fleet=$(curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/fleet" || true)
  [ -n "$fleet" ] && [ "$(jq '[.members[] | select(.live)] | length' <<<"$fleet")" = 2 ] && break
  sleep 0.2
done
[ "$(jq '[.members[] | select(.live)] | length' <<<"$fleet")" = 2 ]
trap - ERR INT TERM

UI_HOST=127.0.0.1
if [ -n "$EXPOSED" ]; then
  UI_HOST=$BIND
  [ "$BIND" = 0.0.0.0 ] && UI_HOST=$(hostname -I | awk '{print $1}') # one of several; the note below lists them
fi

cat <<EOF

Shunt UI demo fleet is ready.

  UI:             http://$UI_HOST:${C1_API##*:}/
  bearer token:   $TOKEN
  S3 proxy:       http://$UI_HOST:${A_LISTEN##*:} (and :${B_LISTEN##*:})
  source bucket:  ui-demo (40 seeded objects)

  Garage cluster: name garage, endpoint 127.0.0.1:3900, region garage
    access key:   $GARAGE_ACCESS_KEY
    secret:       $GARAGE_SECRET
  MinIO cluster:  name minio, endpoint 127.0.0.1:9000, region us-east-1
    access key:   $MINIO_ACCESS_KEY
    secret:       $MINIO_SECRET

The verifier starts when default/ui-demo is adopted with the Garage client key.
Logs and the mode-0600 environment file are in $WORK.
Follow docs/demo-ui.md. Stop everything with: make demo-ui-down
EOF
if [ -n "$EXPOSED" ]; then
  [ "$BIND" = 0.0.0.0 ] && printf '\nThis host answers on: %s\n' "$(hostname -I | xargs)"
  printf '\nListening on %s: the control API is plain http, so the token and every secret entered in the UI\ncross the network in the clear. Use it only on a network you trust.\n' "$BIND"
fi
