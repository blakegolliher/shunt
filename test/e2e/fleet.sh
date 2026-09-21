#!/usr/bin/env bash
# POC-6 item 3, live: two shunt proxies over one directory, A the control node and B a member
# (ADR-0016), on the e2e Garage (source) and MinIO (target). It checks, with real processes:
#
#   1. B joins the fleet, and a ramp step is held until B has it ("paused their writes").
#   2. A client that writes the same keys alternately through A and B across every ramp step and
#      migrate start never loses a write: every key reads back, through both, as its last 200.
#   3. Stale mode: with A stopped (SIGSTOP), B refuses writes to a moving bucket with 503 and
#      serves reads and writes to an ACTIVE one; with A back, B writes again.
#   4. With B stopped, a first step is held, cannot reach B, and is released: the bucket is ACTIVE
#      again. A first step then waits for B even once it is silent, and names `proxy forget`.
#   5. With B stopped long enough to be silent, a later step on a moving bucket goes ahead
#      without it, and says B is silent.
#
#   make e2e-up && make fleet
set -euo pipefail

WORK=
A_LISTEN=127.0.0.1:8038; A_ADMIN=127.0.0.1:9938
B_LISTEN=127.0.0.1:8048; B_ADMIN=127.0.0.1:9948
while [ $# -gt 0 ]; do
  case "$1" in
    --work) WORK=$2; shift 2 ;;
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    *) echo "unknown flag $1" >&2; exit 2 ;;
  esac
done
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
[ -n "$WORK" ] || WORK=$ROOT/test/e2e/data/fleet
SHUNT=$ROOT/bin/shunt
[ -x "$SHUNT" ] || { echo "no $SHUNT; run make build" >&2; exit 1; }
[ -f "$ROOT/test/e2e/data/garage.env" ] || { echo "no garage.env; run make e2e-up" >&2; exit 1; }
# shellcheck disable=SC1091
. "$ROOT/test/e2e/data/garage.env"
command -v aws >/dev/null && command -v jq >/dev/null || { echo "needs aws-cli and jq" >&2; exit 1; }

say()  { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\nFLEET TEST FAILED: %s\n' "$*" >&2; exit 1; }
# refused runs a shunt command that must be refused with a message containing $1.
refused() {
  local want=$1 out; shift
  printf '   $ shunt %s   # must be refused\n' "$*"
  if out=$("$SHUNT" "$@" 2>&1); then printf '%s\n' "$out" | sed 's/^/     /'; fail "shunt $* was accepted"; fi
  printf '%s\n' "$out" | sed 's/^/     /'
  grep -q -- "$want" <<<"$out" || fail "shunt $* was refused, but not with '$want'"
}
# accepted runs a shunt command that must succeed and prints its output; OUT holds it.
accepted() {
  printf '   $ shunt %s\n' "$*"
  OUT=$("$SHUNT" "$@" 2>&1) || { printf '%s\n' "$OUT" | sed 's/^/     /'; fail "shunt $* failed"; }
  printf '%s\n' "$OUT" | sed 's/^/     /'
}

rm -rf "$WORK"; mkdir -p "$WORK/secrets"; chmod 700 "$WORK/secrets"; cd "$WORK"
export SHUNT_API=http://$A_ADMIN
unset SHUNT_API_TOKEN_REF
umask 077
printf '%s' "$GARAGE_SECRET" > secrets/garage.secret
printf '%s' "$MINIO_SECRET" > secrets/minio.secret
CLIENT_AK=SHUNTFLEET$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n' | tr a-f A-F)
CLIENT_SK=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
printf 'credentials:\n  - access_key: %s\n    secret: %s\n' "$CLIENT_AK" "$CLIENT_SK" > credentials.yaml
umask 022
printf 'version: 1\n' > directory.yaml
common="auth: { mode: resign, credentials_file: $WORK/credentials.yaml }
directory: { file: $WORK/directory.yaml, poll_interval: 1s }"
cat > a.yaml <<EOF
listener: { address: "$A_LISTEN", plaintext: true }
admin: { address: "$A_ADMIN" }
$common
control: { heartbeat_interval: 1s, lease_ttl: 3s }
EOF
cat > b.yaml <<EOF
listener: { address: "$B_LISTEN", plaintext: true }
admin: { address: "$B_ADMIN" }
$common
control: { endpoint: "http://$A_ADMIN", proxy_id: proxy-b, heartbeat_interval: 1s, lease_ttl: 3s }
EOF
"$SHUNT" check-config a.yaml >/dev/null || fail "check-config a.yaml"
"$SHUNT" check-config b.yaml >/dev/null || fail "check-config b.yaml"

export AWS_CONFIG_FILE=$WORK/aws.config AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export AWS_RETRY_MODE=standard AWS_MAX_ATTEMPTS=10
printf '[default]\ns3 =\n  addressing_style = path\n' > "$AWS_CONFIG_FILE"
on_garage() { AWS_ACCESS_KEY_ID=$GARAGE_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$GARAGE_SECRET aws --endpoint-url http://127.0.0.1:3900 --region garage "$@"; }
on_minio()  { AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$MINIO_SECRET aws --endpoint-url http://127.0.0.1:9000 --region us-east-1 "$@"; }
via()       { local p=$1; shift; AWS_ACCESS_KEY_ID=$CLIENT_AK AWS_SECRET_ACCESS_KEY=$CLIENT_SK aws --endpoint-url "http://$p" --region us-east-1 "$@"; }

A_PID=; B_PID=; WRITER_PID=
cleanup() {
  local rc=$?
  for p in "$A_PID" "$B_PID"; do [ -n "$p" ] && kill -CONT "$p" 2>/dev/null; done
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null
  for p in "$A_PID" "$B_PID"; do
    if [ -n "$p" ] && kill -0 "$p" 2>/dev/null; then kill -TERM "$p"; fi
  done
  for p in "$A_PID" "$B_PID"; do
    [ -n "$p" ] && for _ in $(seq 1 100); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  done
  [ "$rc" = 0 ] || printf '   logs: %s/a.log, %s/b.log, %s/writer.log\n' "$WORK" "$WORK" "$WORK" >&2
  return 0
}
trap cleanup EXIT
for addr in "$A_LISTEN" "$A_ADMIN" "$B_LISTEN" "$B_ADMIN"; do
  if (exec 3<>"/dev/tcp/${addr%:*}/${addr##*:}") 2>/dev/null; then fail "$addr is already in use"; fi
done

for b in fleet-a fleet-b; do
  for side in on_garage on_minio; do
    if $side s3api head-bucket --bucket $b >/dev/null 2>&1; then $side s3 rb s3://$b --force >/dev/null; fi
    $side s3api create-bucket --bucket $b >/dev/null
  done
done
for i in 1 2 3; do printf 'seed %s' $i | on_garage s3 cp --quiet - s3://fleet-a/seed/$i; done

say "0. Two proxies over one directory: A (control node) on :${A_LISTEN##*:}, B (member) on :${B_LISTEN##*:}"
nohup "$SHUNT" serve -c a.yaml > a.log 2>&1 &
A_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://$A_ADMIN/-/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 "$A_PID" 2>/dev/null || { cat a.log; fail "A exited"; }
accepted cluster add garage --type s3 --scheme http --region garage --endpoint 127.0.0.1:3900 \
  --access-key "$GARAGE_ACCESS_KEY" --secret-ref "file:$WORK/secrets/garage.secret" --conditional-write=false
accepted cluster add minio --type minio --scheme http --region us-east-1 --endpoint 127.0.0.1:9000 \
  --access-key "$MINIO_ACCESS_KEY" --secret-ref "file:$WORK/secrets/minio.secret"
for b in fleet-a fleet-b; do
  accepted adopt garage $b
  accepted expand $b --to minio --name $b
done
nohup "$SHUNT" serve -c b.yaml > b.log 2>&1 &
B_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://$B_ADMIN/-/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 "$B_PID" 2>/dev/null || { cat b.log; fail "B exited"; }

# member_ok waits until B is live and has installed the current version.
member_ok() {
  for _ in $(seq 1 100); do
    fl=$("$SHUNT" proxy list --json)
    [ "$(jq -r '[.members[] | select(.id=="proxy-b" and .live and .applied == '"$(jq .version <<<"$fl")"')] | length' <<<"$fl")" = 1 ] && return 0
    sleep 0.1
  done
  return 1
}
member_ok || fail "B never joined the fleet at the current version"
accepted proxy list
grep -q 'proxy-b  live' <<<"$OUT" || fail "proxy list does not show proxy-b live"
curl -fsS "http://$B_ADMIN/-/fleet" | jq -e '.stale == false' >/dev/null || fail "B reports stale with A up"
note "B joined; /-/fleet on B: $(curl -fsS "http://$B_ADMIN/-/fleet" | jq -c .)"

say "1-2. Ramp fleet-a while a client writes the same keys through A and B in turn"
KEYS=6
mkdir -p last
( i=0
  while [ ! -f stop-writer ]; do
    i=$((i+1))
    for k in $(seq 1 $KEYS); do
      p=$A_LISTEN; [ $(( (i + k) % 2 )) = 0 ] && p=$B_LISTEN
      body="k$k write $i via $p"
      if printf '%s' "$body" | via "$p" s3 cp --quiet - "s3://fleet-a/w/$k" 2>>writer.err; then
        printf '%s' "$body" > "last/$k"
      else
        echo "write w/$k #$i via $p failed after retries" >> writer.log
      fi
    done
    echo "round $i done" >> writer.log
  done ) &
WRITER_PID=$!
sleep 3
accepted ramp fleet-a --ratio 0.25
grep -q 'paused their writes until every proxy had it' <<<"$OUT" || fail "the first ramp step was not held"
grep -q 'in effect on every live proxy (this one and 1 member' <<<"$OUT" || fail "ramp does not say it reached the member"
sleep 3
accepted ramp fleet-a --ratio 0.6
grep -q 'paused their writes' <<<"$OUT" || fail "the second ramp step was not held"
sleep 3
accepted ramp fleet-a --ratio 1.0
sleep 3
accepted migrate start fleet-a
grep -q 'paused their writes' <<<"$OUT" && fail "migrate start at ratio 1 moves no writes and must not be held"
sleep 3
touch stop-writer; wait "$WRITER_PID"; WRITER_PID=
rounds=$(grep -c 'round .* done' writer.log || true)
note "writer: $rounds rounds of $KEYS keys, alternating proxies"
[ "$rounds" -ge 3 ] || fail "the writer finished only $rounds rounds"
if grep -q 'failed after retries' writer.log; then grep 'failed' writer.log >&2; fail "a write failed even with retries"; fi
holds=$(curl -fsS "http://$A_ADMIN/-/metrics" "http://$B_ADMIN/-/metrics" | awk '/^shunt_migration_refused_writes_total\{bucket="default\/fleet-a",reason="hold"\}/ {s+=$2} END {print s+0}')
note "writes paused by holds and retried by the client: $holds"
bad=0
for k in $(seq 1 $KEYS); do
  want=$(cat "last/$k")
  for p in "$A_LISTEN" "$B_LISTEN"; do
    got=$(via "$p" s3 cp --quiet "s3://fleet-a/w/$k" - 2>/dev/null || true)
    if [ "$got" != "$want" ]; then echo "   LOST w/$k via $p: want '$want', read '$got'"; bad=$((bad+1)); fi
  done
done
[ "$bad" = 0 ] || fail "$bad reads did not return the last acknowledged write"
note "every key reads back, through both proxies, as its last acknowledged write"

say "3. Stale mode: A stopped, B keeps reading and refuses writes to the moving bucket"
kill -STOP "$A_PID"
sleep 5
curl -fsS "http://$B_ADMIN/-/fleet" | jq -e '.stale == true' >/dev/null || fail "B is not stale with A stopped"
curl -fsS "http://$B_ADMIN/-/healthz" >/dev/null || fail "B's healthz failed while stale: a control-node outage must not drain the fleet"
printf x > stale.body
if AWS_MAX_ATTEMPTS=1 via "$B_LISTEN" s3api put-object --bucket fleet-a --key stale --body stale.body > /dev/null 2> stale.err; then fail "stale B accepted a write to a moving bucket"; fi
grep -q '503\|ServiceUnavailable' stale.err || fail "stale B refused, but not with 503: $(cat stale.err)"
[ "$(via "$B_LISTEN" s3 cp --quiet s3://fleet-a/seed/1 -)" = "seed 1" ] || fail "stale B does not serve reads of a moving bucket"
printf 'active' | via "$B_LISTEN" s3 cp --quiet - s3://fleet-b/while-stale || fail "stale B refused a write to an ACTIVE bucket"
note "stale B: write to fleet-a 503, read of fleet-a 200, write to fleet-b (ACTIVE) 200, healthz 200"
kill -CONT "$A_PID"
for _ in $(seq 1 50); do curl -fsS "http://$B_ADMIN/-/fleet" | jq -e '.stale == false' >/dev/null && break; sleep 0.2; done
curl -fsS "http://$B_ADMIN/-/fleet" | jq -e '.stale == false' >/dev/null || fail "B stayed stale after A came back"
printf 'back' | via "$B_LISTEN" s3 cp --quiet - s3://fleet-a/after-stale || fail "B refused a write after its lease came back"
note "A back: B's lease holds again and it writes"

say "4. B stopped: a first step cannot reach it and is released; later it waits for B by name"
member_ok || fail "B did not catch up"
kill -STOP "$B_PID"
refused "did not reach every proxy within 3s, waiting on proxy-b" ramp fleet-b --ratio 0.5 --wait 3s
[ "$("$SHUNT" status fleet-b --json | jq -r '.placements[0].state')" = ACTIVE ] || fail "fleet-b is not ACTIVE after the release"
sleep 9 # past B's lease and the control node's margin: B is silent
refused "shunt proxy forget" ramp fleet-b --ratio 0.5 --wait 2s
kill -CONT "$B_PID"
member_ok || fail "B did not come back"
accepted ramp fleet-b --ratio 0.5
grep -q 'paused their writes' <<<"$OUT" || fail "fleet-b's first step was not held"

say "5. B silent: a later step on a moving bucket goes ahead and names it"
kill -STOP "$B_PID"
sleep 9
accepted ramp fleet-b --ratio 1.0
grep -q 'paused their writes' <<<"$OUT" && fail "a step with only a silent member was held"
grep -q 'silent, not waited for: proxy-b' <<<"$OUT" || fail "the step does not name the silent member"
kill -CONT "$B_PID"
member_ok || fail "B did not catch up after being silent"
accepted proxy list

say "FLEET GREEN: holds, stale mode, releases and silent members behave as ADR-0016 says"
