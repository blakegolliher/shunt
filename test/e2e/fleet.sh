#!/usr/bin/env bash
# P3c-1, live: three shunt-control nodes and two shunt proxies on this host, over the e2e Garage
# (source) and MinIO (target). Nothing is shared between the proxies but the network (ADR-0015).
# It checks, with real processes:
#
#   1. Three control nodes form a cluster; both proxies join the fleet through different nodes and
#      get the directory, the client keys and the cluster secrets from the control plane.
#   2. A client that writes the same keys alternately through A and B across every ramp step and
#      migrate start never loses a write (the hold, ADR-0016): every key reads back, through both,
#      as its last acknowledged write. Bucket creation through a proxy lands in the control plane.
#   3. One control node dies: nothing changes for anyone. Two die (quorum lost): a 256-worker
#      workload on an ACTIVE bucket sees zero errors; both proxies go stale and refuse writes on
#      the moving bucket with 503; no step can be taken. Quorum back: everything resumes.
#   4. A proxy restarted with the whole control plane down serves ACTIVE buckets from its cache.
#   5. A proxy paused (SIGSTOP) is silent: a first step waits for it by name until it is forgotten;
#      a later step goes ahead without it.
#   6. The walkthrough's remaining verbs (mover, cutover, purge-source) run through the control
#      plane with two proxies, and the mover signs with secrets the control plane holds.
#
#   make e2e-up && make fleet
set -euo pipefail

WORK=
A_LISTEN=127.0.0.1:8038; A_ADMIN=127.0.0.1:9938
B_LISTEN=127.0.0.1:8048; B_ADMIN=127.0.0.1:9948
C1_API=127.0.0.1:9951; C2_API=127.0.0.1:9952; C3_API=127.0.0.1:9953
C1_PEER=127.0.0.1:9961; C2_PEER=127.0.0.1:9962; C3_PEER=127.0.0.1:9963
while [ $# -gt 0 ]; do
  case "$1" in
    --work) WORK=$2; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown flag $1" >&2; exit 2 ;;
  esac
done
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
[ -n "$WORK" ] || WORK=$ROOT/test/e2e/data/fleet
SHUNT=$ROOT/bin/shunt; CONTROL=$ROOT/bin/shunt-control
[ -x "$SHUNT" ] && [ -x "$CONTROL" ] || { echo "no $SHUNT or $CONTROL; run make build" >&2; exit 1; }
[ -f "$ROOT/test/e2e/data/garage.env" ] || { echo "no garage.env; run make e2e-up" >&2; exit 1; }
# shellcheck disable=SC1091
. "$ROOT/test/e2e/data/garage.env"
command -v aws >/dev/null && command -v jq >/dev/null || { echo "needs aws-cli and jq" >&2; exit 1; }

say()  { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\nFLEET TEST FAILED: %s\n' "$*" >&2; exit 1; }
refused() { # refused <substring> <shunt args...>: must fail with a message containing the substring
  local want=$1 out; shift
  printf '   $ shunt %s   # must be refused\n' "$*"
  if out=$("$SHUNT" "$@" 2>&1); then printf '%s\n' "$out" | sed 's/^/     /'; fail "shunt $* was accepted"; fi
  printf '%s\n' "$out" | sed 's/^/     /'
  grep -q -- "$want" <<<"$out" || fail "shunt $* was refused, but not with '$want'"
}
accepted() { # accepted <shunt args...>: must succeed; OUT holds the output
  printf '   $ shunt %s\n' "$*"
  OUT=$("$SHUNT" "$@" 2>&1) || { printf '%s\n' "$OUT" | sed 's/^/     /'; fail "shunt $* failed"; }
  printf '%s\n' "$OUT" | sed 's/^/     /'
}

rm -rf "$WORK"; mkdir -p "$WORK/secrets"; chmod 700 "$WORK/secrets"; cd "$WORK"
umask 077
printf '%s' "$GARAGE_SECRET" > secrets/garage.secret
printf '%s' "$MINIO_SECRET" > secrets/minio.secret
printf 'fleet-token-%s' "$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')" > secrets/control.token
# Clients use the source cluster's own key, imported with adopt --keys (ADR-0012), as the README does.
CLIENT_AK=$GARAGE_ACCESS_KEY; CLIENT_SK=$GARAGE_SECRET
printf '%s' "$CLIENT_SK" > secrets/client.secret
printf 'credentials:\n  - access_key: %s\n    secret: %s\n' "$CLIENT_AK" "$CLIENT_SK" > client-keys.yaml
umask 022
export SHUNT_API=http://$C1_API SHUNT_CONTROL_API=http://$C1_API SHUNT_API_TOKEN_REF=file:$WORK/secrets/control.token
for p in a b; do
  L=$A_LISTEN; AD=$A_ADMIN; [ $p = b ] && { L=$B_LISTEN; AD=$B_ADMIN; }
  cat > $p.yaml <<EOF2
listener: { address: "$L", plaintext: true }
admin: { address: "$AD" }
auth: { mode: resign }
features: { debug_route_header: true }
control:
  endpoints: ["http://$C1_API", "http://$C2_API", "http://$C3_API"]
  token_ref: file:$WORK/secrets/control.token
  plaintext: true
  proxy_id: proxy-$p
  cache_dir: $WORK/cache-$p
  heartbeat_interval: 1s
  lease_ttl: 3s
EOF2
  "$SHUNT" check-config $p.yaml >/dev/null || fail "check-config $p.yaml"
done

export AWS_CONFIG_FILE=$WORK/aws.config AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export AWS_RETRY_MODE=standard AWS_MAX_ATTEMPTS=10
printf '[default]\ns3 =\n  addressing_style = path\n' > "$AWS_CONFIG_FILE"
on_garage() { AWS_ACCESS_KEY_ID=$GARAGE_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$GARAGE_SECRET aws --endpoint-url http://127.0.0.1:3900 --region garage "$@"; }
on_minio()  { AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$MINIO_SECRET aws --endpoint-url http://127.0.0.1:9000 --region us-east-1 "$@"; }
via()       { local p=$1; shift; AWS_ACCESS_KEY_ID=$CLIENT_AK AWS_SECRET_ACCESS_KEY=$CLIENT_SK aws --endpoint-url "http://$p" --region us-east-1 "$@"; }

declare -A PID
WRITER_PID=
cleanup() {
  local rc=$?
  for k in "${!PID[@]}"; do kill -CONT "${PID[$k]}" 2>/dev/null || true; done
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null
  for k in a b c1 c2 c3; do [ -n "${PID[$k]:-}" ] && kill -TERM "${PID[$k]}" 2>/dev/null; done
  for k in "${!PID[@]}"; do for _ in $(seq 1 100); do kill -0 "${PID[$k]}" 2>/dev/null || break; sleep 0.1; done; kill -KILL "${PID[$k]}" 2>/dev/null || true; done
  [ "$rc" = 0 ] || printf '   logs in %s: c1.log c2.log c3.log a.log b.log writer.log\n' "$WORK" >&2
  return 0
}
trap cleanup EXIT
for addr in $A_LISTEN $A_ADMIN $B_LISTEN $B_ADMIN $C1_API $C2_API $C3_API $C1_PEER $C2_PEER $C3_PEER; do
  if (exec 3<>"/dev/tcp/${addr%:*}/${addr##*:}") 2>/dev/null; then fail "$addr is already in use"; fi
done
wait_http() { for _ in $(seq 1 200); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }

start_control() { # start_control <n> [init|join]
  local n=$1 mode=$2 api peer
  case $n in 1) api=$C1_API; peer=$C1_PEER ;; 2) api=$C2_API; peer=$C2_PEER ;; 3) api=$C3_API; peer=$C3_PEER ;; esac
  if [ "$mode" = init ]; then
    nohup "$CONTROL" init --name c$n --data-dir "$WORK/c$n" --peer-url "http://$peer" --api "$api" --token-ref "file:$WORK/secrets/control.token" --lease-ttl 3s >> "c$n.log" 2>&1 &
  else
    nohup "$CONTROL" join --name c$n --data-dir "$WORK/c$n" --peer-url "http://$peer" --api "$api" --token-ref "file:$WORK/secrets/control.token" --lease-ttl 3s --existing "http://$C1_API" >> "c$n.log" 2>&1 &
  fi
  PID[c$n]=$!
  wait_http "http://$api/-/healthz" || { tail -5 "c$n.log"; fail "control node c$n did not come up"; }
}
start_proxy() { # start_proxy <a|b>
  nohup "$SHUNT" serve -c "$1.yaml" >> "$1.log" 2>&1 &
  PID[$1]=$!
  local ad=$A_ADMIN; [ "$1" = b ] && ad=$B_ADMIN
  wait_http "http://$ad/-/healthz" || { tail -5 "$1.log"; fail "proxy $1 did not come up"; }
}
stop() { kill -TERM "${PID[$1]}" 2>/dev/null || true; for _ in $(seq 1 200); do kill -0 "${PID[$1]}" 2>/dev/null || break; sleep 0.1; done; kill -KILL "${PID[$1]}" 2>/dev/null || true; unset "PID[$1]"; }
member_ok() { # member_ok <id>: live and at the current version, via any node
  for _ in $(seq 1 100); do
    fl=$("$SHUNT" proxy list --json 2>/dev/null || true)
    [ -n "$fl" ] && [ "$(jq -r --arg id "$1" '[.members[] | select(.id==$id and .live and .applied == '"$(jq .version <<<"$fl")"')] | length' <<<"$fl")" = 1 ] && return 0
    sleep 0.1
  done
  return 1
}

for b in fleet-a fleet-b fleet-c; do
  for side in on_garage on_minio; do
    if $side s3api head-bucket --bucket $b >/dev/null 2>&1; then $side s3 rb s3://$b --force >/dev/null; fi
  done
done
for b in fleet-a fleet-b; do on_garage s3api create-bucket --bucket $b >/dev/null; on_minio s3api create-bucket --bucket $b >/dev/null; done
for i in 1 2 3; do printf 'seed %s' $i | on_garage s3 cp --quiet - s3://fleet-a/seed/$i; done

say "1. Three control nodes, then two proxies that get everything from them"
start_control 1 init
start_control 2 join
start_control 3 join
"$CONTROL" status > status-0.txt || fail "shunt-control status"
sed 's/^/     /' status-0.txt
grep -q '3 of 3 members started, 2 needed for writes, quorum' status-0.txt || fail "the cluster did not form"
TOKEN=$(cat "$WORK/secrets/control.token")
probe=$(jq -n --arg endpoint "127.0.0.1:3900" --arg access "$GARAGE_ACCESS_KEY" --arg secret "file:$WORK/secrets/garage.secret" \
  '{name:"garage",cluster:{type:"s3",scheme:"http",region:"garage",endpoints:[$endpoint],credentials:{access_key:$access,secret_ref:$secret},capabilities:{conditional_write:false}}}')
curl -sf -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "$probe" \
  "http://$C1_API/v1/clusters/probe" | jq -e '.reachable == true and .cluster.name == "garage" and .profile == "assumed"' >/dev/null \
  || fail "the non-mutating cluster preflight did not return garage's reachable assumed profile"
note "cluster preflight reached Garage and returned its capability profile before Save"
accepted cluster add garage --type s3 --scheme http --region garage --endpoint 127.0.0.1:3900 --access-key "$GARAGE_ACCESS_KEY" --secret-ref "file:$WORK/secrets/garage.secret" --conditional-write=false
SHUNT_API=http://$C2_API accepted cluster add minio --type minio --scheme http --region us-east-1 --endpoint 127.0.0.1:9000 --access-key "$MINIO_ACCESS_KEY" --secret-ref "file:$WORK/secrets/minio.secret"
# The secrets above are file: refs the control nodes resolve on this host; a secret given to
# cluster add (the README's form) is stored by the control plane instead:
SHUNT_API=http://$C3_API accepted cluster add minio2 http://127.0.0.1:9000 --access-key "$MINIO_ACCESS_KEY" <<<"$MINIO_SECRET"
grep -q 'cluster minio2:' <<<"$OUT" || fail "cluster add with a typed secret"
accepted cluster remove minio2
accepted adopt garage fleet-a --keys client-keys.yaml
grep -q "client key $CLIENT_AK imported" <<<"$OUT" || fail "adopt --keys did not import the clients' key into the control plane"
accepted adopt garage fleet-b
for b in fleet-a fleet-b; do accepted expand $b --to minio --name $b; done
curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/operations?limit=100" \
  | jq -e '[.operations[] | select(.status == "succeeded") | .kind] as $k
           | ($k | index("cluster-add")) != null and ($k | index("adopt")) != null and ($k | index("expand")) != null' >/dev/null \
  || fail "cluster add, adopt and expand do not all have successful operation records"
curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/audit?limit=100" \
  | jq -e '[.changes[] | select(.op == "cluster-put" or .op == "cluster-remove" or .op == "adopt" or .op == "set-target")] as $c
           | ($c | length) >= 8 and all($c[]; .actor | test("^token:[0-9a-f]{12}$"))' >/dev/null \
  || fail "setup actions are missing from the audit tail or expose the wrong actor"
note "cluster add, adopt and expand have operation records and token-fingerprint audit actors"
start_proxy a
start_proxy b
member_ok proxy-a && member_ok proxy-b || fail "the proxies did not join the fleet"
accepted proxy list
grep -q 'proxy-a  live' <<<"$OUT" && grep -q 'proxy-b  live' <<<"$OUT" || fail "proxy list"
for p in $A_ADMIN $B_ADMIN; do curl -fsS "http://$p/-/fleet" | jq -e '.stale == false' >/dev/null || fail "a proxy reports stale"; done
[ "$(via "$A_LISTEN" s3 cp --quiet s3://fleet-a/seed/1 -)" = "seed 1" ] || fail "proxy A cannot read with the delivered key and secret"
[ "$(via "$B_LISTEN" s3 cp --quiet s3://fleet-a/seed/2 -)" = "seed 2" ] || fail "proxy B cannot read"
printf 'read-only proof' | via "$A_LISTEN" s3 cp --quiet - s3://fleet-b/read-only-proof || fail "seed for the read-only check"
accepted readonly fleet-b --wait 5s
printf x > read-only.body
if AWS_MAX_ATTEMPTS=1 via "$B_LISTEN" s3api put-object --bucket fleet-b --key refused --body read-only.body >/dev/null 2>read-only.err; then
  fail "a read-only placement accepted a write"
fi
grep -q '503\|ServiceUnavailable' read-only.err || fail "the read-only refusal was not retryable: $(cat read-only.err)"
[ "$(via "$B_LISTEN" s3 cp --quiet s3://fleet-b/read-only-proof -)" = "read-only proof" ] || fail "read-only mode refused a read"
accepted readonly fleet-b --off --wait 5s
curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/operations?placement=default/fleet-b&limit=100" \
  | jq -e '[.operations[] | select(.kind == "placement-read-only" and .status == "succeeded")] | length == 2' >/dev/null \
  || fail "the read-only toggle does not have two successful operation records"
note "placement read-only fenced both proxies, refused writes with 503, allowed reads, then unfenced"
via "$B_LISTEN" s3 mb s3://fleet-c >/dev/null || fail "CreateBucket through a member"
accepted status fleet-c
grep -q 'fleet-c' <<<"$OUT" || fail "a bucket created through proxy B is not in the control plane"
[ "$(via "$A_LISTEN" s3 ls | grep -c fleet-c)" = 1 ] || fail "proxy A does not list the bucket proxy B created"
note "a bucket created through B is served by A: the write went through the control plane"

say "2. Ramp fleet-a while a client writes the same keys through A and B in turn"
KEYS=6; mkdir -p last
( i=0
  while [ ! -f stop-writer ]; do
    i=$((i+1))
    for k in $(seq 1 $KEYS); do
      p=$A_LISTEN; [ $(( (i + k) % 2 )) = 0 ] && p=$B_LISTEN
      body="k$k write $i via $p"
      if printf '%s' "$body" | via "$p" s3 cp --quiet - "s3://fleet-a/w/$k" 2>>writer.err; then printf '%s' "$body" > "last/$k"
      else echo "write w/$k #$i via $p failed after retries" >> writer.log; fi
    done
    echo "round $i done" >> writer.log
  done ) &
WRITER_PID=$!
sleep 3
# GET /v1/events (ADR-0017) must show the step as it happens: the record's fence and the directory change.
curl -sN --max-time 4 -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/events" > "$WORK/events.log" 2>&1 &
EVENTS_PID=$!
accepted ramp fleet-a --ratio 0.25
grep -q 'paused their writes until every proxy had it' <<<"$OUT" || fail "the first ramp step was not held"
grep -q 'in effect on every live proxy (this one and 2 member' <<<"$OUT" || fail "ramp does not say it reached both members"
wait "$EVENTS_PID" 2>/dev/null || true
grep -q 'event: directory' "$WORK/events.log" || fail "GET /v1/events published no directory event for the ramp step"
grep -q 'event: fence' "$WORK/events.log" || fail "GET /v1/events published no fence event for the ramp step"
note "GET /v1/events carried the step: $(grep -c '^event: ' "$WORK/events.log") events during the ramp"
sleep 3
SHUNT_API=http://$C2_API accepted ramp fleet-a --ratio 0.6
grep -q 'paused their writes' <<<"$OUT" || fail "the second step was not held"
sleep 3
SHUNT_API=http://$C3_API accepted ramp fleet-a --ratio 1.0
sleep 3
accepted migrate start fleet-a
grep -q 'paused their writes' <<<"$OUT" && fail "migrate start at ratio 1 must not be held"
sleep 3
touch stop-writer; wait "$WRITER_PID"; WRITER_PID=
rounds=$(grep -c 'round .* done' writer.log || true)
note "writer: $rounds rounds of $KEYS keys, alternating proxies"
[ "$rounds" -ge 3 ] || fail "the writer finished only $rounds rounds"
if grep -q 'failed after retries' writer.log; then grep failed writer.log >&2; fail "a write failed even with retries"; fi
bad=0
for k in $(seq 1 $KEYS); do
  want=$(cat "last/$k")
  for p in $A_LISTEN $B_LISTEN; do
    got=$(via "$p" s3 cp --quiet "s3://fleet-a/w/$k" - 2>/dev/null || true)
    [ "$got" = "$want" ] || { echo "   LOST w/$k via $p: want '$want', read '$got'"; bad=$((bad+1)); }
  done
done
[ "$bad" = 0 ] || fail "$bad reads did not return the last acknowledged write"
note "every key reads back, through both proxies, as its last acknowledged write"

# Run the percentile comparison after the moving-key fence has settled. The alternating writer
# above supplies proxy B and Garage scopes; this stable workload supplies an uncontaminated
# client-versus-fleet window through proxy A and MinIO.
if ! "$SHUNT" verify --endpoint "http://$A_LISTEN" --bucket fleet-a --access-key "$CLIENT_AK" --secret-ref "file:$WORK/secrets/client.secret" \
  --workers 24 --keys 200 --duration 25s --interval 0 --cleanup --json-out verify-telemetry.json \
  --telemetry-url "http://$C1_API" --telemetry-token-ref "file:$WORK/secrets/control.token" > verify-telemetry.log 2>&1; then
  cat verify-telemetry.log
  fail "verify telemetry cross-check failed"
fi
telemetry=$(curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/telemetry/latest") || fail "GET /v1/telemetry/latest"
jq -e '[.windows[].scope] as $s | ($s | index("proxy:proxy-a")) != null and ($s | index("proxy:proxy-b")) != null and
       ($s | index("cluster:garage")) != null and ($s | index("cluster:minio")) != null and ($s | index("fleet")) != null' \
  <<<"$telemetry" >/dev/null || fail "telemetry latest does not show two proxies, both clusters, and fleet"
jq -e '.telemetry.within_tolerance == true and .latency_p50_us > 0 and .latency_p99_us >= .latency_p50_us' \
  verify-telemetry.json >/dev/null || fail "verify JSON has no passing p50/p99 telemetry comparison"
for cluster in garage minio; do
  curl -sf -H "Authorization: Bearer $TOKEN" \
    "http://$C1_API/v1/telemetry/series?scope=cluster:$cluster&series=requests_per_second&op=all" \
    | jq -e '.points | length > 0 and all(.[]; .value >= 0)' >/dev/null \
    || fail "the telemetry dashboard request-rate series is empty for $cluster"
done
note "telemetry: two proxies, garage + minio, fleet; $(jq -r '"client p99 \(.telemetry.client_p99_us) us, fleet p99 \(.telemetry.fleet_p99_us) us, difference \(.telemetry.difference_percent)%"' verify-telemetry.json)"

say "3. One control node dies, then two: quorum lost under a 256-worker workload on an ACTIVE bucket"
stop c3
sleep 2
SHUNT_API=http://$C2_API accepted status
member_ok proxy-a || fail "proxy A lost the fleet with one control node down"
note "c3 down: nothing changed"
"$SHUNT" verify --endpoint "http://$A_LISTEN" --bucket fleet-b --access-key "$CLIENT_AK" --secret-ref "file:$WORK/secrets/client.secret" \
  --workers 256 --keys 500 --duration 12s --interval 0 --cleanup --json-out verify-quorum.json > verify-quorum.log 2>&1 &
VERIFY_PID=$!
sleep 2
stop c2
note "c2 down too: quorum lost (1 of 3)"
sleep 5
for p in $A_ADMIN $B_ADMIN; do curl -fsS "http://$p/-/fleet" | jq -e '.stale == true' >/dev/null || fail "a proxy is not stale with quorum lost"; done
curl -fsS "http://$A_ADMIN/-/healthz" >/dev/null || fail "healthz failed while stale: a control-plane outage must not drain the fleet"
printf x > stale.body
if AWS_MAX_ATTEMPTS=1 via "$B_LISTEN" s3api put-object --bucket fleet-a --key stale --body stale.body >/dev/null 2>stale.err; then fail "a stale proxy accepted a write to a moving bucket"; fi
grep -q '503\|ServiceUnavailable' stale.err || fail "the stale refusal was not 503: $(cat stale.err)"
[ "$(via "$B_LISTEN" s3 cp --quiet s3://fleet-a/seed/1 -)" = "seed 1" ] || fail "a stale proxy does not serve reads of a moving bucket"
refused "unavailable\|control plane\|not on every proxy\|waiting" ramp fleet-b --ratio 0.5 --wait 2s
wait "$VERIFY_PID" || { cat verify-quorum.log; fail "verify on the ACTIVE bucket failed during quorum loss"; }
errs=$(jq -r '.errors' verify-quorum.json); ops=$(jq -r '.operations // .ops // 0' verify-quorum.json)
note "ACTIVE bucket through a stale proxy with quorum lost: $errs errors in $ops operations (256 workers)"
[ "$errs" = 0 ] || fail "$errs errors on an ACTIVE bucket during quorum loss"

say "4. Proxy A restarts with the whole control plane down: serves ACTIVE from its cache"
stop c1
stop a
start_proxy a
[ "$(via "$A_LISTEN" s3 cp --quiet s3://fleet-a/seed/3 -)" = "seed 3" ] || fail "restarted proxy A does not serve from its cache"
curl -fsS "http://$A_ADMIN/-/fleet" | jq -e '.stale == true and .applied > 0' >/dev/null || fail "restarted proxy A is not stale-with-cache"
grep -q 'directory loaded from the local cache' a.log || fail "proxy A did not say it loaded the cache"
note "A serves ACTIVE buckets from $WORK/cache-a with no control node reachable"

say "5. Quorum back: leases return, a step goes through; a paused proxy is waited for by name"
start_control 1 init
start_control 2 join
start_control 3 join
member_ok proxy-a && member_ok proxy-b || fail "the proxies did not re-join after the control plane returned"
for p in $A_ADMIN $B_ADMIN; do curl -fsS "http://$p/-/fleet" | jq -e '.stale == false' >/dev/null || fail "a proxy stayed stale after quorum returned"; done
printf 'back' | via "$B_LISTEN" s3 cp --quiet - s3://fleet-a/after-outage || fail "B refused a write after its lease came back"
kill -STOP "${PID[b]}"
refused "did not reach every proxy within 3s, waiting on proxy-b" ramp fleet-b --ratio 0.5 --wait 3s
[ "$("$SHUNT" status fleet-b --json | jq -r '.placements[0].state')" = ACTIVE ] || fail "fleet-b is not ACTIVE after the release"
sleep 9
refused "shunt proxy forget" ramp fleet-b --ratio 0.5 --wait 2s
kill -CONT "${PID[b]}"
member_ok proxy-b || fail "B did not come back"
accepted ramp fleet-b --ratio 0.5
grep -q 'paused their writes' <<<"$OUT" || fail "fleet-b's first step was not held"
kill -STOP "${PID[b]}"; sleep 9
accepted ramp fleet-b --ratio 1.0
grep -q 'silent, not waited for: proxy-b' <<<"$OUT" || fail "a step with a silent member did not name it"
kill -CONT "${PID[b]}"; member_ok proxy-b || fail "B did not catch up after being silent"

say "6. Browser mover operation, cutover and purge through the control plane, with the mover signing with secrets it holds"
mover=$(curl -sf -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"kind":"mover","placement":"default/fleet-a","args":{"until_converged":true,"max_passes":10}}' \
  "http://$C1_API/v1/operations") || fail "the browser mover operation was not accepted"
mover_id=$(jq -r '.id' <<<"$mover")
for _ in $(seq 1 600); do
  mover=$(curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/operations/$mover_id") || fail "the mover operation disappeared"
  [ "$(jq -r '.status' <<<"$mover")" = running ] || break
  sleep 0.1
done
jq -e '.status == "succeeded" and .result.converged == true' <<<"$mover" >/dev/null || { printf '%s\n' "$mover"; fail "the browser mover did not converge"; }
curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/placements/default/fleet-a/mover-ledger?limit=20" \
  | jq -e '.entries | length > 0' >/dev/null || fail "the browser mover ledger has no entries"
note "browser mover operation $mover_id converged and exposed its bounded ledger tail"
accepted cutover fleet-a --window 5s
grep -q 'in effect on every live proxy' <<<"$OUT" || fail "cutover did not reach the fleet"
accepted purge-source fleet-a
[ "$(via "$A_LISTEN" s3 cp --quiet s3://fleet-a/seed/1 -)" = "seed 1" ] || fail "seed/1 is gone after the move"
"$CONTROL" status | sed 's/^/     /'
# Every step ran as an operation record (ADR-0017), readable from any control node.
curl -sf -H "Authorization: Bearer $TOKEN" "http://$C1_API/v1/operations?placement=default/fleet-a" \
  | jq -e '[.operations[] | select(.status == "succeeded") | .kind] as $k
           | ($k | index("ramp")) != null and ($k | index("mover")) != null and ($k | index("cutover")) != null and ($k | index("purge-source")) != null' >/dev/null \
  || fail "the operation records for fleet-a do not show ramp, mover, cutover and purge-source succeeded"
note "operation records: ramp, mover, cutover and purge-source on fleet-a all succeeded"
curl -sf -H "Authorization: Bearer $TOKEN" "http://$C2_API/v1/control" | jq -e '.join | test("shunt-control join --name")' >/dev/null \
  || fail "GET /v1/control carries no join line for a new node"

say "FLEET GREEN: three control nodes, two proxies sharing nothing, the fence, quorum loss and a cache restart all behave as ADR-0015 and ADR-0016 say"
