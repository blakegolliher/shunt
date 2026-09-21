#!/usr/bin/env bash
# The README demo, unattended: "Demo: move a live bucket between two clusters, by hand", steps 1 to
# 14, with the README's own commands in the README's order, e2e Garage as cluster A and e2e MinIO as
# cluster B. It fails on the first command that fails, on any count or check the README states that
# comes out different, and on a single reader-loop pass (step 10 onwards) that is short, mismatched,
# or has aws exit non-zero.
#
#   make e2e-up && make readme-demo
#
# The README is the thing people run, so this is what keeps it true: a change that breaks the demo
# breaks this script. Where it has to differ from the README, it says why:
#   - aws-cli profiles live in a private AWS_CONFIG_FILE under --work, not ~/.aws.
#   - secrets go in on stdin, which is what shunt reads when stdin is not a terminal (the prompt's
#     non-interactive form); the README's `aws configure` prompts become `aws configure set`.
#   - Garage signs as region "garage", so cluster A takes --region garage.
#   - T1 (shunt serve) and T3 (the reader loop) run in the background; T2 is this script.
#   - shunt listens on --listen/--admin (default 127.0.0.1:8018 and :9918) so it never collides with a
#     lab shunt on the README's own :8008/:9900.
set -euo pipefail

WORK=; LISTEN=127.0.0.1:8018; ADMIN=127.0.0.1:9918; WINDOW=60s
A_URL=http://127.0.0.1:3900; B_URL=http://127.0.0.1:9000; A_REGION=garage; B_REGION=us-east-1
while [ $# -gt 0 ]; do
  case "$1" in
    --work) WORK=$2; shift 2 ;;
    --listen) LISTEN=$2; shift 2 ;;
    --admin) ADMIN=$2; shift 2 ;;
    --window) WINDOW=$2; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown flag $1" >&2; exit 2 ;;
  esac
done
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
[ -n "$WORK" ] || WORK=$ROOT/test/e2e/data/readme-demo
SHUNT=$ROOT/bin/shunt
[ -x "$SHUNT" ] || { echo "no $SHUNT; run make build" >&2; exit 1; }
[ -f "$ROOT/test/e2e/data/garage.env" ] || { echo "no garage.env; run make e2e-up" >&2; exit 1; }
# shellcheck disable=SC1091
. "$ROOT/test/e2e/data/garage.env"
A_AK=$GARAGE_ACCESS_KEY; A_SK=$GARAGE_SECRET; B_AK=$MINIO_ACCESS_KEY; B_SK=$MINIO_SECRET

say()  { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\nREADME DEMO FAILED: %s\n' "$*" >&2; exit 1; }
expect() { [ "$2" = "$3" ] || fail "$1: README says '$3', got '$2'"; }

rm -rf "$WORK"; mkdir -p "$WORK"; cd "$WORK"
export PATH=$ROOT/bin:$PATH SHUNT_API=http://$ADMIN
export AWS_CONFIG_FILE=$WORK/aws.config AWS_SHARED_CREDENTIALS_FILE=$WORK/aws.credentials
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
: > "$AWS_CONFIG_FILE"; : > "$AWS_SHARED_CREDENTIALS_FILE"; chmod 600 "$AWS_SHARED_CREDENTIALS_FILE"
profile() { # profile name access-key secret endpoint region
  aws configure set aws_access_key_id "$2" --profile "$1"
  aws configure set aws_secret_access_key "$3" --profile "$1"
  aws configure set region "$5" --profile "$1"
  aws configure set endpoint_url "$4" --profile "$1"
  aws configure set s3.addressing_style path --profile "$1"
}
count() { aws --profile "$1" s3 ls s3://demo-source/demo/ | wc -l | tr -d ' '; }

SHUNT_PID=; READER_PID=
cleanup() {
  local rc=$?
  [ -n "$READER_PID" ] && kill "$READER_PID" 2>/dev/null && wait "$READER_PID" 2>/dev/null
  if [ -n "$SHUNT_PID" ] && kill -0 "$SHUNT_PID" 2>/dev/null; then
    kill -TERM "$SHUNT_PID"
    for _ in $(seq 1 100); do kill -0 "$SHUNT_PID" 2>/dev/null || break; sleep 0.1; done
  fi
  [ "$rc" = 0 ] || printf '   logs: %s/shunt.log, %s/reader.log\n' "$WORK" "$WORK" >&2
  return 0
}
trap cleanup EXIT
for addr in "$LISTEN" "$ADMIN"; do
  if (exec 3<>"/dev/tcp/${addr%:*}/${addr##*:}") 2>/dev/null; then fail "$addr is already in use"; fi
done

# "You need: an empty bucket named demo-source on each."
profile clustera "$A_AK" "$A_SK" "$A_URL" "$A_REGION"
profile clusterb "$B_AK" "$B_SK" "$B_URL" "$B_REGION"
for p in clustera clusterb; do
  if aws --profile $p s3api head-bucket --bucket demo-source >/dev/null 2>&1; then
    aws --profile $p s3 rb s3://demo-source --force >/dev/null
  fi
  aws --profile $p s3 mb s3://demo-source >/dev/null
done

say "Step 1: start shunt (T1)"
nohup "$SHUNT" serve --plaintext --listen "$LISTEN" --admin "$ADMIN" > "$WORK/shunt.log" 2>&1 &
SHUNT_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://$ADMIN/-/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 "$SHUNT_PID" 2>/dev/null || { cat "$WORK/shunt.log"; fail "shunt serve exited"; }
[ -d shunt-data ] || fail "shunt serve --plaintext did not create ./shunt-data"
grep -q 'shunt serving' shunt.log || fail "no 'shunt serving' line in the log"
note "pid $SHUNT_PID, state in $WORK/shunt-data"

say "Step 2: point aws-cli at the clusters"
expect "clustera objects" "$(aws --profile clustera s3api list-objects-v2 --bucket demo-source --query 'length(Contents || `[]`)' --output text)" 0
expect "clusterb objects" "$(aws --profile clusterb s3api list-objects-v2 --bucket demo-source --query 'length(Contents || `[]`)' --output text)" 0

say "Step 3: two files straight to cluster A"
mkdir -p files
for i in 01 02; do head -c 1048576 /dev/urandom > files/file-$i; aws --profile clustera s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
expect "A after step 3" "$(count clustera)" 2

say "Step 4: put shunt in front of the bucket, with the clients' own key"
printf '%s\n' "$A_SK" | shunt cluster add clustera "$A_URL" --access-key "$A_AK" --region "$A_REGION"
(umask 077; cat > client-keys.yaml <<EOF
credentials:
  - access_key: $A_AK
    secret: $A_SK
EOF
)
shunt adopt clustera demo-source --keys client-keys.yaml
gen=$(shunt client show | sed -n 's/^access_key=\(SHUNT[^ ]*\).*/\1/p')
[ -n "$gen" ] || fail "client show printed no SHUNT… key"
shunt client remove "$gen"
profile shunt "$A_AK" "$A_SK" "http://$LISTEN" us-east-1
expect "listing through shunt" "$(count shunt)" 2

say "Step 5: ten more, through shunt"
for i in $(seq -w 3 12); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
expect "A after step 5" "$(count clustera)" 12

say "Step 6: add cluster B, live"
printf '%s\n' "$B_SK" | shunt cluster add clusterb "$B_URL" --access-key "$B_AK"
shunt expand demo-source --to clusterb --name demo-source
shunt status demo-source

say "Step 7: send 50% of writes to cluster B"
shunt ramp demo-source --ratio 0.5

check() { rm -rf readback && mkdir readback && aws --profile shunt s3 cp --recursive --quiet s3://demo-source/demo/ readback/;
  bad=0; for f in files/*; do cmp -s "$f" "readback/${f##*/}" || { echo "BAD ${f##*/}"; bad=$((bad+1)); }; done
  echo "read $(ls readback | wc -l), mismatched $bad"; }

say "Step 8: read everything back, then 20 more"
expect "check before step 8 writes" "$(check)" "read 12, mismatched 0"
for i in $(seq 13 32); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
expect "split after step 8" "A: $(count clustera)  B: $(count clusterb)" "A: 21  B: 11"
expect "check after step 8" "$(check)" "read 32, mismatched 0"
shunt status demo-source

say "Step 9: all writes to cluster B, 20 more"
shunt ramp demo-source --ratio 1.0
for i in $(seq 33 52); do head -c 1048576 /dev/urandom > files/file-$i; aws --profile shunt s3 cp --quiet files/file-$i s3://demo-source/demo/file-$i; done
expect "split after step 9" "A: $(count clustera)  B: $(count clusterb)" "A: 21  B: 31"
expect "check after step 9" "$(check)" "read 52, mismatched 0"

say "Step 10: start the reader loop (T3)"
( pass=0; while true; do pass=$((pass+1)); rm -rf rb && mkdir rb
  aws --profile shunt s3 cp --recursive --quiet s3://demo-source/demo/ rb/ 2> err.txt; rc=$?
  bad=0; for f in files/*; do cmp -s "$f" "rb/${f##*/}" || bad=$((bad+1)); done
  echo "$(date +%T) pass $pass: read $(ls rb | wc -l), mismatched $bad, aws exit $rc $(tr '\n' ' ' < err.txt)"; sleep 2; done
) > reader.log 2>&1 &
READER_PID=$!
sleep 5
grep -q 'pass 1:' reader.log || fail "reader loop produced nothing"

say "Step 11: migrate the rest"
shunt migrate start demo-source
shunt migrate run demo-source --until-converged
shunt cutover demo-source --window "$WINDOW"

say "Step 12: remove the data from cluster A"
shunt purge-source demo-source
expect "B after purge" "$(count clusterb)" 52
aws --profile clustera s3 ls s3://demo-source/ > a-after-purge.txt 2>&1 && fail "cluster A still lists demo-source after purge-source"
grep -q NoSuchBucket a-after-purge.txt || fail "cluster A listing did not end in NoSuchBucket: $(cat a-after-purge.txt)"

say "Step 13: shunt forgets cluster A"
shunt tenant set-default clusterb
shunt cluster remove clustera
shunt status
shunt status | grep -q clustera && fail "status still lists clustera"
kill "$READER_PID"; wait "$READER_PID" 2>/dev/null || true; READER_PID=
passes=$(grep -c ' pass ' reader.log || true)
good=$(grep -c ': read 52, mismatched 0, aws exit 0 *$' reader.log || true)
note "reader loop: $good of $passes passes read 52, mismatched 0, aws exit 0"
[ "$passes" -ge 3 ] || fail "reader loop ran only $passes passes"
[ "$good" = "$passes" ] || { grep -v ': read 52, mismatched 0, aws exit 0 *$' reader.log >&2; fail "reader loop: $((passes - good)) bad passes"; }

say "Step 14: hand the clients back and stop shunt"
shunt step-out > step-out-1.txt 2>&1 && { cat step-out-1.txt; fail "step-out passed while the client key is cluster A's"; }
cat step-out-1.txt
grep -q "BLOCKED client key $A_AK" step-out-1.txt || fail "step-out does not block on cluster A's client key"
grep -q "ok      bucket demo-source" step-out-1.txt || fail "step-out does not pass the bucket"
printf '%s\n' "$B_SK" | shunt client add "$B_AK" --check clusterb
shunt client remove "$A_AK"
profile shunt "$B_AK" "$B_SK" "http://$LISTEN" us-east-1
shunt step-out > step-out-2.txt 2>&1 || { cat step-out-2.txt; fail "step-out still blocked after importing cluster B's key"; }
cat step-out-2.txt
grep -q '^READY' step-out-2.txt || fail "step-out did not print READY"
kill -TERM "$SHUNT_PID"; for _ in $(seq 1 100); do kill -0 "$SHUNT_PID" 2>/dev/null || break; sleep 0.1; done
kill -0 "$SHUNT_PID" 2>/dev/null && fail "shunt did not stop on SIGTERM"
SHUNT_PID=
aws configure set endpoint_url "$B_URL" --profile shunt
aws configure set region "$B_REGION" --profile shunt
expect "check straight from cluster B" "$(check)" "read 52, mismatched 0"

say "README demo: every step as written, reader loop clean, clients handed back to cluster B"
