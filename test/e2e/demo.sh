#!/usr/bin/env bash
# POC-4 migration demo: move a bucket's data from one cluster to another while a client keeps
# writing to it, using nothing but aws-cli against one unchanged shunt endpoint.
#
#   make e2e-up && make run-mixed        # in another terminal
#   test/e2e/demo.sh --from minio --to garage
#
# Every client command below points at the same endpoint with the same credentials from start to
# finish. Nothing the client does changes; the bucket moves underneath it.
set -euo pipefail

FROM=garage; TO=minio; BUCKET=demo; OBJECTS=1000; MULTIPART=4; KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --from) FROM=$2; shift 2 ;;
    --to) TO=$2; shift 2 ;;
    --bucket) BUCKET=$2; shift 2 ;;
    --objects) OBJECTS=$2; shift 2 ;;
    --multipart) MULTIPART=$2; shift 2 ;;
    --keep) KEEP=1; shift ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "unknown flag $1" >&2; exit 2 ;;
  esac
done

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
E2E=test/e2e
DATA=$E2E/data
CFG=$DATA/shunt-mixed.yaml
DIR=$DATA/directory-mixed.yaml
ENDPOINT=https://127.0.0.1:8443
ADMIN=http://127.0.0.1:9900
SHUNT=bin/shunt
WORK=$(mktemp -d)
cleanup() {
  [ -n "${WRITER_PID:-}" ] && kill "$WRITER_PID" 2>/dev/null
  [ -n "${MOVER_PID:-}" ] && kill "$MOVER_PID" 2>/dev/null
  if [ "${DEMO_OK:-0}" = 1 ]; then rm -rf "$WORK"; else echo "   (working files kept in $WORK)"; fi
  return 0
}
trap cleanup EXIT

case "$FROM" in garage) TENANT=e2e-a ;; minio) TENANT=e2e-b ;; *) echo "--from must be garage or minio" >&2; exit 2 ;; esac
KEY=$TENANT/$BUCKET

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\n\033[31mFAILED: %s\033[0m\n' "$*" >&2; exit 1; }

# migrate_start runs `shunt migrate start` and shows a refusal for a target without conditional PUT
# (ADR-0004 race 2) instead of hiding it. The demo then accepts the window explicitly, which a real
# operator does only with writers quiesced or at low write volume (docs/migrating.md).
migrate_start() {
  local out
  if out=$($SHUNT migrate start "$@" 2>&1); then
    printf '%s\n' "$out" | sed 's/^/   /'
    return
  fi
  printf '%s\n' "$out" | sed 's/^/   /'
  grep -q -- '--accept-lost-write-window' <<<"$out" || fail "migrate start"
  note "refused as designed: the target ignores If-None-Match: * on PUT. Accepting the window for this demo."
  $SHUNT migrate start "$@" --accept-lost-write-window | sed 's/^/   /'
  MOVER_ACCEPT=--accept-lost-write-window # the mover refuses the same target without it
}

# --- preflight -------------------------------------------------------------------------------
command -v aws >/dev/null || fail "aws-cli is not installed"
command -v jq >/dev/null || fail "jq is not installed"
[ -f "$CFG" ] || fail "$CFG is missing; run make e2e-up first"
[ -x "$SHUNT" ] || fail "$SHUNT is missing; run make build"
curl -fsS "$ADMIN/-/metrics" >/dev/null 2>&1 || fail "shunt is not running; start it with make run-mixed"
# The mover picks its overwrite guard from capabilities.conditional_write. A config that predates
# that key would silently get the optimistic guard on a backend that ignores If-None-Match.
grep -q conditional_write "$DIR" || fail "$DIR has no capabilities.conditional_write; restart make run-mixed"
set -a; . "$DATA/garage.env"; set +a

creds=$(awk -v t="$TENANT" '/access_key:/{ak=$3} /secret:/{sk=$2} /tenant:/{if($2==t){print ak, sk; exit}}' "$DATA/credentials.yaml")
[ -n "$creds" ] || fail "no credential for tenant $TENANT in $DATA/credentials.yaml"
export AWS_ACCESS_KEY_ID=${creds% *} AWS_SECRET_ACCESS_KEY=${creds#* } AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export SHUNT_ACCESS_KEY=$AWS_ACCESS_KEY_ID SHUNT_SECRET=$AWS_SECRET_ACCESS_KEY  # the bench harness's names
# The bench harness dials 127.0.0.1 but speaks the certificate's name, as the other e2e targets do.
BENCH_ADDR="-via https://shunt.example.com:8443 -via-addr 127.0.0.1:8443 -domain shunt.example.com -ca $E2E/certs/wildcard.crt"
s3api() { aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api "$@" 2>/dev/null; }

# metric <name> <label-substring> — the counter's current value, or 0 if it has no series yet.
metric() { curl -fsS "$ADMIN/-/metrics" | awk -v n="$1" -v l="$2" '$0 ~ "^"n"{" && index($0,l){s+=$NF} END{printf "%.0f", s+0}'; }

say "1. A bucket on $FROM, filled through shunt"
s3api create-bucket --bucket "$BUCKET" >/dev/null || true
primary=$($SHUNT status "$KEY" --json | jq -r '.placements[0].primary')
[ "$primary" = "$FROM" ] || fail "$KEY landed on $primary, not $FROM (check the tenant's default_cluster)"
note "$KEY is on $primary as $($SHUNT status "$KEY" --json | jq -r --arg c "$FROM" '.placements[0].names[$c]')"

head -c 4096 /dev/urandom > "$WORK/small.bin"
head -c $((9*1024*1024)) /dev/urandom > "$WORK/large.bin"   # over the 8 MiB cli threshold: multipart
seq 1 "$OBJECTS" | xargs -P 16 -I{} sh -c \
  'aws --endpoint-url "$1" --no-verify-ssl s3api put-object --bucket "$2" --key "seed/{}" --body "$3" >/dev/null 2>&1' \
  _ "$ENDPOINT" "$BUCKET" "$WORK/small.bin"
for i in $(seq 1 "$MULTIPART"); do
  aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 cp "$WORK/large.bin" "s3://$BUCKET/multipart/$i" >/dev/null 2>&1
done
note "$OBJECTS single-part objects and $MULTIPART multipart objects written"

# The manifest is the objects the migration must preserve exactly: the seeded ones. The bench
# harness rewrites its own bench/ keys during the run and the writer adds live/ keys, so both are
# checked separately rather than diffed.
manifest() { s3api list-objects-v2 --bucket "$BUCKET" --query 'Contents[].[Key,ETag,Size]' --output text | { grep -Ev '^(bench|live)/' || true; } | sort; }
manifest > "$WORK/before.txt"
note "$(wc -l < "$WORK/before.txt") objects listed through shunt before the move"

say "6a. Baseline latency through shunt, before anything moves"
go run ./test/bench/s3bench -via-only -mode resign -bucket "$BUCKET" \
  -matrix 4096:8:200,1048576:8:100 -runs 1 -out "$WORK/baseline.json" \
  $BENCH_ADDR || fail "baseline bench"

say "2. Ramp writes onto $TO while a client keeps writing"
: > "$WORK/writer.keys"
( while :; do
    k="live/$(date +%s%N)"
    if aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api put-object --bucket "$BUCKET" --key "$k" --body "$WORK/small.bin" >/dev/null 2>&1; then
      echo "$k" >> "$WORK/writer.keys"
    fi
  done ) & WRITER_PID=$!
note "background writer running (pid $WRITER_PID); it never stops until cutover"

for step in 0.01 0.25 1.0; do
  before_p=$(metric shunt_ramp_writes_total "side=\"primary\"")
  $SHUNT ramp "$KEY" --to "$TO" --create --ratio "$step" | sed 's/^/   /'
  sleep 6
  after_p=$(metric shunt_ramp_writes_total "side=\"primary\"")
  after_s=$(metric shunt_ramp_writes_total "side=\"source\"")
  note "ratio $step: writes to the new primary $before_p -> $after_p (source total $after_s)"
done
[ "$(metric shunt_ramp_writes_total 'side="primary"')" -gt 0 ] || fail "no write ever reached the new primary during the ramp"

say "3. Migrate, then move the bytes"
migrate_start "$KEY"

say "6b. Latency during the migration (fallback reads, merged listings, dual deletes all live)"
go run ./test/bench/s3bench -via-only -mode resign -bucket "$BUCKET" \
  -matrix 4096:8:200,1048576:8:100 -runs 1 -baseline "$WORK/baseline.json" \
  $BENCH_ADDR | tee "$WORK/during.md" || fail "migration bench"

note "copying $KEY from $FROM to $TO while clients keep reading it"
$SHUNT migrate run "$KEY" --cursor-dir "$DATA" --ledger-dir "$DATA" ${MOVER_ACCEPT:-} > "$WORK/mover.log" 2>&1 & MOVER_PID=$!

# Reads of objects the mover has not reached yet are served from the source. Poll that counter while
# reading, so a plateau means "nothing is left on the source", not "nobody asked" (POC.md step 3).
last=-1; stable=0
while [ "$stable" -lt 3 ] || kill -0 "$MOVER_PID" 2>/dev/null; do
  s3api list-objects-v2 --bucket "$BUCKET" --max-keys 50 >/dev/null
  for i in 1 3 7; do s3api head-object --bucket "$BUCKET" --key "seed/$i" >/dev/null || true; done
  now=$(metric shunt_migration_fallback_reads_total "$KEY")
  if [ "$now" = "$last" ]; then stable=$((stable+1)); else stable=0; fi
  note "fallback reads: $now"
  last=$now
  sleep 2
done
wait "$MOVER_PID" || fail "the mover failed: $(tail -3 "$WORK/mover.log")"
MOVER_PID=
sed 's/^/   /' "$WORK/mover.log"
note "reads stopped falling back to $FROM at $last"

say "4. Cut over and verify every object"
kill "$WRITER_PID" 2>/dev/null || true; wait "$WRITER_PID" 2>/dev/null || true; WRITER_PID=
note "writer stopped after $(wc -l < "$WORK/writer.keys") live objects"
# The writer's last object may still be in flight; re-run the mover until a pass copies nothing,
# which cutover requires, then cut over once no read has fallen back for 5s.
$SHUNT migrate run "$KEY" --until-converged --cursor-dir "$DATA" --ledger-dir "$DATA" ${MOVER_ACCEPT:-} | sed 's/^/   /'
$SHUNT cutover "$KEY" --window 5s | sed 's/^/   /'

manifest > "$WORK/after.txt"
if ! diff -u "$WORK/before.txt" "$WORK/after.txt" > "$WORK/listing.diff"; then
  head -40 "$WORK/listing.diff"; fail "the listing changed across the migration"
fi
note "listing diff for the pre-migration objects: empty"

cat > "$WORK/verify.sh" <<'VERIFY'
#!/bin/sh
# Read the object back through shunt and compare what the client saw before the move. ETags are
# compared unquoted: xargs strips the quotes from the recorded value but not from the response.
got=$(aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api head-object --bucket "$BUCKET" --key "$1" --query ETag --output text 2>/dev/null | tr -d '"')
want=$(printf '%s' "$2" | tr -d '"')
[ "$got" = "$want" ] || echo "$1: $want -> ${got:-missing}"
VERIFY
export ENDPOINT BUCKET
awk '{print $1; print $2}' "$WORK/before.txt" | xargs -P 16 -n 2 sh "$WORK/verify.sh" > "$WORK/drift.txt"
if [ -s "$WORK/drift.txt" ]; then
  head -10 "$WORK/drift.txt"; fail "$(wc -l < "$WORK/drift.txt") objects changed ETag or vanished across the migration"
fi
note "$(wc -l < "$WORK/before.txt") objects verified by GET and ETag, including the multipart ones"

# The writer's objects have no recorded ETag, so any ETag counts: only "missing" is a failure.
awk '{print $0; print "-"}' "$WORK/writer.keys" | xargs -P 16 -n 2 sh "$WORK/verify.sh" | grep 'missing$' > "$WORK/lost.txt" || true
if [ -s "$WORK/lost.txt" ]; then
  head -10 "$WORK/lost.txt"; fail "$(wc -l < "$WORK/lost.txt") objects written during the migration are gone after cutover"
fi
note "$(wc -l < "$WORK/writer.keys") objects written during the migration all survived"

$SHUNT migrate finish "$KEY" | sed 's/^/   /'
now_primary=$($SHUNT status "$KEY" --json | jq -r '.placements[0].primary')
[ "$now_primary" = "$TO" ] || fail "after finish the primary is $now_primary, not $TO"

say "6c. Latency once the migration is over, on the new cluster alone"
go run ./test/bench/s3bench -via-only -mode resign -bucket "$BUCKET" \
  -matrix 4096:8:200,1048576:8:100 -runs 1 -baseline "$WORK/baseline.json" \
  $BENCH_ADDR | tee "$WORK/after.md" || fail "post-migration bench"

say "5. The record of what happened"
note "directory change log ($DIR.changes.jsonl):"
grep "\"$KEY\"" "$DIR.changes.jsonl" | tail -8 | sed 's/^/   /'
note "mover ledger (first lines):"
head -3 "$DATA/mover-$TENANT-$BUCKET.ledger.jsonl" | sed 's/^/   /'
note "ledger totals: $(awk -F'"result":"' '{split($2,a,"\""); c[a[1]]++} END{for(k in c) printf "%s=%d ", k, c[k]}' "$DATA/mover-$TENANT-$BUCKET.ledger.jsonl")"

say "6. What the migration cost the client"
note "during the migration, against the pre-migration baseline on $FROM:"
cat "$WORK/during.md"
note "after the migration, against the same baseline: what is left is the difference between the"
note "two vendors, not the migration. Subtract it from the table above to price the migration itself."
cat "$WORK/after.md"
note "shunt_migration_fallback_reads_total=$(metric shunt_migration_fallback_reads_total "$KEY") shunt_migration_dual_delete_total=$(metric shunt_migration_dual_delete_total "$KEY")"

if [ "$KEEP" = 0 ]; then
  s3api list-objects-v2 --bucket "$BUCKET" --query 'Contents[].Key' --output text | tr '\t' '\n' \
    | xargs -P 16 -I{} sh -c 'aws --endpoint-url "$1" --no-verify-ssl s3api delete-object --bucket "$2" --key "{}" >/dev/null 2>&1' _ "$ENDPOINT" "$BUCKET"
  s3api delete-bucket --bucket "$BUCKET" >/dev/null || true
fi

DEMO_OK=1
printf '\n\033[32mDEMO GREEN: %s moved from %s to %s with no client change.\033[0m\n' "$KEY" "$FROM" "$TO"
