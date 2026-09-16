#!/usr/bin/env bash
# POC-4: take a whole cluster out of the estate. Every bucket it holds moves to another cluster
# while clients keep using them, and at the end the cluster is switched off with the clients none
# the wiser.
#
#   make e2e-up && make run-mixed        # in another terminal
#   test/e2e/evacuate.sh --from minio --to garage
#
# This is the fleet version of test/e2e/demo.sh: one command per step for every bucket, rather than
# one bucket walked through by hand.
set -euo pipefail

FROM=minio; TO=garage; OBJECTS=150; KEEP=0; STOP=1
while [ $# -gt 0 ]; do
  case "$1" in
    --from) FROM=$2; shift 2 ;;
    --to) TO=$2; shift 2 ;;
    --objects) OBJECTS=$2; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --no-stop) STOP=0; shift ;;   # leave the evacuated backend running
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
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
COMPOSE="podman compose"
trap 'rm -rf "$WORK"' EXIT

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
  MOVER_ACCEPT=-accept-lost-write-window # the mover refuses the same target without it
}

command -v aws >/dev/null || fail "aws-cli is not installed"
[ -f "$CFG" ] || fail "$CFG is missing; run make e2e-up first"
curl -fsS "$ADMIN/-/metrics" >/dev/null 2>&1 || fail "shunt is not running; start it with make run-mixed"
set -a; . "$DATA/garage.env"; set +a

# Both tenants take part: the cluster being retired holds buckets for more than one of them.
creds() { awk -v t="$1" '/access_key:/{ak=$3} /secret:/{sk=$2} /tenant:/{if($2==t){print ak, sk; exit}}' "$DATA/credentials.yaml"; }
use() { local c; c=$(creds "$1"); export AWS_ACCESS_KEY_ID=${c% *} AWS_SECRET_ACCESS_KEY=${c#* }; }
export AWS_DEFAULT_REGION=us-east-1 AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
s3api() { aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api "$@" 2>/dev/null; }
metric() { curl -fsS "$ADMIN/-/metrics" | awk -v n="$1" -v l="$2" '$0 ~ "^"n"{" && index($0,l){s+=$NF} END{printf "%.0f", s+0}'; }

case "$FROM" in minio) OWNER=e2e-b ;; garage) OWNER=e2e-a ;; *) fail "--from must be garage or minio" ;; esac

say "1. What $FROM is holding"
head -c 8192 /dev/urandom > "$WORK/blob.bin"
for n in one two; do
  use "$OWNER"
  s3api create-bucket --bucket "vendor-$n" >/dev/null || true
  seq 1 "$OBJECTS" | xargs -P 16 -I{} sh -c \
    'aws --endpoint-url "$1" --no-verify-ssl s3api put-object --bucket "$2" --key "data/{}" --body "$3" >/dev/null 2>&1' \
    _ "$ENDPOINT" "vendor-$n" "$WORK/blob.bin"
done
$SHUNT directory get -c "$CFG" --json | python3 -c '
import json,sys
d=json.load(sys.stdin)
on=[(k,p["names"].get(sys.argv[1],"")) for k,p in d["placements"].items() if p["primary"]==sys.argv[1]]
for k,n in sorted(on): print(f"   {k} -> {n}")
print(f"   {len(on)} buckets on {sys.argv[1]}")
' "$FROM"

# Record what every client can see right now, per tenant, so it can be checked at the end.
: > "$WORK/before.txt"
for t in e2e-a e2e-b; do
  use "$t"
  for b in $(s3api list-buckets --query 'Buckets[].Name' --output text | tr '\t' '\n'); do
    # Keys here include spaces, "+" and "%" (the s3diff bucket), so fields are kept tab-separated
    # from end to end and never re-split on whitespace.
    s3api list-objects-v2 --bucket "$b" --query 'Contents[].[Key,ETag]' --output text 2>/dev/null \
      | awk -F'\t' -v t="$t" -v b="$b" 'NF>=2{print t "\t" b "\t" $1 "\t" $2}' >> "$WORK/before.txt"
  done
done
note "$(wc -l < "$WORK/before.txt") objects across both tenants, on both clusters"

say "2. Start every bucket on $FROM moving to $TO"
migrate_start --from "$FROM" --to "$TO" --create -c "$CFG"
$SHUNT migrate status -c "$CFG" | sed 's/^/   /'

say "3. Move the bytes"
go run ./test/mover -config "$CFG" -from "$FROM" -state-dir "$DATA" ${MOVER_ACCEPT:-} | sed 's/^/   /'
note "a second pass must copy nothing:"
go run ./test/mover -config "$CFG" -from "$FROM" -state-dir "$DATA" ${MOVER_ACCEPT:-} | sed 's/^/   /'
note "fallback reads so far: $(metric shunt_migration_fallback_reads_total '')"

say "4. Cut over and let go"
$SHUNT cutover --from "$FROM" -c "$CFG" | sed 's/^/   /'
$SHUNT migrate finish --from "$FROM" -c "$CFG" | sed 's/^/   /'
# Repoint any tenant that would still create new buckets on the cluster being retired.
for t in $($SHUNT directory get -c "$CFG" --json | python3 -c '
import json,sys
d=json.load(sys.stdin)
print(" ".join(n for n,t in sorted((d.get("tenants") or {}).items()) if t.get("default_cluster")==sys.argv[1]))
' "$FROM"); do
  $SHUNT directory set-default "$t" "$TO" -c "$CFG" | sed 's/^/   /'
done
if grep -q "$FROM" "$DIR"; then
  grep -n "$FROM" "$DIR" | sed 's/^/   /'; fail "$DIR still names $FROM"
fi
note "nothing in the directory names $FROM any more"

say "5. Every object, from every client, unchanged"
cat > "$WORK/verify.sh" <<'VERIFY'
#!/bin/sh
got=$(aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api head-object --bucket "$2" --key "$3" --query ETag --output text 2>/dev/null | tr -d '"')
want=$(printf '%s' "$4" | tr -d '"')
[ "$got" = "$want" ] || echo "$1 $2/$3: $want -> ${got:-missing}"
VERIFY
: > "$WORK/bad.txt"
for t in e2e-a e2e-b; do
  use "$t"
  export ENDPOINT
  awk -F'\t' -v t="$t" '$1==t{printf "%s%c%s%c%s%c%s%c", $1, 0, $2, 0, $3, 0, $4, 0}' "$WORK/before.txt" \
    | xargs -0 -P 16 -n 4 sh "$WORK/verify.sh" >> "$WORK/bad.txt" || true
done
if [ -s "$WORK/bad.txt" ]; then head -10 "$WORK/bad.txt"; fail "$(wc -l < "$WORK/bad.txt") objects changed or vanished"; fi
note "$(wc -l < "$WORK/before.txt") objects verified by ETag, through the same endpoint and credentials as before"

say "6. Switch $FROM off"
if [ "$STOP" = 1 ]; then
  $COMPOSE -f "$E2E/docker-compose.yml" stop "shunt-e2e-$FROM" >/dev/null 2>&1 || podman stop "shunt-e2e-$FROM" >/dev/null
  note "container shunt-e2e-$FROM stopped"
  for t in e2e-a e2e-b; do
    use "$t"
    n=$(s3api list-buckets --query 'length(Buckets)' --output text)
    note "$t still sees $n buckets"
  done
  use "$OWNER"
  s3api head-object --bucket vendor-one --key data/1 --query ETag --output text | sed 's/^/   vendor-one\/data\/1 ETag /'
  s3api put-object --bucket vendor-one --key after-shutdown --body "$WORK/blob.bin" >/dev/null \
    && note "a new write to vendor-one succeeded with $FROM switched off"
  $COMPOSE -f "$E2E/docker-compose.yml" start "shunt-e2e-$FROM" >/dev/null 2>&1 || podman start "shunt-e2e-$FROM" >/dev/null
  note "container shunt-e2e-$FROM started again, so the box is left as it was found"
fi

if [ "$KEEP" = 0 ]; then
  use "$OWNER"
  for n in one two; do
    s3api list-objects-v2 --bucket "vendor-$n" --query 'Contents[].Key' --output text | tr '\t' '\n' \
      | xargs -P 16 -I{} sh -c 'aws --endpoint-url "$1" --no-verify-ssl s3api delete-object --bucket "$2" --key "{}" >/dev/null 2>&1' _ "$ENDPOINT" "vendor-$n"
    s3api delete-bucket --bucket "vendor-$n" >/dev/null || true
  done
fi

printf '\n\033[32mVENDOR REMOVED: every bucket on %s now lives on %s, and no client was changed.\033[0m\n' "$FROM" "$TO"
