#!/usr/bin/env bash
# POC-5 operator walkthrough, unattended: docs/walkthrough.md step for step, with an assertion after
# every step. Bucket data01 moves from cluster vast01 to cluster vast02 (as data01-001) behind one
# shunt on :8008, while `shunt verify` reads and writes through that shunt from step 4 to step 10.
# The script fails if verify reports a single error.
#
#   test/e2e/walkthrough.sh --src http://vast01.lab:80 --src-creds vast01.creds \
#                           --dst http://vast02.lab:80 --dst-creds vast02.creds
#   make walkthrough      # the same on the e2e Garage (playing vast01) and MinIO (playing vast02)
#
# A creds file holds two lines: access_key=... and secret=... . Both buckets are created here if
# missing; --reset deletes them first. shunt runs from bin/shunt on --listen with a fresh config
# and directory under --work, and is stopped by its pid at the end.
set -euo pipefail

SRC=; SRC_CREDS=; SRC_TYPE=vast; SRC_REGION=us-east-1; SRC_COND=true
DST=; DST_CREDS=; DST_TYPE=vast; DST_REGION=us-east-1; DST_COND=true
SRC_NAME=vast01; DST_NAME=vast02; BUCKET=data01
LISTEN=127.0.0.1:8008; ADMIN=127.0.0.1:9900; WINDOW=15s; OBJECTS=100; WORK=; RESET=0; KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --src) SRC=$2; shift 2 ;;
    --src-creds) SRC_CREDS=$2; shift 2 ;;
    --src-type) SRC_TYPE=$2; shift 2 ;;
    --src-region) SRC_REGION=$2; shift 2 ;;
    --src-conditional-write) SRC_COND=$2; shift 2 ;;
    --dst) DST=$2; shift 2 ;;
    --dst-creds) DST_CREDS=$2; shift 2 ;;
    --dst-type) DST_TYPE=$2; shift 2 ;;
    --dst-region) DST_REGION=$2; shift 2 ;;
    --dst-conditional-write) DST_COND=$2; shift 2 ;;
    --listen) LISTEN=$2; shift 2 ;;
    --admin) ADMIN=$2; shift 2 ;;
    --window) WINDOW=$2; shift 2 ;;
    --objects) OBJECTS=$2; shift 2 ;;
    --work) WORK=$2; shift 2 ;;
    --reset) RESET=1; shift ;;     # delete data01 on the source and data01-001 on the target first
    --keep) KEEP=1; shift ;;       # leave data01-001 and its objects on the target at the end
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown flag $1" >&2; exit 2 ;;
  esac
done

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
SHUNT=$ROOT/bin/shunt
DST_BUCKET=$BUCKET-001
KEY=$BUCKET   # the default tenant's bucket: the client key names no tenant (ADR-0010)
[ -n "$WORK" ] || WORK=$(mktemp -d)
mkdir -p "$WORK"
WORK=$(cd "$WORK" && pwd)
export SHUNT_API=http://$ADMIN
unset SHUNT_API_TOKEN_REF

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\n\033[31mWALKTHROUGH FAILED: %s\033[0m\n' "$*" >&2; exit 1; }
# run prints a command the way docs/walkthrough.md shows it, then runs it with its output indented.
run() {
  local shown=("$@")
  [ "${shown[0]}" = "$SHUNT" ] && shown[0]=shunt
  printf '   $ %s\n' "${shown[*]}"
  "$@" 2>&1 | sed 's/^/     /'
  return "${PIPESTATUS[0]}"
}
# refused runs a command that must be refused, and fails the walkthrough if it is not.
refused() {
  local out
  printf '   $ shunt %s   # must be refused\n' "${*:2}"
  if out=$("$@" 2>&1); then printf '%s\n' "$out" | sed 's/^/     /'; fail "shunt ${*:2} was accepted"; fi
  printf '%s\n' "$out" | sed 's/^/     /'
  grep -q 'refused' <<<"$out" || fail "shunt ${*:2} failed for another reason than a refusal"
}

# --- inputs -----------------------------------------------------------------------------------
for v in SRC SRC_CREDS DST DST_CREDS; do [ -n "${!v}" ] || fail "--$(tr 'A-Z_' 'a-z-' <<<"$v") is required"; done
command -v aws >/dev/null || fail "aws-cli is not installed"
command -v jq >/dev/null || fail "jq is not installed"
[ -x "$SHUNT" ] || fail "$SHUNT is missing; run make build"
cred() { awk -F= -v k="$1" '$1==k{sub(/^[^=]*=/, ""); print; exit}' "$2"; }
SRC_AK=$(cred access_key "$SRC_CREDS"); SRC_SK=$(cred secret "$SRC_CREDS")
DST_AK=$(cred access_key "$DST_CREDS"); DST_SK=$(cred secret "$DST_CREDS")
[ -n "$SRC_AK" ] && [ -n "$SRC_SK" ] || fail "$SRC_CREDS needs access_key= and secret= lines"
[ -n "$DST_AK" ] && [ -n "$DST_SK" ] || fail "$DST_CREDS needs access_key= and secret= lines"
scheme() { printf '%s' "${1%%://*}"; }
hostport() { local h=${1#*://}; h=${h%%/*}; case "$h" in *:*) printf '%s' "$h" ;; *) [ "$(scheme "$1")" = https ] && printf '%s:443' "$h" || printf '%s:80' "$h" ;; esac; }

# Secrets reach shunt as file: refs, never inline: a cluster added to a running shunt can only use
# a secret shunt can read now, and an env: var would have had to be set when it started.
mkdir -p "$WORK/secrets"; chmod 700 "$WORK/secrets"
umask 077
printf '%s' "$SRC_SK" > "$WORK/secrets/$SRC_NAME.secret"
printf '%s' "$DST_SK" > "$WORK/secrets/$DST_NAME.secret"
CLIENT_AK=SHUNTWALK$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n' | tr a-f A-F)
CLIENT_SK=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
printf '%s' "$CLIENT_SK" > "$WORK/secrets/client.secret"
umask 022

export AWS_CONFIG_FILE=$WORK/aws.config AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
printf '[default]\ns3 =\n  addressing_style = path\n' > "$AWS_CONFIG_FILE"
on_src()   { AWS_ACCESS_KEY_ID=$SRC_AK AWS_SECRET_ACCESS_KEY=$SRC_SK aws --endpoint-url "$SRC" --region "$SRC_REGION" "$@"; }
on_dst()   { AWS_ACCESS_KEY_ID=$DST_AK AWS_SECRET_ACCESS_KEY=$DST_SK aws --endpoint-url "$DST" --region "$DST_REGION" "$@"; }
on_shunt() { AWS_ACCESS_KEY_ID=$CLIENT_AK AWS_SECRET_ACCESS_KEY=$CLIENT_SK aws --endpoint-url "http://$LISTEN" --region us-east-1 "$@"; }

SHUNT_PID=; VERIFY_PID=
cleanup() {
  local rc=$?
  if [ -n "$VERIFY_PID" ] && kill -0 "$VERIFY_PID" 2>/dev/null; then kill -INT "$VERIFY_PID"; wait "$VERIFY_PID" 2>/dev/null || true; fi
  if [ -n "$SHUNT_PID" ] && kill -0 "$SHUNT_PID" 2>/dev/null; then
    kill -TERM "$SHUNT_PID"
    for _ in $(seq 1 100); do kill -0 "$SHUNT_PID" 2>/dev/null || break; sleep 0.1; done
  fi
  [ "$rc" = 0 ] || printf '   logs: %s/shunt.log, %s/verify.log\n' "$WORK" "$WORK" >&2
  return 0
}
trap cleanup EXIT

if (exec 3<>"/dev/tcp/${LISTEN%:*}/${LISTEN##*:}") 2>/dev/null; then fail "$LISTEN is already in use"; fi
if (exec 3<>"/dev/tcp/${ADMIN%:*}/${ADMIN##*:}") 2>/dev/null; then fail "$ADMIN is already in use"; fi

empty_bucket() { # empty_bucket on_src|on_dst bucket: delete it and everything in it, if it exists
  if "$1" s3api head-bucket --bucket "$2" >/dev/null 2>&1; then
    "$1" s3 rb "s3://$2" --force >/dev/null
  fi
}
if [ "$RESET" = 1 ]; then
  empty_bucket on_src "$BUCKET"
  empty_bucket on_dst "$DST_BUCKET"
fi
if on_dst s3api head-bucket --bucket "$DST_BUCKET" >/dev/null 2>&1 &&
   [ "$(on_dst s3api list-objects-v2 --bucket "$DST_BUCKET" --max-keys 1 --query 'length(Contents || `[]`)' --output text)" != 0 ]; then
  fail "$DST_BUCKET on $DST is not empty; pass --reset to start over"
fi

# ----------------------------------------------------------------------------------------------
say "1. Bucket $BUCKET on $SRC_NAME ($SRC), with data in it"
on_src s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 || on_src s3api create-bucket --bucket "$BUCKET" >/dev/null
mkdir -p "$WORK/seed"
for i in $(seq 1 "$OBJECTS"); do head -c $((1024 + RANDOM)) /dev/urandom > "$WORK/seed/obj-$i"; done
on_src s3 cp --recursive --quiet "$WORK/seed" "s3://$BUCKET/seed/"
n=$(on_src s3api list-objects-v2 --bucket "$BUCKET" --prefix seed/ --query 'length(Contents || `[]`)' --output text)
[ "$n" = "$OBJECTS" ] || fail "seeded $OBJECTS objects, $SRC lists $n"
note "$n objects under seed/ in $BUCKET on $SRC_NAME, written directly, before shunt exists"

# ----------------------------------------------------------------------------------------------
say "2. shunt on :${LISTEN##*:}"
cat > "$WORK/credentials.yaml" <<EOF
credentials:
  - access_key: $CLIENT_AK
    secret: $CLIENT_SK
EOF
chmod 600 "$WORK/credentials.yaml"
printf 'version: 1\n' > "$WORK/directory.yaml"
cat > "$WORK/shunt.yaml" <<EOF
listener: { address: "$LISTEN", plaintext: true }
admin: { address: "$ADMIN" }
auth: { mode: resign, credentials_file: $WORK/credentials.yaml }
directory: { file: $WORK/directory.yaml, poll_interval: 1s }
features: { debug_route_header: true }
EOF
run "$SHUNT" check-config "$WORK/shunt.yaml" || fail "check-config"
nohup "$SHUNT" serve -c "$WORK/shunt.yaml" > "$WORK/shunt.log" 2>&1 &
SHUNT_PID=$!
echo "$SHUNT_PID" > "$WORK/shunt.pid"
for _ in $(seq 1 100); do curl -fsS "http://$ADMIN/-/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 "$SHUNT_PID" 2>/dev/null || { cat "$WORK/shunt.log"; fail "shunt serve exited"; }
note "shunt serve running, pid $SHUNT_PID; its startup lines:"
sed 's/^/     /' "$WORK/shunt.log"
lines=$(wc -l < "$WORK/shunt.log"); marked=$(grep -c 'PLAINTEXT http (listener.plaintext: true; lab use only)' "$WORK/shunt.log" || true)
[ "$lines" -gt 0 ] && [ "$lines" = "$marked" ] || fail "$marked of $lines startup lines carry the plaintext warning"

run "$SHUNT" cluster add "$SRC_NAME" --type "$SRC_TYPE" --scheme "$(scheme "$SRC")" --region "$SRC_REGION" \
  --endpoint "$(hostport "$SRC")" --access-key "$SRC_AK" --secret-ref "file:$WORK/secrets/$SRC_NAME.secret" \
  --conditional-write="$SRC_COND" || fail "cluster add $SRC_NAME"
run "$SHUNT" adopt "$SRC_NAME" "$KEY" || fail "adopt"
run "$SHUNT" status "$KEY" || fail "status"

# ----------------------------------------------------------------------------------------------
say "3. A client points at http://${LISTEN}, bucket name unchanged"
printf '   $ aws --endpoint-url http://%s s3 ls s3://%s/seed/ | wc -l\n' "$LISTEN" "$BUCKET"
n=$(on_shunt s3 ls "s3://$BUCKET/seed/" | wc -l)
note "$n"
[ "$n" = "$OBJECTS" ] || fail "shunt lists $n of $OBJECTS seeded objects"
on_shunt s3 cp --quiet "s3://$BUCKET/seed/obj-1" "$WORK/obj-1.via-shunt"
cmp -s "$WORK/seed/obj-1" "$WORK/obj-1.via-shunt" || fail "seed/obj-1 read through shunt differs"
note "seed/obj-1 read back through shunt, byte for byte"

# ----------------------------------------------------------------------------------------------
say "4. Reads and writes, continuously, from here to the end"
verify_args=(--endpoint "http://$LISTEN" --bucket "$BUCKET" --access-key "$CLIENT_AK" --secret-ref "file:$WORK/secrets/client.secret" --debug-route)
printf '   $ shunt verify %s --interval 15s &\n' "${verify_args[*]}"
nohup "$SHUNT" verify "${verify_args[@]}" --interval 15s --json-out "$WORK/verify.json" > "$WORK/verify.log" 2>&1 &
VERIFY_PID=$!
sleep 3
kill -0 "$VERIFY_PID" 2>/dev/null || { cat "$WORK/verify.log"; fail "verify exited early"; }
note "verify running, pid $VERIFY_PID (progress in $WORK/verify.log)"

# checkpoint prints verify's latest progress line and fails on any error it reports so far.
checkpoint() {
  kill -0 "$VERIFY_PID" 2>/dev/null || { cat "$WORK/verify.log"; fail "verify stopped"; }
  local last
  last=$(grep '^verify: [0-9]' "$WORK/verify.log" | tail -n 1 || true)
  [ -z "$last" ] || note "$last"
  if [ -n "$last" ] && ! grep -q ' 0 errors' <<<"$last"; then fail "verify reports errors: $last"; fi
}

# ----------------------------------------------------------------------------------------------
say "5. Bucket $DST_BUCKET on $DST_NAME ($DST)"
printf '   $ aws --endpoint-url %s s3api create-bucket --bucket %s\n' "$DST" "$DST_BUCKET"
on_dst s3api head-bucket --bucket "$DST_BUCKET" >/dev/null 2>&1 || on_dst s3api create-bucket --bucket "$DST_BUCKET" >/dev/null

# ----------------------------------------------------------------------------------------------
say "6. Add $DST_NAME to the running shunt"
run "$SHUNT" cluster add "$DST_NAME" --type "$DST_TYPE" --scheme "$(scheme "$DST")" --region "$DST_REGION" \
  --endpoint "$(hostport "$DST")" --access-key "$DST_AK" --secret-ref "file:$WORK/secrets/$DST_NAME.secret" \
  --conditional-write="$DST_COND" || fail "cluster add $DST_NAME"
run "$SHUNT" expand "$KEY" --to "$DST_NAME" || fail "expand"
kill -0 "$SHUNT_PID" 2>/dev/null || fail "shunt serve is gone"
note "same shunt process (pid $SHUNT_PID): no restart"
checkpoint

# ----------------------------------------------------------------------------------------------
say "7. Ramp writes 50/50 by key hash"
run "$SHUNT" ramp "$KEY" --ratio 0.5 || fail "ramp 0.5"
hash=$(curl -fsS "$SHUNT_API/v1/placements/default/$BUCKET" | jq -r '.placement.ramp.hash')
[ "$hash" = fnv1a-fmix64-v1 ] || fail "the ramp does not record its hash (got $hash)"
note "the ramp records the hash that splits its keys: ramp.hash $hash"

# ----------------------------------------------------------------------------------------------
say "8. The split, then all writes to $DST_NAME"
split_args=("${verify_args[@]}" --keys 1000 --interval 0 --cleanup)
printf '   $ shunt verify %s --keys 1000 --duration 20s --cleanup\n' "${verify_args[*]}"
"$SHUNT" verify "${split_args[@]}" --duration 20s --json-out "$WORK/split-0.5.json" > "$WORK/split-0.5.log" 2>&1 \
  || { cat "$WORK/split-0.5.log"; fail "verify at ratio 0.5"; }
sed -n '/^verify report/,$p' "$WORK/split-0.5.log" | sed 's/^/     /'
jq -e '(.writes_by_side.primary // 0) as $p | (.writes_by_side.source // 0) as $s
       | ($p + $s) > 0 and ($p / ($p + $s)) >= 0.40 and ($p / ($p + $s)) <= 0.60' "$WORK/split-0.5.json" >/dev/null \
  || fail "the write split at ratio 0.5 is outside 40-60%"
jq -e '(.reads_by_side.primary // 0) > 0 and (.reads_by_side.source // 0) > 0' "$WORK/split-0.5.json" >/dev/null \
  || fail "reads did not succeed on both sides at ratio 0.5"
note "write split within 40-60%, and reads served from both $SRC_NAME and $DST_NAME"
run "$SHUNT" status "$KEY" || fail "status"
checkpoint

run "$SHUNT" ramp "$KEY" --ratio 1.0 || fail "ramp 1.0"
printf '   $ shunt verify %s --keys 1000 --duration 10s --cleanup\n' "${verify_args[*]}"
"$SHUNT" verify "${split_args[@]}" --duration 10s --json-out "$WORK/split-1.0.json" > "$WORK/split-1.0.log" 2>&1 \
  || { cat "$WORK/split-1.0.log"; fail "verify at ratio 1.0"; }
sed -n '/^verify report/,$p' "$WORK/split-1.0.log" | sed 's/^/     /'
jq -e '(.writes_by_side.primary // 0) > 0 and (.writes_by_side.source // 0) == 0' "$WORK/split-1.0.json" >/dev/null \
  || fail "at ratio 1.0 a write still went to the source"
note "every write goes to $DST_NAME"

# ----------------------------------------------------------------------------------------------
say "9. Move the rest with the mover, then cut over"
accept=()
[ "$DST_COND" = true ] || accept=(--accept-lost-write-window)
run "$SHUNT" migrate start "$KEY" "${accept[@]}" || fail "migrate start"
refused "$SHUNT" purge-source "$KEY"
refused "$SHUNT" cluster remove "$SRC_NAME"
converged=0
for attempt in 1 2 3; do
  run "$SHUNT" migrate run "$KEY" --until-converged --cursor-dir "$WORK" --ledger-dir "$WORK" "${accept[@]}" || fail "migrate run"
  run "$SHUNT" status "$KEY" || fail "status"
  if run "$SHUNT" cutover "$KEY" --window "$WINDOW"; then converged=1; break; fi
  note "cutover refused (attempt $attempt); running the mover again"
done
[ "$converged" = 1 ] || fail "cutover was refused three times"
checkpoint

# ----------------------------------------------------------------------------------------------
say "10. Purge $BUCKET from $SRC_NAME and remove $SRC_NAME"
run "$SHUNT" purge-source "$KEY" || fail "purge-source"
refused "$SHUNT" cluster remove "$SRC_NAME"
run "$SHUNT" tenant set-default "$DST_NAME" || fail "tenant set-default"
run "$SHUNT" cluster remove "$SRC_NAME" || fail "cluster remove"
run "$SHUNT" status "$KEY" || fail "status"
if on_src s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then fail "$BUCKET still exists on $SRC_NAME"; fi
note "$BUCKET no longer exists on $SRC_NAME"
rm -rf "$WORK/readback"
on_shunt s3 cp --recursive --quiet "s3://$BUCKET/seed/" "$WORK/readback/"
diff -r "$WORK/seed" "$WORK/readback" >/dev/null || fail "the seeded objects read back through shunt differ"
note "all $OBJECTS objects written to $SRC_NAME before shunt existed read back through shunt from $DST_NAME"
checkpoint

kill -INT "$VERIFY_PID"
vrc=0; wait "$VERIFY_PID" || vrc=$?
VERIFY_PID=
note "verify, steps 4 to 10:"
sed -n '/^verify report/,$p' "$WORK/verify.log" | sed 's/^/     /'
errors=$(jq -r '.errors' "$WORK/verify.json")
[ "$vrc" = 0 ] && [ "$errors" = 0 ] || fail "verify reported $errors errors (exit $vrc)"

if [ "$KEEP" = 0 ]; then
  kill -TERM "$SHUNT_PID"; wait "$SHUNT_PID" 2>/dev/null || true; SHUNT_PID=
  empty_bucket on_dst "$DST_BUCKET"
fi

printf '\n\033[32mWALKTHROUGH GREEN: %s moved from %s/%s to %s/%s; verify saw 0 errors in %s operations.\033[0m\n' \
  "$KEY" "$SRC_NAME" "$BUCKET" "$DST_NAME" "$DST_BUCKET" "$(jq -r '.ops' "$WORK/verify.json")"
