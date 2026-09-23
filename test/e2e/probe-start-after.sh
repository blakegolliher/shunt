#!/usr/bin/env bash
# Probe ListObjectsV2 start-after (and V1 marker) semantics on one backend, for the N-way listing
# token of ADR-0018 N2 (docs/reference/backend-compat.md). It creates <bucket>, writes 8 one-byte
# objects, lists them 13 ways, and deletes the bucket. Keys avoid "a" next to "a/1": MinIO cannot
# hold both.
# usage: test/e2e/probe-start-after.sh <endpoint-url> <region> <access-key> <secret> <scratch-bucket>
set -uo pipefail
if [ $# -ne 5 ]; then
  echo "usage: $0 <endpoint-url> <region> <access-key> <secret> <scratch-bucket>" >&2
  echo "  e.g. $0 http://s3.example.com us-east-1 AKIA... \"\$SECRET\" shunt-probe-startafter" >&2
  echo "  The scratch bucket must not exist; the probe creates it, writes 8 objects, and deletes it." >&2
  exit 2
fi
EP=$1 REGION=$2 AK=$3 SK=$4 B=$5
export AWS_CONFIG_FILE=$(mktemp) AWS_SHARED_CREDENTIALS_FILE=/dev/null
printf '[default]\ns3 =\n  addressing_style = path\n' > "$AWS_CONFIG_FILE"
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
s3() { AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK aws --endpoint-url "$EP" --region "$REGION" "$@"; }
MAXCP=$'\U0010FFFF'
s3 s3api create-bucket --bucket "$B" >/dev/null 2>&1
for k in a/1 a/2 a/b/1 b/1 c "é" "ü/x" z; do printf x | s3 s3 cp - "s3://$B/$k" >/dev/null || echo "PUT $k failed"; done
list() { # label, extra args...
  local label=$1; shift
  printf '%-44s ' "$label"
  s3 s3api list-objects-v2 --bucket "$B" "$@" --output json 2>&1 |
    python3 -c 'import sys,json,urllib.parse as u
d=sys.stdin.read()
try: j=json.loads(d)
except Exception: print("ERROR", d.strip()[:160]); sys.exit()
enc=j.get("EncodingType")=="url"
f=(lambda s: u.unquote(s)) if enc else (lambda s: s)
print("keys=",[f(c["Key"]) for c in j.get("Contents",[])], "prefixes=",[f(p["Prefix"]) for p in j.get("CommonPrefixes",[])], "trunc=",j.get("IsTruncated"))'
}
list "all"
list "start-after a/1"                         --start-after a/1
list "start-after a/0 (absent)"                --start-after a/0
list "start-after é (non-ascii)"               --start-after "é"
list "start-after é, encoding url"             --start-after "é" --encoding-type url
list "delim /, no start"                       --delimiter /
list "delim /, start-after a/"                 --delimiter / --start-after a/
list "delim /, start-after a/ + U+10FFFF"      --delimiter / --start-after "a/$MAXCP"
list "delim /, start-after ü/ + U+10FFFF, url" --delimiter / --start-after "ü/$MAXCP" --encoding-type url
list "prefix a/, start-after a/1"              --prefix a/ --start-after a/1
list "max-keys 2, start-after a/2"             --max-keys 2 --start-after a/2
printf '%-44s ' "V1 marker a/1"
s3 s3api list-objects --bucket "$B" --marker a/1 --output json 2>&1 | python3 -c 'import sys,json; j=json.load(sys.stdin); print("keys=",[c["Key"] for c in j.get("Contents",[])])'
printf '%-44s ' "V1 delim /, marker a/ + U+10FFFF"
s3 s3api list-objects --bucket "$B" --delimiter / --marker "a/$MAXCP" --output json 2>&1 | python3 -c 'import sys,json; j=json.load(sys.stdin); print("keys=",[c["Key"] for c in j.get("Contents",[])], "prefixes=",[p["Prefix"] for p in j.get("CommonPrefixes",[])])'
s3 s3 rb "s3://$B" --force >/dev/null 2>&1
rm -f "$AWS_CONFIG_FILE"
