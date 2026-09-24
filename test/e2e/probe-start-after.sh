#!/usr/bin/env bash
# Probe ListObjectsV2 start-after (and V1 marker) semantics on one backend, for the N-way listing
# token of ADR-0018 N2 (docs/reference/backend-compat.md). Everything happens under a fresh prefix:
# a bucket that does not exist is created and deleted afterwards; an existing bucket is used as it
# is, and only the probe's own 8 objects are deleted. Keys avoid "a" next to "a/1": MinIO cannot
# hold both.
set -uo pipefail
if [ $# -ne 5 ]; then
  echo "usage: $0 <endpoint-url> <region> <access-key> <secret> <bucket>" >&2
  echo "  e.g. $0 http://s3.example.com us-east-1 AKIA... \"\$SECRET\" shunt-probe-startafter" >&2
  echo "  <bucket> is created if it does not exist (and deleted after), or used as it is if it does:" >&2
  echo "  the probe writes 8 one-byte objects under a fresh prefix there and deletes only those." >&2
  exit 2
fi
EP=$1 REGION=$2 AK=$3 SK=$4 B=$5
export AWS_CONFIG_FILE=$(mktemp) AWS_SHARED_CREDENTIALS_FILE=/dev/null
printf '[default]\ns3 =\n  addressing_style = path\n' > "$AWS_CONFIG_FILE"
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
s3() { AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK aws --endpoint-url "$EP" --region "$REGION" "$@"; }
MAXCP=$'\U0010FFFF'
P="shunt-probe-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')/" # every key and listing lives under it
KEYS=(a/1 a/2 a/b/1 b/1 c "é" "ü/x" z)

CREATED=
if s3 s3api head-bucket --bucket "$B" >/dev/null 2>&1; then
  echo "using existing bucket $B, under prefix $P"
elif out=$(s3 s3api create-bucket --bucket "$B" 2>&1); then
  CREATED=1
  echo "created bucket $B; the probe deletes it at the end"
else
  echo "cannot use bucket $B: it is not reachable with this key, and creating it failed:" >&2
  echo "  $out" >&2
  echo "Give the name of a bucket this key can write to, or let a user that may create buckets create $B first." >&2
  rm -f "$AWS_CONFIG_FILE"
  exit 1
fi
cleanup() {
  for k in "${KEYS[@]}"; do s3 s3api delete-object --bucket "$B" --key "$P$k" >/dev/null 2>&1; done
  [ -n "$CREATED" ] && s3 s3api delete-bucket --bucket "$B" >/dev/null 2>&1
  rm -f "$AWS_CONFIG_FILE"
}
trap cleanup EXIT
for k in "${KEYS[@]}"; do
  printf x | s3 s3 cp - "s3://$B/$P$k" >/dev/null || { echo "PUT $P$k failed; stopping" >&2; exit 1; }
done

# show strips the probe's prefix from keys and common prefixes; errors print as ERROR, not a trace.
show='import sys,json,urllib.parse as u
pre=sys.argv[1]
d=sys.stdin.read()
try: j=json.loads(d)
except Exception: print("ERROR", d.strip()[:160]); sys.exit()
enc=j.get("EncodingType")=="url"
f=lambda s: (u.unquote(s) if enc else s)
cut=lambda s: s[len(pre):] if s.startswith(pre) else s
print("keys=",[cut(f(c["Key"])) for c in j.get("Contents",[])], "prefixes=",[cut(f(p["Prefix"])) for p in j.get("CommonPrefixes",[])], "trunc=",j.get("IsTruncated"))'
list() { # label, extra args...
  local label=$1; shift
  printf '%-44s ' "$label"
  s3 s3api list-objects-v2 --bucket "$B" "$@" --output json 2>&1 | python3 -c "$show" "$P"
}
list1() { # label, extra args... (ListObjects v1)
  local label=$1; shift
  printf '%-44s ' "$label"
  s3 s3api list-objects --bucket "$B" "$@" --output json 2>&1 | python3 -c "$show" "$P"
}
list  "all"                                    --prefix "$P"
list  "start-after a/1"                        --prefix "$P" --start-after "${P}a/1"
list  "start-after a/0 (absent)"               --prefix "$P" --start-after "${P}a/0"
list  "start-after é (non-ascii)"              --prefix "$P" --start-after "${P}é"
list  "start-after é, encoding url"            --prefix "$P" --start-after "${P}é" --encoding-type url
list  "delim /, no start"                      --prefix "$P" --delimiter /
list  "delim /, start-after a/"                --prefix "$P" --delimiter / --start-after "${P}a/"
list  "delim /, start-after a/ + U+10FFFF"     --prefix "$P" --delimiter / --start-after "${P}a/$MAXCP"
list  "delim /, start-after ü/ + U+10FFFF, url" --prefix "$P" --delimiter / --start-after "${P}ü/$MAXCP" --encoding-type url
list  "prefix a/, start-after a/1"             --prefix "${P}a/" --start-after "${P}a/1"
list  "max-keys 2, start-after a/2"            --prefix "$P" --max-keys 2 --start-after "${P}a/2"
list1 "V1 marker a/1"                          --prefix "$P" --marker "${P}a/1"
list1 "V1 delim /, marker a/"                  --prefix "$P" --delimiter / --marker "${P}a/"
list1 "V1 delim /, marker a/ + U+10FFFF"       --prefix "$P" --delimiter / --marker "${P}a/$MAXCP"
