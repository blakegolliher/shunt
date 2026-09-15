#!/usr/bin/env bash
# Generate a self-signed wildcard certificate for the domain in docs/CONTEXT.md.
# Usage: DOMAIN=shunt.example.com test/e2e/gen-cert.sh
# Output: test/e2e/certs/wildcard.{crt,key}; SANs are *.DOMAIN and DOMAIN.
# Portable across OpenSSL 1.1/3.x and LibreSSL (macOS): extensions come from a config file,
# not from `-addext`, and the SAN check uses `-text` rather than `-ext`.
set -euo pipefail

DOMAIN="${DOMAIN:-shunt.example.com}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
CRT="$DIR/wildcard.crt"
KEY="$DIR/wildcard.key"
DAYS="${DAYS:-825}"

mkdir -p "$DIR"
if [[ -s "$CRT" && -s "$KEY" ]] && openssl x509 -in "$CRT" -noout -checkend 86400 >/dev/null 2>&1 \
   && openssl x509 -in "$CRT" -noout -text 2>/dev/null | grep -q "DNS:\*\.$DOMAIN"; then
  echo "gen-cert: $CRT already covers *.$DOMAIN"
  exit 0
fi

CONF="$(mktemp)"
trap 'rm -f "$CONF"' EXIT
cat > "$CONF" <<CONF
[req]
distinguished_name = dn
x509_extensions = ext
prompt = no
[dn]
CN = *.$DOMAIN
O = shunt e2e
[ext]
subjectAltName = DNS:*.$DOMAIN, DNS:$DOMAIN
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
CONF

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$KEY" -out "$CRT" -days "$DAYS" -config "$CONF" >/dev/null 2>&1
chmod 600 "$KEY"
echo "gen-cert: wrote $CRT and $KEY for *.$DOMAIN and $DOMAIN"
openssl x509 -in "$CRT" -noout -subject -enddate
openssl x509 -in "$CRT" -noout -text | grep 'DNS:'
