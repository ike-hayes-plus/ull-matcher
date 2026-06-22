#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TLS_ROOT="${TLS_ROOT:-${ROOT_DIR}/target/lab/tls}"
NODE_ID="${1:-}"

if [[ -z "${NODE_ID}" ]]; then
  echo "usage: $0 <node-id>" >&2
  exit 1
fi

command -v openssl >/dev/null 2>&1 || {
  echo "openssl is required" >&2
  exit 1
}

NODE_DIR="${TLS_ROOT}/${NODE_ID}"
CA_KEY="${TLS_ROOT}/ca.key"
CA_CERT="${TLS_ROOT}/ca.crt"
SERIAL_FILE="${TLS_ROOT}/ca.srl"

if [[ ! -d "${NODE_DIR}" || ! -f "${CA_KEY}" || ! -f "${CA_CERT}" ]]; then
  echo "missing TLS materials under ${TLS_ROOT}" >&2
  exit 2
fi

tmp_dir="$(mktemp -d "${NODE_DIR}/rotate.XXXXXX")"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout "${tmp_dir}/tls.key" \
  -out "${tmp_dir}/tls.csr" \
  -subj "/CN=${NODE_ID}-rotated-$(date +%s)" >/dev/null 2>&1
cat > "${tmp_dir}/tls.ext" <<EOF
subjectAltName=DNS:${NODE_ID},DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
keyUsage=digitalSignature,keyEncipherment
EOF
openssl x509 -req \
  -in "${tmp_dir}/tls.csr" \
  -CA "${CA_CERT}" \
  -CAkey "${CA_KEY}" \
  -CAcreateserial \
  -CAserial "${SERIAL_FILE}" \
  -out "${tmp_dir}/tls.crt" \
  -days "${DAYS:-3650}" \
  -sha256 \
  -extfile "${tmp_dir}/tls.ext" >/dev/null 2>&1

mv "${tmp_dir}/tls.key" "${NODE_DIR}/tls.key"
mv "${tmp_dir}/tls.crt" "${NODE_DIR}/tls.crt"
cp "${CA_CERT}" "${NODE_DIR}/ca.crt"
rm -f "${tmp_dir}/tls.csr" "${tmp_dir}/tls.ext"

echo "${NODE_DIR}"
