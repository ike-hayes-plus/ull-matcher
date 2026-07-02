#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${1:-${ROOT_DIR}/target/lab/tls}"
DAYS="${DAYS:-3650}"

command -v openssl >/dev/null 2>&1 || {
  echo "openssl is required" >&2
  exit 1
}

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

CA_KEY="${OUT_DIR}/ca.key"
CA_CERT="${OUT_DIR}/ca.crt"
SERIAL_FILE="${OUT_DIR}/ca.srl"

openssl req -x509 -newkey rsa:4096 -sha256 -nodes \
  -keyout "${CA_KEY}" \
  -out "${CA_CERT}" \
  -days "${DAYS}" \
  -subj "/CN=ull-matcher-lab-ca" >/dev/null 2>&1

issue_node_cert() {
  local node_id="$1"
  local node_dir="${OUT_DIR}/${node_id}"
  local key_file="${node_dir}/tls.key"
  local cert_file="${node_dir}/tls.crt"
  local csr_file="${node_dir}/tls.csr"
  local ext_file="${node_dir}/tls.ext"

  mkdir -p "${node_dir}"
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -keyout "${key_file}" \
    -out "${csr_file}" \
    -subj "/CN=${node_id}" >/dev/null 2>&1
  cat > "${ext_file}" <<EOF
subjectAltName=DNS:${node_id},DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
keyUsage=digitalSignature,keyEncipherment
EOF
  openssl x509 -req \
    -in "${csr_file}" \
    -CA "${CA_CERT}" \
    -CAkey "${CA_KEY}" \
    -CAcreateserial \
    -CAserial "${SERIAL_FILE}" \
    -out "${cert_file}" \
    -days "${DAYS}" \
    -sha256 \
    -extfile "${ext_file}" >/dev/null 2>&1
  cp "${CA_CERT}" "${node_dir}/ca.crt"
  rm -f "${csr_file}" "${ext_file}"
}

issue_node_cert node-a
issue_node_cert node-b
issue_node_cert node-c

echo "${OUT_DIR}"
