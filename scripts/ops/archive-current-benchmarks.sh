#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAMP="${1:-$(date +%Y%m%d-%H%M%S)}"
DEST_DIR="${ROOT_DIR}/target/archive/${STAMP}"

mkdir -p "${DEST_DIR}/binary-commit" "${DEST_DIR}/binary-bench-results" "${DEST_DIR}/binary-matrix" "${DEST_DIR}/http-crossing"

copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -f "${src}" ]]; then
    cp "${src}" "${dst}"
  fi
}

copy_if_exists "${ROOT_DIR}/target/current/core-only-crossing-current.json" "${DEST_DIR}/core-only-crossing-current.json"
copy_if_exists "${ROOT_DIR}/target/current/embed-journaled-core/report-clean.json" "${DEST_DIR}/embed-journaled-core-current.json"
copy_if_exists "${ROOT_DIR}/target/current/single-node-http/report-clean.json" "${DEST_DIR}/single-node-http-current.json"
copy_if_exists "${ROOT_DIR}/target/current/single-node-binary/report-clean.json" "${DEST_DIR}/single-node-binary-current.json"
copy_if_exists "${ROOT_DIR}/target/current/direct-ha-binary/report-clean.json" "${DEST_DIR}/direct-ha-binary-current.json"

copy_if_exists "${ROOT_DIR}/target/current/binary-bench-results/binary-1p1s-grpc-crossing-current.json" "${DEST_DIR}/binary-bench-results/binary-1p1s-grpc-crossing-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-bench-results/binary-1p1s-aeron-crossing-current.json" "${DEST_DIR}/binary-bench-results/binary-1p1s-aeron-crossing-current.json"
copy_if_exists "${ROOT_DIR}/target/current/http-crossing/grpc-crossing-benchmark-report.json" "${DEST_DIR}/http-crossing/grpc-crossing-benchmark-report.json"
copy_if_exists "${ROOT_DIR}/target/current/http-crossing/aeron-crossing-benchmark-report.json" "${DEST_DIR}/http-crossing/aeron-crossing-benchmark-report.json"

copy_if_exists "${ROOT_DIR}/target/current/binary-commit/grpc-1p1s-current.json" "${DEST_DIR}/binary-commit/grpc-1p1s-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-commit/aeron-1p1s-current.json" "${DEST_DIR}/binary-commit/aeron-1p1s-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-commit/grpc-1p2s-quorum-current.json" "${DEST_DIR}/binary-commit/grpc-1p2s-quorum-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-commit/aeron-1p2s-quorum-current.json" "${DEST_DIR}/binary-commit/aeron-1p2s-quorum-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-commit/grpc-1p3s-quorum-current.json" "${DEST_DIR}/binary-commit/grpc-1p3s-quorum-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-commit/aeron-1p3s-quorum-current.json" "${DEST_DIR}/binary-commit/aeron-1p3s-quorum-current.json"

copy_if_exists "${ROOT_DIR}/target/current/binary-matrix/grpc-1p2s-quorum-current.json" "${DEST_DIR}/binary-matrix/grpc-1p2s-quorum-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-matrix/aeron-1p2s-quorum-current.json" "${DEST_DIR}/binary-matrix/aeron-1p2s-quorum-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-matrix/grpc-1p3s-quorum-current.json" "${DEST_DIR}/binary-matrix/grpc-1p3s-quorum-current.json"
copy_if_exists "${ROOT_DIR}/target/current/binary-matrix/aeron-1p3s-quorum-current.json" "${DEST_DIR}/binary-matrix/aeron-1p3s-quorum-current.json"

cat > "${DEST_DIR}/README.md" <<EOF
# Archived Current Benchmark Baseline

Archive created at: ${STAMP}

This directory contains a copy of the repository's current benchmark fact sources at the time of archival.
Use these files for historical comparison only. The open-source README should continue to point to the active \`current\` files under \`target/current/\`.
EOF

echo "${DEST_DIR}"
