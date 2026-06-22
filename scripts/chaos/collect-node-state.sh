#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-}"
BASE_URL="${2:-}"

if [[ -z "${OUT_DIR}" || -z "${BASE_URL}" ]]; then
  echo "usage: $0 <out-dir> <base-url>" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

fetch() {
  local path="$1"
  local target="$2"
  local attempts=0
  while (( attempts < 5 )); do
    if curl -fsS "${BASE_URL}${path}" > "${target}.tmp"; then
      mv "${target}.tmp" "${target}"
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 1
  done
  rm -f "${target}.tmp"
  echo "failed to fetch ${BASE_URL}${path}" >&2
  return 1
}

fetch "/api/v1/runtime/health" "${OUT_DIR}/health.json"
fetch "/api/v1/runtime/readiness" "${OUT_DIR}/readiness.json"
fetch "/metrics" "${OUT_DIR}/metrics.prom"

echo "wrote ${OUT_DIR}/health.json"
echo "wrote ${OUT_DIR}/readiness.json"
echo "wrote ${OUT_DIR}/metrics.prom"
