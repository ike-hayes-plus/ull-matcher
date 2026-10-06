#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${REPORT_DIR:-${ROOT_DIR}/target/benchmark/3.0-full}"
FLOOR_FILE="${FLOOR_FILE:-${ROOT_DIR}/doc/operations/benchmark-ha-ci-floor.json}"

exec python3 "${ROOT_DIR}/scripts/ops/check-ha-benchmark-floor.py" \
  --report-dir "${REPORT_DIR}" \
  --floor-file "${FLOOR_FILE}" \
  "$@"
