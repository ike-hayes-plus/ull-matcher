#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAMP="${1:-$(date +%Y%m%d-%H%M%S)}"
CURRENT_DIR="${BENCHMARK_CURRENT_ROOT:-${ROOT_DIR}/target/current}"
ARCHIVE_ROOT="${BENCHMARK_ARCHIVE_ROOT:-${ROOT_DIR}/target/archive}"
DEST_DIR="${ARCHIVE_ROOT}/${STAMP}"

mkdir -p "${DEST_DIR}/binary-commit" "${DEST_DIR}/binary-bench-results" "${DEST_DIR}/binary-matrix" "${DEST_DIR}/http-crossing"

copy_report_if_exists() {
  local src="$1"
  local dst="$2"
  local require_java_schema="${3:-false}"
  if [[ ! -f "${src}" ]]; then
    return
  fi
  python3 - <<'PY' "${src}" "${dst}" "${require_java_schema}" "${STRICT_BENCHMARK_SCHEMA:-false}"
import json
import sys

path = sys.argv[1]
destination = sys.argv[2]
require_java_schema = sys.argv[3] == "true"
strict_schema = sys.argv[4] == "true"
try:
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    payload = json.loads(text)
except json.JSONDecodeError as exc:
    start = text.find("{") if "text" in locals() else -1
    if start < 0:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc
    try:
        payload, end = json.JSONDecoder().raw_decode(text[start:])
    except json.JSONDecodeError as nested:
        raise SystemExit(f"{path}: invalid JSON: {nested}") from nested
    if text[start + end:].strip():
        raise SystemExit(f"{path}: invalid JSON trailing content after benchmark object")
    print(f"[archive] warning: {path}: stripped non-JSON prefix before benchmark object", file=sys.stderr)

if payload.get("success") is False:
    raise SystemExit(f"{path}: benchmark success=false")

if require_java_schema and payload.get("benchmarkSchemaVersion") != 1:
    message = f"{path}: missing benchmarkSchemaVersion=1"
    if strict_schema:
        raise SystemExit(message)
    print(f"[archive] warning: {message}", file=sys.stderr)

if payload.get("latencyMeasured") is False:
    latency_fields = [
        "p50LatencyMicros",
        "p90LatencyMicros",
        "p95LatencyMicros",
        "p99LatencyMicros",
        "p999LatencyMicros",
        "worstLatencyMicros",
    ]
    non_null = [name for name in latency_fields if payload.get(name) is not None]
    if non_null:
        raise SystemExit(f"{path}: latencyMeasured=false but fields are not null: {non_null}")

with open(destination, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=False, indent=2)
    handle.write("\n")
PY
}

copy_report_if_exists "${CURRENT_DIR}/core-only-crossing-current.json" "${DEST_DIR}/core-only-crossing-current.json" true
copy_report_if_exists "${CURRENT_DIR}/embed-journaled-core/report-clean.json" "${DEST_DIR}/embed-journaled-core-current.json" true
copy_report_if_exists "${CURRENT_DIR}/single-node-http/report-clean.json" "${DEST_DIR}/single-node-http-current.json" true
copy_report_if_exists "${CURRENT_DIR}/single-node-binary/report-clean.json" "${DEST_DIR}/single-node-binary-current.json" true
copy_report_if_exists "${CURRENT_DIR}/direct-ha-binary/report-clean.json" "${DEST_DIR}/direct-ha-binary-current.json" true

copy_report_if_exists "${CURRENT_DIR}/binary-bench-results/binary-1p1s-grpc-crossing-current.json" "${DEST_DIR}/binary-bench-results/binary-1p1s-grpc-crossing-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-bench-results/binary-1p1s-aeron-crossing-current.json" "${DEST_DIR}/binary-bench-results/binary-1p1s-aeron-crossing-current.json" true
copy_report_if_exists "${CURRENT_DIR}/http-crossing/grpc-crossing-benchmark-report.json" "${DEST_DIR}/http-crossing/grpc-crossing-benchmark-report.json" true
copy_report_if_exists "${CURRENT_DIR}/http-crossing/aeron-crossing-benchmark-report.json" "${DEST_DIR}/http-crossing/aeron-crossing-benchmark-report.json" true

copy_report_if_exists "${CURRENT_DIR}/binary-commit/grpc-1p1s-current.json" "${DEST_DIR}/binary-commit/grpc-1p1s-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-commit/aeron-1p1s-current.json" "${DEST_DIR}/binary-commit/aeron-1p1s-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-commit/grpc-1p2s-quorum-current.json" "${DEST_DIR}/binary-commit/grpc-1p2s-quorum-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-commit/aeron-1p2s-quorum-current.json" "${DEST_DIR}/binary-commit/aeron-1p2s-quorum-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-commit/grpc-1p3s-quorum-current.json" "${DEST_DIR}/binary-commit/grpc-1p3s-quorum-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-commit/aeron-1p3s-quorum-current.json" "${DEST_DIR}/binary-commit/aeron-1p3s-quorum-current.json" true

copy_report_if_exists "${CURRENT_DIR}/binary-matrix/grpc-1p2s-quorum-current.json" "${DEST_DIR}/binary-matrix/grpc-1p2s-quorum-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-matrix/aeron-1p2s-quorum-current.json" "${DEST_DIR}/binary-matrix/aeron-1p2s-quorum-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-matrix/grpc-1p3s-quorum-current.json" "${DEST_DIR}/binary-matrix/grpc-1p3s-quorum-current.json" true
copy_report_if_exists "${CURRENT_DIR}/binary-matrix/aeron-1p3s-quorum-current.json" "${DEST_DIR}/binary-matrix/aeron-1p3s-quorum-current.json" true

cat > "${DEST_DIR}/README.md" <<EOF
# Archived Current Benchmark Baseline

Archive created at: ${STAMP}

This directory contains a copy of the repository's current benchmark fact sources at the time of archival.
Use these files for historical comparison only. The open-source README should continue to point to the active \`current\` files under \`target/current/\`.
EOF

echo "${DEST_DIR}"
