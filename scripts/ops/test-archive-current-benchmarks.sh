#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ull-archive-test.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

CURRENT_DIR="${TMP_DIR}/current"
ARCHIVE_ROOT="${TMP_DIR}/archive"
mkdir -p "${CURRENT_DIR}/single-node-http"

cat > "${CURRENT_DIR}/core-only-crossing-current.json" <<'JSON'
{
  "success": true,
  "scenario": "core_only_crossing_benchmark",
  "benchmarkSchemaVersion": 1,
  "latencyMeasured": false,
  "p50LatencyMicros": null,
  "p90LatencyMicros": null,
  "p95LatencyMicros": null,
  "p99LatencyMicros": null,
  "p999LatencyMicros": null,
  "worstLatencyMicros": null
}
JSON

cat > "${CURRENT_DIR}/single-node-http/report-clean.json" <<'JSON'
2026-06-27 00:00:00.000 INFO noisy prefix
{
  "success": true,
  "scenario": "single_node_server_crossing_benchmark",
  "benchmarkSchemaVersion": 1
}
JSON

BENCHMARK_CURRENT_ROOT="${CURRENT_DIR}" \
BENCHMARK_ARCHIVE_ROOT="${ARCHIVE_ROOT}" \
STRICT_BENCHMARK_SCHEMA=true \
  "${ROOT_DIR}/scripts/ops/archive-current-benchmarks.sh" ok >/dev/null

python3 - <<'PY' "${ARCHIVE_ROOT}/ok/single-node-http-current.json"
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
assert payload["scenario"] == "single_node_server_crossing_benchmark"
assert payload["benchmarkSchemaVersion"] == 1
PY

python3 - <<'PY' "${CURRENT_DIR}/core-only-crossing-current.json"
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
del payload["benchmarkSchemaVersion"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle)
    handle.write("\n")
PY

if BENCHMARK_CURRENT_ROOT="${CURRENT_DIR}" \
  BENCHMARK_ARCHIVE_ROOT="${ARCHIVE_ROOT}" \
  STRICT_BENCHMARK_SCHEMA=true \
    "${ROOT_DIR}/scripts/ops/archive-current-benchmarks.sh" missing-schema >/dev/null 2>&1; then
  echo "strict schema archive unexpectedly passed" >&2
  exit 1
fi

python3 - <<'PY' "${CURRENT_DIR}/core-only-crossing-current.json"
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
payload["benchmarkSchemaVersion"] = 1
payload["success"] = False
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle)
    handle.write("\n")
PY

if BENCHMARK_CURRENT_ROOT="${CURRENT_DIR}" \
  BENCHMARK_ARCHIVE_ROOT="${ARCHIVE_ROOT}" \
    "${ROOT_DIR}/scripts/ops/archive-current-benchmarks.sh" failed-report >/dev/null 2>&1; then
  echo "failed benchmark archive unexpectedly passed" >&2
  exit 1
fi

echo "archive benchmark self-test passed"
