#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ull-ha-floor-test.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

FLOOR_FILE="${ROOT_DIR}/doc/operations/benchmark-ha-ci-floor.json"
REPORT="${TMP_DIR}/grpc-1p2s-binary-frame1.json"

cat > "${REPORT}" <<'JSON'
{
  "success": true,
  "acceptedCommandsPerSecond": 200000.0,
  "replicationCommittedSubmissionsPerSecond": 190000.0,
  "commitCatchupSeconds": 0.01,
  "latency": { "p99Ms": 0.5 }
}
JSON

REPORT_DIR="${TMP_DIR}" FLOOR_FILE="${FLOOR_FILE}" "${ROOT_DIR}/scripts/ops/check-ha-benchmark-floor.sh"

python3 - <<'PY' "${REPORT}"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
payload["acceptedCommandsPerSecond"] = 1000.0
path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
PY

if REPORT_DIR="${TMP_DIR}" FLOOR_FILE="${FLOOR_FILE}" "${ROOT_DIR}/scripts/ops/check-ha-benchmark-floor.sh" >/dev/null 2>&1; then
  echo "expected failing report to be rejected" >&2
  exit 1
fi

echo "HA benchmark floor self-test passed"
