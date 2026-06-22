#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/transport-compare}"
LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/lab-logs}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"
NODE_URLS=(
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_A}"
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_B}"
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_C}"
)
TRANSPORTS_TEXT="${TRANSPORTS_TEXT:-GRPC AERON}"
read -r -a TRANSPORTS <<<"$TRANSPORTS_TEXT"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

discover_primary() {
  python3 - <<'PY' "$1" "${NODE_URLS[@]}"
import json, sys
from pathlib import Path

base = Path(sys.argv[1])
urls = sys.argv[2:]
for url in urls:
    safe = url.replace("http://", "http___").replace("https://", "https___").replace("/", "_").replace(":", "_")
    readiness = json.loads((base / safe / "readiness.json").read_text())
    if readiness.get("clientTrafficReady") is True:
        print(url)
        raise SystemExit(0)
raise SystemExit(1)
PY
}

wait_for_cluster_validation() {
  local out_dir="$1"
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster validate "$out_dir" "${NODE_URLS[@]}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster validate "$out_dir" "${NODE_URLS[@]}"
}

for transport in "${TRANSPORTS[@]}"; do
  mode_dir="$OUT_DIR/${transport,,}"
  mkdir -p "$mode_dir"
  "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster down >/dev/null 2>&1 || true
  REPLICATION_TRANSPORT="$transport" ENABLE_TRANSPORT_TLS="${ENABLE_TRANSPORT_TLS:-false}" \
    ALLOW_TRANSPORT_CHANGE=true TRANSPORT_CHANGE_WINDOW_ID="transport-compare-${transport,,}" \
    "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster up
  wait_for_cluster_validation "$mode_dir/baseline"
  primary_url="$(discover_primary "$mode_dir/baseline")"
  python3 "$ROOT_DIR/scripts/bench/transport-benchmark.py" \
    --base-url "$primary_url" \
    --report "$mode_dir/benchmark-report.json"
  python3 "$ROOT_DIR/scripts/bench/crossing-benchmark.py" \
    --base-url "$primary_url" \
    --report "$mode_dir/crossing-benchmark-report.json"
  OUT_DIR="$mode_dir/failover-smoke" LOG_ROOT="$LOG_ROOT" \
    "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster failover-smoke
  "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster down >/dev/null 2>&1 || true
done

python3 "$ROOT_DIR/scripts/bench/summarize-transport-compare.py" \
  --report "$OUT_DIR/transport-comparison-summary.json" \
  "$OUT_DIR/grpc/benchmark-report.json" \
  "$OUT_DIR/aeron/benchmark-report.json"
