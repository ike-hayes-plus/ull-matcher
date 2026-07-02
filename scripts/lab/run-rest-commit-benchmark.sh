#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
STANDBY_BASE_URL="${STANDBY_BASE_URL:-}"
REPORT="${REPORT:-$ROOT_DIR/target/current/http-ha/rest-commit-current.json}"
RESTING_ORDERS="${RESTING_ORDERS:-2048}"
CONCURRENCY="${CONCURRENCY:-24}"
ACK_MODE="${ACK_MODE:-committed}"
PRELOAD_ACK_MODE="${PRELOAD_ACK_MODE:-local}"
HTTP_SUBMIT_MODE="${HTTP_SUBMIT_MODE:-single}"
BATCH_SIZE="${BATCH_SIZE:-32}"
COMMIT_TIMEOUT_SECONDS="${COMMIT_TIMEOUT_SECONDS:-90}"
POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-100}"
WAIT_FOR_READY_SECONDS="${WAIT_FOR_READY_SECONDS:-30}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/lab/run-rest-commit-benchmark.sh [options]

Options:
  --base-url URL              Primary HTTP base URL.
  --standby-base-url URL      Optional standby HTTP base URL for health sampling.
  --report FILE               JSON report path.
  --resting-orders N          Number of maker/taker orders. Default: 2048
  --concurrency N             Client worker count. Default: 24
  --ack-mode MODE             local or committed. Default: committed
  --preload-ack-mode MODE     local or committed. Default: local
  --http-submit-mode MODE     single or batch. Default: single
  --batch-size N              Batch size when --http-submit-mode=batch. Default: 32
  --commit-timeout-seconds N  Commit catch-up timeout. Default: 90
  --poll-interval-ms N        Health polling interval. Default: 100
  --wait-for-ready-seconds N  Wait for primary writable and standby health. Default: 30

The script targets an already running matcher cluster and does not start or stop nodes.
USAGE
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer: ${value}" >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url) BASE_URL="$2"; shift 2 ;;
    --standby-base-url) STANDBY_BASE_URL="$2"; shift 2 ;;
    --report) REPORT="$2"; shift 2 ;;
    --resting-orders) RESTING_ORDERS="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --ack-mode) ACK_MODE="$2"; shift 2 ;;
    --preload-ack-mode) PRELOAD_ACK_MODE="$2"; shift 2 ;;
    --http-submit-mode) HTTP_SUBMIT_MODE="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --commit-timeout-seconds) COMMIT_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --poll-interval-ms) POLL_INTERVAL_MS="$2"; shift 2 ;;
    --wait-for-ready-seconds) WAIT_FOR_READY_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

require_positive_integer "--resting-orders" "$RESTING_ORDERS"
require_positive_integer "--concurrency" "$CONCURRENCY"
require_positive_integer "--batch-size" "$BATCH_SIZE"
require_positive_integer "--poll-interval-ms" "$POLL_INTERVAL_MS"
require_positive_integer "--wait-for-ready-seconds" "$WAIT_FOR_READY_SECONDS"

case "$ACK_MODE" in
  local|committed) ;;
  *) echo "--ack-mode must be local or committed: ${ACK_MODE}" >&2; exit 2 ;;
esac
case "$PRELOAD_ACK_MODE" in
  local|committed) ;;
  *) echo "--preload-ack-mode must be local or committed: ${PRELOAD_ACK_MODE}" >&2; exit 2 ;;
esac
case "$HTTP_SUBMIT_MODE" in
  single|batch) ;;
  *) echo "--http-submit-mode must be single or batch: ${HTTP_SUBMIT_MODE}" >&2; exit 2 ;;
esac

python3 - <<'PY' "$BASE_URL" "$STANDBY_BASE_URL" "$WAIT_FOR_READY_SECONDS"
import json
import sys
import time
import urllib.request

base_url = sys.argv[1].rstrip("/")
standby_url = sys.argv[2].rstrip("/")
timeout_seconds = int(sys.argv[3])


def fetch_health(url: str) -> dict:
    with urllib.request.urlopen(f"{url}/api/v1/runtime/health", timeout=2) as response:
        return json.loads(response.read().decode("utf-8"))


deadline = time.time() + timeout_seconds
last_error = ""
while time.time() < deadline:
    try:
        primary = fetch_health(base_url)
        if not primary.get("acceptingClientCommands"):
            last_error = f"primary is not writable: role={primary.get('role')}"
            time.sleep(0.5)
            continue
        if standby_url:
            standby = fetch_health(standby_url)
            if standby.get("nodeId") == primary.get("nodeId"):
                last_error = "standby URL points to the same node as primary"
                time.sleep(0.5)
                continue
        print(f"[rest-bench] primary ready node={primary.get('nodeId')} role={primary.get('role')} url={base_url}")
        if standby_url:
            print(f"[rest-bench] standby health reachable url={standby_url}")
        raise SystemExit(0)
    except Exception as exc:  # noqa: BLE001 - CLI should keep the last readiness failure.
        last_error = str(exc)
        time.sleep(0.5)

raise SystemExit(f"cluster did not become ready within {timeout_seconds}s: {last_error}")
PY

args=(
  "${ROOT_DIR}/scripts/bench/replication-commit-benchmark.py"
  --base-url "$BASE_URL"
  --report "$REPORT"
  --resting-orders "$RESTING_ORDERS"
  --concurrency "$CONCURRENCY"
  --ack-mode "$ACK_MODE"
  --preload-ack-mode "$PRELOAD_ACK_MODE"
  --http-submit-mode "$HTTP_SUBMIT_MODE"
  --batch-size "$BATCH_SIZE"
  --commit-timeout-seconds "$COMMIT_TIMEOUT_SECONDS"
  --poll-interval-ms "$POLL_INTERVAL_MS"
)

if [[ -n "$STANDBY_BASE_URL" ]]; then
  args+=(--standby-base-url "$STANDBY_BASE_URL")
fi

PYTHONPATH="${ROOT_DIR}/scripts/bench${PYTHONPATH:+:$PYTHONPATH}" python3 "${args[@]}"
