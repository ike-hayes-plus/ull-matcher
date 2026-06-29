#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"

TRANSPORT="GRPC"
STANDBYS=1
MODE="crossing"
REPORT=""
STANDBY_COMMIT_MODE="${STANDBY_COMMIT_MODE:-any}"
RESTING_ORDERS="${RESTING_ORDERS:-2048}"
CONCURRENCY="${CONCURRENCY:-24}"
BATCH_SIZE="${BATCH_SIZE:-64}"
CROSSING_ORDERS="${CROSSING_ORDERS:-2048}"
MIXED_ROUNDS="${MIXED_ROUNDS:-64}"
MIXED_MAKER_BATCH_SIZE="${MIXED_MAKER_BATCH_SIZE:-16}"
MIXED_TAKER_BATCH_SIZE="${MIXED_TAKER_BATCH_SIZE:-16}"
DATA_ROOT="${DATA_ROOT:-$ROOT_DIR/target/binary-bench-lab}"
LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/binary-bench-logs}"
CLUSTER_NAME="${CLUSTER_NAME:-binary-bench-$(date +%Y%m%d%H%M%S)-$$}"
SHARD_KEY="${SHARD_KEY:-merchant:42}"
SYMBOL_ID="${SYMBOL_ID:-1}"
ZK_CONNECT="${ZK_CONNECT:-127.0.0.1:2181}"
CLUSTER_STABLE_ROUNDS="${CLUSTER_STABLE_ROUNDS:-8}"
CLUSTER_STABLE_TIMEOUT_SECONDS="${CLUSTER_STABLE_TIMEOUT_SECONDS:-30}"
BENCHMARK_RETRIES="${BENCHMARK_RETRIES:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --transport) TRANSPORT="$2"; shift 2 ;;
    --standbys) STANDBYS="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --report) REPORT="$2"; shift 2 ;;
    --standby-commit-mode) STANDBY_COMMIT_MODE="$2"; shift 2 ;;
    --resting-orders) RESTING_ORDERS="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --crossing-orders) CROSSING_ORDERS="$2"; shift 2 ;;
    --mixed-rounds) MIXED_ROUNDS="$2"; shift 2 ;;
    --mixed-maker-batch-size) MIXED_MAKER_BATCH_SIZE="$2"; shift 2 ;;
    --mixed-taker-batch-size) MIXED_TAKER_BATCH_SIZE="$2"; shift 2 ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer: ${value}" >&2
    exit 2
  fi
}

case "$TRANSPORT" in
  GRPC|AERON) ;;
  *)
    echo "--transport must be GRPC or AERON: ${TRANSPORT}" >&2
    exit 2
    ;;
esac

case "$MODE" in
  crossing|mixed|replication-commit) ;;
  *)
    echo "--mode must be crossing, mixed, or replication-commit: ${MODE}" >&2
    exit 2
    ;;
esac

if [[ -z "$REPORT" ]]; then
  echo "--report is required" >&2
  exit 2
fi
require_positive_integer "--standbys" "$STANDBYS"
require_positive_integer "RESTING_ORDERS" "$RESTING_ORDERS"
require_positive_integer "CONCURRENCY" "$CONCURRENCY"
require_positive_integer "BATCH_SIZE" "$BATCH_SIZE"
require_positive_integer "CROSSING_ORDERS" "$CROSSING_ORDERS"
require_positive_integer "MIXED_ROUNDS" "$MIXED_ROUNDS"
require_positive_integer "MIXED_MAKER_BATCH_SIZE" "$MIXED_MAKER_BATCH_SIZE"
require_positive_integer "MIXED_TAKER_BATCH_SIZE" "$MIXED_TAKER_BATCH_SIZE"
require_positive_integer "CLUSTER_STABLE_ROUNDS" "$CLUSTER_STABLE_ROUNDS"
require_positive_integer "CLUSTER_STABLE_TIMEOUT_SECONDS" "$CLUSTER_STABLE_TIMEOUT_SECONDS"
require_positive_integer "BENCHMARK_RETRIES" "$BENCHMARK_RETRIES"
if [[ "$STANDBYS" -lt 1 || "$STANDBYS" -gt 3 ]]; then
  echo "--standbys must be 1, 2, or 3" >&2
  exit 2
fi
if [[ "$MODE" != "replication-commit" && "$STANDBY_COMMIT_MODE" != "any" ]]; then
  echo "[bench] --standby-commit-mode=${STANDBY_COMMIT_MODE} only applies to replication-commit; using any for ${MODE}" >&2
  STANDBY_COMMIT_MODE="any"
fi

NODE_IDS=(node-a node-b node-c node-d)
HTTP_PORTS=("$LAB_HTTP_PORT_NODE_A" "$LAB_HTTP_PORT_NODE_B" "$LAB_HTTP_PORT_NODE_C" "$LAB_HTTP_PORT_NODE_D")
GRPC_PORTS=("$LAB_GRPC_PORT_NODE_A" "$LAB_GRPC_PORT_NODE_B" "$LAB_GRPC_PORT_NODE_C" "$LAB_GRPC_PORT_NODE_D")
AERON_PORTS=("$LAB_AERON_PORT_NODE_A" "$LAB_AERON_PORT_NODE_B" "$LAB_AERON_PORT_NODE_C" "$LAB_AERON_PORT_NODE_D")
BINARY_PORTS=("$LAB_BINARY_PORT_NODE_A" "$LAB_BINARY_PORT_NODE_B" "$LAB_BINARY_PORT_NODE_C" "$LAB_BINARY_PORT_NODE_D")

cleanup() {
  export LOG_ROOT
  for node_id in "${NODE_IDS[@]}"; do
    "${ROOT_DIR}/scripts/lab/stop-node.sh" "$node_id" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

if ! lsof -iTCP:"${LAB_ZOOKEEPER_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  "${ROOT_DIR}/scripts/run-chaos-tests.sh" lab up >/dev/null
fi

python3 - <<'PY' "${LAB_ZOOKEEPER_PORT}"
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.time() + 30.0
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.0):
            raise SystemExit(0)
    except OSError:
        time.sleep(0.2)
raise SystemExit("zookeeper did not become reachable in time")
PY

cleanup
rm -rf "$DATA_ROOT" "$LOG_ROOT"
mkdir -p "$DATA_ROOT" "$LOG_ROOT"

export SHARD_KEY SYMBOL_ID CLUSTER_NAME ZK_CONNECT DATA_ROOT LOG_ROOT REPLICATION_TRANSPORT="$TRANSPORT"

NODE_COUNT=$((STANDBYS + 1))
for ((i = 0; i < NODE_COUNT; i++)); do
  "${ROOT_DIR}/scripts/lab/start-node.sh" \
    "${NODE_IDS[$i]}" \
    "${HTTP_PORTS[$i]}" \
    "${GRPC_PORTS[$i]}" \
    "${AERON_PORTS[$i]}" \
    "${BINARY_PORTS[$i]}"
done

PRIMARY_JSON="$(python3 - <<'PY' "$NODE_COUNT" "${HTTP_PORTS[@]}" "${BINARY_PORTS[@]}"
import json
import sys
import time
import urllib.request

node_count = int(sys.argv[1])
http_ports = [int(x) for x in sys.argv[2:6]]
binary_ports = [int(x) for x in sys.argv[6:10]]
deadline = time.time() + 60.0
while time.time() < deadline:
    all_reachable = True
    for index in range(node_count):
        url = f"http://127.0.0.1:{http_ports[index]}/api/v1/runtime/health"
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            all_reachable = False
            break
    if not all_reachable:
        time.sleep(0.2)
        continue
    for index in range(node_count):
        url = f"http://127.0.0.1:{http_ports[index]}/api/v1/runtime/health"
        with urllib.request.urlopen(url, timeout=2) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if payload.get("acceptingClientCommands"):
            print(json.dumps({
                "baseUrl": f"http://127.0.0.1:{http_ports[index]}",
                "binaryPort": binary_ports[index],
                "nodeId": f"node-{chr(ord('a') + index)}"
            }))
            raise SystemExit(0)
    time.sleep(0.2)
raise SystemExit(1)
PY
)"

PRIMARY_URL="$(python3 - <<'PY' "$PRIMARY_JSON"
import json, sys
print(json.loads(sys.argv[1])["baseUrl"])
PY
)"
BINARY_PORT="$(python3 - <<'PY' "$PRIMARY_JSON"
import json, sys
print(json.loads(sys.argv[1])["binaryPort"])
PY
)"
PRIMARY_NODE_ID="$(python3 - <<'PY' "$PRIMARY_JSON"
import json, sys
print(json.loads(sys.argv[1])["nodeId"])
PY
)"

python3 - <<'PY' "$NODE_COUNT" "$CLUSTER_STABLE_ROUNDS" "$CLUSTER_STABLE_TIMEOUT_SECONDS" "${HTTP_PORTS[@]}"
import json
import sys
import time
import urllib.request

node_count = int(sys.argv[1])
required_stable_rounds = int(sys.argv[2])
timeout_seconds = float(sys.argv[3])
http_ports = [int(x) for x in sys.argv[4:8]]
stable_rounds = 0
deadline = time.time() + timeout_seconds
while time.time() < deadline:
    reachable = True
    for index in range(node_count):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{http_ports[index]}/api/v1/runtime/health", timeout=2) as response:
                json.loads(response.read().decode("utf-8"))
        except Exception:
            reachable = False
            stable_rounds = 0
            break
    if reachable:
        stable_rounds += 1
        if stable_rounds >= required_stable_rounds:
            raise SystemExit(0)
    time.sleep(0.2)
raise SystemExit("cluster did not stay stable long enough before benchmark")
PY

PRIMARY_JSON="$(python3 - <<'PY' "$NODE_COUNT" "$CLUSTER_STABLE_ROUNDS" "$CLUSTER_STABLE_TIMEOUT_SECONDS" "${HTTP_PORTS[@]}" "${BINARY_PORTS[@]}"
import json
import sys
import time
import urllib.request

node_count = int(sys.argv[1])
required_stable_rounds = int(sys.argv[2])
timeout_seconds = float(sys.argv[3])
http_ports = [int(x) for x in sys.argv[4:8]]
binary_ports = [int(x) for x in sys.argv[8:12]]
stable_primary = None
stable_rounds = 0
deadline = time.time() + timeout_seconds
while time.time() < deadline:
    payloads = []
    try:
        for index in range(node_count):
            with urllib.request.urlopen(f"http://127.0.0.1:{http_ports[index]}/api/v1/runtime/health", timeout=2) as response:
                payloads.append(json.loads(response.read().decode("utf-8")))
    except Exception:
        stable_primary = None
        stable_rounds = 0
        time.sleep(0.2)
        continue
    accepting = [
        index for index, payload in enumerate(payloads)
        if payload.get("acceptingClientCommands") and payload.get("role") == "PRIMARY"
    ]
    if len(accepting) == 1:
        primary = accepting[0]
        if stable_primary == primary:
            stable_rounds += 1
        else:
            stable_primary = primary
            stable_rounds = 1
        if stable_rounds >= required_stable_rounds:
            print(json.dumps({
                "baseUrl": f"http://127.0.0.1:{http_ports[primary]}",
                "binaryPort": binary_ports[primary],
                "nodeId": f"node-{chr(ord('a') + primary)}"
            }))
            raise SystemExit(0)
    else:
        stable_primary = None
        stable_rounds = 0
    time.sleep(0.2)
raise SystemExit("cluster did not keep a single stable primary before benchmark")
PY
)"
PRIMARY_URL="$(python3 - <<'PY' "$PRIMARY_JSON"
import json, sys
print(json.loads(sys.argv[1])["baseUrl"])
PY
)"
BINARY_PORT="$(python3 - <<'PY' "$PRIMARY_JSON"
import json, sys
print(json.loads(sys.argv[1])["binaryPort"])
PY
)"
PRIMARY_NODE_ID="$(python3 - <<'PY' "$PRIMARY_JSON"
import json, sys
print(json.loads(sys.argv[1])["nodeId"])
PY
)"

CMD=(
  python3 "$ROOT_DIR/scripts/bench/binary-ingress-benchmark.py"
  --mode "$MODE"
  --base-url "$PRIMARY_URL"
  --binary-host 127.0.0.1
  --binary-port "$BINARY_PORT"
  --report "$REPORT"
  --standby-commit-mode "$STANDBY_COMMIT_MODE"
  --resting-orders "$RESTING_ORDERS"
  --concurrency "$CONCURRENCY"
  --batch-size "$BATCH_SIZE"
  --crossing-orders "$CROSSING_ORDERS"
  --mixed-rounds "$MIXED_ROUNDS"
  --mixed-maker-batch-size "$MIXED_MAKER_BATCH_SIZE"
  --mixed-taker-batch-size "$MIXED_TAKER_BATCH_SIZE"
)
if [[ "$MODE" == "replication-commit" ]]; then
  CMD=(
    python3 "$ROOT_DIR/scripts/bench/binary-replication-commit-benchmark.py"
    --base-url "$PRIMARY_URL"
    --binary-host 127.0.0.1
    --binary-port "$BINARY_PORT"
    --report "$REPORT"
    --standby-commit-mode "$STANDBY_COMMIT_MODE"
    --resting-orders "$RESTING_ORDERS"
    --concurrency "$CONCURRENCY"
    --batch-size "$BATCH_SIZE"
    --crossing-orders "$CROSSING_ORDERS"
  )
fi
for ((i = 0; i < NODE_COUNT; i++)); do
  if [[ "${NODE_IDS[$i]}" != "$PRIMARY_NODE_ID" ]]; then
    CMD+=(--standby-base-url "http://127.0.0.1:${HTTP_PORTS[$i]}")
  fi
done
attempt=1
while true; do
  if "${CMD[@]}"; then
    break
  else
    status=$?
  fi
  if [[ "$attempt" -ge "$BENCHMARK_RETRIES" ]]; then
    exit "$status"
  fi
  attempt=$((attempt + 1))
  echo "[bench] attempt failed with status ${status}; retrying ${attempt}/${BENCHMARK_RETRIES}" >&2
  sleep 1
done
