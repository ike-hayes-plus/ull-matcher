#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"
COMPOSE_FILE="$ROOT_DIR/deploy/docker-compose/chaos-lab/docker-compose.yml"
OUT_DIR="${1:-$ROOT_DIR/target/chaos-lab}"
shift || true

NODE_URLS=("$@")

mkdir -p "$OUT_DIR"

check_port() {
  local host="$1"
  local port="$2"
  python3 - <<'PY' "$host" "$port"
import socket
import sys

sock = socket.socket()
sock.settimeout(2.0)
try:
    sock.connect((sys.argv[1], int(sys.argv[2])))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
}

fetch_http() {
  local url="$1"
  python3 - <<'PY' "$url"
import sys
import urllib.request

with urllib.request.urlopen(sys.argv[1], timeout=5) as response:
    response.read()
PY
}

echo "[lab] checking compose services"
docker compose -f "$COMPOSE_FILE" ps

check_port 127.0.0.1 "$LAB_ZOOKEEPER_PORT" || { echo "[lab] zookeeper1 port check failed" >&2; exit 2; }
check_port 127.0.0.1 "$LAB_ZOOKEEPER_PORT_NODE_2" || { echo "[lab] zookeeper2 port check failed" >&2; exit 2; }
check_port 127.0.0.1 "$LAB_ZOOKEEPER_PORT_NODE_3" || { echo "[lab] zookeeper3 port check failed" >&2; exit 2; }
check_port 127.0.0.1 "$LAB_ETCD_PORT" || { echo "[lab] etcd1 port check failed" >&2; exit 6; }
check_port 127.0.0.1 "$LAB_ETCD_PORT_NODE_2" || { echo "[lab] etcd2 port check failed" >&2; exit 6; }
check_port 127.0.0.1 "$LAB_ETCD_PORT_NODE_3" || { echo "[lab] etcd3 port check failed" >&2; exit 6; }
check_port 127.0.0.1 "$LAB_TOXIPROXY_PORT" || { echo "[lab] toxiproxy port check failed" >&2; exit 4; }
check_port 127.0.0.1 "$LAB_PROMETHEUS_PORT" || { echo "[lab] prometheus port check failed" >&2; exit 5; }

fetch_http "http://127.0.0.1:${LAB_PROMETHEUS_PORT}/-/ready" >/dev/null
fetch_http "http://127.0.0.1:${LAB_TOXIPROXY_PORT}/version" >/dev/null

echo "[lab] infrastructure is reachable"

if [[ "${#NODE_URLS[@]}" -eq 0 ]]; then
  cat <<EOF
[lab] no matcher node urls provided
[lab] validation finished for infrastructure only
EOF
  exit 0
fi

NODE_DIRS=()
for url in "${NODE_URLS[@]}"; do
  safe_name="$(echo "$url" | sed -E 's#^https?://##; s#[/:]#_#g')"
  node_dir="$OUT_DIR/$safe_name"
  "$ROOT_DIR/scripts/chaos/collect-node-state.sh" "$node_dir" "$url"
  NODE_DIRS+=("$node_dir")
done

"$ROOT_DIR/scripts/chaos/validate-cluster-state.py" --report "$OUT_DIR/validation-report.json" "${NODE_DIRS[@]}"
"$ROOT_DIR/scripts/chaos/validate-transport-rollout.py" \
  --report "$OUT_DIR/transport-rollout-report.json" \
  --data-root "${DATA_ROOT:-$ROOT_DIR/target/lab}" \
  "${NODE_DIRS[@]}"
"$ROOT_DIR/scripts/chaos/summarize-chaos-report.py" --report "$OUT_DIR/chaos-summary.json" \
  "$OUT_DIR/validation-report.json" "$OUT_DIR/transport-rollout-report.json"
echo "[lab] matcher node validation passed"
