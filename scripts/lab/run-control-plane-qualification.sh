#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"

CONTROL_PLANE="${CONTROL_PLANE:-etcd}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/control-plane-qualification/$CONTROL_PLANE}"
SOAK_SECONDS="${SOAK_SECONDS:-60}"
SOAK_INTERVAL_SECONDS="${SOAK_INTERVAL_SECONDS:-5}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-45}"
CLUSTER_NAME="${CLUSTER_NAME:-qualification-${CONTROL_PLANE}-$(date +%Y%m%d%H%M%S)-$$}"
DATA_ROOT="${DATA_ROOT:-$ROOT_DIR/target/control-plane-qualification-data/$CONTROL_PLANE}"
LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/control-plane-qualification-logs/$CONTROL_PLANE}"
ZK_CONNECT="${ZK_CONNECT:-127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183}"
ETCD_ENDPOINT="${ETCD_ENDPOINT:-http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383}"
REPLICATION_TRANSPORT="${REPLICATION_TRANSPORT:-GRPC}"
REPLICATION_MODE="${REPLICATION_MODE:-WAIT_FOR_QUORUM_STANDBYS}"
FAILOVER_MIN_STANDBY_REPLICAS="${FAILOVER_MIN_STANDBY_REPLICAS:-1}"
CLEANUP_CLUSTER="${CLEANUP_CLUSTER:-true}"

NODE_IDS=(node-a node-b node-c)
NODE_URLS=(
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_A}"
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_B}"
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_C}"
)

case "$CONTROL_PLANE" in
  zk|etcd) ;;
  *) echo "CONTROL_PLANE must be zk or etcd: $CONTROL_PLANE" >&2; exit 2 ;;
esac

mkdir -p "$OUT_DIR"
REPORT_FILE="$OUT_DIR/qualification-report.json"
PHASE_DIR="$OUT_DIR/phases"
rm -f "$REPORT_FILE"
rm -rf "$PHASE_DIR"
mkdir -p "$PHASE_DIR"

cleanup() {
  if [[ "$CONTROL_PLANE" == "etcd" ]]; then
    docker compose -f "$ROOT_DIR/deploy/docker-compose/chaos-lab/docker-compose.yml" start etcd1 >/dev/null 2>&1 || true
  fi
  if [[ "$CLEANUP_CLUSTER" == "true" ]]; then
    LOG_ROOT="$LOG_ROOT" "$ROOT_DIR/scripts/lab/stop-three-node-cluster.sh" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

record_phase() {
  python3 - <<'PY' "$REPORT_FILE" "$1" "$2" "$3" "$4"
import json
import sys
import time
from pathlib import Path

path = Path(sys.argv[1])
phase = {
    "name": sys.argv[2],
    "success": sys.argv[3].lower() == "true",
    "conclusion": sys.argv[4],
    "details": json.loads(sys.argv[5]) if sys.argv[5] else {},
    "timestamp": int(time.time()),
}
if path.exists():
    report = json.loads(path.read_text())
else:
    report = {"scenario": "control_plane_qualification", "phases": []}
report["phases"].append(phase)
report["success"] = all(item.get("success") for item in report["phases"])
path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
PY
}

collect_cluster_state() {
  local out_root="$1"
  shift || true
  local urls=("$@")
  if [[ "${#urls[@]}" -eq 0 ]]; then
    urls=("${NODE_URLS[@]}")
  fi
  rm -rf "$out_root"
  mkdir -p "$out_root"
  local node_dirs=()
  for url in "${urls[@]}"; do
    local safe_name
    safe_name="$(echo "$url" | sed -E 's#^https?://##; s#[/:]#_#g')"
    local node_dir="$out_root/$safe_name"
    "$ROOT_DIR/scripts/chaos/collect-node-state.sh" "$node_dir" "$url" >/dev/null
    node_dirs+=("$node_dir")
  done
  "$ROOT_DIR/scripts/chaos/validate-cluster-state.py" --report "$out_root/validation-report.json" "${node_dirs[@]}" >/dev/null
}

wait_for_cluster_state() {
  local phase="$1"
  shift || true
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if collect_cluster_state "$PHASE_DIR/$phase" "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  collect_cluster_state "$PHASE_DIR/$phase" "$@"
}

current_primary_json() {
  python3 - <<'PY' "${NODE_URLS[@]}"
import json
import sys
import urllib.request

for index, url in enumerate(sys.argv[1:]):
    try:
        with urllib.request.urlopen(f"{url}/api/v1/runtime/readiness", timeout=3) as response:
            readiness = json.loads(response.read().decode("utf-8"))
        with urllib.request.urlopen(f"{url}/api/v1/runtime/health", timeout=3) as response:
            health = json.loads(response.read().decode("utf-8"))
    except Exception:
        continue
    if readiness.get("clientTrafficReady") is True and health.get("role") == "PRIMARY":
        print(json.dumps({"nodeId": health.get("nodeId"), "url": url, "index": index}))
        raise SystemExit(0)
raise SystemExit(1)
PY
}

submit_probe_order() {
  python3 - <<'PY' "$1" "$2"
import json
import sys
import urllib.request

base_url = sys.argv[1]
order_id = int(sys.argv[2])
payload = json.dumps({
    "userId": 42,
    "orderId": order_id,
    "side": "BUY" if order_id % 2 == 0 else "SELL",
    "orderType": "LIMIT",
    "timeInForce": "IOC",
    "price": 101 if order_id % 2 == 0 else 100,
    "quantity": 1,
}).encode("utf-8")
request = urllib.request.Request(
    f"{base_url}/api/v1/orders",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=10) as response:
    print(json.loads(response.read().decode("utf-8")).get("sequence", 0))
PY
}

node_ports_for_start() {
  case "$1" in
    node-a) echo "$LAB_HTTP_PORT_NODE_A $LAB_GRPC_PORT_NODE_A $LAB_AERON_PORT_NODE_A" ;;
    node-b) echo "$LAB_HTTP_PORT_NODE_B $LAB_GRPC_PORT_NODE_B $LAB_AERON_PORT_NODE_B" ;;
    node-c) echo "$LAB_HTTP_PORT_NODE_C $LAB_GRPC_PORT_NODE_C $LAB_AERON_PORT_NODE_C" ;;
    *) return 1 ;;
  esac
}

echo "[qualification] starting infra and ${CONTROL_PLANE} cluster"
"$ROOT_DIR/scripts/run-chaos-tests.sh" lab up >/dev/null
"$ROOT_DIR/scripts/run-chaos-tests.sh" lab validate "$OUT_DIR/infra" >/dev/null

if [[ "$CONTROL_PLANE" == "etcd" ]]; then
  export LEASE_PROVIDER=etcd DISCOVERY_PROVIDER=etcd
else
  export LEASE_PROVIDER=zk DISCOVERY_PROVIDER=zk
fi
export CLUSTER_NAME DATA_ROOT LOG_ROOT ZK_CONNECT ETCD_ENDPOINT REPLICATION_TRANSPORT REPLICATION_MODE FAILOVER_MIN_STANDBY_REPLICAS

RESET_CLUSTER_STATE=true "$ROOT_DIR/scripts/lab/start-three-node-cluster.sh" >/dev/null
wait_for_cluster_state baseline
primary="$(current_primary_json)"
probe_sequence="$(submit_probe_order "$(python3 - <<'PY' "$primary"
import json, sys
print(json.loads(sys.argv[1])["url"])
PY
)" "$(( $(date +%s) * 1000000 ))")"
record_phase baseline true "cluster formed with a single writable primary" "{\"primary\":$primary,\"probeSequence\":$probe_sequence}"

echo "[qualification] soak ${SOAK_SECONDS}s"
soak_deadline=$(( $(date +%s) + SOAK_SECONDS ))
probe_count=0
while [[ $(date +%s) -lt $soak_deadline ]]; do
  wait_for_cluster_state "soak-$probe_count"
  primary="$(current_primary_json)"
  submit_probe_order "$(python3 - <<'PY' "$primary"
import json, sys
print(json.loads(sys.argv[1])["url"])
PY
)" "$(( $(date +%s) * 1000000 + probe_count ))" >/dev/null
  probe_count=$((probe_count + 1))
  sleep "$SOAK_INTERVAL_SECONDS"
done
record_phase soak true "cluster stayed single-primary and writable during soak" "{\"probeCount\":$probe_count,\"seconds\":$SOAK_SECONDS}"

primary="$(current_primary_json)"
primary_node="$(python3 - <<'PY' "$primary"
import json, sys
print(json.loads(sys.argv[1])["nodeId"])
PY
)"
primary_url="$(python3 - <<'PY' "$primary"
import json, sys
print(json.loads(sys.argv[1])["url"])
PY
)"
primary_pid="$(cat "$LOG_ROOT/$primary_node.pid")"
echo "[qualification] killing primary ${primary_node} pid=${primary_pid}"
PRIMARY_PID="$primary_pid" "$ROOT_DIR/scripts/run-chaos-tests.sh" env kill-primary >/dev/null
survivor_urls=()
for url in "${NODE_URLS[@]}"; do
  if [[ "$url" != "$primary_url" ]]; then
    survivor_urls+=("$url")
  fi
done
wait_for_cluster_state after-primary-kill "${survivor_urls[@]}"
new_primary="$(current_primary_json)"
new_primary_url="$(python3 - <<'PY' "$new_primary"
import json, sys
print(json.loads(sys.argv[1])["url"])
PY
)"
if [[ "$new_primary_url" == "$primary_url" ]]; then
  record_phase primary_kill false "standby takeover did not move client traffic" "{\"before\":$primary,\"after\":$new_primary}"
  exit 5
fi
record_phase primary_kill true "standby took over after primary kill" "{\"before\":$primary,\"after\":$new_primary}"

echo "[qualification] restarting old primary ${primary_node}"
read -r http_port grpc_port aeron_port < <(node_ports_for_start "$primary_node")
RESET_CLUSTER_STATE=false "$ROOT_DIR/scripts/lab/start-node.sh" "$primary_node" "$http_port" "$grpc_port" "$aeron_port" >/dev/null
wait_for_cluster_state after-old-primary-restore
restored_primary="$(current_primary_json)"
restored_primary_node="$(python3 - <<'PY' "$restored_primary"
import json, sys
print(json.loads(sys.argv[1])["nodeId"])
PY
)"
if [[ "$restored_primary_node" == "$primary_node" ]]; then
  record_phase old_primary_restore false "restored old primary became writable again" "{\"oldPrimary\":$primary,\"current\":$restored_primary}"
  exit 6
fi
record_phase old_primary_restore true "restored old primary rejoined without taking client traffic" "{\"oldPrimary\":$primary,\"current\":$restored_primary}"

if [[ "$CONTROL_PLANE" == "etcd" ]]; then
  echo "[qualification] stopping one etcd member"
  docker compose -f "$ROOT_DIR/deploy/docker-compose/chaos-lab/docker-compose.yml" stop etcd1 >/dev/null
  wait_for_cluster_state after-etcd-member-stop
  primary="$(current_primary_json)"
  submit_probe_order "$(python3 - <<'PY' "$primary"
import json, sys
print(json.loads(sys.argv[1])["url"])
PY
)" "$(( $(date +%s) * 1000000 + 99 ))" >/dev/null
  record_phase etcd_member_stop true "cluster stayed writable with one etcd member stopped" "{\"primary\":$primary}"
  docker compose -f "$ROOT_DIR/deploy/docker-compose/chaos-lab/docker-compose.yml" start etcd1 >/dev/null
  wait_for_cluster_state after-etcd-member-restore
  record_phase etcd_member_restore true "cluster stayed single-primary after etcd member restore" "{}"
else
  echo "[qualification] stopping one ZooKeeper member"
  docker compose -f "$ROOT_DIR/deploy/docker-compose/chaos-lab/docker-compose.yml" stop zookeeper1 >/dev/null
  wait_for_cluster_state after-zookeeper-member-stop
  primary="$(current_primary_json)"
  submit_probe_order "$(python3 - <<'PY' "$primary"
import json, sys
print(json.loads(sys.argv[1])["url"])
PY
)" "$(( $(date +%s) * 1000000 + 99 ))" >/dev/null
  record_phase zookeeper_member_stop true "cluster stayed writable with one ZooKeeper member stopped" "{\"primary\":$primary}"
  docker compose -f "$ROOT_DIR/deploy/docker-compose/chaos-lab/docker-compose.yml" start zookeeper1 >/dev/null
  wait_for_cluster_state after-zookeeper-member-restore
  record_phase zookeeper_member_restore true "cluster stayed single-primary after ZooKeeper member restore" "{}"
fi

python3 - <<'PY' "$REPORT_FILE" "$CONTROL_PLANE" "$CLUSTER_NAME"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
report = json.loads(path.read_text())
report["controlPlane"] = sys.argv[2]
report["clusterName"] = sys.argv[3]
report["success"] = all(phase.get("success") for phase in report.get("phases", []))
report["conclusion"] = "control-plane qualification passed" if report["success"] else "control-plane qualification failed"
path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
PY

echo "[qualification] wrote $REPORT_FILE"
