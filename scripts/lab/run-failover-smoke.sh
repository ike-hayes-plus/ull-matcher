#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/failover-smoke}"
LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/lab-logs}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"

NODE_URLS=(
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_A}"
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_B}"
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_C}"
)

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/before" "$OUT_DIR/after"
REPORT_FILE="$OUT_DIR/failover-smoke-report.json"
RUNBOOK_FILE="$OUT_DIR/transport-change-runbook.md"
SECURITY_ROTATION_REPORT="$OUT_DIR/security-rotation-report.json"

safe_name() {
  echo "$1" | sed 's#https\?://##; s#[/:]#_#g'
}

node_url_for_name() {
  lab_http_url_for_node "$1"
}

read_json_field() {
  python3 - <<'PY' "$1" "$2"
import json, sys
from pathlib import Path

value = json.loads(Path(sys.argv[1]).read_text())
for key in sys.argv[2].split("."):
    if key:
        value = value.get(key)
print("" if value is None else value)
PY
}

submit_probe_order() {
  python3 - <<'PY' "$1" "$2"
import json, sys, urllib.request

base_url = sys.argv[1]
order_id = int(sys.argv[2])
payload = json.dumps({
    "userId": 1,
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

collect_cluster_state() {
  local out_root="$1"
  mkdir -p "$out_root"
  "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster validate "$out_root" "${NODE_URLS[@]}" >/dev/null
}

wait_for_cluster_validation() {
  local out_root="$1"
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster validate "$out_root" "${NODE_URLS[@]}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  "$ROOT_DIR/scripts/run-chaos-tests.sh" cluster validate "$out_root" "${NODE_URLS[@]}"
}

wait_for_failover_validation() {
  local out_root="$1"
  local failed_url="$2"
  local survivor_urls=()
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  for url in "${NODE_URLS[@]}"; do
    if [[ "$url" != "$failed_url" ]]; then
      survivor_urls+=("$url")
    fi
  done
  while [[ $(date +%s) -lt $deadline ]]; do
    if "$ROOT_DIR/scripts/chaos/validate-chaos-lab.sh" "$out_root" "${survivor_urls[@]}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  "$ROOT_DIR/scripts/chaos/validate-chaos-lab.sh" "$out_root" "${survivor_urls[@]}"
}

echo "[smoke] validating baseline cluster state"
wait_for_cluster_validation "$OUT_DIR/before"

primary_url="$(
python3 - <<'PY' "$OUT_DIR/before" "${NODE_URLS[@]}"
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
)"

if [[ -z "$primary_url" ]]; then
  echo "[smoke] failed to detect primary url" >&2
  exit 2
fi

case "$primary_url" in
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_A}") primary_node="node-a" ;;
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_B}") primary_node="node-b" ;;
  "http://127.0.0.1:${LAB_HTTP_PORT_NODE_C}") primary_node="node-c" ;;
  *) echo "[smoke] unknown primary url: $primary_url" >&2; exit 3 ;;
esac

current_transport="$(
python3 - <<'PY' "$OUT_DIR/before"
import json, sys
from pathlib import Path
report = json.loads((Path(sys.argv[1]) / "transport-rollout-report.json").read_text())
nodes = report.get("nodes", [])
print(nodes[0].get("transport", "GRPC") if nodes else "GRPC")
PY
)"

if [[ "${ENABLE_TRANSPORT_TLS:-false}" == "true" ]]; then
  echo "[smoke] validating transport security rotation window"
  rotation_root="$OUT_DIR/security-rotation"
  mkdir -p "$rotation_root"
  standby_nodes=()
  for node in node-a node-b node-c; do
    if [[ "$node" != "${primary_node:-}" ]]; then
      standby_nodes+=("$node")
    fi
  done
  ordered_nodes=("${standby_nodes[@]}")
  ordered_nodes+=("$primary_node")
  entry_jsons=()
  probe_order_id_base=$(( $(date +%s) * 1000000 ))
  probe_index=0
  rotation_failed="false"
  for node_name in "${ordered_nodes[@]}"; do
    node_url="$(node_url_for_name "$node_name")"
    node_safe="$(safe_name "$node_url")"
    before_health="$OUT_DIR/before/$node_safe/health.json"
    generation_before="$(read_json_field "$before_health" "transportSecurityGeneration")"
    reload_before="$(read_json_field "$before_health" "transportSecurityReloadCount")"
    "$ROOT_DIR/scripts/lab/rotate-transport-certificate.sh" "$node_name" >/dev/null
    deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
    reloaded="false"
    while [[ $(date +%s) -lt $deadline ]]; do
      node_dir="$rotation_root/$node_name"
      mkdir -p "$node_dir"
      "$ROOT_DIR/scripts/chaos/collect-node-state.sh" "$node_dir" "$node_url" >/dev/null
      generation_after="$(read_json_field "$node_dir/health.json" "transportSecurityGeneration")"
      reload_after="$(read_json_field "$node_dir/health.json" "transportSecurityReloadCount")"
      if [[ "${generation_after:-0}" -gt "${generation_before:-0}" && "${reload_after:-0}" -gt "${reload_before:-0}" ]]; then
        reloaded="true"
        break
      fi
      sleep 1
    done
    probe_sequence=0
    if [[ "$reloaded" == "true" ]]; then
      probe_sequence="$(submit_probe_order "$primary_url" "$((probe_order_id_base + probe_index))")"
      probe_index=$((probe_index + 1))
      if ! collect_cluster_state "$rotation_root/$node_name/cluster"; then
        rotation_failed="true"
      fi
    else
      rotation_failed="true"
    fi
    python3 - <<'PY' "$rotation_root/$node_name/entry.json" "$node_name" "$reloaded" "$probe_sequence" "$current_transport" "$rotation_root/$node_name/cluster"
import json, sys
from pathlib import Path

cluster_dir = Path(sys.argv[6])
replicated = True
node_states = []
if cluster_dir.exists():
    for health_path in sorted(cluster_dir.glob("http_*/health.json")):
        health = json.loads(health_path.read_text())
        node_states.append({
            "nodeId": health.get("nodeId"),
            "lastReceivedSequence": health.get("lastReceivedSequence"),
            "transportSecurityGeneration": health.get("transportSecurityGeneration"),
            "transportSecurityReloadCount": health.get("transportSecurityReloadCount"),
        })
        if int(sys.argv[4]) > 0 and health.get("lastReceivedSequence", 0) < int(sys.argv[4]):
            replicated = False
entry = {
    "node": sys.argv[2],
    "success": sys.argv[3].lower() == "true" and replicated,
    "conclusion": "certificate rotated and probe sequence replicated under rebuilt secure session" if sys.argv[3].lower() == "true" and replicated else "certificate rotation did not converge before timeout",
    "transport": sys.argv[5],
    "reloaded": sys.argv[3].lower() == "true",
    "probeSequence": int(sys.argv[4]),
    "replicatedAfterRotation": replicated,
    "nodes": node_states,
}
Path(sys.argv[1]).write_text(json.dumps(entry, indent=2) + "\n", encoding="utf-8")
PY
    entry_jsons+=("$rotation_root/$node_name/entry.json")
  done
  python3 - <<'PY' "$SECURITY_ROTATION_REPORT" "${entry_jsons[@]}"
import json, sys
from pathlib import Path

entries = [json.loads(Path(path).read_text()) for path in sys.argv[2:]]
success = all(entry.get("success") for entry in entries)
Path(sys.argv[1]).write_text(json.dumps({
    "success": success,
    "scenario": "transport_security_rotation",
    "category": "ha-security",
    "severity": "ok" if success else "critical",
    "conclusion": "transport security rotation window passed" if success else "transport security rotation window failed",
    "entries": entries,
}, indent=2) + "\n", encoding="utf-8")
PY
  if [[ "$(read_json_field "$SECURITY_ROTATION_REPORT" "success")" != "True" && "$(read_json_field "$SECURITY_ROTATION_REPORT" "success")" != "true" ]]; then
    rotation_failed="true"
  fi
  if [[ "$rotation_failed" == "true" ]]; then
    "$ROOT_DIR/scripts/chaos/summarize-chaos-report.py" --report "$OUT_DIR/chaos-summary.json" \
      "$OUT_DIR/before/validation-report.json" \
      "$OUT_DIR/before/transport-rollout-report.json" \
      "$SECURITY_ROTATION_REPORT" || true
    exit 6
  fi
fi

pid_file="$LOG_ROOT/$primary_node.pid"
if [[ ! -f "$pid_file" ]]; then
  echo "[smoke] missing pid file for $primary_node: $pid_file" >&2
  exit 4
fi

primary_pid="$(cat "$pid_file")"
echo "[smoke] detected primary $primary_node url=$primary_url pid=$primary_pid"

PRIMARY_PID="$primary_pid" "$ROOT_DIR/scripts/run-chaos-tests.sh" env kill-primary

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
switched="false"
while [[ $(date +%s) -lt $deadline ]]; do
  if wait_for_failover_validation "$OUT_DIR/after" "$primary_url" >/dev/null 2>&1; then
    new_primary_url="$(
python3 - <<'PY' "$OUT_DIR/after" "$primary_url" "${NODE_URLS[@]}"
import json, sys
from pathlib import Path

base = Path(sys.argv[1])
failed_url = sys.argv[2]
urls = sys.argv[3:]
for url in urls:
    if url == failed_url:
        continue
    safe = url.replace("http://", "http___").replace("https://", "https___").replace("/", "_").replace(":", "_")
    readiness = json.loads((base / safe / "readiness.json").read_text())
    if readiness.get("clientTrafficReady") is True:
        print(url)
        raise SystemExit(0)
raise SystemExit(1)
PY
)"
    if [[ -n "$new_primary_url" && "$new_primary_url" != "$primary_url" ]]; then
      switched="true"
      echo "[smoke] failover succeeded new_primary=$new_primary_url"
      break
    fi
  fi
  sleep 1
done

if [[ "$switched" != "true" ]]; then
  echo "[smoke] failover did not complete within ${TIMEOUT_SECONDS}s" >&2
  python3 - <<'PY' "$REPORT_FILE" "$primary_url" "" "false" "$primary_pid"
import json, sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({
    "success": False,
    "scenario": "failover_smoke",
    "category": "ha-failover",
    "severity": "critical",
    "conclusion": "standby takeover did not complete before timeout",
    "beforePrimaryUrl": sys.argv[2],
    "afterPrimaryUrl": sys.argv[3],
    "killedPrimaryPid": int(sys.argv[5]),
    "reason": "timeout",
}, indent=2) + "\n", encoding="utf-8")
PY
  summary_inputs=(
    "$OUT_DIR/before/validation-report.json"
    "$OUT_DIR/before/transport-rollout-report.json"
    "$OUT_DIR/after/validation-report.json"
    "$OUT_DIR/after/transport-rollout-report.json"
    "$REPORT_FILE"
  )
  if [[ -f "$SECURITY_ROTATION_REPORT" ]]; then
    summary_inputs+=("$SECURITY_ROTATION_REPORT")
  fi
  "$ROOT_DIR/scripts/chaos/summarize-chaos-report.py" --report "$OUT_DIR/chaos-summary.json" "${summary_inputs[@]}" || true
  exit 5
fi

python3 - <<'PY' "$REPORT_FILE" "$primary_url" "$new_primary_url" "true" "$primary_pid"
import json, sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({
    "success": sys.argv[4].lower() == "true",
    "scenario": "failover_smoke",
    "category": "ha-failover",
    "severity": "ok",
    "conclusion": "primary kill and standby takeover succeeded",
    "beforePrimaryUrl": sys.argv[2],
    "afterPrimaryUrl": sys.argv[3],
    "killedPrimaryPid": int(sys.argv[5]),
    "beforeValidationReport": "before/validation-report.json",
    "afterValidationReport": "after/validation-report.json",
}, indent=2) + "\n", encoding="utf-8")
PY
if [[ "$current_transport" == "AERON" ]]; then
  target_transport="GRPC"
else
  target_transport="AERON"
fi
"$ROOT_DIR/scripts/chaos/generate-transport-change-runbook.py" \
  --report "$RUNBOOK_FILE" \
  --validation "$OUT_DIR/before/validation-report.json" \
  --rollout "$OUT_DIR/before/transport-rollout-report.json" \
  --target-transport "$target_transport" \
  --window-id "failover-smoke-switch-001"

summary_inputs=(
  "$OUT_DIR/before/validation-report.json"
  "$OUT_DIR/before/transport-rollout-report.json"
  "$OUT_DIR/after/validation-report.json"
  "$OUT_DIR/after/transport-rollout-report.json"
  "$REPORT_FILE"
)
if [[ -f "$SECURITY_ROTATION_REPORT" ]]; then
  summary_inputs+=("$SECURITY_ROTATION_REPORT")
fi
"$ROOT_DIR/scripts/chaos/summarize-chaos-report.py" --report "$OUT_DIR/chaos-summary.json" "${summary_inputs[@]}"
