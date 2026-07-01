#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_CONFIG_FILE="${ROOT_DIR}/scripts/deploy/default.conf"
CONFIG_FILE=""
FORCE_STOP=false

usage() {
  cat <<'EOF' >&2
usage:
  scripts/deploy/cluster.sh -c config.env start [node-id]
  scripts/deploy/cluster.sh -c config.env stop [--force] [node-id]
  scripts/deploy/cluster.sh -c config.env restart [node-id]
  scripts/deploy/cluster.sh -c config.env status [node-id]
  scripts/deploy/cluster.sh -c config.env health [node-id]
  scripts/deploy/cluster.sh -c config.env validate [node-id]
  scripts/deploy/cluster.sh -c config.env plan [node-id]

The config file is a real cluster env file. Create one from:
  cp scripts/deploy/cluster.conf.example conf/<cluster>.env

Optional node-id must match a nodeId from the config NODES list.
Use --force with stop only when intentionally bypassing primary stop safety checks.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -f|--force)
      FORCE_STOP=true
      shift
      ;;
    *)
      break
      ;;
  esac
done

ACTION="${1:-}"
if [[ -z "$ACTION" ]]; then
  usage
  exit 1
fi
shift
TARGET_NODE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--force)
      FORCE_STOP=true
      shift
      ;;
    *)
      if [[ -n "$TARGET_NODE" ]]; then
        usage
        exit 1
      fi
      TARGET_NODE="$1"
      shift
      ;;
  esac
done
case "$ACTION" in
  start|stop|restart|status|health|validate|plan) ;;
  *)
    usage
    exit 1
    ;;
esac
if [[ "$FORCE_STOP" == "true" && "$ACTION" != "stop" ]]; then
  echo "--force is only valid with stop" >&2
  usage
  exit 1
fi

if [[ -z "$CONFIG_FILE" ]]; then
  echo "cluster config is required; copy scripts/deploy/cluster.conf.example to conf/<cluster>.env and pass -c" >&2
  usage
  exit 2
fi

case "$CONFIG_FILE" in
  *.example|*.example.*)
    echo "refusing to use example config directly: $CONFIG_FILE" >&2
    echo "copy it to a real cluster env file and pass that path with -c" >&2
    exit 2
    ;;
esac

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "cluster config not found: $CONFIG_FILE" >&2
  exit 2
fi

# shellcheck source=/dev/null
source "$DEFAULT_CONFIG_FILE"
# shellcheck source=/dev/null
source "$CONFIG_FILE"

if ! declare -p NODES >/dev/null 2>&1; then
  echo "NODES must be defined in cluster config: $CONFIG_FILE" >&2
  exit 2
fi
if [[ "${#NODES[@]}" -eq 0 ]]; then
  echo "NODES must not be empty in cluster config: $CONFIG_FILE" >&2
  exit 2
fi

shell_quote() {
  printf "%q" "$1"
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_local_host() {
  case "$1" in
    127.0.0.1|localhost|"$(hostname)"|"$(hostname -s 2>/dev/null || true)")
      return 0
      ;;
  esac
  return 1
}

ssh_target() {
  local host="$1"
  if [[ -n "${REMOTE_USER:-}" ]]; then
    echo "${REMOTE_USER}@${host}"
  else
    echo "$host"
  fi
}

run_on_host() {
  local host="$1"
  shift
  local command="$*"
  if is_local_host "$host"; then
    (cd "$ROOT_DIR" && bash -lc "$command")
  else
    local -a ssh_args=()
    if [[ -n "${SSH_OPTS:-}" ]]; then
      read -r -a ssh_args <<< "$SSH_OPTS"
    fi
    ssh "${ssh_args[@]}" "$(ssh_target "$host")" "cd $(shell_quote "$REMOTE_ROOT") && $command"
  fi
}

validate_provider() {
  local name="$1"
  local value="$2"
  case "$value" in
    zk|etcd) ;;
    *)
      echo "$name must be zk or etcd: $value" >&2
      return 1
      ;;
  esac
}

validate_transport() {
  case "$1" in
    GRPC|AERON|AERON_PREVIEW) ;;
    *)
      echo "REPLICATION_TRANSPORT must be GRPC, AERON, or AERON_PREVIEW: $1" >&2
      return 1
      ;;
  esac
}

validate_replication_mode() {
  case "$1" in
    LOCAL_ONLY|WAIT_FOR_ANY_STANDBY|WAIT_FOR_QUORUM_STANDBYS|WAIT_FOR_ALL_STANDBYS) ;;
    *)
      echo "REPLICATION_MODE must be LOCAL_ONLY, WAIT_FOR_ANY_STANDBY, WAIT_FOR_QUORUM_STANDBYS, or WAIT_FOR_ALL_STANDBYS: $1" >&2
      return 1
      ;;
  esac
}

validate_wal_mode() {
  case "$1" in
    SYNC_PER_BATCH|SYNC_PER_COMMAND|OS_BUFFERED) ;;
    *)
      echo "WAL_DURABILITY_MODE must be SYNC_PER_COMMAND, SYNC_PER_BATCH, or OS_BUFFERED: $1" >&2
      return 1
      ;;
  esac
}

is_loopback_bind_host() {
  case "$1" in
    127.*|localhost|::1) return 0 ;;
  esac
  return 1
}

is_remote_bind_host() {
  ! is_loopback_bind_host "$1"
}

is_temp_data_dir() {
  case "$1" in
    /tmp|/tmp/*|/var/tmp|/var/tmp/*|*/target|*/target/*|*/build|*/build/*)
      return 0
      ;;
  esac
  return 1
}

is_binary_enabled() {
  [[ -n "$1" && "$1" != "-" ]]
}

validate_remote_prerequisites() {
  local host="$1"
  local data_dir="$2"
  local log_dir="$3"
  run_on_host "$host" "test -d $(shell_quote "$REMOTE_ROOT") && command -v bash >/dev/null && command -v lsof >/dev/null && mkdir -p $(shell_quote "$data_dir") $(shell_quote "$log_dir")"
}

node_matches() {
  [[ -z "$TARGET_NODE" || "$1" == "$TARGET_NODE" ]]
}

env_assignments() {
  local node_id="$1"
  local host="$2"
  local data_dir="$3"
  local log_dir="$4"
  local cp_file="$log_dir/classpath.txt"
  local data_root
  data_root="$(dirname "$data_dir")"
  cat <<EOF
MODE_LABEL=$(shell_quote "deploy")
SERVER_MODE_VALUE=$(shell_quote "$SERVER_MODE_VALUE")
CLUSTER_NAME=$(shell_quote "$CLUSTER_NAME")
SHARD_KEY=$(shell_quote "$SHARD_KEY")
SYMBOL_ID=$(shell_quote "$SYMBOL_ID")
ZK_CONNECT=$(shell_quote "$ZK_CONNECT")
ETCD_ENDPOINT=$(shell_quote "$ETCD_ENDPOINT")
LEASE_PROVIDER=$(shell_quote "$LEASE_PROVIDER")
DISCOVERY_PROVIDER=$(shell_quote "$DISCOVERY_PROVIDER")
DATA_ROOT=$(shell_quote "$data_root")
MATCHER_DATA_DIR=$(shell_quote "$data_dir")
LOG_ROOT=$(shell_quote "$log_dir")
CP_FILE=$(shell_quote "$cp_file")
ADVERTISED_HOST=$(shell_quote "$host")
HTTP_BIND_HOST=$(shell_quote "$HTTP_BIND_HOST")
GRPC_BIND_HOST=$(shell_quote "$GRPC_BIND_HOST")
BINARY_BIND_HOST=$(shell_quote "$BINARY_BIND_HOST")
REPLICATION_TRANSPORT=$(shell_quote "$REPLICATION_TRANSPORT")
REPLICATION_MODE=$(shell_quote "$REPLICATION_MODE")
WAL_DURABILITY_MODE=$(shell_quote "$WAL_DURABILITY_MODE")
WAL_FORCE_BATCH_SIZE=$(shell_quote "$WAL_FORCE_BATCH_SIZE")
WAL_FORCE_MAX_DELAY_MICROS=$(shell_quote "$WAL_FORCE_MAX_DELAY_MICROS")
FAILOVER_MIN_STANDBY_REPLICAS=$(shell_quote "$FAILOVER_MIN_STANDBY_REPLICAS")
FAILOVER_PRIMARY_HEARTBEAT_TIMEOUT_MILLIS=$(shell_quote "$FAILOVER_PRIMARY_HEARTBEAT_TIMEOUT_MILLIS")
FAILOVER_MAX_PROMOTION_LAG=$(shell_quote "$FAILOVER_MAX_PROMOTION_LAG")
ENABLE_TRANSPORT_TLS=$(shell_quote "$ENABLE_TRANSPORT_TLS")
TRANSPORT_TLS_DIR=$(shell_quote "$TRANSPORT_TLS_DIR")
TRANSPORT_TLS_RELOAD_MILLIS=$(shell_quote "$TRANSPORT_TLS_RELOAD_MILLIS")
TRANSPORT_CHANGE_WINDOW_ID=$(shell_quote "$TRANSPORT_CHANGE_WINDOW_ID")
ALLOW_TRANSPORT_CHANGE=$(shell_quote "$ALLOW_TRANSPORT_CHANGE")
ALLOW_INSECURE_REMOTE_HTTP=$(shell_quote "$ALLOW_INSECURE_REMOTE_HTTP")
EOF
}

node_command() {
  local script="$1"
  local node_id="$2"
  local host="$3"
  local http_port="$4"
  local grpc_port="$5"
  local aeron_port="$6"
  local binary_port="$7"
  local data_dir="$8"
  local log_dir="$9"
  local envs
  envs="$(env_assignments "$node_id" "$host" "$data_dir" "$log_dir" | tr '\n' ' ')"
  if [[ "$script" == "start-node.sh" ]]; then
    if [[ -n "$binary_port" && "$binary_port" != "-" ]]; then
      echo "env $envs bash scripts/deploy/$script $(shell_quote "$node_id") $(shell_quote "$http_port") $(shell_quote "$grpc_port") $(shell_quote "$aeron_port") $(shell_quote "$binary_port")"
    else
      echo "env $envs bash scripts/deploy/$script $(shell_quote "$node_id") $(shell_quote "$http_port") $(shell_quote "$grpc_port") $(shell_quote "$aeron_port")"
    fi
  else
    echo "env $envs bash scripts/deploy/$script $(shell_quote "$node_id")"
  fi
}

health_url() {
  local host="$1"
  local http_port="$2"
  local bind_host="$HTTP_BIND_HOST"
  if is_loopback_bind_host "$bind_host"; then
    echo "http://127.0.0.1:${http_port}/api/v1/runtime/health"
  else
    echo "http://${host}:${http_port}/api/v1/runtime/health"
  fi
}

health_probe_command() {
  local mode="$1"
  local node_id="$2"
  local url="$3"
  echo "python3 scripts/deploy/health_probe.py $(shell_quote "$mode") $(shell_quote "$node_id") $(shell_quote "$url")"
}

print_status() {
  local node_id="$1"
  local host="$2"
  local http_port="$3"
  run_on_host "$host" "$(health_probe_command status "$node_id" "$(health_url "$host" "$http_port")")"
}

print_health() {
  local host="$1"
  local http_port="$2"
  run_on_host "$host" "curl -fsS $(shell_quote "$(health_url "$host" "$http_port")")"
}

fetch_node_health() {
  local node_id="$1"
  local host="$2"
  local http_port="$3"
  run_on_host "$host" "$(health_probe_command json "$node_id" "$(health_url "$host" "$http_port")")"
}

write_cluster_health_snapshot() {
  local out_file="$1"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local -a output_files=()
  local -a error_files=()
  local -a pids=()
  local item
  local index=0
  : > "$out_file"
  for item in "${NODES[@]}"; do
    IFS='|' read -r node_id host http_port _grpc_port _aeron_port _binary_port _data_dir _log_dir <<<"$item"
    output_files+=("$tmp_dir/$index.out")
    error_files+=("$tmp_dir/$index.err")
    fetch_node_health "$node_id" "$host" "$http_port" >"${output_files[$index]}" 2>"${error_files[$index]}" &
    pids+=("$!")
    index=$((index + 1))
  done
  local status=0
  local i
  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
      status=1
    fi
  done
  if [[ "$status" -eq 0 ]]; then
    for i in "${!output_files[@]}"; do
      cat "${output_files[$i]}" >> "$out_file"
    done
  else
    for i in "${!error_files[@]}"; do
      if [[ -s "${error_files[$i]}" ]]; then
        cat "${error_files[$i]}" >&2
      fi
    done
  fi
  rm -rf "$tmp_dir"
  return "$status"
}

cluster_health_summary() {
  local snapshot_file="$1"
  python3 - "$snapshot_file" <<'PY'
import json
import sys

path = sys.argv[1]
payloads = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
primary = [p for p in payloads if p.get("role") == "PRIMARY"]
standbys = [p for p in payloads if p.get("role") == "STANDBY"]
accepting = [p for p in payloads if p.get("acceptingClientCommands")]
max_durable = max((int(p.get("lastDurableSequence") or 0) for p in payloads), default=0)
print(
    f"nodes={len(payloads)} primary={len(primary)} standby={len(standbys)} "
    f"accepting={len(accepting)} maxDurable={max_durable}"
)
for p in sorted(payloads, key=lambda item: item.get("_deployNodeId", "")):
    durable = int(p.get("lastDurableSequence") or 0)
    applied = int(p.get("lastAppliedSequence") or 0)
    committed = int(p.get("replicationCommittedSequence") or 0)
    role = p.get("role")
    accepting_node = p.get("acceptingClientCommands")
    ready = p.get("serviceReady")
    lag = max(0, max_durable - durable)
    transport = p.get("replicationTransport")
    print(
        f"{p.get('_deployNodeId')} role={role} accepting={accepting_node} ready={ready} "
        f"durable={durable} applied={applied} committed={committed} lag={lag} transport={transport}"
    )
PY
}

cluster_health_ready() {
  local snapshot_file="$1"
  local required_standbys="$FAILOVER_MIN_STANDBY_REPLICAS"
  python3 - "$snapshot_file" "$required_standbys" <<'PY'
import json
import sys

path = sys.argv[1]
required_standbys = int(sys.argv[2])
payloads = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
if not payloads:
    raise SystemExit(1)
primary = [p for p in payloads if p.get("role") == "PRIMARY"]
accepting = [p for p in payloads if p.get("acceptingClientCommands")]
standbys = [p for p in payloads if p.get("role") == "STANDBY"]
if len(primary) != 1:
    raise SystemExit(1)
if len(accepting) != 1 or accepting[0].get("_deployNodeId") != primary[0].get("_deployNodeId"):
    raise SystemExit(1)
if len(standbys) < required_standbys:
    raise SystemExit(1)
primary_durable = int(primary[0].get("lastDurableSequence") or 0)
for standby in standbys:
    durable = int(standby.get("lastDurableSequence") or 0)
    applied = int(standby.get("lastAppliedSequence") or 0)
    if durable < applied:
        raise SystemExit(1)
    if durable > primary_durable:
        raise SystemExit(1)
raise SystemExit(0)
PY
}

wait_cluster_stable() {
  local label="$1"
  local timeout_seconds="$DEPLOY_HEALTH_TIMEOUT_SECONDS"
  local stable_rounds_required="$DEPLOY_STABLE_ROUNDS"
  local poll_interval_seconds="$DEPLOY_POLL_INTERVAL_SECONDS"
  if ! is_positive_integer "$timeout_seconds"; then
    echo "DEPLOY_HEALTH_TIMEOUT_SECONDS must be a positive integer: $timeout_seconds" >&2
    return 2
  fi
  if ! is_positive_integer "$stable_rounds_required"; then
    echo "DEPLOY_STABLE_ROUNDS must be a positive integer: $stable_rounds_required" >&2
    return 2
  fi
  if ! is_positive_integer "$poll_interval_seconds"; then
    echo "DEPLOY_POLL_INTERVAL_SECONDS must be a positive integer: $poll_interval_seconds" >&2
    return 2
  fi
  local snapshot_file
  snapshot_file="$(mktemp)"
  local deadline=$((SECONDS + timeout_seconds))
  local stable_rounds=0
  local last_error=""
  while [[ "$SECONDS" -le "$deadline" ]]; do
    if write_cluster_health_snapshot "$snapshot_file" 2>"$snapshot_file.err"; then
      if cluster_health_ready "$snapshot_file"; then
        stable_rounds=$((stable_rounds + 1))
        if [[ "$stable_rounds" -ge "$stable_rounds_required" ]]; then
          echo "[deploy] $label cluster stable"
          cluster_health_summary "$snapshot_file"
          rm -f "$snapshot_file" "$snapshot_file.err"
          return 0
        fi
      else
        stable_rounds=0
      fi
      last_error="$(cluster_health_summary "$snapshot_file" 2>/dev/null || true)"
    else
      stable_rounds=0
      last_error="$(cat "$snapshot_file.err" 2>/dev/null || true)"
    fi
    sleep "$poll_interval_seconds"
  done
  echo "[deploy] timed out waiting for $label cluster stability" >&2
  if [[ -n "$last_error" ]]; then
    echo "$last_error" >&2
  fi
  rm -f "$snapshot_file" "$snapshot_file.err"
  return 1
}

assert_restart_safe() {
  local target_node="$1"
  local snapshot_file
  snapshot_file="$(mktemp)"
  if ! write_cluster_health_snapshot "$snapshot_file"; then
    rm -f "$snapshot_file"
    echo "cannot verify current cluster health before restart" >&2
    return 1
  fi
  python3 - "$snapshot_file" "$target_node" "$FAILOVER_MIN_STANDBY_REPLICAS" <<'PY'
import json
import sys

path, target_node, required_standbys = sys.argv[1], sys.argv[2], int(sys.argv[3])
payloads = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
primary = [p for p in payloads if p.get("role") == "PRIMARY"]
if len(primary) != 1:
    raise SystemExit("restart requires exactly one healthy primary before stopping a node")
target = next((p for p in payloads if p.get("_deployNodeId") == target_node), None)
if target is None:
    raise SystemExit(f"target node not visible in health snapshot: {target_node}")
standbys = [p for p in payloads if p.get("role") == "STANDBY"]
if target.get("role") == "PRIMARY" and len(standbys) < required_standbys:
    raise SystemExit(
        f"refusing to restart primary {target_node}: standby count {len(standbys)} < required {required_standbys}"
    )
if target.get("role") != "PRIMARY" and len(standbys) - 1 < 0:
    raise SystemExit(f"refusing to restart {target_node}: no standby redundancy visible")
raise SystemExit(0)
PY
  local status=$?
  rm -f "$snapshot_file"
  return "$status"
}

assert_stop_safe() {
  local target_node="$1"
  local snapshot_file
  snapshot_file="$(mktemp)"
  if ! write_cluster_health_snapshot "$snapshot_file"; then
    rm -f "$snapshot_file"
    echo "cannot verify current cluster health before stopping a node; use --force only for an intentional emergency stop" >&2
    return 1
  fi
  python3 - "$snapshot_file" "$target_node" "$FAILOVER_MIN_STANDBY_REPLICAS" <<'PY'
import json
import sys

path, target_node, required_standbys = sys.argv[1], sys.argv[2], int(sys.argv[3])
payloads = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
primary = [p for p in payloads if p.get("role") == "PRIMARY"]
if len(primary) != 1:
    raise SystemExit("stop requires exactly one healthy primary before stopping a target node")
target = next((p for p in payloads if p.get("_deployNodeId") == target_node), None)
if target is None:
    raise SystemExit(f"target node not visible in health snapshot: {target_node}")
if target.get("role") == "PRIMARY":
    standbys = [p for p in payloads if p.get("role") == "STANDBY"]
    if len(standbys) < required_standbys:
        raise SystemExit(
            f"refusing to stop primary {target_node}: standby count {len(standbys)} < required {required_standbys}; use --force only for emergency shutdown"
        )
raise SystemExit(0)
PY
  local status=$?
  rm -f "$snapshot_file"
  return "$status"
}

validate_config() {
  local status=0
  local lease_provider="$LEASE_PROVIDER"
  local discovery_provider="$DISCOVERY_PROVIDER"
  local replication_transport="$REPLICATION_TRANSPORT"
  local replication_mode="$REPLICATION_MODE"
  local wal_durability_mode="$WAL_DURABILITY_MODE"
  local http_bind_host="$HTTP_BIND_HOST"
  local binary_bind_host="$BINARY_BIND_HOST"
  local allow_dual_remote_ingress="$ALLOW_DUAL_REMOTE_INGRESS"
  local allow_insecure_remote_http="$ALLOW_INSECURE_REMOTE_HTTP"
  local allow_transport_change="$ALLOW_TRANSPORT_CHANGE"

  validate_provider "LEASE_PROVIDER" "$lease_provider" || status=1
  validate_provider "DISCOVERY_PROVIDER" "$discovery_provider" || status=1
  validate_transport "$replication_transport" || status=1
  validate_replication_mode "$replication_mode" || status=1
  validate_wal_mode "$wal_durability_mode" || status=1
  if [[ "$lease_provider" != "$discovery_provider" ]]; then
    echo "LEASE_PROVIDER and DISCOVERY_PROVIDER must match for one control plane: $lease_provider != $discovery_provider" >&2
    status=1
  fi
  if [[ "$ENABLE_TRANSPORT_TLS" != "true" && "$ENABLE_TRANSPORT_TLS" != "false" ]]; then
    echo "ENABLE_TRANSPORT_TLS must be true or false" >&2
    status=1
  fi
  if [[ "$allow_dual_remote_ingress" != "true" && "$allow_dual_remote_ingress" != "false" ]]; then
    echo "ALLOW_DUAL_REMOTE_INGRESS must be true or false" >&2
    status=1
  fi
  if [[ "$allow_insecure_remote_http" != "true" && "$allow_insecure_remote_http" != "false" ]]; then
    echo "ALLOW_INSECURE_REMOTE_HTTP must be true or false" >&2
    status=1
  fi
  if [[ "$allow_transport_change" != "true" && "$allow_transport_change" != "false" ]]; then
    echo "ALLOW_TRANSPORT_CHANGE must be true or false" >&2
    status=1
  fi
  if ! is_positive_integer "$DEPLOY_HEALTH_TIMEOUT_SECONDS"; then
    echo "DEPLOY_HEALTH_TIMEOUT_SECONDS must be a positive integer" >&2
    status=1
  fi
  if ! is_positive_integer "$DEPLOY_STABLE_ROUNDS"; then
    echo "DEPLOY_STABLE_ROUNDS must be a positive integer" >&2
    status=1
  fi
  if ! is_positive_integer "$DEPLOY_POLL_INTERVAL_SECONDS"; then
    echo "DEPLOY_POLL_INTERVAL_SECONDS must be a positive integer" >&2
    status=1
  fi
  if is_remote_bind_host "$http_bind_host" && [[ "$allow_insecure_remote_http" != "true" ]]; then
    echo "HTTP_BIND_HOST=$http_bind_host is non-loopback in PROD; set ALLOW_INSECURE_REMOTE_HTTP=true only when an internal sidecar/gateway is the security boundary" >&2
    status=1
  fi

  local seen_nodes=" "
  local seen_hosts_http=" "
  local seen_hosts_grpc=" "
  local seen_hosts_aeron=" "
  local seen_hosts_binary=" "
  local seen_data_dirs=" "
  local matched_nodes=0
  local item
  for item in "${NODES[@]}"; do
    IFS='|' read -r node_id host http_port grpc_port aeron_port binary_port data_dir log_dir extra <<<"$item"
    if [[ -n "${extra:-}" || -z "${node_id:-}" || -z "${host:-}" || -z "${http_port:-}" || -z "${grpc_port:-}" || -z "${aeron_port:-}" || -z "${binary_port:-}" || -z "${data_dir:-}" || -z "${log_dir:-}" ]]; then
      echo "invalid NODES entry; expected 8 fields: $item" >&2
      status=1
      continue
    fi
    if ! node_matches "$node_id"; then
      continue
    fi
    matched_nodes=$((matched_nodes + 1))
    if [[ "$seen_nodes" == *" $node_id "* ]]; then
      echo "duplicate nodeId: $node_id" >&2
      status=1
    fi
    seen_nodes+="$node_id "
    for port_name in http_port grpc_port aeron_port; do
      local port_value="${!port_name}"
      if ! is_positive_integer "$port_value" || [[ "$port_value" -gt 65535 ]]; then
        echo "$node_id $port_name must be 1..65535: $port_value" >&2
        status=1
      fi
    done
    if is_binary_enabled "$binary_port"; then
      if ! is_positive_integer "$binary_port" || [[ "$binary_port" -gt 65535 ]]; then
        echo "$node_id binaryPort must be '-' or 1..65535: $binary_port" >&2
        status=1
      fi
    fi
    if is_temp_data_dir "$data_dir"; then
      echo "$node_id dataDir must not be under tmp/target/build in PROD: $data_dir" >&2
      status=1
    fi
    if [[ "$seen_data_dirs" == *" $data_dir "* ]]; then
      echo "duplicate dataDir: $data_dir" >&2
      status=1
    fi
    seen_data_dirs+="$data_dir "

    local host_http="$host:$http_port"
    local host_grpc="$host:$grpc_port"
    local host_aeron="$host:$aeron_port"
    if [[ "$seen_hosts_http" == *" $host_http "* || "$seen_hosts_grpc" == *" $host_grpc "* || "$seen_hosts_aeron" == *" $host_aeron "* ]]; then
      echo "$node_id duplicate host port on $host" >&2
      status=1
    fi
    seen_hosts_http+="$host_http "
    seen_hosts_grpc+="$host_grpc "
    seen_hosts_aeron+="$host_aeron "
    if is_binary_enabled "$binary_port"; then
      local host_binary="$host:$binary_port"
      if [[ "$seen_hosts_binary" == *" $host_binary "* ]]; then
        echo "$node_id duplicate binary host port: $host_binary" >&2
        status=1
      fi
      seen_hosts_binary+="$host_binary "
      if is_remote_bind_host "$http_bind_host" && is_remote_bind_host "$binary_bind_host" && [[ "$allow_dual_remote_ingress" != "true" ]]; then
        echo "$node_id opens both HTTP and binary on non-loopback bind hosts; set ALLOW_DUAL_REMOTE_INGRESS=true only if both write paths are intentionally exposed" >&2
        status=1
      fi
    fi
  done
  if [[ "$matched_nodes" -eq 0 ]]; then
    echo "node not found in config: $TARGET_NODE" >&2
    status=1
  fi
  return "$status"
}

print_plan() {
  local node_id="$1"
  local host="$2"
  local http_port="$3"
  local grpc_port="$4"
  local aeron_port="$5"
  local binary_port="$6"
  local data_dir="$7"
  local log_dir="$8"
  echo "node=$node_id host=$host http=$http_port grpc=$grpc_port aeron=$aeron_port binary=$binary_port data=$data_dir log=$log_dir"
  echo "  cluster=$CLUSTER_NAME shard=$SHARD_KEY symbol=$SYMBOL_ID"
  echo "  providers lease=$LEASE_PROVIDER discovery=$DISCOVERY_PROVIDER transport=$REPLICATION_TRANSPORT replication=$REPLICATION_MODE"
  echo "  bind http=$HTTP_BIND_HOST grpc=$GRPC_BIND_HOST binary=$BINARY_BIND_HOST"
  if is_loopback_bind_host "$HTTP_BIND_HOST"; then
    echo "  health-url: $(health_url "$host" "$http_port") (resolved on target host)"
  else
    echo "  health-url: $(health_url "$host" "$http_port")"
  fi
  echo "  command: $(node_command start-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
}

if [[ "$ACTION" == "validate" || "$ACTION" == "plan" || "$ACTION" == "start" || "$ACTION" == "restart" ]]; then
  if ! validate_config; then
    exit 2
  fi
fi
if [[ "$ACTION" == "stop" && "$FORCE_STOP" == "true" && -z "$TARGET_NODE" ]]; then
  echo "[deploy] --force accepted for full cluster stop"
fi

matched=0
status_code=0
started_nodes=0
for item in "${NODES[@]}"; do
  IFS='|' read -r node_id host http_port grpc_port aeron_port binary_port data_dir log_dir <<<"$item"
  if ! node_matches "$node_id"; then
    continue
  fi
  matched=1
  case "$ACTION" in
    start)
      echo "[deploy] start $node_id host=$host http=$http_port grpc=$grpc_port data=$data_dir"
      run_on_host "$host" "$(node_command start-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
      started_nodes=$((started_nodes + 1))
      ;;
    stop)
      echo "[deploy] stop $node_id host=$host"
      if [[ "$FORCE_STOP" != "true" && -n "$TARGET_NODE" ]]; then
        assert_stop_safe "$node_id"
      fi
      run_on_host "$host" "$(node_command stop-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
      ;;
    restart)
      echo "[deploy] restart $node_id host=$host"
      assert_restart_safe "$node_id"
      run_on_host "$host" "$(node_command stop-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
      run_on_host "$host" "$(node_command start-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
      wait_cluster_stable "restart $node_id"
      ;;
    status)
      if ! print_status "$node_id" "$host" "$http_port"; then
        status_code=1
      fi
      ;;
    health)
      print_health "$host" "$http_port" || status_code=1
      echo
      ;;
    validate)
      echo "[deploy] validate $node_id host=$host"
      if ! validate_remote_prerequisites "$host" "$data_dir" "$log_dir"; then
        status_code=1
      fi
      ;;
    plan)
      print_plan "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir"
      ;;
  esac
done

if [[ "$matched" -eq 0 ]]; then
  echo "node not found in config: $TARGET_NODE" >&2
  exit 2
fi
if [[ "$ACTION" == "start" && "$started_nodes" -gt 0 ]]; then
  wait_cluster_stable "start"
fi
if [[ "$ACTION" == "status" && -z "$TARGET_NODE" && "$status_code" -eq 0 ]]; then
  snapshot_file="$(mktemp)"
  if write_cluster_health_snapshot "$snapshot_file"; then
    echo "[deploy] cluster summary"
    cluster_health_summary "$snapshot_file"
  else
    status_code=1
  fi
  rm -f "$snapshot_file"
fi
exit "$status_code"
