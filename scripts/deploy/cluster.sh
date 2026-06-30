#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_FILE="${ROOT_DIR}/scripts/deploy/cluster.conf.example"

usage() {
  cat <<'EOF' >&2
usage:
  scripts/deploy/cluster.sh [-c config.env] start [node-id]
  scripts/deploy/cluster.sh [-c config.env] stop [node-id]
  scripts/deploy/cluster.sh [-c config.env] restart [node-id]
  scripts/deploy/cluster.sh [-c config.env] status [node-id]
  scripts/deploy/cluster.sh [-c config.env] health [node-id]

The config file is a bash env file. Start from:
  scripts/deploy/cluster.conf.example
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
    *)
      break
      ;;
  esac
done

ACTION="${1:-}"
TARGET_NODE="${2:-}"
if [[ -z "$ACTION" ]]; then
  usage
  exit 1
fi
case "$ACTION" in
  start|stop|restart|status|health) ;;
  *)
    usage
    exit 1
    ;;
esac

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "cluster config not found: $CONFIG_FILE" >&2
  exit 2
fi

# shellcheck source=/dev/null
source "$CONFIG_FILE"

if [[ "${#NODES[@]}" -eq 0 ]]; then
  echo "NODES is empty in $CONFIG_FILE" >&2
  exit 2
fi

shell_quote() {
  printf "%q" "$1"
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
    bash -lc "$command"
  else
    # shellcheck disable=SC2086
    ssh ${SSH_OPTS:-} "$(ssh_target "$host")" "cd $(shell_quote "${REMOTE_ROOT:-$ROOT_DIR}") && $command"
  fi
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
SERVER_MODE_VALUE=$(shell_quote "PROD")
CLUSTER_NAME=$(shell_quote "${CLUSTER_NAME:-merchant-prod}")
SHARD_KEY=$(shell_quote "${SHARD_KEY:-merchant:42}")
SYMBOL_ID=$(shell_quote "${SYMBOL_ID:-1}")
ZK_CONNECT=$(shell_quote "${ZK_CONNECT:-127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183}")
ETCD_ENDPOINT=$(shell_quote "${ETCD_ENDPOINT:-http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383}")
LEASE_PROVIDER=$(shell_quote "${LEASE_PROVIDER:-zk}")
DISCOVERY_PROVIDER=$(shell_quote "${DISCOVERY_PROVIDER:-zk}")
DATA_ROOT=$(shell_quote "$data_root")
MATCHER_DATA_DIR=$(shell_quote "$data_dir")
LOG_ROOT=$(shell_quote "$log_dir")
CP_FILE=$(shell_quote "$cp_file")
ADVERTISED_HOST=$(shell_quote "$host")
HTTP_BIND_HOST=$(shell_quote "${HTTP_BIND_HOST:-0.0.0.0}")
GRPC_BIND_HOST=$(shell_quote "${GRPC_BIND_HOST:-0.0.0.0}")
BINARY_BIND_HOST=$(shell_quote "${BINARY_BIND_HOST:-0.0.0.0}")
REPLICATION_TRANSPORT=$(shell_quote "${REPLICATION_TRANSPORT:-GRPC}")
REPLICATION_MODE=$(shell_quote "${REPLICATION_MODE:-WAIT_FOR_ANY_STANDBY}")
WAL_DURABILITY_MODE=$(shell_quote "${WAL_DURABILITY_MODE:-SYNC}")
FAILOVER_MIN_STANDBY_REPLICAS=$(shell_quote "${FAILOVER_MIN_STANDBY_REPLICAS:-1}")
ENABLE_TRANSPORT_TLS=$(shell_quote "${ENABLE_TRANSPORT_TLS:-false}")
TRANSPORT_TLS_DIR=$(shell_quote "${TRANSPORT_TLS_DIR:-$data_root/tls}")
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
  echo "http://${host}:${http_port}/api/v1/runtime/health"
}

print_status() {
  local node_id="$1"
  local host="$2"
  local http_port="$3"
  python3 - "$node_id" "$(health_url "$host" "$http_port")" <<'PY'
import json
import sys
import urllib.error
import urllib.request

node_id, url = sys.argv[1], sys.argv[2]
try:
    with urllib.request.urlopen(url, timeout=3) as response:
        payload = json.loads(response.read().decode("utf-8"))
    print(
        f"{node_id} {url} role={payload.get('role')} "
        f"accepting={payload.get('acceptingClientCommands')} "
        f"ready={payload.get('serviceReady')} "
        f"durable={payload.get('lastDurableSequence')} "
        f"applied={payload.get('lastAppliedSequence')} "
        f"committed={payload.get('replicationCommittedSequence')}"
    )
except Exception as exc:
    print(f"{node_id} {url} DOWN {exc}")
    raise SystemExit(1)
PY
}

matched=0
status_code=0
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
      ;;
    stop)
      echo "[deploy] stop $node_id host=$host"
      run_on_host "$host" "$(node_command stop-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
      ;;
    restart)
      echo "[deploy] restart $node_id host=$host"
      run_on_host "$host" "$(node_command stop-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
      run_on_host "$host" "$(node_command start-node.sh "$node_id" "$host" "$http_port" "$grpc_port" "$aeron_port" "$binary_port" "$data_dir" "$log_dir")"
      ;;
    status)
      if ! print_status "$node_id" "$host" "$http_port"; then
        status_code=1
      fi
      ;;
    health)
      curl -fsS "$(health_url "$host" "$http_port")" || status_code=1
      echo
      ;;
  esac
done

if [[ "$matched" -eq 0 ]]; then
  echo "node not found in config: $TARGET_NODE" >&2
  exit 2
fi
exit "$status_code"
