#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-}"
DRY_RUN="${DRY_RUN:-false}"

PRIMARY_PID="${PRIMARY_PID:-}"
ZK_HOST="${ZK_HOST:-127.0.0.1}"
ZK_PORT="${ZK_PORT:-2181}"
NETWORK_IFACE="${NETWORK_IFACE:-lo0}"
DATA_DIR="${DATA_DIR:-}"
FILL_MB="${FILL_MB:-256}"
LATENCY_MS="${LATENCY_MS:-200}"
LOSS_PERCENT="${LOSS_PERCENT:-20}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/chaos/run-env-chaos.sh <scenario>

Scenarios:
  wal-disk-fill
  wal-disk-clean
  zk-disconnect
  zk-reconnect
  grpc-delay
  grpc-loss
  netem-reset
  kill-primary
  help

Examples:
  DRY_RUN=true ./scripts/chaos/run-env-chaos.sh zk-disconnect
  DATA_DIR=/data/ull-matcher ./scripts/chaos/run-env-chaos.sh wal-disk-fill
  PRIMARY_PID=12345 ./scripts/chaos/run-env-chaos.sh kill-primary
EOF
}

log() {
  printf '[chaos] %s\n' "$*"
}

run() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    log "DRY_RUN $*"
  else
    log "exec $*"
    eval "$@"
  fi
}

require_non_empty() {
  local name="$1"
  local value="$2"
  if [[ -z "${value}" ]]; then
    printf 'missing required env: %s\n' "${name}" >&2
    exit 1
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'required command not found: %s\n' "$1" >&2
    exit 1
  }
}

detect_firewall_backend() {
  if command -v pfctl >/dev/null 2>&1; then
    echo "pfctl"
    return
  fi
  if command -v iptables >/dev/null 2>&1; then
    echo "iptables"
    return
  fi
  printf 'supported firewall backend not found (pfctl or iptables)\n' >&2
  exit 1
}

pf_rule() {
  local host="$1"
  local port="$2"
  echo "block drop proto tcp from any to ${host} port ${port}"
}

enable_firewall_rule() {
  local anchor="$1"
  local host="$2"
  local port="$3"
  local backend
  backend="$(detect_firewall_backend)"
  if [[ "${backend}" == "pfctl" ]]; then
    local anchor_file="/tmp/${anchor}.conf"
    printf '%s\n' "$(pf_rule "${host}" "${port}")" > "${anchor_file}"
    run "sudo pfctl -a ${anchor} -f ${anchor_file}"
    run "sudo pfctl -e"
    return
  fi
  run "sudo iptables -I OUTPUT -p tcp -d ${host} --dport ${port} -j DROP"
}

disable_firewall_rule() {
  local anchor="$1"
  local host="$2"
  local port="$3"
  local backend
  backend="$(detect_firewall_backend)"
  if [[ "${backend}" == "pfctl" ]]; then
    run "sudo pfctl -a ${anchor} -F rules"
    return
  fi
  run "sudo iptables -D OUTPUT -p tcp -d ${host} --dport ${port} -j DROP || true"
}

create_fill_file() {
  local path="$1"
  if command -v mkfile >/dev/null 2>&1; then
    run "mkfile ${FILL_MB}m ${path}"
    return
  fi
  if command -v fallocate >/dev/null 2>&1; then
    run "fallocate -l ${FILL_MB}M ${path}"
    return
  fi
  run "dd if=/dev/zero of=${path} bs=1m count=${FILL_MB}"
}

case "${SCENARIO}" in
  help|"")
    usage
    ;;
  wal-disk-fill)
    require_non_empty "DATA_DIR" "${DATA_DIR}"
    mkdir -p "${DATA_DIR}"
    create_fill_file "${DATA_DIR}/.chaos-disk-fill"
    ;;
  wal-disk-clean)
    require_non_empty "DATA_DIR" "${DATA_DIR}"
    run "rm -f ${DATA_DIR}/.chaos-disk-fill"
    ;;
  zk-disconnect)
    enable_firewall_rule "ullmatcher_zk" "${ZK_HOST}" "${ZK_PORT}"
    ;;
  zk-reconnect)
    disable_firewall_rule "ullmatcher_zk" "${ZK_HOST}" "${ZK_PORT}"
    ;;
  grpc-delay)
    require_command tc
    run "sudo tc qdisc replace dev ${NETWORK_IFACE} root netem delay ${LATENCY_MS}ms"
    ;;
  grpc-loss)
    require_command tc
    run "sudo tc qdisc replace dev ${NETWORK_IFACE} root netem loss ${LOSS_PERCENT}%"
    ;;
  netem-reset)
    require_command tc
    run "sudo tc qdisc del dev ${NETWORK_IFACE} root || true"
    ;;
  kill-primary)
    require_non_empty "PRIMARY_PID" "${PRIMARY_PID}"
    run "kill -9 ${PRIMARY_PID}"
    ;;
  *)
    printf 'unknown scenario: %s\n\n' "${SCENARIO}" >&2
    usage
    exit 1
    ;;
esac
