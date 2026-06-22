#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/node-process.sh"

LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/lab-logs}"
MODE_LABEL="lab"

node_ports() {
  case "$1" in
    node-a) echo "${LAB_HTTP_PORT_NODE_A} ${LAB_GRPC_PORT_NODE_A} ${LAB_BINARY_PORT_NODE_A}" ;;
    node-b) echo "${LAB_HTTP_PORT_NODE_B} ${LAB_GRPC_PORT_NODE_B} ${LAB_BINARY_PORT_NODE_B}" ;;
    node-c) echo "${LAB_HTTP_PORT_NODE_C} ${LAB_GRPC_PORT_NODE_C} ${LAB_BINARY_PORT_NODE_C}" ;;
    node-d) echo "${LAB_HTTP_PORT_NODE_D} ${LAB_GRPC_PORT_NODE_D} ${LAB_BINARY_PORT_NODE_D}" ;;
    *) return 1 ;;
  esac
}

PORT_RESOLVER=node_ports
stop_matcher_node "$@"
