#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"

case "${1:-}" in
  up)
    shift
    exec "${ROOT_DIR}/scripts/lab/start-three-node-cluster.sh" "$@"
    ;;
  down)
    shift
    exec "${ROOT_DIR}/scripts/lab/stop-three-node-cluster.sh" "$@"
    ;;
  validate)
    shift
    if [[ "${#}" -eq 0 ]]; then
      exec "${ROOT_DIR}/scripts/chaos/validate-chaos-lab.sh" "${ROOT_DIR}/target/chaos-lab" \
        "http://127.0.0.1:${LAB_HTTP_PORT_NODE_A}" "http://127.0.0.1:${LAB_HTTP_PORT_NODE_B}" "http://127.0.0.1:${LAB_HTTP_PORT_NODE_C}"
    fi
    exec "${ROOT_DIR}/scripts/chaos/validate-chaos-lab.sh" "$@"
    ;;
  failover-smoke)
    shift
    exec "${ROOT_DIR}/scripts/lab/run-failover-smoke.sh" "$@"
    ;;
  transport-compare)
    shift
    exec "${ROOT_DIR}/scripts/lab/run-transport-compare.sh" "$@"
    ;;
  *)
    cat <<'EOF' >&2
usage:
  ./scripts/chaos/cluster.sh up
  ./scripts/chaos/cluster.sh down
  ./scripts/chaos/cluster.sh validate [out-dir] [node-base-url...]
  ./scripts/chaos/cluster.sh failover-smoke
  ./scripts/chaos/cluster.sh transport-compare
EOF
    exit 1
    ;;
esac
