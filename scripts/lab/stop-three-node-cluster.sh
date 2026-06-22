#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/lab-logs}"
export LOG_ROOT

for node_id in node-a node-b node-c; do
  if [[ -f "$LOG_ROOT/$node_id.pid" ]]; then
    "$ROOT_DIR/scripts/lab/stop-node.sh" "$node_id"
  fi
done
