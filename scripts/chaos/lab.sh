#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

case "${1:-}" in
  up)
    shift
    exec "${ROOT_DIR}/scripts/chaos/start-chaos-lab.sh" "$@"
    ;;
  down)
    shift
    exec "${ROOT_DIR}/scripts/chaos/stop-chaos-lab.sh" "$@"
    ;;
  validate)
    shift
    exec "${ROOT_DIR}/scripts/chaos/validate-chaos-lab.sh" "$@"
    ;;
  *)
    cat <<'EOF' >&2
usage:
  ./scripts/chaos/lab.sh up
  ./scripts/chaos/lab.sh down
  ./scripts/chaos/lab.sh validate [out-dir] [node-base-url...]
EOF
    exit 1
    ;;
esac
