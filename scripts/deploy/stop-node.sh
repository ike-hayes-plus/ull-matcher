#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/node-process.sh"

LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/var/log}"
MODE_LABEL="deploy"
stop_matcher_node "$@"
