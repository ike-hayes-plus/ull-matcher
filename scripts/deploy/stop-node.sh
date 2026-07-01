#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/scripts/deploy/default.conf"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/node-process.sh"

MODE_LABEL="deploy"
stop_matcher_node "$@"
