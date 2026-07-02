#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/scripts/deploy/default.conf"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"
JAVA_BIN="${JAVA_HOME}/bin/java"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/node-process.sh"

MODE_LABEL="deploy"

mkdir -p "$ROOT_DIR/var/run"

start_matcher_node "$@"
