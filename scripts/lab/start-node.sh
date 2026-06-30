#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"
JAVA_BIN="${JAVA_HOME}/bin/java"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/node-process.sh"

MODE_LABEL="lab"
SERVER_MODE_VALUE="${SERVER_MODE_VALUE:-DEV}"
SHARD_KEY="${SHARD_KEY:-merchant:42}"
SYMBOL_ID="${SYMBOL_ID:-1}"
CLUSTER_NAME="${CLUSTER_NAME:-merchant-lab}"
ZK_CONNECT="${ZK_CONNECT:-127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183}"
DATA_ROOT="${DATA_ROOT:-$ROOT_DIR/target/lab}"
LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/lab-logs}"
CP_FILE="${CP_FILE:-$ROOT_DIR/target/lab/classpath.txt}"
TRANSPORT_TLS_DIR="${TRANSPORT_TLS_DIR:-$DATA_ROOT/tls}"
CP_BUILD_MAVEN_ARGS="${CP_BUILD_MAVEN_ARGS:- -q -pl matcher-server -am -DskipTests package dependency:build-classpath}"

start_matcher_node "$@"
