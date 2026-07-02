#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

MODE_LABEL="lab"
: "${SERVER_MODE_VALUE:=DEV}"
: "${SHARD_KEY:=merchant:42}"
: "${SYMBOL_ID:=1}"
: "${CLUSTER_NAME:=merchant-lab}"
: "${ZK_CONNECT:=127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183}"
: "${ETCD_ENDPOINT:=http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383}"
: "${DATA_ROOT:=$ROOT_DIR/target/lab}"
: "${LOG_ROOT:=$ROOT_DIR/target/lab-logs}"
: "${CP_FILE:=$ROOT_DIR/target/lab/classpath.txt}"
: "${TRANSPORT_TLS_DIR:=$DATA_ROOT/tls}"
: "${CP_BUILD_MAVEN_ARGS:= -q -pl matcher-server -am -DskipTests package dependency:build-classpath}"

# shellcheck source=/dev/null
source "${ROOT_DIR}/scripts/deploy/default.conf"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"
JAVA_BIN="${JAVA_HOME}/bin/java"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/node-process.sh"

start_matcher_node "$@"
