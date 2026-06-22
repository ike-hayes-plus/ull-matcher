#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"

SHARD_KEY="${SHARD_KEY:-merchant:42}"
SYMBOL_ID="${SYMBOL_ID:-1}"
CLUSTER_NAME="${CLUSTER_NAME:-merchant-lab}"
ZK_CONNECT="${ZK_CONNECT:-127.0.0.1:2181}"
DATA_ROOT="${DATA_ROOT:-$ROOT_DIR/target/lab}"
LOG_ROOT="${LOG_ROOT:-$ROOT_DIR/target/lab-logs}"
CP_FILE="$ROOT_DIR/target/lab/classpath.txt"
REPLICATION_TRANSPORT="${REPLICATION_TRANSPORT:-GRPC}"
ENABLE_TRANSPORT_TLS="${ENABLE_TRANSPORT_TLS:-false}"
TRANSPORT_TLS_DIR="${TRANSPORT_TLS_DIR:-$DATA_ROOT/tls}"
RESET_CLUSTER_STATE="${RESET_CLUSTER_STATE:-true}"

mkdir -p "$DATA_ROOT" "$LOG_ROOT"

if [[ "$RESET_CLUSTER_STATE" == "true" ]]; then
  rm -rf \
    "$DATA_ROOT/node-a" \
    "$DATA_ROOT/node-b" \
    "$DATA_ROOT/node-c" \
    "$LOG_ROOT/node-a.log" "$LOG_ROOT/node-a.pid" \
    "$LOG_ROOT/node-b.log" "$LOG_ROOT/node-b.pid" \
    "$LOG_ROOT/node-c.log" "$LOG_ROOT/node-c.pid"
fi

if [[ "$ENABLE_TRANSPORT_TLS" == "true" && ! -f "$TRANSPORT_TLS_DIR/node-a/tls.crt" ]]; then
  "$ROOT_DIR/scripts/lab/generate-transport-tls.sh" "$TRANSPORT_TLS_DIR" >/dev/null
fi

echo "[lab] building matcher-server runtime classpath"
(cd "$ROOT_DIR" && export JAVA_HOME="$JAVA_HOME" PATH="$JAVA_HOME/bin:$PATH" && \
  mvn -q -pl matcher-server -am -DskipTests package dependency:build-classpath \
    -Dmdep.outputFile="$CP_FILE" -Dmdep.pathSeparator=:)

export SHARD_KEY SYMBOL_ID CLUSTER_NAME ZK_CONNECT DATA_ROOT LOG_ROOT CP_FILE REPLICATION_TRANSPORT ENABLE_TRANSPORT_TLS TRANSPORT_TLS_DIR
"$ROOT_DIR/scripts/lab/start-node.sh" node-a "$LAB_HTTP_PORT_NODE_A" "$LAB_GRPC_PORT_NODE_A" "$LAB_AERON_PORT_NODE_A"
"$ROOT_DIR/scripts/lab/start-node.sh" node-b "$LAB_HTTP_PORT_NODE_B" "$LAB_GRPC_PORT_NODE_B" "$LAB_AERON_PORT_NODE_B"
"$ROOT_DIR/scripts/lab/start-node.sh" node-c "$LAB_HTTP_PORT_NODE_C" "$LAB_GRPC_PORT_NODE_C" "$LAB_AERON_PORT_NODE_C"

cat <<EOF
[lab] cluster started
[lab] validate with:
./scripts/chaos/lab.sh validate "$ROOT_DIR/target/chaos-lab" \
  http://127.0.0.1:${LAB_HTTP_PORT_NODE_A} http://127.0.0.1:${LAB_HTTP_PORT_NODE_B} http://127.0.0.1:${LAB_HTTP_PORT_NODE_C}
EOF
