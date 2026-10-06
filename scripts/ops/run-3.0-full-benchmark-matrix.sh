#!/usr/bin/env bash
# Full 3.0 benchmark matrix → JSON under OUT_DIR (same scenarios as doc/operations/benchmark-baseline.md).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"

OUT_DIR="${OUT_DIR:-${ROOT_DIR}/target/benchmark/3.0-full}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-45}"
mkdir -p "${OUT_DIR}"

echo "[3.0-bench] java=$("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
echo "[3.0-bench] out=${OUT_DIR} start=$(date +%Y-%m-%dT%H:%M:%S%z)"

"${ROOT_DIR}/mvnw" -q -pl matcher-examples -am package -DskipTests -Djacoco.skip=true

DEP_CP="$("${ROOT_DIR}/mvnw" -q -pl matcher-examples dependency:build-classpath -Dmdep.outputFile=/dev/stdout -DincludeScope=runtime | tail -n 1)"
MODULE_CP="${ROOT_DIR}/matcher-examples/target/classes:${ROOT_DIR}/matcher-server/target/classes:${ROOT_DIR}/matcher-core/target/classes:${ROOT_DIR}/matcher-storage/target/classes:${ROOT_DIR}/matcher-runtime/target/classes:${ROOT_DIR}/matcher-ha/target/classes:${ROOT_DIR}/matcher-ha-grpc/target/classes:${ROOT_DIR}/matcher-ha-aeron/target/classes:${ROOT_DIR}/matcher-ha-zookeeper/target/classes:${ROOT_DIR}/matcher-ha-etcd/target/classes:${ROOT_DIR}/matcher-discovery-zookeeper/target/classes:${ROOT_DIR}/matcher-net/target/classes"
CP="${MODULE_CP}:${DEP_CP}"

run_jvm() {
  local report="$1"
  shift
  local log="${report%.json}.log"
  java -cp "${CP}" "$@" > "${report}" 2> "${log}"
}

echo "=== core-only ==="
run_jvm "${OUT_DIR}/core-only.json" io.github.ike.ullmatcher.example.CoreOnlyCrossingBenchmark \
  --restingOrders=2048 --crossingOrders=1000000 --warmupOrders=200000

echo "=== embed-journaled-core (2048/2048) ==="
run_jvm "${OUT_DIR}/embed-journaled-core.json" io.github.ike.ullmatcher.example.EmbeddedCrossingBenchmark \
  --resting-orders 2048 --crossing-orders 2048 --concurrency 24 \
  --wal-dir "${OUT_DIR}/wal-embed"

echo "=== single-node-http (2048/2048) ==="
run_jvm "${OUT_DIR}/single-node-http.json" io.github.ike.ullmatcher.example.SingleNodeServerCrossingBenchmark \
  --resting-orders 2048 --crossing-orders 2048 --concurrency 24 --batch-size 1 \
  --data-root "${OUT_DIR}/data-single-node-http"

echo "=== single-node-binary (2048/2048) ==="
run_jvm "${OUT_DIR}/single-node-binary.json" io.github.ike.ullmatcher.example.BinaryIngressCrossingBenchmark \
  --resting-orders 2048 --crossing-orders 2048 --concurrency 24 --batch-size 64 \
  --data-root "${OUT_DIR}/data-single-node-binary"

echo "=== HA matrix (32768 window) ==="
OUT_DIR="${OUT_DIR}" COOLDOWN_SECONDS="${COOLDOWN_SECONDS}" \
  "${ROOT_DIR}/scripts/ops/run-ha-benchmark-suite.sh"

echo "[3.0-bench] JSON reports: ${OUT_DIR} (update doc/operations/benchmark-baseline.md via validate or manual)"
echo "[3.0-bench] done $(date +%Y-%m-%dT%H:%M:%S%z)"
