#!/usr/bin/env bash
#
# Run the benchmarks that need no cluster and check them against the CI floor.
#
# This is a regression gate, not a performance measurement. Shared CI runners
# are far slower and far noisier than the reference machine in
# doc/operations/benchmark-baseline.md, so the floors in
# doc/operations/benchmark-ci-floor.json are deliberately an order of magnitude
# below the published numbers. The gate catches the failure mode that actually
# matters in review: someone puts a lock, an allocation or a syscall on the hot
# path and throughput collapses. It will not catch a 10% drift.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"

REPORT_DIR="${REPORT_DIR:-${ROOT_DIR}/target/benchmark-regression}"
FLOOR_FILE="${FLOOR_FILE:-${ROOT_DIR}/doc/operations/benchmark-ci-floor.json}"
EXAMPLES_JAR_DIR="${ROOT_DIR}/matcher-examples/target"

mkdir -p "${REPORT_DIR}"

if [[ "${SKIP_BUILD:-false}" != "true" ]]; then
  "${ROOT_DIR}/mvnw" --batch-mode --no-transfer-progress \
    -pl matcher-examples -am package -DskipTests -Djacoco.skip=true
fi

CLASSPATH="$(find "${EXAMPLES_JAR_DIR}" -maxdepth 1 -name 'ull-matcher-examples-*.jar' \
  ! -name '*-sources.jar' ! -name '*-javadoc.jar' | head -n 1)"
if [[ -z "${CLASSPATH}" ]]; then
  echo "[benchmark-regression] examples jar not found under ${EXAMPLES_JAR_DIR}" >&2
  exit 2
fi
CLASSPATH="${CLASSPATH}:$("${ROOT_DIR}/mvnw" --batch-mode --no-transfer-progress -q \
  -pl matcher-examples dependency:build-classpath -Dmdep.outputFile=/dev/stdout -DincludeScope=runtime \
  | tail -n 1)"

run_benchmark() {
  local name="$1"
  local main_class="$2"
  shift 2
  echo "[benchmark-regression] running ${name}"
  java -cp "${CLASSPATH}" "${main_class}" "$@" > "${REPORT_DIR}/${name}.json"
}

run_benchmark core-only \
  io.github.ike.ullmatcher.example.CoreOnlyCrossingBenchmark \
  --restingOrders=2048 --crossingOrders=1000000 --warmupOrders=200000

run_benchmark embed-journaled-core \
  io.github.ike.ullmatcher.example.EmbeddedCrossingBenchmark \
  --resting-orders 2048 --crossing-orders 2048 \
  --wal-dir "${REPORT_DIR}/wal"

"${ROOT_DIR}/scripts/ops/check-benchmark-regression.py" \
  --report-dir "${REPORT_DIR}" \
  --floor-file "${FLOOR_FILE}"
