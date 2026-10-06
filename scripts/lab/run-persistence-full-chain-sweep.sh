#!/usr/bin/env bash
# Full-chain persistence observation (embed -> binary single-node -> HA binary replication-commit).
# Lab uses SERVER_MODE_VALUE=DEV; WAL/snapshot knobs match PersistenceProfile presets.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"

REPORT_DIR="${REPORT_DIR:-${ROOT_DIR}/target/persistence-full-chain-sweep}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-20}"
RESTING="${RESTING_ORDERS:-2048}"
CROSSING="${CROSSING_ORDERS:-2048}"
CONCURRENCY="${CONCURRENCY:-24}"
BATCH_SIZE="${BATCH_SIZE:-64}"
# Set FULL_HA_MATRIX=true to run 1P2S/1P3S and AERON (long run).
FULL_HA_MATRIX="${FULL_HA_MATRIX:-false}"

mkdir -p "${REPORT_DIR}"

"${ROOT_DIR}/mvnw" -q -pl matcher-examples -am package -DskipTests -Djacoco.skip=true

DEP_CP="$("${ROOT_DIR}/mvnw" -q -pl matcher-examples dependency:build-classpath -Dmdep.outputFile=/dev/stdout -DincludeScope=runtime | tail -n 1)"
MODULE_CP="${ROOT_DIR}/matcher-examples/target/classes:${ROOT_DIR}/matcher-server/target/classes:${ROOT_DIR}/matcher-core/target/classes:${ROOT_DIR}/matcher-storage/target/classes:${ROOT_DIR}/matcher-runtime/target/classes:${ROOT_DIR}/matcher-ha/target/classes:${ROOT_DIR}/matcher-ha-grpc/target/classes:${ROOT_DIR}/matcher-ha-aeron/target/classes:${ROOT_DIR}/matcher-ha-zookeeper/target/classes:${ROOT_DIR}/matcher-ha-etcd/target/classes:${ROOT_DIR}/matcher-discovery-zookeeper/target/classes:${ROOT_DIR}/matcher-net/target/classes"
CP="${MODULE_CP}:${DEP_CP}"

summarize_json() {
  local file="$1"
  rg -o '"scenario": "[^"]+"|"topology": "[^"]+"|"acceptedOrdersPerSecond": [0-9.]+|"replicationCommittedSubmissionsPerSecond": [0-9.]+|"processedCommandsPerSecond": [0-9.]+|"p99LatencyMs": [0-9.]+|"p99LatencyMicros": [0-9.]+' "$file" 2>/dev/null || true
}

apply_wal_preset() {
  local preset="$1"
  local max_delay_micros="${2:-}"
  case "$preset" in
    prod)
      export WAL_DURABILITY_MODE=SYNC_PER_COMMAND
      export WAL_FORCE_BATCH_SIZE=1
      export WAL_FORCE_MAX_DELAY_MICROS=0
      ;;
    bench)
      export WAL_DURABILITY_MODE=SYNC_PER_BATCH
      export WAL_FORCE_BATCH_SIZE=32
      if [[ -n "$max_delay_micros" ]]; then
        export WAL_FORCE_MAX_DELAY_MICROS="$max_delay_micros"
      else
        export WAL_FORCE_MAX_DELAY_MICROS=1000000
      fi
      ;;
    *)
      echo "unknown wal preset: $preset" >&2
      exit 2
      ;;
  esac
}

run_jvm_bench() {
  local report="$1"
  shift
  local log="${report%.json}.log"
  java -cp "${CP}" "$@" > "${report}" 2> "${log}"
}

run_embed() {
  local preset="$1"
  local snapshot_ms="$2"
  local name="embed-${preset}-snap${snapshot_ms}"
  echo "=== ${name} (journaled gateway + WAL + match loop) ==="
  run_jvm_bench "${REPORT_DIR}/${name}.json" io.github.ike.ullmatcher.example.EmbeddedCrossingBenchmark \
    --resting-orders "$RESTING" --crossing-orders "$CROSSING" --concurrency "$CONCURRENCY" \
    --durability-mode "$WAL_DURABILITY_MODE" \
    --force-batch-size "$WAL_FORCE_BATCH_SIZE" \
    --force-max-delay-micros "$WAL_FORCE_MAX_DELAY_MICROS" \
    --wal-dir "${REPORT_DIR}/${name}/wal"
  summarize_json "${REPORT_DIR}/${name}.json"
}

run_binary_inprocess() {
  local preset="$1"
  local snapshot_ms="$2"
  local name="binary-single-${preset}-snap${snapshot_ms}"
  echo "=== ${name} (binary ingress + MatcherNodeService + WAL) ==="
  run_jvm_bench "${REPORT_DIR}/${name}.json" io.github.ike.ullmatcher.example.BinaryIngressCrossingBenchmark \
    --resting-orders "$RESTING" --crossing-orders "$CROSSING" \
    --concurrency "$CONCURRENCY" --batch-size "$BATCH_SIZE" \
    --durability-mode "$WAL_DURABILITY_MODE" \
    --force-batch-size "$WAL_FORCE_BATCH_SIZE" \
    --force-max-delay-micros "$WAL_FORCE_MAX_DELAY_MICROS" \
    --snapshot-interval-millis "$snapshot_ms" \
    --data-root "${REPORT_DIR}/${name}"
  summarize_json "${REPORT_DIR}/${name}.json"
}

run_ha_binary() {
  local preset="$1"
  local snapshot_ms="$2"
  local transport="$3"
  local standbys="$4"
  local transport_tag
  transport_tag="$(printf '%s' "$transport" | tr '[:upper:]' '[:lower:]')"
  local name="ha-binary-${transport_tag}-1p${standbys}s-${preset}-snap${snapshot_ms}"
  apply_wal_preset "$preset"
  export SNAPSHOT_INTERVAL_MILLIS="$snapshot_ms"
  export SERVER_MODE_VALUE=DEV
  unset PERSISTENCE_PROFILE WAL_COLD_ARCHIVE_DIR
  # WAL_* from apply_wal_preset must survive start-node lab defaults.
  echo "=== ${name} (ZK + ${standbys} standby + ${transport} + binary replication-commit) ==="
  "${ROOT_DIR}/scripts/lab/run-binary-ingress-benchmark.sh" \
    --mode replication-commit \
    --transport "$transport" \
    --standbys "$standbys" \
    --standby-commit-mode any \
    --resting-orders "$RESTING" \
    --crossing-orders "$CROSSING" \
    --concurrency "$CONCURRENCY" \
    --batch-size "$BATCH_SIZE" \
    --report "${REPORT_DIR}/${name}.json"
  summarize_json "${REPORT_DIR}/${name}.json"
  sleep "$COOLDOWN_SECONDS"
}

for preset in bench prod; do
  if [[ "$preset" == "bench" ]]; then
    apply_wal_preset bench 500
  else
    apply_wal_preset prod
  fi
  run_embed "$preset" 0
  run_binary_inprocess "$preset" 0
done

# Snapshot stress on prod-like WAL (in-process binary only; HA run once with 1s snapshot).
apply_wal_preset prod
run_binary_inprocess prod 1000

run_ha_binary prod 0 GRPC 1
apply_wal_preset bench
run_ha_binary bench 0 GRPC 1
run_ha_binary prod 1000 GRPC 1

if [[ "$FULL_HA_MATRIX" == "true" ]]; then
  run_ha_binary prod 0 GRPC 2
  run_ha_binary prod 0 AERON 1
fi

echo "[persistence-full-chain] reports: ${REPORT_DIR}"
echo "[persistence-full-chain] set FULL_HA_MATRIX=true for 1P2S + AERON scenarios"
