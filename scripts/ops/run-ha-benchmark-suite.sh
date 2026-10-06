#!/usr/bin/env bash
# Run the published HA matrix (6 binary + 2 REST) on Temurin 25 (.sdkmanrc).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/benchmark/3.0-full}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-45}"
LOG="$OUT_DIR/run.log"

usage() {
  cat <<'USAGE'
Usage:
  COOLDOWN_SECONDS=45 scripts/ops/run-ha-benchmark-suite.sh

Environment:
  OUT_DIR             JSON reports and run.log (default: target/benchmark/3.0-full)
  COOLDOWN_SECONDS    Pause between binary scenarios (default: 45)

Requires JDK 25+ (see .sdkmanrc). Matcher nodes and benchmarks use JAVA_HOME from use-project-java.sh.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

# shellcheck source=/dev/null
source "$ROOT_DIR/scripts/lib/use-project-java.sh"

# A previous lab/REST invocation in the same shell must not pin replication mode,
# data dirs, or ZooKeeper cluster identity for this matrix.
unset CLUSTER_NAME DATA_ROOT LOG_ROOT REPLICATION_MODE REPLICATION_TRANSPORT LEASE_PROVIDER DISCOVERY_PROVIDER

mkdir -p "$OUT_DIR"
: > "$LOG"

echo "suite java=$("$JAVA_HOME/bin/java" -version 2>&1 | head -1)" | tee -a "$LOG"
echo "suite start=$(date +%Y-%m-%dT%H:%M:%S%z) load=$(sysctl -n vm.loadavg)" | tee -a "$LOG"

run_binary() {
  local name="$1" transport="$2" standbys="$3" mode="$4"
  unset CLUSTER_NAME DATA_ROOT LOG_ROOT REPLICATION_MODE REPLICATION_TRANSPORT
  echo "=== BEGIN $name $(date +%Y-%m-%dT%H:%M:%S%z) load=$(sysctl -n vm.loadavg) commit=$mode ===" | tee -a "$LOG"
  scripts/lab/run-binary-ingress-benchmark.sh \
    --mode replication-commit \
    --transport "$transport" \
    --standbys "$standbys" \
    --standby-commit-mode "$mode" \
    --resting-orders 32768 \
    --crossing-orders 32768 \
    --concurrency 24 \
    --batch-size 64 \
    --report "$OUT_DIR/${name}.json"
  echo "=== END $name load=$(sysctl -n vm.loadavg) ===" | tee -a "$LOG"
  sleep "$COOLDOWN_SECONDS"
}

run_binary grpc-1p1s GRPC 1 any
run_binary aeron-1p1s AERON 1 any
run_binary grpc-1p2s GRPC 2 quorum
run_binary aeron-1p2s AERON 2 quorum
run_binary grpc-1p3s GRPC 3 quorum
run_binary aeron-1p3s AERON 3 quorum

export SHARD_KEY=merchant:42 SYMBOL_ID=1
export CLUSTER_NAME="rest-suite-$(date +%Y%m%d%H%M%S)-$$"
export ZK_CONNECT="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
export ETCD_ENDPOINT="http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383"
export DATA_ROOT="$ROOT_DIR/target/rest-bench-lab"
export LOG_ROOT="$ROOT_DIR/target/rest-bench-logs"
export REPLICATION_MODE=WAIT_FOR_ANY_STANDBY REPLICATION_TRANSPORT=GRPC
export LEASE_PROVIDER=zk DISCOVERY_PROVIDER=zk
# BENCH 1s force delay makes REST single-order preload wait ~1s/order.
export WAL_FORCE_MAX_DELAY_MICROS=500
export WAL_FORCE_BATCH_SIZE=32

cleanup_rest() {
  export LOG_ROOT
  scripts/lab/stop-node.sh node-a >/dev/null 2>&1 || true
  scripts/lab/stop-node.sh node-b >/dev/null 2>&1 || true
}
trap cleanup_rest EXIT

rm -rf "$DATA_ROOT" "$LOG_ROOT"
mkdir -p "$DATA_ROOT" "$LOG_ROOT"

if ! lsof -iTCP:2181 -sTCP:LISTEN >/dev/null 2>&1; then
  "$ROOT_DIR/scripts/chaos/lab.sh" up >/dev/null
fi

scripts/lab/start-node.sh node-a 8080 9190 15090 10080
scripts/lab/start-node.sh node-b 8081 9191 15091 10081

PRIMARY_JSON="$(python3 - <<'PY'
import json, time, urllib.request
ports = [8080, 8081]
deadline = time.time() + 90
stable = None
rounds = 0
while time.time() < deadline:
    try:
        payloads = []
        for port in ports:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/v1/runtime/health", timeout=2) as response:
                payloads.append(json.loads(response.read().decode()))
    except Exception:
        stable = None
        rounds = 0
        time.sleep(0.2)
        continue
    accepting = [
        port for port, payload in zip(ports, payloads)
        if payload.get("acceptingClientCommands") and payload.get("role") == "PRIMARY"
    ]
    if len(accepting) == 1:
        port = accepting[0]
        if stable == port:
            rounds += 1
        else:
            stable = port
            rounds = 1
        if rounds >= 8:
            standby = 8081 if port == 8080 else 8080
            print(json.dumps({"primary": f"http://127.0.0.1:{port}", "standby": f"http://127.0.0.1:{standby}"}))
            raise SystemExit(0)
    else:
        stable = None
        rounds = 0
    time.sleep(0.2)
raise SystemExit("cluster did not stabilize")
PY
)"
PRIMARY="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["primary"])' "$PRIMARY_JSON")"
STANDBY="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["standby"])' "$PRIMARY_JSON")"

run_rest() {
  local name="$1" ack="$2"
  echo "=== BEGIN $name load=$(sysctl -n vm.loadavg) ===" | tee -a "$LOG"
  scripts/lab/run-rest-commit-benchmark.sh \
    --base-url "$PRIMARY" \
    --standby-base-url "$STANDBY" \
    --resting-orders 2048 \
    --concurrency 64 \
    --ack-mode "$ack" \
    --http-submit-mode single \
    --report "$OUT_DIR/${name}.json"
  echo "=== END $name ===" | tee -a "$LOG"
}

run_rest rest-1p1s-local local
run_rest rest-1p1s-committed committed

"$ROOT_DIR/scripts/chaos/lab.sh" down >/dev/null 2>&1 || true
echo "SUITE_DONE $(date +%Y-%m-%dT%H:%M:%S%z) load=$(sysctl -n vm.loadavg)" | tee -a "$LOG"
