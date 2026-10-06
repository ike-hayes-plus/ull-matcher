#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${REPORT_DIR:-$ROOT_DIR/target/benchmark/http-sweep}"
RESTING_ORDERS="${RESTING_ORDERS:-2048}"
CROSSING_ORDERS="${CROSSING_ORDERS:-2048}"
CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-8,16,24,32,48,64,96,128,192,256,384,512}"
MAIN_CLASS="io.github.ike.ullmatcher.example.SingleNodeServerCrossingBenchmark"

mkdir -p "$REPORT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/use-project-java.sh"

echo "[http-sweep] building matcher-examples (skip tests)"
(
  cd "$ROOT_DIR"
  ./mvnw -q -pl matcher-examples -am install -DskipTests
)
CP_FILE="$(mktemp)"
(
  cd "$ROOT_DIR"
  ./mvnw -q -pl matcher-examples dependency:build-classpath \
    -Dmdep.pathSeparator=: \
    -Dmdep.outputFile="$CP_FILE" \
    -DincludeScope=runtime
)
RUNTIME_CP="$ROOT_DIR/matcher-examples/target/classes:$(tr -d '\n' < "$CP_FILE")"
rm -f "$CP_FILE"

best_rate=0
best_concurrency=0
plateau=false

IFS=',' read -ra LEVELS <<< "$CONCURRENCY_LEVELS"
for concurrency in "${LEVELS[@]}"; do
  echo "[http-sweep] concurrency=$concurrency"
  out="$REPORT_DIR/concurrency-${concurrency}.json"
  "$JAVA_HOME/bin/java" -cp "$RUNTIME_CP" "$MAIN_CLASS" \
    --resting-orders "$RESTING_ORDERS" \
    --crossing-orders "$CROSSING_ORDERS" \
    --concurrency "$concurrency" \
    > "$out"
  rate="$(python3 - <<'PY' "$out"
import json, re, sys
text = open(sys.argv[1]).read()
# benchmark prints JSON object; extract acceptedOrdersPerSecond
m = re.search(r'"acceptedOrdersPerSecond":\s*([0-9.]+)', text)
print(m.group(1) if m else "0")
PY
)"
  echo "[http-sweep] concurrency=$concurrency acceptedOrdersPerSecond=$rate"
  if python3 - <<'PY' "$rate" "$best_rate"
import sys
cur = float(sys.argv[1])
best = float(sys.argv[2])
if best <= 0.0:
    sys.exit(0)
sys.exit(0 if cur >= best * 0.98 else 1)
PY
  then
    if python3 - <<'PY' "$rate" "$best_rate"
import sys
sys.exit(0 if float(sys.argv[1]) > float(sys.argv[2]) else 1)
PY
    then
      best_rate="$rate"
      best_concurrency="$concurrency"
    fi
  else
    plateau=true
    echo "[http-sweep] inflection: throughput dropped below 98% of best ($best_rate at concurrency=$best_concurrency)"
    break
  fi
done

summary="$REPORT_DIR/summary.json"
python3 - <<'PY' "$summary" "$best_concurrency" "$best_rate" "$plateau"
import json, sys
path, best_c, best_r, plateau = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), sys.argv[4] == "true"
json.dump({
  "bestConcurrency": best_c,
  "bestAcceptedOrdersPerSecond": best_r,
  "stoppedEarlyAtPlateau": plateau,
}, open(path, "w"), indent=2)
print(f"[http-sweep] summary written to {path}")
PY
