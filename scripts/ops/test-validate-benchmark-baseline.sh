#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ull-baseline-test.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

DOC_FILE="${TMP_DIR}/benchmark-baseline-current.md"
REPORT_ROOT="${TMP_DIR}/current"

python3 - <<'PY' "${DOC_FILE}" "${REPORT_ROOT}"
import json
import sys
from pathlib import Path

doc = Path(sys.argv[1])
root = Path(sys.argv[2])

rows = [
    ("Core-only matcher", "core-only/report-clean.json", "acceptedOrdersPerSecond", None, None, None, "p99LatencyMicros", 1000.0, None, None, None, 1.0),
    ("本地持久化服务路径", "embed-journaled-core/report-clean.json", "acceptedOrdersPerSecond", "tradeEventsPerSecond", None, None, None, 1000.0, 1000.0, None, None, None),
    ("Single-node HTTP", "single-node-http/report-clean.json", "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", None, "p99LatencyMs", 1000.0, 1000.0, 1000.0, None, 1.0),
    ("Single-node binary", "single-node-binary/report-clean.json", "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", None, "p99LatencyMs", 1000.0, 1000.0, 1000.0, None, 1.0),
    ("External `1P1S` REST + `GRPC`", "http-ha/grpc-1p1s-current.json", "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
    ("External `1P1S` REST + `AERON`", "http-ha/aeron-1p1s-current.json", "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
    ("REST committed single", "http-ha/rest-single-1024-current.json", "acceptedOrdersPerSecond", None, "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, None, 1000.0, 1.0, 1.0),
    ("REST committed batch", "http-ha/rest-batch-1024-current.json", "acceptedOrdersPerSecond", None, "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, None, 1000.0, 1.0, 1.0),
    ("External `1P1S` binary + `GRPC`", "binary-commit/grpc-1p1s-current.json", "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
    ("External `1P1S` binary + `AERON`", "binary-commit/aeron-1p1s-current.json", "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
    ("External `1P2S` binary + `GRPC` quorum", "binary-commit/grpc-1p2s-quorum-current.json", "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
    ("External `1P2S` binary + `AERON` quorum", "binary-commit/aeron-1p2s-quorum-current.json", "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
    ("External `1P3S` binary + `GRPC` quorum", "binary-commit/grpc-1p3s-quorum-current.json", "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
    ("External `1P3S` binary + `AERON` quorum", "binary-commit/aeron-1p3s-quorum-current.json", "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond", "commitCatchupSeconds", "latency.p99Ms", 1000.0, 1000.0, 1000.0, 1.0, 1.0),
]

doc.write_text(
    "| 场景 | 入口 | 复制 | 口径 | Accepted orders/s | Trade events/s | Committed submissions/s | Catch-up | p99 延迟 |\n"
    "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |\n" +
    "\n".join(
        f"| {name} | x | x | x | `{accepted:,.2f}` | "
        f"{'`' + format(trade, ',.2f') + '`' if trade is not None else '`N/A`'} | "
        f"{'`' + format(committed, ',.2f') + '`' if committed is not None else '`N/A`'} | "
        f"{'`' + format(catch_up, '.4f') + ' s`' if catch_up is not None else '`N/A`'} | "
        f"{'`' + format(p99, '.2f') + (' us' if name == 'Core-only matcher' else ' ms') + '`' if p99 is not None else '`N/A`'} |"
        for name, _path, _accepted_key, _trade_key, _committed_key, _catch_key, _p99_key,
            accepted, trade, committed, catch_up, p99 in rows
    ) + "\n",
    encoding="utf-8",
)

for name, path, accepted_key, trade_key, committed_key, catch_key, p99_key, accepted, trade, committed, catch_up, p99 in rows:
    payload = {"success": True}
    for key, value in [
        (accepted_key, accepted),
        (trade_key, trade),
        (committed_key, committed),
        (catch_key, catch_up),
        (p99_key, p99),
    ]:
        if key is None or value is None:
            continue
        target = payload
        parts = key.split(".")
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value
    report = root / path
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(payload) + "\n", encoding="utf-8")
PY

"${ROOT_DIR}/scripts/ops/validate-benchmark-baseline.py" \
  --doc "${DOC_FILE}" \
  --report-root "${REPORT_ROOT}" >/dev/null

python3 - <<'PY' "${REPORT_ROOT}/single-node-http/report-clean.json"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
payload["acceptedOrdersPerSecond"] = 900.0
path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
PY

if "${ROOT_DIR}/scripts/ops/validate-benchmark-baseline.py" \
  --doc "${DOC_FILE}" \
  --report-root "${REPORT_ROOT}" >/dev/null 2>&1; then
  echo "benchmark baseline validation unexpectedly passed" >&2
  exit 1
fi

echo "benchmark baseline validation self-test passed"
