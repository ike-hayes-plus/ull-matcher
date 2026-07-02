#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

PYTHONPATH="${ROOT_DIR}/scripts/bench" python3 - <<'PY'
from replication_metrics import all_standbys_watermark
from replication_metrics import mode_ready
from replication_metrics import mode_watermark
from replication_metrics import required_standby_acks
from replication_metrics import watermark_delta

payloads = [
    {"lastDurableSequence": 4352, "lastAppliedSequence": 4352},
    {"lastDurableSequence": 4097, "lastAppliedSequence": 4097},
    {"lastDurableSequence": 4352, "lastAppliedSequence": 4352},
]

assert required_standby_acks("any", 3) == 1
assert required_standby_acks("quorum", 3) == 2
assert required_standby_acks("all", 3) == 3
assert mode_watermark("any", payloads) == 4352
assert mode_watermark("quorum", payloads) == 4352
assert mode_watermark("all", payloads) == 4097
assert all_standbys_watermark(payloads) == 4097
assert mode_ready("quorum", payloads, 4352)
assert not mode_ready("all", payloads, 4352)
assert watermark_delta(2304, 4352) == 2048

print("benchmark replication metrics self-test passed")
PY
