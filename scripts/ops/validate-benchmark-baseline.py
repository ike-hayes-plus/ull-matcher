#!/usr/bin/env python3
"""Validate current benchmark reports against the published baseline table."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DOC = ROOT / "doc/operations/benchmark-baseline-current.md"
DEFAULT_REPORT_ROOT = ROOT / "target/current"


@dataclass(frozen=True)
class Scenario:
    name: str
    report: str
    accepted_key: str | None
    trade_key: str | None
    committed_key: str | None
    catch_up_key: str | None
    p99_key: str | None
    p99_unit: str | None


SCENARIOS = [
    Scenario("Core-only matcher", "core-only/report-clean.json",
             "acceptedOrdersPerSecond", None, None, None, "p99LatencyMicros", "us"),
    Scenario("本地持久化服务路径", "embed-journaled-core/report-clean.json",
             "acceptedOrdersPerSecond", "tradeEventsPerSecond", None, None, None, None),
    Scenario("Single-node HTTP", "single-node-http/report-clean.json",
             "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             None, "p99LatencyMs", "ms"),
    Scenario("Single-node binary", "single-node-binary/report-clean.json",
             "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             None, "p99LatencyMs", "ms"),
    Scenario("External `1P1S` REST + `GRPC`", "http-ha/grpc-1p1s-current.json",
             "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("External `1P1S` REST + `AERON`", "http-ha/aeron-1p1s-current.json",
             "acceptedOrdersPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("REST committed single", "http-ha/rest-single-1024-current.json",
             "acceptedOrdersPerSecond", None, "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("REST committed batch", "http-ha/rest-batch-1024-current.json",
             "acceptedOrdersPerSecond", None, "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("External `1P1S` binary + `GRPC`", "binary-commit/grpc-1p1s-current.json",
             "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("External `1P1S` binary + `AERON`", "binary-commit/aeron-1p1s-current.json",
             "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("External `1P2S` binary + `GRPC` quorum", "binary-commit/grpc-1p2s-quorum-current.json",
             "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("External `1P2S` binary + `AERON` quorum", "binary-commit/aeron-1p2s-quorum-current.json",
             "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("External `1P3S` binary + `GRPC` quorum", "binary-commit/grpc-1p3s-quorum-current.json",
             "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
    Scenario("External `1P3S` binary + `AERON` quorum", "binary-commit/aeron-1p3s-quorum-current.json",
             "acceptedCommandsPerSecond", "tradeEventsPerSecond", "replicationCommittedSubmissionsPerSecond",
             "commitCatchupSeconds", "latency.p99Ms", "ms"),
]


def load_json(path: Path) -> dict:
    raw = path.read_text(encoding="utf-8")
    offset = raw.find("{")
    if offset < 0:
        raise ValueError(f"{path}: no JSON object found")
    payload = json.loads(raw[offset:])
    if payload.get("success") is False:
        raise ValueError(f"{path}: benchmark success=false")
    return payload


def parse_number(text: str) -> tuple[float, str | None] | None:
    cleaned = text.strip().replace("`", "")
    if cleaned == "N/A":
        return None
    match = re.fullmatch(r"([0-9][0-9,]*(?:\.[0-9]+)?)\s*([a-zA-Z/]+)?", cleaned)
    if match is None:
        raise ValueError(f"cannot parse numeric cell: {text}")
    return float(match.group(1).replace(",", "")), match.group(2)


def baseline_rows(doc: Path) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for line in doc.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped.startswith("| ") or stripped.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not cells or cells[0] == "场景":
            continue
        rows[cells[0]] = cells
    return rows


def value_at(payload: dict, key: str | None) -> float | None:
    if key is None:
        return None
    current: object = payload
    for part in key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    if current is None:
        return None
    return float(current)


def compare_at_least(actual: float, expected: float, tolerance: float) -> bool:
    return actual + max(1.0, expected) * tolerance >= expected


def compare_at_most(actual: float, expected: float, tolerance: float) -> bool:
    return actual <= expected + max(1.0, expected) * tolerance


def check_metric(failures: list[str],
                 scenario: str,
                 metric: str,
                 actual: float | None,
                 expected_cell: str | None,
                 comparator: Callable[[float, float, float], bool],
                 tolerance: float) -> None:
    if expected_cell is None:
        return
    expected = parse_number(expected_cell)
    if expected is None:
        return
    if actual is None:
        failures.append(f"{scenario}: missing report metric for {metric}")
        return
    expected_value, _ = expected
    if not comparator(actual, expected_value, tolerance):
        failures.append(f"{scenario}: {metric} actual={actual:.6f}, baseline={expected_value:.6f}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc", type=Path, default=DEFAULT_DOC)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--throughput-tolerance", type=float, default=0.005,
                        help="relative tolerance for throughput metrics below the documented value")
    parser.add_argument("--latency-tolerance", type=float, default=0.05,
                        help="relative tolerance for catch-up and p99 metrics above the documented value")
    args = parser.parse_args()

    rows = baseline_rows(args.doc)
    failures: list[str] = []
    checked = 0

    for scenario in SCENARIOS:
        cells = rows.get(scenario.name)
        if cells is None:
            failures.append(f"{scenario.name}: missing baseline row")
            continue
        if len(cells) >= 10:
            accepted_cell = cells[5]
            trade_cell = cells[6]
            committed_cell = cells[7]
            catch_up_cell = cells[8]
            p99_cell = cells[9]
        else:
            accepted_cell = cells[2] if len(cells) == 6 else cells[4]
            trade_cell = None if len(cells) == 6 else cells[5]
            committed_cell = cells[3] if len(cells) == 6 else cells[6]
            catch_up_cell = cells[5] if len(cells) == 6 else cells[7]
            p99_cell = cells[4] if len(cells) == 6 else cells[8]
        report_path = args.report_root / scenario.report
        if not report_path.exists():
            failures.append(f"{scenario.name}: missing report {report_path}")
            continue
        try:
            payload = load_json(report_path)
        except Exception as exc:  # noqa: BLE001 - CLI should surface exact input failure.
            failures.append(str(exc))
            continue

        check_metric(failures, scenario.name, "accepted/s", value_at(payload, scenario.accepted_key),
                     accepted_cell, compare_at_least, args.throughput_tolerance)
        check_metric(failures, scenario.name, "trade events/s", value_at(payload, scenario.trade_key),
                     trade_cell, compare_at_least, args.throughput_tolerance)
        check_metric(failures, scenario.name, "committed/s", value_at(payload, scenario.committed_key),
                     committed_cell, compare_at_least, args.throughput_tolerance)
        check_metric(failures, scenario.name, "catch-up", value_at(payload, scenario.catch_up_key),
                     catch_up_cell, compare_at_most, args.latency_tolerance)
        check_metric(failures, scenario.name, "p99", value_at(payload, scenario.p99_key),
                     p99_cell, compare_at_most, args.latency_tolerance)
        checked += 1

    if failures:
        print("benchmark baseline validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print(f"benchmark baseline validation passed: {checked} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
