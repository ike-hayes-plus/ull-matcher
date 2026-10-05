#!/usr/bin/env python3
"""Fail the build when a no-cluster benchmark falls below the CI floor.

The floors live in doc/operations/benchmark-ci-floor.json and are intentionally
far below the reference numbers in doc/operations/benchmark-baseline.md: CI
runners are shared, throttled and noisy, so a tight gate would only produce
flakes. The point is to catch a collapse (a lock, an allocation or a syscall
landing on the hot path), not to track drift.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def resolve(report: dict, key: str) -> float | None:
    node: object = report
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return float(node) if isinstance(node, (int, float)) else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--floor-file", type=Path, required=True)
    args = parser.parse_args()

    floors = load_json(args.floor_file)
    failures: list[str] = []
    checked = 0

    for scenario, limits in floors["scenarios"].items():
        report_path = args.report_dir / f"{scenario}.json"
        if not report_path.is_file():
            failures.append(f"{scenario}: report not found at {report_path}")
            continue
        report = load_json(report_path)
        if not report.get("success", False):
            failures.append(f"{scenario}: benchmark reported success=false")
            continue

        for key, minimum in limits.get("atLeast", {}).items():
            actual = resolve(report, key)
            if actual is None:
                failures.append(f"{scenario}.{key}: missing from report")
                continue
            checked += 1
            status = "ok" if actual >= minimum else "FAIL"
            print(f"[{status}] {scenario}.{key} = {actual:,.2f} (floor {minimum:,.2f})")
            if actual < minimum:
                failures.append(
                    f"{scenario}.{key} = {actual:,.2f} is below the floor {minimum:,.2f}"
                )

        for key, maximum in limits.get("atMost", {}).items():
            actual = resolve(report, key)
            if actual is None:
                failures.append(f"{scenario}.{key}: missing from report")
                continue
            checked += 1
            status = "ok" if actual <= maximum else "FAIL"
            print(f"[{status}] {scenario}.{key} = {actual:,.2f} (ceiling {maximum:,.2f})")
            if actual > maximum:
                failures.append(
                    f"{scenario}.{key} = {actual:,.2f} is above the ceiling {maximum:,.2f}"
                )

    if failures:
        print(f"\n{len(failures)} benchmark regression check(s) failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(f"\nAll {checked} benchmark regression checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
