#!/usr/bin/env python3
"""Validate HA benchmark JSON reports against doc/operations/benchmark-ha-ci-floor.json."""

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
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="fail if any scenario report listed in the floor file is missing",
    )
    args = parser.parse_args()

    floors = load_json(args.floor_file)
    failures: list[str] = []
    checked = 0
    skipped = 0

    for scenario, limits in floors.get("scenarios", {}).items():
        report_name = limits.get("reportFile", f"{scenario}.json")
        report_path = args.report_dir / report_name
        if not report_path.is_file():
            if args.require_all:
                failures.append(f"{scenario}: report not found at {report_path}")
            else:
                skipped += 1
                print(f"[skip] {scenario}: no report at {report_path}")
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
        print(f"\n{len(failures)} HA benchmark floor check(s) failed:", file=sys.stderr)
        for item in failures:
            print(f"  - {item}", file=sys.stderr)
        return 1

    if checked == 0 and skipped > 0:
        print("No HA benchmark reports present; floor checks skipped (use --require-all to fail).")
        return 0

    print(f"\nAll {checked} HA benchmark floor checks passed ({skipped} scenario(s) skipped).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
