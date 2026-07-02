#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def scenario_from_report(path: Path, report: dict):
    scenario = report.get("scenario") or path.stem
    category = report.get("category", "unspecified")
    success = bool(report.get("valid", report.get("success", False)))
    severity = report.get("severity", "ok" if success else "critical")
    conclusion = report.get("conclusion")
    if not conclusion:
        conclusion = "scenario passed" if success else "scenario failed"
    errors = report.get("errors", [])
    checks = report.get("checks", [])
    return {
        "scenario": scenario,
        "category": category,
        "success": success,
        "severity": severity,
        "conclusion": conclusion,
        "errors": errors,
        "checks": checks,
        "source": str(path),
    }


def normalize_paths(args):
    paths = []
    for raw in args:
        path = Path(raw)
        if path.is_dir():
            for name in ("validation-report.json", "transport-rollout-report.json", "security-rotation-report.json", "failover-smoke-report.json"):
                candidate = path / name
                if candidate.exists():
                    paths.append(candidate)
        else:
            paths.append(path)
    return paths


def main() -> int:
    args = sys.argv[1:]
    report_path = None
    if len(args) >= 2 and args[0] == "--report":
        report_path = Path(args[1])
        args = args[2:]
    if not args:
        print("usage: summarize-chaos-report.py [--report <file>] <report-or-dir> [<report-or-dir> ...]", file=sys.stderr)
        return 1

    scenarios = []
    missing = []
    for path in normalize_paths(args):
        if not path.exists():
            missing.append(str(path))
            continue
        scenarios.append(scenario_from_report(path, load_json(path)))

    success_count = sum(1 for item in scenarios if item["success"])
    failure_count = len(scenarios) - success_count
    severity = "ok" if failure_count == 0 and not missing else "critical"
    conclusion = "all chaos scenarios passed" if failure_count == 0 and not missing else "chaos scenarios require attention"
    summary = {
        "success": failure_count == 0 and not missing,
        "severity": severity,
        "conclusion": conclusion,
        "scenarioCount": len(scenarios),
        "successCount": success_count,
        "failureCount": failure_count,
        "missingReports": missing,
        "scenarios": scenarios,
    }
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    for scenario in scenarios:
        status = "PASS" if scenario["success"] else "FAIL"
        print(f"{status} {scenario['scenario']}: {scenario['conclusion']}")
    if missing:
        for item in missing:
            print(f"MISSING report: {item}", file=sys.stderr)
    print(conclusion)
    return 0 if summary["success"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
