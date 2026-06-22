#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def score(report: dict):
    latency = report.get("latency", {})
    throughput = report.get("throughput", {})
    stable_throughput = report.get("stableThroughputProfile") or throughput
    first_failed = report.get("firstFailedProfile")
    engine_progress = stable_throughput.get("engineProgress", {})
    return {
        "success": bool(report.get("success")),
        "completedAllProfiles": report.get("firstFailedProfile") is None,
        "p99Ms": latency.get("p99Ms", float("inf")),
        "meanMs": latency.get("meanMs", float("inf")),
        "jitterMs": latency.get("jitterMs", float("inf")),
        "throughputOpsPerSecond": stable_throughput.get("throughputOpsPerSecond", 0.0),
        "stableConcurrency": stable_throughput.get("concurrency", 0),
        "stableOperationsPerRound": stable_throughput.get("operationsPerRound", 0),
        "stableRounds": stable_throughput.get("rounds", 0),
        "stableProfileLabel": stable_throughput.get("profileLabel"),
        "acceptedOrdersPerSecond": engine_progress.get("acceptedOrdersPerSecond", 0.0),
        "processedCommandsPerSecond": engine_progress.get("processedCommandsPerSecond", 0.0),
        "tradeEventsPerSecond": engine_progress.get("tradeEventsPerSecond", 0.0),
        "matchedOrderSidesPerSecond": engine_progress.get("matchedOrderSidesPerSecond", 0.0),
        "firstFailedProfile": None if first_failed is None else {
            "profileLabel": first_failed.get("profileLabel"),
            "concurrency": first_failed.get("concurrency"),
            "operationsPerRound": first_failed.get("operationsPerRound"),
            "rounds": first_failed.get("rounds"),
            "reason": first_failed.get("stabilityReason"),
        },
    }

def chaos_summary_for(path: Path):
    candidate = path.parent / "failover-smoke" / "chaos-summary.json"
    if not candidate.exists():
        return None
    return load_json(candidate)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True)
    parser.add_argument("benchmarks", nargs="+")
    args = parser.parse_args()

    modes = {}
    for raw in args.benchmarks:
        path = Path(raw)
        report = load_json(path)
        chaos = chaos_summary_for(path)
        modes[report.get("transport", path.parent.name)] = {
            "benchmark": report,
            "score": score(report),
            "chaos": chaos,
        }

    ranked = sorted(
        modes.items(),
        key=lambda item: (
            0 if item[1]["score"]["success"] else 1,
            0 if item[1]["score"]["completedAllProfiles"] else 1,
            item[1]["score"]["p99Ms"],
            item[1]["score"]["jitterMs"],
            -item[1]["score"]["throughputOpsPerSecond"],
        ),
    )
    preferred = ranked[0][0] if ranked else ""
    summary = {
        "success": all(item["score"]["success"] for item in modes.values())
        and all(item["chaos"] is None or item["chaos"].get("success") for item in modes.values()),
        "scenario": "transport_comparison",
        "category": "ha-benchmark",
        "throughputProfileNotation": next(
            (payload["benchmark"].get("throughputProfileNotation") for payload in modes.values()
             if payload["benchmark"].get("throughputProfileNotation") is not None),
            None
        ),
        "severity": "ok" if all(item["score"]["success"] for item in modes.values())
        and all(item["chaos"] is None or item["chaos"].get("success") for item in modes.values()) else "critical",
        "conclusion": f"preferred transport under current benchmark profile: {preferred}" if preferred else "no benchmark data",
        "preferredTransport": preferred,
        "modes": {
            name: {
                **payload["score"],
                "chaosSuccess": None if payload["chaos"] is None else bool(payload["chaos"].get("success")),
                "chaosConclusion": None if payload["chaos"] is None else payload["chaos"].get("conclusion"),
            }
            for name, payload in modes.items()
        },
    }
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
