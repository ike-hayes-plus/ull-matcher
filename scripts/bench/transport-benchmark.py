#!/usr/bin/env python3
import argparse
import json
import math
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DEFAULT_THROUGHPUT_PROFILES = "6x240x1,12x480x2,24x960x2"


def throughput_profile_label(concurrency: int, operations_per_round: int, rounds: int):
    return f"{concurrency}x{operations_per_round}x{rounds}"


def throughput_profile_notation():
    return {
        "format": "并发度x每轮订单数x连续轮数",
        "example": "24x960x2",
        "concurrencyMeaning": "同时发起提交请求的并发 worker 数",
        "operationsPerRoundMeaning": "每一轮压测实际提交的订单数",
        "roundsMeaning": "同一档位连续重复执行的轮数",
    }


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def post_order(base_url: str, order_id: int):
    payload = json.dumps({
        "userId": 1,
        "orderId": order_id,
        "side": "BUY" if order_id % 2 == 0 else "SELL",
        "orderType": "LIMIT",
        "timeInForce": "IOC",
        "price": 101 if order_id % 2 == 0 else 100,
        "quantity": 1,
    }).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/api/v1/orders",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.perf_counter_ns()
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.loads(response.read().decode("utf-8"))
            latency_ms = (time.perf_counter_ns() - started) / 1_000_000.0
            return {
                "status": response.status,
                "sequence": body.get("sequence", 0),
                "result": body.get("result", ""),
                "latencyMs": latency_ms,
            }
    except urllib.error.HTTPError as exc:
        body = {}
        try:
            body = json.loads(exc.read().decode("utf-8"))
        except Exception:
            body = {}
        latency_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        return {
            "status": exc.code,
            "sequence": body.get("sequence", 0),
            "result": body.get("result", f"HTTP_{exc.code}"),
            "error": body.get("error", str(exc)),
            "code": body.get("code", ""),
            "detail": body.get("detail", ""),
            "latencyMs": latency_ms,
        }


def percentile(values, ratio: float):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(ratio * len(ordered)) - 1))
    return ordered[index]


def summarize_latencies(latencies):
    if not latencies:
        return {}
    return {
        "count": len(latencies),
        "minMs": min(latencies),
        "maxMs": max(latencies),
        "meanMs": statistics.fmean(latencies),
        "stddevMs": statistics.pstdev(latencies) if len(latencies) > 1 else 0.0,
        "p50Ms": percentile(latencies, 0.50),
        "p95Ms": percentile(latencies, 0.95),
        "p99Ms": percentile(latencies, 0.99),
        "jitterMs": (percentile(latencies, 0.99) - percentile(latencies, 0.50)),
    }


def counter(payload: dict, key: str):
    value = payload.get(key, 0)
    return value if isinstance(value, (int, float)) else 0


def summarize_engine_progress(before: dict, after: dict, elapsed_seconds: float):
    processed_commands = max(0, counter(after, "processedCommandCount") - counter(before, "processedCommandCount"))
    accepted_orders = max(0, counter(after, "gatewayAcceptedTotal") - counter(before, "gatewayAcceptedTotal"))
    trade_events = max(0, counter(after, "tradeCount") - counter(before, "tradeCount"))
    order_events = max(0, counter(after, "orderEventCount") - counter(before, "orderEventCount"))
    submission_committed = max(0, counter(after, "submissionCommittedCount") - counter(before, "submissionCommittedCount"))
    seconds = elapsed_seconds if elapsed_seconds > 0.0 else 0.0
    return {
        "processedCommands": processed_commands,
        "acceptedOrders": accepted_orders,
        "tradeEvents": trade_events,
        "orderEvents": order_events,
        "matchedOrderSides": trade_events * 2,
        "replicationCommittedSubmissions": submission_committed,
        "processedCommandsPerSecond": 0.0 if seconds == 0.0 else processed_commands / seconds,
        "acceptedOrdersPerSecond": 0.0 if seconds == 0.0 else accepted_orders / seconds,
        "tradeEventsPerSecond": 0.0 if seconds == 0.0 else trade_events / seconds,
        "orderEventsPerSecond": 0.0 if seconds == 0.0 else order_events / seconds,
        "matchedOrderSidesPerSecond": 0.0 if seconds == 0.0 else (trade_events * 2) / seconds,
        "replicationCommittedSubmissionsPerSecond": 0.0 if seconds == 0.0 else submission_committed / seconds,
        "description": {
            "acceptedOrdersPerSecond": "HTTP 提交并被本地网关接受的订单数/秒，用于衡量订单进入系统的能力。",
            "processedCommandsPerSecond": "撮合主循环实际处理的命令数/秒，用于衡量主链处理能力。",
            "tradeEventsPerSecond": "成交事件数/秒，每个成交事件对应一笔撮合成交。",
            "matchedOrderSidesPerSecond": "参与成交的订单边数/秒，按每笔成交折算为买卖双方各一边。",
            "replicationCommittedSubmissionsPerSecond": "达到当前复制确认策略的提交数/秒。",
        },
    }


def run_sequential(base_url: str, warmup: int, samples: int, starting_order_id: int):
    order_id = starting_order_id
    for _ in range(warmup):
        post_order(base_url, order_id)
        order_id += 1
    samples_out = []
    for _ in range(samples):
        samples_out.append(post_order(base_url, order_id))
        order_id += 1
    return samples_out, order_id


def run_throughput(base_url: str, total_ops: int, concurrency: int, starting_order_id: int):
    health_before = fetch_json(f"{base_url}/api/v1/runtime/health")
    results = []
    started = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(post_order, base_url, starting_order_id + i) for i in range(total_ops)]
        for future in as_completed(futures):
            results.append(future.result())
    elapsed_seconds = (time.perf_counter_ns() - started) / 1_000_000_000.0
    health_after = fetch_json(f"{base_url}/api/v1/runtime/health")
    return {
        "operations": total_ops,
        "concurrency": concurrency,
        "elapsedSeconds": elapsed_seconds,
        "throughputOpsPerSecond": 0.0 if elapsed_seconds == 0.0 else total_ops / elapsed_seconds,
        "profileLabel": throughput_profile_label(concurrency, total_ops, 1),
        "latency": summarize_latencies([item["latencyMs"] for item in results]),
        "resultCounts": summarize_results(results),
        "engineProgress": summarize_engine_progress(health_before, health_after, elapsed_seconds),
        "healthBefore": health_before,
        "healthAfter": health_after,
        "_rawLatencies": [item["latencyMs"] for item in results],
    }


def classify_profile_stability(report: dict):
    result_counts = report.get("resultCounts", {})
    accepted = result_counts.get("ACCEPTED", 0)
    total = report.get("operations", 0)
    if accepted != total:
        return {
            "stable": False,
            "reason": f"accepted {accepted}/{total}",
        }
    return {
        "stable": True,
        "reason": "all operations accepted",
    }


def parse_profiles(raw: str):
    profiles = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        segments = value.split("x")
        if len(segments) not in (2, 3):
            raise ValueError(f"invalid throughput profile: {value}")
        concurrency = int(segments[0])
        operations = int(segments[1])
        rounds = int(segments[2]) if len(segments) == 3 else 1
        profiles.append({
            "concurrency": concurrency,
            "operations": operations,
            "rounds": rounds,
        })
    if not profiles:
        raise ValueError("at least one throughput profile is required")
    return profiles


def run_throughput_profiles(base_url: str, profiles, starting_order_id: int):
    reports = []
    next_order_id = starting_order_id
    best = None
    first_failure = None
    for profile in profiles:
        round_reports = []
        result_counts = {}
        started = time.perf_counter_ns()
        total_operations = 0
        failure = None
        for round_index in range(profile["rounds"]):
            try:
                report = run_throughput(base_url, profile["operations"], profile["concurrency"], next_order_id)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                failure = {
                    "round": round_index + 1,
                    "reason": str(exc),
                }
                break
            round_reports.append(report)
            next_order_id += profile["operations"]
            total_operations += profile["operations"]
            for key, value in report["resultCounts"].items():
                result_counts[key] = result_counts.get(key, 0) + value
        elapsed_seconds = (time.perf_counter_ns() - started) / 1_000_000_000.0
        merged_latencies = []
        for round_report in round_reports:
            merged_latencies.extend(round_report.pop("_rawLatencies", []))
        stability = classify_profile_stability({
            "operations": total_operations,
            "resultCounts": result_counts,
        }) if failure is None else {
            "stable": False,
            "reason": failure["reason"],
        }
        profile_report = {
            "concurrency": profile["concurrency"],
            "operationsPerRound": profile["operations"],
            "rounds": profile["rounds"],
            "profileLabel": throughput_profile_label(profile["concurrency"], profile["operations"], profile["rounds"]),
            "operations": total_operations,
            "elapsedSeconds": elapsed_seconds,
            "throughputOpsPerSecond": 0.0 if elapsed_seconds == 0.0 else total_operations / elapsed_seconds,
            "latency": summarize_latencies(merged_latencies),
            "resultCounts": result_counts,
            "roundReports": round_reports,
            "engineProgress": summarize_engine_progress(
                round_reports[0]["healthBefore"],
                round_reports[-1]["healthAfter"],
                elapsed_seconds
            ) if round_reports else summarize_engine_progress({}, {}, elapsed_seconds),
            "stable": stability["stable"],
            "stabilityReason": stability["reason"],
        }
        if failure is not None:
            profile_report["failure"] = failure
        reports.append(profile_report)
        if profile_report["stable"] and (best is None or profile_report["throughputOpsPerSecond"] > best["throughputOpsPerSecond"]):
            best = profile_report
        if not profile_report["stable"]:
            first_failure = profile_report
            break
    return reports, best, first_failure, next_order_id


def summarize_results(results):
    counts = {}
    for item in results:
        key = item.get("result") or "UNKNOWN"
        if key.startswith("HTTP_") and item.get("detail"):
            key = f"{key}:{item['detail']}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--latency-samples", type=int, default=120)
    parser.add_argument("--throughput-ops", type=int, default=240)
    parser.add_argument("--throughput-concurrency", type=int, default=6)
    parser.add_argument("--throughput-profiles", default=DEFAULT_THROUGHPUT_PROFILES)
    parser.add_argument("--order-id-start", type=int, default=int(time.time() * 1_000_000))
    args = parser.parse_args()

    try:
        health_before = fetch_json(f"{args.base_url}/api/v1/runtime/health")
        readiness_before = fetch_json(f"{args.base_url}/api/v1/runtime/readiness")
        sequential, next_order_id = run_sequential(args.base_url, args.warmup, args.latency_samples, args.order_id_start)
        throughput_profiles, best_throughput, first_failure, _ = run_throughput_profiles(
            args.base_url,
            parse_profiles(args.throughput_profiles),
            next_order_id
        )
        health_after = fetch_json(f"{args.base_url}/api/v1/runtime/health")
        readiness_after = fetch_json(f"{args.base_url}/api/v1/runtime/readiness")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        Path(args.report).write_text(json.dumps({
            "success": False,
            "scenario": "transport_benchmark",
            "category": "ha-benchmark",
            "severity": "critical",
            "conclusion": f"benchmark request failed: {exc}",
        }, indent=2) + "\n", encoding="utf-8")
        return 2

    report = {
        "success": readiness_after.get("serviceReady") is True and first_failure is None and best_throughput is not None,
        "scenario": "transport_benchmark",
        "category": "ha-benchmark",
        "severity": "ok" if readiness_after.get("serviceReady") is True and first_failure is None else "critical",
        "conclusion": "transport benchmark completed" if first_failure is None else f"transport benchmark hit instability at {first_failure['concurrency']}x{first_failure['operationsPerRound']}x{first_failure['rounds']}",
        "transport": health_after.get("replicationTransport"),
        "baseUrl": args.base_url,
        "throughputProfileNotation": throughput_profile_notation(),
        "warmup": args.warmup,
        "latency": summarize_latencies([item["latencyMs"] for item in sequential]),
        "latencyResultCounts": summarize_results(sequential),
        "throughput": best_throughput,
        "throughputProfiles": throughput_profiles,
        "stableThroughputProfile": best_throughput,
        "firstFailedProfile": first_failure,
        "healthBefore": health_before,
        "healthAfter": health_after,
        "readinessBefore": readiness_before,
        "readinessAfter": readiness_after,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(Path(args.report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
