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


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def post_order(base_url: str, payload: dict):
    request = urllib.request.Request(
        f"{base_url}/api/v1/orders",
        data=json.dumps(payload).encode("utf-8"),
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
                "result": body.get("result"),
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
            "result": body.get("result") or f"HTTP_{exc.code}",
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
        "jitterMs": percentile(latencies, 0.99) - percentile(latencies, 0.50),
    }


def summarize_results(results):
    counts = {}
    for item in results:
        key = item.get("result") or "UNKNOWN"
        if key.startswith("HTTP_") and item.get("detail"):
            key = f"{key}:{item['detail']}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def wait_for(description: str, predicate, timeout_seconds: float):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        value = predicate()
        if value is not None:
            return value
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {description}")


def build_resting_order(order_id: int):
    return {
        "userId": 2,
        "orderId": order_id,
        "side": "SELL",
        "orderType": "LIMIT",
        "timeInForce": "GTC",
        "price": 100,
        "quantity": 1,
    }


def build_crossing_order(order_id: int):
    return {
        "userId": 1,
        "orderId": order_id,
        "side": "BUY",
        "orderType": "LIMIT",
        "timeInForce": "IOC",
        "price": 101,
        "quantity": 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--resting-orders", type=int, default=2048)
    parser.add_argument("--concurrency", type=int, default=24)
    parser.add_argument("--order-id-start", type=int, default=int(time.time() * 1_000_000))
    args = parser.parse_args()

    try:
        wait_for(
            "primary accepting client commands",
            lambda: (lambda h: h if h.get("acceptingClientCommands") else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            30.0,
        )
        for offset in range(args.resting_orders):
            result = post_order(args.base_url, build_resting_order(args.order_id_start + offset))
            if result["status"] not in (200, 202) or result.get("result") != "ACCEPTED":
                raise RuntimeError(
                    f"failed to preload resting order offset={offset} status={result['status']} result={result.get('result')}"
                )

        health_before = wait_for(
            "resting liquidity preload",
            lambda: (lambda h: h if h.get("gatewayAcceptedTotal", 0) >= args.resting_orders else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            90.0,
        )

        results = []
        started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [
                executor.submit(post_order, args.base_url, build_crossing_order(args.order_id_start + args.resting_orders + i))
                for i in range(args.resting_orders)
            ]
            for future in as_completed(futures):
                results.append(future.result())

        expected_trade_count = health_before.get("tradeCount", 0) + args.resting_orders
        health_after = wait_for(
            "crossing trades",
            lambda: (lambda h: h if h.get("tradeCount", 0) >= expected_trade_count else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            90.0,
        )
        elapsed_seconds = (time.perf_counter_ns() - started) / 1_000_000_000.0
    except (urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps({
            "success": False,
            "scenario": "crossing_match_benchmark",
            "category": "matcher-benchmark",
            "severity": "critical",
            "conclusion": f"crossing benchmark failed: {exc}",
        }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return 2

    accepted_orders = max(0, health_after.get("gatewayAcceptedTotal", 0) - health_before.get("gatewayAcceptedTotal", 0))
    processed_commands = max(0, health_after.get("processedCommandCount", 0) - health_before.get("processedCommandCount", 0))
    trade_events = max(0, health_after.get("tradeCount", 0) - health_before.get("tradeCount", 0))
    matched_order_sides = trade_events * 2
    replication_committed_submissions = max(
        0,
        health_after.get("submissionCommittedCount", 0) - health_before.get("submissionCommittedCount", 0),
    )
    submission_pending_delta = health_after.get("submissionPendingCount", 0) - health_before.get("submissionPendingCount", 0)
    result_counts = summarize_results(results)
    report = {
        "success": result_counts.get("ACCEPTED", 0) == args.resting_orders and trade_events == args.resting_orders,
        "scenario": "crossing_match_benchmark",
        "category": "matcher-benchmark",
        "severity": "ok" if result_counts.get("ACCEPTED", 0) == args.resting_orders and trade_events == args.resting_orders else "critical",
        "conclusion": "crossing benchmark completed" if result_counts.get("ACCEPTED", 0) == args.resting_orders and trade_events == args.resting_orders else "crossing benchmark did not produce the expected trade count",
        "transport": health_after.get("replicationTransport"),
        "baseUrl": args.base_url,
        "restingOrders": args.resting_orders,
        "concurrency": args.concurrency,
        "latency": summarize_latencies([item["latencyMs"] for item in results]),
        "resultCounts": result_counts,
        "elapsedSeconds": elapsed_seconds,
        "acceptedOrders": accepted_orders,
        "processedCommands": processed_commands,
        "tradeEvents": trade_events,
        "matchedOrderSides": matched_order_sides,
        "replicationCommittedSubmissions": replication_committed_submissions,
        "submissionPendingDelta": submission_pending_delta,
        "acceptedOrdersPerSecond": 0.0 if elapsed_seconds == 0.0 else accepted_orders / elapsed_seconds,
        "processedCommandsPerSecond": 0.0 if elapsed_seconds == 0.0 else processed_commands / elapsed_seconds,
        "tradeEventsPerSecond": 0.0 if elapsed_seconds == 0.0 else trade_events / elapsed_seconds,
        "matchedOrderSidesPerSecond": 0.0 if elapsed_seconds == 0.0 else matched_order_sides / elapsed_seconds,
        "replicationCommittedSubmissionsPerSecond": 0.0 if elapsed_seconds == 0.0 else replication_committed_submissions / elapsed_seconds,
        "description": {
            "acceptedOrdersPerSecond": "交叉吃单阶段被本地网关接受的订单数/秒。",
            "processedCommandsPerSecond": "交叉吃单阶段撮合主循环实际处理的命令数/秒。",
            "tradeEventsPerSecond": "交叉吃单阶段生成的成交事件数/秒。",
            "matchedOrderSidesPerSecond": "交叉吃单阶段参与成交的订单边数/秒。",
            "replicationCommittedSubmissionsPerSecond": "交叉吃单阶段达到复制确认状态的 submission 数/秒。",
        },
        "healthBefore": health_before,
        "healthAfter": health_after,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(Path(args.report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
