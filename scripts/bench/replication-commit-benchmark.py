#!/usr/bin/env python3
import argparse
import json
import math
import statistics
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional


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
                "submissionId": body.get("submissionId"),
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


def wait_for(description: str, predicate, timeout_seconds: float):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        value = predicate()
        if value is not None:
            return value
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {description}")


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


class HealthPoller:
    def __init__(self, primary_base_url: str, standby_base_url: Optional[str], interval_seconds: float):
        self.primary_base_url = primary_base_url
        self.standby_base_url = standby_base_url
        self.interval_seconds = interval_seconds
        self.samples = []
        self.errors = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="replication-bench-health", daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5.0)

    def _run(self):
        started = time.perf_counter()
        while not self._stop.is_set():
            try:
                primary_health = fetch_json(f"{self.primary_base_url}/api/v1/runtime/health")
                standby_health = None
                if self.standby_base_url:
                    standby_health = fetch_json(f"{self.standby_base_url}/api/v1/runtime/health")
                self.samples.append({
                    "relativeSeconds": time.perf_counter() - started,
                    "primaryHealth": primary_health,
                    "standbyHealth": standby_health,
                })
            except Exception as exc:
                self.errors.append(str(exc))
            self._stop.wait(self.interval_seconds)


def extract_max(samples, scope: str, key: str):
    values = []
    for sample in samples:
        payload = sample.get(scope)
        if payload is not None:
            values.append(payload.get(key, 0))
    numeric = [value for value in values if isinstance(value, (int, float))]
    return 0 if not numeric else max(numeric)


def extract_mean(samples, scope: str, key: str):
    values = []
    for sample in samples:
        payload = sample.get(scope)
        if payload is not None:
            values.append(payload.get(key, 0))
    numeric = [value for value in values if isinstance(value, (int, float))]
    return 0.0 if not numeric else statistics.fmean(numeric)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--standby-base-url")
    parser.add_argument("--report", required=True)
    parser.add_argument("--resting-orders", type=int, default=2048)
    parser.add_argument("--concurrency", type=int, default=24)
    parser.add_argument("--poll-interval-ms", type=int, default=100)
    parser.add_argument("--commit-timeout-seconds", type=float, default=90.0)
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
        poller = HealthPoller(args.base_url, args.standby_base_url, max(0.02, args.poll_interval_ms / 1000.0))
        poller.start()

        results = []
        submit_started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [
                executor.submit(post_order, args.base_url, build_crossing_order(args.order_id_start + args.resting_orders + i))
                for i in range(args.resting_orders)
            ]
            for future in as_completed(futures):
                results.append(future.result())
        submit_completed = time.perf_counter_ns()

        expected_trade_count = health_before.get("tradeCount", 0) + args.resting_orders
        wait_for(
            "crossing trades",
            lambda: (lambda h: h if h.get("tradeCount", 0) >= expected_trade_count else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            90.0,
        )

    except (urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
        report = {
            "success": False,
            "scenario": "replication_commit_benchmark",
            "category": "matcher-benchmark",
            "severity": "critical",
            "conclusion": f"replication commit benchmark failed: {exc}",
        }
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return 2

    expected_committed = health_before.get("submissionCommittedCount", 0) + args.resting_orders
    timed_out = False
    timeout_error = ""
    standby_health_after = None
    try:
        health_after = wait_for(
            "replication committed submissions",
            lambda: (lambda h: h if h.get("submissionCommittedCount", 0) >= expected_committed else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            args.commit_timeout_seconds,
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        timed_out = True
        timeout_error = str(exc)
        health_after = fetch_json(f"{args.base_url}/api/v1/runtime/health")
        if args.standby_base_url:
            standby_health_after = fetch_json(f"{args.standby_base_url}/api/v1/runtime/health")
    finally:
        committed_completed = time.perf_counter_ns()
        poller.stop()
    if not timed_out and args.standby_base_url:
        standby_health_after = fetch_json(f"{args.standby_base_url}/api/v1/runtime/health")

    submit_elapsed_seconds = (submit_completed - submit_started) / 1_000_000_000.0
    total_elapsed_seconds = (committed_completed - submit_started) / 1_000_000_000.0
    commit_catchup_seconds = max(0.0, total_elapsed_seconds - submit_elapsed_seconds)
    accepted_orders = max(0, health_after.get("gatewayAcceptedTotal", 0) - health_before.get("gatewayAcceptedTotal", 0))
    trade_events = max(0, health_after.get("tradeCount", 0) - health_before.get("tradeCount", 0))
    committed_submissions = max(
        0,
        health_after.get("submissionCommittedCount", 0) - health_before.get("submissionCommittedCount", 0),
    )
    pending_delta = health_after.get("submissionPendingCount", 0) - health_before.get("submissionPendingCount", 0)
    samples = poller.samples

    success = (not timed_out) and committed_submissions >= accepted_orders
    report = {
        "success": success,
        "scenario": "replication_commit_benchmark",
        "category": "matcher-benchmark",
        "severity": "ok" if success else "critical",
        "conclusion": "replication committed chain caught up within the benchmark window"
        if success
        else f"replication committed chain did not catch up within the benchmark window: {timeout_error or 'committed submissions lagged behind accepted submissions'}",
        "transport": health_after.get("replicationTransport"),
        "baseUrl": args.base_url,
        "restingOrders": args.resting_orders,
        "concurrency": args.concurrency,
        "submitWindowSeconds": submit_elapsed_seconds,
        "totalWindowSeconds": total_elapsed_seconds,
        "commitCatchupSeconds": commit_catchup_seconds,
        "latency": summarize_latencies([item["latencyMs"] for item in results]),
        "resultCounts": summarize_results(results),
        "acceptedOrders": accepted_orders,
        "tradeEvents": trade_events,
        "matchedOrderSides": trade_events * 2,
        "replicationCommittedSubmissions": committed_submissions,
        "submissionPendingDelta": pending_delta,
        "acceptedOrdersPerSecond": 0.0 if submit_elapsed_seconds == 0.0 else accepted_orders / submit_elapsed_seconds,
        "tradeEventsPerSecond": 0.0 if submit_elapsed_seconds == 0.0 else trade_events / submit_elapsed_seconds,
        "matchedOrderSidesPerSecond": 0.0 if submit_elapsed_seconds == 0.0 else (trade_events * 2) / submit_elapsed_seconds,
        "replicationCommittedSubmissionsPerSecond": 0.0 if total_elapsed_seconds == 0.0 else committed_submissions / total_elapsed_seconds,
        "primaryPeakHealth": {
            "submissionPendingCount": extract_max(samples, "primaryHealth", "submissionPendingCount"),
            "replicationQueueDepth": extract_max(samples, "primaryHealth", "replicationQueueDepth"),
            "replicationLastBatchSize": extract_max(samples, "primaryHealth", "replicationLastBatchSize"),
            "replicationLastCommitMicros": extract_max(samples, "primaryHealth", "replicationLastCommitMicros"),
        },
        "primaryMeanHealth": {
            "replicationQueueDepth": extract_mean(samples, "primaryHealth", "replicationQueueDepth"),
            "replicationLastBatchSize": extract_mean(samples, "primaryHealth", "replicationLastBatchSize"),
            "replicationLastCommitMicros": extract_mean(samples, "primaryHealth", "replicationLastCommitMicros"),
        },
        "standbyPeakHealth": None if args.standby_base_url is None else {
            "applyQueueDepth": extract_max(samples, "standbyHealth", "standbyApplyQueueDepth"),
            "replicatedBatchSize": extract_max(samples, "standbyHealth", "standbyLastReplicatedBatchSize"),
            "ackFlushCommands": extract_max(samples, "standbyHealth", "standbyAckLastFlushCommands"),
            "ackFlushMicros": extract_max(samples, "standbyHealth", "standbyAckLastFlushMicros"),
            "ackFlushIntervalMicros": extract_max(samples, "standbyHealth", "standbyAckLastFlushIntervalMicros"),
        },
        "standbyMeanHealth": None if args.standby_base_url is None else {
            "applyQueueDepth": extract_mean(samples, "standbyHealth", "standbyApplyQueueDepth"),
            "replicatedBatchSize": extract_mean(samples, "standbyHealth", "standbyLastReplicatedBatchSize"),
            "ackFlushCommands": extract_mean(samples, "standbyHealth", "standbyAckLastFlushCommands"),
            "ackFlushMicros": extract_mean(samples, "standbyHealth", "standbyAckLastFlushMicros"),
            "ackFlushIntervalMicros": extract_mean(samples, "standbyHealth", "standbyAckLastFlushIntervalMicros"),
        },
        "description": {
            "acceptedOrdersPerSecond": "交叉吃单窗口内被主节点受理的订单数/秒，表示接单能力。",
            "tradeEventsPerSecond": "交叉吃单窗口内生成的成交事件数/秒，表示撮合能力。",
            "replicationCommittedSubmissionsPerSecond": "从提交开始直到复制确认全部收敛为止，达到 replication committed 的 submission 数/秒。",
            "commitCatchupSeconds": "交叉吃单全部提交完成后，复制确认从未完成追赶到全部完成所需的额外时间。",
            "replicationQueueDepth": "主节点 replication coordinator 的排队深度。",
            "standbyAckLastFlushMicros": "备节点最近一次 durable ack flush 的耗时。",
        },
        "healthBefore": health_before,
        "healthAfter": health_after,
        "standbyHealthAfter": standby_health_after,
        "sampleCount": len(samples),
        "pollerErrors": poller.errors,
        "timedOut": timed_out,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(Path(args.report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
