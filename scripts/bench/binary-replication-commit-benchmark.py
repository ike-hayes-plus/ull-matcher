#!/usr/bin/env python3
import argparse
import json
import math
import socket
import statistics
import struct
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from benchmark_metadata import benchmark_metadata
from replication_metrics import all_standbys_watermark
from replication_metrics import mode_ready
from replication_metrics import mode_watermark
from replication_metrics import watermark_delta


REQUEST_MAGIC = 0x554C4C42
RESPONSE_MAGIC = 0x554C4C52
PROTOCOL_VERSION = 1
FRAME_TYPE_NEW_ORDER_BATCH = 1
FRAME_TYPE_BATCH_RESULT = 101
RESPONSE_RECORD_BYTES = 24


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for(description: str, predicate, timeout_seconds: float, poll_interval_seconds: float = 0.05):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        value = predicate()
        if value is not None:
            return value
        time.sleep(poll_interval_seconds)
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


class BinarySession:
    def __init__(self, host: str, port: int, timeout_seconds: float):
        self.sock = socket.create_connection((host, port), timeout=timeout_seconds)
        self.sock.settimeout(timeout_seconds)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self):
        self.sock.close()

    def send_new_orders(self, records):
        payload = bytearray()
        for item in records:
            payload.extend(struct.pack(
                ">QQQQqccc5x",
                item["userId"],
                item["orderId"],
                item["price"],
                item["quantity"],
                item["ttlMillis"],
                item["side"].encode("ascii"),
                item["orderType"].encode("ascii"),
                item["tif"].encode("ascii"),
            ))
        header = struct.pack(">IHHII", REQUEST_MAGIC, PROTOCOL_VERSION, FRAME_TYPE_NEW_ORDER_BATCH, len(records), len(payload))
        started = time.perf_counter_ns()
        self.sock.sendall(header + payload)
        response_header = self._recv_exact(16)
        magic, version, response_frame_type, response_count, payload_bytes = struct.unpack(">IHHII", response_header)
        if magic != RESPONSE_MAGIC or version != PROTOCOL_VERSION or response_frame_type != FRAME_TYPE_BATCH_RESULT:
            raise IOError("binary ingress response header mismatch")
        if response_count != len(records) or payload_bytes != response_count * RESPONSE_RECORD_BYTES:
            raise IOError("binary ingress response payload mismatch")
        response_payload = self._recv_exact(payload_bytes)
        latency_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        responses = []
        for i in range(response_count):
            base = i * RESPONSE_RECORD_BYTES
            order_id, sequence, status, _reserved = struct.unpack(">QQII", response_payload[base:base + RESPONSE_RECORD_BYTES])
            responses.append({"orderId": order_id, "sequence": sequence, "status": status})
        return responses, latency_ms

    def _recv_exact(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self.sock.recv(size - len(chunks))
            if not chunk:
                raise IOError("binary ingress connection closed")
            chunks.extend(chunk)
        return bytes(chunks)


def status_to_result(status: int):
    return {
        0: "ACCEPTED",
        1: "MATCHER_NOT_RUNNING",
        2: "RING_FULL_BEFORE_WAL_APPEND",
        3: "COMMAND_POOL_EXHAUSTED",
        4: "MATCHER_STOPPED_AFTER_WAL_APPEND",
        5: "RING_FULL_AFTER_WAL_APPEND",
    }.get(status, f"UNKNOWN_{status}")


def build_new_order(order_id: int, user_id: int, side: str, order_type: str, tif: str, price: int, quantity: int, ttl_millis: int = -1):
    return {
        "userId": user_id,
        "orderId": order_id,
        "price": price,
        "quantity": quantity,
        "ttlMillis": ttl_millis,
        "side": side,
        "orderType": order_type,
        "tif": tif,
    }


def preload_resting(binary_host: str, binary_port: int, timeout_seconds: float, resting_orders: int, batch_size: int, order_id_start: int):
    session = BinarySession(binary_host, binary_port, timeout_seconds)
    try:
        created = 0
        while created < resting_orders:
            count = min(batch_size, resting_orders - created)
            records = [
                build_new_order(order_id_start + created + i, 2, "S", "L", "G", 100, 1)
                for i in range(count)
            ]
            responses, _latency = session.send_new_orders(records)
            for response in responses:
                if status_to_result(response["status"]) != "ACCEPTED":
                    raise RuntimeError(f"failed to preload resting order {response}")
            created += count
    finally:
        session.close()


def run_warmup(binary_host: str, binary_port: int, timeout_seconds: float, warmup_orders: int, batch_size: int, order_id_start: int):
    if warmup_orders <= 0:
        return 0
    session = BinarySession(binary_host, binary_port, timeout_seconds)
    try:
        sent = 0
        while sent < warmup_orders:
            count = min(batch_size, warmup_orders - sent)
            records = [
                build_new_order(order_id_start + sent + i, 9, "S", "L", "G", 102, 1)
                for i in range(count)
            ]
            responses, _latency = session.send_new_orders(records)
            for response in responses:
                if status_to_result(response["status"]) != "ACCEPTED":
                    raise RuntimeError(f"failed to warm up binary ingress {response}")
            sent += count
        return warmup_orders
    finally:
        session.close()


def run_worker(start_latch: threading.Event, binary_host: str, binary_port: int, timeout_seconds: float, batch_size: int, order_id_base: int, order_count: int):
    session = BinarySession(binary_host, binary_port, timeout_seconds)
    latencies = []
    rejected = 0
    max_sequence = 0
    try:
        start_latch.wait()
        sent = 0
        while sent < order_count:
            count = min(batch_size, order_count - sent)
            records = [
                build_new_order(order_id_base + sent + i, 1, "B", "L", "I", 101, 1)
                for i in range(count)
            ]
            responses, latency_ms = session.send_new_orders(records)
            per_order_latency = latency_ms / max(1, count)
            for response in responses:
                latencies.append(per_order_latency)
                if status_to_result(response["status"]) != "ACCEPTED":
                    rejected += 1
                else:
                    max_sequence = max(max_sequence, response["sequence"])
            sent += count
    finally:
        session.close()
    return {
        "latenciesMs": latencies,
        "rejected": rejected,
        "maxSequence": max_sequence,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--standby-base-url", action="append", default=[])
    parser.add_argument("--binary-host", default="127.0.0.1")
    parser.add_argument("--binary-port", type=int, required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--resting-orders", type=int, default=2048)
    parser.add_argument("--concurrency", type=int, default=24)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--crossing-orders", type=int, default=2048)
    parser.add_argument("--socket-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--warmup-orders", type=int, default=256)
    parser.add_argument("--commit-timeout-seconds", type=float, default=90.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.005)
    parser.add_argument("--order-id-start", type=int, default=int(time.time() * 1_000_000))
    parser.add_argument("--standby-commit-mode", choices=("any", "quorum", "all"), default="any")
    args = parser.parse_args()

    phase = "bootstrap"
    standby_after = []
    health_after = None
    try:
        phase = "wait_primary_ready"
        health_before_ready = wait_for(
            "primary accepting client commands",
            lambda: (lambda h: h if h.get("acceptingClientCommands") else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            30.0,
            args.poll_interval_seconds,
        )
        phase = "preload_resting"
        preload_resting(args.binary_host, args.binary_port, args.socket_timeout_seconds, args.resting_orders, args.batch_size, args.order_id_start)
        phase = "wait_preload_primary"
        preload_ready = wait_for(
            "resting liquidity preload",
            lambda: (lambda h: h if h.get("gatewayAcceptedTotal", 0) >= health_before_ready.get("gatewayAcceptedTotal", 0) + args.resting_orders else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            90.0,
            args.poll_interval_seconds,
        )
        preload_target_sequence = preload_ready.get("lastDurableSequence", 0)
        if args.standby_base_url:
            def standby_preload_ready():
                payloads = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url]
                if mode_ready(args.standby_commit_mode, payloads, preload_target_sequence):
                    return payloads
                return None

            phase = "wait_preload_standby_durable"
            wait_for(
                "standby preload durable progression for requested commit mode",
                standby_preload_ready,
                90.0,
                args.poll_interval_seconds,
            )

        next_order_id_start = args.order_id_start + args.resting_orders
        if args.warmup_orders > 0:
            phase = "run_warmup"
            run_warmup(
                args.binary_host,
                args.binary_port,
                args.socket_timeout_seconds,
                args.warmup_orders,
                args.batch_size,
                next_order_id_start,
            )
            warmup_target_total = preload_ready.get("gatewayAcceptedTotal", 0) + args.warmup_orders
            phase = "wait_warmup_primary"
            warmup_ready = wait_for(
                "binary warmup progression",
                lambda: (lambda h: h if h.get("gatewayAcceptedTotal", 0) >= warmup_target_total else None)(
                    fetch_json(f"{args.base_url}/api/v1/runtime/health")
                ),
                90.0,
                args.poll_interval_seconds,
            )
            warmup_target_sequence = warmup_ready.get("lastDurableSequence", 0)
            if args.standby_base_url:
                def standby_warmup_ready():
                    payloads = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url]
                    if mode_ready(args.standby_commit_mode, payloads, warmup_target_sequence):
                        return payloads
                    return None

                phase = "wait_warmup_standby_durable"
                standby_before = wait_for(
                    "standby warmup durable progression for requested commit mode",
                    standby_warmup_ready,
                    90.0,
                    args.poll_interval_seconds,
                )
            else:
                standby_before = []
            health_before = warmup_ready
        else:
            health_before = preload_ready
            standby_before = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url] if args.standby_base_url else []

        start_latch = threading.Event()
        latencies = []
        rejected = 0
        max_sequence = 0
        submit_started = time.perf_counter_ns()
        phase = "run_binary_workload"
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = []
            orders_per_worker = args.crossing_orders // args.concurrency
            remainder = args.crossing_orders % args.concurrency
            next_order_id = args.order_id_start + args.resting_orders + args.warmup_orders
            for worker in range(args.concurrency):
                worker_orders = orders_per_worker + (1 if worker < remainder else 0)
                futures.append(executor.submit(
                    run_worker,
                    start_latch,
                    args.binary_host,
                    args.binary_port,
                    args.socket_timeout_seconds,
                    args.batch_size,
                    next_order_id,
                    worker_orders,
                ))
                next_order_id += worker_orders
            start_latch.set()
            for future in as_completed(futures):
                result = future.result()
                latencies.extend(result["latenciesMs"])
                rejected += result["rejected"]
                max_sequence = max(max_sequence, result["maxSequence"])
        submit_completed = time.perf_counter_ns()

        expected_trade_count = health_before.get("tradeCount", 0) + args.crossing_orders

        phase = "wait_trade_progression"
        wait_for(
            "crossing trades",
            lambda: (lambda h: h if h.get("tradeCount", 0) >= expected_trade_count else None)(
                fetch_json(f"{args.base_url}/api/v1/runtime/health")
            ),
            90.0,
            args.poll_interval_seconds,
        )

        phase = "wait_replication_committed"
        timed_out = False
        timeout_error = ""
        try:
            def committed_ready():
                nonlocal standby_after
                primary = fetch_json(f"{args.base_url}/api/v1/runtime/health")
                if primary.get("replicationCommittedSequence", 0) >= max_sequence:
                    return primary
                return None
            health_after = wait_for("replication committed progression", committed_ready, args.commit_timeout_seconds, args.poll_interval_seconds)
        except Exception as exc:
            timed_out = True
            timeout_error = str(exc)
            health_after = fetch_json(f"{args.base_url}/api/v1/runtime/health")
        committed_completed = time.perf_counter_ns()
        standby_after = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url]
    except Exception as exc:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps({
            "success": False,
            "scenario": "binary_replication_commit_benchmark",
            "category": "matcher-benchmark",
            "severity": "critical",
            "phase": phase,
            "conclusion": f"binary replication commit benchmark failed during {phase}: {exc}",
            "healthAfter": health_after,
            "standbyAfter": standby_after,
        }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return 2

    submit_elapsed_seconds = (submit_completed - submit_started) / 1_000_000_000.0
    total_elapsed_seconds = (committed_completed - submit_started) / 1_000_000_000.0
    commit_catchup_seconds = max(0.0, total_elapsed_seconds - submit_elapsed_seconds)
    accepted_commands = max(0, health_after.get("gatewayAcceptedTotal", 0) - health_before.get("gatewayAcceptedTotal", 0))
    trade_events = max(0, health_after.get("tradeCount", 0) - health_before.get("tradeCount", 0))
    before_committed = health_before.get("replicationCommittedSequence", health_before.get("lastDurableSequence", 0))
    after_committed = health_after.get("replicationCommittedSequence", health_after.get("lastDurableSequence", 0))
    if standby_before and standby_after:
        before_mode_durable = mode_watermark(args.standby_commit_mode, standby_before)
        after_mode_durable = mode_watermark(args.standby_commit_mode, standby_after)
        before_all = all_standbys_watermark(standby_before)
        after_all = all_standbys_watermark(standby_after)
    else:
        before_mode_durable = before_committed
        after_mode_durable = after_committed
        before_all = before_committed
        after_all = after_committed
    committed_submissions = min(accepted_commands, watermark_delta(before_committed, after_committed))
    mode_durable_submissions = watermark_delta(before_mode_durable, after_mode_durable)
    fully_durable_on_all_standbys = watermark_delta(before_all, after_all)
    success = (not timed_out) and rejected == 0 and committed_submissions >= accepted_commands
    report = {
        "success": success,
        "scenario": "binary_replication_commit_benchmark",
        **benchmark_metadata(),
        "category": "matcher-benchmark",
        "severity": "ok" if success else "critical",
        "conclusion": "binary replication committed chain caught up within the benchmark window"
        if success
        else f"binary replication committed chain did not catch up within the benchmark window: {timeout_error or 'committed submissions lagged behind accepted submissions'}",
        "transport": health_after.get("replicationTransport"),
        "baseUrl": args.base_url,
        "binaryHost": args.binary_host,
        "binaryPort": args.binary_port,
        "standbyCommitMode": args.standby_commit_mode,
        "restingOrders": args.resting_orders,
        "concurrency": args.concurrency,
        "batchSize": args.batch_size,
        "submitWindowSeconds": submit_elapsed_seconds,
        "totalWindowSeconds": total_elapsed_seconds,
        "commitCatchupSeconds": commit_catchup_seconds,
        "latency": summarize_latencies(latencies),
        "acceptedCommands": accepted_commands,
        "processedCommands": max(0, health_after.get("processedCommandCount", 0) - health_before.get("processedCommandCount", 0)),
        "tradeEvents": trade_events,
        "matchedOrderSides": trade_events * 2,
        "replicationCommittedSubmissions": committed_submissions,
        "standbyCommitModeDurableCommands": mode_durable_submissions,
        "allStandbysDurableCommands": fully_durable_on_all_standbys,
        "rejectedCommands": rejected,
        "submissionPendingDelta": health_after.get("submissionPendingCount", 0) - health_before.get("submissionPendingCount", 0),
        "acceptedCommandsPerSecond": 0.0 if submit_elapsed_seconds == 0.0 else accepted_commands / submit_elapsed_seconds,
        "tradeEventsPerSecond": 0.0 if submit_elapsed_seconds == 0.0 else trade_events / submit_elapsed_seconds,
        "replicationCommittedSubmissionsPerSecond": 0.0 if total_elapsed_seconds == 0.0 else committed_submissions / total_elapsed_seconds,
        "standbyCommitModeDurableCommandsPerSecond": 0.0 if total_elapsed_seconds == 0.0 else mode_durable_submissions / total_elapsed_seconds,
        "allStandbysDurableCommandsPerSecond": 0.0 if total_elapsed_seconds == 0.0 else fully_durable_on_all_standbys / total_elapsed_seconds,
        "healthBefore": health_before,
        "healthAfter": health_after,
        "standbyBefore": standby_before,
        "standbyAfter": standby_after,
        "timedOut": timed_out,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(Path(args.report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
