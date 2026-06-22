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


REQUEST_MAGIC = 0x554C4C42
RESPONSE_MAGIC = 0x554C4C52
PROTOCOL_VERSION = 1
FRAME_TYPE_NEW_ORDER_BATCH = 1
FRAME_TYPE_CANCEL_ORDER_BATCH = 2
FRAME_TYPE_BATCH_RESULT = 101
NEW_ORDER_RECORD_BYTES = 48
CANCEL_RECORD_BYTES = 16
RESPONSE_RECORD_BYTES = 24


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


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
        return self._round_trip(FRAME_TYPE_NEW_ORDER_BATCH, payload, len(records))

    def send_cancels(self, order_ids):
        payload = bytearray()
        for order_id in order_ids:
            payload.extend(struct.pack(">QQ", order_id, 0))
        return self._round_trip(FRAME_TYPE_CANCEL_ORDER_BATCH, payload, len(order_ids))

    def _round_trip(self, frame_type: int, payload: bytes, record_count: int):
        header = struct.pack(">IHHII", REQUEST_MAGIC, PROTOCOL_VERSION, frame_type, record_count, len(payload))
        started = time.perf_counter_ns()
        self.sock.sendall(header + payload)
        response_header = self._recv_exact(16)
        magic, version, response_frame_type, response_count, payload_bytes = struct.unpack(">IHHII", response_header)
        if magic != RESPONSE_MAGIC or version != PROTOCOL_VERSION or response_frame_type != FRAME_TYPE_BATCH_RESULT:
            raise IOError("binary ingress response header mismatch")
        if response_count != record_count or payload_bytes != response_count * RESPONSE_RECORD_BYTES:
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


def run_warmup(binary_host: str,
               binary_port: int,
               timeout_seconds: float,
               warmup_orders: int,
               batch_size: int,
               order_id_start: int):
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


def run_crossing_worker(start_latch: threading.Event, binary_host: str, binary_port: int, timeout_seconds: float, batch_size: int, order_id_base: int, order_count: int):
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


def run_mixed_worker(start_latch: threading.Event,
                     binary_host: str,
                     binary_port: int,
                     timeout_seconds: float,
                     maker_batch_size: int,
                     taker_batch_size: int,
                     rounds: int,
                     maker_order_id_base: int,
                     taker_order_id_base: int):
    session = BinarySession(binary_host, binary_port, timeout_seconds)
    latencies = []
    rejected = 0
    accepted_new = 0
    accepted_cancel = 0
    max_sequence = 0
    posted_order_ids = []
    try:
        start_latch.wait()
        maker_cursor = 0
        taker_cursor = 0
        for _round in range(rounds):
            maker_records = [
                build_new_order(maker_order_id_base + maker_cursor + i, 3, "S", "L", "G", 102, 1)
                for i in range(maker_batch_size)
            ]
            maker_cursor += maker_batch_size
            maker_responses, maker_latency_ms = session.send_new_orders(maker_records)
            per_order_maker_latency = maker_latency_ms / max(1, maker_batch_size)
            for item, response in zip(maker_records, maker_responses):
                latencies.append(per_order_maker_latency)
                if status_to_result(response["status"]) == "ACCEPTED":
                    accepted_new += 1
                    posted_order_ids.append(item["orderId"])
                    max_sequence = max(max_sequence, response["sequence"])
                else:
                    rejected += 1

            if posted_order_ids:
                cancel_ids = posted_order_ids[:maker_batch_size]
                del posted_order_ids[:maker_batch_size]
                cancel_responses, cancel_latency_ms = session.send_cancels(cancel_ids)
                per_cancel_latency = cancel_latency_ms / max(1, len(cancel_ids))
                for response in cancel_responses:
                    latencies.append(per_cancel_latency)
                    if status_to_result(response["status"]) == "ACCEPTED":
                        accepted_cancel += 1
                        max_sequence = max(max_sequence, response["sequence"])
                    else:
                        rejected += 1

            taker_records = [
                build_new_order(taker_order_id_base + taker_cursor + i, 1, "B", "L", "I", 101, 1)
                for i in range(taker_batch_size)
            ]
            taker_cursor += taker_batch_size
            taker_responses, taker_latency_ms = session.send_new_orders(taker_records)
            per_taker_latency = taker_latency_ms / max(1, taker_batch_size)
            for response in taker_responses:
                latencies.append(per_taker_latency)
                if status_to_result(response["status"]) == "ACCEPTED":
                    accepted_new += 1
                    max_sequence = max(max_sequence, response["sequence"])
                else:
                    rejected += 1
    finally:
        session.close()
    return {
        "latenciesMs": latencies,
        "rejected": rejected,
        "acceptedNew": accepted_new,
        "acceptedCancel": accepted_cancel,
        "maxSequence": max_sequence,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("crossing", "mixed"), required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--standby-base-url", action="append", default=[])
    parser.add_argument("--binary-host", default="127.0.0.1")
    parser.add_argument("--binary-port", type=int, required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--resting-orders", type=int, default=2048)
    parser.add_argument("--concurrency", type=int, default=24)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--crossing-orders", type=int, default=2048)
    parser.add_argument("--mixed-rounds", type=int, default=64)
    parser.add_argument("--mixed-maker-batch-size", type=int, default=16)
    parser.add_argument("--mixed-taker-batch-size", type=int, default=16)
    parser.add_argument("--socket-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--warmup-orders", type=int, default=256)
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
        )
        preload_target_sequence = preload_ready.get("lastDurableSequence", 0)
        if args.standby_base_url:
            def standby_preload_ready():
                payloads = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url]
                if all(
                    item.get("lastDurableSequence", 0) >= preload_target_sequence
                    for item in payloads
                ):
                    return payloads
                return None

            phase = "wait_preload_standby_durable"
            wait_for("standby preload durable progression", standby_preload_ready, 90.0)
        else:
            pass

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
            )
            warmup_target_sequence = warmup_ready.get("lastDurableSequence", 0)
            if args.standby_base_url:
                def standby_warmup_ready():
                    payloads = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url]
                    if all(
                        item.get("lastDurableSequence", 0) >= warmup_target_sequence
                        for item in payloads
                    ):
                        return payloads
                    return None

                phase = "wait_warmup_standby_durable"
                standby_before = wait_for("standby warmup durable progression", standby_warmup_ready, 90.0)
            else:
                standby_before = []
            health_before = warmup_ready
            next_order_id_start += args.warmup_orders
        else:
            health_before = preload_ready
            standby_before = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url] if args.standby_base_url else []

        start_latch = threading.Event()
        latencies = []
        rejected = 0
        accepted_new = 0
        accepted_cancel = 0
        max_sequence = 0
        started = time.perf_counter_ns()
        phase = "run_binary_workload"
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = []
            if args.mode == "crossing":
                orders_per_worker = args.crossing_orders // args.concurrency
                remainder = args.crossing_orders % args.concurrency
                next_order_id = next_order_id_start
                for worker in range(args.concurrency):
                    worker_orders = orders_per_worker + (1 if worker < remainder else 0)
                    futures.append(executor.submit(
                        run_crossing_worker,
                        start_latch,
                        args.binary_host,
                        args.binary_port,
                        args.socket_timeout_seconds,
                        args.batch_size,
                        next_order_id,
                        worker_orders,
                    ))
                    next_order_id += worker_orders
            else:
                next_maker = args.order_id_start + args.resting_orders
                next_taker = args.order_id_start + args.resting_orders + args.concurrency * args.mixed_rounds * args.mixed_maker_batch_size
                for _worker in range(args.concurrency):
                    futures.append(executor.submit(
                        run_mixed_worker,
                        start_latch,
                        args.binary_host,
                        args.binary_port,
                        args.socket_timeout_seconds,
                        args.mixed_maker_batch_size,
                        args.mixed_taker_batch_size,
                        args.mixed_rounds,
                        next_maker,
                        next_taker,
                    ))
                    next_maker += args.mixed_rounds * args.mixed_maker_batch_size
                    next_taker += args.mixed_rounds * args.mixed_taker_batch_size

            start_latch.set()
            for future in as_completed(futures):
                result = future.result()
                latencies.extend(result["latenciesMs"])
                rejected += result["rejected"]
                accepted_new += result.get("acceptedNew", 0)
                accepted_cancel += result.get("acceptedCancel", 0)
                max_sequence = max(max_sequence, result["maxSequence"])

        expected_trade_count = health_before.get("tradeCount", 0) + args.crossing_orders

        def standby_progress():
            if not args.standby_base_url:
                return True, []
            payloads = [fetch_json(f"{url}/api/v1/runtime/health") for url in args.standby_base_url]
            durable_count = sum(1 for item in payloads if item.get("lastDurableSequence", 0) >= max_sequence)
            required = required_standby_acks(args.standby_commit_mode, len(payloads))
            return durable_count >= required, payloads

        def health_ready():
            nonlocal standby_after
            primary = fetch_json(f"{args.base_url}/api/v1/runtime/health")
            committed_ready, payloads = standby_progress()
            standby_after = payloads
            trade_ready = args.mode != "crossing" or primary.get("tradeCount", 0) >= expected_trade_count
            if trade_ready and committed_ready:
                return primary
            return None

        phase = "wait_trade_and_committed_progression"
        health_after = wait_for("trade and committed progression", health_ready, 90.0)
        elapsed_seconds = (time.perf_counter_ns() - started) / 1_000_000_000.0
    except Exception as exc:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps({
            "success": False,
            "scenario": f"binary_{args.mode}_benchmark",
            "category": "matcher-benchmark",
            "severity": "critical",
            "phase": phase,
            "conclusion": f"binary benchmark failed during {phase}: {exc}",
            "healthAfter": health_after,
            "standbyAfter": standby_after,
        }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return 2

    accepted_commands = max(0, health_after.get("gatewayAcceptedTotal", 0) - health_before.get("gatewayAcceptedTotal", 0))
    processed_commands = max(0, health_after.get("processedCommandCount", 0) - health_before.get("processedCommandCount", 0))
    trade_events = max(0, health_after.get("tradeCount", 0) - health_before.get("tradeCount", 0))
    if standby_before and standby_after:
        before_any = max(item.get("lastDurableSequence", 0) for item in standby_before)
        after_any = max(item.get("lastDurableSequence", 0) for item in standby_after)
        before_all = min(item.get("lastDurableSequence", 0) for item in standby_before)
        after_all = min(item.get("lastDurableSequence", 0) for item in standby_after)
    else:
        before_any = health_before.get("submissionCommittedCount", 0)
        after_any = health_after.get("submissionCommittedCount", 0)
        before_all = before_any
        after_all = after_any
    committed_submissions = max(0, after_any - before_any)
    fully_durable_on_all_standbys = max(0, after_all - before_all)
    report = {
        "success": rejected == 0,
        "scenario": f"binary_{args.mode}_benchmark",
        "category": "matcher-benchmark",
        "severity": "ok" if rejected == 0 else "critical",
        "conclusion": "binary ingress benchmark completed" if rejected == 0 else "binary ingress benchmark completed with rejected commands",
        "transport": health_after.get("replicationTransport"),
        "baseUrl": args.base_url,
        "binaryHost": args.binary_host,
        "binaryPort": args.binary_port,
        "mode": args.mode,
        "standbyCommitMode": args.standby_commit_mode,
        "restingOrders": args.resting_orders,
        "concurrency": args.concurrency,
        "batchSize": args.batch_size,
        "elapsedSeconds": elapsed_seconds,
        "acceptedCommands": accepted_commands,
        "processedCommands": processed_commands,
        "tradeEvents": trade_events,
        "matchedOrderSides": trade_events * 2,
        "replicationCommittedSubmissions": committed_submissions,
        "allStandbysDurableCommands": fully_durable_on_all_standbys,
        "clientAcceptedNewOrders": accepted_new if args.mode == "mixed" else accepted_commands,
        "clientAcceptedCancelCommands": accepted_cancel,
        "rejectedCommands": rejected,
        "submissionPendingDelta": health_after.get("submissionPendingCount", 0) - health_before.get("submissionPendingCount", 0),
        "latency": summarize_latencies(latencies),
        "acceptedCommandsPerSecond": 0.0 if elapsed_seconds == 0.0 else accepted_commands / elapsed_seconds,
        "processedCommandsPerSecond": 0.0 if elapsed_seconds == 0.0 else processed_commands / elapsed_seconds,
        "tradeEventsPerSecond": 0.0 if elapsed_seconds == 0.0 else trade_events / elapsed_seconds,
        "matchedOrderSidesPerSecond": 0.0 if elapsed_seconds == 0.0 else (trade_events * 2) / elapsed_seconds,
        "replicationCommittedSubmissionsPerSecond": 0.0 if elapsed_seconds == 0.0 else committed_submissions / elapsed_seconds,
        "allStandbysDurableCommandsPerSecond": 0.0 if elapsed_seconds == 0.0 else fully_durable_on_all_standbys / elapsed_seconds,
        "healthBefore": health_before,
        "healthAfter": health_after,
        "standbyBefore": standby_before,
        "standbyAfter": standby_after,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(Path(args.report))
    return 0


def required_standby_acks(mode: str, standby_count: int) -> int:
    if mode == "any":
        return min(1, standby_count)
    if mode == "quorum":
        return 0 if standby_count == 0 else (standby_count // 2) + 1
    if mode == "all":
        return standby_count
    raise ValueError(f"unsupported standby commit mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())
