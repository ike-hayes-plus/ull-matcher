#!/usr/bin/env python3
"""Self-test: keep-alive client reuses one TCP connection for two JSON posts."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from http_keepalive import KeepAliveHttp, get_json


class _Handler(BaseHTTPRequestHandler):
    connections = 0

    def setup(self):
        super().setup()
        type(self).connections += 1

    def do_GET(self):
        self._write(200, {"ok": True})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        self._write(200, {"result": "ACCEPTED", "echo": payload})

    def log_message(self, format, *args):
        return

    def _write(self, status: int, body: dict):
        raw = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(raw)


def main() -> int:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        client = KeepAliveHttp(base)
        status_one, body_one = client.request_json("POST", "/api/v1/orders", {"orderId": 1})
        status_two, body_two = client.request_json("POST", "/api/v1/orders", {"orderId": 2})
        health = get_json(f"{base}/api/v1/runtime/health")
        if status_one != 200 or status_two != 200 or body_one.get("result") != "ACCEPTED":
            raise SystemExit(f"unexpected responses: {status_one} {body_one} {status_two} {body_two}")
        if health.get("ok") is not True:
            raise SystemExit(f"unexpected get_json: {health}")
        if _Handler.connections != 1:
            raise SystemExit(f"expected 1 reused TCP connection, got {_Handler.connections}")
    finally:
        server.shutdown()
        server.server_close()
    print("http keep-alive self-test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
