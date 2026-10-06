#!/usr/bin/env python3
"""Thread-local HTTP/1.1 keep-alive for REST benchmarks (stdlib only)."""

from __future__ import annotations

import http.client
import json
import threading
from urllib.error import HTTPError
from urllib.parse import urlparse

_thread = threading.local()


class KeepAliveHttp:
    def __init__(self, base_url: str, timeout: float = 10.0):
        parsed = urlparse(base_url)
        if not parsed.hostname:
            raise ValueError(f"invalid base url: {base_url}")
        self.host = parsed.hostname
        self.port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self.scheme = parsed.scheme
        self.timeout = timeout

    def request_json(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
        body = None
        headers = {"Accept": "application/json", "Connection": "keep-alive"}
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        return self._request(method, path, body, headers)

    def _request(self, method: str, path: str, body: bytes | None, headers: dict[str, str]) -> tuple[int, dict]:
        try:
            return self._exchange(self._conn(), method, path, body, headers)
        except (http.client.HTTPException, OSError, json.JSONDecodeError):
            self.reset()
            return self._exchange(self._conn(), method, path, body, headers)

    def _exchange(
            self,
            conn: http.client.HTTPConnection,
            method: str,
            path: str,
            body: bytes | None,
            headers: dict[str, str],
    ) -> tuple[int, dict]:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        raw = response.read()
        parsed = json.loads(raw.decode("utf-8")) if raw else {}
        return response.status, parsed

    def _conn(self) -> http.client.HTTPConnection:
        key = (self.scheme, self.host, self.port)
        conn = getattr(_thread, "conn", None)
        if conn is not None and getattr(_thread, "conn_key", None) == key:
            return conn
        self.reset()
        if self.scheme == "https":
            conn = http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout)
        else:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        _thread.conn = conn
        _thread.conn_key = key
        return conn

    def reset(self) -> None:
        conn = getattr(_thread, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass
        _thread.conn = None
        _thread.conn_key = None


def get_json(url: str, timeout: float = 5.0) -> dict:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    status, body = KeepAliveHttp(origin, timeout=timeout).request_json("GET", path)
    if status >= 400:
        raise HTTPError(url, status, str(body), hdrs=None, fp=None)
    return body
