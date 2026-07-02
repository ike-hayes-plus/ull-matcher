#!/usr/bin/env python3
"""Fetch and format matcher node health for deploy scripts."""

from __future__ import annotations

import json
import sys
import urllib.request


def fetch_health(node_id: str, url: str) -> dict:
    with urllib.request.urlopen(url, timeout=3) as response:
        payload = json.loads(response.read().decode("utf-8"))
    payload["_deployNodeId"] = node_id
    payload["_deployUrl"] = url
    return payload


def print_json(node_id: str, url: str) -> None:
    print(json.dumps(fetch_health(node_id, url), separators=(",", ":")))


def print_status(node_id: str, url: str) -> None:
    try:
        payload = fetch_health(node_id, url)
    except Exception as exc:  # noqa: BLE001 - deploy CLI should print the concrete failure.
        print(f"{node_id} {url} DOWN {exc}")
        raise SystemExit(1) from exc
    print(
        f"{node_id} {url} role={payload.get('role')} "
        f"accepting={payload.get('acceptingClientCommands')} "
        f"ready={payload.get('serviceReady')} "
        f"durable={payload.get('lastDurableSequence')} "
        f"applied={payload.get('lastAppliedSequence')} "
        f"committed={payload.get('replicationCommittedSequence')}"
    )


def main(argv: list[str]) -> int:
    if len(argv) != 4 or argv[1] not in {"json", "status"}:
        print("usage: health_probe.py json|status <node-id> <health-url>", file=sys.stderr)
        return 2
    mode, node_id, url = argv[1], argv[2], argv[3]
    if mode == "json":
        print_json(node_id, url)
    else:
        print_status(node_id, url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
