#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def main() -> int:
    args = sys.argv[1:]
    report_path = None
    if len(args) >= 2 and args[0] == "--report":
        report_path = Path(args[1])
        args = args[2:]

    if len(args) < 1:
        print("usage: validate-cluster-state.py [--report <file>] <node-dir> [<node-dir> ...]", file=sys.stderr)
        return 1

    nodes = []
    for raw in args:
        node_dir = Path(raw)
        health = load_json(node_dir / "health.json")
        readiness = load_json(node_dir / "readiness.json")
        nodes.append((node_dir.name, health, readiness))

    primaries = [n for n in nodes if n[1].get("role") == "PRIMARY"]
    traffic_ready = [n for n in nodes if n[2].get("clientTrafficReady") is True]
    transport_drift = [n for n in nodes if n[2].get("transportPolicyStatus") == "DRIFT"]

    errors = []
    exit_code = 0
    if len(primaries) == 0:
        errors.append("no PRIMARY node found")
        exit_code = 2
    if len(primaries) > 1:
        errors.append("more than one PRIMARY node")
        exit_code = 2
    if len(traffic_ready) == 0:
        errors.append("no node has clientTrafficReady=true")
        exit_code = max(exit_code, 3)
    if len(traffic_ready) > 1:
        errors.append("more than one node has clientTrafficReady=true")
        exit_code = max(exit_code, 3)
    if primaries and traffic_ready and primaries[0][0] != traffic_ready[0][0]:
        errors.append("PRIMARY node and clientTrafficReady node do not match")
        exit_code = max(exit_code, 4)
    if transport_drift:
        errors.append("one or more nodes report transportPolicyStatus=DRIFT")
        exit_code = max(exit_code, 5)

    checks = [
        {
            "name": "single_primary",
            "passed": len(primaries) == 1,
            "detail": "exactly one PRIMARY node must exist",
        },
        {
            "name": "single_client_traffic_ready",
            "passed": len(traffic_ready) == 1,
            "detail": "exactly one node must expose clientTrafficReady=true",
        },
        {
            "name": "primary_matches_client_traffic_ready",
            "passed": len(primaries) == 1 and len(traffic_ready) == 1 and primaries[0][0] == traffic_ready[0][0],
            "detail": "PRIMARY node must match the node accepting client traffic",
        },
        {
            "name": "no_transport_drift",
            "passed": not transport_drift,
            "detail": "no node may report transportPolicyStatus=DRIFT",
        },
    ]
    severity = "ok" if exit_code == 0 else "critical"
    conclusion = "cluster validation passed" if exit_code == 0 else "cluster validation failed"

    report = {
        "scenario": "cluster_state_validation",
        "category": "ha-readiness",
        "valid": exit_code == 0,
        "severity": severity,
        "conclusion": conclusion,
        "errors": errors,
        "checks": checks,
        "primaryNodes": [n[0] for n in primaries],
        "clientTrafficReadyNodes": [n[0] for n in traffic_ready],
        "nodes": [
            {
                "name": name,
                "role": health.get("role"),
                "replicationTransport": health.get("replicationTransport"),
                "clientTrafficReady": readiness.get("clientTrafficReady"),
                "serviceReady": readiness.get("serviceReady"),
                "transportPolicyStatus": readiness.get("transportPolicyStatus"),
                "transportReconciliationStatus": readiness.get("transportReconciliationStatus"),
            }
            for name, health, readiness in nodes
        ],
    }
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    for name, health, readiness in nodes:
        print(f"{name}: role={health.get('role')} clientTrafficReady={readiness.get('clientTrafficReady')} serviceReady={readiness.get('serviceReady')}")
    for error in errors:
        print(f"invalid: {error}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
