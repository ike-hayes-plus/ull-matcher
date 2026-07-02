#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_lock(path: Path):
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def main() -> int:
    args = sys.argv[1:]
    report_path = None
    data_root = None
    while args[:1]:
        if args[0] == "--report" and len(args) >= 2:
            report_path = Path(args[1])
            args = args[2:]
            continue
        if args[0] == "--data-root" and len(args) >= 2:
            data_root = Path(args[1])
            args = args[2:]
            continue
        break

    if not args:
        print("usage: validate-transport-rollout.py [--report <file>] [--data-root <dir>] <node-dir> [<node-dir> ...]", file=sys.stderr)
        return 1

    nodes = []
    errors = []
    for raw in args:
        node_dir = Path(raw)
        health = load_json(node_dir / "health.json")
        readiness = load_json(node_dir / "readiness.json")
        node = {
            "name": node_dir.name,
            "transport": health.get("replicationTransport"),
            "policyStatus": readiness.get("transportPolicyStatus"),
            "policyConclusion": readiness.get("transportPolicyConclusion"),
            "reconciliationStatus": readiness.get("transportReconciliationStatus"),
            "transportSecurityGeneration": readiness.get("transportSecurityGeneration", 0),
            "transportSecurityReloadCount": readiness.get("transportSecurityReloadCount", 0),
            "transportSecurityFailureCount": readiness.get("transportSecurityFailureCount", 0),
            "transportSecurityReloadInProgress": readiness.get("tlsReloadInProgress", False),
            "transportSecurityLastError": readiness.get("transportSecurityLastError", ""),
        }
        if data_root is not None:
            node_id = health.get("nodeId")
            lock_file = data_root / node_id / "replication-transport.lock"
            if not lock_file.exists():
                errors.append(f"missing transport lock file for {node_id}: {lock_file}")
            else:
                lock_values = load_lock(lock_file)
                node["lockTransport"] = lock_values.get("transport", "")
                node["lockWindowId"] = lock_values.get("windowId", "")
                if node["lockTransport"] != node["transport"]:
                    errors.append(f"transport lock mismatch for {node_id}: lock={node['lockTransport']} runtime={node['transport']}")
        nodes.append(node)

    transports = {node["transport"] for node in nodes if node["transport"]}
    policy_statuses = {node["policyStatus"] for node in nodes if node["policyStatus"]}
    if "DRIFT" in policy_statuses:
        errors.append("transport policy drift detected in readiness output")
    if len(transports) > 1 and not policy_statuses.issubset({"CHANGE_WINDOW"}):
        errors.append("multiple transport modes detected without all nodes reporting CHANGE_WINDOW")
    if any(node["transportSecurityFailureCount"] for node in nodes):
        errors.append("transport security reload failures detected")

    valid = not errors
    report = {
        "scenario": "transport_rollout_validation",
        "category": "ha-transport",
        "valid": valid,
        "severity": "ok" if valid else "critical",
        "conclusion": "transport rollout validation passed" if valid else "transport rollout validation failed",
        "errors": errors,
        "nodes": nodes,
    }
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    for node in nodes:
        print(f"{node['name']}: transport={node.get('transport')} policyStatus={node.get('policyStatus')} reconciliationStatus={node.get('reconciliationStatus')}")
    for error in errors:
        print(f"invalid: {error}", file=sys.stderr)
    return 0 if valid else 2


if __name__ == "__main__":
    raise SystemExit(main())
