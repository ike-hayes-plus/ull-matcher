#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def node_http_port(node_name: str) -> int:
    return {
        "node-a": int(os.environ.get("LAB_HTTP_PORT_NODE_A", "8080")),
        "node-b": int(os.environ.get("LAB_HTTP_PORT_NODE_B", "8081")),
        "node-c": int(os.environ.get("LAB_HTTP_PORT_NODE_C", "8082")),
    }.get(node_name, int(os.environ.get("LAB_HTTP_PORT_NODE_A", "8080")))


def node_grpc_port(node_name: str) -> int:
    return {
        "node-a": int(os.environ.get("LAB_GRPC_PORT_NODE_A", "9190")),
        "node-b": int(os.environ.get("LAB_GRPC_PORT_NODE_B", "9191")),
        "node-c": int(os.environ.get("LAB_GRPC_PORT_NODE_C", "9192")),
    }.get(node_name, int(os.environ.get("LAB_GRPC_PORT_NODE_A", "9190")))


def node_aeron_port(node_name: str) -> int:
    return {
        "node-a": int(os.environ.get("LAB_AERON_PORT_NODE_A", "15090")),
        "node-b": int(os.environ.get("LAB_AERON_PORT_NODE_B", "15091")),
        "node-c": int(os.environ.get("LAB_AERON_PORT_NODE_C", "15092")),
    }.get(node_name, int(os.environ.get("LAB_AERON_PORT_NODE_A", "15090")))


def usage() -> int:
    print(
        "usage: generate-transport-change-runbook.py "
        "--report <file> --validation <file> --rollout <file> --target-transport <name> --window-id <id>",
        file=sys.stderr,
    )
    return 1


def main() -> int:
    args = sys.argv[1:]
    report_path = None
    validation_path = None
    rollout_path = None
    target_transport = None
    window_id = None
    while args[:1]:
        if args[0] == "--report" and len(args) >= 2:
            report_path = Path(args[1])
            args = args[2:]
            continue
        if args[0] == "--validation" and len(args) >= 2:
            validation_path = Path(args[1])
            args = args[2:]
            continue
        if args[0] == "--rollout" and len(args) >= 2:
            rollout_path = Path(args[1])
            args = args[2:]
            continue
        if args[0] == "--target-transport" and len(args) >= 2:
            target_transport = args[1]
            args = args[2:]
            continue
        if args[0] == "--window-id" and len(args) >= 2:
            window_id = args[1]
            args = args[2:]
            continue
        return usage()

    if not report_path or not validation_path or not rollout_path or not target_transport or not window_id:
        return usage()

    validation = load_json(validation_path)
    rollout = load_json(rollout_path)
    nodes = rollout.get("nodes", [])
    current_transport = None
    primary_name = None
    standby_names = []
    for check in validation.get("checks", []):
        if check.get("name") == "single_primary":
            primary_name = check.get("details", {}).get("primaryNode")
            break
    for node in nodes:
        current_transport = current_transport or node.get("transport")
        if node.get("name") == primary_name:
            continue
        standby_names.append(node.get("name"))
    if primary_name:
        ordered_nodes = standby_names + [primary_name]
    else:
        ordered_nodes = [node.get("name") for node in nodes]

    lines = [
        "# Transport Change Runbook",
        "",
        f"- Current transport: `{current_transport or 'UNKNOWN'}`",
        f"- Target transport: `{target_transport}`",
        f"- Change window id: `{window_id}`",
        "",
        "## Preconditions",
        "",
        "```bash",
        "./scripts/chaos/cluster.sh validate",
        "```",
        "",
        "确认 `validation-report.json` 和 `transport-rollout-report.json` 都是通过状态，再开始切换。",
        "",
        "## Phase 1: Open change window and roll standbys first",
        "",
    ]
    for node_name in ordered_nodes:
        aeron_port = node_aeron_port(node_name)
        http_port = node_http_port(node_name)
        grpc_port = node_grpc_port(node_name)
        role_hint = "primary last" if node_name == primary_name else "standby"
        lines.extend([
            f"### {node_name} ({role_hint})",
            "",
            "```bash",
            f"./scripts/lab/stop-node.sh {node_name}",
            (
                f"ALLOW_TRANSPORT_CHANGE=true TRANSPORT_CHANGE_WINDOW_ID={window_id} "
                f"REPLICATION_TRANSPORT={target_transport} "
                f"./scripts/lab/start-node.sh {node_name} {http_port} {grpc_port} {aeron_port}"
            ),
            "./scripts/chaos/cluster.sh validate",
            "```",
            "",
        ])
    lines.extend([
        "## Phase 2: Verify change window closes over a uniform cluster",
        "",
        "```bash",
        f"./scripts/run-chaos-tests.sh transport-validate target/chaos-lab/http_127.0.0.1_{node_http_port('node-a')} \\",
        f"  target/chaos-lab/http_127.0.0.1_{node_http_port('node-b')} target/chaos-lab/http_127.0.0.1_{node_http_port('node-c')}",
        "./scripts/chaos/cluster.sh failover-smoke",
        "```",
        "",
        "## Phase 3: Security Material Rotation Window",
        "",
        "如果当前集群启用了 `matcher.transportTls*`，在关闭传输切换窗口前，按 `standbys -> primary` 顺序轮换证书材料，并验证 session 已失效后自动重建。",
        "",
    ])
    for node_name in ordered_nodes:
        lines.extend([
            f"### Rotate transport certificate on {node_name}",
            "",
            "```bash",
            f"./scripts/lab/rotate-transport-certificate.sh {node_name}",
            "./scripts/chaos/cluster.sh validate",
            "# 提交一笔 probe order，确认复制/控制面/快照通道在新证书代际下恢复",
            "```",
            "",
        ])
    lines.extend([
        "轮换窗口结束前，确认 `security-rotation-report.json` 为通过状态，且所有节点 `transportSecurityReloadCount` 已增加。",
        "",
        "## Phase 4: Remove change window flags",
        "",
        "再次按 `standbys -> primary` 顺序逐个重启节点，但去掉 `ALLOW_TRANSPORT_CHANGE` 和 `TRANSPORT_CHANGE_WINDOW_ID`，保持同一个 `REPLICATION_TRANSPORT`。",
        "",
        "```bash",
        "# 示例",
        f"REPLICATION_TRANSPORT={target_transport} ./scripts/lab/start-node.sh node-a {node_http_port('node-a')} {node_grpc_port('node-a')} {node_aeron_port('node-a')}",
        "```",
        "",
        "最后再跑一次：",
        "",
        "```bash",
        "./scripts/chaos/cluster.sh validate",
        "./scripts/chaos/cluster.sh failover-smoke",
        "```",
        "",
    ])
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
