#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"
COMPOSE_FILE="$ROOT_DIR/deploy/docker-compose/chaos-lab/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" up -d --remove-orphans

cat <<EOF
chaos lab started

services:
  zookeeper  : localhost:${LAB_ZOOKEEPER_PORT}, localhost:${LAB_ZOOKEEPER_PORT_NODE_2}, localhost:${LAB_ZOOKEEPER_PORT_NODE_3}
  etcd       : localhost:${LAB_ETCD_PORT}, localhost:${LAB_ETCD_PORT_NODE_2}, localhost:${LAB_ETCD_PORT_NODE_3}
  toxiproxy  : localhost:${LAB_TOXIPROXY_PORT}
  prometheus : localhost:${LAB_PROMETHEUS_PORT}

note:
  run matcher-server nodes on host machine for higher-fidelity performance and CPU binding tests
EOF
