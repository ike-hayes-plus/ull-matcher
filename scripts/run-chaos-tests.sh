#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/use-project-java.sh"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lab/port-layout.sh"

if [[ "${1:-}" == "env" ]]; then
  shift
  exec "${ROOT_DIR}/scripts/chaos/run-env-chaos.sh" "$@"
fi

if [[ "${1:-}" == "collect" ]]; then
  shift
  exec "${ROOT_DIR}/scripts/chaos/collect-node-state.sh" "$@"
fi

if [[ "${1:-}" == "validate" ]]; then
  shift
  exec "${ROOT_DIR}/scripts/chaos/validate-cluster-state.py" "$@"
fi

if [[ "${1:-}" == "transport-validate" ]]; then
  shift
  exec "${ROOT_DIR}/scripts/chaos/validate-transport-rollout.py" "$@"
fi

if [[ "${1:-}" == "summarize" ]]; then
  shift
  exec "${ROOT_DIR}/scripts/chaos/summarize-chaos-report.py" "$@"
fi

if [[ "${1:-}" == "lab" ]]; then
  shift
  exec "${ROOT_DIR}/scripts/chaos/lab.sh" "$@"
fi

if [[ "${1:-}" == "cluster" ]]; then
  shift
  exec "${ROOT_DIR}/scripts/chaos/cluster.sh" "$@"
fi

cd "${ROOT_DIR}"
mvn -Pchaos-tests test "$@"
