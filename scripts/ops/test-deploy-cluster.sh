#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ull-deploy-cluster-test.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

CONFIG_FILE="${TMP_DIR}/cluster.env"

write_config() {
  local extra="${1:-}"
  cat >"${CONFIG_FILE}" <<EOF
CLUSTER_NAME="test-cluster"
SHARD_KEY="merchant:test"
SYMBOL_ID="7"
REMOTE_ROOT="${ROOT_DIR}"
DEPLOY_HEALTH_TIMEOUT_SECONDS=1
DEPLOY_STABLE_ROUNDS=1
DEPLOY_POLL_INTERVAL_SECONDS=1
${extra}
NODES=(
  "node-a|127.0.0.1|18080|19190|25090|-|/var/lib/ull-matcher-test/node-a|/var/log/ull-matcher-test/node-a"
  "node-b|127.0.0.1|18081|19191|25091|-|/var/lib/ull-matcher-test/node-b|/var/log/ull-matcher-test/node-b"
)
EOF
}

run_cluster() {
  "${ROOT_DIR}/scripts/deploy/cluster.sh" -c "${CONFIG_FILE}" "$@"
}

assert_fails_with() {
  local expected="$1"
  shift
  local output
  if output="$("$@" 2>&1)"; then
    echo "command unexpectedly passed: $*" >&2
    echo "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"$expected"* ]]; then
    echo "expected failure to contain: $expected" >&2
    echo "$output" >&2
    exit 1
  fi
}

write_config
plan_output="$(run_cluster plan node-a)"
if [[ "$plan_output" != *"cluster=test-cluster shard=merchant:test symbol=7"* ]]; then
  echo "plan output did not include cluster/shard/symbol context" >&2
  echo "$plan_output" >&2
  exit 1
fi
if [[ "$plan_output" != *"health-url: http://127.0.0.1:18080/api/v1/runtime/health (resolved on target host)"* ]]; then
  echo "plan output did not include loopback health URL scope" >&2
  echo "$plan_output" >&2
  exit 1
fi

write_config 'WAL_DURABILITY_MODE="BAD_MODE"'
assert_fails_with "WAL_DURABILITY_MODE must be" run_cluster start node-a

write_config 'HTTP_BIND_HOST="0.0.0.0"'
assert_fails_with "ALLOW_INSECURE_REMOTE_HTTP=true" run_cluster plan node-a

cat >"${CONFIG_FILE}" <<EOF
REMOTE_ROOT="${ROOT_DIR}"
NODES=(
  "node-a|127.0.0.1|18080|19190|25090|-|/var/lib/ull-matcher-test/shared|/var/log/ull-matcher-test/node-a"
  "node-b|127.0.0.1|18081|19191|25091|-|/var/lib/ull-matcher-test/shared|/var/log/ull-matcher-test/node-b"
)
EOF
assert_fails_with "duplicate dataDir" run_cluster plan

write_config
assert_fails_with "node not found in config: node-z" run_cluster plan node-z
assert_fails_with "--force is only valid with stop" run_cluster --force plan node-a

write_config
assert_fails_with "cannot verify current cluster health before stopping a node" run_cluster stop node-a

if ! run_cluster stop --force node-a >/dev/null 2>&1; then
  echo "forced stop should bypass health safety checks" >&2
  exit 1
fi

echo "deploy cluster script self-test passed"
