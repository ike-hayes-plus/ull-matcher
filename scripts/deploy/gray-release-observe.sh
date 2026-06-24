#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ "${#}" -lt 2 ]]; then
  cat <<'EOF' >&2
usage:
  scripts/deploy/gray-release-observe.sh <out-dir> <node-base-url> [<node-base-url> ...]

example:
  scripts/deploy/gray-release-observe.sh target/gray/before \
    http://127.0.0.1:8080 http://127.0.0.1:8081
EOF
  exit 1
fi

OUT_DIR="$1"
shift

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

safe_name() {
  echo "$1" | sed 's#https\?://##; s#[/:]#_#g'
}

for base_url in "$@"; do
  node_dir="$OUT_DIR/$(safe_name "$base_url")"
  mkdir -p "$node_dir"
  "${ROOT_DIR}/scripts/ops/collect-health.sh" "$base_url" "$node_dir"
done

python3 "${ROOT_DIR}/scripts/chaos/validate-cluster-state.py" "$OUT_DIR"/* | tee "$OUT_DIR/validation.txt"

echo "[gray-release] wrote cluster snapshot to $OUT_DIR"

