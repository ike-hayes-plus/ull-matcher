#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ull-python-scripts.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

mapfile -t PYTHON_SCRIPTS < <(find "${ROOT_DIR}/scripts" -name '*.py' -type f | sort)

if [[ "${#PYTHON_SCRIPTS[@]}" -eq 0 ]]; then
  echo "no Python scripts found" >&2
  exit 1
fi

PYTHONPYCACHEPREFIX="${TMP_DIR}/pycache" python3 -m py_compile "${PYTHON_SCRIPTS[@]}"

echo "python script compile self-test passed (${#PYTHON_SCRIPTS[@]} files)"
