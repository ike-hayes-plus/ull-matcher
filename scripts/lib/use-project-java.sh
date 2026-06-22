#!/usr/bin/env bash
set -euo pipefail

SCRIPT_SOURCE="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_SOURCE}")/../.." && pwd)"
SDKMAN_INIT="${HOME}/.sdkman/bin/sdkman-init.sh"
SDKMAN_RC="${ROOT_DIR}/.sdkmanrc"

if [[ -f "${SDKMAN_INIT}" ]]; then
  # shellcheck disable=SC1090
  set +u
  source "${SDKMAN_INIT}"
  if [[ -f "${SDKMAN_RC}" ]]; then
    _ULL_MATCHER_PREV_PWD="$(pwd)"
    cd "${ROOT_DIR}"
    sdk env >/dev/null
    cd "${_ULL_MATCHER_PREV_PWD}"
    unset _ULL_MATCHER_PREV_PWD
  fi
  set -u
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "[java-env] JAVA_HOME is not set. Install sdkman and run: sdk env install" >&2
  exit 2
fi

if [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
  echo "[java-env] JAVA_HOME does not contain a runnable java binary: ${JAVA_HOME}" >&2
  exit 3
fi

export JAVA_HOME
export PATH="${JAVA_HOME}/bin:${PATH}"
