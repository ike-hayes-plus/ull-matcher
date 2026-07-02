#!/usr/bin/env bash
set -euo pipefail

SCRIPT_SOURCE="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_SOURCE}")/../.." && pwd)"
SDKMAN_INIT="${HOME}/.sdkman/bin/sdkman-init.sh"
SDKMAN_RC="${ROOT_DIR}/.sdkmanrc"

export_git_commit() {
  if [[ -z "${ULL_MATCHER_GIT_COMMIT:-}" ]] && command -v git >/dev/null 2>&1; then
    ULL_MATCHER_GIT_COMMIT="$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || true)"
    if [[ -n "${ULL_MATCHER_GIT_COMMIT}" ]]; then
      export ULL_MATCHER_GIT_COMMIT
    fi
  fi
}

java_major_version() {
  local java_bin="$1"
  local version_line
  version_line="$("${java_bin}" -version 2>&1 | head -n 1)"
  if printf '%s\n' "$version_line" | grep -Eq '"1\.[0-9]+\.'; then
    printf '%s\n' "$version_line" | sed -E 's/.*"1\.([0-9]+)\..*/\1/'
    return
  fi
  if printf '%s\n' "$version_line" | grep -Eq '"[0-9]+(\.|"|-)' ; then
    printf '%s\n' "$version_line" | sed -E 's/.*"([0-9]+)(\.|"|-).*/\1/'
    return
  fi
  echo 0
}

java_home_is_usable() {
  local candidate="$1"
  [[ -x "${candidate}/bin/java" ]] || return 1
  local major
  major="$(java_major_version "${candidate}/bin/java")"
  [[ "$major" -ge 21 ]]
}

if [[ -n "${JAVA_HOME:-}" ]] && java_home_is_usable "${JAVA_HOME}"; then
  export JAVA_HOME
  export PATH="${JAVA_HOME}/bin:${PATH}"
  export_git_commit
  return 0 2>/dev/null || exit 0
fi

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

if ! java_home_is_usable "${JAVA_HOME}"; then
  echo "[java-env] JAVA_HOME must point to Java 21 or newer: ${JAVA_HOME}" >&2
  "${JAVA_HOME}/bin/java" -version >&2
  exit 4
fi

export JAVA_HOME
export PATH="${JAVA_HOME}/bin:${PATH}"
export_git_commit
