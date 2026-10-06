# shellcheck shell=bash
# Lab persistence after default.conf. Caller-exported values always win.
# Deploy scripts must not source this file.

if [[ -z "${_ULL_LAB_PERSISTENCE_EXPLICIT_PROFILE:-}" ]]; then
  PERSISTENCE_PROFILE=BENCH
fi
if [[ -z "${_ULL_LAB_PERSISTENCE_EXPLICIT_SNAPSHOT:-}" ]]; then
  SNAPSHOT_INTERVAL_MILLIS=0
fi
if [[ -z "${_ULL_LAB_PERSISTENCE_EXPLICIT_COLD:-}" ]]; then
  WAL_COLD_ARCHIVE_DIR=
fi
SERVER_MODE_VALUE="${SERVER_MODE_VALUE:-DEV}"
if [[ -z "${_ULL_LAB_TLS_EXPLICIT:-}" ]]; then
  ENABLE_TRANSPORT_TLS=false
fi
