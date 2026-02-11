#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BOOTSTRAP="${SCRIPT_DIR}/bootstrap_msquic.sh"

if [[ ! -x "${BOOTSTRAP}" ]]; then
  echo "validate_msquic: bootstrap script not found at ${BOOTSTRAP}" >&2
  exit 1
fi

if export_line=$("${BOOTSTRAP}" env); then
  if [[ -n "${export_line}" ]]; then
    eval "${export_line}"
  fi
else
  echo "validate_msquic: bootstrap failed" >&2
  exit 1
fi

if [[ -z "${NIM_MSQUIC_LIB:-}" ]]; then
  echo "validate_msquic: NIM_MSQUIC_LIB not configured" >&2
  exit 1
fi

if [[ ! -f "${NIM_MSQUIC_LIB}" ]]; then
  echo "validate_msquic: MsQuic library not found at ${NIM_MSQUIC_LIB}" >&2
  exit 1
fi

echo "MsQuic runtime ready: ${NIM_MSQUIC_LIB}"
