#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN="${REPO_ROOT}/build/dual_stack_bootstrap_node"

if [[ ! -x "${BIN}" ]]; then
  echo "[run_dual_stack_bootstrap_node] missing binary: ${BIN}" >&2
  exit 1
fi

cd "${REPO_ROOT}"
exec "${BIN}"
