#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NIM_BIN="${NIM_BIN:-${NIM:-$(command -v nim || true)}}"
NIMBLE_BIN="${NIMBLE_BIN:-${NIMBLE:-$(command -v nimble || true)}}"
export HOME="${HOME:-/root}"
export NIMBLE_DIR="${NIMBLE_DIR:-${HOME}/.nimble}"

if [[ -z "${NIM_BIN}" ]]; then
  if [[ -x /root/Nim/bin/nim ]]; then
    NIM_BIN=/root/Nim/bin/nim
  else
    echo "[build_dual_stack_bootstrap_node] nim not found" >&2
    exit 1
  fi
fi

if [[ -z "${NIMBLE_BIN}" ]]; then
  if [[ -x /root/Nim/bin/nimble ]]; then
    NIMBLE_BIN=/root/Nim/bin/nimble
  else
    echo "[build_dual_stack_bootstrap_node] nimble not found" >&2
    exit 1
  fi
fi

mkdir -p "${REPO_ROOT}/build"
cd "${REPO_ROOT}"

exec "${NIMBLE_BIN}" c \
  --hints:off \
  --warnings:off \
  -d:ssl \
  -d:libp2p_msquic_experimental \
  -d:libp2p_msquic_builtin \
  -o:"${REPO_ROOT}/build/dual_stack_bootstrap_node" \
  "${REPO_ROOT}/examples/dual_stack_bootstrap_node.nim"
