#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/build}"
GO_BIN="${GO_BIN:-go}"

case "$(uname -s)" in
  Darwin) LIB_NAME="libtsnetbridge.dylib" ;;
  Linux) LIB_NAME="libtsnetbridge.so" ;;
  *)
    echo "[build_tsnetbridge_host] unsupported host OS $(uname -s)" >&2
    exit 1
    ;;
esac

mkdir -p "${OUT_DIR}"

echo "[build_tsnetbridge_host] building ${LIB_NAME}"
(
  cd "${REPO_ROOT}/go/tsnetbridge"
  "${GO_BIN}" build -buildmode=c-shared -o "${OUT_DIR}/${LIB_NAME}" .
)

rm -f "${OUT_DIR}/libtsnetbridge.h"
echo "[build_tsnetbridge_host] output -> ${OUT_DIR}/${LIB_NAME}"
