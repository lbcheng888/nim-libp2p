#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
UNIMAKER_ANDROID_ROOT="${UNIMAKER_ANDROID_ROOT:-/Users/lbcheng/UniMaker/android}"
API_LEVEL="${API_LEVEL:-24}"
ABI="${ABI:-arm64-v8a}"
GO_BIN="${GO_BIN:-go}"

if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
  echo "[build_tsnetbridge_android] ANDROID_NDK_HOME is not set" >&2
  exit 1
fi

case "$(uname -s)" in
  Darwin) HOST_TAG="darwin" ;;
  Linux) HOST_TAG="linux" ;;
  *)
    echo "[build_tsnetbridge_android] unsupported host OS $(uname -s)" >&2
    exit 1
    ;;
esac

case "${ABI}" in
  arm64-v8a)
    TARGET_TRIPLE="aarch64-linux-android"
    GOARCH="arm64"
    ;;
  *)
    echo "[build_tsnetbridge_android] unsupported ABI ${ABI}" >&2
    exit 1
    ;;
esac

LLVM_PREBUILT="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/${HOST_TAG}-x86_64/bin"
CLANG_BIN="${LLVM_PREBUILT}/${TARGET_TRIPLE}${API_LEVEL}-clang"
AR_BIN="${LLVM_PREBUILT}/llvm-ar"
OUT_DIR="${OUT_DIR:-${UNIMAKER_ANDROID_ROOT}/core-bridge/src/main/jniLibs/${ABI}}"
OUT_PATH="${OUT_DIR}/libtsnetbridge.so"

mkdir -p "${OUT_DIR}"

echo "[build_tsnetbridge_android] building ${OUT_PATH}"
(
  cd "${REPO_ROOT}/go/tsnetbridge"
  CGO_ENABLED=1 \
  GOOS=android \
  GOARCH="${GOARCH}" \
  CC="${CLANG_BIN}" \
  CXX="${CLANG_BIN}" \
  AR="${AR_BIN}" \
  "${GO_BIN}" build -buildmode=c-shared -o "${OUT_PATH}" .
)

rm -f "${OUT_DIR}/libtsnetbridge.h"
echo "[build_tsnetbridge_android] output -> ${OUT_PATH}"
