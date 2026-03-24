#!/usr/bin/env bash
set -euo pipefail

# Build the Nim libp2p shared library for Android (arm64-v8a) with pure-Nim crypto
# and builtin native QUIC only.
# Usage: scripts/build_nim_android.sh [release|debug]
# Requires ANDROID_NDK_HOME pointing to the Android NDK root.

MODE=${1:-release}
API_LEVEL=${API_LEVEL:-24}
TARGET_TRIPLE="aarch64-linux-android"
CPU_FLAG="arm64"
ABI_DIR="arm64-v8a"

if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
  echo "[nim-android] ANDROID_NDK_HOME is not set" >&2
  exit 1
fi
if [[ ! -d "$ANDROID_NDK_HOME" ]]; then
  echo "[nim-android] ANDROID_NDK_HOME=$ANDROID_NDK_HOME is not a directory" >&2
  exit 1
fi

HOST_TAG=""
case "$(uname -s)" in
  Darwin) HOST_TAG="darwin" ;;
  Linux) HOST_TAG="linux" ;;
  *) echo "[nim-android] Unsupported host OS $(uname -s)" >&2; exit 1 ;;
esac

LLVM_PREBUILT="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/${HOST_TAG}-x86_64/bin"
CLANG_BIN="$LLVM_PREBUILT/${TARGET_TRIPLE}${API_LEVEL}-clang"
AR_BIN="$LLVM_PREBUILT/llvm-ar"
SYSROOT="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/${HOST_TAG}-x86_64/sysroot"
export PATH="$LLVM_PREBUILT:$PATH"

if [[ ! -x "$CLANG_BIN" ]]; then
  echo "[nim-android] clang toolchain not found at $CLANG_BIN" >&2
  exit 1
fi
if [[ ! -d "$SYSROOT" ]]; then
  echo "[nim-android] sysroot not found at $SYSROOT" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NIM_LIBP2P_DIR="$REPO_ROOT"
NIM_UNIMAKER_DIR="$REPO_ROOT/examples/mobile_ffi"
UNIMAKER_ANDROID_ROOT="${UNIMAKER_ANDROID_ROOT:-/Users/lbcheng/UniMaker/android}"

MSQUIC_BOOTSTRAP="$NIM_LIBP2P_DIR/scripts/nim/bootstrap_msquic.sh"
if [[ -x "$MSQUIC_BOOTSTRAP" ]]; then
  eval "$("$MSQUIC_BOOTSTRAP" env || true)"
fi
OUT_DIR="${OUT_DIR:-$UNIMAKER_ANDROID_ROOT/core-bridge/src/main/jniLibs/$ABI_DIR}"
mkdir -p "$OUT_DIR"

LIB_NAME="libnimlibp2p.so"
OUT_PATH="$OUT_DIR/$LIB_NAME"
TSNET_BRIDGE_OUT="$OUT_DIR/libtsnetbridge.so"
NIM_ANDROID_QUIC_BACKEND="${NIM_ANDROID_QUIC_BACKEND:-builtin}"
NIM_ANDROID_PURE_CRYPTO="${NIM_ANDROID_PURE_CRYPTO:-1}"
NIM_ANDROID_ENABLE_TSNET="${NIM_ANDROID_ENABLE_TSNET:-1}"

if [[ "${NIM_ANDROID_PURE_CRYPTO}" != "1" ]]; then
  echo "[nim-android] OpenSSL/libssl packaging has been removed; set NIM_ANDROID_PURE_CRYPTO=1" >&2
  exit 1
fi

SECP_PATH="$REPO_ROOT/vendor/secp256k1"

NIM_FLAGS=(
  "c"
  "--app:lib"
  "--noMain"
  "--forceBuild:on"
  "--os:android"
  "--cpu:${CPU_FLAG}"
  "--mm:arc"
  "--deepCopy:on"
  "--threads:on"
  "--cc:clang"
  "--path:examples/mobile_ffi/compat/chronicles_stub"
  "--path:."
  "--path:examples/mobile_ffi"
  "--path:${REPO_ROOT}/nimbledeps/pkgs2"
  "--path:${SECP_PATH}"
  "--passC:-Iexamples/mobile_ffi/compat"
  "--passC:-I${NIM_LIBP2P_DIR}"
  "--passC:-include"
  "--passC:${REPO_ROOT}/examples/mobile_ffi/compat/explicit_bzero.h"
  "--passC:-fPIC"
  "--passC:-D_POSIX_C_SOURCE=200809L"
  "--passC:--target=${TARGET_TRIPLE}${API_LEVEL}"
  "--passC:--sysroot=${SYSROOT}"
  "--passC:-fvisibility=default"
  "--passL:--target=${TARGET_TRIPLE}${API_LEVEL}"
  "--passL:--sysroot=${SYSROOT}"
  "--passL:-shared"
  "--passL:-Wl,-export-dynamic"
  "--passL:-llog"
  "--define:libp2p_autotls_support"
  "--passL:-landroid"
  "--nimcache:${REPO_ROOT}/build/android-nimcache"
  "--out:${OUT_PATH}"
)

echo "[nim-android] Pure builtin crypto packaging enabled"

# QUIC backend:
#   builtin -> 编进 pure-Nim builtin MsQuic API
#   off     -> 不启用 QUIC
case "${NIM_ANDROID_QUIC_BACKEND}" in
  builtin|off)
    ;;
  *)
    echo "[nim-android] Unsupported NIM_ANDROID_QUIC_BACKEND=${NIM_ANDROID_QUIC_BACKEND}; expected builtin|off" >&2
    exit 1
    ;;
esac

# Enable QUIC by default unless the backend is explicitly disabled.
if [[ -z "${NIM_ANDROID_ENABLE_QUIC:-}" ]]; then
  if [[ "${NIM_ANDROID_QUIC_BACKEND}" != "off" ]]; then
    NIM_ANDROID_ENABLE_QUIC=1
  else
    NIM_ANDROID_ENABLE_QUIC=0
  fi
fi

if [[ "${NIM_ANDROID_ENABLE_QUIC:-0}" == "1" ]]; then
  NIM_FLAGS+=("--define:libp2p_msquic_experimental")
  if [[ "${NIM_ANDROID_QUIC_BACKEND}" == "builtin" ]]; then
    NIM_FLAGS+=("--define:libp2p_msquic_builtin")
    echo "[nim-android] Using builtin pure-Nim MsQuic API"
  else
    echo "[nim-android] QUIC disabled because NIM_ANDROID_QUIC_BACKEND=off" >&2
    exit 1
  fi
fi

NIM_FLAGS+=("--define:libp2p_pure_crypto")

# Enable stacktrace/lineTrace only when显式开启，规避 Nim 2.3.x 交叉编译 SIGSEGV
if [[ "${NIM_ANDROID_ENABLE_TRACES:-0}" == "1" ]]; then
  NIM_FLAGS+=("--stacktrace:on" "--lineTrace:on")
else
  NIM_FLAGS+=("--stacktrace:off" "--lineTrace:off")
fi

# Optional extra flags
if [[ "${ENABLE_CHRONICLES:-}" == "1" ]]; then
  NIM_FLAGS+=("--define:chronicles_enabled=true")
fi

if [[ "$MODE" == "release" ]]; then
  NIM_FLAGS+=("--define:release")
else
  NIM_FLAGS+=("--define:debug")
fi

# Fix for potential Android SIGSEGV with nimcrypto ASM and ART signal handlers
NIM_FLAGS+=("--define:nimcrypto_disable_asm")
NIM_FLAGS+=("--define:noSignalHandler")

echo "[nim-android] Building Nim libp2p -> $OUT_PATH"
(
  cd "$NIM_LIBP2P_DIR"
  CC="$CLANG_BIN" \
  CXX="$CLANG_BIN" \
  AR="$AR_BIN" \
  nim "${NIM_FLAGS[@]}" "$NIM_UNIMAKER_DIR/libnimlibp2p.nim"
)

if [[ "${NIM_ANDROID_ENABLE_TSNET}" == "1" ]]; then
  if ! command -v go >/dev/null 2>&1; then
    echo "[nim-android] go is required to build libtsnetbridge.so" >&2
    exit 1
  fi
  echo "[nim-android] Building tsnet bridge -> $TSNET_BRIDGE_OUT"
  (
    cd "$REPO_ROOT/go/tsnetbridge"
    CGO_ENABLED=1 \
    GOOS=android \
    GOARCH=arm64 \
    CC="$CLANG_BIN" \
    CXX="$CLANG_BIN" \
    AR="$AR_BIN" \
    go build -buildmode=c-shared -o "$TSNET_BRIDGE_OUT" .
  )
  rm -f "$OUT_DIR/libtsnetbridge.h"
else
  rm -f "$TSNET_BRIDGE_OUT" "$OUT_DIR/libtsnetbridge.h"
fi

rm -f "$OUT_DIR/libssl.so" "$OUT_DIR/libcrypto.so" "$OUT_DIR/libcrypto_compat.so" "$OUT_DIR/libmsquic.so"
rm -rf "$OUT_DIR/ossl-modules" "$OUT_DIR/engines-4"

LIBCXX_SRC="$SYSROOT/usr/lib/${TARGET_TRIPLE}/${API_LEVEL}/libc++_shared.so"
if [[ ! -f "$LIBCXX_SRC" ]]; then
  LIBCXX_SRC="$SYSROOT/usr/lib/${TARGET_TRIPLE}/libc++_shared.so"
fi
if objdump -p "${OUT_PATH}" | grep -q "NEEDED .*libc++_shared.so"; then
  if [[ -f "$LIBCXX_SRC" ]]; then
    cp "$LIBCXX_SRC" "$OUT_DIR/libc++_shared.so"
    echo "[nim-android] Copied libc++ runtime -> $OUT_DIR/libc++_shared.so"
  else
    echo "[nim-android] WARNING: libc++_shared.so not found under $SYSROOT" >&2
  fi
else
  rm -f "$OUT_DIR/libc++_shared.so"
fi

echo "[nim-android] Done."
