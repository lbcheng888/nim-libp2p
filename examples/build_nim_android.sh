#!/usr/bin/env bash
set -euo pipefail

# Build the Nim libp2p shared library for Android (arm64-v8a) with TLS/QUIC enabled.
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

MSQUIC_BOOTSTRAP="$NIM_LIBP2P_DIR/scripts/nim/bootstrap_msquic.sh"
if [[ -x "$MSQUIC_BOOTSTRAP" ]]; then
  eval "$("$MSQUIC_BOOTSTRAP" env || true)"
fi
OUT_DIR="${OUT_DIR:-$REPO_ROOT/examples/android/app/src/main/jniLibs/$ABI_DIR}"
mkdir -p "$OUT_DIR"

OPENSSL_ANDROID_DIR="${OPENSSL_ANDROID_DIR:-$REPO_ROOT/build/openssl-android/android-arm64}"
LIB_NAME="libnimlibp2p.so"
OUT_PATH="$OUT_DIR/$LIB_NAME"
MSQUIC_ANDROID_LIB_DEFAULT="$REPO_ROOT/../msquic/build-android-arm64/bin/Release/libmsquic.so"
MSQUIC_ANDROID_LIB="${ANDROID_MSQUIC_LIB:-}"
if [[ -z "${MSQUIC_ANDROID_LIB}" && -f "${MSQUIC_ANDROID_LIB_DEFAULT}" ]]; then
  MSQUIC_ANDROID_LIB="${MSQUIC_ANDROID_LIB_DEFAULT}"
fi
NIM_ANDROID_QUIC_BACKEND="${NIM_ANDROID_QUIC_BACKEND:-builtin}"
NIM_ANDROID_PURE_CRYPTO="${NIM_ANDROID_PURE_CRYPTO:-auto}"

if [[ "${NIM_ANDROID_PURE_CRYPTO}" == "auto" ]]; then
  if [[ "${NIM_ANDROID_QUIC_BACKEND}" == "builtin" ]]; then
    NIM_ANDROID_PURE_CRYPTO=1
  else
    NIM_ANDROID_PURE_CRYPTO=0
  fi
fi

if [[ "${NIM_ANDROID_PURE_CRYPTO}" != "1" ]]; then
  if [[ ! -d "$OPENSSL_ANDROID_DIR/lib" ]]; then
    echo "[nim-android] OpenSSL for Android not found at $OPENSSL_ANDROID_DIR"
    echo "[nim-android] Run: scripts/build_openssl_android.sh $API_LEVEL"
    exit 1
  fi

  for lib in libssl.so libcrypto.so; do
    if [[ ! -f "$OPENSSL_ANDROID_DIR/lib/$lib" ]]; then
      echo "[nim-android] Missing $lib under $OPENSSL_ANDROID_DIR/lib"
      echo "[nim-android] Rebuild OpenSSL for Android with shared libraries enabled"
      exit 1
    fi
    if file "$OPENSSL_ANDROID_DIR/lib/$lib" | grep -q "Mach-O"; then
      echo "[nim-android] $lib under $OPENSSL_ANDROID_DIR/lib targets macOS; rebuild for Android"
      exit 1
    fi
  done
fi

OPENSSL_VERSION_HEX="${OPENSSL_VERSION_HEX:-0x40000000L}"

SECP_PATH="$REPO_ROOT/vendor/secp256k1"

NIM_FLAGS=(
  "c"
  "--app:lib"
  "--noMain"
  "--os:android"
  "--cpu:${CPU_FLAG}"
  "--mm:arc"
  "--deepCopy:on"
  "--threads:on"
  "--cc:clang"
  "--path:examples/mobile_ffi/compat/chronicles_stub"
  "--path:."
  "--path:examples/mobile_ffi"
  "--path:examples/dex"
  "--path:examples/dex/compat"
  "--path:${REPO_ROOT}/nimbledeps/pkgs2"
  "--path:${SECP_PATH}"
  "--passC:-Iexamples/mobile_ffi/compat"
  "--passC:-I${NIM_LIBP2P_DIR}"
  "--passC:-I$REPO_ROOT/compat"
  "--passC:-include"
  "--passC:${REPO_ROOT}/compat/explicit_bzero.h"
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
  "--out:${OUT_PATH}"
)

if [[ "${NIM_ANDROID_PURE_CRYPTO}" != "1" ]]; then
  NIM_FLAGS+=(
    "--passC:-I${OPENSSL_ANDROID_DIR}/include"
    "--passC:-DOPENSSL_VERSION_NUMBER=${OPENSSL_VERSION_HEX}"
    "--passL:-L${OPENSSL_ANDROID_DIR}/lib"
    "--passL:-lssl"
    "--passL:-lcrypto"
  )
else
  echo "[nim-android] Pure builtin crypto packaging enabled"
fi

# QUIC backend:
#   builtin  -> 编进 pure-Nim builtin MsQuic API，不依赖外部 libmsquic.so
#   external -> 继续加载外部 libmsquic.so
#   off      -> 不启用 QUIC
case "${NIM_ANDROID_QUIC_BACKEND}" in
  builtin|external|off)
    ;;
  *)
    echo "[nim-android] Unsupported NIM_ANDROID_QUIC_BACKEND=${NIM_ANDROID_QUIC_BACKEND}; expected builtin|external|off" >&2
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
  elif [[ "${NIM_ANDROID_QUIC_BACKEND}" == "external" ]]; then
    if [[ -z "${MSQUIC_ANDROID_LIB}" || ! -f "${MSQUIC_ANDROID_LIB}" ]]; then
      echo "[nim-android] Android MsQuic library not found; set ANDROID_MSQUIC_LIB or place libmsquic.so at ${MSQUIC_ANDROID_LIB_DEFAULT}" >&2
      exit 1
    fi
    echo "[nim-android] Using external MsQuic runtime at ${MSQUIC_ANDROID_LIB}"
  else
    echo "[nim-android] QUIC disabled because NIM_ANDROID_QUIC_BACKEND=off" >&2
    exit 1
  fi
fi

if [[ "${NIM_ANDROID_PURE_CRYPTO}" == "1" ]]; then
  NIM_FLAGS+=("--define:libp2p_pure_crypto")
fi

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

if [[ "${NIM_ANDROID_PURE_CRYPTO}" != "1" ]]; then
  for lib in libssl.so libcrypto.so; do
    cp "${OPENSSL_ANDROID_DIR}/lib/$lib" "$OUT_DIR/$lib"
  done
else
  rm -f "$OUT_DIR/libssl.so" "$OUT_DIR/libcrypto.so" "$OUT_DIR/libcrypto_compat.so"
  rm -rf "$OUT_DIR/ossl-modules" "$OUT_DIR/engines-4"
fi

if [[ "${NIM_ANDROID_ENABLE_QUIC:-0}" == "1" && "${NIM_ANDROID_QUIC_BACKEND}" == "external" ]]; then
  cp "${MSQUIC_ANDROID_LIB}" "${OUT_DIR}/libmsquic.so"
  echo "[nim-android] Copied MsQuic runtime -> ${OUT_DIR}/libmsquic.so"
else
  rm -f "${OUT_DIR}/libmsquic.so"
fi

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

if [[ "${NIM_ANDROID_PURE_CRYPTO}" != "1" ]]; then
  for dir in ossl-modules engines-4; do
    SRC_DIR="${OPENSSL_ANDROID_DIR}/lib/${dir}"
    DEST_DIR="${OUT_DIR}/${dir}"
    if [[ -d "${SRC_DIR}" ]]; then
      rm -rf "${DEST_DIR}"
      mkdir -p "${DEST_DIR}"
      cp -R "${SRC_DIR}/." "${DEST_DIR}/"
      echo "[nim-android] Copied OpenSSL ${dir} -> ${DEST_DIR}"
    else
      echo "[nim-android] WARNING: OpenSSL directory ${SRC_DIR} missing" >&2
    fi
  done

  COMPAT_SRC="${REPO_ROOT}/compat/openssl_compat.c"
  if [[ -f "${COMPAT_SRC}" ]]; then
    COMPAT_OUT="${OUT_DIR}/libcrypto_compat.so"
    echo "[nim-android] Building OpenSSL compatibility shim -> ${COMPAT_OUT}"
    "$CLANG_BIN" -shared -fPIC \
      --target="${TARGET_TRIPLE}${API_LEVEL}" \
      --sysroot="${SYSROOT}" \
      -I"${OPENSSL_ANDROID_DIR}/include" \
      -L"${OPENSSL_ANDROID_DIR}/lib" \
      -lssl -lcrypto \
      "${COMPAT_SRC}" \
      -o "${COMPAT_OUT}"
  else
    echo "[nim-android] WARNING: OpenSSL compatibility source not found at ${COMPAT_SRC}" >&2
  fi
fi

echo "[nim-android] Done."
