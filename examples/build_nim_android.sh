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

LIB_NAME="libnimlibp2p.so"
OUT_PATH="$OUT_DIR/$LIB_NAME"

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
  "--passC:--target=${TARGET_TRIPLE}${API_LEVEL}"
  "--passC:--sysroot=${SYSROOT}"
  "--passC:-fvisibility=default"
  "--passC:-I${OPENSSL_ANDROID_DIR}/include"
  "--passC:-DOPENSSL_VERSION_NUMBER=${OPENSSL_VERSION_HEX}"
  "--passL:--target=${TARGET_TRIPLE}${API_LEVEL}"
  "--passL:--sysroot=${SYSROOT}"
  "--passL:-shared"
  "--passL:-Wl,-export-dynamic"
  "--passL:-llog"
  "--passL:-L${OPENSSL_ANDROID_DIR}/lib"
  "--passL:-lssl"
  "--passL:-lcrypto"
  "--define:libp2p_autotls_support"
  "--passL:-landroid"
  "--out:${OUT_PATH}"
)

# Optionally disable QUIC via NIM_ANDROID_ENABLE_QUIC=0. Default is disabled to prevent crashes.
# Set to 1 to enable QUIC (Experimental MsQuic support)
# WARNING: Requires libmsquic.so for Android ARM64 to be present in jniLibs.
if [[ "${NIM_ANDROID_ENABLE_QUIC:-0}" == "1" ]]; then
  # NIM_FLAGS+=("--define:libp2p_quic_support")
  NIM_FLAGS+=("--define:libp2p_msquic_experimental")
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

for lib in libssl.so libcrypto.so; do
  cp "${OPENSSL_ANDROID_DIR}/lib/$lib" "$OUT_DIR/$lib"
done

LIBCXX_SRC="$SYSROOT/usr/lib/${TARGET_TRIPLE}/${API_LEVEL}/libc++_shared.so"
if [[ ! -f "$LIBCXX_SRC" ]]; then
  LIBCXX_SRC="$SYSROOT/usr/lib/${TARGET_TRIPLE}/libc++_shared.so"
fi
if [[ -f "$LIBCXX_SRC" ]]; then
  cp "$LIBCXX_SRC" "$OUT_DIR/libc++_shared.so"
  echo "[nim-android] Copied libc++ runtime -> $OUT_DIR/libc++_shared.so"
else
  echo "[nim-android] WARNING: libc++_shared.so not found under $SYSROOT" >&2
fi

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

echo "[nim-android] Done."
