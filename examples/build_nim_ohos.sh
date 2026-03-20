#!/usr/bin/env bash
set -euo pipefail

# Build the Nim libp2p shared library for HarmonyOS (OpenHarmony) arm64.
# Usage: scripts/build_nim_ohos.sh [release|debug]

MODE=${1:-release}
TARGET_TRIPLE="aarch64-linux-ohos"
CPU_FLAG="arm64"
OH_SDK_ROOT=${OH_SDK_ROOT:-"/Applications/DevEco-Studio.app/Contents/sdk/default/openharmony/native"}
LLVM_DIR="${OH_SDK_ROOT}/llvm"
SYSROOT_DIR="${OH_SDK_ROOT}/sysroot"
if [[ ! -d "${LLVM_DIR}" || ! -d "${SYSROOT_DIR}" ]]; then
  echo "[nim-ohos] OpenHarmony toolchain not found under ${OH_SDK_ROOT}" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NIM_LIBP2P_DIR="${REPO_ROOT}"
NIM_UNIMAKER_DIR="${REPO_ROOT}/examples/mobile_ffi"

MSQUIC_BOOTSTRAP="${NIM_LIBP2P_DIR}/scripts/nim/bootstrap_msquic.sh"
if [[ -x "${MSQUIC_BOOTSTRAP}" ]]; then
  eval "$("${MSQUIC_BOOTSTRAP}" env || true)"
fi

if [[ -n "${OHOS_PROJECT_DIR:-}" ]]; then
  :
else
  DEFAULT_PROJECT_DIR="${REPO_ROOT}/examples/hos"
  if [[ -d "${DEFAULT_PROJECT_DIR}" ]]; then
    OHOS_PROJECT_DIR="${DEFAULT_PROJECT_DIR}"
  else
    OHOS_PROJECT_DIR="$HOME/DevEcoStudioProjects/UniMaker"
    echo "[nim-ohos] Default project directory ${DEFAULT_PROJECT_DIR} missing; falling back to ${OHOS_PROJECT_DIR}"
  fi
fi

OUT_DIR="${OUT_DIR:-$OHOS_PROJECT_DIR/entry/libs/arm64-v8a}"
SECONDARY_DIR="${SECONDARY_DIR:-$OHOS_PROJECT_DIR/entry/libs/arm64/lib}"
LEGACY_DIR="${LEGACY_DIR:-$OHOS_PROJECT_DIR/entry/libs/arm64}"
MODULE_DIR="${MODULE_DIR:-$OHOS_PROJECT_DIR/entry/libs/module/arm64-v8a}"
MODULE_LIB64_DIR="${MODULE_LIB64_DIR:-$OHOS_PROJECT_DIR/entry/libs/module/lib64}"
JNI_DIR="${JNI_DIR:-$OHOS_PROJECT_DIR/entry/src/main/jniLibs/arm64-v8a}"
ENTRY_LIB_DIR="${ENTRY_LIB_DIR:-$OHOS_PROJECT_DIR/entry/lib}"
ENABLE_Z_SO_PACKAGING="${ENABLE_Z_SO_PACKAGING:-0}"
zip_copy() {
  local src="$1"
  local dest="$2"
  if [[ -z "$src" || -z "$dest" ]]; then
    return
  fi
  python3 - "$src" "$dest" <<'PY'
import os, sys, zipfile
src = sys.argv[1]
dest = sys.argv[2]
tmp_dest = dest + ".tmp"
with zipfile.ZipFile(tmp_dest, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    zf.write(src, arcname=os.path.basename(src))
if os.path.exists(dest):
    os.remove(dest)
os.replace(tmp_dest, dest)
PY
}

mkdir -p "${OUT_DIR}"
mkdir -p "${SECONDARY_DIR}"
mkdir -p "${LEGACY_DIR}"
mkdir -p "${MODULE_DIR}"
mkdir -p "${MODULE_LIB64_DIR}"
mkdir -p "${JNI_DIR}"
mkdir -p "${ENTRY_LIB_DIR}"

LIBCXX_SHARED="${LLVM_DIR}/lib/aarch64-linux-ohos/libc++_shared.so"
if [[ ! -f "${LIBCXX_SHARED}" ]]; then
  echo "[nim-ohos] libc++_shared.so not found at ${LIBCXX_SHARED}"
  exit 1
fi
LIBCXX_STATIC="${LLVM_DIR}/lib/aarch64-linux-ohos/libc++_static.a"
LIBCXXABI_STATIC="${LLVM_DIR}/lib/aarch64-linux-ohos/libc++abi.a"
LIBUNWIND_STATIC="${LLVM_DIR}/lib/aarch64-linux-ohos/libunwind.a"
for required in "${LIBCXX_STATIC}" "${LIBCXXABI_STATIC}" "${LIBUNWIND_STATIC}"; do
  if [[ ! -f "${required}" ]]; then
    echo "[nim-ohos] required static runtime not found at ${required}" >&2
    exit 1
  fi
done

LIB_NAME="libnimlibp2p.so"
OUT_PATH="${OUT_DIR}/${LIB_NAME}"
BRIDGE_SRC="${REPO_ROOT}/examples/hos/entry/src/main/cpp/nim_bridge.cpp"
BRIDGE_OBJ_DIR="${REPO_ROOT}/build/ohos-bridge"
BRIDGE_OBJ="${BRIDGE_OBJ_DIR}/nim_bridge.o"
NIM_OHOS_QUIC_BACKEND="${NIM_OHOS_QUIC_BACKEND:-builtin}"
NIM_OHOS_PURE_CRYPTO="${NIM_OHOS_PURE_CRYPTO:-1}"

if [[ "${NIM_OHOS_PURE_CRYPTO}" != "1" ]]; then
  echo "[nim-ohos] OpenSSL/libssl packaging has been removed; set NIM_OHOS_PURE_CRYPTO=1" >&2
  exit 1
fi

CLANG_BIN="${LLVM_DIR}/bin/clang"
AR_BIN="${LLVM_DIR}/bin/llvm-ar"
export PATH="${LLVM_DIR}/bin:${PATH}"
SYSROOT_FLAG="--sysroot=${SYSROOT_DIR}"
TARGET_FLAG="--target=${TARGET_TRIPLE}"

mkdir -p "${BRIDGE_OBJ_DIR}"
echo "[nim-ohos] Building embedded N-API bridge object -> ${BRIDGE_OBJ}"
"${CLANG_BIN}" \
  ${TARGET_FLAG} \
  ${SYSROOT_FLAG} \
  -I"${REPO_ROOT}/examples/hos/entry/src/main/cpp" \
  -I"${SYSROOT_DIR}/usr/include" \
  -I"${SYSROOT_DIR}/usr/include/napi" \
  -std=c++17 \
  -DNAPI_VERSION=8 \
  -DNIMBRIDGE_EMBEDDED=1 \
  -fPIC \
  -c \
  -o "${BRIDGE_OBJ}" \
  "${BRIDGE_SRC}"

NIM_FLAGS=(
  "c"
  "--app:lib"
  "--noMain"
  "--os:linux"
  "--cpu:${CPU_FLAG}"
  "--mm:arc"
  "--deepCopy:on"
  "--threads:on"
  "--cc:clang"
  "--path:."
  "--path:examples/mobile_ffi"
  "--path:examples/mobile_ffi/compat/chronicles_stub"
  "--path:examples/mobile_ffi/compat/chronicles_stub"
  "--path:${REPO_ROOT}/nimbledeps/pkgs2"
  $(for d in "${REPO_ROOT}/nimbledeps/pkgs2"/*; do if [ -d "$d" ]; then echo "--path:$d"; fi; done)
  "--passC:-Iexamples/mobile_ffi/compat"
  "--passC:-I${NIM_LIBP2P_DIR}"
  "--passC:-I$REPO_ROOT/compat"
  "--passC:-include"
  "--passC:${REPO_ROOT}/compat/explicit_bzero.h"
  "--passC:-fPIC"
  "--passC:${TARGET_FLAG}"
  "--passC:${SYSROOT_FLAG}"
  "--passL:${TARGET_FLAG}"
  "--passL:${SYSROOT_FLAG}"
  "--passL:-shared"
  "--passL:-Wl,-export-dynamic"
  "--passL:-Wl,-soname,libnimlibp2p.so"
  "--passL:-lhilog_ndk.z"
  "--passL:${BRIDGE_OBJ}"
  "--passL:-lace_napi.z"
  "--passL:-Wl,--start-group"
  "--passL:${LIBCXX_STATIC}"
  "--passL:${LIBCXXABI_STATIC}"
  "--passL:${LIBUNWIND_STATIC}"
  "--passL:-Wl,--end-group"
  "--define:libp2p_autotls_support"
  "--out:${OUT_PATH}"
)

echo "[nim-ohos] Pure builtin crypto packaging enabled"

case "${NIM_OHOS_QUIC_BACKEND}" in
  builtin)
    NIM_FLAGS+=("--define:libp2p_msquic_experimental" "--define:libp2p_msquic_builtin")
    echo "[nim-ohos] Using builtin pure-Nim MsQuic API"
    ;;
  off)
    echo "[nim-ohos] QUIC disabled"
    ;;
  *)
    echo "[nim-ohos] Unsupported NIM_OHOS_QUIC_BACKEND=${NIM_OHOS_QUIC_BACKEND}; expected builtin|off" >&2
    exit 1
    ;;
esac

NIM_FLAGS+=("--define:libp2p_pure_crypto")

# Allow enabling trace info explicitly to avoid Nim 2.3.x cross-compilation crashes by default
if [[ "${NIM_OHOS_ENABLE_TRACES:-0}" == "1" ]]; then
  NIM_FLAGS+=("--stacktrace:on" "--lineTrace:on")
else
  NIM_FLAGS+=("--stacktrace:off" "--lineTrace:off")
fi

if [[ "${MODE}" == "release" ]]; then
  NIM_FLAGS+=("--define:release")
else
  NIM_FLAGS+=("--define:debug")
fi

NIM_FLAGS+=("-d:ohos")
NIM_FLAGS+=("--nimcache:${REPO_ROOT}/build/ohos-nimcache")
NIM_FLAGS+=("--passL:-Wl,-rpath,\$ORIGIN")

if [[ "${ENABLE_CHRONICLES:-}" == "1" ]]; then
  NIM_FLAGS+=("--define:chronicles_enabled=true")
else
  NIM_FLAGS+=("--define:chronicles_enabled=false")
fi

# Disable Hilog dependency by default; opt-in with NIM_OHOS_USE_HILOG=1.
if [[ "${NIM_OHOS_USE_HILOG:-0}" != "1" ]]; then
  NIM_FLAGS+=("--define:nimlibp2p_no_hilog")
fi

echo "[nim-ohos] Validated Paths:"
for d in "${REPO_ROOT}/nimbledeps/pkgs2"/*; do
  if [ -d "$d" ]; then echo "Found dep: $d"; fi
done

echo "[nim-ohos] Building Nim libp2p -> ${OUT_PATH}"
(
  cd "${NIM_LIBP2P_DIR}"
  CC="${CLANG_BIN} ${TARGET_FLAG} ${SYSROOT_FLAG}" \
  CXX="${CLANG_BIN} ${TARGET_FLAG} ${SYSROOT_FLAG}" \
  AR="${AR_BIN}" \
  nim "${NIM_FLAGS[@]}" "${NIM_UNIMAKER_DIR}/libnimlibp2p.nim"
)

# Propagate libnimlibp2p to secondary copies (stage expects multiple layouts).
for dest in "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}" "${ENTRY_LIB_DIR}"; do
  mkdir -p "${dest}"
  cp "${OUT_PATH}" "${dest}/libnimlibp2p.so"
  if [[ "${ENABLE_Z_SO_PACKAGING}" == "1" && "${dest}" == "${JNI_DIR}" ]]; then
    zip_copy "${dest}/libnimlibp2p.so" "${dest}/libnimlibp2p.z.so"
  fi
done

for dest in "${OUT_DIR}" "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}" "${ENTRY_LIB_DIR}"; do
  mkdir -p "${dest}"
  rm -f "${dest}"/libssl.so "${dest}"/libssl.so.3 "${dest}"/libssl.so.4 \
    "${dest}"/libcrypto.so "${dest}"/libcrypto.so.3 "${dest}"/libcrypto.so.4 \
    "${dest}"/libmsquic.so
  rm -rf "${dest}/ossl-modules" "${dest}/engines-3" "${dest}/engines-4"
done

NEEDS_LIBCXX_SHARED=0
if command -v objdump >/dev/null 2>&1 && objdump -p "${OUT_PATH}" 2>/dev/null | grep -q 'NEEDED[[:space:]]\+libc++_shared\.so'; then
  NEEDS_LIBCXX_SHARED=1
fi

for dest in "${OUT_DIR}" "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}" "${ENTRY_LIB_DIR}"; do
  mkdir -p "${dest}"
  rm -f "${dest}/libnimbridge.so" "${dest}/libnimbridge.z.so" \
    "${dest}/libp2pbridge.so" "${dest}/libp2pbridge.z.so"
  if [[ "${NEEDS_LIBCXX_SHARED}" == "1" ]]; then
    cp "${LIBCXX_SHARED}" "${dest}/libc++_shared.so"
  else
    rm -f "${dest}/libc++_shared.so"
  fi
done

echo "[nim-ohos] Done."
