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
OPENSSL_VERSION_HEX="${OPENSSL_VERSION_HEX:-0x40000000L}"

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

OPENSSL_DIR="${OPENSSL_DIR:-$REPO_ROOT/build/openssl-ohos/${TARGET_TRIPLE}}"
if [[ ! -d "${OPENSSL_DIR}" ]]; then
  echo "[nim-ohos] OpenSSL for HarmonyOS not found at ${OPENSSL_DIR}"
  echo "[nim-ohos] Run: scripts/build_openssl_ohos.sh"
  exit 1
fi

LIBCXX_SHARED="${LLVM_DIR}/lib/aarch64-linux-ohos/libc++_shared.so"
if [[ ! -f "${LIBCXX_SHARED}" ]]; then
  echo "[nim-ohos] libc++_shared.so not found at ${LIBCXX_SHARED}"
  exit 1
fi

LIB_NAME="libnimlibp2p.so"
OUT_PATH="${OUT_DIR}/${LIB_NAME}"
BRIDGE_NAME="libnimbridge.so"
LEGACY_BRIDGE_NAME="libp2pbridge.so"

CLANG_BIN="${LLVM_DIR}/bin/clang"
AR_BIN="${LLVM_DIR}/bin/llvm-ar"
export PATH="${LLVM_DIR}/bin:${PATH}"
SYSROOT_FLAG="--sysroot=${SYSROOT_DIR}"
TARGET_FLAG="--target=${TARGET_TRIPLE}"

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
  "--passC:-I${OPENSSL_DIR}/include"
  "--passL:${TARGET_FLAG}"
  "--passL:${SYSROOT_FLAG}"
  "--passL:-shared"
  "--passL:-Wl,-export-dynamic"
  "--passL:-L${OPENSSL_DIR}/lib"
  "--passC:-DOPENSSL_VERSION_NUMBER=${OPENSSL_VERSION_HEX}"
"--passL:-lhilog_ndk.z"
"--passL:-lssl"
"--passL:-lcrypto"
"--define:libp2p_autotls_support"

"--define:libp2p_msquic_experimental"
"--out:${OUT_PATH}"
)

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

BRIDGE_SRC="${REPO_ROOT}/examples/hos/entry/src/main/cpp/nim_bridge.cpp"
BRIDGE_OUT="${OUT_DIR}/${BRIDGE_NAME}"
echo "[nim-ohos] Building nimbridge -> ${BRIDGE_OUT}"
"${CLANG_BIN}" \
  ${TARGET_FLAG} \
  ${SYSROOT_FLAG} \
  -I"${REPO_ROOT}/examples/hos/entry/src/main/cpp" \
  -I"${SYSROOT_DIR}/usr/include" \
  -I"${SYSROOT_DIR}/usr/include/napi" \
  -I"${OPENSSL_DIR}/include" \
  -std=c++17 \
  -DNAPI_VERSION=8 \
  -fPIC \
  -shared \
  -Wl,-rpath,\$ORIGIN \
  -L"${OUT_DIR}" \
  -L"${OPENSSL_DIR}/lib" \
  -L"${LLVM_DIR}/lib/aarch64-linux-ohos" \
  -o "${BRIDGE_OUT}" \
  "${BRIDGE_SRC}" \
    -lace_napi.z \
  -lc++_shared \
  -lhilog_ndk.z \
  -lssl \
  -lcrypto \
  -ldl

# Propagate libnimlibp2p to secondary copies (stage expects multiple layouts).
for dest in "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}" "${ENTRY_LIB_DIR}"; do
  cp "${OUT_PATH}" "${dest}/libnimlibp2p.so"
  if [[ "${dest}" == "${JNI_DIR}" ]]; then
    zip_copy "${dest}/libnimlibp2p.so" "${dest}/libnimlibp2p.z.so"
  fi
done

# Distribute the N-API bridge to every expected lookup path.
for dest in "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}" "${ENTRY_LIB_DIR}"; do
  rm -f "${dest}/libp2pbridge.so" "${dest}/libp2pbridge.z.so"
  cp "${BRIDGE_OUT}" "${dest}/${BRIDGE_NAME}"
  if [[ -n "${LEGACY_BRIDGE_NAME}" ]]; then
    cp "${BRIDGE_OUT}" "${dest}/${LEGACY_BRIDGE_NAME}"
  fi
  if [[ "${dest}" == "${JNI_DIR}" ]]; then
    zip_copy "${dest}/${BRIDGE_NAME}" "${dest}/libnimbridge.z.so"
    if [[ -n "${LEGACY_BRIDGE_NAME}" ]]; then
      zip_copy "${dest}/${LEGACY_BRIDGE_NAME}" "${dest}/libp2pbridge.z.so"
    fi
  fi
done

# Copy OpenSSL runtime libraries required for QUIC/DTLS.
for lib in libssl.so libssl.so.3 libssl.so.4 libcrypto.so libcrypto.so.3 libcrypto.so.4; do
  if [[ -f "${OPENSSL_DIR}/lib/${lib}" ]]; then
    for dest in "${OUT_DIR}" "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}" "${ENTRY_LIB_DIR}"; do
      cp "${OPENSSL_DIR}/lib/${lib}" "${dest}/${lib}"
    done
  else
    echo "[nim-ohos] WARNING: ${lib} not found under ${OPENSSL_DIR}/lib" >&2
  fi
done

# Distribute C++ runtime shared library required by the bridge.
for dest in "${OUT_DIR}" "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}" "${ENTRY_LIB_DIR}"; do
  cp "${LIBCXX_SHARED}" "${dest}/libc++_shared.so"
done

# Distribute OpenSSL provider/engine directories.
for dir in ossl-modules engines-4; do
  SRC_DIR="${OPENSSL_DIR}/lib/${dir}"
  if [[ -d "${SRC_DIR}" ]]; then
    for dest in "${OUT_DIR}" "${SECONDARY_DIR}" "${LEGACY_DIR}" "${MODULE_DIR}" "${MODULE_LIB64_DIR}" "${JNI_DIR}"; do
      rm -rf "${dest}/${dir}"
      mkdir -p "${dest}/${dir}"
      cp -R "${SRC_DIR}/." "${dest}/${dir}/"
    done
    echo "[nim-ohos] Copied ${dir}"
  else
    echo "[nim-ohos] WARNING: OpenSSL directory ${SRC_DIR} missing" >&2
  fi
done

echo "[nim-ohos] Done."
