#!/usr/bin/env bash
set -euo pipefail

# Build OpenSSL (with QUIC enabled) for HarmonyOS / OpenHarmony arm64.
# The script prefers a local source tree (default /Users/lbcheng/openssl, i.e. OpenSSL 4.0.0-dev).
# Usage: scripts/build_openssl_ohos.sh

OPENSSL_LOCAL_SOURCE="${OPENSSL_SOURCE_DIR:-/Users/lbcheng/openssl}"
OPENSSL_VERSION="${OPENSSL_VERSION:-4.0.0-dev}"
TARGET_TRIPLE="aarch64-linux-ohos"
OH_SDK_ROOT=${OH_SDK_ROOT:-"/Applications/DevEco-Studio.app/Contents/sdk/default/openharmony/native"}
LLVM_DIR="${OH_SDK_ROOT}/llvm"
SYSROOT_DIR="${OH_SDK_ROOT}/sysroot"

if [[ ! -d "${LLVM_DIR}" || ! -d "${SYSROOT_DIR}" ]]; then
  echo "[openssl-ohos] OpenHarmony toolchain not found under ${OH_SDK_ROOT}" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_ROOT="${REPO_ROOT}/build"
SOURCE_ROOT="${BUILD_ROOT}/_deps/openssl-ohos"
PREFIX="${BUILD_ROOT}/openssl-ohos/${TARGET_TRIPLE}"

mkdir -p "${SOURCE_ROOT}"
mkdir -p "${PREFIX}"

if [[ -d "${OPENSSL_LOCAL_SOURCE}" && -f "${OPENSSL_LOCAL_SOURCE}/Configure" ]]; then
  SOURCE_DIR="${SOURCE_ROOT}/openssl-local"
  echo "[openssl-ohos] Using local OpenSSL source at ${OPENSSL_LOCAL_SOURCE}"
  rm -rf "${SOURCE_DIR}"
  rsync -a --delete "${OPENSSL_LOCAL_SOURCE}/" "${SOURCE_DIR}/"
else
  ARCHIVE="openssl-${OPENSSL_VERSION}.tar.gz"
  URL="https://www.openssl.org/source/${ARCHIVE}"
  SOURCE_DIR="${SOURCE_ROOT}/openssl-${OPENSSL_VERSION}"
  if [[ ! -d "${SOURCE_DIR}" ]]; then
    echo "[openssl-ohos] Fetching OpenSSL ${OPENSSL_VERSION}..."
    mkdir -p "${SOURCE_ROOT}"
    curl -fsSL "${URL}" -o "${SOURCE_ROOT}/${ARCHIVE}"
    tar -xzf "${SOURCE_ROOT}/${ARCHIVE}" -C "${SOURCE_ROOT}"
  fi
fi

pushd "${SOURCE_DIR}" >/dev/null
  make distclean >/dev/null 2>&1 || true

  CC="${LLVM_DIR}/bin/clang --target=${TARGET_TRIPLE} --sysroot=${SYSROOT_DIR}"
  AR="${LLVM_DIR}/bin/llvm-ar"
  export CC AR

  echo "[openssl-ohos] Configuring (target ${TARGET_TRIPLE})..."
  ./Configure linux-generic64 \
    enable-quic \
    shared \
    --prefix="${PREFIX}" \
    --openssldir="${PREFIX}/ssl"

  echo "[openssl-ohos] Building..."
  make -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)"

  echo "[openssl-ohos] Installing..."
  make install_sw
popd >/dev/null

# Mirror artefacts into the HarmonyOS project tree so the HAP picks them up.
DESTS=(
  "${REPO_ROOT}/examples/hos/entry/libs/arm64-v8a"
  "${REPO_ROOT}/examples/hos/entry/libs/arm64"
  "${REPO_ROOT}/examples/hos/entry/libs/arm64/lib"
  "${REPO_ROOT}/examples/hos/entry/libs/module/arm64-v8a"
  "${REPO_ROOT}/examples/hos/entry/libs/module/lib64"
  "${REPO_ROOT}/examples/hos/entry/src/main/jniLibs/arm64-v8a"
  "${REPO_ROOT}/examples/hos/entry/lib"
)

for dest in "${DESTS[@]}"; do
  mkdir -p "${dest}"
  cp "${PREFIX}/lib/libssl.so" "${dest}/libssl.so"
  cp "${PREFIX}/lib/libcrypto.so" "${dest}/libcrypto.so"
done

for dest in "${REPO_ROOT}/examples/hos/entry/libs/arm64-v8a" \
             "${REPO_ROOT}/examples/hos/entry/libs/arm64/lib" \
             "${REPO_ROOT}/examples/hos/entry/libs/module/arm64-v8a" \
             "${REPO_ROOT}/examples/hos/entry/libs/module/lib64" \
             "${REPO_ROOT}/examples/hos/entry/src/main/jniLibs/arm64-v8a"; do
  mkdir -p "${dest}/ossl-modules"
  rsync -a --delete "${PREFIX}/lib/ossl-modules/" "${dest}/ossl-modules/"
  if [[ -d "${PREFIX}/lib/engines-4" ]]; then
    mkdir -p "${dest}/engines-4"
    rsync -a --delete "${PREFIX}/lib/engines-4/" "${dest}/engines-4/"
  fi
done

echo "[openssl-ohos] Finished. Artifacts staged under ${PREFIX} and copied into the HarmonyOS project."
