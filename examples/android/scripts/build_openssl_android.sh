#!/usr/bin/env bash
set -euo pipefail

# Build OpenSSL (shared + static) for Android arm64. Gradle calls this helper
# before building the Nim shared library. The artifacts are written to
# $REPO_ROOT/build/openssl-android/android-arm64 and mirrored under the Android
# example's local build/ directory for Gradle's convenience.

API_LEVEL="${1:-24}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
OPENSSL_LOCAL_SOURCE="${OPENSSL_SOURCE_DIR:-/Users/lbcheng/openssl}"
OPENSSL_VERSION="${OPENSSL_VERSION:-4.0.0-dev}"
OPENSSL_ARCHIVE="openssl-${OPENSSL_VERSION}.tar.gz"
OPENSSL_URL="https://www.openssl.org/source/${OPENSSL_ARCHIVE}"
SOURCE_ROOT="${PROJECT_ROOT}/build/_deps/openssl"
SOURCE_DIR="${SOURCE_ROOT}/openssl-${OPENSSL_VERSION}"
INSTALL_ROOT="${PROJECT_ROOT}/build/openssl-android/android-arm64"
ANDROID_BUILD_DIR="${PROJECT_ROOT}/examples/android/build/openssl-android/android-arm64"

if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
  echo "[openssl-android] ANDROID_NDK_HOME is not set" >&2
  exit 1
fi
if [[ ! -d "${ANDROID_NDK_HOME}" ]]; then
  echo "[openssl-android] ANDROID_NDK_HOME=${ANDROID_NDK_HOME} is not a directory" >&2
  exit 1
fi

case "$(uname -s)" in
  Darwin)
    HOST_TAG="darwin-x86_64"
    ;;
  Linux)
    HOST_TAG="linux-x86_64"
    ;;
  *)
    echo "[openssl-android] Unsupported host OS $(uname -s)" >&2
    exit 1
    ;;
esac

export ANDROID_NDK_ROOT="${ANDROID_NDK_HOME}"
export PATH="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/${HOST_TAG}/bin:${PATH}"

mkdir -p "${SOURCE_ROOT}"
mkdir -p "${INSTALL_ROOT}"

if [[ -d "${OPENSSL_LOCAL_SOURCE}" && -f "${OPENSSL_LOCAL_SOURCE}/Configure" ]]; then
  echo "[openssl-android] Using local OpenSSL source at ${OPENSSL_LOCAL_SOURCE}"
  SOURCE_DIR="${SOURCE_ROOT}/openssl-local"
  rm -rf "${SOURCE_DIR}"
  rsync -a --delete "${OPENSSL_LOCAL_SOURCE}/" "${SOURCE_DIR}/"
else
  if [[ ! -d "${SOURCE_DIR}" ]]; then
    echo "[openssl-android] Fetching OpenSSL ${OPENSSL_VERSION}..."
    curl -fsSL "${OPENSSL_URL}" -o "${SOURCE_ROOT}/${OPENSSL_ARCHIVE}"
    tar -xzf "${SOURCE_ROOT}/${OPENSSL_ARCHIVE}" -C "${SOURCE_ROOT}"
  fi
fi

pushd "${SOURCE_DIR}" >/dev/null
  # Clean previous builds to avoid configuration mismatches.
  make distclean >/dev/null 2>&1 || true
  echo "[openssl-android] Configuring (android-arm64, API ${API_LEVEL})..."
  ./Configure android-arm64 "-D__ANDROID_API__=${API_LEVEL}" \
    enable-quic \
    --prefix="${INSTALL_ROOT}" \
    --openssldir="${INSTALL_ROOT}/ssl" \
    shared no-tests

  echo "[openssl-android] Building..."
  make -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)" >/dev/null
  echo "[openssl-android] Installing..."
  make install_sw >/dev/null
popd >/dev/null

# Mirror the install tree into the Android example's local build/ directory so
# Gradle's existence checks succeed.
mkdir -p "${ANDROID_BUILD_DIR}"
rsync -a --delete "${INSTALL_ROOT}/" "${ANDROID_BUILD_DIR}/"

echo "[openssl-android] Finished. Artifacts available under:"
echo "  - ${INSTALL_ROOT}"
echo "  - ${ANDROID_BUILD_DIR}"
