#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--install] [--device SERIAL]

Builds Android ARM64 test binaries (mDNS selftest, direct DM) and stages runtime
libraries under build/android-tests. With --install it also pushes the payload
onto a connected device (default location /data/local/tmp/nimlibp2p).
USAGE
}

INSTALL=0
DEVICE_ID=${ANDROID_DEVICE_ID:-}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install)
      INSTALL=1
      shift
      ;;
    --device)
      if [[ $# -lt 2 ]]; then
        echo "[android-tests] --device requires an argument" >&2
        exit 1
      fi
      DEVICE_ID="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[android-tests] unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
  echo "[android-tests] ANDROID_NDK_HOME is not set" >&2
  exit 1
fi

if [[ ! -d "$ANDROID_NDK_HOME" ]]; then
  echo "[android-tests] ANDROID_NDK_HOME=$ANDROID_NDK_HOME is not a directory" >&2
  exit 1
fi

HOST_UNAME=$(uname -s)
case "$HOST_UNAME" in
  Darwin) HOST_TAG="darwin" ;;
  Linux) HOST_TAG="linux" ;;
  *) echo "[android-tests] unsupported host: $HOST_UNAME" >&2; exit 1 ;;
esac

API_LEVEL=${API_LEVEL:-24}
TARGET_TRIPLE="aarch64-linux-android"
TOOLCHAIN_ROOT="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/${HOST_TAG}-x86_64"
CLANG_BIN="$TOOLCHAIN_ROOT/bin/${TARGET_TRIPLE}${API_LEVEL}-clang"
AR_BIN="$TOOLCHAIN_ROOT/bin/llvm-ar"
SYSROOT="$TOOLCHAIN_ROOT/sysroot"

if [[ ! -x "$CLANG_BIN" ]]; then
  echo "[android-tests] clang not found at $CLANG_BIN" >&2
  exit 1
fi

OPENSSL_DIR_DEFAULT="$(pwd)/build/openssl-android/android-arm64"
OPENSSL_DIR="${OPENSSL_DIR:-$OPENSSL_DIR_DEFAULT}"
if [[ ! -d "$OPENSSL_DIR/lib" ]]; then
  echo "[android-tests] OpenSSL artefacts missing under $OPENSSL_DIR" >&2
  echo "[android-tests] Run scripts/build_openssl_android.sh or set OPENSSL_DIR" >&2
  exit 1
fi

REPO_ROOT="$(pwd)"
BUILD_ROOT="$REPO_ROOT/build/android-tests"

MSQUIC_BOOTSTRAP="$REPO_ROOT/nim-libp2p/scripts/nim/bootstrap_msquic.sh"
if [[ -x "$MSQUIC_BOOTSTRAP" ]]; then
  eval "$("$MSQUIC_BOOTSTRAP" env || true)"
fi
BIN_DIR="$BUILD_ROOT/bin"
LIB_DIR="$BUILD_ROOT/lib"

rm -rf "$BIN_DIR" "$LIB_DIR"
mkdir -p "$BIN_DIR" "$LIB_DIR"

export PATH="$TOOLCHAIN_ROOT/bin:$PATH"
COMMON_NIM_FLAGS=(
  "c"
  "--app:console"
  "--os:android"
  "--cpu:arm64"
  "--mm:arc"
  "--deepCopy:on"
  "--threads:on"
  "--cc:clang"
  "--define:release"
  "--define:libp2p_msquic_experimental"
  "--stacktrace:off"
  "--lineTrace:off"
  "--path:."
  "--path:libp2p"
  "--passC:-fPIC"
  "--passC:-I$REPO_ROOT/compat"
  "--passC:-includeexplicit_bzero.h"
  "--passC:-D_GNU_SOURCE"
  "--passC:--target=${TARGET_TRIPLE}${API_LEVEL}"
  "--passC:--sysroot=$SYSROOT"
  "--passC:-I$OPENSSL_DIR/include"
  "--passL:--target=${TARGET_TRIPLE}${API_LEVEL}"
  "--passL:--sysroot=$SYSROOT"
  "--passL:-L$OPENSSL_DIR/lib"
  "--passL:-L$LIB_DIR"
  "--passL:-l:libssl.so"
  "--passL:-l:libcrypto.so"
  "--passL:-llog"
  "--passL:-landroid"
  "--passL:-Wl,-rpath,\$ORIGIN/../lib"
)

build_test() {
  local src="$1"
  local out_name="$2"
  echo "[android-tests] Building $src -> $out_name"
  CC="$CLANG_BIN" AR="$AR_BIN" nim "${COMMON_NIM_FLAGS[@]}" "--out:$BIN_DIR/$out_name" "$src"
}

build_test tests/android_mdns_selftest.nim mdns_selftest_android
build_test tests/android_direct_dm_test.nim direct_dm_test_android

# Stage OpenSSL shared objects (both legacy and versioned names for compat).
for lib in libssl.so libcrypto.so; do
  if [[ -f "$OPENSSL_DIR/lib/$lib" ]]; then
    cp "$OPENSSL_DIR/lib/$lib" "$LIB_DIR/$lib"
    cp "$OPENSSL_DIR/lib/$lib" "$LIB_DIR/${lib%.so}3.so"
  fi
done

# libc++ runtime from NDK sysroot
LIBCXX_SRC="$TOOLCHAIN_ROOT/sysroot/usr/lib/${TARGET_TRIPLE}/${API_LEVEL}/libc++_shared.so"
if [[ ! -f "$LIBCXX_SRC" ]]; then
  LIBCXX_SRC="$TOOLCHAIN_ROOT/sysroot/usr/lib/${TARGET_TRIPLE}/libc++_shared.so"
fi
if [[ -f "$LIBCXX_SRC" ]]; then
  cp "$LIBCXX_SRC" "$LIB_DIR/libc++_shared.so"
else
  echo "[android-tests] WARNING: libc++_shared.so not found; dm binary may require manual copy" >&2
fi

# Copy OpenSSL provider directories if present.
for dir in ossl-modules engines-4; do
  if [[ -d "$OPENSSL_DIR/lib/$dir" ]]; then
    rm -rf "$LIB_DIR/$dir"
    mkdir -p "$LIB_DIR/$dir"
    cp -R "$OPENSSL_DIR/lib/$dir/." "$LIB_DIR/$dir/"
  fi
done

# Build the compatibility shim translating legacy OpenSSL 1.x symbols.
$CLANG_BIN -shared -fPIC --target=${TARGET_TRIPLE}${API_LEVEL} --sysroot="$SYSROOT" \
  -I"$OPENSSL_DIR/include" -L"$OPENSSL_DIR/lib" \
  -Wl,-rpath,'$ORIGIN' \
  -lssl -lcrypto \
  "$REPO_ROOT/compat/openssl_compat.c" \
  -o "$LIB_DIR/libcrypto_compat.so"

cat <<'RUN' > "$BIN_DIR/run_mdns_selftest.sh"
#!/system/bin/sh
APP_ROOT="/data/local/tmp/nimlibp2p"
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"
export LD_PRELOAD="$APP_ROOT/lib/libcrypto_compat.so${LD_PRELOAD:+:$LD_PRELOAD}"
exec "$APP_ROOT/bin/mdns_selftest_android" "$@"
RUN

cat <<'RUN' > "$BIN_DIR/run_direct_dm_test.sh"
#!/system/bin/sh
APP_ROOT="/data/local/tmp/nimlibp2p"
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"
export LD_PRELOAD="$APP_ROOT/lib/libcrypto_compat.so${LD_PRELOAD:+:$LD_PRELOAD}"
exec "$APP_ROOT/bin/direct_dm_test_android" "$@"
RUN

chmod +x "$BIN_DIR"/*.sh "$BIN_DIR"/*_android

if [[ $INSTALL -eq 0 ]]; then
  echo "[android-tests] Build artifacts staged under $BUILD_ROOT"
  exit 0
fi

ADB_BIN=${ADB:-adb}
ADB_ARGS=()
if [[ -n "$DEVICE_ID" ]]; then
  ADB_ARGS+=("-s" "$DEVICE_ID")
fi

dest_root="/data/local/tmp/nimlibp2p"
$ADB_BIN "${ADB_ARGS[@]}" shell "rm -rf $dest_root && mkdir -p $dest_root/bin $dest_root/lib"
$ADB_BIN "${ADB_ARGS[@]}" push "$BIN_DIR" "$dest_root/" >/dev/null
$ADB_BIN "${ADB_ARGS[@]}" push "$LIB_DIR" "$dest_root/" >/dev/null
$ADB_BIN "${ADB_ARGS[@]}" shell "chmod 755 $dest_root/bin/* $dest_root/lib/*.so" >/dev/null
if $ADB_BIN "${ADB_ARGS[@]}" shell "command -v restorecon" >/dev/null 2>&1; then
  $ADB_BIN "${ADB_ARGS[@]}" shell "restorecon -RF $dest_root" >/dev/null || true
fi

echo "[android-tests] Payload installed to $dest_root"
