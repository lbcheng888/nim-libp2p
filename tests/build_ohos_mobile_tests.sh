#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--install] [--target TARGET_ID]

Cross-compiles HarmonyOS (OpenHarmony) ARM64 binaries for mDNS self-test and
Direct DM test, stages runtime libs under build/ohos-tests, and optionally
pushes them onto a connected device via hdc.
USAGE
}

INSTALL=0
TARGET_ID=${OHOS_DEVICE_ID:-}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install)
      INSTALL=1
      shift
      ;;
    --target)
      if [[ $# -lt 2 ]]; then
        echo "[ohos-tests] --target requires an argument" >&2
        exit 1
      fi
      TARGET_ID="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ohos-tests] unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

OH_SDK_ROOT=${OH_SDK_ROOT:-"/Applications/DevEco-Studio.app/Contents/sdk/default/openharmony/native"}
LLVM_DIR="$OH_SDK_ROOT/llvm"
SYSROOT_DIR="$OH_SDK_ROOT/sysroot"
if [[ ! -d "$LLVM_DIR" || ! -d "$SYSROOT_DIR" ]]; then
  echo "[ohos-tests] OpenHarmony toolchain not found under $OH_SDK_ROOT" >&2
  exit 1
fi

OPENSSL_DIR_DEFAULT="$(pwd)/build/openssl-ohos/aarch64-linux-ohos"
OPENSSL_DIR="${OPENSSL_DIR:-$OPENSSL_DIR_DEFAULT}"
if [[ ! -d "$OPENSSL_DIR/lib" ]]; then
  echo "[ohos-tests] OpenSSL artefacts missing under $OPENSSL_DIR" >&2
  echo "[ohos-tests] Run scripts/build_openssl_ohos.sh or set OPENSSL_DIR" >&2
  exit 1
fi

REPO_ROOT="$(pwd)"
BUILD_ROOT="$REPO_ROOT/build/ohos-tests"

MSQUIC_BOOTSTRAP="$REPO_ROOT/nim-libp2p/scripts/nim/bootstrap_msquic.sh"
if [[ -x "$MSQUIC_BOOTSTRAP" ]]; then
  eval "$("$MSQUIC_BOOTSTRAP" env || true)"
fi
BIN_DIR="$BUILD_ROOT/bin"
LIB_DIR="$BUILD_ROOT/lib"

rm -rf "$BIN_DIR" "$LIB_DIR"
mkdir -p "$BIN_DIR" "$LIB_DIR"

CLANG_BIN="$LLVM_DIR/bin/clang"
AR_BIN="$LLVM_DIR/bin/llvm-ar"
export PATH="$LLVM_DIR/bin:$PATH"
TARGET_TRIPLE="aarch64-linux-ohos"
SYSROOT_FLAG="--sysroot=$SYSROOT_DIR"
TARGET_FLAG="--target=$TARGET_TRIPLE"

COMMON_FLAGS=(
  "c"
  "--app:console"
  "--os:linux"
  "--cpu:arm64"
  "--mm:arc"
  "--deepCopy:on"
  "--threads:on"
  "--cc:clang"
  "--define:release"
  "--define:libp2p_msquic_experimental"
  "--define:ohos"
  "--define:chronicles_enabled=false"
  "--stacktrace:off"
  "--lineTrace:off"
  "--path:."
  "--path:libp2p"
  "--passC:-fPIC"
  "--passC:-I$REPO_ROOT/compat"
  "--passC:-includeexplicit_bzero.h"
  "--passC:$TARGET_FLAG"
  "--passC:$SYSROOT_FLAG"
  "--passC:-I$OPENSSL_DIR/include"
  "--passL:$TARGET_FLAG"
  "--passL:$SYSROOT_FLAG"
  "--passL:-L$OPENSSL_DIR/lib"
  "--passL:-L$LIB_DIR"
  "--passL:-Wl,-export-dynamic"
  "--passL:-Wl,-rpath,\$ORIGIN/../lib"
  "--passL:-lhilog_ndk.z"
  "--passL:-l:libssl.so"
  "--passL:-l:libcrypto.so"
)

DYNAMIC_LINKER="${OHOS_TEST_DYNAMIC_LINKER:-}"
if [[ -n "$DYNAMIC_LINKER" ]]; then
  COMMON_FLAGS+=("--passL:-Wl,-dynamic-linker,$DYNAMIC_LINKER")
fi

build_test() {
  local src="$1"
  local out_name="$2"
  echo "[ohos-tests] Building $src -> $out_name"
  CC="$CLANG_BIN $TARGET_FLAG $SYSROOT_FLAG" AR="$AR_BIN" nim "${COMMON_FLAGS[@]}" "--out:$BIN_DIR/$out_name" "$src"
}

build_test tests/android_mdns_selftest.nim mdns_selftest_ohos
build_test tests/android_direct_dm_test.nim direct_dm_test_ohos

for lib in libssl.so libcrypto.so; do
  if [[ -f "$OPENSSL_DIR/lib/$lib" ]]; then
    cp "$OPENSSL_DIR/lib/$lib" "$LIB_DIR/$lib"
    cp "$OPENSSL_DIR/lib/$lib" "$LIB_DIR/${lib%.so}3.so"
  fi
done

for dir in ossl-modules engines-4; do
  if [[ -d "$OPENSSL_DIR/lib/$dir" ]]; then
    rm -rf "$LIB_DIR/$dir"
    mkdir -p "$LIB_DIR/$dir"
    cp -R "$OPENSSL_DIR/lib/$dir/." "$LIB_DIR/$dir/"
  fi
done

$CLANG_BIN $TARGET_FLAG $SYSROOT_FLAG -shared -fPIC \
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
exec "$APP_ROOT/bin/mdns_selftest_ohos" "$@"
RUN

cat <<'RUN' > "$BIN_DIR/run_direct_dm_test.sh"
#!/system/bin/sh
APP_ROOT="/data/local/tmp/nimlibp2p"
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"
export LD_PRELOAD="$APP_ROOT/lib/libcrypto_compat.so${LD_PRELOAD:+:$LD_PRELOAD}"
exec "$APP_ROOT/bin/direct_dm_test_ohos" "$@"
RUN

chmod +x "$BIN_DIR"/*.sh "$BIN_DIR"/*_ohos

if [[ $INSTALL -eq 0 ]]; then
  echo "[ohos-tests] Build artifacts staged under $BUILD_ROOT"
  exit 0
fi

HDC_BIN=${HDC:-hdc}
HDC_ARGS=()
if [[ -n "${TARGET_ID:-}" ]]; then
  HDC_ARGS+=("-t" "$TARGET_ID")
fi
if [[ ${#HDC_ARGS[@]} -eq 0 ]]; then
  HDC_ARGS=("")
fi

dest_root="/data/local/tmp/nimlibp2p"
$HDC_BIN ${HDC_ARGS[@]} shell "rm -rf $dest_root && mkdir -p $dest_root/bin $dest_root/lib"
$HDC_BIN ${HDC_ARGS[@]} file send "$BIN_DIR/mdns_selftest_ohos" "$dest_root/bin/mdns_selftest_ohos" >/dev/null
$HDC_BIN ${HDC_ARGS[@]} file send "$BIN_DIR/direct_dm_test_ohos" "$dest_root/bin/direct_dm_test_ohos" >/dev/null
for script in run_mdns_selftest.sh run_direct_dm_test.sh; do
  $HDC_BIN ${HDC_ARGS[@]} file send "$BIN_DIR/$script" "$dest_root/bin/$script" >/dev/null
done
${HDC_ARGS[@]:+true} >/dev/null 2>&1 || true
$HDC_BIN ${HDC_ARGS[@]} shell "mkdir -p $dest_root/lib" >/dev/null
for entry in "$LIB_DIR"/*; do
  name=$(basename "$entry")
  if [[ -d "$entry" ]]; then
    $HDC_BIN ${HDC_ARGS[@]} file send -r "$entry" "$dest_root/lib/$name" >/dev/null
  else
    $HDC_BIN ${HDC_ARGS[@]} file send "$entry" "$dest_root/lib/$name" >/dev/null
  fi
done
$HDC_BIN ${HDC_ARGS[@]} shell "chmod 755 $dest_root/bin/* $dest_root/lib/*.so" >/dev/null || true

echo "[ohos-tests] Payload installed to $dest_root"
