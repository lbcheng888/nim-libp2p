#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $(basename "$0") (android|ohos) [--install] [--device SERIAL]

Builds the mobile mDNS/DM MVP examples under examples/android/ for the specified
target. With --install it also pushes the binaries plus runtime libraries to the
device.
USAGE
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

TARGET="$1"
shift

INSTALL=0
DEVICE_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install)
      INSTALL=1
      shift
      ;;
    --device)
      if [[ $# -lt 2 ]]; then
        echo "Missing argument for --device" >&2
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
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

case "$TARGET" in
  android|ohos) ;;
  *)
    echo "Unsupported target: $TARGET" >&2
    usage
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

EXAMPLE_SRCS=(
  "$SCRIPT_DIR/mobile_mdns_mvp.nim"
  "$SCRIPT_DIR/mobile_dm_mvp.nim"
)

build_android() {
  if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
    echo "[mobile-mvp] ANDROID_NDK_HOME not set" >&2
    exit 1
  fi
  local host_tag
  case "$(uname -s)" in
    Darwin) host_tag="darwin" ;;
    Linux) host_tag="linux" ;;
    *) echo "[mobile-mvp] unsupported host" >&2; exit 1 ;;
  esac
  local api=${API_LEVEL:-24}
  local triple="aarch64-linux-android"
  local toolchain="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/${host_tag}-x86_64"
  local clang="$toolchain/bin/${triple}${api}-clang"
  local ar="$toolchain/bin/llvm-ar"
  local sysroot="$toolchain/sysroot"
  local openssl_dir="${OPENSSL_DIR:-$REPO_ROOT/build/openssl-android/android-arm64}"
  if [[ ! -f "$openssl_dir/lib/libssl.so" ]]; then
    echo "[mobile-mvp] OpenSSL not found at $openssl_dir" >&2
    exit 1
  fi

  local out_root="$REPO_ROOT/build/android-examples"
  local bin_dir="$out_root/bin"
  local lib_dir="$out_root/lib"
  rm -rf "$bin_dir" "$lib_dir"
  mkdir -p "$bin_dir" "$lib_dir"
  export PATH="$toolchain/bin:$PATH"

  local nim_flags=(
    "c"
    "--app:console"
    "--os:android"
    "--cpu:arm64"
    "--mm:arc"
    "--deepCopy:on"
    "--threads:on"
    "--cc:clang"
    "--define:release"
    "--stacktrace:off"
    "--lineTrace:off"
    "--path:."
    "--path:libp2p"
    "--passC:-fPIC"
    "--passC:-I$REPO_ROOT/compat"
    "--passC:-includeexplicit_bzero.h"
    "--passC:-D_GNU_SOURCE"
    "--passC:--target=${triple}${api}"
    "--passC:--sysroot=$sysroot"
    "--passC:-I$openssl_dir/include"
    "--passL:--target=${triple}${api}"
    "--passL:--sysroot=$sysroot"
    "--passL:-L$openssl_dir/lib"
    "--passL:-L$lib_dir"
    "--passL:-l:libssl.so"
    "--passL:-l:libcrypto.so"
    "--passL:-llog"
    "--passL:-landroid"
  )

  for src in "${EXAMPLE_SRCS[@]}"; do
    local base
    base=$(basename "$src" .nim)
    local out="$bin_dir/${base}_android"
    echo "[mobile-mvp] building $src -> $out"
    CC="$clang" AR="$ar" nim "${nim_flags[@]}" "--out:$out" "$src"
  done

  for lib in libssl.so libcrypto.so; do
    cp "$openssl_dir/lib/$lib" "$lib_dir/$lib"
  done

  local libcxx="$toolchain/sysroot/usr/lib/${triple}/${api}/libc++_shared.so"
  [[ -f "$libcxx" ]] || libcxx="$toolchain/sysroot/usr/lib/${triple}/libc++_shared.so"
  cp "$libcxx" "$lib_dir/libc++_shared.so"

  for dir in ossl-modules engines-4; do
    if [[ -d "$openssl_dir/lib/$dir" ]]; then
      rm -rf "$lib_dir/$dir"
      mkdir -p "$lib_dir/$dir"
      cp -R "$openssl_dir/lib/$dir/." "$lib_dir/$dir/"
    fi
  done

  "$clang" -shared -fPIC --target=${triple}${api} --sysroot="$sysroot" \
    -I"$openssl_dir/include" -L"$openssl_dir/lib" \
    -lssl -lcrypto \
    "$REPO_ROOT/compat/openssl_compat.c" \
    -o "$lib_dir/libcrypto_compat.so"

  cat <<'SH' > "$bin_dir/run_mobile_mdns.sh"
#!/system/bin/sh
APP_ROOT="/data/local/tmp/nimlibp2p_mvp"
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"
export LD_PRELOAD="$APP_ROOT/lib/libcrypto_compat.so${LD_PRELOAD:+:$LD_PRELOAD}"
exec "$APP_ROOT/bin/mobile_mdns_mvp_android" "$@"
SH

  cat <<'SH' > "$bin_dir/run_mobile_dm.sh"
#!/system/bin/sh
APP_ROOT="/data/local/tmp/nimlibp2p_mvp"
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"
export LD_PRELOAD="$APP_ROOT/lib/libcrypto_compat.so${LD_PRELOAD:+:$LD_PRELOAD}"
exec "$APP_ROOT/bin/mobile_dm_mvp_android" "$@"
SH

  chmod +x "$bin_dir"/*.sh "$bin_dir"/*_android

  if [[ $INSTALL -eq 1 ]]; then
    local adb=${ADB:-adb}
    local args=()
    if [[ -n "$DEVICE_ID" ]]; then
      args+=("-s" "$DEVICE_ID")
    fi
    local dest="/data/local/tmp/nimlibp2p_mvp"
    if [[ ${#args[@]} -gt 0 ]]; then
      $adb "${args[@]}" shell "rm -rf $dest && mkdir -p $dest/bin $dest/lib"
      $adb "${args[@]}" push "$bin_dir" "$dest/" >/dev/null
      $adb "${args[@]}" push "$lib_dir" "$dest/" >/dev/null
      $adb "${args[@]}" shell "chmod 755 $dest/bin/* $dest/lib/*.so"
    else
      $adb shell "rm -rf $dest && mkdir -p $dest/bin $dest/lib"
      $adb push "$bin_dir" "$dest/" >/dev/null
      $adb push "$lib_dir" "$dest/" >/dev/null
      $adb shell "chmod 755 $dest/bin/* $dest/lib/*.so"
    fi
    echo "[mobile-mvp] Android payload installed to $dest"
  else
    echo "[mobile-mvp] Android artifacts at $out_root"
  fi
}

build_ohos() {
  local oh_root=${OH_SDK_ROOT:-"/Applications/DevEco-Studio.app/Contents/sdk/default/openharmony/native"}
  local llvm_dir="$oh_root/llvm"
  local sysroot_dir="$oh_root/sysroot"
  if [[ ! -d "$llvm_dir" || ! -d "$sysroot_dir" ]]; then
    echo "[mobile-mvp] OpenHarmony toolchain missing; set OH_SDK_ROOT" >&2
    exit 1
  fi
  local openssl_dir="${OPENSSL_DIR:-$REPO_ROOT/build/openssl-ohos/aarch64-linux-ohos}"
  if [[ ! -f "$openssl_dir/lib/libssl.so" ]]; then
    echo "[mobile-mvp] OpenSSL for OHOS not found at $openssl_dir" >&2
    exit 1
  fi

  local out_root="$REPO_ROOT/build/ohos-examples"
  local bin_dir="$out_root/bin"
  local lib_dir="$out_root/lib"
  rm -rf "$bin_dir" "$lib_dir"
  mkdir -p "$bin_dir" "$lib_dir"

  local clang="$llvm_dir/bin/clang"
  local ar="$llvm_dir/bin/llvm-ar"
  export PATH="$llvm_dir/bin:$PATH"
  local target="aarch64-linux-ohos"
  local sysflag="--sysroot=$sysroot_dir"
  local targetflag="--target=$target"

  local nim_flags=(
    "c"
    "--app:console"
    "--os:linux"
    "--cpu:arm64"
    "--mm:arc"
    "--deepCopy:on"
    "--threads:on"
    "--cc:clang"
    "--define:release"
    "--define:ohos"
    "--define:chronicles_enabled=false"
    "--stacktrace:off"
    "--lineTrace:off"
    "--path:."
    "--path:libp2p"
    "--passC:-fPIC"
    "--passC:-I$REPO_ROOT/compat"
    "--passC:-includeexplicit_bzero.h"
    "--passC:$targetflag"
    "--passC:$sysflag"
    "--passC:-I$openssl_dir/include"
    "--passL:$targetflag"
    "--passL:$sysflag"
    "--passL:-L$openssl_dir/lib"
    "--passL:-L$lib_dir"
    "--passL:-shared"
    "--passL:-Wl,-export-dynamic"
    "--passL:-lhilog_ndk.z"
    "--passL:-l:libssl.so"
    "--passL:-l:libcrypto.so"
  )

  for src in "${EXAMPLE_SRCS[@]}"; do
    local base
    base=$(basename "$src" .nim)
    local out="$bin_dir/${base}_ohos"
    echo "[mobile-mvp] building $src -> $out"
    CC="$clang $targetflag $sysflag" AR="$ar" nim "${nim_flags[@]}" "--out:$out" "$src"
  done

  for lib in libssl.so libcrypto.so; do
    cp "$openssl_dir/lib/$lib" "$lib_dir/$lib"
  done

  for dir in ossl-modules engines-4; do
    if [[ -d "$openssl_dir/lib/$dir" ]]; then
      rm -rf "$lib_dir/$dir"
      mkdir -p "$lib_dir/$dir"
      cp -R "$openssl_dir/lib/$dir/." "$lib_dir/$dir/"
    fi
  done

  $clang $targetflag $sysflag -shared -fPIC \
    -I"$openssl_dir/include" -L"$openssl_dir/lib" \
    -lssl -lcrypto \
    "$REPO_ROOT/compat/openssl_compat.c" \
    -o "$lib_dir/libcrypto_compat.so"

  cat <<'SH' > "$bin_dir/run_mobile_mdns.sh"
#!/system/bin/sh
APP_ROOT="/data/local/tmp/nimlibp2p_mvp"
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"
export LD_PRELOAD="$APP_ROOT/lib/libcrypto_compat.so${LD_PRELOAD:+:$LD_PRELOAD}"
exec "$APP_ROOT/bin/mobile_mdns_mvp_ohos" "$@"
SH

  cat <<'SH' > "$bin_dir/run_mobile_dm.sh"
#!/system/bin/sh
APP_ROOT="/data/local/tmp/nimlibp2p_mvp"
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"
export LD_PRELOAD="$APP_ROOT/lib/libcrypto_compat.so${LD_PRELOAD:+:$LD_PRELOAD}"
exec "$APP_ROOT/bin/mobile_dm_mvp_ohos" "$@"
SH

  chmod +x "$bin_dir"/*.sh "$bin_dir"/*_ohos

  if [[ $INSTALL -eq 1 ]]; then
    local hdc=${HDC:-hdc}
    local args=()
    if [[ -n "$DEVICE_ID" ]]; then
      args+=("-t" "$DEVICE_ID")
    fi
    local dest="/data/local/tmp/nimlibp2p_mvp"
    if [[ ${#args[@]} -gt 0 ]]; then
      $hdc "${args[@]}" shell "rm -rf $dest && mkdir -p $dest/bin $dest/lib"
      for bin in "$bin_dir"/*; do
        local name
        name=$(basename "$bin")
        $hdc "${args[@]}" file send "$bin" "$dest/bin/$name" >/dev/null
      done
      for entry in "$lib_dir"/*; do
        local name
        name=$(basename "$entry")
        if [[ -d "$entry" ]]; then
          $hdc "${args[@]}" file send -r "$entry" "$dest/lib/$name" >/dev/null
        else
          $hdc "${args[@]}" file send "$entry" "$dest/lib/$name" >/dev/null
        fi
      done
      $hdc "${args[@]}" shell "chmod 755 $dest/bin/* $dest/lib/*.so" >/dev/null || true
    else
      $hdc shell "rm -rf $dest && mkdir -p $dest/bin $dest/lib"
      for bin in "$bin_dir"/*; do
        local name
        name=$(basename "$bin")
        $hdc file send "$bin" "$dest/bin/$name" >/dev/null
      done
      for entry in "$lib_dir"/*; do
        local name
        name=$(basename "$entry")
        if [[ -d "$entry" ]]; then
          $hdc file send -r "$entry" "$dest/lib/$name" >/dev/null
        else
          $hdc file send "$entry" "$dest/lib/$name" >/dev/null
        fi
      done
      $hdc shell "chmod 755 $dest/bin/* $dest/lib/*.so" >/dev/null || true
    fi
    echo "[mobile-mvp] HarmonyOS payload installed to $dest (note: directory may be mounted noexec)"
  else
    echo "[mobile-mvp] HarmonyOS artifacts at $out_root"
  fi
}

case "$TARGET" in
  android) build_android ;;
  ohos) build_ohos ;;
esac
