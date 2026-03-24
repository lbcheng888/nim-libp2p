#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNIMAKER_ANDROID_ROOT="${UNIMAKER_ANDROID_ROOT:-/Users/lbcheng/UniMaker/android}"
HEADSCALE_JSON_DEFAULT="/Users/lbcheng/UniMaker/examples/go-libp2p-mobile/build/headscale-singapore.json"
HEADSCALE_JSON="${HEADSCALE_JSON:-$HEADSCALE_JSON_DEFAULT}"
ANDROID_TMP="${ANDROID_TMP:-/data/local/tmp/nim-libp2p-tsnet-acceptance}"
ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-/Users/lbcheng/Library/Android/sdk/ndk/26.1.10909125}"
API_LEVEL="${API_LEVEL:-24}"
HOST_ENABLE_QUIC="${HOST_ENABLE_QUIC:-1}"
WORK_DIR="${WORK_DIR:-$REPO_ROOT/build/tsnet-acceptance-$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$WORK_DIR"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--headscale-json PATH]

Environment overrides:
  HEADSCALE_JSON
  ANDROID_TMP
  ANDROID_NDK_HOME
  API_LEVEL
  HOST_ENABLE_QUIC=1|0
  WORK_DIR
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --headscale-json)
      HEADSCALE_JSON="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$HEADSCALE_JSON" ]]; then
  echo "missing headscale config: $HEADSCALE_JSON" >&2
  exit 1
fi

ADB_SERIAL="${ADB_SERIAL:-$(adb devices | awk 'NR>1 && $2=="device"{print $1; exit}')}"
if [[ -z "$ADB_SERIAL" ]]; then
  echo "no adb device available" >&2
  exit 1
fi

HEADSCALE_ENV="$(
  python3 - "$HEADSCALE_JSON" <<'PY'
import json, shlex, sys
cfg = json.load(open(sys.argv[1], "r", encoding="utf-8"))
control_url = cfg.get("controlURL") or cfg.get("controlUrl")
auth_key = cfg.get("authKey")
derp_region_id = cfg.get("derpRegionId", cfg.get("regionId"))
derp_region_code = cfg.get("derpRegionCode", cfg.get("regionCode"))
if not control_url:
    raise SystemExit("missing controlURL/controlUrl in headscale json")
if not auth_key:
    raise SystemExit("missing authKey in headscale json")
if derp_region_id is None or not derp_region_code:
    raise SystemExit("missing derp region in headscale json")
for key, value in {
    "CONTROL_URL": control_url,
    "AUTH_KEY": auth_key,
    "EXPECTED_DERP": f"{derp_region_id}/{derp_region_code}",
}.items():
    print(f"{key}={shlex.quote(str(value))}")
PY
)"
while IFS= read -r line; do
  eval "$line"
done <<< "$HEADSCALE_ENV"

HOST_BRIDGE="$REPO_ROOT/build/libtsnetbridge.dylib"
ANDROID_BRIDGE="$UNIMAKER_ANDROID_ROOT/core-bridge/src/main/jniLibs/arm64-v8a/libtsnetbridge.so"
HOST_BIN="$WORK_DIR/tsnet_acceptance_host"
ANDROID_BIN="$WORK_DIR/tsnet_acceptance_android"
HOST_NIMCACHE="$WORK_DIR/nimcache-host"
ANDROID_NIMCACHE="$WORK_DIR/nimcache-android"
MAC_LOCAL="$WORK_DIR/mac-local.json"
MAC_REMOTE="$WORK_DIR/android-remote.json"
MAC_RESULT="$WORK_DIR/mac-result.json"
MAC_LOG="$WORK_DIR/mac.log"
ANDROID_LOCAL_HOST="$WORK_DIR/android-local.json"
ANDROID_RESULT_HOST="$WORK_DIR/android-result.json"
ANDROID_LOG_HOST="$WORK_DIR/android.log"
SUMMARY_JSON="$WORK_DIR/summary.json"

cleanup() {
  if [[ -n "${MAC_PID:-}" ]]; then
    kill "$MAC_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

build_host_bridge() {
  if [[ -f "$HOST_BRIDGE" ]]; then
    return
  fi
  bash "$REPO_ROOT/scripts/tailnet/build_tsnetbridge_host.sh"
}

build_android_bridge() {
  if [[ -f "$ANDROID_BRIDGE" ]]; then
    return
  fi
  ANDROID_NDK_HOME="$ANDROID_NDK_HOME" bash "$REPO_ROOT/scripts/tailnet/build_tsnetbridge_android.sh"
}

build_host_harness() {
  local -a flags=(
    c
    --hints:off
    --warnings:off
    --threads:on
    --mm:arc
    --deepCopy:on
    --out:"$HOST_BIN"
    --nimcache:"$HOST_NIMCACHE"
  )
  if [[ "$HOST_ENABLE_QUIC" == "1" ]]; then
    flags+=(--define:libp2p_msquic_experimental --define:libp2p_msquic_builtin)
  fi
  (
    cd "$REPO_ROOT"
    nim "${flags[@]}" examples/mobile_ffi/tsnet_acceptance_harness.nim
  )
}

build_android_harness() {
  local host_tag llvm_prebuilt clang_bin ar_bin sysroot
  case "$(uname -s)" in
    Darwin) host_tag="darwin" ;;
    Linux) host_tag="linux" ;;
    *) echo "unsupported host os" >&2; exit 1 ;;
  esac
  llvm_prebuilt="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/${host_tag}-x86_64/bin"
  clang_bin="$llvm_prebuilt/aarch64-linux-android${API_LEVEL}-clang"
  ar_bin="$llvm_prebuilt/llvm-ar"
  sysroot="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/${host_tag}-x86_64/sysroot"
  if [[ ! -x "$clang_bin" ]]; then
    echo "missing android clang: $clang_bin" >&2
    exit 1
  fi
  (
    cd "$REPO_ROOT"
    export PATH="$llvm_prebuilt:$PATH"
    CC="$clang_bin" \
    CXX="$clang_bin" \
    AR="$ar_bin" \
    nim c \
      --hints:off \
      --warnings:off \
      --app:console \
      --os:android \
      --cpu:arm64 \
      --threads:on \
      --mm:arc \
      --deepCopy:on \
      --cc:clang \
      --define:libp2p_msquic_experimental \
      --define:libp2p_msquic_builtin \
      --define:libp2p_pure_crypto \
      --define:nimcrypto_disable_asm \
      --define:noSignalHandler \
      --stacktrace:off \
      --lineTrace:off \
      --path:examples/mobile_ffi/compat/chronicles_stub \
      --path:. \
      --path:examples/mobile_ffi \
      --path:"$REPO_ROOT/nimbledeps/pkgs2" \
      --path:"$REPO_ROOT/vendor/secp256k1" \
      --passC:-Iexamples/mobile_ffi/compat \
      --passC:-I"$REPO_ROOT" \
      --passC:-include \
      --passC:"$REPO_ROOT/examples/mobile_ffi/compat/explicit_bzero.h" \
      --passC:-D_POSIX_C_SOURCE=200809L \
      --passC:--target=aarch64-linux-android${API_LEVEL} \
      --passC:--sysroot="$sysroot" \
      --passL:--target=aarch64-linux-android${API_LEVEL} \
      --passL:--sysroot="$sysroot" \
      --passL:-llog \
      --passL:-landroid \
      --nimcache:"$ANDROID_NIMCACHE" \
      --out:"$ANDROID_BIN" \
      examples/mobile_ffi/tsnet_acceptance_harness.nim
  )
}

build_host_bridge
build_android_bridge
build_host_harness
build_android_harness

adb -s "$ADB_SERIAL" shell "rm -rf '$ANDROID_TMP' && mkdir -p '$ANDROID_TMP'"
adb -s "$ADB_SERIAL" push "$ANDROID_BIN" "$ANDROID_TMP/tsnet_acceptance_android" >/dev/null
adb -s "$ADB_SERIAL" push "$ANDROID_BRIDGE" "$ANDROID_TMP/libtsnetbridge.so" >/dev/null
adb -s "$ADB_SERIAL" shell "chmod 755 '$ANDROID_TMP/tsnet_acceptance_android'"

adb -s "$ADB_SERIAL" shell -n "\
  cd '$ANDROID_TMP' && \
  setsid env LD_LIBRARY_PATH='$ANDROID_TMP' \
    ./tsnet_acceptance_android \
    --role responder \
    --label android \
    --controlUrl '$CONTROL_URL' \
    --authKey '$AUTH_KEY' \
    --hostname nim-android-acceptance \
    --stateDir '$ANDROID_TMP/android-tsnet-state' \
    --dataDir '$ANDROID_TMP/android-data' \
    --bridgeLib '$ANDROID_TMP/libtsnetbridge.so' \
    --localInfo '$ANDROID_TMP/android-local.json' \
    --remoteInfo '$ANDROID_TMP/mac-remote.json' \
    --result '$ANDROID_TMP/android-result.json' \
    --wireguardPort 41642 \
    >'$ANDROID_TMP/android.log' 2>&1 </dev/null & \
  echo \$! > '$ANDROID_TMP/android.pid' && \
  exit 0"

echo "waiting for local peer info files..."
for _ in $(seq 1 180); do
  if adb -s "$ADB_SERIAL" shell "[ -f '$ANDROID_TMP/android-local.json' ]" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! adb -s "$ADB_SERIAL" shell "[ -f '$ANDROID_TMP/android-local.json' ]" >/dev/null 2>&1; then
  echo "android local info did not appear" >&2
  exit 1
fi

adb -s "$ADB_SERIAL" pull "$ANDROID_TMP/android-local.json" "$ANDROID_LOCAL_HOST" >/dev/null
cp "$ANDROID_LOCAL_HOST" "$MAC_REMOTE"

"$HOST_BIN" \
  --role initiator \
  --label mac \
  --controlUrl "$CONTROL_URL" \
  --authKey "$AUTH_KEY" \
  --hostname nim-mac-acceptance \
  --stateDir "$WORK_DIR/mac-tsnet-state" \
  --dataDir "$WORK_DIR/mac-data" \
  --bridgeLib "$HOST_BRIDGE" \
  --localInfo "$MAC_LOCAL" \
  --remoteInfo "$MAC_REMOTE" \
  --result "$MAC_RESULT" \
  --wireguardPort 41641 \
  >"$MAC_LOG" 2>&1 &
MAC_PID=$!

for _ in $(seq 1 180); do
  if [[ -f "$MAC_LOCAL" ]]; then
    break
  fi
  sleep 1
done
if [[ ! -f "$MAC_LOCAL" ]]; then
  echo "mac local info did not appear" >&2
  exit 1
fi

adb -s "$ADB_SERIAL" push "$MAC_LOCAL" "$ANDROID_TMP/mac-remote.json" >/dev/null

echo "waiting for harness completion..."
wait "$MAC_PID"
adb -s "$ADB_SERIAL" pull "$ANDROID_TMP/android-result.json" "$ANDROID_RESULT_HOST" >/dev/null
adb -s "$ADB_SERIAL" pull "$ANDROID_TMP/android.log" "$ANDROID_LOG_HOST" >/dev/null || true

python3 - "$MAC_RESULT" "$ANDROID_RESULT_HOST" "$EXPECTED_DERP" "$SUMMARY_JSON" <<'PY'
import json, sys
mac_path, android_path, expected_derp, summary_path = sys.argv[1:]
mac = json.load(open(mac_path, "r", encoding="utf-8"))
android = json.load(open(android_path, "r", encoding="utf-8"))

def derp_ok(payload):
    derp = payload.get("derpMap") or {}
    regions = derp.get("regions") or []
    summary = []
    for region in regions:
        code = str(region.get("regionCode", "")).lower()
        rid = region.get("regionId")
        if rid is not None and code:
            summary.append(f"{rid}/{code}")
    return summary == [expected_derp.lower()], summary

mac_derp_ok, mac_derp = derp_ok(mac)
android_derp_ok, android_derp = derp_ok(android)

summary = {
    "ok": bool(mac.get("ok")) and bool(android.get("ok")) and mac_derp_ok and android_derp_ok and mac.get("libp2pPath") == "direct",
    "macOk": bool(mac.get("ok")),
    "androidOk": bool(android.get("ok")),
    "macLibp2pPath": mac.get("libp2pPath"),
    "androidLibp2pPath": android.get("libp2pPath"),
    "macTailnetPath": mac.get("tailnetPathObserved"),
    "androidTailnetPath": android.get("tailnetPathObserved"),
    "macConnectExactOk": bool((mac.get("connectExact") or {}).get("ok")),
    "macDmSent": bool(mac.get("dmSent")),
    "macReplySeen": bool(mac.get("replySeen")),
    "androidReplySent": bool(android.get("responderReplySent")),
    "expectedDerp": expected_derp.lower(),
    "macDerp": mac_derp,
    "androidDerp": android_derp,
    "macResultPath": mac_path,
    "androidResultPath": android_path,
}
json.dump(summary, open(summary_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print(json.dumps(summary, indent=2, ensure_ascii=False))
PY

echo
echo "mac log: $MAC_LOG"
echo "android log: $ANDROID_LOG_HOST"
echo "summary: $SUMMARY_JSON"
