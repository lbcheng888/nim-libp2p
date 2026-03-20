#!/usr/bin/env bash
set -euo pipefail

MODE=${1:-release}
MIN_IOS_VERSION=${MIN_IOS_VERSION:-15.0}
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NIM_UNIMAKER_DIR="$REPO_ROOT/examples/mobile_ffi"
HEADER_DIR="$REPO_ROOT/examples/ios/include"
BUILD_ROOT="${BUILD_ROOT:-$REPO_ROOT/build/ios}"
DIST_DIR="${DIST_DIR:-$BUILD_ROOT/dist}"
TOOLCHAIN_DIR="$BUILD_ROOT/toolchains"
NIMCACHE_DIR="$BUILD_ROOT/nimcache"
XCFRAMEWORK_PATH="$DIST_DIR/nimlibp2p.xcframework"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "[nim-ios] iOS packaging requires macOS + Xcode" >&2
  exit 1
fi

mkdir -p "$BUILD_ROOT" "$DIST_DIR" "$TOOLCHAIN_DIR" "$NIMCACHE_DIR"

CLANG_BIN="$(xcrun -f clang)"
AR_BIN="$(xcrun -f ar)"
RANLIB_BIN="$(xcrun -f ranlib)"
NM_BIN="$(xcrun -f nm)"
LIPO_BIN="$(xcrun -f lipo)"
XCODEBUILD_BIN="$(xcrun -f xcodebuild)"

COMMON_FLAGS=(
  "c"
  "--app:staticlib"
  "--noMain"
  "--forceBuild:on"
  "--os:ios"
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
  "--path:${REPO_ROOT}/vendor/secp256k1"
  "--passC:-Iexamples/mobile_ffi/compat"
  "--passC:-I${REPO_ROOT}"
  "--passC:-I${REPO_ROOT}/compat"
  "--passC:-include"
  "--passC:${REPO_ROOT}/compat/explicit_bzero.h"
  "--passC:-fvisibility=default"
  "--define:libp2p_autotls_support"
  "--define:libp2p_pure_crypto"
  "--define:nimcrypto_disable_asm"
  "--define:noSignalHandler"
  "--stacktrace:off"
  "--lineTrace:off"
)

case "$MODE" in
  release)
    COMMON_FLAGS+=("--define:release")
    ;;
  debug)
    COMMON_FLAGS+=("--define:debug")
    ;;
  *)
    echo "[nim-ios] unsupported mode: $MODE" >&2
    exit 1
    ;;
esac

required_symbols=(
  nim_thread_attach
  nim_thread_detach
  libp2p_get_last_error
  libp2p_string_free
  libp2p_node_init
  libp2p_node_start
  libp2p_node_stop
  libp2p_node_free
  libp2p_node_is_started
  libp2p_generate_identity_json
  libp2p_identity_from_seed
  libp2p_get_local_peer_id
  libp2p_get_listen_addresses
  libp2p_get_dialable_addresses
  libp2p_get_connected_peers_json
  libp2p_connected_peers_info
  libp2p_poll_events
  libp2p_fetch_file_providers
  libp2p_register_local_file
  libp2p_request_file_chunk
  libp2p_last_chunk_size
  libp2p_get_local_system_profile_json
  libp2p_get_network_resources_json
  libp2p_refresh_node_resources
  libp2p_network_discovery_snapshot
  libp2p_game_mesh_create_room
  libp2p_game_mesh_join_room
  libp2p_game_mesh_handle_incoming
  libp2p_game_mesh_submit_action
  libp2p_game_mesh_current_state
  libp2p_game_mesh_wait_state
  libp2p_game_mesh_apply_script
  libp2p_game_mesh_apply_step
  libp2p_game_mesh_replay_verify
  libp2p_game_mesh_local_smoke
  social_list_discovered_peers
  social_connect_peer
  social_dm_send
  social_dm_edit
  social_dm_revoke
  social_dm_ack
  social_contacts_send_request
  social_contacts_accept
  social_contacts_reject
  social_contacts_remove
  social_groups_create
  social_groups_update
  social_groups_invite
  social_groups_kick
  social_groups_leave
  social_groups_send
  social_synccast_upsert_program
  social_synccast_join
  social_synccast_leave
  social_synccast_control
  social_synccast_get_state
  social_synccast_list_rooms
  social_moments_publish
  social_moments_delete
  social_moments_like
  social_moments_comment
  social_feed_snapshot
  social_content_detail
  social_playback_open
  social_playback_state
  social_playback_control
  social_media_asset_status
  social_publish_enqueue
  social_publish_task
  social_publish_tasks
  social_notifications_list
  social_notifications_summary
  social_notifications_mark
  social_subscriptions_set
  social_query_presence
  social_poll_events
  social_poll_events_without_dm
  social_received_direct_messages
)

verify_symbols() {
  local library="$1"
  local missing=0
  local nm_output
  nm_output="$("${NM_BIN}" -gU "$library" 2>/dev/null || true)"
  for symbol in "${required_symbols[@]}"; do
    if ! grep -Eq "(^|[[:space:]])_${symbol}($|[[:space:]])" <<<"$nm_output"; then
      echo "[nim-ios] missing symbol ${symbol} in ${library}" >&2
      missing=1
    fi
  done
  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi
}

build_slice() {
  local name="$1"
  local cpu="$2"
  local sdk="$3"
  local target="$4"
  local out_dir="$BUILD_ROOT/${name}"
  local sdk_path
  sdk_path="$(xcrun --sdk "$sdk" --show-sdk-path)"
  mkdir -p "$out_dir" "$NIMCACHE_DIR/$name"
  local out_path="$out_dir/libnimlibp2p.a"

  echo "[nim-ios] building ${name} (${target}) -> ${out_path}" >&2
  (
    cd "$REPO_ROOT"
    CC="$CLANG_BIN" \
    CXX="$CLANG_BIN" \
    AR="$AR_BIN" \
    RANLIB="$RANLIB_BIN" \
    SDKROOT="$sdk_path" \
    nim "${COMMON_FLAGS[@]}" \
      "--cpu:${cpu}" \
      "--passC:--target=${target}" \
      "--passC:-isysroot" \
      "--passC:${sdk_path}" \
      "--nimcache:${NIMCACHE_DIR}/${name}" \
      "--out:${out_path}" \
      "$NIM_UNIMAKER_DIR/libnimlibp2p.nim"
  )

  verify_symbols "$out_path"
  printf '%s\n' "$out_path"
}

DEVICE_LIB="$(build_slice ios-arm64 arm64 iphoneos "arm64-apple-ios${MIN_IOS_VERSION}")"
SIM_ARM64_LIB="$(build_slice ios-sim-arm64 arm64 iphonesimulator "arm64-apple-ios${MIN_IOS_VERSION}-simulator")"
SIM_X86_64_LIB="$(build_slice ios-sim-x86_64 amd64 iphonesimulator "x86_64-apple-ios${MIN_IOS_VERSION}-simulator")"

SIM_UNIVERSAL_LIB="$BUILD_ROOT/ios-simulator/libnimlibp2p.a"
mkdir -p "$(dirname "$SIM_UNIVERSAL_LIB")"
"${LIPO_BIN}" -create "$SIM_ARM64_LIB" "$SIM_X86_64_LIB" -output "$SIM_UNIVERSAL_LIB"
verify_symbols "$SIM_UNIVERSAL_LIB"

rm -rf "$XCFRAMEWORK_PATH"
"${XCODEBUILD_BIN}" -create-xcframework \
  -library "$DEVICE_LIB" -headers "$HEADER_DIR" \
  -library "$SIM_UNIVERSAL_LIB" -headers "$HEADER_DIR" \
  -output "$XCFRAMEWORK_PATH"

echo "[nim-ios] packaged xcframework -> $XCFRAMEWORK_PATH"
