# HarmonyOS mDNS + DM + Feed + Livestream Demo

The `entry` module delivers a Stage UI + NAPI bridge for the `libnimlibp2p`
FFI. It mirrors the Android MVP: LAN discovery, direct messaging, feed
publishing and a lightweight livestream publisher.

## Prerequisites

1. Install the DevEco Studio toolchain (OpenHarmony NDK, armeabi-v8a).
2. Build the Nim runtime and copy OpenSSL artefacts:

   ```bash
   # From repo root
   ./examples/build_nim_ohos.sh
   ```

   The script places the native payload under:

   ```
   examples/hos/entry/libs/arm64-v8a/libnimlibp2p.so
   examples/hos/entry/libs/arm64-v8a/libssl.so
   examples/hos/entry/libs/arm64-v8a/libcrypto.so
   ```

3. The module CMake target (`entry/src/main/cpp/CMakeLists.txt`) builds
   `libnimbridge.so`, linking it with the prebuilt Nim/OpenSSL objects and
   exposing N-API bindings.

## Running

1. Open `examples/hos` in DevEco Studio.
2. Install dependencies (`node`, HarmonyOS SDK) as prompted.
3. Select an arm64-v8a device/emulator and build/deploy the `entry` target.

## Architecture Notes

- `entry/src/main/cpp/nim_bridge.cpp` wraps the Nim FFI exports in N-API
  callables and logs `nim_bridge_emit_event` via `hilog`.
- `entry/src/main/ets/common/NimLibp2p.ts` loads the native module
  (`libnimbridge.so`) and presents a typed façade to ArkTS.
- `entry/src/main/ets/pages/Index.ets` manages polling, state reduction and
  renders tabs for Overview, Chat, Feed and Livestream interactions.

The ArkTS UI reuses the same JSON event contract used on Android. Ensure all
native libraries are kept in sync by rebuilding `libnimlibp2p.so` whenever
the Nim sources change.
