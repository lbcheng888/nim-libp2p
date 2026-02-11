# Android Production Delivery Baseline (libp2p DEX demo)

This repository contains an **example** Android app (`examples/android`) that integrates `nim-libp2p` via JNI/FFI and exposes DEX-related flows (orders/matching/trades) for smoke-testing.

“Production-grade delivery” in this context means:

- **Safe-by-default release build**: no demo keys baked into release, no unsafe discovery defaults, signing/verification enabled by default.
- **Runtime-configurable connectivity**: bootstrap/relay peers and market-data polling can be changed **without rebuilding**.
- **Operational controls**: reset node data, inspect connection status, and collect logs deterministically.
- **Reproducible builds**: one-command debug build for device verification; optional release signing.

## Security defaults

The current baseline enforces:

- Release builds do **not** bundle demo private keys/mnemonics.
- Release builds keep **mDNS disabled** by default (device-specific stability/memory risks).
- DEX init defaults to **signing enabled** and **unsigned messages disallowed**.

## Runtime configuration (recommended)

Open the **Settings** tab:

- Set `Bootstrap peers` / `Relay peers` (supports JSON array or newline/comma separated multiaddrs).
- Toggle `Market data` (if off, UI falls back to DEX-derived price when available).
- Tap `Apply & Restart Node` to apply peer changes.
- Tap `Reset Node Data` if you need to wipe the embedded node state directory.

## Build (debug)

From repo root:

```bash
cd examples/android
./gradlew :app:assembleDebug
```

Install (device must allow ADB installs):

```bash
adb install -r -g app/build/outputs/apk/debug/app-debug.apk
```

Notes for Huawei/EMUI devices: installs may require **on-device confirmation** (risk prompts + lockscreen password).

## Build (release)

Release builds are expected to be **signed** for installation/distribution.

Build an unsigned release APK:

```bash
cd examples/android
./gradlew :app:assembleRelease
```

Then sign it using your Android keystore (example with `apksigner`):

```bash
APKSIGNER="$ANDROID_HOME/build-tools/<ver>/apksigner"
$APKSIGNER sign \
  --ks /path/to/keystore.jks \
  --ks-key-alias <alias> \
  --out app-release-signed.apk \
  app/build/outputs/apk/release/app-release-unsigned.apk
```

## Logs

Suggested filters:

```bash
adb logcat -v time NimBridge:D P2PUseCase:D NimNodeViewModel:D '*:S'
```
