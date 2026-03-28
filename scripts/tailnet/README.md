# Tailnet Scripts

This directory contains the operational pieces needed for the `tsnet` transport:

- `build_tsnetbridge_host.sh`
  Builds the deprecated legacy Go `libtsnetbridge` for the local host and writes
  it to `build/`. This is now an oracle/compatibility path, not the default
  runtime packaging path.
- `build_tsnetbridge_android.sh`
  Cross-compiles the deprecated legacy Go `libtsnetbridge.so` for Android
  `arm64-v8a` and writes it to
  `/Users/lbcheng/UniMaker/android/core-bridge/src/main/jniLibs/arm64-v8a/`.
- `setup_headscale_derp.sh`
  Provisions a Headscale control plane with embedded DERP on a remote host.
- `run_mac_android_acceptance_nofile.py`
  Runs the Mac↔Android `tsnet` acceptance flow without writing handoff/result
  JSON files. Local/result payloads stay on stdout and remote handoff is pushed
  over an `adb forward` TCP control socket.

Useful runtime knobs:

- `NIM_TSNET_BRIDGE_LIB`
  Explicit path to `libtsnetbridge.{dylib,so}` for macOS/Linux legacy testing.
- `NIM_ANDROID_ENABLE_LEGACY_TSNET_BRIDGE=1`
  Opt-in packaging flag for the deprecated Android bridge. The default build no
  longer ships `libtsnetbridge.so`.

If `controlUrl`/`authKey` are configured without an explicit legacy bridge path,
`tsnet` now fails fast instead of auto-discovering `libtsnetbridge`. That keeps
the default runtime on the pure-Nim path until a real in-app provider exists.
- Gradle properties for Android:
  - `-Pp2p_underlay=tsnet`
  - `-Ptsnet_control_url=https://...`
  - `-Ptsnet_auth_key=tskey-auth-...`
  - `-Ptsnet_hostname=android-probe`
  - `-Ptsnet_state_dir=/data/user/0/<pkg>/files/tailnet-state`
  - `-Ptsnet_wireguard_port=41641`
  - `-Ptsnet_bridge_library_path=`
  - `-Ptsnet_log_level=debug`
  - `-Ptsnet_enable_debug=true`

The mobile FFI exports these JSON helpers for scripts and UI inspection:

- `libp2p_tailnet_status_json`
- `libp2p_tailnet_ping`
- `libp2p_tailnet_derp_map`
