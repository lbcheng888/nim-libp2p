#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BOOTSTRAP="${SCRIPT_DIR}/bootstrap_msquic.sh"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MODE="${NIM_MSQUIC_VALIDATE_MODE:-builtin}"

run_builtin_validation() {
  echo "validate_msquic: validating builtin pure-Nim runtime"
  (
    cd "${REPO_ROOT}"
    local builtin_flags=(
      -d:libp2p_msquic_experimental
      -d:libp2p_msquic_builtin
    )
    local pr_gate=(
      tests/test_qpack_huffman.nim
      tests/test_msquic_builder.nim
      tests/test_quic_runtime_info.nim
      tests/test_quic_runtime_events.nim
      tests/test_quic_runtime_handlers.nim
      tests/test_msquic_driver.nim
      tests/test_msquic_listener.nim
      tests/test_msquic_stream.nim
      tests/test_msquic_dial.nim
      tests/testquicconfig.nim
      tests/testmobile_ffi_ui_frame_snapshot.nim
      tests/test_libnimlibp2p_quic_runtime.nim
    )
    local protocol_gate=(
      tests/test_msquic_webtransport.nim
      tests/testdirectdm.nim
      tests/testfeed_service.nim
      tests/testtsnetquiccontrol.nim
      tests/testtsnetquicrelay.nim
      tests/testtsnetruntime.nim
      tests/testtsnettransport_quic.nim
    )
    local test_file
    for test_file in "${pr_gate[@]}"; do
      echo "validate_msquic: builtin PR gate ${test_file}"
      nim c -r --hints:off --warnings:off \
        "${builtin_flags[@]}" \
        "${test_file}"
    done
    for test_file in "${protocol_gate[@]}"; do
      echo "validate_msquic: builtin protocol gate ${test_file}"
      nim c -r --hints:off --warnings:off \
        "${builtin_flags[@]}" \
        "${test_file}"
    done
  )
  echo "validate_msquic: builtin runtime ready"
}

ensure_native_runtime() {
  if [[ ! -x "${BOOTSTRAP}" ]]; then
    echo "validate_msquic: bootstrap script not found at ${BOOTSTRAP}" >&2
    exit 1
  fi

  if [[ "${NIM_MSQUIC_BOOTSTRAP_SKIP:-0}" != "1" ]]; then
    if export_line=$("${BOOTSTRAP}" env); then
      if [[ -n "${export_line}" ]]; then
        eval "${export_line}"
      fi
    else
      echo "validate_msquic: bootstrap failed" >&2
      exit 1
    fi
  fi

  if [[ -z "${NIM_MSQUIC_LIB:-}" ]]; then
    echo "validate_msquic: NIM_MSQUIC_LIB not configured for native validation" >&2
    exit 1
  fi

  if [[ ! -f "${NIM_MSQUIC_LIB}" ]]; then
    echo "validate_msquic: MsQuic library not found at ${NIM_MSQUIC_LIB}" >&2
    exit 1
  fi
}

run_native_validation() {
  ensure_native_runtime
  echo "validate_msquic: validating native runtime at ${NIM_MSQUIC_LIB}"
  (
    cd "${REPO_ROOT}"
    NIM_MSQUIC_EXPECT_NATIVE=1 \
      nim c -r --hints:off --warnings:off \
      -d:libp2p_msquic_experimental \
      tests/test_quic_runtime_info.nim
  )
  echo "validate_msquic: native runtime ready: ${NIM_MSQUIC_LIB}"
}

case "${MODE}" in
  builtin)
    run_builtin_validation
    ;;
  native)
    run_native_validation
    ;;
  *)
    echo "validate_msquic: unsupported NIM_MSQUIC_VALIDATE_MODE=${MODE}; expected builtin|native" >&2
    exit 1
    ;;
esac
