#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: tests/run_lsmr_tests.sh [fast|service|slow|all|<test-file.nim>] [--compile-only]

Groups:
  fast  Compile and run the stable short LSMR test set.
  service Compile and run medium-cost LSMR service integration tests.
  slow  Compile and run the split slow LSMR service network tests.
  all   Run fast, service, then slow.
  <test-file.nim> Compile and run one explicit LSMR test file under tests/.

Options:
  --compile-only   Only compile tests, do not execute them.

Environment:
  FAST_TIMEOUT_SEC      Per-test timeout for fast tests (default: 20)
  SERVICE_TIMEOUT_SEC   Per-test timeout for service tests (default: 40)
  SLOW_TIMEOUT_SEC      Per-test timeout for slow tests (default: 40)
EOF
}

group="fast"
compile_only=0
custom_test=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    fast|service|slow|all)
      group="$1"
      shift
      ;;
    *.nim)
      custom_test="$1"
      shift
      ;;
    --compile-only)
      compile_only=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[lsmr-tests] unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tests_dir="$repo_root/tests"
build_dir="$repo_root/build/lsmr-tests"
bin_dir="$build_dir/bin"
mkdir -p "$bin_dir"

fast_timeout="${FAST_TIMEOUT_SEC:-20}"
slow_timeout="${SLOW_TIMEOUT_SEC:-40}"
service_timeout="${SERVICE_TIMEOUT_SEC:-40}"

fast_tests=(
  "testlsmr.nim"
  "testlsmr_bagua_prefix_tree.nim"
  "testlsmr_dayan_flow.nim"
)

service_tests=(
  "testlsmrservice_dayan.nim"
)

slow_tests=(
  "testlsmrservice_network_slow_sync_imports.nim"
  "testlsmrservice_network_slow_near_field.nim"
  "testlsmrservice_network_slow_witness_refresh.nim"
  "testlsmrservice_network_slow_sync_foreign_rooted.nim"
  "testlsmrservice_network_slow_publish_foreign_rooted.nim"
)

compile_test() {
  local src="$1"
  local name="${src%.nim}"
  echo "[lsmr-tests] compile $src"
  nim c --out:"$bin_dir/$name" "$tests_dir/$src"
}

run_test() {
  local src="$1"
  local timeout_sec="$2"
  local name="${src%.nim}"
  echo "[lsmr-tests] run $src"
  timeout "${timeout_sec}s" "$bin_dir/$name"
}

run_group() {
  local timeout_sec="$1"
  shift
  local tests=("$@")
  local src

  for src in "${tests[@]}"; do
    compile_test "$src"
  done

  if [[ "$compile_only" -eq 1 ]]; then
    return
  fi

  for src in "${tests[@]}"; do
    run_test "$src" "$timeout_sec"
  done
}

run_single() {
  local src="$1"
  local timeout_sec="$2"
  compile_test "$src"
  if [[ "$compile_only" -eq 1 ]]; then
    return
  fi
  run_test "$src" "$timeout_sec"
}

if [[ -n "$custom_test" ]]; then
  if [[ ! -f "$tests_dir/$custom_test" ]]; then
    echo "[lsmr-tests] test file not found: $custom_test" >&2
    exit 1
  fi
  run_single "$custom_test" "$service_timeout"
  exit 0
fi

case "$group" in
  fast)
    run_group "$fast_timeout" "${fast_tests[@]}"
    ;;
  service)
    run_group "$service_timeout" "${service_tests[@]}"
    ;;
  slow)
    run_group "$slow_timeout" "${slow_tests[@]}"
    ;;
  all)
    run_group "$fast_timeout" "${fast_tests[@]}"
    run_group "$service_timeout" "${service_tests[@]}"
    run_group "$slow_timeout" "${slow_tests[@]}"
    ;;
esac
