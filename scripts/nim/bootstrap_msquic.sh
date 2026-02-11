#!/usr/bin/env bash
set -euo pipefail

resolve_path() {
  local target="$1"
  if command -v realpath >/dev/null 2>&1; then
    realpath "${target}"
  elif command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY' "${target}"
import os, sys
print(os.path.realpath(sys.argv[1]))
PY
  elif command -v python >/dev/null 2>&1; then
    python - <<'PY' "${target}"
import os, sys
print(os.path.realpath(sys.argv[1]))
PY
  else
    echo "${target}"
  fi
}

MODE=${1:-env}

if [[ "${MODE}" != "env" ]]; then
  cat <<'EOF' >&2
Usage: bootstrap_msquic.sh env

Detect (or download) an MsQuic shared library and output an export command
for the NIM_MSQUIC_LIB environment variable. When MsQuic is already configured
via NIM_MSQUIC_LIB this script is a no-op. Set NIM_MSQUIC_BOOTSTRAP_SKIP=1 to skip.

Optional environment variables:
  NIM_MSQUIC_BOOTSTRAP_SKIP   Skip detection/downloading when set to 1.
  MSQUIC_LIB_SEARCH_PATHS     Colon separated search paths to scan first.
  MSQUIC_BOOTSTRAP_URL        Custom download URL for an MsQuic archive.
  MSQUIC_BOOTSTRAP_CACHE      Cache directory (default: $HOME/.cache/msquic).
EOF
  exit 1
fi

if [[ "${NIM_MSQUIC_BOOTSTRAP_SKIP:-}" == "1" ]]; then
  exit 0
fi

declare -a SEARCH_DIRS=()
IFS=':' read -ra CUSTOM_SEARCH <<<"${MSQUIC_LIB_SEARCH_PATHS:-}"
for dir in "${CUSTOM_SEARCH[@]}"; do
  [[ -n "${dir}" ]] && SEARCH_DIRS+=("${dir}")
done

uname_s=$(uname -s 2>/dev/null || echo "Unknown")
case "${uname_s}" in
  Darwin)
    LIB_NAMES=("libmsquic.dylib")
    SEARCH_DIRS+=("/usr/local/lib" "/opt/homebrew/lib" "/usr/lib")
    ;;
  Linux)
    LIB_NAMES=("libmsquic.so")
    SEARCH_DIRS+=("/usr/lib" "/usr/lib64" "/usr/local/lib" "/usr/lib/x86_64-linux-gnu")
    ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT)
    LIB_NAMES=("msquic.dll" "msquic.lib")
    if [[ -n "${ProgramFiles:-}" ]]; then
      SEARCH_DIRS+=("${ProgramFiles}/MsQuic/bin")
    fi
    ;;
  *)
    LIB_NAMES=("libmsquic.so" "libmsquic.dylib" "msquic.dll")
    ;;
esac

find_library() {
  for candidate in "${LIB_NAMES[@]}"; do
    if [[ -f "${candidate}" ]]; then
      printf '%s\n' "$(resolve_path "${candidate}")"
      return 0
    fi
  done
  for dir in "${SEARCH_DIRS[@]}"; do
    for candidate in "${LIB_NAMES[@]}"; do
      local path="${dir}/${candidate}"
      if [[ -f "${path}" ]]; then
        printf '%s\n' "$(resolve_path "${path}")"
        return 0
      fi
    done
  done
  return 1
}

emit_export() {
  local path="$1"
  if [[ -z "${path}" ]]; then
    return 1
  fi
  printf 'export NIM_MSQUIC_LIB="%s"\n' "$(resolve_path "${path}")"
}

if [[ -n "${NIM_MSQUIC_LIB:-}" ]]; then
  emit_export "${NIM_MSQUIC_LIB}"
  exit 0
fi

if lib_path=$(find_library); then
  emit_export "${lib_path}"
  exit 0
fi

download_archive() {
  local url="$1"
  local cache_dir="${MSQUIC_BOOTSTRAP_CACHE:-$HOME/.cache/msquic}"
  mkdir -p "${cache_dir}"
  local filename="${cache_dir}/$(basename "${url}")"
  if [[ ! -f "${filename}" ]]; then
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL -o "${filename}" "${url}"
    elif command -v wget >/dev/null 2>&1; then
      wget -O "${filename}" "${url}"
    else
      echo "bootstrap_msquic: curl/wget not available to download ${url}" >&2
      return 1
    fi
  fi

  local extract_dir="${cache_dir}/extract"
  rm -rf "${extract_dir}"
  mkdir -p "${extract_dir}"
  case "${filename}" in
    *.tar.gz|*.tgz)
      tar -C "${extract_dir}" -xzf "${filename}"
      ;;
    *.zip)
      if ! command -v unzip >/dev/null 2>&1; then
        echo "bootstrap_msquic: unzip not available to extract ${filename}" >&2
        return 1
      fi
      unzip -q "${filename}" -d "${extract_dir}"
      ;;
    *)
      echo "bootstrap_msquic: unsupported archive ${filename}" >&2
      return 1
      ;;
  esac

  local found
  found=$(cd "${extract_dir}" && find . -type f \( -name 'libmsquic.so' -o -name 'libmsquic.dylib' -o -name 'msquic.dll' \) -print -quit)
  if [[ -n "${found}" ]]; then
    resolve_path "${extract_dir}/${found#./}"
  fi
}

if [[ -n "${MSQUIC_BOOTSTRAP_URL:-}" ]]; then
  if lib_path=$(download_archive "${MSQUIC_BOOTSTRAP_URL}"); then
    emit_export "${lib_path}"
    exit 0
  fi
fi

cat <<'EOF' >&2
bootstrap_msquic: unable to locate MsQuic library automatically.

Please install the official MsQuic release for your platform and set
NIM_MSQUIC_LIB to the absolute path of the shared library, or set
MSQUIC_BOOTSTRAP_URL to a download URL that contains the library.
EOF
exit 1
