#!/usr/bin/env bash
set -euo pipefail

# Thin wrapper that delegates to the repository-level builder. Gradle invokes
# this script from the Android example root (examples/android), so we resolve
# the repository root relative to this file and forward all arguments.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

exec "${REPO_ROOT}/examples/build_nim_android.sh" "$@"
