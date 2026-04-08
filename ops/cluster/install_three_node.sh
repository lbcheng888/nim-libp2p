#!/bin/zsh
set -euo pipefail

ROOT="/Users/lbcheng/nim-libp2p"
if [[ -d /root/nim-libp2p ]]; then
  ROOT="/root/nim-libp2p"
fi

TARGET="${1:-}"
if [[ -z "${TARGET}" ]]; then
  echo "usage: $0 {64|dmit|mac}" >&2
  exit 1
fi

case "${TARGET}" in
  64)
    install -m 0644 "${ROOT}/ops/systemd/nim-tsnet-product-64.service" /etc/systemd/system/nim-tsnet-product.service
    systemctl daemon-reload
    systemctl enable --now nim-tsnet-product.service
    ;;
  dmit)
    install -m 0644 "${ROOT}/ops/systemd/nim-tsnet-product-dmit.service" /etc/systemd/system/nim-tsnet-product.service
    systemctl daemon-reload
    systemctl enable --now nim-tsnet-product.service
    ;;
  mac)
    mkdir -p "${HOME}/Library/LaunchAgents"
    install -m 0644 "${ROOT}/ops/launchd/com.lbcheng.nim-tsnet-product.mac.plist" "${HOME}/Library/LaunchAgents/com.lbcheng.nim-tsnet-product.mac.plist"
    launchctl bootout "gui/$(id -u)" "${HOME}/Library/LaunchAgents/com.lbcheng.nim-tsnet-product.mac.plist" >/dev/null 2>&1 || true
    launchctl bootstrap "gui/$(id -u)" "${HOME}/Library/LaunchAgents/com.lbcheng.nim-tsnet-product.mac.plist"
    launchctl kickstart -k "gui/$(id -u)/com.lbcheng.nim-tsnet-product.mac"
    ;;
  *)
    echo "unknown target: ${TARGET}" >&2
    exit 1
    ;;
esac
