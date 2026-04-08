#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/lbcheng/nim-libp2p"
if [[ -d /root/nim-libp2p ]]; then
  ROOT="/root/nim-libp2p"
fi

install_if_different() {
  local mode="$1"
  local src="$2"
  local dst="$3"
  if [[ "$(cd "$(dirname "${src}")" && pwd)/$(basename "${src}")" == "$(cd "$(dirname "${dst}")" && pwd)/$(basename "${dst}")" ]]; then
    return 0
  fi
  install -m "${mode}" "${src}" "${dst}"
}

TARGET="${1:-}"
if [[ -z "${TARGET}" ]]; then
  echo "usage: $0 {64|dmit|mac}" >&2
  exit 1
fi

case "${TARGET}" in
  64)
    install_if_different 0755 "${ROOT}/ops/cluster/run_fabric_product_node.sh" /root/nim-libp2p/ops/cluster/run_fabric_product_node.sh
    install_if_different 0644 "${ROOT}/ops/cluster/fabric_cluster_genesis.json" /root/nim-libp2p/ops/cluster/fabric_cluster_genesis.json
    install_if_different 0644 "${ROOT}/ops/systemd/nim-fabric-product-64.service" /etc/systemd/system/nim-fabric-product.service
    systemctl disable --now nim-tsnet-product.service >/dev/null 2>&1 || true
    systemctl daemon-reload
    systemctl enable --now nim-fabric-product.service
    ;;
  dmit)
    install_if_different 0755 "${ROOT}/ops/cluster/run_fabric_product_node.sh" /root/nim-libp2p/ops/cluster/run_fabric_product_node.sh
    install_if_different 0644 "${ROOT}/ops/cluster/fabric_cluster_genesis.json" /root/nim-libp2p/ops/cluster/fabric_cluster_genesis.json
    install_if_different 0644 "${ROOT}/ops/systemd/nim-fabric-product-dmit.service" /etc/systemd/system/nim-fabric-product.service
    systemctl disable --now nim-tsnet-product.service >/dev/null 2>&1 || true
    systemctl daemon-reload
    systemctl enable --now nim-fabric-product.service
    ;;
  mac)
    mkdir -p "${HOME}/Library/LaunchAgents"
    install_if_different 0644 "${ROOT}/ops/launchd/com.lbcheng.nim-fabric-product.mac.plist" "${HOME}/Library/LaunchAgents/com.lbcheng.nim-fabric-product.mac.plist"
    launchctl bootout "gui/$(id -u)" "${HOME}/Library/LaunchAgents/com.lbcheng.nim-fabric-product.mac.plist" >/dev/null 2>&1 || true
    launchctl bootstrap "gui/$(id -u)" "${HOME}/Library/LaunchAgents/com.lbcheng.nim-fabric-product.mac.plist"
    launchctl kickstart -k "gui/$(id -u)/com.lbcheng.nim-fabric-product.mac"
    ;;
  *)
    echo "unknown target: ${TARGET}" >&2
    exit 1
    ;;
esac
