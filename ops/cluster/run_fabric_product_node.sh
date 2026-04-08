#!/usr/bin/env bash
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

CHAIN_ID="lsmr-cluster"
GENESIS_PATH="${ROOT}/ops/cluster/fabric_cluster_genesis.json"
RWADD_BIN="${ROOT}/build/rwadd"
ROUTING_MODE="lsmr"
PRIMARY_PLANE="lsmr"
LSMR_NETWORK_ID="${CHAIN_ID}"
LSMR_SERVE_DEPTH="2"
LSMR_MIN_WITNESS_QUORUM="1"
ANCHOR_64_PEER_ID="12D3KooWNE1E71rdMeQfqMoMDLpkVcJzPs4GJHsVuPj9ZGgFH333"
ANCHOR_64_ADDR="/ip4/64.176.84.12/tcp/40111"

case "${TARGET}" in
  64)
    RPC_HOST="0.0.0.0"
    RPC_PORT="19111"
    DATA_DIR="${ROOT}/build/prod-64/fabric"
    IDENTITY_PATH="${DATA_DIR}/identity.json"
    LISTEN_ADDRS="/ip4/0.0.0.0/tcp/40111"
    LSMR_BOOTSTRAP_ADDRS="/ip4/154.26.191.226/tcp/40112/p2p/12D3KooWDhA6FodFnjQFQRrt5L6MUEQtfvnC2nmFZ18bbZUmmW73"
    LSMR_PATH="5"
    LSMR_ANCHORS_JSON='[{"peerId":"12D3KooWNE1E71rdMeQfqMoMDLpkVcJzPs4GJHsVuPj9ZGgFH333","addrs":["/ip4/64.176.84.12/tcp/40111"],"operatorId":"root-64","regionDigit":5,"attestedPrefix":[5],"serveDepth":2,"directionMask":511,"canIssueRootCert":true}]'
    ;;
  dmit)
    RPC_HOST="0.0.0.0"
    RPC_PORT="19112"
    DATA_DIR="${ROOT}/build/prod-dmit/fabric"
    IDENTITY_PATH="${DATA_DIR}/identity.json"
    LISTEN_ADDRS="/ip4/0.0.0.0/tcp/40112"
    LSMR_BOOTSTRAP_ADDRS="/ip4/64.176.84.12/tcp/40111/p2p/${ANCHOR_64_PEER_ID}"
    LSMR_PATH="5,3"
    LSMR_ANCHORS_JSON='[{"peerId":"12D3KooWNE1E71rdMeQfqMoMDLpkVcJzPs4GJHsVuPj9ZGgFH333","addrs":["/ip4/64.176.84.12/tcp/40111"],"operatorId":"root-64","regionDigit":5,"attestedPrefix":[5],"serveDepth":2,"directionMask":511,"canIssueRootCert":true}]'
    ;;
  mac)
    RPC_HOST="127.0.0.1"
    RPC_PORT="19113"
    DATA_DIR="${ROOT}/build/prod-mac/fabric"
    IDENTITY_PATH="${DATA_DIR}/identity.json"
    LISTEN_ADDRS="/ip4/0.0.0.0/tcp/40113"
    LSMR_BOOTSTRAP_ADDRS="/ip4/64.176.84.12/tcp/40111/p2p/${ANCHOR_64_PEER_ID},/ip4/154.26.191.226/tcp/40112/p2p/12D3KooWDhA6FodFnjQFQRrt5L6MUEQtfvnC2nmFZ18bbZUmmW73"
    LSMR_PATH="5,9"
    LSMR_ANCHORS_JSON='[{"peerId":"12D3KooWNE1E71rdMeQfqMoMDLpkVcJzPs4GJHsVuPj9ZGgFH333","addrs":["/ip4/64.176.84.12/tcp/40111"],"operatorId":"root-64","regionDigit":5,"attestedPrefix":[5],"serveDepth":2,"directionMask":511,"canIssueRootCert":true}]'
    ;;
  *)
    echo "unknown target: ${TARGET}" >&2
    exit 1
    ;;
esac

mkdir -p "${DATA_DIR}"

if [[ ! -x "${RWADD_BIN}" ]]; then
  echo "missing rwadd binary: ${RWADD_BIN}" >&2
  exit 1
fi

if [[ ! -f "${GENESIS_PATH}" ]]; then
  echo "missing genesis: ${GENESIS_PATH}" >&2
  exit 1
fi

"${RWADD_BIN}" init \
  --data-dir "${DATA_DIR}" \
  --identity "${IDENTITY_PATH}" \
  --genesis "${GENESIS_PATH}" \
  --chain-id "${CHAIN_ID}" >/dev/null

exec "${RWADD_BIN}" run \
  --data-dir "${DATA_DIR}" \
  --identity "${IDENTITY_PATH}" \
  --genesis "${GENESIS_PATH}" \
  --rpc-host "${RPC_HOST}" \
  --rpc-port "${RPC_PORT}" \
  --listen-addrs "${LISTEN_ADDRS}" \
  --routing-mode "${ROUTING_MODE}" \
  --primary-plane "${PRIMARY_PLANE}" \
  --lsmr-network-id "${LSMR_NETWORK_ID}" \
  --lsmr-serve-depth "${LSMR_SERVE_DEPTH}" \
  --lsmr-min-witness-quorum "${LSMR_MIN_WITNESS_QUORUM}" \
  --lsmr-bootstrap-addrs "${LSMR_BOOTSTRAP_ADDRS}" \
  --lsmr-anchors-json "${LSMR_ANCHORS_JSON}" \
  --lsmr-path "${LSMR_PATH}"
