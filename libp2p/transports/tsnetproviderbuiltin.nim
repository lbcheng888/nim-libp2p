{.push raises: [].}

import std/json

import ./tsnetprovidertypes

proc builtinSyntheticCapabilities*(): TsnetProviderCapabilities =
  TsnetProviderCapabilities(
    inApp: true,
    realTailnet: false,
    proxyBacked: false,
    tcpProxy: false,
    udpProxy: false,
    remoteResolution: false,
    statusApi: false,
    pingApi: false,
    derpMapApi: true
  )

proc unavailableCapabilities*(): TsnetProviderCapabilities =
  TsnetProviderCapabilities(
    inApp: false,
    realTailnet: false,
    proxyBacked: false,
    tcpProxy: false,
    udpProxy: false,
    remoteResolution: false,
    statusApi: false,
    pingApi: false,
    derpMapApi: false
  )

proc builtinSyntheticDerpMapPayload*(): JsonNode =
  %*{
    "ok": true,
    "omitDefaultRegions": false,
    "regionCount": 0,
    "regions": newJArray(),
  }
