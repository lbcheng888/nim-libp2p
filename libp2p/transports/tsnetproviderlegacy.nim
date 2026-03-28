{.push raises: [].}

import std/[json, os, strutils]

import ../multiaddress
import ../utility
import ./tsnetbridge as tsbridge
import ./tsnetprovidertypes

type
  TsnetLegacyBridgeHandle* = tsbridge.TsnetBridge

proc legacyBridgeCapabilities*(): TsnetProviderCapabilities =
  TsnetProviderCapabilities(
    inApp: false,
    realTailnet: true,
    proxyBacked: true,
    tcpProxy: true,
    udpProxy: true,
    remoteResolution: true,
    statusApi: true,
    pingApi: true,
    derpMapApi: true
  )

proc legacyBridgeRequested*(cfg: TsnetProviderConfig): bool =
  cfg.bridgeLibraryPath.strip().len > 0 or
    getEnv(tsbridge.DefaultTsnetBridgeEnvVar).strip().len > 0

proc bridgeActive*(bridge: TsnetLegacyBridgeHandle): bool =
  not bridge.isNil and bridge.handle > 0

proc configJson*(cfg: TsnetProviderConfig): string =
  var payload =
    block:
      let raw = cfg.bridgeExtraJson.strip()
      if raw.len == 0:
        newJObject()
      else:
        try:
          let parsed = parseJson(raw)
          if parsed.kind == JObject:
            parsed
          else:
            newJObject()
        except CatchableError:
          newJObject()
  payload["controlUrl"] = %cfg.controlUrl
  payload["authKey"] = %cfg.authKey
  payload["hostname"] = %cfg.hostname
  payload["stateDir"] = %cfg.stateDir
  payload["wireguardPort"] = %cfg.wireguardPort
  payload["logLevel"] = %cfg.logLevel
  payload["enableDebug"] = %cfg.enableDebug
  $payload

proc openLegacyBridge*(cfg: TsnetProviderConfig): Result[TsnetLegacyBridgeHandle, string] =
  tsbridge.openTsnetBridge(cfg.configJson(), cfg.bridgeLibraryPath)

proc closeLegacyBridge*(bridge: TsnetLegacyBridgeHandle) =
  bridge.close()

proc resetLegacyBridge*(bridge: TsnetLegacyBridgeHandle): Result[void, string] =
  tsbridge.reset(bridge)

proc statusPayload*(bridge: TsnetLegacyBridgeHandle): Result[JsonNode, string] =
  tsbridge.statusPayload(bridge)

proc pingPayload*(
    bridge: TsnetLegacyBridgeHandle,
    request: JsonNode
): Result[JsonNode, string] =
  tsbridge.pingPayload(bridge, request)

proc derpMapPayload*(bridge: TsnetLegacyBridgeHandle): Result[JsonNode, string] =
  tsbridge.derpMapPayload(bridge)

proc listenTcpProxy*(
    bridge: TsnetLegacyBridgeHandle,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  tsbridge.listenTcpProxy(bridge, family, port, localAddress)

proc listenUdpProxy*(
    bridge: TsnetLegacyBridgeHandle,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  tsbridge.listenUdpProxy(bridge, family, port, localAddress)

proc dialTcpProxy*(
    bridge: TsnetLegacyBridgeHandle,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  tsbridge.dialTcpProxy(bridge, family, ip, port)

proc dialUdpProxy*(
    bridge: TsnetLegacyBridgeHandle,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  tsbridge.dialUdpProxy(bridge, family, ip, port)

proc resolveRemote*(
    bridge: TsnetLegacyBridgeHandle,
    rawAddress: MultiAddress
): Result[MultiAddress, string] =
  tsbridge.resolveRemote(bridge, rawAddress)
