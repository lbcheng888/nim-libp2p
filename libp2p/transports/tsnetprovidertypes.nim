{.push raises: [].}

import std/[json, strutils]

import ../multiaddress

type
  TsnetProxyDialMode* {.pure.} = enum
    Local
    DirectRoute
    RelayBridge

  TsnetDirectRouteTarget* = object
    rawAddress*: MultiAddress
    pathKind*: string

  TsnetProxyDialTarget* = object
    rawAddress*: MultiAddress
    mode*: TsnetProxyDialMode

  TsnetProviderKind* {.pure.} = enum
    BuiltinSynthetic
    InAppUnavailable
    InAppReal
    LegacyBridge

  TsnetProviderCapabilities* = object
    inApp*: bool
    realTailnet*: bool
    proxyBacked*: bool
    tcpProxy*: bool
    udpProxy*: bool
    remoteResolution*: bool
    statusApi*: bool
    pingApi*: bool
    derpMapApi*: bool

  TsnetProviderConfig* = object
    controlUrl*: string
    controlProtocol*: string
    controlEndpoint*: string
    relayEndpoint*: string
    authKey*: string
    hostname*: string
    stateDir*: string
    wireguardPort*: int
    bridgeLibraryPath*: string
    logLevel*: string
    enableDebug*: bool
    bridgeExtraJson*: string

const
  TsnetControlProtocolAuto* = "auto"
  TsnetControlProtocolTs2021H2* = "ts2021_h2"
  TsnetControlProtocolNimH3* = "nim_h3"
  TsnetControlProtocolNimQuic* = "nim_quic"
  TsnetControlProtocolNimTcp* = "nim_tcp"
  TsnetPathRelay* = "relay"
  TsnetPathPunchedDirect* = "punched_direct"
  TsnetPathDirect* = "direct"
  TsnetPathIdle* = "idle"
  TsnetLegacyBridgeWarning* =
    "Go tsnet bridge is deprecated; a real Nim in-app tsnet provider is not implemented yet"
  TsnetMissingInAppProviderError* =
    "self-hosted tsnet was requested, but the Nim in-app tsnet provider is not implemented yet; set bridgeLibraryPath or NIM_TSNET_BRIDGE_LIB only to opt into the deprecated legacy Go bridge"

proc init*(_: type[TsnetProviderConfig]): TsnetProviderConfig =
  TsnetProviderConfig(
    controlUrl: "",
    controlProtocol: TsnetControlProtocolAuto,
    controlEndpoint: "",
    relayEndpoint: "",
    authKey: "",
    hostname: "",
    stateDir: "",
    wireguardPort: 0,
    bridgeLibraryPath: "",
    logLevel: "",
    enableDebug: false,
    bridgeExtraJson: ""
  )

proc normalizeControlProtocol*(value: string): string =
  case value.strip().toLowerAscii()
  of "", TsnetControlProtocolAuto:
    TsnetControlProtocolAuto
  of "ts2021_h2", "h2", "http2":
    TsnetControlProtocolTs2021H2
  of "nim_h3", "h3", "http3":
    TsnetControlProtocolNimH3
  of "nim_quic", "quic", "control_quic":
    TsnetControlProtocolNimQuic
  of "nim_tcp", "tcp", "control_tcp":
    TsnetControlProtocolNimTcp
  else:
    TsnetControlProtocolAuto

proc normalizeTailnetPath*(value: string): string =
  case value.strip().toLowerAscii()
  of "", TsnetPathIdle:
    TsnetPathIdle
  of "derp", "relay":
    TsnetPathRelay
  of "punched_direct", "hole_punched_direct", "hole-punched-direct", "punch_direct":
    TsnetPathPunchedDirect
  of "direct":
    TsnetPathDirect
  else:
    value.strip().toLowerAscii()

proc tailnetPathUsesRelay*(value: string): bool =
  normalizeTailnetPath(value) == TsnetPathRelay

proc kindLabel*(kind: TsnetProviderKind): string =
  case kind
  of TsnetProviderKind.BuiltinSynthetic:
    "builtin-synthetic"
  of TsnetProviderKind.InAppUnavailable:
    "inapp-unavailable"
  of TsnetProviderKind.InAppReal:
    "nim-inapp"
  of TsnetProviderKind.LegacyBridge:
    "legacy-go-bridge"

proc toJson*(caps: TsnetProviderCapabilities): JsonNode =
  %*{
    "inApp": caps.inApp,
    "realTailnet": caps.realTailnet,
    "proxyBacked": caps.proxyBacked,
    "tcpProxy": caps.tcpProxy,
    "udpProxy": caps.udpProxy,
    "remoteResolution": caps.remoteResolution,
    "statusApi": caps.statusApi,
    "pingApi": caps.pingApi,
    "derpMapApi": caps.derpMapApi,
  }

proc jsonField(node: JsonNode, key: string): JsonNode =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc jsonString(node: JsonNode, key: string): string =
  let value = jsonField(node, key)
  if value.kind == JString:
    return value.getStr()
  ""

proc jsonBool(node: JsonNode, key: string): bool =
  let value = jsonField(node, key)
  case value.kind
  of JBool:
    value.getBool()
  of JString:
    value.getStr().strip().toLowerAscii() in ["1", "true", "yes", "y"]
  else:
    false

proc jsonInt(node: JsonNode, key: string): int =
  let value = jsonField(node, key)
  case value.kind
  of JInt:
    value.getBiggestInt().int
  of JFloat:
    value.getFloat().int
  else:
    0

proc toJson*(cfg: TsnetProviderConfig): JsonNode =
  %*{
    "controlUrl": cfg.controlUrl,
    "controlProtocol": cfg.controlProtocol,
    "controlEndpoint": cfg.controlEndpoint,
    "relayEndpoint": cfg.relayEndpoint,
    "authKey": cfg.authKey,
    "hostname": cfg.hostname,
    "stateDir": cfg.stateDir,
    "wireguardPort": cfg.wireguardPort,
    "bridgeLibraryPath": cfg.bridgeLibraryPath,
    "logLevel": cfg.logLevel,
    "enableDebug": cfg.enableDebug,
    "bridgeExtraJson": cfg.bridgeExtraJson,
  }

proc parseProviderConfig*(node: JsonNode): Result[TsnetProviderConfig, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet provider config must be a JSON object")
  ok(TsnetProviderConfig(
    controlUrl: jsonString(node, "controlUrl"),
    controlProtocol: jsonString(node, "controlProtocol"),
    controlEndpoint: jsonString(node, "controlEndpoint"),
    relayEndpoint: jsonString(node, "relayEndpoint"),
    authKey: jsonString(node, "authKey"),
    hostname: jsonString(node, "hostname"),
    stateDir: jsonString(node, "stateDir"),
    wireguardPort: jsonInt(node, "wireguardPort"),
    bridgeLibraryPath: jsonString(node, "bridgeLibraryPath"),
    logLevel: jsonString(node, "logLevel"),
    enableDebug: jsonBool(node, "enableDebug"),
    bridgeExtraJson: jsonString(node, "bridgeExtraJson"),
  ))
