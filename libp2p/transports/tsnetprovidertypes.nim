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
