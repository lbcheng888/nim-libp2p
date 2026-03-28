{.push raises: [].}

import std/[json, strutils]

import ./tsnetprovidertypes
import ./tsnet/runtime as tsruntime
import ../multiaddress
import ../utility

type
  TsnetInAppRuntime* = tsruntime.TsnetInAppRuntime

proc realRuntimeRequested*(cfg: TsnetProviderConfig): bool {.gcsafe.} =
  cfg.controlUrl.strip().len > 0 or
    cfg.controlEndpoint.strip().len > 0 or
    cfg.relayEndpoint.strip().len > 0 or
    cfg.authKey.strip().len > 0 or
    cfg.hostname.strip().len > 0 or
    cfg.stateDir.strip().len > 0

proc inAppRuntimeCapabilities*(): TsnetProviderCapabilities {.gcsafe.} =
  TsnetProviderCapabilities(
    inApp: true,
    realTailnet: true,
    proxyBacked: false,
    tcpProxy: false,
    udpProxy: false,
    remoteResolution: false,
    statusApi: true,
    pingApi: true,
    derpMapApi: true
  )

proc runtimeActive*(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  not runtime.isNil and runtime.ready()

proc runtimePresent*(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  not runtime.isNil

proc runtimeCapabilities*(runtime: TsnetInAppRuntime): TsnetProviderCapabilities {.gcsafe.} =
  if runtime.isNil:
    return TsnetProviderCapabilities(
      inApp: true,
      realTailnet: false,
      proxyBacked: false,
      tcpProxy: false,
      udpProxy: false,
      remoteResolution: false,
      statusApi: false,
      pingApi: false,
      derpMapApi: false
    )
  tsruntime.capabilities(runtime)

proc openInAppRuntime*(cfg: TsnetProviderConfig): Result[TsnetInAppRuntime, string] =
  let runtime = tsruntime.TsnetInAppRuntime.new(cfg)
  let started = tsruntime.start(runtime)
  if started.isErr():
    return err(started.error)
  ok(runtime)

proc newInAppRuntime*(cfg: TsnetProviderConfig): TsnetInAppRuntime =
  tsruntime.TsnetInAppRuntime.new(cfg)

proc startInAppRuntime*(runtime: TsnetInAppRuntime): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  tsruntime.start(runtime)

proc closeInAppRuntime*(runtime: var TsnetInAppRuntime) =
  if runtime.isNil:
    return
  tsruntime.stop(runtime)
  runtime = nil

proc resetInAppRuntime*(runtime: TsnetInAppRuntime): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  tsruntime.reset(runtime)

proc statusPayload*(runtime: TsnetInAppRuntime): Result[JsonNode, string] =
  tsruntime.statusPayload(runtime)

proc pingPayload*(
    runtime: TsnetInAppRuntime,
    request: JsonNode
): Result[JsonNode, string] =
  tsruntime.pingPayload(runtime, request)

proc derpMapPayload*(runtime: TsnetInAppRuntime): Result[JsonNode, string] =
  tsruntime.derpMapPayload(runtime)

proc listenTcpProxy*(
    runtime: TsnetInAppRuntime,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  tsruntime.listenTcpProxy(runtime, family, port, localAddress)

proc listenUdpProxy*(
    runtime: TsnetInAppRuntime,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  tsruntime.listenUdpProxy(runtime, family, port, localAddress)

proc dialTcpProxy*(
    runtime: TsnetInAppRuntime,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  tsruntime.dialTcpProxy(runtime, family, ip, port)

proc dialUdpProxy*(
    runtime: TsnetInAppRuntime,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  tsruntime.dialUdpProxy(runtime, family, ip, port)

proc dialUdpProxyRelayFallback*(
    runtime: TsnetInAppRuntime,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  tsruntime.dialUdpProxyRelayFallback(runtime, family, ip, port)

proc markFailedDirectProxyRoute*(
    runtime: TsnetInAppRuntime,
    advertised: MultiAddress,
    rawAddress: MultiAddress
): Result[void, string] =
  tsruntime.markFailedDirectProxyRoute(runtime, advertised, rawAddress)

proc udpDialState*(
    runtime: TsnetInAppRuntime,
    rawAddress: MultiAddress
): tuple[
    known, ready: bool,
    error, phase, detail: string,
    attempts: int,
    updatedUnixMilli: int64
  ] =
  tsruntime.udpDialState(runtime, rawAddress)

proc resolveRemote*(
    runtime: TsnetInAppRuntime,
    rawAddress: MultiAddress
): Result[MultiAddress, string] =
  tsruntime.resolveRemote(runtime, rawAddress)
