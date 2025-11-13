## MsQuic 设置与性能调节蓝图，简化 `QUIC_SETTINGS` 的 Nim 表示。

import std/options
import ./param_catalog

type
  QuicSettingsOverlay* {.bycopy.} = object
    idleTimeoutMs*: uint64
    handshakeIdleTimeoutMs*: uint64
    keepAliveIntervalMs*: uint32
    maxBytesPerKey*: uint64
    sendBufferingEnabled*: bool
    pacingEnabled*: bool
    migrationEnabled*: bool
    datagramReceiveEnabled*: bool
    peerBidiStreamCount*: uint16
    peerUnidiStreamCount*: uint16

  PerformanceProfile* = enum
    profileLowLatency
    profileMaxThroughput
    profileScavenger

  SettingRequest* = object
    param*: uint32
    buffer*: seq[uint8]

proc defaultQuicSettingsOverlay*(): QuicSettingsOverlay =
  QuicSettingsOverlay(
    idleTimeoutMs: 30_000,
    handshakeIdleTimeoutMs: 10_000,
    keepAliveIntervalMs: 0,
    maxBytesPerKey: 0,
    sendBufferingEnabled: true,
    pacingEnabled: true,
    migrationEnabled: true,
    datagramReceiveEnabled: false,
    peerBidiStreamCount: 0,
    peerUnidiStreamCount: 0)

proc overlayForProfile*(profile: PerformanceProfile): QuicSettingsOverlay =
  result = defaultQuicSettingsOverlay()
  case profile
  of profileLowLatency:
    result.keepAliveIntervalMs = 5_000
    result.sendBufferingEnabled = false
    result.pacingEnabled = false
  of profileMaxThroughput:
    result.keepAliveIntervalMs = 15_000
    result.sendBufferingEnabled = true
    result.pacingEnabled = true
    result.datagramReceiveEnabled = true
  of profileScavenger:
    result.keepAliveIntervalMs = 30_000
    result.sendBufferingEnabled = true
    result.pacingEnabled = true
    result.migrationEnabled = false

proc buildSettingRequests*(overlay: QuicSettingsOverlay): seq[SettingRequest] =
  var buffer = newSeq[uint8](sizeof(QuicSettingsOverlay))
  copyMem(addr buffer[0], addr overlay, sizeof(QuicSettingsOverlay))
  result.add SettingRequest(param: QUIC_PARAM_CONN_SETTINGS, buffer: buffer)
