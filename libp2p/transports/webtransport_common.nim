# WebTransport handshake/common helpers shared by MsQuic and legacy QUIC transports.

import std/[options, strutils]

const
  http3StreamTypeControl* = 0'u64
  http3StreamTypeQpackEncoder* = 0x02'u64
  http3StreamTypeQpackDecoder* = 0x03'u64
  http3StreamTypeWebtransport* = 0x54'u64
  http3FrameTypeHeaders* = 0x01'u64
  http3FrameTypeSettings* = 0x04'u64
  http3SettingsEnableConnectProtocol* = 0x8'u64
  http3SettingsH3Datagram* = 0x33'u64
  http3SettingsWebtransportMaxSessions* = 0xc671706a'u64

type
  WebtransportMode* = enum
    wtmServer,
    wtmClient

  WebtransportHandshakeInfo* = object
    mode*: WebtransportMode
    authority*: string
    path*: string
    origin*: string
    draft*: string
    maxSessions*: uint32

  Http3Settings* = object
    enableConnectProtocol*: bool
    enableDatagram*: bool
    maxSessions*: uint32

proc normaliseAuthority*(host, port: string): string =
  if host.len == 0 or port.len == 0:
    return ""
  if host.contains(':') and not host.startsWith("["):
    "[" & host & "]:" & port
  else:
    host & ":" & port

proc normaliseOrigin*(authority: string): string =
  if authority.len == 0:
    ""
  else:
    "https://" & authority

proc settingsValidationError*(settings: Http3Settings): Option[string] =
  if not settings.enableConnectProtocol:
    return some("remote endpoint did not enable CONNECT protocol")
  if not settings.enableDatagram:
    return some("remote endpoint did not enable HTTP/3 datagrams")
  if settings.maxSessions == 0:
    return some("remote endpoint does not accept WebTransport sessions")
  none(string)
