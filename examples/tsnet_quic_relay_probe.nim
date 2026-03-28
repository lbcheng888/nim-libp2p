{.push raises: [].}

when not defined(libp2p_msquic_experimental):
  {.error: "tsnet_quic_relay_probe requires -d:libp2p_msquic_experimental".}

import std/[json, os, strutils]
import chronos

import ../libp2p/transports/tsnet/quicrelay
import ../libp2p/transports/quicruntime as qrt

proc parsePreference(raw: string): qrt.QuicRuntimePreference =
  case raw.strip().toLowerAscii()
  of "", "builtin", "builtin_only", "nim", "nim_quic":
    qrt.qrpBuiltinOnly
  of "auto":
    qrt.qrpAuto
  of "native", "native_only":
    qrt.qrpNativeOnly
  of "builtin_preferred":
    qrt.qrpBuiltinPreferred
  else:
    qrt.qrpBuiltinOnly

proc main() =
  let relayUrl =
    if paramCount() >= 1: paramStr(1).strip()
    else: getEnv("NIM_TSNET_RELAY_URL", "").strip()
  if relayUrl.len == 0:
    quit("usage: tsnet_quic_relay_probe <quic://host:port/nim-tsnet-relay-quic/v1>", QuitFailure)
  let pref = parsePreference(getEnv("NIM_TSNET_RELAY_QUIC_RUNTIME", "builtin_only"))
  let probe =
    try:
      waitFor probeRelayPing(relayUrl, pref)
    except CatchableError as exc:
      echo $(%*{
        "ok": false,
        "relayUrl": relayUrl,
        "error": exc.msg
      })
      quit(QuitFailure)
  echo $(%*{
    "ok": probe.ok,
    "relayUrl": probe.relayUrl,
    "runtimePreference": probe.runtimePreference,
    "runtimeImplementation": probe.runtimeImplementation,
    "runtimePath": probe.runtimePath,
    "runtimeKind": probe.runtimeKind,
    "connected": probe.connected,
    "acknowledged": probe.acknowledged,
    "error": probe.error
  })
  if not probe.ok:
    quit(QuitFailure)

when isMainModule:
  main()
