import std/[json, os, sequtils, strutils]

import chronos

import ../libp2p/transports/tsnet/quicrelay
import ../libp2p/transports/quicruntime as qrt

proc main() {.async: (raises: [CancelledError]).} =
  var relayUrl = ""
  var route = "/ip4/100.64.0.10/udp/4001/quic-v1/tsnet"
  var listenerCandidates = @[
    "/ip4/198.51.100.10/udp/4001/quic-v1",
    "/ip6/2001:db8::10/udp/4001/quic-v1"
  ]
  var dialerCandidates = @[
    "/ip4/203.0.113.20/udp/4555/quic-v1"
  ]

  for idx in 1 .. paramCount():
    let arg = paramStr(idx)
    if arg.startsWith("--route="):
      route = arg.split("=", 1)[1].strip()
    elif arg.startsWith("--listener-candidates="):
      let raw = arg.split("=", 1)[1].strip()
      listenerCandidates =
        if raw.len == 0: @[]
        else: raw.split(',').mapIt(it.strip()).filterIt(it.len > 0)
    elif arg.startsWith("--dialer-candidates="):
      let raw = arg.split("=", 1)[1].strip()
      dialerCandidates =
        if raw.len == 0: @[]
        else: raw.split(',').mapIt(it.strip()).filterIt(it.len > 0)
    elif relayUrl.len == 0:
      relayUrl = arg.strip()

  if relayUrl.len == 0:
    echo "{\"ok\":false,\"error\":\"usage: tsnet_quic_accept_probe <relay-url> [--route=...]\"}"
    return

  let result =
    await probeAcceptStreamReuse(
      relayUrl,
      route,
      listenerCandidates,
      dialerCandidates,
      qrt.qrpBuiltinOnly
    )

  echo(
    $ %*{
      "ok": result.ok,
      "relayUrl": result.relayUrl,
      "route": result.route,
      "listenerCandidates": result.listenerCandidates,
      "dialerCandidates": result.dialerCandidates,
      "error": result.error
    }
  )

waitFor main()
