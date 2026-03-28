import std/[json, os]
import chronos

import ../libp2p/transports/tsnet/quicrelay
import ../libp2p/transports/quicruntime as qrt

when isMainModule:
  if paramCount() < 1:
    quit(
      "usage: tsnet_quic_relay_accept_probe <quic://host:port/nim-tsnet-relay-quic/v1>",
      QuitFailure
    )

  let relayUrl = paramStr(1)
  let pref = qrt.qrpBuiltinOnly
  let listenerCandidates = @[
    "/ip4/198.51.100.10/udp/4001/quic-v1",
    "/ip4/203.0.113.10/udp/4002/quic-v1"
  ]
  let dialerCandidates = @[
    "/ip4/203.0.113.20/udp/4555/quic-v1"
  ]

  let probe =
    try:
      waitFor probeAcceptStreamReuse(
        relayUrl,
        "/ip4/100.64.0.10/udp/4001/quic-v1/tsnet",
        listenerCandidates,
        dialerCandidates,
        pref
      )
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
    "route": probe.route,
    "listenerCandidates": probe.listenerCandidates,
    "dialerCandidates": probe.dialerCandidates,
    "error": probe.error
  })
