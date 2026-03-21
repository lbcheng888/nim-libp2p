{.used.}

import std/[os, strutils]
import chronos
import libp2p
import libp2p/protocols/ping

const
  DefaultClientListen = "/ip4/0.0.0.0/udp/0/quic-v1,/ip6/::/udp/0/quic-v1"

proc parseListenAddrs(raw: string): seq[MultiAddress] =
  for item in raw.split(','):
    let candidate = item.strip()
    if candidate.len == 0:
      continue
    let parsed = MultiAddress.init(candidate)
    if parsed.isErr:
      raise newException(ValueError, "invalid listen multiaddr: " & candidate)
    result.add(parsed.get())

proc main() {.async: (raises: [CatchableError, Exception]).} =
  let
    rawTargetAddr = getEnv("TARGET_ADDR").strip()
    rawTargetPeer = getEnv("TARGET_PEER_ID").strip()
  if rawTargetAddr.len == 0 or rawTargetPeer.len == 0:
    raise newException(
      ValueError, "TARGET_ADDR and TARGET_PEER_ID are required"
    )

  let
    rng = newRng()
    pingProto = Ping.new(rng = rng)
    targetAddr = MultiAddress.init(rawTargetAddr).tryGet()
    targetPeer = PeerId.init(rawTargetPeer).tryGet()
    listenAddrs = parseListenAddrs(
      getEnv("CLIENT_QUIC_LISTEN", DefaultClientListen)
    )

  let sw = SwitchBuilder
    .new()
    .withRng(rng)
    .withAddresses(listenAddrs)
    .withMsQuicTransport()
    .withNoise()
    .withMplex()
    .build()

  await sw.start()
  try:
    let conn = await sw.dial(targetPeer, @[targetAddr], PingCodec)
    let rtt = await pingProto.ping(conn)
    echo "[quic-ping] peerId: ", $targetPeer
    echo "[quic-ping] addr: ", rawTargetAddr
    echo "[quic-ping] rtt: ", rtt
    await conn.close()
  finally:
    await sw.stop()

when isMainModule:
  try:
    waitFor main()
  except CatchableError as exc:
    quit("[quic-ping] failed: " & exc.msg, QuitFailure)
