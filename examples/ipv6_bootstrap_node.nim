{.used.}

import std/[os, strutils]
import chronos
import libp2p
import libp2p/protocols/ping
import libp2p/protocols/rendezvous

const
  DefaultIpv6TcpListen = "/ip6/::/tcp/4001"
  DefaultIpv6QuicListen = "/ip6/::/udp/4001/quic-v1"

proc parseListenAddrs(raw: string): seq[MultiAddress] =
  for item in raw.split(','):
    let candidate = item.strip()
    if candidate.len == 0:
      continue

    let parsed = MultiAddress.init(candidate)
    if parsed.isErr:
      raise newException(ValueError, "invalid listen multiaddr: " & candidate)

    result.add(parsed.get())

  if result.len == 0:
    raise newException(ValueError, "no IPv6 listen addresses configured")

proc main() {.async: (raises: [CatchableError, Exception]).} =
  let
    rng = newRng()
    rdv = RendezVous.new()
    ping = Ping.new(rng = rng)
  var listenAddrs = parseListenAddrs(
    getEnv("BOOTSTRAP_IPV6_TCP_LISTEN", DefaultIpv6TcpListen)
  )
  listenAddrs.add(
    parseListenAddrs(getEnv("BOOTSTRAP_IPV6_QUIC_LISTEN", DefaultIpv6QuicListen))
  )

  var builder = SwitchBuilder
    .new()
    .withRng(rng)
    .withAddresses(listenAddrs)
    .withTcpTransport()
    .withMplex()
    .withNoise()
    .withRendezVous(rdv)
  when defined(libp2p_msquic_experimental):
    builder = builder.withMsQuicTransport()
  else:
    raise newException(
      CatchableError, "QUIC support requires -d:libp2p_msquic_experimental"
    )

  let sw = builder.build()

  sw.mount(ping)

  await sw.start()

  echo "[bootstrap] peerId: ", $sw.peerInfo.peerId
  echo "[bootstrap] listening on:"
  for addr in sw.peerInfo.addrs:
    echo "  ", $addr, "/p2p/", $sw.peerInfo.peerId

  while true:
    await sleepAsync(1.hours)

when isMainModule:
  try:
    waitFor main()
  except CatchableError as exc:
    quit("[bootstrap] failed: " & exc.msg, QuitFailure)
