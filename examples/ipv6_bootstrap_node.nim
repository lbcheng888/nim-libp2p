{.used.}

import std/[os, strutils]
import chronos
import libp2p
import libp2p/protocols/ping
import libp2p/protocols/rendezvous

const
  DefaultIpv6TcpListen = "/ip6/::/tcp/4001"
  DefaultIpv6QuicListen = "/ip6/::/udp/4001/quic-v1"
  DefaultDialTimeoutMs = 15000

proc parseMultiAddrs(raw: string; kind: string): seq[MultiAddress] =
  for item in raw.split(','):
    let candidate = item.strip()
    if candidate.len == 0:
      continue

    let parsed = MultiAddress.init(candidate)
    if parsed.isErr:
      raise newException(ValueError, "invalid " & kind & " multiaddr: " & candidate)

    result.add(parsed.get())

  if result.len == 0:
    raise newException(ValueError, "no " & kind & " multiaddrs configured")

proc parseListenAddrs(raw: string): seq[MultiAddress] =
  parseMultiAddrs(raw, "listen")

proc parseDialAddrs(raw: string): seq[MultiAddress] =
  parseMultiAddrs(raw, "dial")

proc parseDialTimeoutMs(raw: string): int =
  let value = raw.strip()
  if value.len == 0:
    return DefaultDialTimeoutMs

  result = parseInt(value)
  if result <= 0:
    raise newException(ValueError, "BOOTSTRAP_DIAL_TIMEOUT_MS must be > 0")

proc dialTargets(
    sw: Switch, targets: seq[MultiAddress], timeoutMs: int
) {.async: (raises: [CatchableError, Exception]).} =
  echo "[dial] local peerId: ", $sw.peerInfo.peerId
  echo "[dial] timeoutMs: ", timeoutMs
  when defined(libp2p_msquic_builtin):
    echo "[dial] quic runtime: builtin"
  elif defined(libp2p_msquic_experimental):
    echo "[dial] quic runtime: native"

  var successCount = 0
  for target in targets:
    echo "[dial] attempting: ", $target
    let connectFut = sw.connect(target, allowUnknownPeerId = true)
    try:
      let peerId = await connectFut.wait(timeoutMs.milliseconds)
      inc successCount
      echo "[dial] success: ", $target, " peerId=", $peerId
      try:
        await sw.disconnect(peerId)
      except CatchableError:
        discard
    except AsyncTimeoutError:
      if not connectFut.finished():
        connectFut.cancel()
      echo "[dial] timeout: ", $target, " afterMs=", timeoutMs
    except CatchableError as exc:
      echo "[dial] failure: ", $target, " error=", exc.msg

  echo "[dial] summary: ", successCount, "/", targets.len, " succeeded"

proc main() {.async: (raises: [CatchableError, Exception]).} =
  let
    rng = newRng()
    rdv = RendezVous.new()
    ping = Ping.new(rng = rng)
    dialTargetsRaw = getEnv("BOOTSTRAP_DIAL_TARGETS")
    dialTimeoutMs = parseDialTimeoutMs(getEnv("BOOTSTRAP_DIAL_TIMEOUT_MS"))
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

  if dialTargetsRaw.len > 0:
    await dialTargets(sw, parseDialAddrs(dialTargetsRaw), dialTimeoutMs)
    await sw.stop()
    return

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
