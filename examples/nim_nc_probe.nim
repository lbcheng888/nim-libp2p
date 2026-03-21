{.used.}

import std/[os, parseopt, sequtils, strutils]
from std/times import epochTime
import chronos
import libp2p
import libp2p/wire

const
  DefaultTimeoutMs = 5000
  DefaultListenTcp = "/ip6/::/tcp/0"
  DefaultListenQuic = "/ip6/::/udp/0/quic-v1"

type
  ProbeMode = enum
    pmRawTcp
    pmLibp2p

proc usage() =
  echo "nim_nc_probe: native IPv6 reachability probe for TCP and libp2p TCP/QUIC"
  echo ""
  echo "Usage:"
  echo "  nim c -r -d:libp2p_msquic_experimental -d:libp2p_msquic_builtin examples/nim_nc_probe.nim -- \\"
  echo "    --timeout-ms=15000 \\"
  echo "    /ip6/2001:db8::1/tcp/4001/p2p/12D3KooW... \\"
  echo "    /ip6/2001:db8::1/udp/4001/quic-v1/p2p/12D3KooW..."
  echo ""
  echo "Behavior:"
  echo "  - /tcp/ targets run a raw TCP connect probe and, if possible, a libp2p dial probe."
  echo "  - /quic-v1 targets run a libp2p QUIC dial probe."

proc parseTimeoutMs(raw: string): int =
  result = parseInt(raw)
  if result <= 0:
    raise newException(ValueError, "timeout must be > 0")

proc parseTargets(rawTargets: seq[string]): seq[MultiAddress] =
  for raw in rawTargets:
    let candidate = raw.strip()
    if candidate.len == 0:
      continue
    let parsed = MultiAddress.init(candidate)
    if parsed.isErr:
      raise newException(ValueError, "invalid multiaddr: " & candidate)
    result.add(parsed.get())
  if result.len == 0:
    raise newException(ValueError, "no probe targets provided")

proc classifyTarget(target: MultiAddress): set[ProbeMode] =
  let text = $target
  if "/tcp/" in text:
    result.incl(pmRawTcp)
    result.incl(pmLibp2p)
  elif "/quic-v1" in text:
    result.incl(pmLibp2p)

proc rawTcpAddress(target: MultiAddress): MultiAddress =
  if "/p2p/" in $target:
    return target[0 .. 1].valueOr:
      raise newException(ValueError, "failed to derive raw tcp address from " & $target)
  target

proc elapsedMs(startedAt: float): int =
  int((epochTime() - startedAt) * 1000.0)

proc closeStream(stream: StreamTransport) {.async: (raises: []).} =
  if stream.isNil:
    return
  try:
    await stream.closeWait()
  except CatchableError:
    discard

proc rawTcpProbe(target: MultiAddress; timeoutMs: int) {.async: (raises: []).} =
  let rawTarget =
    try:
      rawTcpAddress(target)
    except CatchableError as exc:
      echo "[raw-tcp] failure: ", $target, " afterMs=0 error=", exc.msg
      return
  let startedAt = epochTime()
  let dialFut = wire.connect(rawTarget)
  try:
    let stream = await dialFut.wait(timeoutMs.milliseconds)
    defer:
      await closeStream(stream)
    echo "[raw-tcp] success: ", $rawTarget, " afterMs=", elapsedMs(startedAt)
  except AsyncTimeoutError:
    if not dialFut.finished():
      dialFut.cancel()
    echo "[raw-tcp] timeout: ", $rawTarget, " afterMs=", timeoutMs
  except CatchableError as exc:
    echo "[raw-tcp] failure: ", $rawTarget, " afterMs=", elapsedMs(startedAt),
      " error=", exc.msg

proc buildDialSwitch(targets: seq[MultiAddress]): Switch =
  let rng = newRng()
  var listenAddrs = @[MultiAddress.init(DefaultListenTcp).get()]
  if targets.anyIt("/quic-v1" in $it):
    when defined(libp2p_msquic_experimental):
      listenAddrs.add(MultiAddress.init(DefaultListenQuic).get())
    else:
      discard

  var builder = SwitchBuilder
    .new()
    .withRng(rng)
    .withAddresses(listenAddrs)
    .withTcpTransport()
    .withMplex()
    .withNoise()

  if targets.anyIt("/quic-v1" in $it):
    when defined(libp2p_msquic_experimental):
      builder = builder.withMsQuicTransport()

  builder.build()

proc libp2pProbe(sw: Switch; target: MultiAddress; timeoutMs: int) {.async: (raises: []).} =
  let startedAt = epochTime()
  let dialFut = sw.connect(target, allowUnknownPeerId = true)
  try:
    let peerId = await dialFut.wait(timeoutMs.milliseconds)
    echo "[libp2p] success: ", $target, " afterMs=", elapsedMs(startedAt),
      " peerId=", $peerId
    try:
      await sw.disconnect(peerId)
    except CatchableError:
      discard
  except AsyncTimeoutError:
    if not dialFut.finished():
      dialFut.cancel()
    echo "[libp2p] timeout: ", $target, " afterMs=", timeoutMs
  except CatchableError as exc:
    echo "[libp2p] failure: ", $target, " afterMs=", elapsedMs(startedAt),
      " error=", exc.msg

proc main() {.async: (raises: [CatchableError, Exception]).} =
  var
    parser = initOptParser(commandLineParams())
    timeoutMs = DefaultTimeoutMs
    rawTargets: seq[string] = @[]

  while true:
    parser.next()
    case parser.kind
    of cmdEnd:
      break
    of cmdLongOption, cmdShortOption:
      case parser.key
      of "timeout-ms", "t":
        timeoutMs = parseTimeoutMs(parser.val)
      of "help", "h":
        usage()
        return
      else:
        raise newException(ValueError, "unknown option: " & parser.key)
    of cmdArgument:
      rawTargets.add(parser.key)

  let targets = parseTargets(rawTargets)
  echo "[probe] timeoutMs: ", timeoutMs
  echo "[probe] targetCount: ", targets.len

  let needLibp2p = targets.anyIt(pmLibp2p in classifyTarget(it))
  var sw: Switch = nil

  if needLibp2p:
    sw = buildDialSwitch(targets)
    await sw.start()
    echo "[probe] localPeerId: ", $sw.peerInfo.peerId

  try:
    for target in targets:
      let modes = classifyTarget(target)
      if pmRawTcp in modes:
        await rawTcpProbe(target, timeoutMs)
      if pmLibp2p in modes:
        if "/quic-v1" in $target and not defined(libp2p_msquic_experimental):
          echo "[libp2p] skipped: ", $target,
            " reason=quic requires -d:libp2p_msquic_experimental"
        else:
          await libp2pProbe(sw, target, timeoutMs)
  finally:
    if not sw.isNil:
      await sw.stop()

when isMainModule:
  try:
    waitFor main()
  except CatchableError as exc:
    quit("[probe] failed: " & exc.msg, QuitFailure)
