{.used.}

import std/[os, strutils]
import chronos
import libp2p
import libp2p/protocols/ping
import libp2p/crypto/crypto
import bearssl/[rand, hash]
import nimcrypto/sha2 as nimsha2

const
  DefaultTcpListen =
    "/ip4/0.0.0.0/tcp/4001,/ip6/::/tcp/4001"
  DefaultQuicListen =
    "/ip4/0.0.0.0/udp/4001/quic-v1,/ip6/::/udp/4001/quic-v1"

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
    raise newException(ValueError, "no listen addresses configured")

proc deterministicPrivateKeyFromSeed(seedText: string): PrivateKey =
  let normalized = seedText.strip()
  if normalized.len == 0:
    raise newException(ValueError, "bootstrap identity seed is empty")

  var seedBytes = newSeq[byte](normalized.len)
  for idx, ch in normalized:
    seedBytes[idx] = byte(ord(ch) and 0xff)

  let digest = nimsha2.sha256.digest(seedBytes)
  var entropy = newSeq[byte](digest.data.len)
  for idx in 0 ..< entropy.len:
    entropy[idx] = digest.data[idx]

  var rngRef = new(HmacDrbgContext)
  hmacDrbgInit(
    rngRef[],
    addr sha256Vtable,
    (if entropy.len > 0: cast[pointer](addr entropy[0]) else: nil),
    uint(entropy.len)
  )

  let pairRes = KeyPair.random(PKScheme.Ed25519, rngRef[])
  if pairRes.isErr():
    raise newException(CatchableError, "failed to derive bootstrap keypair: " & $pairRes.error)
  pairRes.get().seckey

proc main() {.async: (raises: [CatchableError, Exception]).} =
  let
    rng = newRng()
    ping = Ping.new(rng = rng)
    identitySeed = getEnv("BOOTSTRAP_IDENTITY_SEED").strip()
    networkId = getEnv("BOOTSTRAP_NETWORK_ID", "unimaker-mobile").strip()
    journalPath = getEnv("BOOTSTRAP_JOURNAL_PATH").strip()
  var listenAddrs = parseListenAddrs(
    getEnv("BOOTSTRAP_TCP_LISTEN", DefaultTcpListen)
  )
  listenAddrs.add(
    parseListenAddrs(getEnv("BOOTSTRAP_QUIC_LISTEN", DefaultQuicListen))
  )

  var builder = SwitchBuilder
    .new()
    .withRng(rng)
    .withAddresses(listenAddrs)
    .withWanBootstrapProfile(
      bootstrapRoleConfig(
        role = WanBootstrapRole.anchor,
        networkId = if networkId.len > 0: networkId else: "unimaker-mobile",
        journalPath = journalPath,
      )
    )
    .withTcpTransport()
    .withYamux()
    .withMplex()
    .withTls()
    .withNoise()

  if identitySeed.len > 0:
    builder = builder.withPrivateKey(deterministicPrivateKeyFromSeed(identitySeed))

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
  if identitySeed.len > 0:
    echo "[bootstrap] identity source: env BOOTSTRAP_IDENTITY_SEED"
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
