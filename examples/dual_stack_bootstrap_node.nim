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

proc parseEnvInt(name: string, defaultValue: int): int =
  let raw = getEnv(name).strip()
  if raw.len == 0:
    return defaultValue
  try:
    parseInt(raw)
  except ValueError:
    raise newException(ValueError, "invalid integer in " & name & ": " & raw)

proc main() {.async: (raises: [CatchableError, Exception]).} =
  let
    rng = newRng()
    ping = Ping.new(rng = rng)
    identitySeed = getEnv("BOOTSTRAP_IDENTITY_SEED").strip()
    networkId = getEnv("BOOTSTRAP_NETWORK_ID", "unimaker-mobile").strip()
    journalPath = getEnv("BOOTSTRAP_JOURNAL_PATH").strip()
    resolvedNetworkId = if networkId.len > 0: networkId else: "unimaker-mobile"
    lsmrOperatorId =
      block:
        let raw = getEnv("BOOTSTRAP_LSMR_OPERATOR_ID", "unimaker-root").strip()
        if raw.len > 0: raw else: "unimaker-root"
    lsmrRegionDigit = uint8(min(9, max(1, parseEnvInt("BOOTSTRAP_LSMR_REGION_DIGIT", 5))))
    lsmrServeDepth = max(1, parseEnvInt("BOOTSTRAP_LSMR_SERVE_DEPTH", 2))
    lsmrMinWitnessQuorum = max(1, parseEnvInt("BOOTSTRAP_LSMR_MIN_WITNESS_QUORUM", 1))
    lsmrConfig =
      LsmrConfig.init(
        networkId = resolvedNetworkId,
        serveWitness = true,
        operatorId = lsmrOperatorId,
        regionDigit = lsmrRegionDigit,
        serveDepth = lsmrServeDepth,
        minWitnessQuorum = lsmrMinWitnessQuorum,
      )
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
    .withLsmr(lsmrConfig)
    .withWanBootstrapProfile(
      bootstrapDualStackRoleConfig(
        role = WanBootstrapRole.anchor,
        lsmrConfig = lsmrConfig,
        networkId = resolvedNetworkId,
        journalPath = journalPath,
        primaryPlane = PrimaryRoutingPlane.lsmr,
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
  echo "[bootstrap] lsmr root anchor: operator=", lsmrOperatorId,
    " regionDigit=", int(lsmrRegionDigit),
    " serveDepth=", lsmrServeDepth,
    " minWitnessQuorum=", lsmrMinWitnessQuorum
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
