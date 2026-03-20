{.push raises: [].}

import std/[algorithm, base64, json, options, os, random, sequtils, sets, strutils, tables]
from std/times import epochTime, format, getTime, utc

import chronos, chronicles
import metrics except collect
import stew/byteutils

import ../multiaddress
import ../nameresolving/nameresolver
import ../peerid
import ../peerinfo
import ../peerstore
import ../protocols/kademlia/kademlia
import ../protocols/protocol
import ../protocols/rendezvous
import ../routing_record
import ../stream/connection
import ../switch
import ../utility

const
  DefaultWanBootstrapCandidates* = 7
  DefaultWanBootstrapDialConcurrency* = 3
  DefaultWanBootstrapRecentStableSlots* = 2
  DefaultWanBootstrapRandomPoolCap* = 28
  DefaultWanBootstrapSnapshotLimit* = 32
  DefaultWanBootstrapSnapshotPeers* = 2
  DefaultWanBootstrapSnapshotCodec* = "/nim-libp2p/bootstrap-snapshot/1.0.0"
  DefaultWanBootstrapSuccessThreshold* = 1

logScope:
  topics = "libp2p wanbootstrap"

declareCounter(
  libp2p_wanbootstrap_refresh_runs,
  "wan bootstrap refresh runs",
  labels = ["source"],
)
declareCounter(
  libp2p_wanbootstrap_imported_candidates,
  "wan bootstrap imported candidates",
  labels = ["source"],
)
declareCounter(libp2p_wanbootstrap_join_runs, "wan bootstrap join runs")
declareCounter(
  libp2p_wanbootstrap_join_attempts,
  "wan bootstrap join attempts",
  labels = ["stage", "result"],
)
declareCounter(
  libp2p_wanbootstrap_attest_runs,
  "wan bootstrap attestation runs",
  labels = ["result"],
)
declareCounter(
  libp2p_wanbootstrap_kad_bootstrap_runs,
  "wan bootstrap post-join kad bootstrap runs",
  labels = ["result"],
)
declareGauge(libp2p_wanbootstrap_candidates, "wan bootstrap visible candidates")
declareGauge(libp2p_wanbootstrap_connected_peers, "wan bootstrap connected peers")
declareGauge(libp2p_wanbootstrap_hint_peers, "wan bootstrap hinted peers")

type
  WanBootstrapRole* {.pure.} = enum
    anchor
    publicNode
    mobileEdge

  WanBootstrapTrust* {.pure.} = enum
    rejected
    quarantine
    trusted

  WanBootstrapAttester* = proc(
    switch: Switch,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    sources: seq[string],
    currentTrust: WanBootstrapTrust,
    stage: string,
  ): Future[WanBootstrapTrust] {.async: (raises: []), gcsafe, closure.}

  WanBootstrapConfig* = object
    networkId*: string
    role*: WanBootstrapRole
    journalPath*: string
    peerStoreTtl*: Duration
    maxBootstrapCandidates*: int
    maxConcurrentBootstrapDials*: int
    recentStableSlots*: int
    randomPoolCap*: int
    stopAfterConnected*: int
    connectTimeout*: Duration
    fallbackDnsAddrs*: seq[MultiAddress]
    enableDnsaddrFallback*: bool
    enableRendezvous*: bool
    mountRendezvousProtocol*: bool
    enableSnapshotProtocol*: bool
    snapshotCodec*: string
    snapshotLimit*: int
    maxSnapshotPeers*: int
    rendezvousAdvertiseTtl*: Duration
    rendezvousRefreshInterval*: Duration
    enableKadAfterJoin*: bool
    attestPeer*: WanBootstrapAttester

  WanBootstrapCandidate* = object
    peerId*: PeerId
    addrs*: seq[MultiAddress]
    lastSeenAtMs*: int64
    sources*: seq[string]
    selectionReason*: string
    trust*: WanBootstrapTrust
    relayCapable*: bool
    signedPeerRecordSeen*: bool

  WanBootstrapSnapshotEntry* = object
    peerId*: PeerId
    addrs*: seq[MultiAddress]
    lastSeenAtMs*: int64
    sources*: seq[string]
    trust*: WanBootstrapTrust
    relayCapable*: bool
    signedPeerRecordSeen*: bool
    signedPeerRecord*: string

  WanBootstrapJoinAttempt* = object
    stage*: string
    candidate*: WanBootstrapCandidate
    connected*: bool
    attemptedAddr*: string
    error*: string

  WanBootstrapJoinResult* = object
    attempts*: seq[WanBootstrapJoinAttempt]
    attemptedCount*: int
    connectedCount*: int
    ok*: bool
    timestampMs*: int64

  WanBootstrapRefreshReport* = object
    timestampMs*: int64
    dnsaddrImported*: int
    rendezvousAdvertised*: int
    rendezvousImported*: int
    snapshotImported*: int
    totalImported*: int

  WanBootstrapMetricsSnapshot* = object
    refreshRuns*: int64
    dnsaddrRefreshRuns*: int64
    rendezvousAdvertiseRuns*: int64
    rendezvousRefreshRuns*: int64
    snapshotRefreshRuns*: int64
    dnsaddrImported*: int64
    rendezvousAdvertised*: int64
    rendezvousImported*: int64
    snapshotImported*: int64
    joinRuns*: int64
    joinAttempts*: int64
    joinConnected*: int64
    joinFailed*: int64
    attestRuns*: int64
    attestTrusted*: int64
    attestQuarantine*: int64
    attestRejected*: int64
    attestErrors*: int64
    kadBootstrapRuns*: int64
    kadBootstrapProtocols*: int64
    kadBootstrapFailures*: int64

  WanBootstrapStateSnapshot* = object
    networkId*: string
    role*: WanBootstrapRole
    running*: bool
    journalLoaded*: bool
    journalDirty*: bool
    hintPeerCount*: int
    candidateCount*: int
    connectedPeerCount*: int
    connectedPeers*: seq[PeerId]
    selectedCandidates*: seq[WanBootstrapCandidate]
    lastRefresh*: WanBootstrapRefreshReport
    metrics*: WanBootstrapMetricsSnapshot
    hasLastJoinResult*: bool
    lastJoinResult*: WanBootstrapJoinResult

  WanBootstrapService* = ref object of Service
    switch*: Switch
    config*: WanBootstrapConfig
    hints: Table[PeerId, seq[MultiAddress]]
    hintSources: Table[PeerId, seq[string]]
    lastSeen: Table[PeerId, int64]
    trustBuckets: Table[PeerId, WanBootstrapTrust]
    connectedHandler: ConnEventHandler
    snapshotProtocol: LPProtocol
    snapshotMounted: bool
    rendezvous: RendezVous
    rendezvousMounted: bool
    runner: Future[void]
    running: bool
    journalLoaded: bool
    journalDirty: bool
    lastRefreshReport: WanBootstrapRefreshReport
    metricsSnapshot: WanBootstrapMetricsSnapshot
    lastJoinResult: WanBootstrapJoinResult
    hasLastJoinResult: bool

  WanBootstrapDialOutcome = object
    connected: bool
    attemptedAddr: string
    error: string

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc peerIdText(peerId: PeerId): string =
  $peerId

proc currentUtcDayStamp(): string =
  getTime().utc.format("yyyyMMdd")

proc isLegacyPublicBootstrapAddr(address: MultiAddress): bool =
  let text = $address
  text.contains("/dnsaddr/bootstrap.libp2p.io/")

proc bootstrapDialRank(address: MultiAddress): int =
  let text = $address
  let isIpv6 = text.contains("/ip6/")
  let isIpv4 = text.contains("/ip4/")
  let isQuic = text.contains("/quic-v1")
  let isTcp = text.contains("/tcp/")
  if isIpv6 and isQuic:
    return 0
  if isIpv6 and isTcp:
    return 1
  if isIpv4 and isQuic:
    return 2
  if isIpv4 and isTcp:
    return 3
  4

proc compareBootstrapDialAddrs(a, b: MultiAddress): int =
  let rankCmp = cmp(bootstrapDialRank(a), bootstrapDialRank(b))
  if rankCmp != 0:
    return rankCmp
  cmp($a, $b)

proc sortBootstrapDialAddrs(addrs: var seq[MultiAddress]) =
  addrs.sort(compareBootstrapDialAddrs)

proc stringToBytes(text: string): seq[byte] =
  text.toBytes()

proc bytesToString(data: openArray[byte]): string =
  string.fromBytes(@data)

proc maxTrust(a, b: WanBootstrapTrust): WanBootstrapTrust =
  if ord(a) >= ord(b):
    a
  else:
    b

proc trustToString(value: WanBootstrapTrust): string =
  case value
  of rejected:
    "rejected"
  of quarantine:
    "quarantine"
  of trusted:
    "trusted"

proc parseTrust(text: string): WanBootstrapTrust =
  case text.strip().toLowerAscii()
  of "trusted":
    trusted
  of "rejected":
    rejected
  else:
    quarantine

proc roleLabel(role: WanBootstrapRole): string =
  case role
  of anchor:
    "anchor"
  of publicNode:
    "public"
  of mobileEdge:
    "mobile"

proc clampDuration(value, fallback: Duration): Duration =
  if value <= ZeroDuration:
    fallback
  else:
    value

proc addSource(target: var seq[string], source: string) =
  let normalized = source.strip()
  if normalized.len > 0 and normalized notin target:
    target.add(normalized)

proc dedupeAddrs(addrs: openArray[MultiAddress]): seq[MultiAddress] =
  for addr in addrs:
    var duplicate = false
    for existing in result:
      if existing == addr:
        duplicate = true
        break
    if not duplicate:
      result.add(addr)

proc parseMultiaddrs(texts: JsonNode): seq[MultiAddress] =
  if texts.kind != JArray:
    return @[]
  for item in texts.items():
    if item.kind != JString:
      continue
    let addrRes = MultiAddress.init(item.getStr())
    if addrRes.isErr:
      continue
    result.add(addrRes.get())

proc peerRecordAddresses(record: PeerRecord): seq[MultiAddress] =
  for info in record.addresses:
    result.add(info.address)

proc relayCapableFromAddrs(addrs: openArray[MultiAddress]): bool =
  for addr in addrs:
    if "/p2p-circuit" in $addr:
      return true
  false

proc isBootstrapInfraHint(source: string): bool =
  let normalized = source.strip().toLowerAscii()
  normalized in ["bootstrap", "relay", "mdns", "dht", "peerstore", "dnsaddr", "rendezvous", "journal"]

proc isDirectHintSource(source: string): bool =
  let normalized = source.strip()
  if normalized.len == 0:
    return false
  not isBootstrapInfraHint(normalized)

proc isSnapshotSource(source: string): bool =
  let normalized = source.strip().toLowerAscii()
  normalized.startsWith("snapshot:")

proc sourceClass(source: string): string =
  let normalized = source.strip()
  if normalized.startsWith("Hint:"):
    return normalized
  if isSnapshotSource(normalized):
    return "Snapshot"
  normalized

proc supportsRendezvousCodecs(protocols: openArray[string]): bool =
  for codec in RendezVousCodecs():
    if codec in protocols:
      return true
  false

proc init*(
    _: type WanBootstrapConfig,
    networkId = "default",
    role = WanBootstrapRole.publicNode,
    journalPath = "",
    peerStoreTtl = 30.minutes,
    maxBootstrapCandidates = DefaultWanBootstrapCandidates,
    maxConcurrentBootstrapDials = 0,
    recentStableSlots = DefaultWanBootstrapRecentStableSlots,
    randomPoolCap = DefaultWanBootstrapRandomPoolCap,
    stopAfterConnected = DefaultWanBootstrapSuccessThreshold,
    connectTimeout = 8.seconds,
    fallbackDnsAddrs: seq[MultiAddress] = @[],
    enableDnsaddrFallback = true,
    enableRendezvous = true,
    mountRendezvousProtocol = role == WanBootstrapRole.anchor,
    enableSnapshotProtocol = true,
    snapshotCodec = DefaultWanBootstrapSnapshotCodec,
    snapshotLimit = DefaultWanBootstrapSnapshotLimit,
    maxSnapshotPeers = DefaultWanBootstrapSnapshotPeers,
    rendezvousAdvertiseTtl = 2.hours,
    rendezvousRefreshInterval = 30.minutes,
    enableKadAfterJoin = true,
    attestPeer: WanBootstrapAttester = nil,
): WanBootstrapConfig =
  let dialConcurrency =
    if maxConcurrentBootstrapDials > 0:
      maxConcurrentBootstrapDials
    elif role == WanBootstrapRole.mobileEdge:
      2
    else:
      DefaultWanBootstrapDialConcurrency
  WanBootstrapConfig(
    networkId: if networkId.len == 0: "default" else: networkId,
    role: role,
    journalPath: journalPath,
    peerStoreTtl: clampDuration(peerStoreTtl, 30.minutes),
    maxBootstrapCandidates: max(1, maxBootstrapCandidates),
    maxConcurrentBootstrapDials: max(1, dialConcurrency),
    recentStableSlots: max(0, recentStableSlots),
    randomPoolCap: max(1, randomPoolCap),
    stopAfterConnected: max(1, stopAfterConnected),
    connectTimeout: clampDuration(connectTimeout, 8.seconds),
    fallbackDnsAddrs: fallbackDnsAddrs,
    enableDnsaddrFallback: enableDnsaddrFallback and fallbackDnsAddrs.len > 0,
    enableRendezvous: enableRendezvous,
    mountRendezvousProtocol: mountRendezvousProtocol and enableRendezvous,
    enableSnapshotProtocol: enableSnapshotProtocol,
    snapshotCodec:
      if snapshotCodec.len == 0:
        DefaultWanBootstrapSnapshotCodec
      else:
        snapshotCodec,
    snapshotLimit: max(1, snapshotLimit),
    maxSnapshotPeers: max(1, maxSnapshotPeers),
    rendezvousAdvertiseTtl: clampDuration(rendezvousAdvertiseTtl, 2.hours),
    rendezvousRefreshInterval: clampDuration(rendezvousRefreshInterval, 30.minutes),
    enableKadAfterJoin: enableKadAfterJoin,
    attestPeer: attestPeer,
  )

proc new*(
    T: typedesc[WanBootstrapService],
    config: WanBootstrapConfig = WanBootstrapConfig.init(),
): T =
  T(
    config: config,
    hints: initTable[PeerId, seq[MultiAddress]](),
    hintSources: initTable[PeerId, seq[string]](),
    lastSeen: initTable[PeerId, int64](),
    trustBuckets: initTable[PeerId, WanBootstrapTrust](),
  )

proc getWanBootstrapService*(switch: Switch): WanBootstrapService =
  if switch.isNil:
    return nil
  for service in switch.services:
    if service of WanBootstrapService:
      return WanBootstrapService(service)

proc recordRefreshRun(svc: WanBootstrapService, source: string, imported: int) =
  if svc.isNil:
    return
  libp2p_wanbootstrap_refresh_runs.inc(labelValues = [source])
  if imported > 0:
    libp2p_wanbootstrap_imported_candidates.inc(imported.int64, labelValues = [source])
  inc svc.metricsSnapshot.refreshRuns
  case source
  of "dnsaddr":
    inc svc.metricsSnapshot.dnsaddrRefreshRuns
    svc.metricsSnapshot.dnsaddrImported += imported.int64
  of "rendezvous_advertise":
    inc svc.metricsSnapshot.rendezvousAdvertiseRuns
    svc.metricsSnapshot.rendezvousAdvertised += imported.int64
  of "rendezvous":
    inc svc.metricsSnapshot.rendezvousRefreshRuns
    svc.metricsSnapshot.rendezvousImported += imported.int64
  of "snapshot":
    inc svc.metricsSnapshot.snapshotRefreshRuns
    svc.metricsSnapshot.snapshotImported += imported.int64
  else:
    discard

proc recordJoinResult(svc: WanBootstrapService, joinResult: WanBootstrapJoinResult) =
  if svc.isNil:
    return
  libp2p_wanbootstrap_join_runs.inc()
  inc svc.metricsSnapshot.joinRuns
  svc.lastJoinResult = joinResult
  svc.hasLastJoinResult = true
  for attempt in joinResult.attempts:
    let status = if attempt.connected: "connected" else: "failed"
    libp2p_wanbootstrap_join_attempts.inc(
      labelValues = [attempt.stage, status]
    )
    inc svc.metricsSnapshot.joinAttempts
    if attempt.connected:
      inc svc.metricsSnapshot.joinConnected
    else:
      inc svc.metricsSnapshot.joinFailed

proc recordAttestationResult(svc: WanBootstrapService, trust: WanBootstrapTrust, failed = false) =
  if svc.isNil:
    return
  if failed:
    libp2p_wanbootstrap_attest_runs.inc(labelValues = ["error"])
    inc svc.metricsSnapshot.attestErrors
    return
  libp2p_wanbootstrap_attest_runs.inc(labelValues = [trustToString(trust)])
  inc svc.metricsSnapshot.attestRuns
  case trust
  of trusted:
    inc svc.metricsSnapshot.attestTrusted
  of quarantine:
    inc svc.metricsSnapshot.attestQuarantine
  of rejected:
    inc svc.metricsSnapshot.attestRejected

proc recordKadBootstrapResult(
    svc: WanBootstrapService, protocolCount: int, success: bool
) =
  if svc.isNil:
    return
  if success:
    libp2p_wanbootstrap_kad_bootstrap_runs.inc(labelValues = ["success"])
    inc svc.metricsSnapshot.kadBootstrapRuns
    svc.metricsSnapshot.kadBootstrapProtocols += protocolCount.int64
  else:
    libp2p_wanbootstrap_kad_bootstrap_runs.inc(labelValues = ["failure"])
    inc svc.metricsSnapshot.kadBootstrapFailures

proc stableNamespace*(config: WanBootstrapConfig): string =
  "/bootstrap/" & config.networkId & "/" & roleLabel(config.role) & "/stable"

proc dailyNamespace*(config: WanBootstrapConfig): string =
  "/bootstrap/" & config.networkId & "/" & roleLabel(config.role) & "/" &
    currentUtcDayStamp()

proc rendezvousNamespaces*(config: WanBootstrapConfig): seq[string] =
  @[stableNamespace(config), dailyNamespace(config)]

proc defaultRendezvousNamespacePolicy*(config: WanBootstrapConfig): NamespacePolicy =
  let maxTtl =
    if config.rendezvousAdvertiseTtl < 2.hours:
      config.rendezvousAdvertiseTtl
    else:
      2.hours
  NamespacePolicy.init(
    minTTL = 10.minutes, maxTTL = maxTtl, perPeerLimit = 4, totalLimit = 50000
  )

proc clampBootstrapLimit(svc: WanBootstrapService, limit: int): int =
  let fallback =
    if svc.isNil:
      DefaultWanBootstrapCandidates
    else:
      svc.config.maxBootstrapCandidates
  let resolved = if limit > 0: limit else: fallback
  max(1, min(resolved, fallback))

proc snapshotLimit(svc: WanBootstrapService, limit: int): int =
  let fallback =
    if svc.isNil:
      DefaultWanBootstrapSnapshotLimit
    else:
      svc.config.snapshotLimit
  let resolved = if limit > 0: limit else: fallback
  max(1, min(resolved, fallback))

proc connectedPeerIds(svc: WanBootstrapService): seq[PeerId] =
  if svc.isNil or svc.switch.isNil:
    return @[]
  var seen = initHashSet[string]()
  try:
    for peerId in svc.switch.connectedPeers(Direction.Out):
      if seen.containsOrIncl(peerIdText(peerId)):
        continue
      result.add(peerId)
    for peerId in svc.switch.connectedPeers(Direction.In):
      if seen.containsOrIncl(peerIdText(peerId)):
        continue
      result.add(peerId)
  except CatchableError:
    discard

proc hasSignedPeerRecord(svc: WanBootstrapService, peerId: PeerId): bool =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return false
  try:
    svc.switch.peerStore[SPRBook].contains(peerId)
  except CatchableError:
    false

proc distinctSourceClasses(sources: openArray[string]): int =
  var classes = initHashSet[string]()
  for source in sources:
    let key = sourceClass(source)
    if key.len > 0:
      classes.incl(key)
  classes.len

proc candidateTrust(
    svc: WanBootstrapService,
    peerId: PeerId,
    sources: openArray[string],
    signedPeerRecordSeen: bool,
): WanBootstrapTrust =
  result = svc.trustBuckets.getOrDefault(peerId, WanBootstrapTrust.quarantine)
  if result == trusted or result == rejected:
    return
  if not svc.config.attestPeer.isNil:
    if sources.len > 0:
      return maxTrust(result, quarantine)
    return rejected
  if "Connected" in sources:
    result = maxTrust(result, trusted)
  elif signedPeerRecordSeen:
    result = maxTrust(result, trusted)
  elif distinctSourceClasses(sources) >= 2:
    result = maxTrust(result, trusted)
  elif sources.len > 0:
    result = maxTrust(result, quarantine)
  else:
    result = rejected

proc markBootstrapSeen*(
    svc: WanBootstrapService,
    peerId: PeerId,
    seenAtMs = 0'i64,
    trust = WanBootstrapTrust.quarantine,
): bool =
  if svc.isNil:
    return false
  let ts = if seenAtMs > 0: seenAtMs else: nowMillis()
  svc.lastSeen[peerId] = max(svc.lastSeen.getOrDefault(peerId, 0'i64), ts)
  svc.trustBuckets[peerId] = maxTrust(svc.trustBuckets.getOrDefault(peerId, quarantine), trust)
  svc.journalDirty = true
  true

proc markBootstrapSeen*(
    switch: Switch,
    peerId: PeerId,
    seenAtMs = 0'i64,
    trust = WanBootstrapTrust.quarantine,
): bool =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return false
  svc.markBootstrapSeen(peerId, seenAtMs, trust)

proc registerBootstrapHint*(
    svc: WanBootstrapService,
    peerId: PeerId,
    addrs: openArray[MultiAddress],
    source = "hint",
    trust = WanBootstrapTrust.quarantine,
): bool =
  if svc.isNil or svc.switch.isNil or peerId == svc.switch.peerInfo.peerId:
    return false
  var normalized = dedupeAddrs(addrs)
  if not svc.config.enableDnsaddrFallback:
    normalized.keepItIf(not isLegacyPublicBootstrapAddr(it))
  if normalized.len == 0:
    return false

  let existing = svc.hints.getOrDefault(peerId, @[])
  svc.hints[peerId] = dedupeAddrs(existing & normalized)
  var sources = svc.hintSources.getOrDefault(peerId, @[])
  addSource(sources, source)
  svc.hintSources[peerId] = sources
  discard svc.markBootstrapSeen(peerId, trust = trust)

  if svc.config.peerStoreTtl > ZeroDuration and not svc.switch.peerStore.isNil:
    try:
      svc.switch.peerStore.addAddressesWithTTL(peerId, normalized, svc.config.peerStoreTtl)
    except CatchableError as exc:
      debug "failed to add bootstrap hint addresses to peerstore", peerId = peerId, err = exc.msg
  svc.journalDirty = true
  true

proc registerBootstrapHint*(
    switch: Switch,
    peerId: PeerId,
    addrs: openArray[MultiAddress],
    source = "hint",
    trust = WanBootstrapTrust.quarantine,
): bool =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return false
  svc.registerBootstrapHint(peerId, addrs, source, trust)

proc registerSignedPeerRecord*(
    svc: WanBootstrapService,
    spr: SignedPeerRecord,
    source = "signed-peer-record",
): bool =
  if svc.isNil or svc.switch.isNil:
    return false
  if spr.checkValid().isErr:
    return false
  let peerId = spr.data.peerId
  if peerId == svc.switch.peerInfo.peerId:
    return false
  if not svc.switch.peerStore.isNil:
    try:
      svc.switch.peerStore[SPRBook][peerId] = spr.envelope
    except CatchableError as exc:
      debug "failed to store signed peer record", peerId = peerId, err = exc.msg
  let importedTrust = if svc.config.attestPeer.isNil: trusted else: quarantine
  svc.trustBuckets[peerId] = maxTrust(
    svc.trustBuckets.getOrDefault(peerId, quarantine), importedTrust
  )
  result = svc.registerBootstrapHint(
    peerId, spr.data.peerRecordAddresses(), source = source, trust = importedTrust
  )
  discard svc.markBootstrapSeen(peerId, trust = importedTrust)

proc registerSignedPeerRecord*(
    switch: Switch, spr: SignedPeerRecord, source = "signed-peer-record"
): bool =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return false
  svc.registerSignedPeerRecord(spr, source)

proc encodeSignedPeerRecord(svc: WanBootstrapService, peerId: PeerId): string =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return ""
  try:
    if not svc.switch.peerStore[SPRBook].contains(peerId):
      return ""
    let env = svc.switch.peerStore[SPRBook][peerId]
    let encoded = env.encode().valueOr:
      return ""
    base64.encode(bytesToString(encoded))
  except CatchableError:
    ""

proc decodeSignedPeerRecord(encoded: string): Option[SignedPeerRecord] =
  if encoded.len == 0:
    return none(SignedPeerRecord)
  try:
    let decoded = base64.decode(encoded).toBytes()
    let spr = SignedPeerRecord.decode(decoded).valueOr:
      return none(SignedPeerRecord)
    some(spr)
  except CatchableError:
    none(SignedPeerRecord)

proc collectBootstrapCandidates*(svc: WanBootstrapService): seq[WanBootstrapCandidate] =
  if svc.isNil or svc.switch.isNil:
    return @[]

  type CandidateEntry = object
    peerId: PeerId
    addrs: seq[MultiAddress]
    lastSeenAtMs: int64
    sources: seq[string]

  var candidates = initTable[string, CandidateEntry]()
  let localId = svc.switch.peerInfo.peerId
  let nowTs = nowMillis()

  proc upsert(
      peerId: PeerId,
      addrs: openArray[MultiAddress],
      source: string,
      seenAtMs: int64 = 0'i64
  ) =
    if peerId == localId:
      return
    let key = peerIdText(peerId)
    var entry = candidates.getOrDefault(
      key, CandidateEntry(peerId: peerId, addrs: @[], lastSeenAtMs: 0, sources: @[])
    )
    entry.addrs = dedupeAddrs(entry.addrs & @addrs)
    addSource(entry.sources, source)
    let knownSeen = svc.lastSeen.getOrDefault(peerId, 0'i64)
    entry.lastSeenAtMs = max(entry.lastSeenAtMs, max(knownSeen, seenAtMs))
    candidates[key] = entry

  for peerId, addrs in svc.hints.pairs:
    upsert(peerId, addrs, "Hints")
    for source in svc.hintSources.getOrDefault(peerId):
      upsert(peerId, @[], "Hint:" & source)

  for peerId in svc.connectedPeerIds():
    upsert(peerId, svc.switch.peerStore.getAddresses(peerId), "Connected", nowTs)

  if not svc.switch.peerStore.isNil:
    try:
      let addrBook = svc.switch.peerStore[AddressBook]
      for peerId, addrs in addrBook.book.pairs:
        upsert(peerId, addrs, "PeerStore")
    except CatchableError as exc:
      debug "failed to read bootstrap candidates from peerstore", err = exc.msg

  for item in candidates.values():
    if item.addrs.len == 0:
      continue
    var seenAt = item.lastSeenAtMs
    if seenAt <= 0 and "Connected" in item.sources:
      seenAt = nowTs
    if seenAt <= 0:
      seenAt = nowTs - 1
    let signedPeerRecordSeen = svc.hasSignedPeerRecord(item.peerId)
    let trust = svc.candidateTrust(item.peerId, item.sources, signedPeerRecordSeen)
    if trust == rejected:
      continue
    result.add(
      WanBootstrapCandidate(
        peerId: item.peerId,
        addrs: item.addrs,
        lastSeenAtMs: seenAt,
        sources: item.sources,
        selectionReason: "",
        trust: trust,
        relayCapable: relayCapableFromAddrs(item.addrs),
        signedPeerRecordSeen: signedPeerRecordSeen,
      )
    )

proc bootstrapSourceScore(candidate: WanBootstrapCandidate): int =
  result = min(candidate.addrs.len, 8)
  case candidate.trust
  of trusted:
    result += 300
  of quarantine:
    result += 120
  of rejected:
    result -= 500
  if candidate.signedPeerRecordSeen:
    result += 160
  if candidate.relayCapable:
    result += 20
  for source in candidate.sources:
    case source
    of "Connected":
      result += 400
    of "Hints":
      result += 220
    of "PeerStore":
      result += 100
    of "mdns", "Hint:mdns":
      result += 40
    else:
      if source.startsWith("Hint:"):
        let hintSource = source[5 .. ^1]
        if isDirectHintSource(hintSource):
          result += 450
        elif isBootstrapInfraHint(hintSource):
          result += 120
      elif isSnapshotSource(source):
        result += 180

proc compareBootstrapCandidates(a, b: WanBootstrapCandidate): int =
  result = cmp(b.lastSeenAtMs, a.lastSeenAtMs)
  if result == 0:
    result = cmp(ord(b.trust), ord(a.trust))
  if result == 0:
    result = cmp(bootstrapSourceScore(b), bootstrapSourceScore(a))
  if result == 0:
    result = cmp(b.addrs.len, a.addrs.len)
  if result == 0:
    result = cmp(peerIdText(a.peerId), peerIdText(b.peerId))

proc sortBootstrapCandidates(candidates: var seq[WanBootstrapCandidate]) =
  candidates.sort(compareBootstrapCandidates)

proc selectBootstrapCandidates*(
    svc: WanBootstrapService, limit = 0
): seq[WanBootstrapCandidate] =
  if svc.isNil:
    return @[]

  let target = svc.clampBootstrapLimit(limit)
  var ordered = svc.collectBootstrapCandidates()
  if ordered.len == 0:
    return @[]
  sortBootstrapCandidates(ordered)
  var selected: seq[WanBootstrapCandidate] = @[]
  var selectedPeerIds = initHashSet[string]()

  proc addSelected(entry: WanBootstrapCandidate, reason: string): bool =
    let key = peerIdText(entry.peerId)
    if key.len == 0 or key in selectedPeerIds:
      return false
    var chosen = entry
    chosen.selectionReason = reason
    selected.add(chosen)
    selectedPeerIds.incl(key)
    true

  proc takeMatching(
      reason: string,
      maxCount: int,
      predicate: proc(candidate: WanBootstrapCandidate): bool {.gcsafe, raises: [].}
  ) =
    var added = 0
    for entry in ordered:
      if selected.len >= target or added >= maxCount:
        break
      if peerIdText(entry.peerId) in selectedPeerIds or not predicate(entry):
        continue
      if addSelected(entry, reason):
        inc added

  takeMatching(
    "direct_hint",
    1,
    proc(candidate: WanBootstrapCandidate): bool =
      for source in candidate.sources:
        if source.startsWith("Hint:") and isDirectHintSource(source[5 .. ^1]):
          return true
      false
  )

  if selected.len < target:
    takeMatching(
      "hint",
      1,
      proc(candidate: WanBootstrapCandidate): bool =
        "Hints" in candidate.sources
    )

  if selected.len < target:
    takeMatching(
      "recent_stable",
      min(svc.config.recentStableSlots, target - selected.len),
      proc(candidate: WanBootstrapCandidate): bool = true
    )

  if selected.len >= target:
    return selected

  var pool: seq[WanBootstrapCandidate] = @[]
  for entry in ordered:
    if peerIdText(entry.peerId) in selectedPeerIds:
      continue
    pool.add(entry)
    if pool.len >= svc.config.randomPoolCap:
      break
  if pool.len == 0:
    return selected

  random.randomize(int(nowMillis() and 0x7fff_ffff'i64))
  for i in countdown(pool.high, 1):
    let j = random.rand(i)
    swap(pool[i], pool[j])
  for entry in pool:
    if selected.len >= target:
      break
    discard addSelected(entry, "random_pool")
  selected

proc collectBootstrapCandidates*(switch: Switch): seq[WanBootstrapCandidate] =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return @[]
  svc.collectBootstrapCandidates()

proc updateObservability(svc: WanBootstrapService) =
  if svc.isNil:
    return
  libp2p_wanbootstrap_hint_peers.set(svc.hints.len.int64)
  libp2p_wanbootstrap_connected_peers.set(svc.connectedPeerIds().len.int64)
  libp2p_wanbootstrap_candidates.set(svc.collectBootstrapCandidates().len.int64)

proc selectBootstrapCandidates*(
    switch: Switch, limit = 0
): seq[WanBootstrapCandidate] =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return @[]
  svc.selectBootstrapCandidates(limit)

proc snapshotEntryToJson(entry: WanBootstrapSnapshotEntry): JsonNode =
  result = newJObject()
  result["peerId"] = %peerIdText(entry.peerId)
  var addrsNode = newJArray()
  for addr in entry.addrs:
    addrsNode.add(%($addr))
  result["addrs"] = addrsNode
  result["lastSeenAtMs"] = %entry.lastSeenAtMs
  var sourcesNode = newJArray()
  for source in entry.sources:
    sourcesNode.add(%source)
  result["sources"] = sourcesNode
  result["trust"] = %trustToString(entry.trust)
  result["relayCapable"] = %entry.relayCapable
  result["signedPeerRecordSeen"] = %entry.signedPeerRecordSeen
  if entry.signedPeerRecord.len > 0:
    result["signedPeerRecord"] = %entry.signedPeerRecord

proc candidateToJson(candidate: WanBootstrapCandidate): JsonNode =
  result = newJObject()
  result["peerId"] = %peerIdText(candidate.peerId)
  var addrsNode = newJArray()
  for addr in candidate.addrs:
    addrsNode.add(%($addr))
  result["addrs"] = addrsNode
  result["lastSeenAtMs"] = %candidate.lastSeenAtMs
  var sourcesNode = newJArray()
  for source in candidate.sources:
    sourcesNode.add(%source)
  result["sources"] = sourcesNode
  result["selectionReason"] = %candidate.selectionReason
  result["trust"] = %trustToString(candidate.trust)
  result["relayCapable"] = %candidate.relayCapable
  result["signedPeerRecordSeen"] = %candidate.signedPeerRecordSeen

proc joinAttemptToJson(attempt: WanBootstrapJoinAttempt): JsonNode =
  result = newJObject()
  result["stage"] = %attempt.stage
  result["connected"] = %attempt.connected
  if attempt.attemptedAddr.len > 0:
    result["attemptedAddr"] = %attempt.attemptedAddr
  if attempt.error.len > 0:
    result["error"] = %attempt.error
  result["candidate"] = candidateToJson(attempt.candidate)

proc joinResultToJson(joinResult: WanBootstrapJoinResult): JsonNode =
  result = newJObject()
  result["attemptedCount"] = %joinResult.attemptedCount
  result["connectedCount"] = %joinResult.connectedCount
  result["ok"] = %joinResult.ok
  result["timestampMs"] = %joinResult.timestampMs
  var attemptsNode = newJArray()
  for attempt in joinResult.attempts:
    attemptsNode.add(joinAttemptToJson(attempt))
  result["attempts"] = attemptsNode

proc refreshReportToJson(report: WanBootstrapRefreshReport): JsonNode =
  result = newJObject()
  result["timestampMs"] = %report.timestampMs
  result["dnsaddrImported"] = %report.dnsaddrImported
  result["rendezvousAdvertised"] = %report.rendezvousAdvertised
  result["rendezvousImported"] = %report.rendezvousImported
  result["snapshotImported"] = %report.snapshotImported
  result["totalImported"] = %report.totalImported

proc metricsSnapshotToJson(metrics: WanBootstrapMetricsSnapshot): JsonNode =
  result = newJObject()
  result["refreshRuns"] = %metrics.refreshRuns
  result["dnsaddrRefreshRuns"] = %metrics.dnsaddrRefreshRuns
  result["rendezvousAdvertiseRuns"] = %metrics.rendezvousAdvertiseRuns
  result["rendezvousRefreshRuns"] = %metrics.rendezvousRefreshRuns
  result["snapshotRefreshRuns"] = %metrics.snapshotRefreshRuns
  result["dnsaddrImported"] = %metrics.dnsaddrImported
  result["rendezvousAdvertised"] = %metrics.rendezvousAdvertised
  result["rendezvousImported"] = %metrics.rendezvousImported
  result["snapshotImported"] = %metrics.snapshotImported
  result["joinRuns"] = %metrics.joinRuns
  result["joinAttempts"] = %metrics.joinAttempts
  result["joinConnected"] = %metrics.joinConnected
  result["joinFailed"] = %metrics.joinFailed
  result["attestRuns"] = %metrics.attestRuns
  result["attestTrusted"] = %metrics.attestTrusted
  result["attestQuarantine"] = %metrics.attestQuarantine
  result["attestRejected"] = %metrics.attestRejected
  result["attestErrors"] = %metrics.attestErrors
  result["kadBootstrapRuns"] = %metrics.kadBootstrapRuns
  result["kadBootstrapProtocols"] = %metrics.kadBootstrapProtocols
  result["kadBootstrapFailures"] = %metrics.kadBootstrapFailures

proc parseSnapshotEntry(node: JsonNode): Option[WanBootstrapSnapshotEntry] =
  if node.kind != JObject or not node.hasKey("peerId"):
    return none(WanBootstrapSnapshotEntry)
  let peerIdNode = node{"peerId"}
  if peerIdNode.isNil or peerIdNode.kind != JString:
    return none(WanBootstrapSnapshotEntry)
  let peerId = PeerId.init(peerIdNode.getStr()).valueOr:
    return none(WanBootstrapSnapshotEntry)
  let addrs =
    if not node{"addrs"}.isNil:
      parseMultiaddrs(node{"addrs"})
    else:
      @[]
  let sources =
    if not node{"sources"}.isNil and node{"sources"}.kind == JArray:
      node{"sources"}.items().toSeq().filterIt(it.kind == JString).mapIt(it.getStr())
    else:
      @[]
  some(
    WanBootstrapSnapshotEntry(
      peerId: peerId,
      addrs: addrs,
      lastSeenAtMs:
        if not node{"lastSeenAtMs"}.isNil:
          node{"lastSeenAtMs"}.getBiggestInt().int64
        else:
          0'i64,
      sources: sources,
      trust:
        if not node{"trust"}.isNil:
          parseTrust(node{"trust"}.getStr())
        else:
          quarantine,
      relayCapable:
        if not node{"relayCapable"}.isNil:
          node{"relayCapable"}.getBool()
        else:
          relayCapableFromAddrs(addrs),
      signedPeerRecordSeen:
        if not node{"signedPeerRecordSeen"}.isNil:
          node{"signedPeerRecordSeen"}.getBool()
        else:
          false,
      signedPeerRecord:
        if not node{"signedPeerRecord"}.isNil:
          node{"signedPeerRecord"}.getStr()
        else:
          "",
    )
  )

proc bootstrapSnapshot*(svc: WanBootstrapService, limit = 0): seq[WanBootstrapSnapshotEntry] =
  if svc.isNil:
    return @[]
  let capped = svc.snapshotLimit(limit)
  var candidates = svc.collectBootstrapCandidates()
  sortBootstrapCandidates(candidates)
  for candidate in candidates:
    if result.len >= capped:
      break
    result.add(
      WanBootstrapSnapshotEntry(
        peerId: candidate.peerId,
        addrs: candidate.addrs,
        lastSeenAtMs: candidate.lastSeenAtMs,
        sources: candidate.sources,
        trust: candidate.trust,
        relayCapable: candidate.relayCapable,
        signedPeerRecordSeen: candidate.signedPeerRecordSeen,
        signedPeerRecord: svc.encodeSignedPeerRecord(candidate.peerId),
      )
    )

proc bootstrapSnapshot*(switch: Switch, limit = 0): seq[WanBootstrapSnapshotEntry] =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return @[]
  svc.bootstrapSnapshot(limit)

proc bootstrapStateSnapshot*(
    svc: WanBootstrapService, candidateLimit = 0
): WanBootstrapStateSnapshot =
  if svc.isNil:
    return WanBootstrapStateSnapshot()
  let connectedPeers = svc.connectedPeerIds()
  let selected =
    svc.selectBootstrapCandidates(
      if candidateLimit > 0: candidateLimit else: svc.config.maxBootstrapCandidates
    )
  WanBootstrapStateSnapshot(
    networkId: svc.config.networkId,
    role: svc.config.role,
    running: svc.running,
    journalLoaded: svc.journalLoaded,
    journalDirty: svc.journalDirty,
    hintPeerCount: svc.hints.len,
    candidateCount: svc.collectBootstrapCandidates().len,
    connectedPeerCount: connectedPeers.len,
    connectedPeers: connectedPeers,
    selectedCandidates: selected,
    lastRefresh: svc.lastRefreshReport,
    metrics: svc.metricsSnapshot,
    hasLastJoinResult: svc.hasLastJoinResult,
    lastJoinResult: svc.lastJoinResult,
  )

proc bootstrapStateSnapshot*(
    switch: Switch, candidateLimit = 0
): WanBootstrapStateSnapshot =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return WanBootstrapStateSnapshot()
  svc.bootstrapStateSnapshot(candidateLimit)

proc bootstrapStateJson*(
    svc: WanBootstrapService, candidateLimit = 0
): JsonNode =
  let snapshot = svc.bootstrapStateSnapshot(candidateLimit)
  result = newJObject()
  result["networkId"] = %snapshot.networkId
  result["role"] = %roleLabel(snapshot.role)
  result["running"] = %snapshot.running
  result["journalLoaded"] = %snapshot.journalLoaded
  result["journalDirty"] = %snapshot.journalDirty
  result["hintPeerCount"] = %snapshot.hintPeerCount
  result["candidateCount"] = %snapshot.candidateCount
  result["connectedPeerCount"] = %snapshot.connectedPeerCount
  var peersNode = newJArray()
  for peerId in snapshot.connectedPeers:
    peersNode.add(%peerIdText(peerId))
  result["connectedPeers"] = peersNode
  var candidatesNode = newJArray()
  for candidate in snapshot.selectedCandidates:
    candidatesNode.add(candidateToJson(candidate))
  result["selectedCandidates"] = candidatesNode
  result["lastRefresh"] = refreshReportToJson(snapshot.lastRefresh)
  result["metrics"] = metricsSnapshotToJson(snapshot.metrics)
  result["hasLastJoinResult"] = %snapshot.hasLastJoinResult
  if snapshot.hasLastJoinResult:
    result["lastJoinResult"] = joinResultToJson(snapshot.lastJoinResult)

proc bootstrapStateJson*(switch: Switch, candidateLimit = 0): JsonNode =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return newJObject()
  svc.bootstrapStateJson(candidateLimit)

proc saveBootstrapJournal*(svc: WanBootstrapService): bool =
  if svc.isNil or svc.config.journalPath.len == 0:
    return false
  let directory = splitFile(svc.config.journalPath).dir
  if directory.len > 0:
    try:
      createDir(directory)
    except CatchableError as exc:
      warn "failed to ensure bootstrap journal directory", path = svc.config.journalPath, err = exc.msg
      return false

  let root = newJObject()
  root["version"] = %1
  root["networkId"] = %svc.config.networkId
  var entriesNode = newJArray()
  for entry in svc.bootstrapSnapshot(limit = 512):
    entriesNode.add(snapshotEntryToJson(entry))
  root["entries"] = entriesNode
  try:
    writeFile(svc.config.journalPath, $root)
    svc.journalDirty = false
    return true
  except CatchableError as exc:
    warn "failed to write bootstrap journal", path = svc.config.journalPath, err = exc.msg
    return false

proc saveBootstrapJournal*(switch: Switch): bool =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return false
  svc.saveBootstrapJournal()

proc loadBootstrapJournal*(svc: WanBootstrapService): bool =
  if svc.isNil:
    return false
  svc.journalLoaded = true
  if svc.config.journalPath.len == 0 or not fileExists(svc.config.journalPath):
    return false
  try:
    let root = parseJson(readFile(svc.config.journalPath))
    if root.kind != JObject or not root.hasKey("entries") or root["entries"].kind != JArray:
      return false
    for item in root["entries"].items():
      let entryOpt = parseSnapshotEntry(item)
      if entryOpt.isNone:
        continue
      let entry = entryOpt.get()
      if entry.peerId == svc.switch.peerInfo.peerId or entry.addrs.len == 0:
        continue
      var imported = false
      decodeSignedPeerRecord(entry.signedPeerRecord).withValue(spr):
        imported = svc.registerSignedPeerRecord(spr, "journal")
      if not imported:
        imported = svc.registerBootstrapHint(
          entry.peerId, entry.addrs, source = "journal", trust = entry.trust
        )
      if imported:
        discard svc.markBootstrapSeen(entry.peerId, entry.lastSeenAtMs, entry.trust)
        svc.trustBuckets[entry.peerId] =
          maxTrust(svc.trustBuckets.getOrDefault(entry.peerId, quarantine), entry.trust)
    svc.journalDirty = false
    svc.updateObservability()
    true
  except CatchableError as exc:
    warn "failed to load bootstrap journal", path = svc.config.journalPath, err = exc.msg
    false

proc loadBootstrapJournal*(switch: Switch): bool =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return false
  svc.loadBootstrapJournal()

proc extractSeedPeer(address: MultiAddress): Option[(PeerId, MultiAddress)] =
  let text = $address
  let marker = "/p2p/"
  let idx = text.rfind(marker)
  if idx < 0:
    return none((PeerId, MultiAddress))
  let peerText = text[idx + marker.len .. ^1]
  let baseText = text[0 ..< idx]
  let peerId = PeerId.init(peerText).valueOr:
    return none((PeerId, MultiAddress))
  let baseAddr = MultiAddress.init(baseText).valueOr:
    return none((PeerId, MultiAddress))
  some((peerId, baseAddr))

proc resolveBootstrapFallbacks*(
    svc: WanBootstrapService
): Future[seq[(PeerId, seq[MultiAddress])]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or not svc.config.enableDnsaddrFallback:
    return @[]
  type SeedGroup = object
    peerId: PeerId
    addrs: seq[MultiAddress]
  var grouped = initTable[string, SeedGroup]()
  for configured in svc.config.fallbackDnsAddrs:
    var resolved = @[configured]
    if DNS.matchPartial(configured):
      if svc.switch.nameResolver.isNil:
        debug "skipping dnsaddr fallback without name resolver", address = $configured
        continue
      try:
        resolved = await svc.switch.nameResolver.resolveMAddress(configured)
      except CatchableError as exc:
        debug "failed to resolve dnsaddr fallback", address = $configured, err = exc.msg
        continue
    for address in resolved:
      let seedOpt = extractSeedPeer(address)
      if seedOpt.isNone:
        continue
      let (peerId, baseAddr) = seedOpt.get()
      let key = peerIdText(peerId)
      var group = grouped.getOrDefault(key, SeedGroup(peerId: peerId, addrs: @[]))
      group.addrs = dedupeAddrs(group.addrs & @[baseAddr])
      grouped[key] = group
  for group in grouped.values():
    result.add((group.peerId, group.addrs))

proc refreshFromDnsaddrFallback*(
    svc: WanBootstrapService
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return 0
  let resolved = await svc.resolveBootstrapFallbacks()
  for (peerId, addrs) in resolved:
    if svc.registerBootstrapHint(peerId, addrs, source = "dnsaddr", trust = quarantine):
      inc result
  svc.recordRefreshRun("dnsaddr", result)
  svc.updateObservability()

proc refreshFromDnsaddrFallback*(
    switch: Switch
): Future[int] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return 0
  await svc.refreshFromDnsaddrFallback()

proc rendezvousPeers(svc: WanBootstrapService): seq[PeerId] =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  let protoBook = svc.switch.peerStore[ProtoBook]
  for peerId in svc.connectedPeerIds():
    if supportsRendezvousCodecs(protoBook[peerId]):
      result.add(peerId)

proc supportsSnapshot(svc: WanBootstrapService, peerId: PeerId): bool =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return false
  svc.config.snapshotCodec in svc.switch.peerStore[ProtoBook][peerId]

proc advertiseRendezvousNamespaces*(
    svc: WanBootstrapService
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil or not svc.config.enableRendezvous or svc.rendezvous.isNil:
    return 0
  let peers = svc.rendezvousPeers()
  if peers.len == 0:
    return 0
  for namespace in svc.config.rendezvousNamespaces():
    try:
      await svc.rendezvous.advertise(namespace, svc.config.rendezvousAdvertiseTtl, peers)
      inc result
    except CatchableError as exc:
      debug "failed to advertise rendezvous namespace", namespace = namespace, err = exc.msg
  svc.recordRefreshRun("rendezvous_advertise", result)

proc advertiseRendezvousNamespaces*(
    switch: Switch
): Future[int] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return 0
  await svc.advertiseRendezvousNamespaces()

proc refreshFromRendezvousNamespaces*(
    svc: WanBootstrapService
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil or not svc.config.enableRendezvous or svc.rendezvous.isNil:
    return 0
  let peers = svc.rendezvousPeers()
  if peers.len == 0:
    return 0
  for namespace in svc.config.rendezvousNamespaces():
    try:
      let records =
        await svc.rendezvous.request(Opt.some(namespace), svc.config.snapshotLimit, peers)
      for record in records:
        let addrs = record.peerRecordAddresses()
        if record.peerId == svc.switch.peerInfo.peerId or addrs.len == 0:
          continue
        if svc.registerBootstrapHint(
            record.peerId, addrs, source = "rendezvous", trust = trusted
        ):
          inc result
        discard svc.markBootstrapSeen(record.peerId, trust = trusted)
      if records.len > 0:
        debug "rendezvous namespace refreshed", namespace = namespace, discovered = records.len
    except CatchableError as exc:
      debug "failed to refresh rendezvous namespace", namespace = namespace, err = exc.msg
  svc.recordRefreshRun("rendezvous", result)
  svc.updateObservability()

proc refreshFromRendezvousNamespaces*(
    switch: Switch
): Future[int] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return 0
  await svc.refreshFromRendezvousNamespaces()

proc buildSnapshotResponse(svc: WanBootstrapService, limit: int): JsonNode =
  result = newJObject()
  result["version"] = %1
  result["networkId"] = %svc.config.networkId
  result["role"] = %roleLabel(svc.config.role)
  result["timestampMs"] = %nowMillis()
  var entriesNode = newJArray()
  for entry in svc.bootstrapSnapshot(limit):
    entriesNode.add(snapshotEntryToJson(entry))
  result["entries"] = entriesNode

proc buildSnapshotProtocol(svc: WanBootstrapService): LPProtocol =
  proc handleSnapshot(
      conn: Connection, proto: string
  ) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(32 * 1024)
      var limit = svc.config.snapshotLimit
      if payload.len > 0:
        try:
          let node = parseJson(bytesToString(payload))
          if node.kind == JObject and node.hasKey("limit"):
            limit = max(1, node["limit"].getInt())
        except CatchableError:
          discard
      await conn.writeLp(stringToBytes($svc.buildSnapshotResponse(limit)))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "snapshot handler failed", err = exc.msg
    finally:
      await conn.close()
  LPProtocol.new(@[svc.config.snapshotCodec], handleSnapshot)

proc requestBootstrapSnapshot*(
    svc: WanBootstrapService,
    peerId: PeerId,
    limit = 0,
): Future[seq[WanBootstrapSnapshotEntry]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or not svc.config.enableSnapshotProtocol:
    return @[]
  try:
    let conn = await svc.switch.dial(peerId, @[svc.config.snapshotCodec])
    defer:
      await conn.close()
    let req = %*{"op": "snapshot", "limit": svc.snapshotLimit(limit)}
    await conn.writeLp(stringToBytes($req))
    let payload = await conn.readLp(4 * 1024 * 1024)
    let root = parseJson(bytesToString(payload))
    if root.kind != JObject:
      return @[]
    let networkNode = root{"networkId"}
    if not networkNode.isNil and networkNode.getStr() != svc.config.networkId:
      return @[]
    let entriesNode = root{"entries"}
    if entriesNode.isNil or entriesNode.kind != JArray:
      return @[]
    for item in entriesNode.items():
      let entryOpt = parseSnapshotEntry(item)
      if entryOpt.isSome():
        result.add(entryOpt.get())
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "snapshot request failed", peerId = peerId, err = exc.msg
    return @[]

proc requestBootstrapSnapshot*(
    switch: Switch,
    peerId: PeerId,
    limit = 0,
): Future[seq[WanBootstrapSnapshotEntry]] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return @[]
  await svc.requestBootstrapSnapshot(peerId, limit)

proc refreshFromBootstrapSnapshots*(
    svc: WanBootstrapService
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil or not svc.config.enableSnapshotProtocol:
    return 0
  var queried = 0
  for peerId in svc.connectedPeerIds():
    if queried >= svc.config.maxSnapshotPeers:
      break
    if not svc.supportsSnapshot(peerId):
      continue
    let entries = await svc.requestBootstrapSnapshot(peerId, svc.config.snapshotLimit)
    inc queried
    for entry in entries:
      if entry.peerId == svc.switch.peerInfo.peerId or entry.addrs.len == 0:
        continue
      var imported = false
      decodeSignedPeerRecord(entry.signedPeerRecord).withValue(spr):
        imported = svc.registerSignedPeerRecord(spr, "snapshot:" & peerIdText(peerId))
      if not imported:
        imported = svc.registerBootstrapHint(
          entry.peerId,
          entry.addrs,
          source = "snapshot:" & peerIdText(peerId),
          trust = entry.trust,
        )
      if imported:
        discard svc.markBootstrapSeen(entry.peerId, entry.lastSeenAtMs, entry.trust)
        svc.trustBuckets[entry.peerId] =
          maxTrust(svc.trustBuckets.getOrDefault(entry.peerId, quarantine), entry.trust)
        inc result
  svc.recordRefreshRun("snapshot", result)
  svc.updateObservability()

proc refreshFromBootstrapSnapshots*(
    switch: Switch
): Future[int] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return 0
  await svc.refreshFromBootstrapSnapshots()

proc refreshBootstrapState*(
    svc: WanBootstrapService
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return 0
  var report = WanBootstrapRefreshReport(timestampMs: nowMillis())
  if not svc.journalLoaded:
    discard svc.loadBootstrapJournal()
  if not svc.switch.isNil and svc.connectedPeerIds().len == 0 and svc.config.enableDnsaddrFallback:
    report.dnsaddrImported = await svc.refreshFromDnsaddrFallback()
    result += report.dnsaddrImported
  if svc.connectedPeerIds().len > 0:
    report.rendezvousAdvertised = await svc.advertiseRendezvousNamespaces()
    result += report.rendezvousAdvertised
    report.rendezvousImported = await svc.refreshFromRendezvousNamespaces()
    result += report.rendezvousImported
    report.snapshotImported = await svc.refreshFromBootstrapSnapshots()
    result += report.snapshotImported
  report.totalImported =
    report.dnsaddrImported + report.rendezvousImported + report.snapshotImported
  svc.lastRefreshReport = report
  if svc.journalDirty:
    discard svc.saveBootstrapJournal()
  svc.updateObservability()

proc refreshBootstrapState*(
    switch: Switch
): Future[int] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return 0
  await svc.refreshBootstrapState()

proc mountedKadDHTs(svc: WanBootstrapService): seq[KadDHT] =
  if svc.isNil or svc.switch.isNil or svc.switch.ms.isNil:
    return @[]
  for handler in svc.switch.ms.handlers:
    if handler.isNil or handler.protocol.isNil or not (handler.protocol of KadDHT):
      continue
    let kad = KadDHT(handler.protocol)
    if kad notin result:
      result.add(kad)

proc connectedBootstrapPeers(svc: WanBootstrapService): seq[PeerInfo] =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  for peerId in svc.connectedPeerIds():
    let addrs = dedupeAddrs(svc.switch.peerStore.getAddresses(peerId))
    if addrs.len == 0:
      continue
    result.add(PeerInfo(peerId: peerId, listenAddrs: addrs, addrs: addrs))

proc attestConnectedPeer(
    svc: WanBootstrapService, candidate: WanBootstrapCandidate, stage: string
): Future[tuple[accepted: bool, trust: WanBootstrapTrust]] {.async.} =
  if svc.isNil:
    return (false, rejected)
  if svc.config.attestPeer.isNil:
    return (true, trusted)
  try:
    let trust = await svc.config.attestPeer(
      svc.switch,
      candidate.peerId,
      candidate.addrs,
      candidate.sources,
      candidate.trust,
      stage,
    )
    svc.recordAttestationResult(trust)
    if trust == rejected:
      await svc.switch.disconnect(candidate.peerId)
      return (false, rejected)
    return (true, trust)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "wan bootstrap attestation failed", peerId = candidate.peerId, stage = stage, err = exc.msg
    svc.recordAttestationResult(quarantine, failed = true)
    return (true, quarantine)

proc bootstrapMountedKadDHTs(
    svc: WanBootstrapService
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil or not svc.config.enableKadAfterJoin:
    return 0
  let peers = svc.connectedBootstrapPeers()
  if peers.len == 0:
    return 0
  let kads = svc.mountedKadDHTs()
  if kads.len == 0:
    return 0
  var successful = 0
  for kad in kads:
    try:
      await kad.bootstrap(peers)
      inc successful
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "wan bootstrap kademlia bootstrap failed", err = exc.msg
  svc.recordKadBootstrapResult(kads.len, successful == kads.len)
  successful

proc connectBootstrapCandidate(
    svc: WanBootstrapService, candidate: WanBootstrapCandidate, stage: string
): Future[WanBootstrapDialOutcome] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or candidate.addrs.len == 0:
    return WanBootstrapDialOutcome(connected: false, error: "no_candidate_addrs")
  var ordered = candidate.addrs
  sortBootstrapDialAddrs(ordered)
  var lastError = ""
  var attemptedAddr = ""
  for addr in ordered:
    attemptedAddr = $addr
    let fut = svc.switch.connect(candidate.peerId, @[addr], forceDial = true)
    let completed = await withTimeout(fut, svc.config.connectTimeout)
    if not completed:
      fut.cancelSoon()
      lastError = "timeout"
      continue
    try:
      await fut
      let provisionalTrust =
        if svc.config.attestPeer.isNil:
          trusted
        else:
          maxTrust(candidate.trust, quarantine)
      svc.trustBuckets[candidate.peerId] = maxTrust(
        svc.trustBuckets.getOrDefault(candidate.peerId, quarantine), provisionalTrust
      )
      discard svc.markBootstrapSeen(candidate.peerId, trust = provisionalTrust)
      let (accepted, attestedTrust) = await svc.attestConnectedPeer(candidate, stage)
      if not accepted:
        svc.trustBuckets[candidate.peerId] = rejected
        discard svc.markBootstrapSeen(candidate.peerId, trust = rejected)
        return WanBootstrapDialOutcome(
          connected: false,
          attemptedAddr: attemptedAddr,
          error: "attestation_rejected",
        )
      svc.trustBuckets[candidate.peerId] = maxTrust(
        svc.trustBuckets.getOrDefault(candidate.peerId, quarantine), attestedTrust
      )
      discard svc.markBootstrapSeen(candidate.peerId, trust = attestedTrust)
      return WanBootstrapDialOutcome(connected: true, attemptedAddr: attemptedAddr)
    except CatchableError as exc:
      lastError = if exc.msg.len > 0: exc.msg else: $exc.name
  WanBootstrapDialOutcome(
    connected: false,
    attemptedAddr: attemptedAddr,
    error: lastError,
  )

proc connectBootstrapCandidateExact(
    svc: WanBootstrapService, candidate: WanBootstrapCandidate, stage: string
): Future[WanBootstrapDialOutcome] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or candidate.addrs.len == 0:
    return WanBootstrapDialOutcome(connected: false, error: "no_candidate_addrs")
  var ordered = candidate.addrs
  sortBootstrapDialAddrs(ordered)
  var lastError = ""
  var attemptedAddr = ""
  for addr in ordered:
    attemptedAddr = $addr
    var hadOriginalAddrs = false
    var originalAddrs: seq[MultiAddress] = @[]
    let peerStore = svc.switch.peerStore
    if not peerStore.isNil:
      try:
        originalAddrs = peerStore[AddressBook][candidate.peerId]
        hadOriginalAddrs = peerStore[AddressBook].contains(candidate.peerId)
        peerStore.setAddresses(candidate.peerId, @[addr])
      except CatchableError:
        hadOriginalAddrs = false
        originalAddrs = @[]
    let fut = svc.switch.connect(candidate.peerId, @[addr], forceDial = true)
    let completed = await withTimeout(fut, svc.config.connectTimeout)
    if not peerStore.isNil:
      try:
        if hadOriginalAddrs:
          peerStore.setAddresses(candidate.peerId, originalAddrs)
        else:
          discard peerStore[AddressBook].del(candidate.peerId)
      except CatchableError:
        discard
    if not completed:
      fut.cancelSoon()
      lastError = "timeout"
      continue
    try:
      await fut
      let provisionalTrust =
        if svc.config.attestPeer.isNil:
          trusted
        else:
          maxTrust(candidate.trust, quarantine)
      svc.trustBuckets[candidate.peerId] = maxTrust(
        svc.trustBuckets.getOrDefault(candidate.peerId, quarantine), provisionalTrust
      )
      discard svc.markBootstrapSeen(candidate.peerId, trust = provisionalTrust)
      let (accepted, attestedTrust) = await svc.attestConnectedPeer(candidate, stage)
      if not accepted:
        svc.trustBuckets[candidate.peerId] = rejected
        discard svc.markBootstrapSeen(candidate.peerId, trust = rejected)
        return WanBootstrapDialOutcome(
          connected: false,
          attemptedAddr: attemptedAddr,
          error: "attestation_rejected",
        )
      svc.trustBuckets[candidate.peerId] = maxTrust(
        svc.trustBuckets.getOrDefault(candidate.peerId, quarantine), attestedTrust
      )
      discard svc.markBootstrapSeen(candidate.peerId, trust = attestedTrust)
      return WanBootstrapDialOutcome(connected: true, attemptedAddr: attemptedAddr)
    except CatchableError as exc:
      lastError = if exc.msg.len > 0: exc.msg else: $exc.name
  WanBootstrapDialOutcome(
    connected: false,
    attemptedAddr: attemptedAddr,
    error: lastError,
  )

proc connectBootstrapCandidates(
    svc: WanBootstrapService,
    stage: string,
    candidates: seq[WanBootstrapCandidate],
): Future[seq[WanBootstrapDialOutcome]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or candidates.len == 0:
    return @[]

  let batchSize = max(1, svc.config.maxConcurrentBootstrapDials)
  var start = 0
  var connectedCount = 0
  while start < candidates.len:
    let stop = min(start + batchSize, candidates.len)
    var futs: seq[Future[WanBootstrapDialOutcome]] = @[]
    for idx in start ..< stop:
      futs.add(connectBootstrapCandidate(svc, candidates[idx], stage))
    let grouped = allFutures(futs)
    if not grouped.isNil:
      await grouped
    for offset, fut in futs:
      if fut.isNil or not fut.finished():
        continue
      let outcome =
        try:
          fut.read()
        except CatchableError:
          WanBootstrapDialOutcome(connected: false, error: "future_read_failed")
      result.add(outcome)
      if outcome.connected:
        inc connectedCount
    if connectedCount >= svc.config.stopAfterConnected:
      break
    start = stop

proc joinViaSeedBootstrap*(
    svc: WanBootstrapService,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "seeded_join",
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  result = WanBootstrapJoinResult(
    attempts: @[],
    attemptedCount: 0,
    connectedCount: 0,
    ok: false,
    timestampMs: nowMillis(),
  )
  if svc.isNil or svc.switch.isNil or addrs.len == 0 or peerId == svc.switch.peerInfo.peerId:
    return

  if not svc.journalLoaded:
    discard svc.loadBootstrapJournal()

  var normalized = dedupeAddrs(addrs)
  if not svc.config.enableDnsaddrFallback:
    normalized.keepItIf(not isLegacyPublicBootstrapAddr(it))
  if normalized.len == 0:
    return

  let hintSource = source.strip()
  let effectiveSource = if hintSource.len > 0: hintSource else: "seeded_join"
  discard svc.registerBootstrapHint(peerId, normalized, effectiveSource, quarantine)

  let signedPeerRecordSeen = svc.hasSignedPeerRecord(peerId)
  let candidateSources = @["Hints", "Hint:" & effectiveSource]
  let candidate = WanBootstrapCandidate(
    peerId: peerId,
    addrs: normalized,
    lastSeenAtMs: nowMillis(),
    sources: candidateSources,
    selectionReason: "seeded_direct_hint",
    trust: svc.candidateTrust(peerId, candidateSources, signedPeerRecordSeen),
    relayCapable: relayCapableFromAddrs(normalized),
    signedPeerRecordSeen: signedPeerRecordSeen,
  )

  let outcome = await svc.connectBootstrapCandidateExact(candidate, "seeded_direct_hint")
  result.attempts.add(
    WanBootstrapJoinAttempt(
      stage: "seeded_direct_hint",
      candidate: candidate,
      connected: outcome.connected,
      attemptedAddr: outcome.attemptedAddr,
      error: outcome.error,
    )
  )
  result.attemptedCount = 1
  if outcome.connected:
    result.connectedCount = 1
    result.ok = true

  if svc.connectedPeerIds().len > 0:
    discard await svc.advertiseRendezvousNamespaces()
    discard await svc.refreshFromRendezvousNamespaces()
    discard await svc.refreshFromBootstrapSnapshots()

  if result.ok:
    discard await svc.bootstrapMountedKadDHTs()

  svc.recordJoinResult(result)
  if result.ok and svc.journalDirty:
    discard svc.saveBootstrapJournal()
  svc.updateObservability()

proc joinViaSeedBootstrap*(
    switch: Switch,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "seeded_join",
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return WanBootstrapJoinResult(timestampMs: nowMillis())
  await svc.joinViaSeedBootstrap(peerId, addrs, source)

proc runBootstrapStage(
    svc: WanBootstrapService,
    stage: string,
    attemptedAddrCounts: TableRef[string, int],
    limit = 0,
): Future[seq[WanBootstrapJoinAttempt]] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return @[]
  var candidates: seq[WanBootstrapCandidate] = @[]
  for candidate in svc.selectBootstrapCandidates(limit):
    let key = peerIdText(candidate.peerId)
    let knownAddrCount = attemptedAddrCounts.getOrDefault(key, 0)
    if candidate.addrs.len <= knownAddrCount:
      continue
    candidates.add(candidate)
  if candidates.len == 0:
    return @[]
  let orderedReasons = ["direct_hint", "hint", "recent_stable", "random_pool"]
  var connectedCount = 0

  proc appendAttemptsForGroup(
      groupCandidates: seq[WanBootstrapCandidate],
      outcomes: seq[WanBootstrapDialOutcome]
  ) =
    for idx in 0 ..< outcomes.len:
      let candidate = groupCandidates[idx]
      let key = peerIdText(candidate.peerId)
      attemptedAddrCounts[key] =
        max(attemptedAddrCounts.getOrDefault(key, 0), candidate.addrs.len)
      let outcome =
        if idx < outcomes.len:
          outcomes[idx]
        else:
          WanBootstrapDialOutcome(connected: false, error: "missing_outcome")
      result.add(
        WanBootstrapJoinAttempt(
          stage: stage,
          candidate: candidate,
          connected: outcome.connected,
          attemptedAddr: outcome.attemptedAddr,
          error: outcome.error,
        )
      )
      if outcome.connected:
        inc connectedCount

  var usedReasons = initHashSet[string]()
  for reason in orderedReasons:
    var group: seq[WanBootstrapCandidate] = @[]
    for candidate in candidates:
      if candidate.selectionReason == reason:
        group.add(candidate)
    if group.len == 0:
      continue
    usedReasons.incl(reason)
    let groupStage =
      if reason.len > 0: stage & ":" & reason
      else: stage
    if group.len == 0 or connectedCount >= svc.config.stopAfterConnected:
      continue
    let outcomes = await svc.connectBootstrapCandidates(groupStage, group)
    appendAttemptsForGroup(group, outcomes)
    if connectedCount >= svc.config.stopAfterConnected:
      break

  if connectedCount >= svc.config.stopAfterConnected:
    return

  var remaining: seq[WanBootstrapCandidate] = @[]
  for candidate in candidates:
    if candidate.selectionReason.len == 0 or candidate.selectionReason notin usedReasons:
      remaining.add(candidate)
  if remaining.len > 0 and connectedCount < svc.config.stopAfterConnected:
    let outcomes = await svc.connectBootstrapCandidates(stage, remaining)
    appendAttemptsForGroup(remaining, outcomes)

proc joinViaBootstrap*(
    svc: WanBootstrapService, limit = 0, allowFallback = true
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return WanBootstrapJoinResult(timestampMs: nowMillis())
  if not svc.journalLoaded:
    discard svc.loadBootstrapJournal()

  result = WanBootstrapJoinResult(
    attempts: @[],
    attemptedCount: 0,
    connectedCount: 0,
    ok: false,
    timestampMs: nowMillis(),
  )
  let attemptedAddrCounts = newTable[string, int]()

  proc appendAttempts(items: seq[WanBootstrapJoinAttempt]) =
    for item in items:
      result.attempts.add(item)
      inc result.attemptedCount
      if item.connected:
        inc result.connectedCount
    result.ok = result.connectedCount > 0

  appendAttempts(await svc.runBootstrapStage("local", attemptedAddrCounts, limit))

  if result.connectedCount < svc.config.stopAfterConnected and svc.config.enableDnsaddrFallback and allowFallback:
    discard await svc.refreshFromDnsaddrFallback()
    appendAttempts(await svc.runBootstrapStage("dnsaddr", attemptedAddrCounts, limit))

  if svc.connectedPeerIds().len > 0:
    discard await svc.advertiseRendezvousNamespaces()
    discard await svc.refreshFromRendezvousNamespaces()
    if result.connectedCount < svc.config.stopAfterConnected:
      appendAttempts(await svc.runBootstrapStage("rendezvous", attemptedAddrCounts, limit))

    discard await svc.refreshFromBootstrapSnapshots()
    if result.connectedCount < svc.config.stopAfterConnected:
      appendAttempts(await svc.runBootstrapStage("snapshot", attemptedAddrCounts, limit))

  result.ok = result.connectedCount > 0
  if result.ok:
    discard await svc.bootstrapMountedKadDHTs()
  svc.recordJoinResult(result)
  if result.ok and svc.journalDirty:
    discard svc.saveBootstrapJournal()
  svc.updateObservability()

proc bootstrapReconnect*(
    svc: WanBootstrapService, limit = 0
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  await svc.joinViaBootstrap(limit)

proc joinViaBootstrap*(
    switch: Switch, limit = 0, allowFallback = true
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return WanBootstrapJoinResult(timestampMs: nowMillis())
  await svc.joinViaBootstrap(limit, allowFallback)

proc bootstrapReconnect*(
    switch: Switch, limit = 0
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return WanBootstrapJoinResult(timestampMs: nowMillis())
  await svc.bootstrapReconnect(limit)

proc bootstrapRoleConfig*(
    role: WanBootstrapRole,
    networkId = "default",
    journalPath = "",
    fallbackDnsAddrs: seq[MultiAddress] = @[],
): WanBootstrapConfig =
  case role
  of anchor:
    WanBootstrapConfig.init(
      networkId = networkId,
      role = role,
      journalPath = journalPath,
      fallbackDnsAddrs = fallbackDnsAddrs,
      mountRendezvousProtocol = true,
      maxConcurrentBootstrapDials = 3,
      stopAfterConnected = 2,
    )
  of publicNode:
    WanBootstrapConfig.init(
      networkId = networkId,
      role = role,
      journalPath = journalPath,
      fallbackDnsAddrs = fallbackDnsAddrs,
      maxConcurrentBootstrapDials = 3,
    )
  of mobileEdge:
    WanBootstrapConfig.init(
      networkId = networkId,
      role = role,
      journalPath = journalPath,
      fallbackDnsAddrs = fallbackDnsAddrs,
      maxConcurrentBootstrapDials = 2,
      stopAfterConnected = 1,
    )

proc syncLoop(svc: WanBootstrapService) {.async: (raises: [CancelledError]).} =
  while svc.running:
    try:
      discard await svc.refreshBootstrapState()
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "wan bootstrap refresh loop failed", err = exc.msg
    await sleepAsync(svc.config.rendezvousRefreshInterval)

proc configureManagedRendezvous(svc: WanBootstrapService) =
  if svc.isNil or not svc.config.enableRendezvous:
    return
  if svc.rendezvous.isNil:
    let minDuration =
      if svc.config.rendezvousAdvertiseTtl > 10.minutes:
        10.minutes
      else:
        1.minutes
    let maxDuration =
      if svc.config.rendezvousAdvertiseTtl > minDuration:
        svc.config.rendezvousAdvertiseTtl
      else:
        minDuration + 1.minutes
    try:
      svc.rendezvous = RendezVous.new(minDuration = minDuration, maxDuration = maxDuration)
      svc.rendezvous.setup(svc.switch)
    except CatchableError as exc:
      warn "failed to initialize rendezvous helper", err = exc.msg
      svc.rendezvous = nil
      return
  let policy = svc.config.defaultRendezvousNamespacePolicy()
  for namespace in svc.config.rendezvousNamespaces():
    try:
      svc.rendezvous.setNamespacePolicy(namespace, policy)
    except CatchableError as exc:
      debug "failed to configure rendezvous namespace policy", namespace = namespace, err = exc.msg

method setup*(
    svc: WanBootstrapService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(svc).setup(switch)
  if hasBeenSetup:
    svc.switch = switch
    if not svc.journalLoaded:
      discard svc.loadBootstrapJournal()

    if svc.config.enableSnapshotProtocol and svc.snapshotProtocol.isNil:
      svc.snapshotProtocol = svc.buildSnapshotProtocol()
    if not svc.snapshotProtocol.isNil and switch.isStarted() and not svc.snapshotProtocol.started:
      await svc.snapshotProtocol.start()
    if svc.config.enableSnapshotProtocol and not svc.snapshotMounted and not svc.snapshotProtocol.isNil:
      try:
        switch.mount(svc.snapshotProtocol)
        svc.snapshotMounted = true
      except CatchableError as exc:
        warn "failed to mount bootstrap snapshot protocol", err = exc.msg

    svc.configureManagedRendezvous()
    if not svc.rendezvous.isNil and switch.isStarted() and not svc.rendezvous.started:
      await svc.rendezvous.start()
    if svc.config.mountRendezvousProtocol and not svc.rendezvous.isNil and not svc.rendezvousMounted:
      try:
        switch.mount(svc.rendezvous)
        svc.rendezvousMounted = true
      except CatchableError as exc:
        warn "failed to mount rendezvous protocol for wan bootstrap", err = exc.msg

    svc.connectedHandler = proc(
        peerId: PeerId, event: ConnEvent
    ): Future[void] {.async: (raises: [CancelledError]).} =
      let connectedTrust = if svc.config.attestPeer.isNil: trusted else: quarantine
      discard svc.markBootstrapSeen(peerId, trust = connectedTrust)
    switch.addConnEventHandler(svc.connectedHandler, ConnEventKind.Connected)
    await svc.run(switch)
    svc.updateObservability()
  hasBeenSetup

method run*(
    svc: WanBootstrapService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  if svc.running:
    return
  svc.running = true
  if svc.config.rendezvousRefreshInterval > ZeroDuration:
    svc.runner = svc.syncLoop()
  svc.updateObservability()

method stop*(
    svc: WanBootstrapService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(svc).stop(switch)
  if hasBeenStopped:
    svc.running = false
    if not svc.runner.isNil:
      await svc.runner.cancelAndWait()
      svc.runner = nil
    if not svc.connectedHandler.isNil and not switch.isNil:
      switch.removeConnEventHandler(svc.connectedHandler, ConnEventKind.Connected)
      svc.connectedHandler = nil
    if not svc.snapshotProtocol.isNil and svc.snapshotProtocol.started:
      await svc.snapshotProtocol.stop()
    if not svc.rendezvous.isNil and svc.rendezvous.started:
      await svc.rendezvous.stop()
    if svc.journalDirty:
      discard svc.saveBootstrapJournal()
    svc.updateObservability()
  hasBeenStopped

proc startWanBootstrapService*(
    switch: Switch,
    config: WanBootstrapConfig = WanBootstrapConfig.init(),
): Future[WanBootstrapService] {.async: (raises: [CancelledError]).} =
  if switch.isNil:
    return nil
  var svc = getWanBootstrapService(switch)
  if svc.isNil:
    svc = WanBootstrapService.new(config)
    switch.services.add(svc)
  if switch.isStarted():
    discard await svc.setup(switch)
  svc

{.pop.}
