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
import ../lsmr
import ../protocols/kademlia/kademlia
import ../protocols/protocol
import ../protocols/rendezvous
import ../routing_record
import ./lsmrservice as lsmrsvc
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

  ExactSeedDialPolicy* {.pure.} = enum
    single
    serial
    parallel

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
    routingMode*: RoutingPlaneMode
    primaryPlane*: PrimaryRoutingPlane
    lsmrConfig*: Option[LsmrConfig]
    legacyDiscoveryEnabled*: bool
    legacyCompatibilityOnly*: bool
    journalPath*: string
    authoritativePeerIds*: seq[string]
    deferAutoJoinUntilRelayReady*: bool
    exactSeedDialPolicy*: ExactSeedDialPolicy
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
    lsmrRecordSeen*: bool
    lsmrPrefix*: LsmrPath
    lsmrConfidence*: uint8
    lsmrWitnessCount*: int
    lsmrExpiresAtMs*: int64
    lsmrDistance*: int

  WanBootstrapSnapshotEntry* = object
    peerId*: PeerId
    addrs*: seq[MultiAddress]
    lastSeenAtMs*: int64
    sources*: seq[string]
    trust*: WanBootstrapTrust
    relayCapable*: bool
    signedPeerRecordSeen*: bool
    signedPeerRecord*: string
    lsmrRecordSeen*: bool
    lsmrCoordinateRecord*: string
    lsmrMigrationRecords*: seq[string]
    lsmrIsolationEvidence*: seq[string]

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
    lsmrWitnessRequests*: int64
    lsmrWitnessSuccess*: int64
    lsmrWitnessQuorumSuccess*: int64
    lsmrWitnessQuorumFailure*: int64
    lsmrValidationTrusted*: int64
    lsmrValidationQuarantine*: int64
    lsmrValidationRejected*: int64
    lsmrStaleRecords*: int64
    lsmrForgedRecords*: int64
    lsmrBiasReorders*: int64

  WanBootstrapStateSnapshot* = object
    networkId*: string
    role*: WanBootstrapRole
    routingMode*: RoutingPlaneMode
    primaryPlane*: PrimaryRoutingPlane
    shadowMode*: bool
    exactSeedDialPolicy*: ExactSeedDialPolicy
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
    routingStatus*: RoutingPlaneStatus
    lastRendezvousAdvertiseError*: string
    lastRendezvousRequestError*: string
    lastSnapshotError*: string
    hasLastJoinResult*: bool
    lastJoinResult*: WanBootstrapJoinResult

  WanBootstrapService* = ref object of Service
    switch*: Switch
    config*: WanBootstrapConfig
    startupRelayReady: bool
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
    lastRendezvousAdvertiseError: string
    lastRendezvousRequestError: string
    lastSnapshotError: string
    joinInProgress: bool
    joinFuture: Future[WanBootstrapJoinResult].Raising([CancelledError])

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
  of WanBootstrapTrust.rejected:
    "rejected"
  of WanBootstrapTrust.quarantine:
    "quarantine"
  of WanBootstrapTrust.trusted:
    "trusted"

proc parseTrust(text: string): WanBootstrapTrust =
  case text.strip().toLowerAscii()
  of "trusted":
    WanBootstrapTrust.trusted
  of "rejected":
    WanBootstrapTrust.rejected
  else:
    WanBootstrapTrust.quarantine

proc lsmrDiscoveryActive(config: WanBootstrapConfig): bool =
  config.routingMode in {RoutingPlaneMode.dualStack, RoutingPlaneMode.lsmrOnly} and
    config.lsmrConfig.isSome()

proc legacyDiscoveryActive(config: WanBootstrapConfig): bool =
  config.legacyDiscoveryEnabled and config.routingMode != RoutingPlaneMode.lsmrOnly

proc shadowMode(config: WanBootstrapConfig): bool =
  config.routingMode == RoutingPlaneMode.dualStack

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

proc isExactPeerMultiaddr(address: MultiAddress): bool =
  let text = $address
  text.contains("/p2p/") or text.contains("/ipfs/")

proc exactBootstrapDialAddrs(addrs: openArray[MultiAddress]): seq[MultiAddress] =
  for addr in addrs:
    if isExactPeerMultiaddr(addr):
      result.add(addr)
  result = dedupeAddrs(result)
  if result.len == 0:
    result = dedupeAddrs(addrs)
  if result.len > 1:
    sortBootstrapDialAddrs(result)

proc exactSeedDialPolicyLabel(policy: ExactSeedDialPolicy): string =
  case policy
  of ExactSeedDialPolicy.single:
    "same_family_serial"
  of ExactSeedDialPolicy.serial:
    "serial"
  of ExactSeedDialPolicy.parallel:
    "parallel"

proc selectExactBootstrapDialAddrs(
    addrs: openArray[MultiAddress], policy: ExactSeedDialPolicy
): seq[MultiAddress] =
  result = exactBootstrapDialAddrs(addrs)
  if policy == ExactSeedDialPolicy.single and result.len > 1:
    let primaryRank = bootstrapDialRank(result[0])
    let allowRanks =
      if primaryRank <= 1:
        @[0, 1]
      elif primaryRank <= 3:
        @[2, 3]
      else:
        @[primaryRank]
    result.keepItIf(bootstrapDialRank(it) in allowRanks)

proc isBootstrapInfraHint(source: string): bool =
  let normalized = source.strip().toLowerAscii()
  normalized in [
    "bootstrap",
    "relay",
    "mdns",
    "dht",
    "peerstore",
    "dnsaddr",
    "rendezvous",
    "journal",
  ] or normalized.startsWith("auto_authoritative_seed")

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
    routingMode = RoutingPlaneMode.legacyOnly,
    primaryPlane = PrimaryRoutingPlane.legacy,
    lsmrConfig = none(LsmrConfig),
    legacyDiscoveryEnabled = true,
    legacyCompatibilityOnly = false,
    journalPath = "",
    authoritativePeerIds: seq[string] = @[],
    deferAutoJoinUntilRelayReady = false,
    exactSeedDialPolicy = ExactSeedDialPolicy.serial,
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
  let legacyEnabled =
    legacyDiscoveryEnabled and routingMode != RoutingPlaneMode.lsmrOnly
  let dialConcurrency =
    if maxConcurrentBootstrapDials > 0:
      maxConcurrentBootstrapDials
    elif role == WanBootstrapRole.mobileEdge:
      2
    else:
      DefaultWanBootstrapDialConcurrency
  let resolvedExactSeedDialPolicy =
    if exactSeedDialPolicy == ExactSeedDialPolicy.serial and
        role == WanBootstrapRole.mobileEdge and authoritativePeerIds.len > 0:
      ExactSeedDialPolicy.single
    else:
      exactSeedDialPolicy
  WanBootstrapConfig(
    networkId: if networkId.len == 0: "default" else: networkId,
    role: role,
    routingMode: routingMode,
    primaryPlane: primaryPlane,
    lsmrConfig: lsmrConfig,
    legacyDiscoveryEnabled: legacyEnabled,
    legacyCompatibilityOnly: legacyCompatibilityOnly or primaryPlane == PrimaryRoutingPlane.lsmr,
    journalPath: journalPath,
    authoritativePeerIds: authoritativePeerIds,
    deferAutoJoinUntilRelayReady: deferAutoJoinUntilRelayReady,
    exactSeedDialPolicy: resolvedExactSeedDialPolicy,
    peerStoreTtl: clampDuration(peerStoreTtl, 30.minutes),
    maxBootstrapCandidates: max(1, maxBootstrapCandidates),
    maxConcurrentBootstrapDials: max(1, dialConcurrency),
    recentStableSlots: max(0, recentStableSlots),
    randomPoolCap: max(1, randomPoolCap),
    stopAfterConnected: max(1, stopAfterConnected),
    connectTimeout: clampDuration(connectTimeout, 8.seconds),
    fallbackDnsAddrs: fallbackDnsAddrs,
    enableDnsaddrFallback: legacyEnabled and enableDnsaddrFallback and fallbackDnsAddrs.len > 0,
    enableRendezvous: legacyEnabled and enableRendezvous,
    mountRendezvousProtocol: legacyEnabled and mountRendezvousProtocol and enableRendezvous,
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
    enableKadAfterJoin: legacyEnabled and enableKadAfterJoin,
    attestPeer: attestPeer,
  )

proc new*(
    T: typedesc[WanBootstrapService],
    config: WanBootstrapConfig = WanBootstrapConfig.init(),
): T =
  T(
    config: config,
    startupRelayReady: not config.deferAutoJoinUntilRelayReady,
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
  of WanBootstrapTrust.trusted:
    inc svc.metricsSnapshot.attestTrusted
  of WanBootstrapTrust.quarantine:
    inc svc.metricsSnapshot.attestQuarantine
  of WanBootstrapTrust.rejected:
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
  if result == WanBootstrapTrust.trusted or result == WanBootstrapTrust.rejected:
    return
  if not svc.config.attestPeer.isNil:
    if sources.len > 0:
      return maxTrust(result, WanBootstrapTrust.quarantine)
    return WanBootstrapTrust.rejected
  if "Connected" in sources:
    result = maxTrust(result, WanBootstrapTrust.trusted)
  elif signedPeerRecordSeen:
    result = maxTrust(result, WanBootstrapTrust.trusted)
  elif distinctSourceClasses(sources) >= 2:
    result = maxTrust(result, WanBootstrapTrust.trusted)
  elif sources.len > 0:
    result = maxTrust(result, WanBootstrapTrust.quarantine)
  else:
    result = WanBootstrapTrust.rejected

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
  svc.trustBuckets[peerId] =
    maxTrust(svc.trustBuckets.getOrDefault(peerId, WanBootstrapTrust.quarantine), trust)
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
  let importedTrust =
    if svc.config.attestPeer.isNil:
      WanBootstrapTrust.trusted
    else:
      WanBootstrapTrust.quarantine
  svc.trustBuckets[peerId] = maxTrust(
    svc.trustBuckets.getOrDefault(peerId, WanBootstrapTrust.quarantine), importedTrust
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

proc encodeLsmrCoordinateRecord(svc: WanBootstrapService, peerId: PeerId): string =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return ""
  try:
    let record =
      if svc.switch.peerStore[ActiveLsmrBook].contains(peerId):
        some(svc.switch.peerStore[ActiveLsmrBook][peerId])
      elif svc.switch.peerStore[LsmrBook].contains(peerId):
        some(svc.switch.peerStore[LsmrBook][peerId])
      else:
        none(SignedLsmrCoordinateRecord)
    if record.isNone():
      return ""
    let encoded = record.get().encode().valueOr:
      return ""
    base64.encode(bytesToString(encoded))
  except CatchableError:
    ""

proc decodeLsmrCoordinateRecord(
    encoded: string
): Option[SignedLsmrCoordinateRecord] =
  if encoded.len == 0:
    return none(SignedLsmrCoordinateRecord)
  try:
    let decoded = base64.decode(encoded).toBytes()
    let record = SignedLsmrCoordinateRecord.decode(decoded).valueOr:
      return none(SignedLsmrCoordinateRecord)
    some(record)
  except CatchableError:
    none(SignedLsmrCoordinateRecord)

proc encodeLsmrMigrationRecords(
    svc: WanBootstrapService, peerId: PeerId
): seq[string] =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  for record in svc.switch.peerStore[LsmrMigrationBook][peerId]:
    let encoded = record.encode().valueOr:
      continue
    result.add(base64.encode(bytesToString(encoded)))

proc decodeLsmrMigrationRecord(encoded: string): Option[SignedLsmrMigrationRecord] =
  if encoded.len == 0:
    return none(SignedLsmrMigrationRecord)
  try:
    let decoded = base64.decode(encoded).toBytes()
    let record = SignedLsmrMigrationRecord.decode(decoded).valueOr:
      return none(SignedLsmrMigrationRecord)
    some(record)
  except CatchableError:
    none(SignedLsmrMigrationRecord)

proc encodeLsmrIsolationEvidence(
    svc: WanBootstrapService, peerId: PeerId
): seq[string] =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  for evidence in svc.switch.peerStore[LsmrIsolationBook][peerId]:
    let encoded = evidence.encode().valueOr:
      continue
    result.add(base64.encode(bytesToString(encoded)))

proc decodeLsmrIsolationEvidence(
    encoded: string
): Option[SignedLsmrIsolationEvidence] =
  if encoded.len == 0:
    return none(SignedLsmrIsolationEvidence)
  try:
    let decoded = base64.decode(encoded).toBytes()
    let evidence = SignedLsmrIsolationEvidence.decode(decoded).valueOr:
      return none(SignedLsmrIsolationEvidence)
    some(evidence)
  except CatchableError:
    none(SignedLsmrIsolationEvidence)

proc lsmrSnapshotForPeer(
    svc: WanBootstrapService, peerId: PeerId
): lsmrsvc.LsmrValidationSnapshot =
  let lsmrService = lsmrsvc.getLsmrService(svc.switch)
  if lsmrService.isNil:
    return lsmrsvc.LsmrValidationSnapshot(
      trust: lsmrsvc.LsmrValidationTrust(1),
      reason: "disabled",
      recordSeen: false,
      attestedPrefix: @[],
      confidence: 0'u8,
      witnessCount: 0,
      expiresAtMs: 0'i64,
      distance: high(int),
    )
  let snapshot = lsmrsvc.validateLsmrPeer(svc.switch, lsmrService.config, peerId)
  if snapshot.trust == lsmrsvc.LsmrValidationTrust.trusted:
    discard lsmrsvc.promoteValidatedCoordinateRecord(
      svc.switch, lsmrService.config, peerId
    )
  snapshot

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
  let connectedPeerIds = svc.connectedPeerIds()
  let authoritativeBootstrapActive =
    svc.config.authoritativePeerIds.len > 0 and connectedPeerIds.len == 0

  proc isAuthoritativePeer(peerId: PeerId): bool =
    let text = peerIdText(peerId)
    for item in svc.config.authoritativePeerIds:
      if item == text:
        return true
    false

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
    if authoritativeBootstrapActive and not isAuthoritativePeer(item.peerId):
      continue
    var seenAt = item.lastSeenAtMs
    if seenAt <= 0 and "Connected" in item.sources:
      seenAt = nowTs
    if seenAt <= 0:
      seenAt = nowTs - 1
    let signedPeerRecordSeen = svc.hasSignedPeerRecord(item.peerId)
    let trust = svc.candidateTrust(item.peerId, item.sources, signedPeerRecordSeen)
    if trust == WanBootstrapTrust.rejected:
      continue
    let lsmr = svc.lsmrSnapshotForPeer(item.peerId)
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
        lsmrRecordSeen: lsmr.recordSeen,
        lsmrPrefix: lsmr.attestedPrefix,
        lsmrConfidence: lsmr.confidence,
        lsmrWitnessCount: lsmr.witnessCount,
        lsmrExpiresAtMs: lsmr.expiresAtMs,
        lsmrDistance: lsmr.distance,
      )
    )

proc bootstrapSourceScore(candidate: WanBootstrapCandidate): int =
  result = min(candidate.addrs.len, 8)
  case candidate.trust
  of WanBootstrapTrust.trusted:
    result += 300
  of WanBootstrapTrust.quarantine:
    result += 120
  of WanBootstrapTrust.rejected:
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

proc compareBootstrapCandidates(
    svc: WanBootstrapService, a, b: WanBootstrapCandidate
): int =
  result = cmp(b.lastSeenAtMs, a.lastSeenAtMs)
  if result == 0:
    result = cmp(ord(b.trust), ord(a.trust))
  let lsmrService =
    if not svc.isNil and not svc.switch.isNil:
      lsmrsvc.getLsmrService(svc.switch)
    else:
      nil
  if result == 0 and not lsmrService.isNil and lsmrService.config.enableBootstrapBias:
    let aValid = a.lsmrDistance != high(int)
    let bValid = b.lsmrDistance != high(int)
    if aValid and bValid:
      result = cmp(a.lsmrDistance, b.lsmrDistance)
    elif aValid xor bValid:
      result = if aValid: -1 else: 1
  if result == 0:
    result = cmp(bootstrapSourceScore(b), bootstrapSourceScore(a))
  if result == 0:
    result = cmp(b.addrs.len, a.addrs.len)
  if result == 0:
    result = cmp(peerIdText(a.peerId), peerIdText(b.peerId))

proc isAuthoritativePeer(
    svc: WanBootstrapService, peerId: PeerId
): bool =
  if svc.isNil:
    return false
  let text = peerIdText(peerId)
  for item in svc.config.authoritativePeerIds:
    if item == text:
      return true
  false

proc authoritativeExactBootstrapAddrs(
    svc: WanBootstrapService,
    peerId: PeerId,
): seq[MultiAddress] {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil or peerId == svc.switch.peerInfo.peerId:
    return @[]

  var collected: seq[MultiAddress] = @[]
  collected.add(svc.hints.getOrDefault(peerId, @[]))

  if not svc.switch.peerStore.isNil:
    try:
      collected.add(svc.switch.peerStore.getAddresses(peerId))
    except CatchableError:
      discard

  for candidate in svc.collectBootstrapCandidates():
    if candidate.peerId == peerId:
      collected.add(candidate.addrs)

  exactBootstrapDialAddrs(dedupeAddrs(collected))

proc hasImportedBootstrapPeers(
    svc: WanBootstrapService
): bool {.gcsafe, raises: [].}

proc selectBootstrapCandidates*(
    svc: WanBootstrapService, limit = 0
): seq[WanBootstrapCandidate] {.gcsafe, raises: [].}

proc sortBootstrapCandidates(
    svc: WanBootstrapService, candidates: var seq[WanBootstrapCandidate]
){.gcsafe, raises: [].}

proc peerCurrentlyConnected(
    svc: WanBootstrapService, peerId: PeerId
): bool {.gcsafe, raises: [].}

proc bootstrapAutoJoinNeedsConnectedSync(
    svc: WanBootstrapService
): bool {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil:
    return false
  let connected = svc.connectedPeerIds()
  connected.len > 0 and (
    (not svc.hasLastJoinResult) or
    (not svc.lastJoinResult.ok) or
    (not svc.hasImportedBootstrapPeers())
  )

proc chooseConnectedBootstrapPeer(
    svc: WanBootstrapService
): Option[tuple[peerId: PeerId, addrs: seq[MultiAddress]]] {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil:
    return

  var preferred = svc.selectBootstrapCandidates(max(1, svc.config.stopAfterConnected))
  if preferred.len == 0:
    preferred = svc.collectBootstrapCandidates()
    svc.sortBootstrapCandidates(preferred)

  for candidate in preferred:
    if not svc.peerCurrentlyConnected(candidate.peerId):
      continue
    let addrs = svc.authoritativeExactBootstrapAddrs(candidate.peerId)
    if addrs.len > 0:
      return some((candidate.peerId, addrs))

  for peerId in svc.connectedPeerIds():
    let addrs = svc.authoritativeExactBootstrapAddrs(peerId)
    if addrs.len > 0:
      return some((peerId, addrs))

proc hasImportedBootstrapPeers(
    svc: WanBootstrapService
): bool {.gcsafe, raises: [].} =
  if svc.isNil:
    return false
  for candidate in svc.collectBootstrapCandidates():
    if not svc.isAuthoritativePeer(candidate.peerId):
      return true
  false

proc isQuicBootstrapDial(ma: MultiAddress): bool =
  let lower = ($ma).toLowerAscii()
  lower.contains("/quic-v1") or lower.contains("/quic/")

proc bootstrapExactConnectBudget(
    svc: WanBootstrapService, ma: MultiAddress
): Duration =
  if svc.isNil:
    return chronos.seconds(5)
  if isQuicBootstrapDial(ma):
    return min(
      max(svc.config.connectTimeout, chronos.seconds(4)),
      chronos.seconds(5),
    )
  svc.config.connectTimeout

proc bootstrapPostConnectBudget(
    svc: WanBootstrapService
): Duration =
  if svc.isNil:
    return chronos.seconds(5)
  min(
    max(svc.config.connectTimeout, chronos.seconds(3)),
    chronos.seconds(5),
  )

proc peerCurrentlyConnected(
    svc: WanBootstrapService, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if svc.isNil:
    return false
  peerId in svc.connectedPeerIds()

proc awaitConnectFutureOrPeerConnected[T](
    svc: WanBootstrapService,
    fut: Future[T],
    peerId: PeerId,
    timeout: Duration,
): Future[tuple[completed: bool, observedConnected: bool]] {.async: (raises: [CancelledError]).} =
  if fut.isNil:
    return (false, false)
  if fut.finished():
    return (true, false)
  if svc.peerCurrentlyConnected(peerId):
    return (false, true)
  let deadline = Moment.now() + timeout
  while not fut.finished():
    if svc.peerCurrentlyConnected(peerId):
      return (false, true)
    let remaining = deadline - Moment.now()
    if remaining <= chronos.milliseconds(0):
      return (false, false)
    await sleepAsync(min(chronos.milliseconds(50), remaining))
  (true, false)

proc sortBootstrapCandidates(
    svc: WanBootstrapService, candidates: var seq[WanBootstrapCandidate]
) {.gcsafe, raises: [].} =
  let before = candidates.mapIt(peerIdText(it.peerId))
  candidates.sort(proc(a, b: WanBootstrapCandidate): int = compareBootstrapCandidates(svc, a, b))
  let lsmrService =
    if not svc.isNil and not svc.switch.isNil:
      lsmrsvc.getLsmrService(svc.switch)
    else:
      nil
  if not lsmrService.isNil and lsmrService.config.enableBootstrapBias:
    let after = candidates.mapIt(peerIdText(it.peerId))
    if before != after:
      inc lsmrService.biasReorders

proc lsmrSelectionTargetPath(svc: WanBootstrapService): LsmrPath =
  if svc.isNil or svc.switch.isNil:
    return @[]
  let localActive = activeCoordinateRecord(svc.switch, svc.switch.peerInfo.peerId)
  if localActive.isSome():
    return localActive.get().data.certifiedPrefix()
  let localRaw = localCoordinateRecord(svc.switch)
  if localRaw.isSome():
    return localRaw.get().data.certifiedPrefix()
  if svc.config.lsmrConfig.isSome():
    let cfg = svc.config.lsmrConfig.get()
    for anchor in cfg.anchors:
      if anchor.attestedPrefix.len > 0:
        return anchor.attestedPrefix
      if isValidLsmrDigit(anchor.regionDigit):
        return @[anchor.regionDigit]
  @[]

proc selectLsmrBootstrapCandidates(
    svc: WanBootstrapService, limit = 0
): seq[WanBootstrapCandidate] =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  let target = svc.clampBootstrapLimit(limit)
  var candidatesById = initTable[string, WanBootstrapCandidate]()
  for candidate in svc.collectBootstrapCandidates():
    if not svc.switch.peerStore[ActiveLsmrBook].contains(candidate.peerId):
      continue
    var promoted = candidate
    let active = svc.switch.peerStore[ActiveLsmrBook][candidate.peerId]
    promoted.trust = WanBootstrapTrust.trusted
    promoted.lsmrRecordSeen = true
    promoted.lsmrPrefix = active.data.certifiedPrefix()
    promoted.lsmrConfidence = active.data.confidence
    promoted.lsmrWitnessCount = active.data.witnesses.len
    promoted.lsmrExpiresAtMs = active.data.expiresAtMs
    promoted.lsmrDistance =
      if promoted.lsmrPrefix.len > 0:
        lsmrDistance(promoted.lsmrPrefix, svc.lsmrSelectionTargetPath())
      else:
        high(int)
    candidatesById[peerIdText(candidate.peerId)] = promoted
  if candidatesById.len == 0:
    return @[]

  let targetPath = svc.lsmrSelectionTargetPath()
  var orderedPeerIds: seq[PeerId] = @[]
  if targetPath.len > 0:
    orderedPeerIds = svc.switch.peerStore.closestCertifiedPeers(targetPath)
  else:
    let orderedKeys = candidatesById.keys.toSeq().sorted(system.cmp[string])
    for key in orderedKeys:
      let parsed = PeerId.init(key).valueOr:
        continue
      orderedPeerIds.add(parsed)
  for peerId in orderedPeerIds:
    if result.len >= target:
      break
    let key = peerIdText(peerId)
    if not candidatesById.hasKey(key):
      continue
    var selected = candidatesById.getOrDefault(key)
    selected.selectionReason = "lsmr_certified"
    result.add(selected)

proc selectLegacyBootstrapCandidates(
    svc: WanBootstrapService, limit = 0
): seq[WanBootstrapCandidate] =
  if svc.isNil:
    return @[]

  let target = svc.clampBootstrapLimit(limit)
  var ordered = svc.collectBootstrapCandidates()
  if ordered.len == 0:
    return @[]
  sortBootstrapCandidates(svc, ordered)
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

proc selectBootstrapCandidates*(
    svc: WanBootstrapService, limit = 0
): seq[WanBootstrapCandidate] =
  if svc.isNil:
    return @[]

  if svc.config.primaryPlane == PrimaryRoutingPlane.lsmr or
      svc.config.routingMode == RoutingPlaneMode.lsmrOnly:
    result = svc.selectLsmrBootstrapCandidates(limit)
    if result.len > 0:
      return

  result = svc.selectLegacyBootstrapCandidates(limit)

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
  result["lsmrRecordSeen"] = %entry.lsmrRecordSeen
  if entry.lsmrCoordinateRecord.len > 0:
    result["lsmrCoordinateRecord"] = %entry.lsmrCoordinateRecord
  if entry.lsmrMigrationRecords.len > 0:
    result["lsmrMigrationRecords"] = %entry.lsmrMigrationRecords
  if entry.lsmrIsolationEvidence.len > 0:
    result["lsmrIsolationEvidence"] = %entry.lsmrIsolationEvidence

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
  result["lsmrRecordSeen"] = %candidate.lsmrRecordSeen
  if candidate.lsmrPrefix.len > 0:
    var prefixNode = newJArray()
    for digit in candidate.lsmrPrefix:
      prefixNode.add(%int(digit))
    result["lsmrPrefix"] = prefixNode
  result["lsmrConfidence"] = %int(candidate.lsmrConfidence)
  result["lsmrWitnessCount"] = %candidate.lsmrWitnessCount
  result["lsmrExpiresAtMs"] = %candidate.lsmrExpiresAtMs
  if candidate.lsmrDistance != high(int):
    result["lsmrDistance"] = %candidate.lsmrDistance

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
  result["lsmrWitnessRequests"] = %metrics.lsmrWitnessRequests
  result["lsmrWitnessSuccess"] = %metrics.lsmrWitnessSuccess
  result["lsmrWitnessQuorumSuccess"] = %metrics.lsmrWitnessQuorumSuccess
  result["lsmrWitnessQuorumFailure"] = %metrics.lsmrWitnessQuorumFailure
  result["lsmrValidationTrusted"] = %metrics.lsmrValidationTrusted
  result["lsmrValidationQuarantine"] = %metrics.lsmrValidationQuarantine
  result["lsmrValidationRejected"] = %metrics.lsmrValidationRejected
  result["lsmrStaleRecords"] = %metrics.lsmrStaleRecords
  result["lsmrForgedRecords"] = %metrics.lsmrForgedRecords
  result["lsmrBiasReorders"] = %metrics.lsmrBiasReorders

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
          WanBootstrapTrust.quarantine,
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
      lsmrRecordSeen:
        if not node{"lsmrRecordSeen"}.isNil:
          node{"lsmrRecordSeen"}.getBool()
        else:
          false,
      lsmrCoordinateRecord:
        if not node{"lsmrCoordinateRecord"}.isNil:
          node{"lsmrCoordinateRecord"}.getStr()
        else:
          "",
      lsmrMigrationRecords:
        if not node{"lsmrMigrationRecords"}.isNil and node{"lsmrMigrationRecords"}.kind == JArray:
          node{"lsmrMigrationRecords"}.items().toSeq().filterIt(it.kind == JString).mapIt(it.getStr())
        else:
          @[],
      lsmrIsolationEvidence:
        if not node{"lsmrIsolationEvidence"}.isNil and node{"lsmrIsolationEvidence"}.kind == JArray:
          node{"lsmrIsolationEvidence"}.items().toSeq().filterIt(it.kind == JString).mapIt(it.getStr())
        else:
          @[],
    )
  )

proc bootstrapSnapshot*(svc: WanBootstrapService, limit = 0): seq[WanBootstrapSnapshotEntry] =
  if svc.isNil:
    return @[]
  let capped = svc.snapshotLimit(limit)
  var candidates = svc.collectBootstrapCandidates()
  sortBootstrapCandidates(svc, candidates)
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
        lsmrRecordSeen: candidate.lsmrRecordSeen,
        lsmrCoordinateRecord: svc.encodeLsmrCoordinateRecord(candidate.peerId),
        lsmrMigrationRecords: svc.encodeLsmrMigrationRecords(candidate.peerId),
        lsmrIsolationEvidence: svc.encodeLsmrIsolationEvidence(candidate.peerId),
      )
    )

proc bootstrapSnapshot*(switch: Switch, limit = 0): seq[WanBootstrapSnapshotEntry] =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return @[]
  svc.bootstrapSnapshot(limit)

proc routingPlaneStatus*(svc: WanBootstrapService): RoutingPlaneStatus =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return RoutingPlaneStatus()
  result.mode = svc.config.routingMode
  result.primary = svc.config.primaryPlane
  result.shadowMode = svc.config.shadowMode()
  result.legacyCandidates = svc.collectBootstrapCandidates().len
  for candidate in svc.collectBootstrapCandidates():
    if candidate.trust == WanBootstrapTrust.trusted:
      inc result.legacyTrusted
  result.lsmrActiveCertificates = svc.switch.peerStore[ActiveLsmrBook].len
  result.lsmrMigrations = svc.switch.peerStore[LsmrMigrationBook].len
  result.lsmrIsolations = svc.switch.peerStore[LsmrIsolationBook].len

proc routingPlaneStatus*(switch: Switch): RoutingPlaneStatus =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return RoutingPlaneStatus()
  svc.routingPlaneStatus()

proc bootstrapStateSnapshot*(
    svc: WanBootstrapService, candidateLimit = 0
): WanBootstrapStateSnapshot =
  if svc.isNil:
    return WanBootstrapStateSnapshot()
  let connectedPeers = svc.connectedPeerIds()
  var metrics = svc.metricsSnapshot
  let lsmrService = lsmrsvc.getLsmrService(svc.switch)
  if not lsmrService.isNil:
    metrics.lsmrWitnessRequests = lsmrService.witnessRequests
    metrics.lsmrWitnessSuccess = lsmrService.witnessSuccess
    metrics.lsmrWitnessQuorumSuccess = lsmrService.witnessQuorumSuccess
    metrics.lsmrWitnessQuorumFailure = lsmrService.witnessQuorumFailure
    metrics.lsmrValidationTrusted = lsmrService.validationTrusted
    metrics.lsmrValidationQuarantine = lsmrService.validationQuarantine
    metrics.lsmrValidationRejected = lsmrService.validationRejected
    metrics.lsmrStaleRecords = lsmrService.staleRecords
    metrics.lsmrForgedRecords = lsmrService.forgedRecords
    metrics.lsmrBiasReorders = lsmrService.biasReorders
  let selected =
    svc.selectBootstrapCandidates(
      if candidateLimit > 0: candidateLimit else: svc.config.maxBootstrapCandidates
    )
  WanBootstrapStateSnapshot(
    networkId: svc.config.networkId,
    role: svc.config.role,
    routingMode: svc.config.routingMode,
    primaryPlane: svc.config.primaryPlane,
    shadowMode: svc.config.shadowMode(),
    exactSeedDialPolicy: svc.config.exactSeedDialPolicy,
    running: svc.running,
    journalLoaded: svc.journalLoaded,
    journalDirty: svc.journalDirty,
    hintPeerCount: svc.hints.len,
    candidateCount: svc.collectBootstrapCandidates().len,
    connectedPeerCount: connectedPeers.len,
    connectedPeers: connectedPeers,
    selectedCandidates: selected,
    lastRefresh: svc.lastRefreshReport,
    metrics: metrics,
    routingStatus: svc.routingPlaneStatus(),
    lastRendezvousAdvertiseError: svc.lastRendezvousAdvertiseError,
    lastRendezvousRequestError: svc.lastRendezvousRequestError,
    lastSnapshotError: svc.lastSnapshotError,
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
  result["routingMode"] = %($snapshot.routingMode)
  result["primaryPlane"] = %($snapshot.primaryPlane)
  result["shadowMode"] = %snapshot.shadowMode
  result["exactSeedDialPolicy"] = %exactSeedDialPolicyLabel(snapshot.exactSeedDialPolicy)
  result["exactSeedAddrFallbackAllowed"] = %(snapshot.exactSeedDialPolicy != ExactSeedDialPolicy.single)
  result["exactSeedParallelDial"] = %(snapshot.exactSeedDialPolicy == ExactSeedDialPolicy.parallel)
  result["deferAutoJoinUntilRelayReady"] = %svc.config.deferAutoJoinUntilRelayReady
  result["startupRelayReady"] = %svc.startupRelayReady
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
  ## Keep key counters flattened for mobile automation/status readers that still
  ## look at the root bootstrap object instead of the nested metrics payload.
  result["refreshRuns"] = %snapshot.metrics.refreshRuns
  result["dnsaddrRefreshRuns"] = %snapshot.metrics.dnsaddrRefreshRuns
  result["rendezvousAdvertiseRuns"] = %snapshot.metrics.rendezvousAdvertiseRuns
  result["rendezvousRefreshRuns"] = %snapshot.metrics.rendezvousRefreshRuns
  result["snapshotRefreshRuns"] = %snapshot.metrics.snapshotRefreshRuns
  result["dnsaddrImported"] = %snapshot.metrics.dnsaddrImported
  result["rendezvousAdvertised"] = %snapshot.metrics.rendezvousAdvertised
  result["rendezvousImported"] = %snapshot.metrics.rendezvousImported
  result["snapshotImported"] = %snapshot.metrics.snapshotImported
  result["joinRuns"] = %snapshot.metrics.joinRuns
  result["joinAttempts"] = %snapshot.metrics.joinAttempts
  result["joinConnected"] = %snapshot.metrics.joinConnected
  result["joinFailed"] = %snapshot.metrics.joinFailed
  result["lsmrWitnessRequests"] = %snapshot.metrics.lsmrWitnessRequests
  result["lsmrWitnessSuccess"] = %snapshot.metrics.lsmrWitnessSuccess
  result["lsmrWitnessQuorumSuccess"] = %snapshot.metrics.lsmrWitnessQuorumSuccess
  result["lsmrWitnessQuorumFailure"] = %snapshot.metrics.lsmrWitnessQuorumFailure
  result["lsmrValidationTrusted"] = %snapshot.metrics.lsmrValidationTrusted
  result["lsmrValidationQuarantine"] = %snapshot.metrics.lsmrValidationQuarantine
  result["lsmrValidationRejected"] = %snapshot.metrics.lsmrValidationRejected
  result["lsmrStaleRecords"] = %snapshot.metrics.lsmrStaleRecords
  result["lsmrForgedRecords"] = %snapshot.metrics.lsmrForgedRecords
  result["lsmrBiasReorders"] = %snapshot.metrics.lsmrBiasReorders
  if snapshot.lastRendezvousAdvertiseError.len > 0:
    result["lastRendezvousAdvertiseError"] = %snapshot.lastRendezvousAdvertiseError
  if snapshot.lastRendezvousRequestError.len > 0:
    result["lastRendezvousRequestError"] = %snapshot.lastRendezvousRequestError
  if snapshot.lastSnapshotError.len > 0:
    result["lastSnapshotError"] = %snapshot.lastSnapshotError
  result["routingStatus"] = %*{
    "mode": $snapshot.routingStatus.mode,
    "primary": $snapshot.routingStatus.primary,
    "shadowMode": snapshot.routingStatus.shadowMode,
    "legacyCandidates": snapshot.routingStatus.legacyCandidates,
    "legacyTrusted": snapshot.routingStatus.legacyTrusted,
    "lsmrActiveCertificates": snapshot.routingStatus.lsmrActiveCertificates,
    "lsmrIsolations": snapshot.routingStatus.lsmrIsolations,
    "lsmrMigrations": snapshot.routingStatus.lsmrMigrations,
  }
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
          maxTrust(
            svc.trustBuckets.getOrDefault(entry.peerId, WanBootstrapTrust.quarantine),
            entry.trust,
          )
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
    if svc.registerBootstrapHint(
        peerId, addrs, source = "dnsaddr", trust = WanBootstrapTrust.quarantine
    ):
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
  var seen = initHashSet[string]()
  for authoritativePeerId in svc.config.authoritativePeerIds:
    let normalized = authoritativePeerId.strip()
    if normalized.len == 0 or seen.contains(normalized):
      continue
    let parsed = PeerId.init(normalized).valueOr:
      continue
    seen.incl(normalized)
    result.add(parsed)
  let protoBook = svc.switch.peerStore[ProtoBook]
  for peerId in svc.connectedPeerIds():
    let text = peerIdText(peerId)
    if seen.contains(text):
      continue
    let protos = protoBook[peerId]
    if protos.len == 0 or supportsRendezvousCodecs(protos) or svc.isAuthoritativePeer(peerId):
      seen.incl(text)
      result.add(peerId)

proc supportsSnapshot(svc: WanBootstrapService, peerId: PeerId): bool =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return false
  if svc.isAuthoritativePeer(peerId):
    return true
  let protos = svc.switch.peerStore[ProtoBook][peerId]
  protos.len == 0 or svc.config.snapshotCodec in protos

proc defaultJoinResult(): WanBootstrapJoinResult {.inline.} =
  WanBootstrapJoinResult(timestampMs: nowMillis())

proc completedJoinResultFuture(
    joinResult: WanBootstrapJoinResult
): Future[WanBootstrapJoinResult].Raising([CancelledError]) =
  let fut = Future[WanBootstrapJoinResult].Raising([CancelledError]).init(
    "wanbootstrap.waitOngoingJoin.ready"
  )
  fut.complete(joinResult)
  fut

proc waitOngoingJoin(
    svc: WanBootstrapService
): Future[WanBootstrapJoinResult].Raising([CancelledError]) {.raises: [].} =
  if svc.isNil:
    return completedJoinResultFuture(defaultJoinResult())
  if not svc.joinFuture.isNil and not svc.joinFuture.finished():
    return svc.joinFuture
  if svc.hasLastJoinResult:
    return completedJoinResultFuture(svc.lastJoinResult)
  completedJoinResultFuture(defaultJoinResult())

proc advertiseRendezvousNamespaces*(
    svc: WanBootstrapService
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil or not svc.config.enableRendezvous or svc.rendezvous.isNil:
    return 0
  let peers = svc.rendezvousPeers()
  if peers.len == 0:
    return 0
  svc.lastRendezvousAdvertiseError = ""
  for namespace in svc.config.rendezvousNamespaces():
    try:
      await svc.rendezvous.advertise(namespace, svc.config.rendezvousAdvertiseTtl, peers)
      inc result
    except CatchableError as exc:
      svc.lastRendezvousAdvertiseError = namespace & ": " & exc.msg
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
  let timeout = svc.bootstrapPostConnectBudget()
  svc.lastRendezvousRequestError = ""
  for namespace in svc.config.rendezvousNamespaces():
    try:
      let records =
        await svc.rendezvous.request(
          Opt.some(namespace),
          svc.config.snapshotLimit,
          peers,
        ).wait(timeout)
      for record in records:
        let addrs = record.peerRecordAddresses()
        if record.peerId == svc.switch.peerInfo.peerId or addrs.len == 0:
          continue
        let imported = svc.registerBootstrapHint(
          record.peerId,
          addrs,
          source = "rendezvous",
          trust = WanBootstrapTrust.trusted,
        )
        if imported:
          inc result
          discard svc.markBootstrapSeen(record.peerId, trust = WanBootstrapTrust.trusted)
      if records.len > 0:
        debug "rendezvous namespace refreshed", namespace = namespace, discovered = records.len
    except AsyncTimeoutError:
      svc.lastRendezvousRequestError =
        namespace & ": timeout after " & $timeout.milliseconds.int & "ms"
      debug "rendezvous namespace refresh timed out",
        namespace = namespace,
        timeoutMs = timeout.milliseconds.int
    except CatchableError as exc:
      svc.lastRendezvousRequestError = namespace & ": " & exc.msg
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
  result["version"] = %2
  result["networkId"] = %svc.config.networkId
  result["role"] = %roleLabel(svc.config.role)
  result["routingMode"] = %($svc.config.routingMode)
  result["primaryPlane"] = %($svc.config.primaryPlane)
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
  let timeout = svc.bootstrapPostConnectBudget()
  svc.lastSnapshotError = ""
  try:
    let conn = await svc.switch.dial(peerId, @[svc.config.snapshotCodec]).wait(timeout)
    defer:
      await conn.close()
    let req = %*{"op": "snapshot", "limit": svc.snapshotLimit(limit)}
    await conn.writeLp(stringToBytes($req)).wait(timeout)
    let payload = await conn.readLp(4 * 1024 * 1024).wait(timeout)
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
  except AsyncTimeoutError:
    svc.lastSnapshotError =
      peerIdText(peerId) & ": timeout after " & $timeout.milliseconds.int & "ms"
    debug "snapshot request timed out",
      peerId = peerId,
      timeoutMs = timeout.milliseconds.int
    return @[]
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    svc.lastSnapshotError = peerIdText(peerId) & ": " & exc.msg
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
      decodeLsmrCoordinateRecord(entry.lsmrCoordinateRecord).withValue(record):
        if not svc.switch.peerStore.isNil:
          svc.switch.peerStore[LsmrBook][entry.peerId] = record
          let lsmrService = lsmrsvc.getLsmrService(svc.switch)
          if not lsmrService.isNil:
            discard lsmrsvc.promoteValidatedCoordinateRecord(
              svc.switch, lsmrService.config, entry.peerId
            )
      for encoded in entry.lsmrMigrationRecords:
        decodeLsmrMigrationRecord(encoded).withValue(record):
          discard lsmrsvc.installMigrationRecord(svc.switch, record)
      for encoded in entry.lsmrIsolationEvidence:
        decodeLsmrIsolationEvidence(encoded).withValue(evidence):
          discard lsmrsvc.installIsolationEvidence(svc.switch, evidence)
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
          maxTrust(
            svc.trustBuckets.getOrDefault(entry.peerId, WanBootstrapTrust.quarantine),
            entry.trust,
          )
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
  let lsmrService = lsmrsvc.getLsmrService(svc.switch)
  if not lsmrService.isNil and svc.config.lsmrDiscoveryActive():
    discard await lsmrService.refreshCoordinateRecord()
  if not svc.journalLoaded:
    discard svc.loadBootstrapJournal()
  if not svc.switch.isNil and svc.connectedPeerIds().len == 0 and svc.config.enableDnsaddrFallback and
      svc.config.legacyDiscoveryActive():
    report.dnsaddrImported = await svc.refreshFromDnsaddrFallback()
    result += report.dnsaddrImported
  if svc.config.legacyDiscoveryActive() and svc.rendezvousPeers().len > 0:
    report.rendezvousAdvertised = await svc.advertiseRendezvousNamespaces()
    result += report.rendezvousAdvertised
    report.rendezvousImported = await svc.refreshFromRendezvousNamespaces()
    result += report.rendezvousImported
  if svc.connectedPeerIds().len > 0:
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
): Future[tuple[accepted: bool, trust: WanBootstrapTrust]] {.
    async: (raises: [CancelledError])
.} =
  if svc.isNil:
    return (false, WanBootstrapTrust.rejected)
  if svc.config.attestPeer.isNil:
    return (true, WanBootstrapTrust.trusted)
  try:
    warn "wan bootstrap attestation begin", peerId = candidate.peerId, stage = stage
    let trust = await svc.config.attestPeer(
      svc.switch,
      candidate.peerId,
      candidate.addrs,
      candidate.sources,
      candidate.trust,
      stage,
    )
    warn "wan bootstrap attestation completed", peerId = candidate.peerId, stage = stage, trust = trust
    svc.recordAttestationResult(trust)
    if trust == WanBootstrapTrust.rejected:
      await svc.switch.disconnect(candidate.peerId)
      return (false, WanBootstrapTrust.rejected)
    return (true, trust)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "wan bootstrap attestation failed", peerId = candidate.peerId, stage = stage, err = exc.msg
    svc.recordAttestationResult(WanBootstrapTrust.quarantine, failed = true)
    return (true, WanBootstrapTrust.quarantine)

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
  let timeout = svc.bootstrapPostConnectBudget()
  var successful = 0
  for kad in kads:
    try:
      await kad.bootstrap(peers).wait(timeout)
      inc successful
    except AsyncTimeoutError:
      debug "wan bootstrap kademlia bootstrap timed out",
        timeoutMs = timeout.milliseconds.int,
        peerCount = peers.len
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "wan bootstrap kademlia bootstrap failed", err = exc.msg
  svc.recordKadBootstrapResult(kads.len, successful == kads.len)
  successful

proc finalizeBootstrapConnectedCandidate(
    svc: WanBootstrapService,
    candidate: WanBootstrapCandidate,
    stage: string,
    attemptedAddr: string,
): Future[WanBootstrapDialOutcome] {.async: (raises: [CancelledError]).} =
  warn "wan bootstrap finalize connected candidate begin",
    peerId = candidate.peerId,
    stage = stage,
    attemptedAddr = attemptedAddr
  let provisionalTrust =
    if svc.config.attestPeer.isNil:
      WanBootstrapTrust.trusted
    else:
      maxTrust(candidate.trust, WanBootstrapTrust.quarantine)
  svc.trustBuckets[candidate.peerId] = maxTrust(
    svc.trustBuckets.getOrDefault(candidate.peerId, WanBootstrapTrust.quarantine),
    provisionalTrust,
  )
  discard svc.markBootstrapSeen(candidate.peerId, trust = provisionalTrust)
  let acceptedResult =
    try:
      await svc.attestConnectedPeer(candidate, stage)
    except CatchableError as exc:
      return WanBootstrapDialOutcome(
        connected: false,
        attemptedAddr: attemptedAddr,
        error: if exc.msg.len > 0: exc.msg else: $exc.name,
      )
  let (accepted, attestedTrust) = acceptedResult
  if not accepted:
    svc.trustBuckets[candidate.peerId] = WanBootstrapTrust.rejected
    discard svc.markBootstrapSeen(candidate.peerId, trust = WanBootstrapTrust.rejected)
    warn "wan bootstrap finalize connected candidate rejected",
      peerId = candidate.peerId,
      stage = stage,
      attemptedAddr = attemptedAddr
    return WanBootstrapDialOutcome(
      connected: false,
      attemptedAddr: attemptedAddr,
      error: "attestation_rejected",
    )
  svc.trustBuckets[candidate.peerId] = maxTrust(
    svc.trustBuckets.getOrDefault(candidate.peerId, WanBootstrapTrust.quarantine),
    attestedTrust,
  )
  discard svc.markBootstrapSeen(candidate.peerId, trust = attestedTrust)
  warn "wan bootstrap finalize connected candidate completed",
    peerId = candidate.peerId,
    stage = stage,
    attemptedAddr = attemptedAddr,
    trust = attestedTrust
  WanBootstrapDialOutcome(connected: true, attemptedAddr: attemptedAddr)

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
    let waitRes = await svc.awaitConnectFutureOrPeerConnected(
      fut,
      candidate.peerId,
      svc.bootstrapExactConnectBudget(addr),
    )
    if not waitRes.completed and not waitRes.observedConnected:
      fut.cancelSoon()
      lastError = "timeout"
      continue
    if waitRes.observedConnected:
      return await svc.finalizeBootstrapConnectedCandidate(candidate, stage, attemptedAddr)
    try:
      await fut
      return await svc.finalizeBootstrapConnectedCandidate(candidate, stage, attemptedAddr)
    except CatchableError as exc:
      lastError = if exc.msg.len > 0: exc.msg else: $exc.name
  WanBootstrapDialOutcome(
    connected: false,
    attemptedAddr: attemptedAddr,
    error: lastError,
  )

proc dialBootstrapCandidateExactAddr(
    svc: WanBootstrapService,
    candidate: WanBootstrapCandidate,
    stage: string,
    dialAddr: MultiAddress,
): Future[WanBootstrapDialOutcome] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return WanBootstrapDialOutcome(connected: false, error: "service_unavailable")
  let attemptedAddr = $dialAddr
  let timeout = svc.bootstrapExactConnectBudget(dialAddr)
  let quicDial = isQuicBootstrapDial(dialAddr)
  if quicDial and svc.peerCurrentlyConnected(candidate.peerId):
    try:
      await svc.switch.disconnect(candidate.peerId)
    except CatchableError:
      discard
  var connectAddr = dialAddr
  if isExactPeerMultiaddr(dialAddr):
    let extracted = extractSeedPeer(dialAddr)
    if extracted.isNone:
      return WanBootstrapDialOutcome(
        connected: false,
        attemptedAddr: attemptedAddr,
        error: "invalid_exact_multiaddr",
      )
    let (resolvedPeerId, baseAddr) = extracted.get()
    if resolvedPeerId != candidate.peerId:
      return WanBootstrapDialOutcome(
        connected: false,
        attemptedAddr: attemptedAddr,
        error: "peer_id_mismatch",
      )
    connectAddr = baseAddr
  var hadOriginalAddrs = false
  var originalAddrs: seq[MultiAddress] = @[]
  let peerStore = svc.switch.peerStore
  if not peerStore.isNil:
    try:
      originalAddrs = peerStore[AddressBook][candidate.peerId]
      hadOriginalAddrs = peerStore[AddressBook].contains(candidate.peerId)
      peerStore.setAddresses(candidate.peerId, @[connectAddr])
    except CatchableError:
      hadOriginalAddrs = false
      originalAddrs = @[]
  let fut = svc.switch.connect(candidate.peerId, @[connectAddr], forceDial = true)
  let waitRes = await svc.awaitConnectFutureOrPeerConnected(
    fut,
    candidate.peerId,
    timeout,
  )
  warn "wan bootstrap exact dial wait completed",
    peerId = candidate.peerId,
    stage = stage,
    attemptedAddr = attemptedAddr,
    completed = waitRes.completed,
    observedConnected = waitRes.observedConnected,
    timeoutMs = timeout.milliseconds.int
  if not peerStore.isNil:
    try:
      if hadOriginalAddrs:
        peerStore.setAddresses(candidate.peerId, originalAddrs)
      else:
        discard peerStore[AddressBook].del(candidate.peerId)
    except CatchableError:
      discard
  if not waitRes.completed and not waitRes.observedConnected:
    fut.cancelSoon()
    return WanBootstrapDialOutcome(
      connected: false,
      attemptedAddr: attemptedAddr,
      error: "timeout",
    )
  if waitRes.observedConnected:
    return await svc.finalizeBootstrapConnectedCandidate(candidate, stage, attemptedAddr)
  try:
    await fut
    return await svc.finalizeBootstrapConnectedCandidate(candidate, stage, attemptedAddr)
  except CatchableError as exc:
    WanBootstrapDialOutcome(
      connected: false,
      attemptedAddr: attemptedAddr,
      error: if exc.msg.len > 0: exc.msg else: $exc.name,
    )

proc connectBootstrapCandidateExact(
    svc: WanBootstrapService, candidate: WanBootstrapCandidate, stage: string
): Future[WanBootstrapDialOutcome] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or candidate.addrs.len == 0:
    return WanBootstrapDialOutcome(connected: false, error: "no_candidate_addrs")
  let policy = svc.config.exactSeedDialPolicy
  let ordered = selectExactBootstrapDialAddrs(candidate.addrs, policy)
  if ordered.len == 0:
    return WanBootstrapDialOutcome(connected: false, error: "no_candidate_addrs")
  if policy == ExactSeedDialPolicy.parallel and ordered.len > 1 and ordered.allIt(isExactPeerMultiaddr(it)):
    var futs: seq[Future[WanBootstrapDialOutcome]] = @[]
    var lastOutcome = WanBootstrapDialOutcome(
      connected: false,
      attemptedAddr: $ordered[0],
      error: "all_attempts_failed",
    )
    try:
      for addr in ordered:
        futs.add(svc.dialBootstrapCandidateExactAddr(candidate, stage, addr))
      while true:
        var finishedCount = 0
        for fut in futs:
          if fut.isNil or not fut.finished():
            continue
          inc finishedCount
          let outcome =
            try:
              fut.read()
            except CatchableError as exc:
              WanBootstrapDialOutcome(
                connected: false,
                error: if exc.msg.len > 0: exc.msg else: $exc.name,
              )
          if outcome.attemptedAddr.len > 0 or outcome.error.len > 0:
            lastOutcome = outcome
          if outcome.connected:
            return outcome
        if finishedCount >= futs.len:
          return lastOutcome
        await sleepAsync(chronos.milliseconds(50))
    finally:
      for fut in futs:
        if not fut.isNil and not fut.finished():
          fut.cancelSoon()
  var lastOutcome = WanBootstrapDialOutcome(connected: false, error: "all_attempts_failed")
  for addr in ordered:
    warn "wan bootstrap exact dial begin",
      peerId = candidate.peerId,
      stage = stage,
      attemptedAddr = $addr
    lastOutcome = await svc.dialBootstrapCandidateExactAddr(candidate, stage, addr)
    warn "wan bootstrap exact dial completed",
      peerId = candidate.peerId,
      stage = stage,
      attemptedAddr = $addr,
      connected = lastOutcome.connected,
      error = lastOutcome.error
    if lastOutcome.connected:
      return lastOutcome
  lastOutcome

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

proc joinViaSeedBootstrapImpl(
    svc: WanBootstrapService,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "seeded_join",
    syncConnectedState = true,
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
  discard svc.registerBootstrapHint(
    peerId, normalized, effectiveSource, WanBootstrapTrust.quarantine
  )

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

  warn "wan bootstrap seeded exact connect begin",
    peerId = peerId,
    source = effectiveSource,
    addrCount = normalized.len,
    syncConnectedState = syncConnectedState
  let outcome = await svc.connectBootstrapCandidateExact(candidate, "seeded_direct_hint")
  warn "wan bootstrap seeded exact connect completed",
    peerId = peerId,
    source = effectiveSource,
    connected = outcome.connected,
    attemptedAddr = outcome.attemptedAddr,
    error = outcome.error
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

  if syncConnectedState and svc.connectedPeerIds().len > 0:
    discard await svc.advertiseRendezvousNamespaces()
    discard await svc.refreshFromRendezvousNamespaces()
    discard await svc.refreshFromBootstrapSnapshots()

  if result.ok and syncConnectedState:
    discard await svc.bootstrapMountedKadDHTs()

  svc.recordJoinResult(result)
  if result.ok and svc.journalDirty:
    discard svc.saveBootstrapJournal()
  svc.updateObservability()

proc promoteConnectedBootstrapPeerImpl(
    svc: WanBootstrapService,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "direct_exact_connect",
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
  if not svc.peerCurrentlyConnected(peerId):
    return

  var normalized = dedupeAddrs(addrs)
  if not svc.config.enableDnsaddrFallback:
    normalized.keepItIf(not isLegacyPublicBootstrapAddr(it))
  if normalized.len == 0:
    return

  let hintSource = source.strip()
  let effectiveSource =
    if hintSource.len > 0:
      hintSource
    else:
      "direct_exact_connect"
  discard svc.registerBootstrapHint(
    peerId, normalized, effectiveSource, WanBootstrapTrust.quarantine
  )

  let signedPeerRecordSeen = svc.hasSignedPeerRecord(peerId)
  let candidateSources = @["Connected", "Hints", "Hint:" & effectiveSource]
  let candidate = WanBootstrapCandidate(
    peerId: peerId,
    addrs: normalized,
    lastSeenAtMs: nowMillis(),
    sources: candidateSources,
    selectionReason: "direct_exact_connect",
    trust: svc.candidateTrust(peerId, candidateSources, signedPeerRecordSeen),
    relayCapable: relayCapableFromAddrs(normalized),
    signedPeerRecordSeen: signedPeerRecordSeen,
  )

  let attemptedAddr = if normalized.len > 0: $normalized[0] else: ""
  let outcome = await svc.finalizeBootstrapConnectedCandidate(
    candidate,
    "direct_exact_connect",
    attemptedAddr,
  )
  if not outcome.connected:
    result.attempts.add(
      WanBootstrapJoinAttempt(
        stage: "direct_exact_connect",
        candidate: candidate,
        connected: false,
        attemptedAddr: attemptedAddr,
        error: outcome.error,
      )
    )
    result.attemptedCount = 1
    svc.recordJoinResult(result)
    svc.updateObservability()
    return

  discard await svc.advertiseRendezvousNamespaces()
  discard await svc.refreshFromRendezvousNamespaces()
  discard await svc.refreshFromBootstrapSnapshots()
  discard await svc.bootstrapMountedKadDHTs()

  result = WanBootstrapJoinResult(
    attempts: @[
      WanBootstrapJoinAttempt(
        stage: "direct_exact_connect",
        candidate: candidate,
        connected: true,
        attemptedAddr: attemptedAddr,
        error: "",
      )
    ],
    attemptedCount: 1,
    connectedCount: 1,
    ok: true,
    timestampMs: nowMillis(),
  )
  svc.recordJoinResult(result)
  if svc.journalDirty:
    discard svc.saveBootstrapJournal()
  svc.updateObservability()

proc promoteConnectedBootstrapPeer*(
    svc: WanBootstrapService,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "direct_exact_connect",
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return defaultJoinResult()
  if svc.hasLastJoinResult and svc.lastJoinResult.ok and svc.hasImportedBootstrapPeers():
    return svc.lastJoinResult
  if svc.joinInProgress:
    return await svc.waitOngoingJoin()
  let joinFuture = cast[Future[WanBootstrapJoinResult].Raising([CancelledError])](
    svc.promoteConnectedBootstrapPeerImpl(peerId, addrs, source)
  )
  svc.joinInProgress = true
  svc.joinFuture = joinFuture
  try:
    return await joinFuture
  finally:
    if svc.joinFuture == joinFuture:
      svc.joinFuture = nil
    svc.joinInProgress = false

proc recordConnectedBootstrapPeer*(
    svc: WanBootstrapService,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "direct_exact_connect",
): Future[bool] {.async: (raises: [CancelledError]).} =
  (await svc.promoteConnectedBootstrapPeer(peerId, addrs, source)).ok

proc joinViaSeedBootstrap*(
    svc: WanBootstrapService,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "seeded_join",
    syncConnectedState = true,
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return WanBootstrapJoinResult(timestampMs: nowMillis())
  if svc.connectedPeerIds().len >= svc.config.stopAfterConnected and svc.hasLastJoinResult and svc.lastJoinResult.ok:
    return svc.lastJoinResult
  if svc.joinInProgress:
    return await svc.waitOngoingJoin()
  let joinFuture = cast[Future[WanBootstrapJoinResult].Raising([CancelledError])](
    svc.joinViaSeedBootstrapImpl(
      peerId,
      addrs,
      source,
      syncConnectedState = syncConnectedState,
    )
  )
  svc.joinInProgress = true
  svc.joinFuture = joinFuture
  try:
    return await joinFuture
  finally:
    if svc.joinFuture == joinFuture:
      svc.joinFuture = nil
    svc.joinInProgress = false

proc joinViaSeedBootstrap*(
    switch: Switch,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    source = "seeded_join",
    syncConnectedState = true,
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return WanBootstrapJoinResult(timestampMs: nowMillis())
  await svc.joinViaSeedBootstrap(
    peerId,
    addrs,
    source,
    syncConnectedState = syncConnectedState,
  )

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

proc joinViaBootstrapImpl(
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

  if svc.rendezvousPeers().len > 0:
    discard await svc.advertiseRendezvousNamespaces()
    discard await svc.refreshFromRendezvousNamespaces()
    if result.connectedCount < svc.config.stopAfterConnected:
      appendAttempts(await svc.runBootstrapStage("rendezvous", attemptedAddrCounts, limit))
  if svc.connectedPeerIds().len > 0:
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

proc joinViaBootstrap*(
    svc: WanBootstrapService, limit = 0, allowFallback = true
): Future[WanBootstrapJoinResult] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return WanBootstrapJoinResult(timestampMs: nowMillis())
  if svc.connectedPeerIds().len >= svc.config.stopAfterConnected and svc.hasLastJoinResult and svc.lastJoinResult.ok:
    return svc.lastJoinResult
  if svc.joinInProgress:
    return await svc.waitOngoingJoin()
  let joinFuture = cast[Future[WanBootstrapJoinResult].Raising([CancelledError])](
    svc.joinViaBootstrapImpl(limit, allowFallback)
  )
  svc.joinInProgress = true
  svc.joinFuture = joinFuture
  try:
    return await joinFuture
  finally:
    if svc.joinFuture == joinFuture:
      svc.joinFuture = nil
    svc.joinInProgress = false

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

proc bootstrapAutoJoinReady*(svc: WanBootstrapService): bool {.gcsafe, raises: [].} =
  if svc.isNil:
    return true
  (not svc.config.deferAutoJoinUntilRelayReady) or svc.startupRelayReady

proc bootstrapAutoJoinReady*(switch: Switch): bool {.gcsafe, raises: [].} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return true
  svc.bootstrapAutoJoinReady()

proc markBootstrapAutoJoinReady*(svc: WanBootstrapService) {.gcsafe, raises: [].} =
  if svc.isNil:
    return
  svc.startupRelayReady = true

proc markBootstrapAutoJoinReady*(switch: Switch) {.gcsafe, raises: [].} =
  let svc = getWanBootstrapService(switch)
  if svc.isNil:
    return
  svc.markBootstrapAutoJoinReady()

proc bootstrapRoleConfig*(
    role: WanBootstrapRole,
    networkId = "default",
    journalPath = "",
    authoritativePeerIds: seq[string] = @[],
    deferAutoJoinUntilRelayReady = false,
    fallbackDnsAddrs: seq[MultiAddress] = @[],
): WanBootstrapConfig =
  case role
  of anchor:
    WanBootstrapConfig.init(
      networkId = networkId,
      role = role,
      journalPath = journalPath,
      authoritativePeerIds = authoritativePeerIds,
      deferAutoJoinUntilRelayReady = deferAutoJoinUntilRelayReady,
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
      authoritativePeerIds = authoritativePeerIds,
      deferAutoJoinUntilRelayReady = deferAutoJoinUntilRelayReady,
      fallbackDnsAddrs = fallbackDnsAddrs,
      maxConcurrentBootstrapDials = 3,
    )
  of mobileEdge:
    WanBootstrapConfig.init(
      networkId = networkId,
      role = role,
      journalPath = journalPath,
      authoritativePeerIds = authoritativePeerIds,
      deferAutoJoinUntilRelayReady = deferAutoJoinUntilRelayReady,
      fallbackDnsAddrs = fallbackDnsAddrs,
      maxConcurrentBootstrapDials = 2,
      stopAfterConnected = 1,
      connectTimeout = 3.seconds,
    )

proc bootstrapDualStackRoleConfig*(
    role: WanBootstrapRole,
    lsmrConfig: LsmrConfig,
    networkId = "default",
    journalPath = "",
    authoritativePeerIds: seq[string] = @[],
    deferAutoJoinUntilRelayReady = false,
    fallbackDnsAddrs: seq[MultiAddress] = @[],
    primaryPlane = PrimaryRoutingPlane.legacy,
): WanBootstrapConfig =
  var cfg = bootstrapRoleConfig(
    role = role,
    networkId = networkId,
    journalPath = journalPath,
    authoritativePeerIds = authoritativePeerIds,
    deferAutoJoinUntilRelayReady = deferAutoJoinUntilRelayReady,
    fallbackDnsAddrs = fallbackDnsAddrs,
  )
  cfg.routingMode = RoutingPlaneMode.dualStack
  cfg.primaryPlane = primaryPlane
  cfg.lsmrConfig = some(lsmrConfig)
  cfg.legacyDiscoveryEnabled = true
  cfg.legacyCompatibilityOnly = primaryPlane == PrimaryRoutingPlane.lsmr
  cfg.enableRendezvous = cfg.legacyDiscoveryActive() and cfg.enableRendezvous
  cfg.mountRendezvousProtocol = cfg.legacyDiscoveryActive() and cfg.mountRendezvousProtocol
  cfg.enableDnsaddrFallback = cfg.legacyDiscoveryActive() and cfg.enableDnsaddrFallback
  cfg.enableKadAfterJoin = cfg.legacyDiscoveryActive() and cfg.enableKadAfterJoin
  cfg

proc syncLoop(svc: WanBootstrapService) {.async: (raises: [CancelledError]).} =
  const allowMobileEdgeAutoJoin = true
  while svc.running:
    try:
      if svc.joinInProgress:
        await sleepAsync(250.milliseconds)
        continue
      discard await svc.refreshBootstrapState()
      if
        allowMobileEdgeAutoJoin and
        svc.config.role == mobileEdge and
        svc.config.deferAutoJoinUntilRelayReady and
        not svc.startupRelayReady:
        debug "wan bootstrap auto join deferred until relay ready"
      if
        allowMobileEdgeAutoJoin and
        svc.config.role == mobileEdge and
        svc.bootstrapAutoJoinReady() and
        not svc.joinInProgress:
        if svc.bootstrapAutoJoinNeedsConnectedSync():
          let connectedTarget = svc.chooseConnectedBootstrapPeer()
          if connectedTarget.isSome():
            let target = connectedTarget.get()
            discard await svc.promoteConnectedBootstrapPeer(
              target.peerId,
              target.addrs,
              "auto_connected_bootstrap_seed",
            )
            continue
        let visibleCandidates = svc.selectBootstrapCandidates(1)
        if visibleCandidates.len > 0:
          let selected = visibleCandidates[0]
          if svc.peerCurrentlyConnected(selected.peerId):
            let exactAddrs = svc.authoritativeExactBootstrapAddrs(selected.peerId)
            if exactAddrs.len > 0:
              discard await svc.promoteConnectedBootstrapPeer(
                selected.peerId,
                exactAddrs,
                "auto_connected_bootstrap_seed",
              )
          elif svc.isAuthoritativePeer(selected.peerId):
            let exactAddrs = svc.authoritativeExactBootstrapAddrs(selected.peerId)
            if exactAddrs.len > 0:
              discard await svc.joinViaSeedBootstrap(
                selected.peerId,
                exactAddrs,
                "auto_authoritative_seed",
              )
            else:
              discard await svc.joinViaBootstrap(allowFallback = false)
          else:
            discard await svc.joinViaBootstrap(allowFallback = false)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "wan bootstrap refresh loop failed", err = exc.msg
    let connectedPeers = svc.connectedPeerIds()
    let mobileFastFollowup =
      svc.config.role == mobileEdge and (
        connectedPeers.len == 0 or not svc.hasImportedBootstrapPeers()
      )
    let sleepFor =
      if mobileFastFollowup:
        5.seconds
      else:
        svc.config.rendezvousRefreshInterval
    await sleepAsync(sleepFor)

proc configureManagedRendezvous(svc: WanBootstrapService) =
  if svc.isNil or not svc.config.enableRendezvous or not svc.config.legacyDiscoveryActive():
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
      let connectedTrust =
        if svc.config.attestPeer.isNil:
          WanBootstrapTrust.trusted
        else:
          WanBootstrapTrust.quarantine
      discard svc.markBootstrapSeen(peerId, trust = connectedTrust)
      if
        event.kind == ConnEventKind.Connected and
        svc.config.role == mobileEdge and
        svc.bootstrapAutoJoinReady() and
        svc.bootstrapAutoJoinNeedsConnectedSync()
      :
        let addrs = svc.authoritativeExactBootstrapAddrs(peerId)
        if addrs.len > 0:
          proc promoteConnectedSeed() {.async: (raises: [CancelledError]).} =
            discard await svc.promoteConnectedBootstrapPeer(
              peerId,
              addrs,
              "connected_bootstrap_seed",
            )
          asyncSpawn(promoteConnectedSeed())
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
