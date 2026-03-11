{.push raises: [].}

import std/[algorithm, options, sequtils, strutils, tables]
from std/times import epochTime

import chronos, chronicles

import ../multiaddress
import ../peerid
import ../peerstore
import ../switch
import ../utils/heartbeat

const
  DefaultMobileCandidateLimit* = 16

logScope:
  topics = "libp2p mobilemesh"

type
  MobileDiscoveryMedium* {.pure.} = enum
    mdmBleProximity
    mdmMdnsFallback
    mdmWifiAware
    mdmAwdl
    mdmNearLink
    mdmPushWake

  MobilePeerWakeState* {.pure.} = enum
    mpwsUnknown
    mpwsActive
    mpwsHibernated

  PhantomPeerState* {.pure.} = enum
    ppsUnknown
    ppsActive
    ppsMigrating
    ppsHibernated
    ppsWaking

  MobilePeerSighting* = object
    peerId*: PeerId
    medium*: MobileDiscoveryMedium
    addrs*: seq[MultiAddress]
    observedAtMs*: int64
    proximityScore*: int
    note*: string

  MobileWakeToken* = object
    peerId*: PeerId
    provider*: string
    token*: string
    updatedAtMs*: int64
    expiresAtMs*: int64

  PhantomPeerSnapshot* = object
    peerId*: PeerId
    state*: PhantomPeerState
    updatedAtMs*: int64
    lastMedium*: MobileDiscoveryMedium

  TransportHealthSnapshot* = object
    state*: PhantomPeerState
    activeMedium*: MobileDiscoveryMedium
    lastKnownMedium*: MobileDiscoveryMedium
    directQuicOnly*: bool
    connectionMigrationEnabled*: bool
    lastUpdatedAtMs*: int64
    note*: string

  MobileDialHint* = object
    peerId*: PeerId
    preferredAddrs*: seq[MultiAddress]
    medium*: MobileDiscoveryMedium
    wakeState*: MobilePeerWakeState
    phantomState*: PhantomPeerState
    proximityScore*: int
    note*: string

  MobileDiscoverySnapshot* = object
    updatedAtMs*: int64
    nearfield*: seq[MobilePeerSighting]
    wakeTokens*: seq[MobileWakeToken]
    phantomPeers*: seq[PhantomPeerSnapshot]
    dialHints*: seq[MobileDialHint]
    transportHealth*: TransportHealthSnapshot

  MobileMeshServiceConfig* = object
    cleanupInterval*: Duration
    sightingTtl*: Duration
    wakeTokenTtl*: Duration
    peerStoreTtl*: Duration
    maxSightings*: int
    maxWakeTokens*: int

  MobileMeshService* = ref object of Service
    switch*: Switch
    config*: MobileMeshServiceConfig
    sightings: Table[PeerId, MobilePeerSighting]
    wakeTokens: Table[PeerId, MobileWakeToken]
    wakeStates: Table[PeerId, MobilePeerWakeState]
    phantomStates: Table[PeerId, PhantomPeerSnapshot]
    transportHealthState: TransportHealthSnapshot
    runner: Future[void]
    running: bool

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc durationToMillis(value: Duration): int64 =
  value.nanoseconds div 1_000_000

proc peerIdText(peerId: PeerId): string =
  $peerId

proc mediumPriority(medium: MobileDiscoveryMedium): int =
  case medium
  of mdmNearLink:
    5
  of mdmWifiAware, mdmAwdl:
    4
  of mdmBleProximity:
    3
  of mdmMdnsFallback:
    1
  of mdmPushWake:
    0

proc phantomPriority(state: PhantomPeerState): int =
  case state
  of ppsWaking:
    5
  of ppsActive:
    4
  of ppsMigrating:
    3
  of ppsHibernated:
    2
  of ppsUnknown:
    0

proc wakeStateFromPhantom(state: PhantomPeerState): MobilePeerWakeState =
  case state
  of ppsActive, ppsMigrating, ppsWaking:
    mpwsActive
  of ppsHibernated:
    mpwsHibernated
  of ppsUnknown:
    mpwsUnknown

proc phantomFromWakeState(state: MobilePeerWakeState): PhantomPeerState =
  case state
  of mpwsActive:
    ppsActive
  of mpwsHibernated:
    ppsHibernated
  of mpwsUnknown:
    ppsUnknown

proc isValidPeerId(peerId: PeerId): bool =
  peerIdText(peerId).len > 0

proc dedupeAddrs(addrs: openArray[MultiAddress]): seq[MultiAddress] =
  for addr in addrs:
    var duplicate = false
    for existing in result:
      if existing == addr:
        duplicate = true
        break
    if not duplicate:
      result.add(addr)

proc init*(
    _: type MobileMeshServiceConfig,
    cleanupInterval = 30.seconds,
    sightingTtl = 10.minutes,
    wakeTokenTtl = chronos.hours(24 * 7),
    peerStoreTtl = 30.minutes,
    maxSightings = 1024,
    maxWakeTokens = 1024,
): MobileMeshServiceConfig =
  MobileMeshServiceConfig(
    cleanupInterval:
      if cleanupInterval <= ZeroDuration: ZeroDuration else: cleanupInterval,
    sightingTtl: if sightingTtl <= ZeroDuration: 5.minutes else: sightingTtl,
    wakeTokenTtl: if wakeTokenTtl <= ZeroDuration: 24.hours else: wakeTokenTtl,
    peerStoreTtl:
      if peerStoreTtl <= ZeroDuration: ZeroDuration else: peerStoreTtl,
    maxSightings: max(1, maxSightings),
    maxWakeTokens: max(1, maxWakeTokens),
  )

proc new*(
    T: typedesc[MobileMeshService],
    config: MobileMeshServiceConfig = MobileMeshServiceConfig.init(),
): T =
  T(config: config)

proc getMobileMeshService*(switch: Switch): MobileMeshService =
  if switch.isNil:
    return nil
  for service in switch.services:
    if service of MobileMeshService:
      return MobileMeshService(service)

proc cleanupSightings(svc: MobileMeshService, nowMs: int64) =
  if svc.isNil:
    return
  let ttlMs = durationToMillis(svc.config.sightingTtl)
  if ttlMs <= 0:
    return

  var expired: seq[PeerId] = @[]
  for peerId, sighting in svc.sightings.pairs():
    if sighting.observedAtMs <= 0 or sighting.observedAtMs + ttlMs <= nowMs:
      expired.add(peerId)
  for peerId in expired:
    svc.sightings.del(peerId)

proc cleanupWakeTokens(svc: MobileMeshService, nowMs: int64) =
  if svc.isNil:
    return
  let ttlMs = durationToMillis(svc.config.wakeTokenTtl)

  var expired: seq[PeerId] = @[]
  for peerId, token in svc.wakeTokens.pairs():
    let defaultExpiry =
      if ttlMs > 0:
        token.updatedAtMs + ttlMs
      else:
        token.updatedAtMs
    let expiresAt =
      if token.expiresAtMs > 0:
        token.expiresAtMs
      else:
        defaultExpiry
    if token.provider.len == 0 or token.token.len == 0 or expiresAt <= nowMs:
      expired.add(peerId)
  for peerId in expired:
    svc.wakeTokens.del(peerId)

proc compareSightings(a, b: MobilePeerSighting): int =
  if a.proximityScore != b.proximityScore:
    return cmp(b.proximityScore, a.proximityScore)
  let aPriority = mediumPriority(a.medium)
  let bPriority = mediumPriority(b.medium)
  if aPriority != bPriority:
    return cmp(bPriority, aPriority)
  if a.observedAtMs != b.observedAtMs:
    return cmp(b.observedAtMs, a.observedAtMs)
  cmp(peerIdText(a.peerId), peerIdText(b.peerId))

proc compareWakeTokens(a, b: MobileWakeToken): int =
  if a.expiresAtMs != b.expiresAtMs:
    return cmp(b.expiresAtMs, a.expiresAtMs)
  if a.updatedAtMs != b.updatedAtMs:
    return cmp(b.updatedAtMs, a.updatedAtMs)
  cmp(peerIdText(a.peerId), peerIdText(b.peerId))

proc comparePhantomSnapshots(a, b: PhantomPeerSnapshot): int =
  let aPriority = phantomPriority(a.state)
  let bPriority = phantomPriority(b.state)
  if aPriority != bPriority:
    return cmp(bPriority, aPriority)
  if a.updatedAtMs != b.updatedAtMs:
    return cmp(b.updatedAtMs, a.updatedAtMs)
  cmp(peerIdText(a.peerId), peerIdText(b.peerId))

proc trimSightings(svc: MobileMeshService) =
  if svc.isNil or svc.sightings.len <= svc.config.maxSightings:
    return
  var ordered = toSeq(svc.sightings.values())
  ordered.sort(compareSightings)
  for idx in svc.config.maxSightings ..< ordered.len:
    svc.sightings.del(ordered[idx].peerId)

proc trimWakeTokens(svc: MobileMeshService) =
  if svc.isNil or svc.wakeTokens.len <= svc.config.maxWakeTokens:
    return
  var ordered = toSeq(svc.wakeTokens.values())
  ordered.sort(compareWakeTokens)
  for idx in svc.config.maxWakeTokens ..< ordered.len:
    svc.wakeTokens.del(ordered[idx].peerId)

proc cleanupState(svc: MobileMeshService, nowMs = nowMillis()) =
  if svc.isNil:
    return
  svc.cleanupSightings(nowMs)
  svc.cleanupWakeTokens(nowMs)
  svc.trimSightings()
  svc.trimWakeTokens()
  var expiredPhantoms: seq[PeerId] = @[]
  let sightingTtlMs = durationToMillis(svc.config.sightingTtl)
  let wakeTtlMs = durationToMillis(svc.config.wakeTokenTtl)
  for peerId, snapshot in svc.phantomStates.pairs():
    let hasSighting = svc.sightings.hasKey(peerId)
    let hasWakeToken = svc.wakeTokens.hasKey(peerId)
    let phantomExpiry =
      if snapshot.updatedAtMs <= 0:
        0
      elif wakeTtlMs > 0:
        snapshot.updatedAtMs + wakeTtlMs
      elif sightingTtlMs > 0:
        snapshot.updatedAtMs + sightingTtlMs
      else:
        high(int64)
    if snapshot.state == ppsUnknown or
        ((not hasSighting and not hasWakeToken) and phantomExpiry <= nowMs):
      expiredPhantoms.add(peerId)
  for peerId in expiredPhantoms:
    svc.phantomStates.del(peerId)

proc recordMobilePeerSighting*(
    svc: MobileMeshService, sighting: MobilePeerSighting
): bool =
  if svc.isNil or not isValidPeerId(sighting.peerId):
    return false

  var next = sighting
  if next.observedAtMs <= 0:
    next.observedAtMs = nowMillis()
  if next.proximityScore < 0:
    next.proximityScore = 0
  next.addrs = dedupeAddrs(next.addrs)
  svc.sightings[next.peerId] = next
  if not svc.phantomStates.hasKey(next.peerId):
    svc.phantomStates[next.peerId] = PhantomPeerSnapshot(
      peerId: next.peerId,
      state: ppsActive,
      updatedAtMs: next.observedAtMs,
      lastMedium: next.medium,
    )
  else:
    let existing = svc.phantomStates.getOrDefault(next.peerId)
    svc.phantomStates[next.peerId] = PhantomPeerSnapshot(
      peerId: next.peerId,
      state:
        if existing.state == ppsHibernated: existing.state else: ppsActive,
      updatedAtMs: max(existing.updatedAtMs, next.observedAtMs),
      lastMedium:
        if next.medium == mdmPushWake: existing.lastMedium else: next.medium,
    )

  if not svc.switch.isNil and svc.config.peerStoreTtl > ZeroDuration and
      next.addrs.len > 0:
    svc.switch.peerStore.addAddressesWithTTL(
      next.peerId, next.addrs, svc.config.peerStoreTtl
    )

  svc.cleanupState(next.observedAtMs)
  true

proc recordMobilePeerSighting*(
    switch: Switch, sighting: MobilePeerSighting
): bool =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return false
  svc.recordMobilePeerSighting(sighting)

proc registerMobileWakeToken*(
    svc: MobileMeshService, token: MobileWakeToken
): bool =
  if svc.isNil or not isValidPeerId(token.peerId):
    return false

  var next = token
  if next.provider.len == 0 or next.token.len == 0:
    return false
  if next.updatedAtMs <= 0:
    next.updatedAtMs = nowMillis()
  if next.expiresAtMs <= 0:
    next.expiresAtMs = next.updatedAtMs + durationToMillis(svc.config.wakeTokenTtl)
  if next.expiresAtMs <= next.updatedAtMs:
    next.expiresAtMs = next.updatedAtMs + durationToMillis(svc.config.wakeTokenTtl)
  svc.wakeTokens[next.peerId] = next
  svc.cleanupState(next.updatedAtMs)
  true

proc registerMobileWakeToken*(
    switch: Switch, token: MobileWakeToken
): bool =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return false
  svc.registerMobileWakeToken(token)

proc setMobilePeerWakeState*(
    svc: MobileMeshService, peerId: PeerId, state: MobilePeerWakeState
): bool =
  if svc.isNil or not isValidPeerId(peerId):
    return false
  svc.wakeStates[peerId] = state
  let lastMedium =
    if svc.sightings.hasKey(peerId):
      svc.sightings.getOrDefault(peerId).medium
    else:
      mdmPushWake
  svc.phantomStates[peerId] = PhantomPeerSnapshot(
    peerId: peerId,
    state: phantomFromWakeState(state),
    updatedAtMs: nowMillis(),
    lastMedium: lastMedium,
  )
  true

proc setMobilePeerWakeState*(
    switch: Switch, peerId: PeerId, state: MobilePeerWakeState
): bool =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return false
  svc.setMobilePeerWakeState(peerId, state)

proc mobilePeerState*(
    svc: MobileMeshService, peerId: PeerId
): Option[MobilePeerWakeState] =
  if svc.isNil or not isValidPeerId(peerId):
    return none(MobilePeerWakeState)
  svc.wakeStates.withValue(peerId, state):
    return some(state[])
  none(MobilePeerWakeState)

proc mobilePeerState*(
    switch: Switch, peerId: PeerId
): Option[MobilePeerWakeState] =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return none(MobilePeerWakeState)
  svc.mobilePeerState(peerId)

proc mobileNearfieldCandidates*(
    svc: MobileMeshService, limit = DefaultMobileCandidateLimit
): seq[MobilePeerSighting] =
  if svc.isNil:
    return @[]
  svc.cleanupState()
  for sighting in svc.sightings.values():
    result.add(sighting)
  result.sort(compareSightings)
  if limit >= 0 and result.len > limit:
    result.setLen(limit)

proc mobileNearfieldCandidates*(
    switch: Switch, limit = DefaultMobileCandidateLimit
): seq[MobilePeerSighting] =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return @[]
  svc.mobileNearfieldCandidates(limit)

proc mobileWakeCandidates*(
    svc: MobileMeshService, limit = DefaultMobileCandidateLimit
): seq[MobileWakeToken] =
  if svc.isNil:
    return @[]
  svc.cleanupState()
  for token in svc.wakeTokens.values():
    result.add(token)
  result.sort(compareWakeTokens)
  if limit >= 0 and result.len > limit:
    result.setLen(limit)

proc mobileWakeCandidates*(
    switch: Switch, limit = DefaultMobileCandidateLimit
): seq[MobileWakeToken] =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return @[]
  svc.mobileWakeCandidates(limit)

proc setPhantomPeerState*(
    svc: MobileMeshService,
    peerId: PeerId,
    state: PhantomPeerState,
    medium = mdmPushWake,
    updatedAtMs = nowMillis(),
): bool =
  if svc.isNil or not isValidPeerId(peerId):
    return false
  svc.phantomStates[peerId] = PhantomPeerSnapshot(
    peerId: peerId,
    state: state,
    updatedAtMs: updatedAtMs,
    lastMedium:
      if medium == mdmPushWake and svc.sightings.hasKey(peerId):
        svc.sightings.getOrDefault(peerId).medium
      else:
        medium,
  )
  svc.wakeStates[peerId] = wakeStateFromPhantom(state)
  true

proc setPhantomPeerState*(
    switch: Switch,
    peerId: PeerId,
    state: PhantomPeerState,
    medium = mdmPushWake,
    updatedAtMs = nowMillis(),
): bool =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return false
  svc.setPhantomPeerState(peerId, state, medium, updatedAtMs)

proc phantomPeerState*(
    svc: MobileMeshService, peerId: PeerId
): Option[PhantomPeerState] =
  if svc.isNil or not isValidPeerId(peerId):
    return none(PhantomPeerState)
  svc.phantomStates.withValue(peerId, snapshot):
    return some(snapshot[].state)
  none(PhantomPeerState)

proc phantomPeerState*(
    switch: Switch, peerId: PeerId
): Option[PhantomPeerState] =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return none(PhantomPeerState)
  svc.phantomPeerState(peerId)

proc phantomPeers*(
    svc: MobileMeshService, limit = DefaultMobileCandidateLimit
): seq[PhantomPeerSnapshot] =
  if svc.isNil:
    return @[]
  svc.cleanupState()
  for snapshot in svc.phantomStates.values():
    result.add(snapshot)
  result.sort(comparePhantomSnapshots)
  if limit >= 0 and result.len > limit:
    result.setLen(limit)

proc phantomPeers*(
    switch: Switch, limit = DefaultMobileCandidateLimit
): seq[PhantomPeerSnapshot] =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return @[]
  svc.phantomPeers(limit)

proc setTransportHealth*(
    svc: MobileMeshService, health: TransportHealthSnapshot
): bool =
  if svc.isNil:
    return false
  var next = health
  if next.lastUpdatedAtMs <= 0:
    next.lastUpdatedAtMs = nowMillis()
  if next.directQuicOnly and next.activeMedium == mdmMdnsFallback:
    next.activeMedium = mdmPushWake
  svc.transportHealthState = next
  true

proc setTransportHealth*(
    switch: Switch, health: TransportHealthSnapshot
): bool =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return false
  svc.setTransportHealth(health)

proc transportHealth*(
    svc: MobileMeshService
): TransportHealthSnapshot =
  if svc.isNil:
    return TransportHealthSnapshot()
  svc.transportHealthState

proc transportHealth*(
    switch: Switch
): TransportHealthSnapshot =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return TransportHealthSnapshot()
  svc.transportHealth()

proc mobileDialHints*(
    svc: MobileMeshService, limit = DefaultMobileCandidateLimit
): seq[MobileDialHint] =
  if svc.isNil:
    return @[]
  svc.cleanupState()
  for sighting in svc.mobileNearfieldCandidates(-1):
    let wakeState = svc.mobilePeerState(sighting.peerId).get(mpwsUnknown)
    let phantomState = svc.phantomPeerState(sighting.peerId).get(ppsUnknown)
    result.add(
      MobileDialHint(
        peerId: sighting.peerId,
        preferredAddrs: sighting.addrs,
        medium: sighting.medium,
        wakeState: wakeState,
        phantomState: phantomState,
        proximityScore: sighting.proximityScore,
        note: sighting.note,
      )
    )
  result.sort(
    proc(a, b: MobileDialHint): int =
      if a.proximityScore != b.proximityScore:
        return cmp(b.proximityScore, a.proximityScore)
      let aPhantom = phantomPriority(a.phantomState)
      let bPhantom = phantomPriority(b.phantomState)
      if aPhantom != bPhantom:
        return cmp(bPhantom, aPhantom)
      let aMedium = mediumPriority(a.medium)
      let bMedium = mediumPriority(b.medium)
      if aMedium != bMedium:
        return cmp(bMedium, aMedium)
      cmp(peerIdText(a.peerId), peerIdText(b.peerId))
  )
  if limit >= 0 and result.len > limit:
    result.setLen(limit)

proc mobileDialHints*(
    switch: Switch, limit = DefaultMobileCandidateLimit
): seq[MobileDialHint] =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return @[]
  svc.mobileDialHints(limit)

proc mobileDiscoverySnapshot*(
    svc: MobileMeshService, limit = DefaultMobileCandidateLimit
): MobileDiscoverySnapshot =
  if svc.isNil:
    return MobileDiscoverySnapshot()
  svc.cleanupState()
  MobileDiscoverySnapshot(
    updatedAtMs: nowMillis(),
    nearfield: svc.mobileNearfieldCandidates(limit),
    wakeTokens: svc.mobileWakeCandidates(limit),
    phantomPeers: svc.phantomPeers(limit),
    dialHints: svc.mobileDialHints(limit),
    transportHealth: svc.transportHealth(),
  )

proc mobileDiscoverySnapshot*(
    switch: Switch, limit = DefaultMobileCandidateLimit
): MobileDiscoverySnapshot =
  let svc = getMobileMeshService(switch)
  if svc.isNil:
    return MobileDiscoverySnapshot()
  svc.mobileDiscoverySnapshot(limit)

proc cleanupLoop(svc: MobileMeshService) {.async: (raises: [CancelledError]).} =
  heartbeat "mobilemesh-cleanup", svc.config.cleanupInterval:
    svc.cleanupState()

method setup*(
    svc: MobileMeshService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(svc).setup(switch)
  if hasBeenSetup:
    svc.switch = switch
    svc.cleanupState()
    await svc.run(switch)
  hasBeenSetup

method run*(
    svc: MobileMeshService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  if svc.running:
    return
  svc.running = true
  if svc.config.cleanupInterval > ZeroDuration:
    svc.runner = svc.cleanupLoop()

method stop*(
    svc: MobileMeshService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(svc).stop(switch)
  if hasBeenStopped:
    svc.running = false
    if not svc.runner.isNil:
      await svc.runner.cancelAndWait()
      svc.runner = nil
  hasBeenStopped

proc startMobileMeshService*(
    switch: Switch,
    config: MobileMeshServiceConfig = MobileMeshServiceConfig.init(),
): Future[MobileMeshService] {.async: (raises: [CancelledError]).} =
  if switch.isNil:
    return nil
  var svc = getMobileMeshService(switch)
  if svc.isNil:
    svc = MobileMeshService.new(config)
    switch.services.add(svc)
  if switch.isStarted():
    discard await svc.setup(switch)
  svc

{.pop.}
