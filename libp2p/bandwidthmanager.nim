# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[tables, options, math]
import chronos
import pkg/chronos/asyncsync

import ./peerid
import ./stream/lpstream
import ./utility

type
  DirectionLimit* = object
    maxBytesPerSecond*: float
    burstSeconds*: float

  ThrottleConfig* = object
    inbound*: Option[DirectionLimit]
    outbound*: Option[DirectionLimit]

  BandwidthLimitConfig* = object
    global*: ThrottleConfig
    perPeer*: ThrottleConfig
    perProtocol*: Table[string, ThrottleConfig]
    perProtocolPeer*: Table[string, Table[PeerId, ThrottleConfig]]
    perGroup*: Table[string, ThrottleConfig]
    perGroupPeer*: Table[string, Table[PeerId, ThrottleConfig]]
    protocolGroups*: Table[string, string]

  TokenBucket = object
    capacity: float
    tokens: float
    fillRate: float
    lastRefill: Moment

  ThrottleState = object
    inbound: TokenBucket
    outbound: TokenBucket
    hasInbound: bool
    hasOutbound: bool

  DirectionStats* = object
    totalBytes*: uint64
    emaBytesPerSecond*: float
    lastUpdated*: Moment

  PeerBandwidthStats* = object
    inbound*: DirectionStats
    outbound*: DirectionStats

  ProtocolBandwidthStats* = object
    inbound*: DirectionStats
    outbound*: DirectionStats

  BandwidthSnapshot* = object
    timestamp*: uint64
    totalInbound*: uint64
    totalOutbound*: uint64
    inboundRateBps*: float
    outboundRateBps*: float

  BandwidthManager* = ref object of RootObj
    alpha: float
    stats: Table[PeerId, PeerBandwidthStats]
    protocolStats: Table[string, ProtocolBandwidthStats]
    totalInbound*: uint64
    totalOutbound*: uint64
    globalConfig: ThrottleConfig
    perPeerConfig: ThrottleConfig
    perProtocolConfig: Table[string, ThrottleConfig]
    perProtocolPeerConfig: Table[string, Table[PeerId, ThrottleConfig]]
    perGroupConfig: Table[string, ThrottleConfig]
    perGroupPeerConfig: Table[string, Table[PeerId, ThrottleConfig]]
    protocolGroups: Table[string, string]
    globalState: ThrottleState
    peerStates: Table[PeerId, ThrottleState]
    protocolStates: Table[string, ThrottleState]
    protocolPeerStates: Table[string, Table[PeerId, ThrottleState]]
    groupStates: Table[string, ThrottleState]
    groupPeerStates: Table[string, Table[PeerId, ThrottleState]]
    limitLock: AsyncLock
    hasLimits: bool

const
  DefaultBandwidthAlpha* = 0.25

proc init*(
    _: type DirectionLimit, maxBytesPerSecond: float, burstSeconds: float = 1.0
): DirectionLimit =
  if maxBytesPerSecond <= 0:
    raise newException(Defect, "maxBytesPerSecond must be positive")
  if burstSeconds <= 0:
    raise newException(Defect, "burstSeconds must be positive")
  DirectionLimit(
    maxBytesPerSecond: maxBytesPerSecond,
    burstSeconds: burstSeconds,
  )

proc init*(_: type ThrottleConfig): ThrottleConfig =
  ThrottleConfig(
    inbound: none(DirectionLimit), outbound: none(DirectionLimit)
  )

proc init*(_: type BandwidthLimitConfig): BandwidthLimitConfig =
  BandwidthLimitConfig(
    global: ThrottleConfig.init(),
    perPeer: ThrottleConfig.init(),
    perProtocol: initTable[string, ThrottleConfig](),
    perProtocolPeer: initTable[string, Table[PeerId, ThrottleConfig]](),
    perGroup: initTable[string, ThrottleConfig](),
    perGroupPeer: initTable[string, Table[PeerId, ThrottleConfig]](),
    protocolGroups: initTable[string, string](),
  )

func throttleEnabled(config: ThrottleConfig): bool =
  config.inbound.isSome or config.outbound.isSome

proc setProtocolLimit*(
    limits: var BandwidthLimitConfig, protocol: string, config: ThrottleConfig
) =
  if protocol.len == 0:
    raise newException(Defect, "protocol name must not be empty")
  if throttleEnabled(config):
    limits.perProtocol[protocol] = config
  else:
    limits.perProtocol.del(protocol)

proc setProtocolLimit*(
    limits: var BandwidthLimitConfig,
    protocol: string,
    inbound: Option[DirectionLimit],
    outbound: Option[DirectionLimit],
) =
  var cfg = ThrottleConfig.init()
  cfg.inbound = inbound
  cfg.outbound = outbound
  limits.setProtocolLimit(protocol, cfg)

proc clearProtocolLimit*(limits: var BandwidthLimitConfig, protocol: string) =
  limits.perProtocol.del(protocol)

proc setProtocolPeerLimit*(
    limits: var BandwidthLimitConfig,
    protocol: string,
    peerId: PeerId,
    config: ThrottleConfig
) =
  if protocol.len == 0:
    raise newException(Defect, "protocol name must not be empty")
  if peerId.len == 0:
    raise newException(Defect, "peer id must not be empty")
  var table =
    limits.perProtocolPeer.getOrDefault(protocol, initTable[PeerId, ThrottleConfig]())
  if throttleEnabled(config):
    table[peerId] = config
    limits.perProtocolPeer[protocol] = table
  else:
    table.del(peerId)
    if table.len == 0:
      limits.perProtocolPeer.del(protocol)
    else:
      limits.perProtocolPeer[protocol] = table

proc setProtocolPeerLimit*(
    limits: var BandwidthLimitConfig,
    protocol: string,
    peerId: PeerId,
    inbound: Option[DirectionLimit],
    outbound: Option[DirectionLimit],
) =
  var cfg = ThrottleConfig.init()
  cfg.inbound = inbound
  cfg.outbound = outbound
  limits.setProtocolPeerLimit(protocol, peerId, cfg)

proc clearProtocolPeerLimit*(
    limits: var BandwidthLimitConfig, protocol: string, peerId: PeerId
) =
  if protocol.len == 0 or peerId.len == 0:
    return
  var table =
    limits.perProtocolPeer.getOrDefault(protocol, initTable[PeerId, ThrottleConfig]())
  table.del(peerId)
  if table.len == 0:
    limits.perProtocolPeer.del(protocol)
  else:
    limits.perProtocolPeer[protocol] = table

proc setProtocolGroup*(
    limits: var BandwidthLimitConfig, protocol: string, group: string
) =
  if protocol.len == 0:
    return
  if group.len == 0:
    limits.protocolGroups.del(protocol)
  else:
    limits.protocolGroups[protocol] = group

proc clearProtocolGroup*(limits: var BandwidthLimitConfig, protocol: string) =
  limits.protocolGroups.del(protocol)

proc setGroupLimit*(
    limits: var BandwidthLimitConfig, group: string, config: ThrottleConfig
) =
  if group.len == 0:
    return
  if throttleEnabled(config):
    limits.perGroup[group] = config
  else:
    limits.perGroup.del(group)

proc setGroupLimit*(
    limits: var BandwidthLimitConfig,
    group: string,
    inbound: Option[DirectionLimit],
    outbound: Option[DirectionLimit],
) =
  var cfg = ThrottleConfig.init()
  cfg.inbound = inbound
  cfg.outbound = outbound
  limits.setGroupLimit(group, cfg)

proc clearGroupLimit*(limits: var BandwidthLimitConfig, group: string) =
  limits.perGroup.del(group)

proc setGroupPeerLimit*(
    limits: var BandwidthLimitConfig,
    group: string,
    peerId: PeerId,
    config: ThrottleConfig
) =
  if group.len == 0 or peerId.len == 0:
    return
  var table =
    limits.perGroupPeer.getOrDefault(group, initTable[PeerId, ThrottleConfig]())
  if throttleEnabled(config):
    table[peerId] = config
    limits.perGroupPeer[group] = table
  else:
    table.del(peerId)
    if table.len == 0:
      limits.perGroupPeer.del(group)
    else:
      limits.perGroupPeer[group] = table

proc setGroupPeerLimit*(
    limits: var BandwidthLimitConfig,
    group: string,
    peerId: PeerId,
    inbound: Option[DirectionLimit],
    outbound: Option[DirectionLimit],
) =
  var cfg = ThrottleConfig.init()
  cfg.inbound = inbound
  cfg.outbound = outbound
  limits.setGroupPeerLimit(group, peerId, cfg)

proc clearGroupPeerLimit*(
    limits: var BandwidthLimitConfig, group: string, peerId: PeerId
) =
  if group.len == 0 or peerId.len == 0:
    return
  var table =
    limits.perGroupPeer.getOrDefault(group, initTable[PeerId, ThrottleConfig]())
  table.del(peerId)
  if table.len == 0:
    limits.perGroupPeer.del(group)
  else:
    limits.perGroupPeer[group] = table

func limitsEnabled(config: BandwidthLimitConfig): bool =
  if throttleEnabled(config.global) or throttleEnabled(config.perPeer):
    return true
  for _, protoConfig in config.perProtocol.pairs():
    if throttleEnabled(protoConfig):
      return true
  for _, peerConfigs in config.perProtocolPeer.pairs():
    for _, peerConfig in peerConfigs.pairs():
      if throttleEnabled(peerConfig):
        return true
  for _, groupConfig in config.perGroup.pairs():
    if throttleEnabled(groupConfig):
      return true
  for _, groupPeerConfigs in config.perGroupPeer.pairs():
    for _, peerConfig in groupPeerConfigs.pairs():
      if throttleEnabled(peerConfig):
        return true
  false

proc cloneNestedThrottleConfig(
    source: Table[string, Table[PeerId, ThrottleConfig]]
): Table[string, Table[PeerId, ThrottleConfig]] =
  result = initTable[string, Table[PeerId, ThrottleConfig]]()
  for proto, peers in source.pairs():
    var perPeer = initTable[PeerId, ThrottleConfig]()
    for pid, cfg in peers.pairs():
      perPeer[pid] = cfg
    result[proto] = perPeer

proc cloneThrottleConfig(
    source: Table[string, ThrottleConfig]
): Table[string, ThrottleConfig] =
  result = initTable[string, ThrottleConfig]()
  for key, cfg in source.pairs():
    result[key] = cfg

proc cloneProtocolGroups(source: Table[string, string]): Table[string, string] =
  result = initTable[string, string]()
  for proto, group in source.pairs():
    result[proto] = group

func hasDirection(config: ThrottleConfig, dir: Direction): bool =
  case dir
  of Direction.In:
    config.inbound.isSome
  of Direction.Out:
    config.outbound.isSome

proc initTokenBucket(limit: DirectionLimit): TokenBucket =
  let capacity = max(
    limit.maxBytesPerSecond * limit.burstSeconds, limit.maxBytesPerSecond
  )
  TokenBucket(
    capacity: capacity,
    tokens: capacity,
    fillRate: limit.maxBytesPerSecond,
    lastRefill: Moment.now(),
  )

proc initThrottleState(config: ThrottleConfig): ThrottleState =
  result.hasInbound = config.inbound.isSome
  if result.hasInbound:
    result.inbound = initTokenBucket(config.inbound.get())
  result.hasOutbound = config.outbound.isSome
  if result.hasOutbound:
    result.outbound = initTokenBucket(config.outbound.get())

proc normalizeAlpha(alpha: float): float =
  if alpha <= 0 or alpha > 1:
    DefaultBandwidthAlpha
  else:
    alpha

proc refill(bucket: var TokenBucket, now: Moment) =
  if bucket.fillRate <= 0:
    bucket.tokens = bucket.capacity
    bucket.lastRefill = now
    return

  let elapsed = now - bucket.lastRefill
  if elapsed.nanoseconds <= 0:
    return

  let added = bucket.fillRate * (float(elapsed.nanoseconds) / 1_000_000_000.0)
  let updated = bucket.tokens + added
  if updated >= bucket.capacity:
    bucket.tokens = bucket.capacity
  else:
    bucket.tokens = updated
  bucket.lastRefill = now

proc consumeWithWait(
    bucket: var TokenBucket, bytes: int, now: Moment
): Duration =
  if bytes <= 0:
    return chronos.ZeroDuration

  bucket.refill(now)
  let available = bucket.tokens
  bucket.tokens -= float(bytes)
  if available >= float(bytes):
    return chronos.ZeroDuration

  let deficit = float(bytes) - available
  if deficit <= 0 or bucket.fillRate <= 0:
    return chronos.ZeroDuration

  let waitSeconds = deficit / bucket.fillRate
  if waitSeconds <= 0:
    return chronos.ZeroDuration
  let waitNs = int64(ceil(waitSeconds * 1_000_000_000.0))
  if waitNs <= 0:
    return chronos.ZeroDuration
  chronos.nanoseconds(waitNs)

proc consumeWithWait(
    state: var ThrottleState, dir: Direction, bytes: int, now: Moment
): Duration =
  case dir
  of Direction.In:
    if state.hasInbound:
      consumeWithWait(state.inbound, bytes, now)
    else:
      chronos.ZeroDuration
  of Direction.Out:
    if state.hasOutbound:
      consumeWithWait(state.outbound, bytes, now)
    else:
      chronos.ZeroDuration

proc new*(
    _: type BandwidthManager,
    alpha: float = DefaultBandwidthAlpha,
    limits: BandwidthLimitConfig = BandwidthLimitConfig.init(),
): BandwidthManager {.public.} =
  BandwidthManager(
    alpha: normalizeAlpha(alpha),
    stats: initTable[PeerId, PeerBandwidthStats](),
    protocolStats: initTable[string, ProtocolBandwidthStats](),
    globalConfig: limits.global,
    perPeerConfig: limits.perPeer,
    perProtocolConfig: cloneThrottleConfig(limits.perProtocol),
    perProtocolPeerConfig: cloneNestedThrottleConfig(limits.perProtocolPeer),
    perGroupConfig: cloneThrottleConfig(limits.perGroup),
    perGroupPeerConfig: cloneNestedThrottleConfig(limits.perGroupPeer),
    protocolGroups: cloneProtocolGroups(limits.protocolGroups),
    globalState: initThrottleState(limits.global),
    peerStates: initTable[PeerId, ThrottleState](),
    protocolStates: initTable[string, ThrottleState](),
    protocolPeerStates: initTable[string, Table[PeerId, ThrottleState]](),
    groupStates: initTable[string, ThrottleState](),
    groupPeerStates: initTable[string, Table[PeerId, ThrottleState]](),
    limitLock: newAsyncLock(),
    hasLimits: limitsEnabled(limits),
  )

proc updateStats(
    mgr: BandwidthManager, stats: var DirectionStats, bytes: int
) =
  if stats.lastUpdated == Moment.low():
    stats.emaBytesPerSecond = float(bytes)
    stats.lastUpdated = Moment.now()
    return

  let now = Moment.now()
  let elapsedMs = max(1, (now - stats.lastUpdated).milliseconds)
  let rate = (float(bytes) * 1000.0) / float(elapsedMs)
  if stats.emaBytesPerSecond <= 0:
    stats.emaBytesPerSecond = rate
  else:
    stats.emaBytesPerSecond =
      mgr.alpha * rate + (1.0 - mgr.alpha) * stats.emaBytesPerSecond
  stats.lastUpdated = now

proc awaitLimit*(
    mgr: BandwidthManager,
    peerId: PeerId,
    dir: Direction,
    bytes: int,
    protocol: string = "",
) {.async: (raises: [CancelledError]).} =
  if mgr.isNil or bytes <= 0 or not mgr.hasLimits:
    return

  let globalEnabled = hasDirection(mgr.globalConfig, dir)
  let peerEnabled = hasDirection(mgr.perPeerConfig, dir) and peerId.len > 0
  var protoEnabled = false
  var protoConfig: ThrottleConfig
  if protocol.len > 0 and mgr.perProtocolConfig.hasKey(protocol):
    protoConfig =
      mgr.perProtocolConfig.getOrDefault(protocol, ThrottleConfig.init())
    protoEnabled = hasDirection(protoConfig, dir)

  var protoPeerEnabled = false
  var protoPeerConfig: ThrottleConfig
  if protocol.len > 0 and peerId.len > 0 and mgr.perProtocolPeerConfig.hasKey(protocol):
    let peerConfigs =
      mgr.perProtocolPeerConfig.getOrDefault(protocol, initTable[PeerId, ThrottleConfig]())
    if peerConfigs.contains(peerId):
      let cfg = peerConfigs.getOrDefault(peerId, ThrottleConfig.init())
      protoPeerConfig = cfg
      protoPeerEnabled = hasDirection(protoPeerConfig, dir)

  var groupName = ""
  var groupEnabled = false
  var groupConfig: ThrottleConfig
  if protocol.len > 0:
    groupName = mgr.protocolGroups.getOrDefault(protocol, "")
    if groupName.len > 0 and mgr.perGroupConfig.hasKey(groupName):
      groupConfig =
        mgr.perGroupConfig.getOrDefault(groupName, ThrottleConfig.init())
      groupEnabled = hasDirection(groupConfig, dir)

  var groupPeerEnabled = false
  var groupPeerConfig: ThrottleConfig
  if groupName.len > 0 and peerId.len > 0 and mgr.perGroupPeerConfig.hasKey(groupName):
    let groupPeerConfigs =
      mgr.perGroupPeerConfig.getOrDefault(groupName, initTable[PeerId, ThrottleConfig]())
    if groupPeerConfigs.contains(peerId):
      let cfg = groupPeerConfigs.getOrDefault(peerId, ThrottleConfig.init())
      groupPeerConfig = cfg
      groupPeerEnabled = hasDirection(groupPeerConfig, dir)

  if not (
    globalEnabled or peerEnabled or protoEnabled or protoPeerEnabled or groupEnabled or
    groupPeerEnabled
  ):
    return

  var waitDur = chronos.ZeroDuration
  var acquired = false
  try:
    await mgr.limitLock.acquire()
    acquired = true
    let now = Moment.now()

    if globalEnabled:
      waitDur = consumeWithWait(mgr.globalState, dir, bytes, now)

    if peerEnabled:
      withMapEntry(
        mgr.peerStates, peerId, initThrottleState(mgr.perPeerConfig), state
      ):
        let peerWait = consumeWithWait(state, dir, bytes, now)
        if peerWait > waitDur:
          waitDur = peerWait

    if protoEnabled:
      withMapEntry(
        mgr.protocolStates, protocol, initThrottleState(protoConfig), protoState
      ):
        let protoWait = consumeWithWait(protoState, dir, bytes, now)
        if protoWait > waitDur:
          waitDur = protoWait

    if protoPeerEnabled:
      withMapEntry(
        mgr.protocolPeerStates,
        protocol,
        initTable[PeerId, ThrottleState](),
        perPeerStates
      ):
        withMapEntry(
          perPeerStates, peerId, initThrottleState(protoPeerConfig), peerState
        ):
          let protoPeerWait = consumeWithWait(peerState, dir, bytes, now)
          if protoPeerWait > waitDur:
            waitDur = protoPeerWait
    if groupName.len > 0 and groupEnabled:
      withMapEntry(
        mgr.groupStates, groupName, initThrottleState(groupConfig), groupState
      ):
        let groupWait = consumeWithWait(groupState, dir, bytes, now)
        if groupWait > waitDur:
          waitDur = groupWait

    if groupName.len > 0 and groupPeerEnabled:
      withMapEntry(
        mgr.groupPeerStates,
        groupName,
        initTable[PeerId, ThrottleState](),
        perPeerGroupStates
      ):
        withMapEntry(
          perPeerGroupStates,
          peerId,
          initThrottleState(groupPeerConfig),
          peerGroupState
        ):
          let groupPeerWait = consumeWithWait(peerGroupState, dir, bytes, now)
          if groupPeerWait > waitDur:
            waitDur = groupPeerWait
  except AsyncLockError as exc:
    raise newException(CancelledError, exc.msg)
  finally:
    if acquired:
      try:
        mgr.limitLock.release()
      except AsyncLockError:
        discard

  if waitDur > chronos.ZeroDuration:
    await sleepAsync(waitDur)

proc record*(
    mgr: BandwidthManager,
    peerId: PeerId,
    dir: Direction,
    bytes: int,
    protocol: string = "",
) {.public.} =
  if mgr.isNil or bytes <= 0:
    return

  if dir == Direction.In:
    mgr.totalInbound += uint64(bytes)
  else:
    mgr.totalOutbound += uint64(bytes)

  if peerId.len > 0:
    var entry = mgr.stats.getOrDefault(peerId)
    if dir == Direction.In:
      entry.inbound.totalBytes += uint64(bytes)
      mgr.updateStats(entry.inbound, bytes)
    else:
      entry.outbound.totalBytes += uint64(bytes)
      mgr.updateStats(entry.outbound, bytes)
    mgr.stats[peerId] = entry

  if protocol.len > 0:
    var protoEntry = mgr.protocolStats.getOrDefault(protocol)
    if dir == Direction.In:
      protoEntry.inbound.totalBytes += uint64(bytes)
      mgr.updateStats(protoEntry.inbound, bytes)
    else:
      protoEntry.outbound.totalBytes += uint64(bytes)
      mgr.updateStats(protoEntry.outbound, bytes)
    mgr.protocolStats[protocol] = protoEntry

proc stats*(
    mgr: BandwidthManager, peerId: PeerId
): Option[PeerBandwidthStats] {.public.} =
  if mgr.isNil or peerId.len == 0 or not mgr.stats.contains(peerId):
    return none(PeerBandwidthStats)
  some(mgr.stats.getOrDefault(peerId))

proc protocolStats*(
    mgr: BandwidthManager, protocol: string
): Option[ProtocolBandwidthStats] {.public.} =
  if mgr.isNil or protocol.len == 0 or not mgr.protocolStats.contains(protocol):
    return none(ProtocolBandwidthStats)
  some(mgr.protocolStats.getOrDefault(protocol))

proc peers*(mgr: BandwidthManager): seq[(PeerId, PeerBandwidthStats)] {.public.} =
  if mgr.isNil:
    return @[]
  for key, value in mgr.stats.pairs():
    result.add((key, value))

proc protocols*(
    mgr: BandwidthManager
): seq[(string, ProtocolBandwidthStats)] {.public.} =
  if mgr.isNil:
    return @[]
  for proto, stats in mgr.protocolStats.pairs():
    result.add((proto, stats))

proc groupStats*(
    mgr: BandwidthManager
): Table[string, ProtocolBandwidthStats] {.public.} =
  var aggregated = initTable[string, ProtocolBandwidthStats]()
  if mgr.isNil:
    return aggregated

  for proto, stats in mgr.protocolStats.pairs():
    let group = mgr.protocolGroups.getOrDefault(proto, "")
    if group.len == 0:
      continue

    var entry = aggregated.getOrDefault(group, ProtocolBandwidthStats())

    entry.inbound.totalBytes += stats.inbound.totalBytes
    entry.inbound.emaBytesPerSecond += stats.inbound.emaBytesPerSecond
    if stats.inbound.lastUpdated > entry.inbound.lastUpdated:
      entry.inbound.lastUpdated = stats.inbound.lastUpdated

    entry.outbound.totalBytes += stats.outbound.totalBytes
    entry.outbound.emaBytesPerSecond += stats.outbound.emaBytesPerSecond
    if stats.outbound.lastUpdated > entry.outbound.lastUpdated:
      entry.outbound.lastUpdated = stats.outbound.lastUpdated

    aggregated[group] = entry

  for group, _ in mgr.perGroupConfig.pairs():
    if group.len > 0 and not aggregated.hasKey(group):
      aggregated[group] = ProtocolBandwidthStats()

  for _, group in mgr.protocolGroups.pairs():
    if group.len > 0 and not aggregated.hasKey(group):
      aggregated[group] = ProtocolBandwidthStats()

  aggregated

proc snapshot*(mgr: BandwidthManager): BandwidthSnapshot {.public.} =
  if mgr.isNil:
    return BandwidthSnapshot()
  result.timestamp = Moment.now().epochSeconds.uint64
  result.totalInbound = mgr.totalInbound
  result.totalOutbound = mgr.totalOutbound
  var inboundRate = 0.0
  var outboundRate = 0.0
  for _, stats in mgr.stats.pairs():
    inboundRate += stats.inbound.emaBytesPerSecond
    outboundRate += stats.outbound.emaBytesPerSecond
  result.inboundRateBps = inboundRate.float
  result.outboundRateBps = outboundRate.float

proc rebuildLimitState(mgr: BandwidthManager, limits: BandwidthLimitConfig) =
  mgr.globalConfig = limits.global
  mgr.perPeerConfig = limits.perPeer
  mgr.perProtocolConfig = cloneThrottleConfig(limits.perProtocol)
  mgr.perProtocolPeerConfig = cloneNestedThrottleConfig(limits.perProtocolPeer)
  mgr.perGroupConfig = cloneThrottleConfig(limits.perGroup)
  mgr.perGroupPeerConfig = cloneNestedThrottleConfig(limits.perGroupPeer)
  mgr.protocolGroups = cloneProtocolGroups(limits.protocolGroups)
  mgr.globalState = initThrottleState(limits.global)
  mgr.peerStates.clear()
  mgr.protocolStates.clear()
  mgr.protocolPeerStates = initTable[string, Table[PeerId, ThrottleState]]()
  mgr.groupStates = initTable[string, ThrottleState]()
  mgr.groupPeerStates = initTable[string, Table[PeerId, ThrottleState]]()
  var combined = BandwidthLimitConfig(
    global: mgr.globalConfig,
    perPeer: mgr.perPeerConfig,
    perProtocol: mgr.perProtocolConfig,
    perProtocolPeer: mgr.perProtocolPeerConfig,
    perGroup: mgr.perGroupConfig,
    perGroupPeer: mgr.perGroupPeerConfig,
    protocolGroups: mgr.protocolGroups,
  )
  mgr.hasLimits = limitsEnabled(combined)

proc updateLimits*(
    mgr: BandwidthManager, limits: BandwidthLimitConfig
) {.async: (raises: [CancelledError]).} =
  if mgr.isNil:
    return

  var acquired = false
  try:
    await mgr.limitLock.acquire()
    acquired = true
    mgr.rebuildLimitState(limits)
  except AsyncLockError as exc:
    raise newException(CancelledError, exc.msg)
  finally:
    if acquired:
      try:
        mgr.limitLock.release()
      except AsyncLockError:
        discard

proc setProtocolLimit*(
    mgr: BandwidthManager, protocol: string, config: ThrottleConfig
) {.async: (raises: [CancelledError]).} =
  if mgr.isNil or protocol.len == 0:
    return

  var acquired = false
  try:
    await mgr.limitLock.acquire()
    acquired = true

    if throttleEnabled(config):
      mgr.perProtocolConfig[protocol] = config
      mgr.protocolStates[protocol] = initThrottleState(config)
    else:
      mgr.perProtocolConfig.del(protocol)
      mgr.protocolStates.del(protocol)

    var combined = BandwidthLimitConfig(
      global: mgr.globalConfig,
      perPeer: mgr.perPeerConfig,
      perProtocol: mgr.perProtocolConfig,
      perProtocolPeer: mgr.perProtocolPeerConfig,
      perGroup: mgr.perGroupConfig,
      perGroupPeer: mgr.perGroupPeerConfig,
      protocolGroups: mgr.protocolGroups,
    )
    mgr.hasLimits = limitsEnabled(combined)
  except AsyncLockError as exc:
    raise newException(CancelledError, exc.msg)
  finally:
    if acquired:
      try:
        mgr.limitLock.release()
      except AsyncLockError:
        discard

proc clearProtocolLimit*(mgr: BandwidthManager, protocol: string) {.
    async: (raises: [CancelledError])
  .} =
  await mgr.setProtocolLimit(protocol, ThrottleConfig.init())

proc setProtocolPeerLimit*(
    mgr: BandwidthManager, protocol: string, peerId: PeerId, config: ThrottleConfig
) {.async: (raises: [CancelledError]).} =
  if mgr.isNil or protocol.len == 0 or peerId.len == 0:
    return

  var acquired = false
  try:
    await mgr.limitLock.acquire()
    acquired = true

    if throttleEnabled(config):
      withMapEntry(
        mgr.perProtocolPeerConfig,
        protocol,
        initTable[PeerId, ThrottleConfig](),
        cfgTable
      ):
        cfgTable[peerId] = config
      withMapEntry(
        mgr.protocolPeerStates,
        protocol,
        initTable[PeerId, ThrottleState](),
        stateTable
      ):
        stateTable[peerId] = initThrottleState(config)
    else:
      var removeCfgTable = false
      withMapEntry(
        mgr.perProtocolPeerConfig,
        protocol,
        initTable[PeerId, ThrottleConfig](),
        cfgTable
      ):
        cfgTable.del(peerId)
        if cfgTable.len == 0:
          removeCfgTable = true
      if removeCfgTable:
        mgr.perProtocolPeerConfig.del(protocol)
      var removeStateTable = false
      withMapEntry(
        mgr.protocolPeerStates,
        protocol,
        initTable[PeerId, ThrottleState](),
        stateTable
      ):
        stateTable.del(peerId)
        if stateTable.len == 0:
          removeStateTable = true
      if removeStateTable:
        mgr.protocolPeerStates.del(protocol)

    var combined = BandwidthLimitConfig(
      global: mgr.globalConfig,
      perPeer: mgr.perPeerConfig,
      perProtocol: mgr.perProtocolConfig,
      perProtocolPeer: mgr.perProtocolPeerConfig,
      perGroup: mgr.perGroupConfig,
      perGroupPeer: mgr.perGroupPeerConfig,
      protocolGroups: mgr.protocolGroups,
    )
    mgr.hasLimits = limitsEnabled(combined)
  except AsyncLockError as exc:
    raise newException(CancelledError, exc.msg)
  finally:
    if acquired:
      try:
        mgr.limitLock.release()
      except AsyncLockError:
        discard

proc clearProtocolPeerLimit*(
    mgr: BandwidthManager, protocol: string, peerId: PeerId
) {.async: (raises: [CancelledError]).} =
  await mgr.setProtocolPeerLimit(protocol, peerId, ThrottleConfig.init())

proc setProtocolGroup*(
    mgr: BandwidthManager, protocol: string, group: string
) {.async: (raises: [CancelledError]).} =
  if mgr.isNil or protocol.len == 0:
    return

  var acquired = false
  try:
    await mgr.limitLock.acquire()
    acquired = true
    if group.len == 0:
      mgr.protocolGroups.del(protocol)
    else:
      mgr.protocolGroups[protocol] = group

    var combined = BandwidthLimitConfig(
      global: mgr.globalConfig,
      perPeer: mgr.perPeerConfig,
      perProtocol: mgr.perProtocolConfig,
      perProtocolPeer: mgr.perProtocolPeerConfig,
      perGroup: mgr.perGroupConfig,
      perGroupPeer: mgr.perGroupPeerConfig,
      protocolGroups: mgr.protocolGroups,
    )
    mgr.hasLimits = limitsEnabled(combined)
  except AsyncLockError as exc:
    raise newException(CancelledError, exc.msg)
  finally:
    if acquired:
      try:
        mgr.limitLock.release()
      except AsyncLockError:
        discard

proc clearProtocolGroup*(
    mgr: BandwidthManager, protocol: string
) {.async: (raises: [CancelledError]).} =
  await mgr.setProtocolGroup(protocol, "")

proc setGroupLimit*(
    mgr: BandwidthManager, group: string, config: ThrottleConfig
) {.async: (raises: [CancelledError]).} =
  if mgr.isNil or group.len == 0:
    return

  var acquired = false
  try:
    await mgr.limitLock.acquire()
    acquired = true

    if throttleEnabled(config):
      mgr.perGroupConfig[group] = config
      mgr.groupStates[group] = initThrottleState(config)
    else:
      mgr.perGroupConfig.del(group)
      mgr.groupStates.del(group)

    var combined = BandwidthLimitConfig(
      global: mgr.globalConfig,
      perPeer: mgr.perPeerConfig,
      perProtocol: mgr.perProtocolConfig,
      perProtocolPeer: mgr.perProtocolPeerConfig,
      perGroup: mgr.perGroupConfig,
      perGroupPeer: mgr.perGroupPeerConfig,
      protocolGroups: mgr.protocolGroups,
    )
    mgr.hasLimits = limitsEnabled(combined)
  except AsyncLockError as exc:
    raise newException(CancelledError, exc.msg)
  finally:
    if acquired:
      try:
        mgr.limitLock.release()
      except AsyncLockError:
        discard

proc clearGroupLimit*(
    mgr: BandwidthManager, group: string
) {.async: (raises: [CancelledError]).} =
  await mgr.setGroupLimit(group, ThrottleConfig.init())

proc setGroupPeerLimit*(
    mgr: BandwidthManager, group: string, peerId: PeerId, config: ThrottleConfig
) {.async: (raises: [CancelledError]).} =
  if mgr.isNil or group.len == 0 or peerId.len == 0:
    return

  var acquired = false
  try:
    await mgr.limitLock.acquire()
    acquired = true

    if throttleEnabled(config):
      withMapEntry(
        mgr.perGroupPeerConfig,
        group,
        initTable[PeerId, ThrottleConfig](),
        cfgTable
      ):
        cfgTable[peerId] = config
      withMapEntry(
        mgr.groupPeerStates,
        group,
        initTable[PeerId, ThrottleState](),
        stateTable
      ):
        stateTable[peerId] = initThrottleState(config)
    else:
      var removeCfgTable = false
      withMapEntry(
        mgr.perGroupPeerConfig,
        group,
        initTable[PeerId, ThrottleConfig](),
        cfgTable
      ):
        cfgTable.del(peerId)
        if cfgTable.len == 0:
          removeCfgTable = true
      if removeCfgTable:
        mgr.perGroupPeerConfig.del(group)
      var removeStateTable = false
      withMapEntry(
        mgr.groupPeerStates,
        group,
        initTable[PeerId, ThrottleState](),
        stateTable
      ):
        stateTable.del(peerId)
        if stateTable.len == 0:
          removeStateTable = true
      if removeStateTable:
        mgr.groupPeerStates.del(group)

    var combined = BandwidthLimitConfig(
      global: mgr.globalConfig,
      perPeer: mgr.perPeerConfig,
      perProtocol: mgr.perProtocolConfig,
      perProtocolPeer: mgr.perProtocolPeerConfig,
      perGroup: mgr.perGroupConfig,
      perGroupPeer: mgr.perGroupPeerConfig,
      protocolGroups: mgr.protocolGroups,
    )
    mgr.hasLimits = limitsEnabled(combined)
  except AsyncLockError as exc:
    raise newException(CancelledError, exc.msg)
  finally:
    if acquired:
      try:
        mgr.limitLock.release()
      except AsyncLockError:
        discard

proc clearGroupPeerLimit*(
    mgr: BandwidthManager, group: string, peerId: PeerId
) {.async: (raises: [CancelledError]).} =
  await mgr.setGroupPeerLimit(group, peerId, ThrottleConfig.init())
