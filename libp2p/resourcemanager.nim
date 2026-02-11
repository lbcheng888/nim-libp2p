# Nim-LibP2P
# Copyright (c) 2023-2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[tables, locks, strformat]
import pkg/results
import chronicles, chronos

import peerid, stream/connection, errors

logScope:
  topics = "libp2p resourcemanager"

const
  UnlimitedLimit* = -1

type
  ResourceLimitError* = object of LPError

  ConnectionLimits* = object
    maxTotal*: int
    maxInbound*: int
    maxOutbound*: int
    perPeerTotal*: int
    perPeerInbound*: int
    perPeerOutbound*: int

  ProtocolLimit* = object
    maxTotal*: int
    maxInbound*: int
    maxOutbound*: int
    perPeerTotal*: int
    perPeerInbound*: int
    perPeerOutbound*: int

  PeerCounters = object
    inbound: int
    outbound: int

  ProtocolCounters = object
    inbound: int
    outbound: int

  SharedPoolLimit* = object
    connections*: ProtocolLimit
    streams*: ProtocolLimit

  StreamLimits* = object
    maxTotal*: int
    maxInbound*: int
    maxOutbound*: int
    perPeerTotal*: int
    perPeerInbound*: int
    perPeerOutbound*: int
    protocolLimits*: Table[string, ProtocolLimit]
    groupLimits*: Table[string, ProtocolLimit]
    protocolGroups*: Table[string, string]
    protocolPools*: Table[string, string]

  ResourceManagerConfig* = object
    connections*: ConnectionLimits
    streams*: StreamLimits
    sharedPools*: Table[string, SharedPoolLimit]

  SharedPoolPeerCounters = object
    connections: ProtocolCounters
    streams: ProtocolCounters

  SharedPoolCounters = object
    connections: ProtocolCounters
    streams: ProtocolCounters

  ResourcePeerMetric* = object
    peerId*: PeerId
    inbound*: int
    outbound*: int

  ProtocolMetric* = object
    protocol*: string
    inbound*: int
    outbound*: int

  SharedPoolPeerMetric* = object
    peerId*: PeerId
    connInbound*: int
    connOutbound*: int
    streamInbound*: int
    streamOutbound*: int

  SharedPoolMetric* = object
    pool*: string
    connInbound*: int
    connOutbound*: int
    streamInbound*: int
    streamOutbound*: int
    peers*: seq[SharedPoolPeerMetric]

  ResourceMetrics* = object
    connInbound*: int
    connOutbound*: int
    streamInbound*: int
    streamOutbound*: int
    connectionsByPeer*: seq[ResourcePeerMetric]
    streamsByPeer*: seq[ResourcePeerMetric]
    streamsByProtocol*: seq[ProtocolMetric]
    sharedPools*: seq[SharedPoolMetric]

  ResourceKind = enum
    rkConnection, rkStream

  ResourcePermit* = ref object
    manager: ResourceManager
    kind: ResourceKind
    peerId: PeerId
    protocol: string
    group: string
    pool: string
    dir: Direction
    released: bool

  ResourceManager* = ref object of RootObj
    config*: ResourceManagerConfig
    lock: Lock
    connInbound*: int
    connOutbound*: int
    streamInbound*: int
    streamOutbound*: int
    connPerPeer: Table[PeerId, PeerCounters]
    streamPerPeer: Table[PeerId, PeerCounters]
    streamPerProtocol: Table[string, ProtocolCounters]
    streamPerPeerProtocol: Table[PeerId, Table[string, ProtocolCounters]]
    streamPerGroup: Table[string, ProtocolCounters]
    streamPerPeerGroup: Table[PeerId, Table[string, ProtocolCounters]]
    sharedPoolCounters: Table[string, SharedPoolCounters]
    sharedPoolPerPeer: Table[string, Table[PeerId, SharedPoolPeerCounters]]

proc init*(
    _: type ConnectionLimits,
    maxTotal = UnlimitedLimit,
    maxInbound = UnlimitedLimit,
    maxOutbound = UnlimitedLimit,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
): ConnectionLimits =
  ConnectionLimits(
    maxTotal: maxTotal,
    maxInbound: maxInbound,
    maxOutbound: maxOutbound,
    perPeerTotal: perPeerTotal,
    perPeerInbound: perPeerInbound,
    perPeerOutbound: perPeerOutbound,
  )

proc init*(
    _: type ProtocolLimit,
    maxTotal = UnlimitedLimit,
    maxInbound = UnlimitedLimit,
    maxOutbound = UnlimitedLimit,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
): ProtocolLimit =
  ProtocolLimit(
    maxTotal: maxTotal,
    maxInbound: maxInbound,
    maxOutbound: maxOutbound,
    perPeerTotal: perPeerTotal,
    perPeerInbound: perPeerInbound,
    perPeerOutbound: perPeerOutbound,
  )

proc init*(
    _: type StreamLimits,
    maxTotal = UnlimitedLimit,
    maxInbound = UnlimitedLimit,
    maxOutbound = UnlimitedLimit,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
): StreamLimits =
  StreamLimits(
    maxTotal: maxTotal,
    maxInbound: maxInbound,
    maxOutbound: maxOutbound,
    perPeerTotal: perPeerTotal,
    perPeerInbound: perPeerInbound,
    perPeerOutbound: perPeerOutbound,
    protocolLimits: initTable[string, ProtocolLimit](),
    groupLimits: initTable[string, ProtocolLimit](),
    protocolGroups: initTable[string, string](),
    protocolPools: initTable[string, string](),
  )

proc init*(
    _: type SharedPoolLimit,
    connections: ProtocolLimit = ProtocolLimit.init(),
    streams: ProtocolLimit = ProtocolLimit.init(),
): SharedPoolLimit =
  SharedPoolLimit(connections: connections, streams: streams)

proc init*(
    _: type ResourceManagerConfig,
    connections: ConnectionLimits = ConnectionLimits.init(),
    streams: StreamLimits = StreamLimits.init(),
): ResourceManagerConfig =
  ResourceManagerConfig(
    connections: connections,
    streams: streams,
    sharedPools: initTable[string, SharedPoolLimit](),
  )

proc new*(
    _: type ResourceManager,
    config: ResourceManagerConfig = ResourceManagerConfig.init(),
): ResourceManager =
  var manager = ResourceManager(
    config: config,
    connPerPeer: initTable[PeerId, PeerCounters](),
    streamPerPeer: initTable[PeerId, PeerCounters](),
    streamPerProtocol: initTable[string, ProtocolCounters](),
    streamPerPeerProtocol: initTable[PeerId, Table[string, ProtocolCounters]](),
    streamPerGroup: initTable[string, ProtocolCounters](),
    streamPerPeerGroup: initTable[PeerId, Table[string, ProtocolCounters]](),
    sharedPoolCounters: initTable[string, SharedPoolCounters](),
    sharedPoolPerPeer: initTable[string, Table[PeerId, SharedPoolPeerCounters]](),
  )
  initLock(manager.lock)
  manager

proc setProtocolLimit*(
    limits: var StreamLimits,
    protocol: string,
    maxTotal = UnlimitedLimit,
    maxInbound = UnlimitedLimit,
    maxOutbound = UnlimitedLimit,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) =
  limits.protocolLimits[protocol] = ProtocolLimit.init(
    maxTotal = maxTotal,
    maxInbound = maxInbound,
    maxOutbound = maxOutbound,
    perPeerTotal = perPeerTotal,
    perPeerInbound = perPeerInbound,
    perPeerOutbound = perPeerOutbound,
  )

proc setProtocolPeerLimit*(
    limits: var StreamLimits,
    protocol: string,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) =
  var current = limits.protocolLimits.getOrDefault(protocol, ProtocolLimit.init())
  current.perPeerTotal = perPeerTotal
  current.perPeerInbound = perPeerInbound
  current.perPeerOutbound = perPeerOutbound
  limits.protocolLimits[protocol] = current

proc setProtocolGroup*(
    limits: var StreamLimits, protocol: string, group: string
) =
  if protocol.len == 0 or group.len == 0:
    return
  limits.protocolGroups[protocol] = group

proc clearProtocolGroup*(limits: var StreamLimits, protocol: string) =
  if protocol.len == 0:
    return
  limits.protocolGroups.del(protocol)

proc setProtocolPool*(
    limits: var StreamLimits, protocol: string, pool: string
) =
  if protocol.len == 0 or pool.len == 0:
    return
  limits.protocolPools[protocol] = pool

proc clearProtocolPool*(limits: var StreamLimits, protocol: string) =
  if protocol.len == 0:
    return
  limits.protocolPools.del(protocol)

proc setGroupLimit*(
    limits: var StreamLimits,
    group: string,
    maxTotal = UnlimitedLimit,
    maxInbound = UnlimitedLimit,
    maxOutbound = UnlimitedLimit,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) =
  if group.len == 0:
    return
  limits.groupLimits[group] = ProtocolLimit.init(
    maxTotal = maxTotal,
    maxInbound = maxInbound,
    maxOutbound = maxOutbound,
    perPeerTotal = perPeerTotal,
    perPeerInbound = perPeerInbound,
    perPeerOutbound = perPeerOutbound,
  )

proc setGroupPeerLimit*(
    limits: var StreamLimits,
    group: string,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) =
  if group.len == 0:
    return
  var current = limits.groupLimits.getOrDefault(group, ProtocolLimit.init())
  current.perPeerTotal = perPeerTotal
  current.perPeerInbound = perPeerInbound
  current.perPeerOutbound = perPeerOutbound
  limits.groupLimits[group] = current

proc clearGroupLimit*(limits: var StreamLimits, group: string) =
  if group.len == 0:
    return
  limits.groupLimits.del(group)

proc newResourceLimitError*(message: string): ref ResourceLimitError =
  newException(ResourceLimitError, message)

template exceeds(limit: int, value: int): bool =
  (limit >= 0) and (value > limit)

proc describeLimit(
    scope: string, current: int, limit: int, peerId: PeerId, protocol: string = ""
): string =
  if protocol.len > 0:
    &"{scope} limit hit for {protocol} (peer: {shortLog(peerId)}, current: {current}, limit: {limit})"
  else:
    &"{scope} limit hit (peer: {shortLog(peerId)}, current: {current}, limit: {limit})"

proc acquireLock(manager: ResourceManager) {.inline.} =
  acquire(manager.lock)

proc releaseLock(manager: ResourceManager) {.inline.} =
  release(manager.lock)

proc acquireConnection*(
    manager: ResourceManager, peerId: PeerId, dir: Direction, pool: string = ""
): Result[ResourcePermit, string] =
  if manager.isNil:
    return err("resource manager not configured")

  manager.acquireLock()
  defer:
    manager.releaseLock()

  let
    limits = manager.config.connections
    incInbound = if dir == Direction.In: 1 else: 0
    incOutbound = if dir == Direction.Out: 1 else: 0
    newInbound = manager.connInbound + incInbound
    newOutbound = manager.connOutbound + incOutbound
    newTotal = newInbound + newOutbound

  var counters = manager.connPerPeer.getOrDefault(peerId)
  let
    newPeerInbound = counters.inbound + incInbound
    newPeerOutbound = counters.outbound + incOutbound
    newPeerTotal = newPeerInbound + newPeerOutbound

  var reason = ""
  if exceeds(limits.maxTotal, newTotal):
    reason = describeLimit("total connection", newTotal, limits.maxTotal, peerId)
  elif incInbound == 1 and exceeds(limits.maxInbound, newInbound):
    reason = describeLimit("inbound connection", newInbound, limits.maxInbound, peerId)
  elif incOutbound == 1 and exceeds(limits.maxOutbound, newOutbound):
    reason = describeLimit("outbound connection", newOutbound, limits.maxOutbound, peerId)
  elif exceeds(limits.perPeerTotal, newPeerTotal):
    reason = describeLimit("per-peer connection", newPeerTotal, limits.perPeerTotal, peerId)
  elif incInbound == 1 and exceeds(limits.perPeerInbound, newPeerInbound):
    reason =
      describeLimit("per-peer inbound connection", newPeerInbound, limits.perPeerInbound, peerId)
  elif incOutbound == 1 and exceeds(limits.perPeerOutbound, newPeerOutbound):
    reason =
      describeLimit(
        "per-peer outbound connection", newPeerOutbound, limits.perPeerOutbound, peerId
      )

  if reason.len > 0:
    trace "connection limit reached", peerId = peerId, direction = $dir, reason = reason
    return err(reason)

  var poolName = pool
  if poolName.len > 0:
    let poolLimit = manager.config.sharedPools.getOrDefault(
      poolName, SharedPoolLimit.init()
    )
    var poolCounters = manager.sharedPoolCounters.getOrDefault(poolName, SharedPoolCounters())
    var perPeerPoolTable =
      manager.sharedPoolPerPeer.getOrDefault(poolName, initTable[PeerId, SharedPoolPeerCounters]())
    var poolPeerCounters = perPeerPoolTable.getOrDefault(peerId, SharedPoolPeerCounters())

    var poolReason = ""
    let
      newPoolInbound = poolCounters.connections.inbound + incInbound
      newPoolOutbound = poolCounters.connections.outbound + incOutbound
      newPoolTotal = newPoolInbound + newPoolOutbound
      newPoolPeerInbound = poolPeerCounters.connections.inbound + incInbound
      newPoolPeerOutbound = poolPeerCounters.connections.outbound + incOutbound
      newPoolPeerTotal = newPoolPeerInbound + newPoolPeerOutbound
      connLimit = poolLimit.connections

    if exceeds(connLimit.maxTotal, newPoolTotal):
      poolReason = describeLimit(
        "shared pool connection", newPoolTotal, connLimit.maxTotal, peerId, poolName
      )
    elif incInbound == 1 and exceeds(connLimit.maxInbound, newPoolInbound):
      poolReason = describeLimit(
        "shared pool inbound connection", newPoolInbound, connLimit.maxInbound, peerId, poolName
      )
    elif incOutbound == 1 and exceeds(connLimit.maxOutbound, newPoolOutbound):
      poolReason = describeLimit(
        "shared pool outbound connection",
        newPoolOutbound,
        connLimit.maxOutbound,
        peerId,
        poolName,
      )
    elif exceeds(connLimit.perPeerTotal, newPoolPeerTotal):
      poolReason = describeLimit(
        "per-peer shared pool connection",
        newPoolPeerTotal,
        connLimit.perPeerTotal,
        peerId,
        poolName,
      )
    elif incInbound == 1 and exceeds(connLimit.perPeerInbound, newPoolPeerInbound):
      poolReason = describeLimit(
        "per-peer shared pool inbound connection",
        newPoolPeerInbound,
        connLimit.perPeerInbound,
        peerId,
        poolName,
      )
    elif incOutbound == 1 and exceeds(connLimit.perPeerOutbound, newPoolPeerOutbound):
      poolReason = describeLimit(
        "per-peer shared pool outbound connection",
        newPoolPeerOutbound,
        connLimit.perPeerOutbound,
        peerId,
        poolName,
      )

    if poolReason.len > 0:
      trace "connection limit reached",
        peerId = peerId, direction = $dir, pool = poolName, reason = poolReason
      return err(poolReason)

    poolCounters.connections.inbound = newPoolInbound
    poolCounters.connections.outbound = newPoolOutbound
    manager.sharedPoolCounters[poolName] = poolCounters

    poolPeerCounters.connections.inbound = newPoolPeerInbound
    poolPeerCounters.connections.outbound = newPoolPeerOutbound
    perPeerPoolTable[peerId] = poolPeerCounters
    manager.sharedPoolPerPeer[poolName] = perPeerPoolTable

  manager.connInbound = newInbound
  manager.connOutbound = newOutbound
  counters.inbound = newPeerInbound
  counters.outbound = newPeerOutbound
  manager.connPerPeer[peerId] = counters

  ok(
    ResourcePermit(
      manager: manager,
      kind: rkConnection,
      peerId: peerId,
      dir: dir,
      protocol: "",
      group: "",
      pool: poolName,
    )
  )

proc acquireStream*(
    manager: ResourceManager,
    peerId: PeerId,
    protocol: string,
    dir: Direction,
    pool: string = "",
): Result[ResourcePermit, string] =
  if manager.isNil:
    return err("resource manager not configured")

  manager.acquireLock()
  defer:
    manager.releaseLock()

  let
    limits = manager.config.streams
    incInbound = if dir == Direction.In: 1 else: 0
    incOutbound = if dir == Direction.Out: 1 else: 0
    newInbound = manager.streamInbound + incInbound
    newOutbound = manager.streamOutbound + incOutbound
    newTotal = newInbound + newOutbound

  var peerCounters = manager.streamPerPeer.getOrDefault(peerId)
  let
    newPeerInbound = peerCounters.inbound + incInbound
    newPeerOutbound = peerCounters.outbound + incOutbound
    newPeerTotal = newPeerInbound + newPeerOutbound

  var protoCounters = manager.streamPerProtocol.getOrDefault(protocol)
  var perPeerProtoTable = manager.streamPerPeerProtocol.getOrDefault(peerId)
  var peerProtoCounters = perPeerProtoTable.getOrDefault(protocol)
  let
    newProtoInbound = protoCounters.inbound + incInbound
    newProtoOutbound = protoCounters.outbound + incOutbound
    newProtoTotal = newProtoInbound + newProtoOutbound
    newPeerProtoInbound = peerProtoCounters.inbound + incInbound
    newPeerProtoOutbound = peerProtoCounters.outbound + incOutbound
    newPeerProtoTotal = newPeerProtoInbound + newPeerProtoOutbound
    protoLimit = manager.config.streams.protocolLimits.getOrDefault(
      protocol, ProtocolLimit.init()
    )

  let groupName = manager.config.streams.protocolGroups.getOrDefault(protocol, "")
  let hasGroup = groupName.len > 0
  var groupLimit = ProtocolLimit.init()
  var groupCounters: ProtocolCounters
  var perPeerGroupTable: Table[string, ProtocolCounters]
  var peerGroupCounters: ProtocolCounters
  var newGroupInbound = 0
  var newGroupOutbound = 0
  var newGroupTotal = 0
  var newPeerGroupInbound = 0
  var newPeerGroupOutbound = 0
  var newPeerGroupTotal = 0
  if hasGroup:
    groupCounters = manager.streamPerGroup.getOrDefault(groupName)
    let hasPeerGroupEntry = manager.streamPerPeerGroup.hasKey(peerId)
    perPeerGroupTable = manager.streamPerPeerGroup.getOrDefault(peerId)
    if not hasPeerGroupEntry:
      perPeerGroupTable = initTable[string, ProtocolCounters]()
    peerGroupCounters = perPeerGroupTable.getOrDefault(groupName)
    newGroupInbound = groupCounters.inbound + incInbound
    newGroupOutbound = groupCounters.outbound + incOutbound
    newGroupTotal = newGroupInbound + newGroupOutbound
    newPeerGroupInbound = peerGroupCounters.inbound + incInbound
    newPeerGroupOutbound = peerGroupCounters.outbound + incOutbound
    newPeerGroupTotal = newPeerGroupInbound + newPeerGroupOutbound
    groupLimit = manager.config.streams.groupLimits.getOrDefault(
      groupName, ProtocolLimit.init()
    )

  var poolName = pool
  if poolName.len == 0:
    poolName = manager.config.streams.protocolPools.getOrDefault(protocol, "")
  if poolName.len == 0 and hasGroup:
    poolName = groupName
  let hasPool = poolName.len > 0
  var poolCounters = SharedPoolCounters()
  var perPeerPoolTable: Table[PeerId, SharedPoolPeerCounters]
  var poolPeerCounters = SharedPoolPeerCounters()
  var poolLimit = SharedPoolLimit.init()
  var newPoolInbound = 0
  var newPoolOutbound = 0
  var newPoolTotal = 0
  var newPoolPeerInbound = 0
  var newPoolPeerOutbound = 0
  var newPoolPeerTotal = 0
  if hasPool:
    poolLimit =
      manager.config.sharedPools.getOrDefault(poolName, SharedPoolLimit.init())
    poolCounters = manager.sharedPoolCounters.getOrDefault(poolName, SharedPoolCounters())
    perPeerPoolTable =
      manager.sharedPoolPerPeer.getOrDefault(poolName, initTable[PeerId, SharedPoolPeerCounters]())
    poolPeerCounters = perPeerPoolTable.getOrDefault(peerId, SharedPoolPeerCounters())
    newPoolInbound = poolCounters.streams.inbound + incInbound
    newPoolOutbound = poolCounters.streams.outbound + incOutbound
    newPoolTotal = newPoolInbound + newPoolOutbound
    newPoolPeerInbound = poolPeerCounters.streams.inbound + incInbound
    newPoolPeerOutbound = poolPeerCounters.streams.outbound + incOutbound
    newPoolPeerTotal = newPoolPeerInbound + newPoolPeerOutbound

  var reason = ""
  if exceeds(limits.maxTotal, newTotal):
    reason = describeLimit("total stream", newTotal, limits.maxTotal, peerId)
  elif incInbound == 1 and exceeds(limits.maxInbound, newInbound):
    reason = describeLimit("inbound stream", newInbound, limits.maxInbound, peerId)
  elif incOutbound == 1 and exceeds(limits.maxOutbound, newOutbound):
    reason = describeLimit("outbound stream", newOutbound, limits.maxOutbound, peerId)
  elif exceeds(limits.perPeerTotal, newPeerTotal):
    reason = describeLimit("per-peer stream", newPeerTotal, limits.perPeerTotal, peerId)
  elif incInbound == 1 and exceeds(limits.perPeerInbound, newPeerInbound):
    reason =
      describeLimit("per-peer inbound stream", newPeerInbound, limits.perPeerInbound, peerId)
  elif incOutbound == 1 and exceeds(limits.perPeerOutbound, newPeerOutbound):
    reason =
      describeLimit(
        "per-peer outbound stream", newPeerOutbound, limits.perPeerOutbound, peerId
      )
  elif exceeds(protoLimit.maxTotal, newProtoTotal):
    reason =
      describeLimit("protocol stream", newProtoTotal, protoLimit.maxTotal, peerId, protocol)
  elif incInbound == 1 and exceeds(protoLimit.maxInbound, newProtoInbound):
    reason =
      describeLimit("protocol inbound stream", newProtoInbound, protoLimit.maxInbound, peerId, protocol)
  elif incOutbound == 1 and exceeds(protoLimit.maxOutbound, newProtoOutbound):
    reason =
      describeLimit("protocol outbound stream", newProtoOutbound, protoLimit.maxOutbound, peerId, protocol)
  elif exceeds(protoLimit.perPeerTotal, newPeerProtoTotal):
    reason = describeLimit(
      "per-peer protocol stream", newPeerProtoTotal, protoLimit.perPeerTotal, peerId, protocol
    )
  elif incInbound == 1 and exceeds(protoLimit.perPeerInbound, newPeerProtoInbound):
    reason = describeLimit(
      "per-peer protocol inbound stream", newPeerProtoInbound, protoLimit.perPeerInbound, peerId, protocol
    )
  elif incOutbound == 1 and exceeds(protoLimit.perPeerOutbound, newPeerProtoOutbound):
    reason = describeLimit(
      "per-peer protocol outbound stream", newPeerProtoOutbound, protoLimit.perPeerOutbound, peerId, protocol
    )
  elif hasGroup and exceeds(groupLimit.maxTotal, newGroupTotal):
    reason = describeLimit(
      "protocol group stream", newGroupTotal, groupLimit.maxTotal, peerId, groupName
    )
  elif hasGroup and incInbound == 1 and exceeds(groupLimit.maxInbound, newGroupInbound):
    reason = describeLimit(
      "protocol group inbound stream", newGroupInbound, groupLimit.maxInbound, peerId, groupName
    )
  elif hasGroup and incOutbound == 1 and exceeds(groupLimit.maxOutbound, newGroupOutbound):
    reason = describeLimit(
      "protocol group outbound stream", newGroupOutbound, groupLimit.maxOutbound, peerId, groupName
    )
  elif hasGroup and exceeds(groupLimit.perPeerTotal, newPeerGroupTotal):
    reason = describeLimit(
      "per-peer protocol group stream",
      newPeerGroupTotal,
      groupLimit.perPeerTotal,
      peerId,
      groupName,
    )
  elif hasGroup and incInbound == 1 and exceeds(groupLimit.perPeerInbound, newPeerGroupInbound):
    reason = describeLimit(
      "per-peer protocol group inbound stream",
      newPeerGroupInbound,
      groupLimit.perPeerInbound,
      peerId,
      groupName,
    )
  elif hasGroup and incOutbound == 1 and
      exceeds(groupLimit.perPeerOutbound, newPeerGroupOutbound):
    reason = describeLimit(
      "per-peer protocol group outbound stream",
      newPeerGroupOutbound,
      groupLimit.perPeerOutbound,
      peerId,
      groupName,
    )
  elif hasPool:
    let streamLimit = poolLimit.streams
    if exceeds(streamLimit.maxTotal, newPoolTotal):
      reason = describeLimit(
        "shared pool stream", newPoolTotal, streamLimit.maxTotal, peerId, poolName
      )
    elif incInbound == 1 and exceeds(streamLimit.maxInbound, newPoolInbound):
      reason = describeLimit(
        "shared pool inbound stream", newPoolInbound, streamLimit.maxInbound, peerId, poolName
      )
    elif incOutbound == 1 and exceeds(streamLimit.maxOutbound, newPoolOutbound):
      reason = describeLimit(
        "shared pool outbound stream", newPoolOutbound, streamLimit.maxOutbound, peerId, poolName
      )
    elif exceeds(streamLimit.perPeerTotal, newPoolPeerTotal):
      reason = describeLimit(
        "per-peer shared pool stream",
        newPoolPeerTotal,
        streamLimit.perPeerTotal,
        peerId,
        poolName,
      )
    elif incInbound == 1 and exceeds(streamLimit.perPeerInbound, newPoolPeerInbound):
      reason = describeLimit(
        "per-peer shared pool inbound stream",
        newPoolPeerInbound,
        streamLimit.perPeerInbound,
        peerId,
        poolName,
      )
    elif incOutbound == 1 and exceeds(streamLimit.perPeerOutbound, newPoolPeerOutbound):
      reason = describeLimit(
        "per-peer shared pool outbound stream",
        newPoolPeerOutbound,
        streamLimit.perPeerOutbound,
        peerId,
        poolName,
      )

  if reason.len > 0:
    trace "stream limit reached",
      peerId = peerId, protocol = protocol, direction = $dir, reason = reason
    return err(reason)

  manager.streamInbound = newInbound
  manager.streamOutbound = newOutbound
  peerCounters.inbound = newPeerInbound
  peerCounters.outbound = newPeerOutbound
  manager.streamPerPeer[peerId] = peerCounters
  protoCounters.inbound = newProtoInbound
  protoCounters.outbound = newProtoOutbound
  manager.streamPerProtocol[protocol] = protoCounters
  peerProtoCounters.inbound = newPeerProtoInbound
  peerProtoCounters.outbound = newPeerProtoOutbound
  perPeerProtoTable[protocol] = peerProtoCounters
  manager.streamPerPeerProtocol[peerId] = perPeerProtoTable
  if hasGroup:
    groupCounters.inbound = newGroupInbound
    groupCounters.outbound = newGroupOutbound
    manager.streamPerGroup[groupName] = groupCounters
    peerGroupCounters.inbound = newPeerGroupInbound
    peerGroupCounters.outbound = newPeerGroupOutbound
    perPeerGroupTable[groupName] = peerGroupCounters
    manager.streamPerPeerGroup[peerId] = perPeerGroupTable

  if hasPool:
    poolCounters.streams.inbound = newPoolInbound
    poolCounters.streams.outbound = newPoolOutbound
    manager.sharedPoolCounters[poolName] = poolCounters

    poolPeerCounters.streams.inbound = newPoolPeerInbound
    poolPeerCounters.streams.outbound = newPoolPeerOutbound
    perPeerPoolTable[peerId] = poolPeerCounters
    manager.sharedPoolPerPeer[poolName] = perPeerPoolTable

  ok(
    ResourcePermit(
      manager: manager,
      kind: rkStream,
      peerId: peerId,
      protocol: protocol,
      group: groupName,
      pool: poolName,
      dir: dir,
    )
  )

proc release*(permit: ResourcePermit) =
  if permit.isNil or permit.released:
    return

  let manager = permit.manager
  if manager.isNil:
    return

  manager.acquireLock()
  defer:
    manager.releaseLock()

  if permit.released:
    return
  permit.released = true

  case permit.kind
  of rkConnection:
    if permit.dir == Direction.In:
      if manager.connInbound > 0:
        dec manager.connInbound
    else:
      if manager.connOutbound > 0:
        dec manager.connOutbound
    manager.connPerPeer.withValue(permit.peerId, counters):
      if permit.dir == Direction.In:
        if counters[].inbound > 0:
          dec counters[].inbound
      else:
        if counters[].outbound > 0:
          dec counters[].outbound
      if counters[].inbound == 0 and counters[].outbound == 0:
        manager.connPerPeer.del(permit.peerId)
    do:
      discard
    if permit.pool.len > 0:
      manager.sharedPoolCounters.withValue(permit.pool, counters):
        if permit.dir == Direction.In:
          if counters[].connections.inbound > 0:
            dec counters[].connections.inbound
        else:
          if counters[].connections.outbound > 0:
            dec counters[].connections.outbound
        if counters[].connections.inbound == 0 and counters[].connections.outbound == 0 and
            counters[].streams.inbound == 0 and counters[].streams.outbound == 0:
          manager.sharedPoolCounters.del(permit.pool)
      do:
        discard
      manager.sharedPoolPerPeer.withValue(permit.pool, poolTable):
        poolTable[].withValue(permit.peerId, counters):
          if permit.dir == Direction.In:
            if counters[].connections.inbound > 0:
              dec counters[].connections.inbound
          else:
            if counters[].connections.outbound > 0:
              dec counters[].connections.outbound
          if counters[].connections.inbound == 0 and counters[].connections.outbound == 0 and
              counters[].streams.inbound == 0 and counters[].streams.outbound == 0:
            poolTable[].del(permit.peerId)
        if poolTable[].len == 0:
          manager.sharedPoolPerPeer.del(permit.pool)
      do:
        discard
  of rkStream:
    if permit.dir == Direction.In:
      if manager.streamInbound > 0:
        dec manager.streamInbound
    else:
      if manager.streamOutbound > 0:
        dec manager.streamOutbound
    manager.streamPerPeer.withValue(permit.peerId, counters):
      if permit.dir == Direction.In:
        if counters[].inbound > 0:
          dec counters[].inbound
      else:
        if counters[].outbound > 0:
          dec counters[].outbound
      if counters[].inbound == 0 and counters[].outbound == 0:
        manager.streamPerPeer.del(permit.peerId)
    do:
      discard
    manager.streamPerProtocol.withValue(permit.protocol, counters):
      if permit.dir == Direction.In:
        if counters[].inbound > 0:
          dec counters[].inbound
      else:
        if counters[].outbound > 0:
          dec counters[].outbound
      if counters[].inbound == 0 and counters[].outbound == 0:
        manager.streamPerProtocol.del(permit.protocol)
    do:
      discard
    manager.streamPerPeerProtocol.withValue(permit.peerId, protoTable):
      protoTable[].withValue(permit.protocol, counters):
        if permit.dir == Direction.In:
          if counters[].inbound > 0:
            dec counters[].inbound
        else:
          if counters[].outbound > 0:
            dec counters[].outbound
        if counters[].inbound == 0 and counters[].outbound == 0:
          protoTable[].del(permit.protocol)
      if protoTable[].len == 0:
        manager.streamPerPeerProtocol.del(permit.peerId)
    do:
      discard
    if permit.group.len > 0:
      manager.streamPerGroup.withValue(permit.group, counters):
        if permit.dir == Direction.In:
          if counters[].inbound > 0:
            dec counters[].inbound
        else:
          if counters[].outbound > 0:
            dec counters[].outbound
        if counters[].inbound == 0 and counters[].outbound == 0:
          manager.streamPerGroup.del(permit.group)
      do:
        discard
      manager.streamPerPeerGroup.withValue(permit.peerId, groupTable):
        groupTable[].withValue(permit.group, counters):
          if permit.dir == Direction.In:
            if counters[].inbound > 0:
              dec counters[].inbound
          else:
            if counters[].outbound > 0:
              dec counters[].outbound
          if counters[].inbound == 0 and counters[].outbound == 0:
            groupTable[].del(permit.group)
        if groupTable[].len == 0:
          manager.streamPerPeerGroup.del(permit.peerId)
      do:
        discard
    if permit.pool.len > 0:
      manager.sharedPoolCounters.withValue(permit.pool, counters):
        if permit.dir == Direction.In:
          if counters[].streams.inbound > 0:
            dec counters[].streams.inbound
        else:
          if counters[].streams.outbound > 0:
            dec counters[].streams.outbound
        if counters[].connections.inbound == 0 and counters[].connections.outbound == 0 and
            counters[].streams.inbound == 0 and counters[].streams.outbound == 0:
          manager.sharedPoolCounters.del(permit.pool)
      do:
        discard
      manager.sharedPoolPerPeer.withValue(permit.pool, poolTable):
        poolTable[].withValue(permit.peerId, counters):
          if permit.dir == Direction.In:
            if counters[].streams.inbound > 0:
              dec counters[].streams.inbound
          else:
            if counters[].streams.outbound > 0:
              dec counters[].streams.outbound
          if counters[].connections.inbound == 0 and counters[].connections.outbound == 0 and
              counters[].streams.inbound == 0 and counters[].streams.outbound == 0:
            poolTable[].del(permit.peerId)
        if poolTable[].len == 0:
          manager.sharedPoolPerPeer.del(permit.pool)
      do:
        discard

proc attachPermit*(manager: ResourceManager, permit: ResourcePermit, conn: Connection) =
  if permit.isNil or manager.isNil or conn.isNil:
    return

  proc monitor() {.async: (raises: []).} =
    try:
      await conn.join()
    except CancelledError:
      discard
    finally:
      permit.release()

  asyncSpawn monitor()

proc updateConfig*(manager: ResourceManager, config: ResourceManagerConfig) {.public.} =
  if manager.isNil:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config = config
  var poolsToRemove: seq[string] = @[]
  for poolName in manager.sharedPoolCounters.keys:
    if not manager.config.sharedPools.hasKey(poolName):
      poolsToRemove.add(poolName)
  for poolName in poolsToRemove:
    manager.sharedPoolCounters.del(poolName)
    manager.sharedPoolPerPeer.del(poolName)
  for poolName in manager.config.sharedPools.keys:
    if not manager.sharedPoolCounters.hasKey(poolName):
      manager.sharedPoolCounters[poolName] = SharedPoolCounters()
    if not manager.sharedPoolPerPeer.hasKey(poolName):
      manager.sharedPoolPerPeer[poolName] = initTable[PeerId, SharedPoolPeerCounters]()

proc updateConnectionLimits*(
    manager: ResourceManager, limits: ConnectionLimits
) {.public.} =
  if manager.isNil:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.connections = limits

proc updateStreamLimits*(
    manager: ResourceManager, limits: StreamLimits
) {.public.} =
  if manager.isNil:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams = limits

proc setStreamProtocolLimit*(
    manager: ResourceManager, protocol: string, limit: ProtocolLimit
) {.public.} =
  if manager.isNil or protocol.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.protocolLimits[protocol] = limit

proc setStreamProtocolPeerLimit*(
    manager: ResourceManager,
    protocol: string,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) {.public.} =
  if manager.isNil or protocol.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  var limit = manager.config.streams.protocolLimits.getOrDefault(
    protocol, ProtocolLimit.init()
  )
  limit.perPeerTotal = perPeerTotal
  limit.perPeerInbound = perPeerInbound
  limit.perPeerOutbound = perPeerOutbound
  manager.config.streams.protocolLimits[protocol] = limit

proc clearStreamProtocolLimit*(
    manager: ResourceManager, protocol: string
) {.public.} =
  if manager.isNil or protocol.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.protocolLimits.del(protocol)

proc setStreamProtocolGroup*(
    manager: ResourceManager, protocol: string, group: string
) {.public.} =
  if manager.isNil or protocol.len == 0 or group.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.protocolGroups[protocol] = group

proc clearStreamProtocolGroup*(
    manager: ResourceManager, protocol: string
) {.public.} =
  if manager.isNil or protocol.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.protocolGroups.del(protocol)

proc setStreamProtocolPool*(
    manager: ResourceManager, protocol: string, pool: string
) {.public.} =
  if manager.isNil or protocol.len == 0 or pool.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.protocolPools[protocol] = pool

proc clearStreamProtocolPool*(
    manager: ResourceManager, protocol: string
) {.public.} =
  if manager.isNil or protocol.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.protocolPools.del(protocol)

proc setStreamGroupLimit*(
    manager: ResourceManager, group: string, limit: ProtocolLimit
) {.public.} =
  if manager.isNil or group.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.groupLimits[group] = limit

proc setStreamGroupPeerLimit*(
    manager: ResourceManager,
    group: string,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) {.public.} =
  if manager.isNil or group.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  var limit =
    manager.config.streams.groupLimits.getOrDefault(group, ProtocolLimit.init())
  limit.perPeerTotal = perPeerTotal
  limit.perPeerInbound = perPeerInbound
  limit.perPeerOutbound = perPeerOutbound
  manager.config.streams.groupLimits[group] = limit

proc clearStreamGroupLimit*(
    manager: ResourceManager, group: string
) {.public.} =
  if manager.isNil or group.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.streams.groupLimits.del(group)

proc setSharedPoolLimit*(
    manager: ResourceManager, pool: string, limit: SharedPoolLimit
) {.public.} =
  if manager.isNil or pool.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.sharedPools[pool] = limit
  if not manager.sharedPoolCounters.hasKey(pool):
    manager.sharedPoolCounters[pool] = SharedPoolCounters()
  if not manager.sharedPoolPerPeer.hasKey(pool):
    manager.sharedPoolPerPeer[pool] = initTable[PeerId, SharedPoolPeerCounters]()

proc clearSharedPoolLimit*(
    manager: ResourceManager, pool: string
) {.public.} =
  if manager.isNil or pool.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.config.sharedPools.del(pool)
  manager.sharedPoolCounters.del(pool)
  manager.sharedPoolPerPeer.del(pool)

proc snapshot*(manager: ResourceManager): ResourceMetrics {.public.} =
  var metrics = ResourceMetrics()
  if manager.isNil:
    return metrics
  manager.acquireLock()
  defer:
    manager.releaseLock()

  metrics.connInbound = manager.connInbound
  metrics.connOutbound = manager.connOutbound
  metrics.streamInbound = manager.streamInbound
  metrics.streamOutbound = manager.streamOutbound

  metrics.connectionsByPeer = @[]
  for peerId, counters in manager.connPerPeer.pairs:
    metrics.connectionsByPeer.add(
      ResourcePeerMetric(peerId: peerId, inbound: counters.inbound, outbound: counters.outbound)
    )

  metrics.streamsByPeer = @[]
  for peerId, counters in manager.streamPerPeer.pairs:
    metrics.streamsByPeer.add(
      ResourcePeerMetric(peerId: peerId, inbound: counters.inbound, outbound: counters.outbound)
    )

  metrics.streamsByProtocol = @[]
  for proto, counters in manager.streamPerProtocol.pairs:
    metrics.streamsByProtocol.add(
      ProtocolMetric(protocol: proto, inbound: counters.inbound, outbound: counters.outbound)
    )

  metrics.sharedPools = @[]
  for poolName, counters in manager.sharedPoolCounters.pairs:
    var poolMetric = SharedPoolMetric(
      pool: poolName,
      connInbound: counters.connections.inbound,
      connOutbound: counters.connections.outbound,
      streamInbound: counters.streams.inbound,
      streamOutbound: counters.streams.outbound,
      peers: @[],
    )
    let peerTable = manager.sharedPoolPerPeer.getOrDefault(poolName)
    for peerId, peerCounters in peerTable.pairs:
      poolMetric.peers.add(
        SharedPoolPeerMetric(
          peerId: peerId,
          connInbound: peerCounters.connections.inbound,
          connOutbound: peerCounters.connections.outbound,
          streamInbound: peerCounters.streams.inbound,
          streamOutbound: peerCounters.streams.outbound,
        )
      )
    metrics.sharedPools.add(poolMetric)

  metrics
