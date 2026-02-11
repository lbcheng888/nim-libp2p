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

import peerid, errors, utility

const
  UnlimitedMemory* = -1

type
  MemoryLimitError* = object of LPError

  MemoryProtocolLimit* = object
    maxTotal*: int
    perPeer*: int

  MemoryLimitConfig* = object
    maxTotal*: int
    perPeer*: int
    perProtocol*: Table[string, MemoryProtocolLimit]

  MemoryManager* = ref object of RootObj
    limits*: MemoryLimitConfig
    totalUsed*: int
    perPeer: Table[PeerId, int]
    perProtocol: Table[string, int]
    perPeerProtocol: Table[PeerId, Table[string, int]]
    lock: Lock

  MemoryPermit* = ref object
    manager: MemoryManager
    peerId: PeerId
    protocol: string
    size: int
    released: bool

  PeerMemoryStats* = object
    total*: int
    protocols*: Table[string, int]

proc init*(
    _: type MemoryProtocolLimit,
    maxTotal: int = UnlimitedMemory,
    perPeer: int = UnlimitedMemory,
): MemoryProtocolLimit =
  MemoryProtocolLimit(maxTotal: maxTotal, perPeer: perPeer)

proc init*(_: type MemoryLimitConfig): MemoryLimitConfig =
  MemoryLimitConfig(
    maxTotal: UnlimitedMemory,
    perPeer: UnlimitedMemory,
    perProtocol: initTable[string, MemoryProtocolLimit](),
  )

func hasLimits*(config: MemoryLimitConfig): bool =
  if config.maxTotal >= 0 or config.perPeer >= 0:
    return true
  for _, limit in config.perProtocol.pairs():
    if limit.maxTotal >= 0 or limit.perPeer >= 0:
      return true
  false

proc setProtocolLimit*(
    config: var MemoryLimitConfig, protocol: string, limit: MemoryProtocolLimit
) =
  if protocol.len == 0:
    raise newException(Defect, "protocol name must not be empty")
  config.perProtocol[protocol] = limit

proc setProtocolLimit*(
    config: var MemoryLimitConfig,
    protocol: string,
    maxTotal: int = UnlimitedMemory,
    perPeer: int = UnlimitedMemory,
) =
  config.setProtocolLimit(protocol, MemoryProtocolLimit.init(maxTotal, perPeer))

proc clearProtocolLimit*(config: var MemoryLimitConfig, protocol: string) =
  config.perProtocol.del(protocol)

proc new*(
    _: type MemoryManager, limits: MemoryLimitConfig = MemoryLimitConfig.init()
): MemoryManager =
  var manager = MemoryManager(
    limits: limits,
    totalUsed: 0,
    perPeer: initTable[PeerId, int](),
    perProtocol: initTable[string, int](),
    perPeerProtocol: initTable[PeerId, Table[string, int]](),
  )
  initLock(manager.lock)
  manager

proc acquireLock(manager: MemoryManager) {.inline.} =
  if not manager.isNil:
    acquire(manager.lock)

proc releaseLock(manager: MemoryManager) {.inline.} =
  if not manager.isNil:
    release(manager.lock)

proc reserve*(
    manager: MemoryManager,
    peerId: PeerId,
    protocol: string,
    size: int,
): Result[MemoryPermit, string] =
  if manager.isNil or size <= 0:
    return ok(nil)

  manager.acquireLock()
  defer:
    manager.releaseLock()

  let newTotal = manager.totalUsed + size
  if manager.limits.maxTotal >= 0 and newTotal > manager.limits.maxTotal:
    return err(
      &"total memory limit hit (current: {manager.totalUsed}, limit: {manager.limits.maxTotal}, requested: {size})"
    )

  var newPeer = 0
  if peerId.len > 0:
    newPeer = manager.perPeer.getOrDefault(peerId) + size
    if manager.limits.perPeer >= 0 and newPeer > manager.limits.perPeer:
      return err(
        &"per-peer memory limit hit (peer: {shortLog(peerId)}, current: {manager.perPeer.getOrDefault(peerId)}, limit: {manager.limits.perPeer}, requested: {size})"
      )

  var newProtocol = 0
  var protoLimit = MemoryProtocolLimit.init()
  let hasProtocol = protocol.len > 0
  if hasProtocol:
    protoLimit = manager.limits.perProtocol.getOrDefault(protocol, MemoryProtocolLimit.init())
    newProtocol = manager.perProtocol.getOrDefault(protocol) + size
    if protoLimit.maxTotal >= 0 and newProtocol > protoLimit.maxTotal:
      return err(
        &"protocol memory limit hit (protocol: {protocol}, current: {manager.perProtocol.getOrDefault(protocol)}, limit: {protoLimit.maxTotal}, requested: {size})"
      )

  var newPeerProtocol = 0
  var currentPeerProtocol = 0
  if hasProtocol and peerId.len > 0:
    let perPeerTable =
      manager.perPeerProtocol.getOrDefault(peerId, initTable[string, int]())
    currentPeerProtocol = perPeerTable.getOrDefault(protocol)
    newPeerProtocol = currentPeerProtocol + size
    if protoLimit.perPeer >= 0 and newPeerProtocol > protoLimit.perPeer:
      return err(
        &"per-peer protocol memory limit hit (peer: {shortLog(peerId)}, protocol: {protocol}, current: {currentPeerProtocol}, limit: {protoLimit.perPeer}, requested: {size})"
      )

  manager.totalUsed = newTotal

  if peerId.len > 0:
    manager.perPeer[peerId] = newPeer

  if hasProtocol:
    manager.perProtocol[protocol] = newProtocol
    if peerId.len > 0:
      withMapEntry(
        manager.perPeerProtocol, peerId, initTable[string, int](), perPeerTable
      ):
        perPeerTable[protocol] = newPeerProtocol

  ok(
    MemoryPermit(
      manager: manager, peerId: peerId, protocol: protocol, size: size, released: false
    )
  )

proc release*(permit: MemoryPermit) =
  if permit.isNil or permit.released:
    return

  let manager = permit.manager
  if manager.isNil:
    permit.released = true
    return

  manager.acquireLock()
  defer:
    manager.releaseLock()

  manager.totalUsed = max(0, manager.totalUsed - permit.size)

  if permit.peerId.len > 0:
    manager.perPeer.withValue(permit.peerId, usage):
      usage[] = max(0, usage[] - permit.size)
      if usage[] == 0:
        manager.perPeer.del(permit.peerId)
    do:
      discard

  if permit.protocol.len > 0:
    manager.perProtocol.withValue(permit.protocol, usage):
      usage[] = max(0, usage[] - permit.size)
      if usage[] == 0:
        manager.perProtocol.del(permit.protocol)
    do:
      discard

    if permit.peerId.len > 0:
      manager.perPeerProtocol.withValue(permit.peerId, table):
        table[].withValue(permit.protocol, usage):
          usage[] = max(0, usage[] - permit.size)
          if usage[] == 0:
            table[].del(permit.protocol)
        if table[].len == 0:
          manager.perPeerProtocol.del(permit.peerId)
      do:
        discard

  permit.released = true

proc updateLimits*(manager: MemoryManager, limits: MemoryLimitConfig) =
  if manager.isNil:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.limits = limits

proc setProtocolLimit*(
    manager: MemoryManager, protocol: string, limit: MemoryProtocolLimit
) =
  if manager.isNil:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.limits.setProtocolLimit(protocol, limit)

proc clearProtocolLimit*(manager: MemoryManager, protocol: string) =
  if manager.isNil:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.limits.clearProtocolLimit(protocol)

proc setProtocolPeerLimit*(manager: MemoryManager, protocol: string, perPeer: int) =
  if manager.isNil or protocol.len == 0:
    return
  manager.acquireLock()
  defer:
    manager.releaseLock()
  var limit = manager.limits.perProtocol.getOrDefault(
    protocol, MemoryProtocolLimit.init()
  )
  limit.perPeer = perPeer
  manager.limits.perProtocol[protocol] = limit

proc totalUsage*(manager: MemoryManager): int =
  if manager.isNil:
    return 0
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.totalUsed

proc peerUsage*(manager: MemoryManager, peerId: PeerId): int =
  if manager.isNil or peerId.len == 0:
    return 0
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.perPeer.getOrDefault(peerId)

proc protocolUsage*(manager: MemoryManager, protocol: string): int =
  if manager.isNil or protocol.len == 0:
    return 0
  manager.acquireLock()
  defer:
    manager.releaseLock()
  manager.perProtocol.getOrDefault(protocol)

proc stats*(manager: MemoryManager, peerId: PeerId): PeerMemoryStats =
  if manager.isNil or peerId.len == 0:
    return PeerMemoryStats(total: 0, protocols: initTable[string, int]())

  manager.acquireLock()
  defer:
    manager.releaseLock()

  let total = manager.perPeer.getOrDefault(peerId)
  let table = manager.perPeerProtocol.getOrDefault(peerId)
  var copied = initTable[string, int](table.len)
  for proto, usage in table.pairs():
    copied[proto] = usage
  PeerMemoryStats(total: total, protocols: copied)
