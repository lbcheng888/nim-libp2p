# Resource quota and capacity planning primitives for the Pebble Nim rewrite.
# Tracks usage across memory and IO channels with simple soft/hard limit
# enforcement hooks for subsystems such as compaction scheduling.

import std/[locks, tables]

type
  ResourceError* = object of CatchableError

  ResourceKind* = enum
    resMemoryBytes,
    resDiskBytes,
    resFileHandles,
    resCompactionTokens

  ResourceQuota* = object
    softLimit*: int64
    hardLimit*: int64
    inUse*: int64

  ResourceTicket* = object
    kind*: ResourceKind
    amount*: int64

  ResourceManager* = ref object
    lock: Lock
    quotas*: Table[ResourceKind, ResourceQuota]

proc newResourceManager*(): ResourceManager =
  result = ResourceManager(quotas: initTable[ResourceKind, ResourceQuota]())
  initLock(result.lock)

proc setQuota*(rm: ResourceManager; kind: ResourceKind;
               softLimit, hardLimit: int64) =
  if softLimit > hardLimit:
    raise newException(ResourceError, "soft limit exceeds hard limit")
  acquire(rm.lock)
  defer: release(rm.lock)
  rm.quotas[kind] = ResourceQuota(softLimit: softLimit,
                                  hardLimit: hardLimit,
                                  inUse: 0)

proc acquireQuota*(rm: ResourceManager; kind: ResourceKind;
                   amount: int64): ResourceTicket =
  acquire(rm.lock)
  defer: release(rm.lock)
  if not rm.quotas.hasKey(kind):
    raise newException(ResourceError, "quota not configured for kind: " & $kind)
  var quota = rm.quotas[kind]
  if quota.inUse + amount > quota.hardLimit:
    raise newException(ResourceError, "hard limit exceeded for " & $kind)
  quota.inUse += amount
  rm.quotas[kind] = quota
  ResourceTicket(kind: kind, amount: amount)

proc release*(rm: ResourceManager; ticket: ResourceTicket) =
  acquire(rm.lock)
  defer: release(rm.lock)
  if not rm.quotas.hasKey(ticket.kind):
    return
  var quota = rm.quotas[ticket.kind]
  quota.inUse = quota.inUse - ticket.amount
  if quota.inUse < 0:
    quota.inUse = 0
  rm.quotas[ticket.kind] = quota

proc utilization*(rm: ResourceManager; kind: ResourceKind): float =
  acquire(rm.lock)
  defer: release(rm.lock)
  if not rm.quotas.hasKey(kind):
    return 0.0
  let quota = rm.quotas[kind]
  if quota.hardLimit == 0:
    return 0.0
  quota.inUse.float / quota.hardLimit.float

proc isSoftExceeded*(rm: ResourceManager; kind: ResourceKind): bool =
  acquire(rm.lock)
  defer: release(rm.lock)
  if not rm.quotas.hasKey(kind):
    return false
  let quota = rm.quotas[kind]
  quota.softLimit > 0 and quota.inUse >= quota.softLimit
