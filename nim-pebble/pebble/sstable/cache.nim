# 简单的块缓存实现，使用近似 LRU 策略缓存 SSTable 数据块。

import std/[options, tables]

type
  CacheEntry = ref object
    payload: string

  BlockCache* = ref object
    capacityBytes*: int
    usageBytes*: int
    entries: Table[string, CacheEntry]
    order: seq[string]

proc newBlockCache*(capacityBytes: int): BlockCache =
  ## 创建块缓存。容量<=0时返回 nil，表示禁用缓存。
  if capacityBytes <= 0:
    return nil
  BlockCache(
    capacityBytes: capacityBytes,
    usageBytes: 0,
    entries: initTable[string, CacheEntry](),
    order: @[]
  )

proc enabled(cache: BlockCache): bool =
  not cache.isNil() and cache.capacityBytes > 0

proc removeFromOrder(cache: BlockCache; key: string) =
  for idx, value in cache.order.pairs():
    if value == key:
      cache.order.delete(idx)
      break

proc touch(cache: BlockCache; key: string) =
  if not cache.enabled():
    return
  if not cache.entries.hasKey(key):
    return
  cache.removeFromOrder(key)
  cache.order.add(key)

proc evict(cache: BlockCache) =
  if not cache.enabled():
    return
  while cache.usageBytes > cache.capacityBytes and cache.order.len > 0:
    let victim = cache.order[0]
    cache.order.delete(0)
    if cache.entries.hasKey(victim):
      let entry = cache.entries[victim]
      cache.usageBytes -= entry.payload.len
      cache.entries.del(victim)

proc get*(cache: BlockCache; key: string): Option[string] =
  if not cache.enabled():
    return none(string)
  if cache.entries.hasKey(key):
    let entry = cache.entries[key]
    cache.touch(key)
    return some(entry.payload)
  none(string)

proc put*(cache: BlockCache; key: string; payload: string) =
  if not cache.enabled():
    return
  if payload.len == 0 or payload.len > cache.capacityBytes:
    return
  if cache.entries.hasKey(key):
    let existing = cache.entries[key]
    cache.usageBytes -= existing.payload.len
    cache.entries.del(key)
    cache.removeFromOrder(key)
  cache.entries[key] = CacheEntry(payload: payload)
  cache.order.add(key)
  cache.usageBytes += payload.len
  cache.evict()

proc clear*(cache: BlockCache) =
  if cache.isNil():
    return
  cache.entries.clear()
  cache.order.setLen(0)
  cache.usageBytes = 0
