# 事件发布与监听管道的基本实现。

import std/[locks, times]

import pebble/obs/types

type
  EventBus* = ref object
    lock: Lock
    listeners: seq[EventListener]
    buffer: seq[ObsEvent]
    maxBuffer*: int

proc newEventBus*(maxBuffer = 256): EventBus =
  result = EventBus(listeners: @[], buffer: @[], maxBuffer: maxBuffer)
  initLock(result.lock)

proc registerListener*(bus: EventBus; listener: EventListener) =
  if listener.isNil():
    return
  acquire(bus.lock)
  bus.listeners.add(listener)
  release(bus.lock)

proc publish*(bus: EventBus; event: ObsEvent) =
  var enriched = event
  if enriched.timestamp == default(DateTime):
    enriched.timestamp = now()
  acquire(bus.lock)
  bus.buffer.add(enriched)
  if bus.buffer.len > bus.maxBuffer:
    bus.buffer.delete(0)
  let listeners = bus.listeners
  release(bus.lock)
  for listener in listeners:
    try:
      if not listener.isNil():
        listener(enriched)
    except:
      discard

proc recent*(bus: EventBus; limit = 20): seq[ObsEvent] =
  acquire(bus.lock)
  if bus.buffer.len == 0:
    result = @[]
  else:
    let start = if bus.buffer.len > limit: bus.buffer.len - limit else: 0
    result = bus.buffer[start ..< bus.buffer.len]
  release(bus.lock)
