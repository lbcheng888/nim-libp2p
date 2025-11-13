# Lightweight dependency injection container for Pebble Nim rewrite modules.
# Supports singleton/transient lifecycles and thread-safe resolution.

import std/[locks, options, tables]

type
  ServiceBase* = ref object of RootObj

  ServiceLifecycle* = enum
    serviceSingleton,
    serviceTransient

  ServiceFactory* = proc (c: ServiceContainer): ServiceBase {.gcsafe.}

  ServiceRegistration = object
    factory: ServiceFactory
    lifecycle: ServiceLifecycle

  ServiceContainer* = ref object
    lock: Lock
    registry: Table[string, ServiceRegistration]
    cache: Table[string, ServiceBase]

proc newServiceContainer*(): ServiceContainer =
  var container = ServiceContainer(registry: initTable[string, ServiceRegistration](),
                                   cache: initTable[string, ServiceBase]())
  initLock(container.lock)
  container

proc register*(c: ServiceContainer; name: string; factory: ServiceFactory;
               lifecycle: ServiceLifecycle = serviceSingleton) =
  acquire(c.lock)
  defer: release(c.lock)
  c.registry[name] = ServiceRegistration(factory: factory, lifecycle: lifecycle)
  if lifecycle == serviceSingleton:
    c.cache.del(name)

proc hasService*(c: ServiceContainer; name: string): bool =
  acquire(c.lock)
  defer: release(c.lock)
  c.registry.hasKey(name)

proc resolve*(c: ServiceContainer; name: string): ServiceBase =
  acquire(c.lock)
  defer: release(c.lock)
  if not c.registry.hasKey(name):
    raise newException(KeyError, "service not registered: " & name)
  let registration = c.registry[name]
  if registration.lifecycle == serviceSingleton:
    if c.cache.hasKey(name):
      return c.cache[name]
    let instance = registration.factory(c)
    c.cache[name] = instance
    return instance
  registration.factory(c)

template resolveAs*(c: ServiceContainer, T: typedesc; name: string): T =
  (T)(c.resolve(name))

proc clearCache*(c: ServiceContainer) =
  acquire(c.lock)
  defer: release(c.lock)
  c.cache.clear()
