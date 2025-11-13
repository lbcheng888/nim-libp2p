# 运维相关能力：配置热更新、运行期调优、遥测映射与仪表盘模板。

import std/[locks, monotimes, os, tables, times]

import pebble/config/loader
import pebble/obs/types

type
  ConfigReloadCallback* = proc (cfg: FlatConfig) {.gcsafe.}

  ConfigHotReloader* = ref object
    path*: string
    intervalMs*: int
    callback*: ConfigReloadCallback
    lastCheck*: MonoTime
    lastMtime*: Time
    active*: bool

  RuntimeCommandEntry = object
    handler: RuntimeCommand
    help: string

  RuntimeCommandRegistry* = ref object
    lock: Lock
    entries: Table[string, RuntimeCommandEntry]

  TelemetryCatalog* = ref object
    lock: Lock
    fields: seq[TelemetryField]

  DashboardLibrary* = ref object
    lock: Lock
    templates: seq[DashboardTemplate]

  OpsCenter* = ref object
    reloaders: Table[string, ConfigHotReloader]
    registry: RuntimeCommandRegistry
    telemetry: TelemetryCatalog
    dashboards: DashboardLibrary

proc newRuntimeCommandRegistry*(): RuntimeCommandRegistry =
  result = RuntimeCommandRegistry(entries: initTable[string, RuntimeCommandEntry]())
  initLock(result.lock)

proc register*(registry: RuntimeCommandRegistry; name, help: string;
               handler: RuntimeCommand) =
  if handler.isNil():
    return
  acquire(registry.lock)
  registry.entries[name] = RuntimeCommandEntry(handler: handler, help: help)
  release(registry.lock)

proc execute*(registry: RuntimeCommandRegistry; name: string;
              args: seq[string]): tuple[ok: bool, output: string] =
  acquire(registry.lock)
  let entry = registry.entries.getOrDefault(name)
  let exists = registry.entries.hasKey(name)
  release(registry.lock)
  if not exists or entry.handler.isNil():
    return (false, "unknown command: " & name)
  try:
    (true, entry.handler(args))
  except CatchableError as err:
    (false, "command failed: " & err.msg)

proc list*(registry: RuntimeCommandRegistry): seq[(string, string)] =
  acquire(registry.lock)
  for name, entry in registry.entries:
    result.add((name, entry.help))
  release(registry.lock)

proc newConfigHotReloader*(path: string; callback: ConfigReloadCallback;
                           intervalMs = 1000): ConfigHotReloader =
  result = ConfigHotReloader(path: path,
                             intervalMs: intervalMs,
                             callback: callback,
                             lastCheck: getMonoTime(),
                             lastMtime: fromUnix(0),
                             active: true)

proc poll*(reloader: ConfigHotReloader) =
  if reloader.isNil() or not reloader.active:
    return
  let nowMono = getMonoTime()
  if (nowMono - reloader.lastCheck).inMilliseconds < reloader.intervalMs:
    return
  reloader.lastCheck = nowMono
  if not reloader.path.fileExists():
    return
  try:
    let info = getFileInfo(reloader.path)
    let mtime = info.lastWriteTime
    if mtime <= reloader.lastMtime:
      return
    try:
      let cfg = loadConfigFile(reloader.path)
      if not reloader.callback.isNil():
        reloader.callback(cfg)
      reloader.lastMtime = mtime
    except CatchableError:
      discard
  except OSError:
    discard

proc deactivate*(reloader: ConfigHotReloader) =
  if not reloader.isNil():
    reloader.active = false

proc newTelemetryCatalog*(): TelemetryCatalog =
  result = TelemetryCatalog(fields: @[])
  initLock(result.lock)

proc register*(catalog: TelemetryCatalog; field: TelemetryField) =
  acquire(catalog.lock)
  catalog.fields.add(field)
  release(catalog.lock)

proc fields*(catalog: TelemetryCatalog): seq[TelemetryField] =
  acquire(catalog.lock)
  result = catalog.fields
  release(catalog.lock)

proc ensureCockroachDefaults*(catalog: TelemetryCatalog) =
  let defaults = @[
    TelemetryField(name: "pebble.l0.sublevels",
                   description: "当前 L0 子层数量",
                   exportKey: "timeseries.pebble.l0.sublevels",
                   required: true),
    TelemetryField(name: "pebble.memtable.size",
                   description: "MemTable 已使用内存",
                   exportKey: "timeseries.pebble.mem.used",
                   required: true),
    TelemetryField(name: "pebble.compaction.write_amp",
                   description: "压实写放大指标",
                   exportKey: "timeseries.pebble.compaction.write_amp",
                   required: false),
    TelemetryField(name: "pebble.cache.hit_ratio",
                   description: "BlockCache 命中率",
                   exportKey: "timeseries.pebble.cache.hit_ratio",
                   required: false)
  ]
  acquire(catalog.lock)
  for field in defaults:
    var exists = false
    for current in catalog.fields:
      if current.name == field.name:
        exists = true
        break
    if not exists:
      catalog.fields.add(field)
  release(catalog.lock)

proc newDashboardLibrary*(): DashboardLibrary =
  result = DashboardLibrary(templates: @[])
  initLock(result.lock)

proc addTemplate*(library: DashboardLibrary; tpl: DashboardTemplate) =
  acquire(library.lock)
  library.templates.add(tpl)
  release(library.lock)

proc templates*(library: DashboardLibrary): seq[DashboardTemplate] =
  acquire(library.lock)
  result = library.templates
  release(library.lock)

proc defaultDashboard*(): DashboardTemplate =
  DashboardTemplate(
    name: "Pebble Nim Observability",
    panels: @[
      DashboardPanel(
        title: "写放大",
        promQuery: "pebble_compaction_write_amp",
        unit: "ratio",
        description: "压实调度实时写放大"
      ),
      DashboardPanel(
        title: "MemTable 利用率",
        promQuery: "pebble_memtable_utilization",
        unit: "percent",
        description: "MemTable 水位与使用占比"
      ),
      DashboardPanel(
        title: "事件速率",
        promQuery: "pebble_events_total",
        unit: "count",
        description: "每类事件的累计计数"
      )
    ]
  )

proc newOpsCenter*(): OpsCenter =
  result = OpsCenter(reloaders: initTable[string, ConfigHotReloader](),
                     registry: newRuntimeCommandRegistry(),
                     telemetry: newTelemetryCatalog(),
                     dashboards: newDashboardLibrary())
  result.telemetry.ensureCockroachDefaults()
  result.dashboards.addTemplate(defaultDashboard())

proc registerReloader*(ops: OpsCenter; name: string; reloader: ConfigHotReloader) =
  if ops.isNil() or reloader.isNil():
    return
  ops.reloaders[name] = reloader

proc pollReloaders*(ops: OpsCenter) =
  if ops.isNil():
    return
  for reloader in ops.reloaders.values:
    reloader.poll()

proc registerCommand*(ops: OpsCenter; name, help: string; handler: RuntimeCommand) =
  if ops.isNil():
    return
  ops.registry.register(name, help, handler)

proc executeCommand*(ops: OpsCenter; name: string;
                     args: seq[string]): tuple[ok: bool, output: string] =
  if ops.isNil():
    return (false, "observability ops not configured")
  ops.registry.execute(name, args)

proc listCommands*(ops: OpsCenter): seq[(string, string)] =
  if ops.isNil():
    return @[]
  ops.registry.list()

proc telemetry*(ops: OpsCenter): seq[TelemetryField] =
  if ops.isNil():
    return @[]
  ops.telemetry.fields()

proc dashboards*(ops: OpsCenter): seq[DashboardTemplate] =
  if ops.isNil():
    return @[]
  ops.dashboards.templates()

proc simulateHotSwap*(ops: OpsCenter; scenario: HotSwapScenario;
                      apply: proc (): bool {.gcsafe.}): HotSwapResult =
  if apply.isNil():
    return HotSwapResult(scenario: scenario, success: false,
                         details: "missing simulation callback")
  try:
    let ok = apply()
    HotSwapResult(scenario: scenario,
                  success: ok,
                  details: if ok: "scenario completed without rollback"
                           else: "simulation reported failure")
  except CatchableError as err:
    HotSwapResult(scenario: scenario, success: false,
                  details: "exception: " & err.msg)
