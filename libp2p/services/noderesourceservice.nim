{.push raises: [].}

import std/[algorithm, json, options, os, sequtils, strtabs, strutils, tables]
when not defined(android):
  import std/osproc
from std/times import epochTime

import chronos, chronicles

when defined(posix):
  import posix

import ../multiaddress
import ../peerid
import ../peerstore
import ../protocols/protocol
import ../stream/connection
import ../stream/lpstream
import ../switch
import ../utils/heartbeat

const
  NodeResourceCodec* = "/libp2p/node-resource/1.0.0"
  NodeResourceRequestToken = "snapshot"
  DefaultNodeResourceMaxMessageBytes* = 32 * 1024
  MaxNodeResourceBytes = (1'i64 shl 60)
  MaxNodeResourceTimestampSkewMs = 7'i64 * 24 * 60 * 60 * 1000

logScope:
  topics = "libp2p node-resource"

type
  NodeOsInfo* = object
    name*: string
    version*: string
    kernel*: string
    arch*: string

  NodeCpuInfo* = object
    model*: string
    coresLogical*: int
    frequencyMHz*: int

  NodeMemoryInfo* = object
    totalBytes*: int64
    availableBytes*: int64

  NodeGpuInfo* = object
    model*: string
    vendor*: string
    vramTotalBytes*: int64
    vramAvailableBytes*: int64
    computeClass*: string
    computeScore*: int
    availableKnown*: bool

  NodeDiskInfo* = object
    mountPath*: string
    `type`*: string
    totalBytes*: int64
    availableBytes*: int64

  NodeBandwidthInfo* = object
    uplinkBps*: int64
    downlinkBps*: int64

  NodeResourceInfo* = object
    os*: NodeOsInfo
    cpu*: NodeCpuInfo
    memory*: NodeMemoryInfo
    gpu*: NodeGpuInfo
    disk*: NodeDiskInfo
    bandwidth*: NodeBandwidthInfo
    updatedAtMs*: int64

  NodeResourceSnapshot* = object
    peerId*: PeerId
    resource*: NodeResourceInfo
    lastSeenMs*: int64

  NodeResourceTotals* = object
    peers*: int
    peersWithGpu*: int
    cpuCoresLogical*: int64
    memoryTotalBytes*: int64
    memoryAvailableBytes*: int64
    gpuVramTotalBytes*: int64
    gpuVramAvailableBytes*: int64
    diskTotalBytes*: int64
    diskAvailableBytes*: int64
    bandwidthUplinkBps*: int64
    bandwidthDownlinkBps*: int64

  NodeResourceDiskStat* = object
    totalBytes*: int64
    availableBytes*: int64

  ReadTextProc* = proc(path: string): string {.gcsafe, raises: [].}
  ListDirsProc* = proc(path: string): seq[string] {.gcsafe, raises: [].}
  ExecProc* = proc(command: string, args: seq[string]): string {.gcsafe, raises: [].}
  EnvLookupProc* = proc(name: string): string {.gcsafe, raises: [].}
  StatVolumeProc* = proc(path: string): NodeResourceDiskStat {.gcsafe, raises: [].}
  ResolvePathProc* = proc(path: string): string {.gcsafe, raises: [].}
  NowMsProc* = proc(): int64 {.gcsafe, raises: [].}

  NodeResourceCollectorSource* = object
    platform*: string
    readText*: ReadTextProc
    listDirs*: ListDirsProc
    exec*: ExecProc
    envLookup*: EnvLookupProc
    statVolume*: StatVolumeProc
    resolvePath*: ResolvePathProc
    nowMs*: NowMsProc

  NodeResourceRegistry* = ref object
    ttlMs*: int64
    maxEntries*: int
    entries*: Table[PeerId, NodeResourceSnapshot]

  NodeResourceServiceConfig* = object
    codec*: string
    syncInterval*: Duration
    requestTimeout*: Duration
    localRefreshInterval*: Duration
    registryTtl*: Duration
    registryMaxEntries*: int
    maxRequestBytes*: int
    maxResponseBytes*: int
    syncBatchSize*: int
    maxClockSkew*: Duration

  NodeResourceService* = ref object of Service
    switch*: Switch
    dataDir*: string
    config*: NodeResourceServiceConfig
    registry*: NodeResourceRegistry
    source*: NodeResourceCollectorSource
    localInfo: NodeResourceInfo
    protocol: LPProtocol
    runner: Future[void]
    running: bool
    mounted: bool
    syncCursor: int
    lastLocalRefreshMs: int64

proc bytesToString(data: seq[byte]): string =
  if data.len == 0:
    return ""
  result = newString(data.len)
  copyMem(addr result[0], unsafeAddr data[0], data.len)

proc stringToBytes(text: string): seq[byte] =
  if text.len == 0:
    return @[]
  result = newSeq[byte](text.len)
  copyMem(addr result[0], text.cstring, text.len)

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc durationToMillis(value: Duration): int64 =
  value.nanoseconds div 1_000_000

proc safeReadText(path: string): string =
  if path.len == 0:
    return ""
  try:
    if fileExists(path):
      return readFile(path).strip()
  except CatchableError:
    discard
  ""

proc safeListDirs(path: string): seq[string] =
  if path.len == 0:
    return @[]
  try:
    if not dirExists(path):
      return @[]
    for kind, child in walkDir(path):
      if kind == pcDir:
        result.add(child)
  except CatchableError:
    discard

when defined(android):
  proc safeExec(command: string, args: seq[string]): string =
    discard command
    discard args
    ""
elif defined(macosx):
  proc resolveMacExecCommand(command: string): string =
    case command
    of "sw_vers":
      "/usr/bin/sw_vers"
    of "sysctl":
      "/usr/sbin/sysctl"
    of "vm_stat":
      "/usr/bin/vm_stat"
    of "df":
      "/bin/df"
    of "system_profiler":
      "/usr/sbin/system_profiler"
    else:
      command

  proc safeExec(command: string, args: seq[string]): string =
    let resolvedCommand = resolveMacExecCommand(command)
    if resolvedCommand.len == 0:
      return ""
    var emptyEnv = newStringTable(modeCaseSensitive)
    try:
      execProcess(
        resolvedCommand,
        args = args,
        env = emptyEnv,
        options = {poStdErrToStdOut},
      ).strip()
    except CatchableError:
      ""
else:
  proc safeExec(command: string, args: seq[string]): string =
    try:
      execProcess(
        command,
        args = args,
        options = {poUsePath, poStdErrToStdOut},
      ).strip()
    except CatchableError:
      ""

proc safeEnvLookup(name: string): string =
  try:
    getEnv(name)
  except CatchableError:
    ""

proc safeResolvePath(path: string): string =
  var resolved = path.strip()
  if resolved.len == 0:
    try:
      resolved = getCurrentDir()
    except CatchableError:
      resolved = "/"
  try:
    resolved = expandTilde(resolved)
  except CatchableError:
    discard
  try:
    resolved = absolutePath(resolved)
  except CatchableError:
    discard
  resolved

proc safeStatVolume(path: string): NodeResourceDiskStat =
  when defined(posix):
    when declared(statvfs):
      var statbuf: Statvfs
      if statvfs(path.cstring, statbuf) == 0:
        let blockSize = int64(statbuf.f_frsize)
        return NodeResourceDiskStat(
          totalBytes: int64(statbuf.f_blocks) * blockSize,
          availableBytes: int64(statbuf.f_bavail) * blockSize,
        )
  NodeResourceDiskStat()

proc defaultCollectorSource*(): NodeResourceCollectorSource =
  result.readText = safeReadText
  result.listDirs = safeListDirs
  result.exec = safeExec
  result.envLookup = safeEnvLookup
  result.statVolume = safeStatVolume
  result.resolvePath = safeResolvePath
  result.nowMs = nowMillis
  when defined(android):
    result.platform = "android"
  elif defined(macosx):
    result.platform = "macos"
  elif defined(linux):
    result.platform = "linux"
  else:
    result.platform = hostOS.toLowerAscii()

proc effectiveSource(
    source: NodeResourceCollectorSource
): NodeResourceCollectorSource =
  result = source
  let defaults = defaultCollectorSource()
  if result.platform.len == 0:
    result.platform = defaults.platform
  if result.readText.isNil:
    result.readText = defaults.readText
  if result.listDirs.isNil:
    result.listDirs = defaults.listDirs
  if result.exec.isNil:
    result.exec = defaults.exec
  if result.envLookup.isNil:
    result.envLookup = defaults.envLookup
  if result.statVolume.isNil:
    result.statVolume = defaults.statVolume
  if result.resolvePath.isNil:
    result.resolvePath = defaults.resolvePath
  if result.nowMs.isNil:
    result.nowMs = defaults.nowMs

proc parseIntLoose(text: string): int64 =
  var token = ""
  for ch in text:
    if ch == '-' and token.len == 0:
      token.add(ch)
    elif ch in {'0' .. '9'}:
      token.add(ch)
    elif token.len > 0:
      break
  if token.len == 0 or token == "-":
    return 0
  try:
    parseBiggestInt(token)
  except CatchableError:
    0

proc parseIntFlexible(text: string): int64 =
  let trimmed = text.strip()
  if trimmed.len == 0:
    return 0
  if trimmed.startsWith("0x") or trimmed.startsWith("0X"):
    try:
      return parseHexInt(trimmed).int64
    except CatchableError:
      discard
  parseIntLoose(trimmed)

proc parseFloatFlexible(text: string): float64 =
  let trimmed = text.strip().replace(",", "")
  if trimmed.len == 0:
    return 0
  var token = ""
  var seenDot = false
  for ch in trimmed:
    if ch in {'0' .. '9'}:
      token.add(ch)
    elif ch == '.' and not seenDot:
      token.add(ch)
      seenDot = true
    elif token.len > 0:
      break
  if token.len == 0:
    return 0
  try:
    parseFloat(token)
  except CatchableError:
    0

proc valueAfterColon(line: string): string =
  let idx = line.find(':')
  if idx < 0:
    return line.strip()
  line[idx + 1 ..< line.len].strip()

proc decodeMountField(text: string): string =
  result = text
  result = result.replace("\\040", " ")
  result = result.replace("\\011", "\t")
  result = result.replace("\\012", "\n")
  result = result.replace("\\134", "\\")

proc normalizePlatform(platform: string): string =
  let lowered = platform.toLowerAscii()
  if lowered.contains("android"):
    return "android"
  if lowered.contains("mac"):
    return "macos"
  if lowered.contains("darwin"):
    return "macos"
  if lowered.contains("linux"):
    return "linux"
  lowered

proc defaultResourceInfo(): NodeResourceInfo =
  NodeResourceInfo(
    os: NodeOsInfo(
      name:
        (
          when defined(android):
            "Android"
          elif defined(macosx):
            "macOS"
          elif defined(linux):
            "Linux"
          else:
            hostOS
        ),
      arch: hostCPU,
    ),
    cpu: NodeCpuInfo(model: "unknown"),
    gpu: NodeGpuInfo(model: "unknown", vendor: "unknown"),
    disk: NodeDiskInfo(mountPath: "/", `type`: "unknown"),
  )

proc parseSizeToBytes(text: string): int64 =
  let normalized = text.strip().replace(",", "").toUpperAscii()
  if normalized.len == 0:
    return 0
  let value = parseFloatFlexible(normalized)
  if value <= 0:
    return 0
  var multiplier = 1.0
  if "TB" in normalized:
    multiplier = 1024.0 * 1024.0 * 1024.0 * 1024.0
  elif "GB" in normalized:
    multiplier = 1024.0 * 1024.0 * 1024.0
  elif "MB" in normalized:
    multiplier = 1024.0 * 1024.0
  elif "KB" in normalized:
    multiplier = 1024.0
  elif "B" in normalized:
    multiplier = 1.0
  int64(value * multiplier)

proc hasGpu(info: NodeResourceInfo): bool =
  info.gpu.computeScore > 0 or info.gpu.vramTotalBytes > 0 or
    (info.gpu.model.len > 0 and info.gpu.model.toLowerAscii() != "unknown")

proc inferGpuIdentity(
    modelText: string, vendorHint = ""
): tuple[vendor: string, computeClass: string, computeScore: int] =
  let model = modelText.strip()
  let lowered = (vendorHint & " " & model).toLowerAscii()
  result.vendor = vendorHint.strip()
  if result.vendor.len == 0:
    if lowered.contains("nvidia") or lowered.contains("geforce") or lowered.contains("rtx") or
        lowered.contains("tesla") or lowered.contains("quadro") or lowered.contains("a100") or
        lowered.contains("h100") or lowered.contains("l40"):
      result.vendor = "NVIDIA"
    elif lowered.contains("amd") or lowered.contains("radeon") or lowered.contains("instinct") or
        lowered.contains("mi300") or lowered.contains("mi250"):
      result.vendor = "AMD"
    elif lowered.contains("apple"):
      result.vendor = "Apple"
    elif lowered.contains("intel") or lowered.contains("arc"):
      result.vendor = "Intel"
    else:
      result.vendor = "unknown"

  if lowered.contains("h200"):
    return (result.vendor, "nvidia-hopper", 1100)
  if lowered.contains("h100"):
    return (result.vendor, "nvidia-hopper", 1000)
  if lowered.contains("a100"):
    return (result.vendor, "nvidia-ampere", 900)
  if lowered.contains("l40s"):
    return (result.vendor, "nvidia-ada", 820)
  if lowered.contains("l40"):
    return (result.vendor, "nvidia-ada", 780)
  if lowered.contains("rtx 5090"):
    return (result.vendor, "nvidia-blackwell", 900)
  if lowered.contains("rtx 5080"):
    return (result.vendor, "nvidia-blackwell", 820)
  if lowered.contains("rtx 4090"):
    return (result.vendor, "nvidia-ada", 800)
  if lowered.contains("rtx 4080"):
    return (result.vendor, "nvidia-ada", 700)
  if lowered.contains("rtx 3090") or lowered.contains("rtx a6000"):
    return (result.vendor, "nvidia-ampere", 650)
  if lowered.contains("v100"):
    return (result.vendor, "nvidia-volta", 500)
  if lowered.contains("t4"):
    return (result.vendor, "nvidia-turing", 300)
  if lowered.contains("mi300"):
    return (result.vendor, "amd-cdna3", 950)
  if lowered.contains("mi250"):
    return (result.vendor, "amd-cdna2", 850)
  if lowered.contains("w7900") or lowered.contains("7900 xtx"):
    return (result.vendor, "amd-rdna3", 650)
  if lowered.contains("m4 max"):
    return (result.vendor, "apple-mseries", 500)
  if lowered.contains("m3 max"):
    return (result.vendor, "apple-mseries", 450)
  if lowered.contains("m2 ultra"):
    return (result.vendor, "apple-mseries", 420)
  if lowered.contains("m2 max"):
    return (result.vendor, "apple-mseries", 340)
  if lowered.contains("m1 ultra"):
    return (result.vendor, "apple-mseries", 320)
  if lowered.contains("m1 max"):
    return (result.vendor, "apple-mseries", 260)
  if lowered.contains("intel max"):
    return (result.vendor, "intel-data-center", 450)
  if lowered.contains("arc"):
    return (result.vendor, "intel-arc", 220)
  if result.vendor == "NVIDIA":
    return (result.vendor, "nvidia-gpu", 100)
  if result.vendor == "AMD":
    return (result.vendor, "amd-gpu", 100)
  if result.vendor == "Apple":
    return (result.vendor, "apple-gpu", 120)
  if result.vendor == "Intel":
    return (result.vendor, "intel-gpu", 80)
  (result.vendor, "", 0)

proc parseLinuxCpuModel(cpuInfo: string): string =
  if cpuInfo.len == 0:
    return "unknown"
  for lineRaw in cpuInfo.splitLines():
    let line = lineRaw.strip()
    if line.len == 0:
      continue
    let lowered = line.toLowerAscii()
    if lowered.startsWith("hardware") or lowered.startsWith("model name") or
        lowered.startsWith("cpu model"):
      let value = valueAfterColon(line)
      if value.len > 0:
        return value
  "unknown"

proc parseLinuxCpuCores(cpuInfo: string): int =
  var count = 0
  for lineRaw in cpuInfo.splitLines():
    let line = lineRaw.strip().toLowerAscii()
    if line.startsWith("processor") and ':' in line:
      inc count
  count

proc parseLinuxCpuFrequencyMHz(cpuInfo, sysFreqText: string): int =
  let sysValue = parseIntFlexible(sysFreqText)
  if sysValue > 0:
    if sysValue > 10_000:
      return int(sysValue div 1_000)
    return int(sysValue)
  for lineRaw in cpuInfo.splitLines():
    let line = lineRaw.strip().toLowerAscii()
    if line.startsWith("cpu mhz"):
      let value = parseIntFlexible(valueAfterColon(lineRaw))
      if value > 0:
        return int(value)
  0

proc parseLinuxMemory(memInfo: string): tuple[totalBytes: int64, availableBytes: int64] =
  var totalBytes = 0'i64
  var availableBytes = 0'i64
  for lineRaw in memInfo.splitLines():
    let line = lineRaw.strip()
    if line.startsWith("MemTotal:"):
      let kb = parseIntFlexible(line)
      if kb > 0:
        totalBytes = kb * 1024
    elif line.startsWith("MemAvailable:"):
      let kb = parseIntFlexible(line)
      if kb > 0:
        availableBytes = kb * 1024
    elif availableBytes == 0 and line.startsWith("MemFree:"):
      let kb = parseIntFlexible(line)
      if kb > 0:
        availableBytes = kb * 1024
  (totalBytes, availableBytes)

proc parseLinuxRouteDefaultIface(routeText: string): string =
  for idx, lineRaw in routeText.splitLines().pairs():
    if idx == 0:
      continue
    let parts = lineRaw.splitWhitespace()
    if parts.len >= 2 and parts[1] == "00000000":
      return parts[0]
  ""

proc parseLinuxPrimaryIface(devText: string): string =
  var bestBytes = -1'i64
  for lineRaw in devText.splitLines():
    let idx = lineRaw.find(':')
    if idx < 0:
      continue
    let iface = lineRaw[0 ..< idx].strip()
    if iface.len == 0 or iface == "lo":
      continue
    let fields = lineRaw[idx + 1 ..< lineRaw.len].splitWhitespace()
    if fields.len < 16:
      continue
    let totalBytes = parseIntFlexible(fields[0]) + parseIntFlexible(fields[8])
    if totalBytes > bestBytes:
      bestBytes = totalBytes
      result = iface

proc pathMatchesMount(path, mountPoint: string): bool =
  if mountPoint.len == 0:
    return false
  if mountPoint == "/":
    return path.startsWith("/")
  path == mountPoint or path.startsWith(mountPoint & "/")

proc findLinuxMountInfo(
    mountsText, path: string
): tuple[mountPoint: string, fsType: string] =
  var bestLen = -1
  for lineRaw in mountsText.splitLines():
    let parts = lineRaw.splitWhitespace()
    if parts.len < 3:
      continue
    let mountPoint = decodeMountField(parts[1])
    if not pathMatchesMount(path, mountPoint):
      continue
    if mountPoint.len > bestLen:
      bestLen = mountPoint.len
      result = (mountPoint, parts[2])
  if result.mountPoint.len == 0:
    result.mountPoint = "/"
    result.fsType = "unknown"

proc parseLinuxGpuInfo(
    source: NodeResourceCollectorSource
): NodeGpuInfo =
  let nvidiaDirs = source.listDirs("/proc/driver/nvidia/gpus")
  for dir in nvidiaDirs:
    let infoText = source.readText(dir / "information")
    if infoText.len == 0:
      continue
    var model = ""
    for lineRaw in infoText.splitLines():
      let line = lineRaw.strip()
      if line.toLowerAscii().startsWith("model"):
        model = valueAfterColon(line)
        break
    if model.len > 0:
      let inferred = inferGpuIdentity(model, "NVIDIA")
      return NodeGpuInfo(
        model: model,
        vendor: inferred.vendor,
        computeClass: inferred.computeClass,
        computeScore: inferred.computeScore,
      )

  let drmDirs = source.listDirs("/sys/class/drm")
  for dir in drmDirs:
    let name = splitPath(dir).tail
    if not name.startsWith("card") or "-" in name:
      continue
    let total = parseIntFlexible(source.readText(dir / "device/mem_info_vram_total"))
    let used = parseIntFlexible(source.readText(dir / "device/mem_info_vram_used"))
    let productName = source.readText(dir / "device/product_name")
    let vendorId = source.readText(dir / "device/vendor").strip()
    let vendor =
      case vendorId.toLowerAscii()
      of "0x10de":
        "NVIDIA"
      of "0x1002":
        "AMD"
      of "0x8086":
        "Intel"
      else:
        ""
    let inferred = inferGpuIdentity(productName, vendor)
    var info = NodeGpuInfo(
      model: if productName.len > 0: productName else: "unknown",
      vendor: inferred.vendor,
      vramTotalBytes: total,
      computeClass: inferred.computeClass,
      computeScore: inferred.computeScore,
    )
    if total > 0 and used >= 0 and used <= total:
      info.vramAvailableBytes = total - used
      info.availableKnown = true
    return info

proc parseLinuxBandwidth(
    source: NodeResourceCollectorSource
): NodeBandwidthInfo =
  let routeText = source.readText("/proc/net/route")
  let devText = source.readText("/proc/net/dev")
  var iface = parseLinuxRouteDefaultIface(routeText)
  if iface.len == 0:
    iface = parseLinuxPrimaryIface(devText)
  if iface.len == 0:
    return
  let speed = parseIntFlexible(source.readText("/sys/class/net" / iface / "speed"))
  if speed > 0:
    let bps = speed * 1_000_000
    result.uplinkBps = bps
    result.downlinkBps = bps

proc parseMacVmAvailableBytes(vmStatText: string): int64 =
  var pageSize = 4096'i64
  var availablePages = 0'i64
  for lineRaw in vmStatText.splitLines():
    let line = lineRaw.strip()
    let lowered = line.toLowerAscii()
    if lowered.startsWith("mach virtual memory statistics") and "page size of" in lowered:
      let idx = lowered.find("page size of")
      if idx >= 0:
        let sizeText = lowered[idx + "page size of".len ..< lowered.len]
        let parsed = parseIntFlexible(sizeText)
        if parsed > 0:
          pageSize = parsed
    elif lowered.startsWith("pages free") or lowered.startsWith("pages inactive") or
        lowered.startsWith("pages speculative"):
      availablePages += parseIntFlexible(line)
  availablePages * pageSize

proc parseMacDfMountPath(dfText: string): string =
  let lines = dfText.splitLines()
  if lines.len < 2:
    return ""
  let parts = lines[^1].splitWhitespace()
  if parts.len == 0:
    return ""
  parts[^1]

proc parseMacGpuInfo(spText: string): NodeGpuInfo =
  var model = ""
  var vendor = ""
  var vramTotalBytes = 0'i64
  for lineRaw in spText.splitLines():
    let line = lineRaw.strip()
    let lowered = line.toLowerAscii()
    if lowered.startsWith("chipset model:") or lowered.startsWith("model:"):
      model = valueAfterColon(line)
    elif lowered.startsWith("vendor:"):
      vendor = valueAfterColon(line)
    elif lowered.startsWith("vram (total):") or lowered.startsWith("vram (dynamic, max):") or
        lowered.startsWith("vram:"):
      vramTotalBytes = parseSizeToBytes(valueAfterColon(line))
  let inferred = inferGpuIdentity(model, vendor)
  NodeGpuInfo(
    model: if model.len > 0: model else: "unknown",
    vendor: inferred.vendor,
    vramTotalBytes: vramTotalBytes,
    computeClass: inferred.computeClass,
    computeScore: inferred.computeScore,
  )

proc parseAndroidVersion(source: NodeResourceCollectorSource): string =
  for path in ["/system/build.prop", "/system/system/build.prop", "/vendor/build.prop"]:
    for lineRaw in source.readText(path).splitLines():
      let line = lineRaw.strip()
      if line.startsWith("ro.build.version.release="):
        let idx = line.find('=')
        if idx >= 0 and idx + 1 < line.len:
          return line[idx + 1 ..< line.len].strip()
  "Android"

proc clampNonNegative(value: int64): int64 =
  max(0'i64, value)

proc clampLocalInfo(info: var NodeResourceInfo) =
  info.cpu.coresLogical = max(0, info.cpu.coresLogical)
  info.cpu.frequencyMHz = max(0, info.cpu.frequencyMHz)
  info.gpu.computeScore = max(0, info.gpu.computeScore)

  info.memory.totalBytes = clampNonNegative(info.memory.totalBytes)
  info.memory.availableBytes = clampNonNegative(info.memory.availableBytes)
  if info.memory.totalBytes > 0 and info.memory.availableBytes > info.memory.totalBytes:
    info.memory.availableBytes = info.memory.totalBytes

  info.gpu.vramTotalBytes = clampNonNegative(info.gpu.vramTotalBytes)
  info.gpu.vramAvailableBytes = clampNonNegative(info.gpu.vramAvailableBytes)
  if info.gpu.availableKnown:
    if info.gpu.vramTotalBytes > 0 and info.gpu.vramAvailableBytes > info.gpu.vramTotalBytes:
      info.gpu.vramAvailableBytes = info.gpu.vramTotalBytes
  else:
    info.gpu.vramAvailableBytes = 0

  info.disk.totalBytes = clampNonNegative(info.disk.totalBytes)
  info.disk.availableBytes = clampNonNegative(info.disk.availableBytes)
  if info.disk.totalBytes > 0 and info.disk.availableBytes > info.disk.totalBytes:
    info.disk.availableBytes = info.disk.totalBytes

  info.bandwidth.uplinkBps = clampNonNegative(info.bandwidth.uplinkBps)
  info.bandwidth.downlinkBps = clampNonNegative(info.bandwidth.downlinkBps)

  if info.os.name.len == 0:
    info.os.name = hostOS
  if info.os.arch.len == 0:
    info.os.arch = hostCPU
  if info.cpu.model.len == 0:
    info.cpu.model = "unknown"
  if info.gpu.model.len == 0:
    info.gpu.model = "unknown"
  if info.gpu.vendor.len == 0:
    let inferred = inferGpuIdentity(info.gpu.model)
    info.gpu.vendor = inferred.vendor
    if info.gpu.computeClass.len == 0:
      info.gpu.computeClass = inferred.computeClass
    if info.gpu.computeScore == 0:
      info.gpu.computeScore = inferred.computeScore
  if info.disk.mountPath.len == 0:
    info.disk.mountPath = "/"
  if info.disk.`type`.len == 0:
    info.disk.`type` = "unknown"

proc applyEnvOverrides(
    info: var NodeResourceInfo,
    source: NodeResourceCollectorSource,
    fallbackDataDir: string
) =
  let nodeOs = source.envLookup("NODE_OS").strip()
  if nodeOs.len > 0:
    info.os.name = nodeOs

  let cpuCoresRaw = source.envLookup("CPU_CORES").strip()
  let cpuCores = parseIntFlexible(cpuCoresRaw)
  if cpuCoresRaw.len > 0 and cpuCores > 0:
    info.cpu.coresLogical = int(cpuCores)

  let cpuFreqRaw = source.envLookup("CPU_FREQ_MHZ").strip()
  let cpuFreq = parseIntFlexible(cpuFreqRaw)
  if cpuFreqRaw.len > 0 and cpuFreq > 0:
    info.cpu.frequencyMHz = int(cpuFreq)

  let memTotalRaw = source.envLookup("MEMORY_BYTES").strip()
  let memTotal = parseIntFlexible(memTotalRaw)
  if memTotalRaw.len > 0 and memTotal >= 0:
    info.memory.totalBytes = memTotal

  let memAvailRaw = source.envLookup("MEMORY_AVAILABLE_BYTES").strip()
  let memAvail = parseIntFlexible(memAvailRaw)
  if memAvailRaw.len > 0 and memAvail >= 0:
    info.memory.availableBytes = memAvail

  let gpuModel = source.envLookup("GPU_MODEL").strip()
  if gpuModel.len > 0:
    info.gpu.model = gpuModel

  let gpuVramRaw = source.envLookup("GPU_VRAM_BYTES").strip()
  let gpuVram = parseIntFlexible(gpuVramRaw)
  if gpuVramRaw.len > 0 and gpuVram >= 0:
    info.gpu.vramTotalBytes = gpuVram

  let gpuAvailRaw = source.envLookup("GPU_VRAM_AVAILABLE_BYTES").strip()
  if gpuAvailRaw.len > 0:
    info.gpu.vramAvailableBytes = parseIntFlexible(gpuAvailRaw)
    info.gpu.availableKnown = true

  let gpuScoreRaw = source.envLookup("GPU_COMPUTE_SCORE").strip()
  if gpuScoreRaw.len > 0:
    info.gpu.computeScore = int(parseIntFlexible(gpuScoreRaw))

  let diskTotalRaw = source.envLookup("DISK_TOTAL_BYTES").strip()
  let diskTotal = parseIntFlexible(diskTotalRaw)
  if diskTotalRaw.len > 0 and diskTotal >= 0:
    info.disk.totalBytes = diskTotal

  let diskAvailRaw = source.envLookup("DISK_AVAILABLE_BYTES").strip()
  let diskAvail = parseIntFlexible(diskAvailRaw)
  if diskAvailRaw.len > 0 and diskAvail >= 0:
    info.disk.availableBytes = diskAvail

  let upBpsRaw = source.envLookup("BANDWIDTH_UP_BPS").strip()
  let upBps = parseIntFlexible(upBpsRaw)
  if upBpsRaw.len > 0 and upBps >= 0:
    info.bandwidth.uplinkBps = upBps

  let downBpsRaw = source.envLookup("BANDWIDTH_DOWN_BPS").strip()
  let downBps = parseIntFlexible(downBpsRaw)
  if downBpsRaw.len > 0 and downBps >= 0:
    info.bandwidth.downlinkBps = downBps

  if info.disk.mountPath.len == 0:
    info.disk.mountPath = fallbackDataDir

  let inferred = inferGpuIdentity(info.gpu.model, info.gpu.vendor)
  if info.gpu.vendor.len == 0 or info.gpu.vendor == "unknown":
    info.gpu.vendor = inferred.vendor
  if info.gpu.computeClass.len == 0:
    info.gpu.computeClass = inferred.computeClass
  if info.gpu.computeScore == 0:
    info.gpu.computeScore = inferred.computeScore

proc collectLinuxResourceInfo(
    dataDir: string, source: NodeResourceCollectorSource
): NodeResourceInfo =
  result = defaultResourceInfo()
  result.os.name = "Linux"
  let osRelease = source.readText("/etc/os-release")
  for lineRaw in osRelease.splitLines():
    let line = lineRaw.strip()
    if line.startsWith("PRETTY_NAME="):
      result.os.version = line["PRETTY_NAME=".len ..< line.len].strip(chars = {'"', '\''})
      break
    elif result.os.version.len == 0 and line.startsWith("VERSION="):
      result.os.version = line["VERSION=".len ..< line.len].strip(chars = {'"', '\''})
  result.os.kernel = source.readText("/proc/sys/kernel/osrelease")
  if result.os.kernel.len == 0:
    result.os.kernel = source.readText("/proc/version")

  let cpuInfo = source.readText("/proc/cpuinfo")
  result.cpu.model = parseLinuxCpuModel(cpuInfo)
  result.cpu.coresLogical = parseLinuxCpuCores(cpuInfo)
  result.cpu.frequencyMHz = parseLinuxCpuFrequencyMHz(
    cpuInfo, source.readText("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq")
  )

  let mem = parseLinuxMemory(source.readText("/proc/meminfo"))
  result.memory.totalBytes = mem.totalBytes
  result.memory.availableBytes = mem.availableBytes

  let resolvedDir = source.resolvePath(dataDir)
  let stat = source.statVolume(resolvedDir)
  let mountInfo = findLinuxMountInfo(source.readText("/proc/mounts"), resolvedDir)
  result.disk.mountPath =
    if mountInfo.mountPoint.len > 0: mountInfo.mountPoint else: resolvedDir
  result.disk.`type` = mountInfo.fsType
  result.disk.totalBytes = stat.totalBytes
  result.disk.availableBytes = stat.availableBytes

  result.gpu = parseLinuxGpuInfo(source)
  result.bandwidth = parseLinuxBandwidth(source)

proc collectMacResourceInfo(
    dataDir: string, source: NodeResourceCollectorSource
): NodeResourceInfo =
  result = defaultResourceInfo()
  result.os.name = "macOS"
  result.os.version = source.exec("sw_vers", @["-productVersion"])
  if result.os.version.len == 0:
    result.os.version = source.exec("sysctl", @["-n", "kern.osproductversion"])
  result.os.kernel = source.exec("sysctl", @["-n", "kern.osrelease"])
  let machine = source.exec("sysctl", @["-n", "hw.machine"])
  if machine.len > 0:
    result.os.arch = machine

  result.cpu.model = source.exec("sysctl", @["-n", "machdep.cpu.brand_string"])
  result.cpu.coresLogical = int(parseIntFlexible(source.exec("sysctl", @["-n", "hw.logicalcpu"])))
  let cpuHz = parseIntFlexible(source.exec("sysctl", @["-n", "hw.cpufrequency"]))
  if cpuHz > 0:
    result.cpu.frequencyMHz = int(cpuHz div 1_000_000)

  result.memory.totalBytes = parseIntFlexible(source.exec("sysctl", @["-n", "hw.memsize"]))
  result.memory.availableBytes = parseMacVmAvailableBytes(source.exec("vm_stat", @[]))

  let resolvedDir = source.resolvePath(dataDir)
  let stat = source.statVolume(resolvedDir)
  let dfOutput = source.exec("df", @["-P", resolvedDir])
  result.disk.mountPath = parseMacDfMountPath(dfOutput)
  if result.disk.mountPath.len == 0:
    result.disk.mountPath = resolvedDir
  result.disk.totalBytes = stat.totalBytes
  result.disk.availableBytes = stat.availableBytes
  result.gpu = parseMacGpuInfo(source.exec("system_profiler", @["SPDisplaysDataType"]))

proc collectAndroidResourceInfo(
    dataDir: string, source: NodeResourceCollectorSource
): NodeResourceInfo =
  result = defaultResourceInfo()
  result.os.name = "Android"
  result.os.version = parseAndroidVersion(source)
  result.os.kernel = source.readText("/proc/sys/kernel/osrelease")
  if result.os.kernel.len == 0:
    result.os.kernel = "unknown"

  let cpuInfo = source.readText("/proc/cpuinfo")
  result.cpu.model = parseLinuxCpuModel(cpuInfo)
  result.cpu.coresLogical = parseLinuxCpuCores(cpuInfo)
  result.cpu.frequencyMHz = parseLinuxCpuFrequencyMHz(
    cpuInfo, source.readText("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq")
  )

  let mem = parseLinuxMemory(source.readText("/proc/meminfo"))
  result.memory.totalBytes = mem.totalBytes
  result.memory.availableBytes = mem.availableBytes

  let resolvedDir = source.resolvePath(dataDir)
  let stat = source.statVolume(resolvedDir)
  result.disk.mountPath = resolvedDir
  result.disk.`type` = "flash"
  result.disk.totalBytes = stat.totalBytes
  result.disk.availableBytes = stat.availableBytes

  result.bandwidth = parseLinuxBandwidth(source)

proc collectGenericResourceInfo(
    dataDir: string, source: NodeResourceCollectorSource
): NodeResourceInfo =
  result = defaultResourceInfo()
  result.disk.mountPath = source.resolvePath(dataDir)
  let stat = source.statVolume(result.disk.mountPath)
  result.disk.totalBytes = stat.totalBytes
  result.disk.availableBytes = stat.availableBytes

proc collectNodeResourceInfo*(
    dataDir: string, source: NodeResourceCollectorSource
): NodeResourceInfo =
  let src = effectiveSource(source)
  let resolvedDir = src.resolvePath(dataDir)
  case normalizePlatform(src.platform)
  of "android":
    result = collectAndroidResourceInfo(resolvedDir, src)
  of "linux":
    result = collectLinuxResourceInfo(resolvedDir, src)
  of "macos":
    result = collectMacResourceInfo(resolvedDir, src)
  else:
    result = collectGenericResourceInfo(resolvedDir, src)
  applyEnvOverrides(result, src, resolvedDir)
  result.updatedAtMs = src.nowMs()
  clampLocalInfo(result)

proc collectNodeResourceInfo*(dataDir: string): NodeResourceInfo =
  collectNodeResourceInfo(dataDir, defaultCollectorSource())

proc jsonGetStr(node: JsonNode, key: string, defaultValue = ""): string =
  if node.kind == JObject:
    let value = node.getOrDefault(key)
    if not value.isNil and value.kind == JString:
      return value.getStr()
  defaultValue

proc jsonGetBool(node: JsonNode, key: string, defaultValue = false): bool =
  if node.kind == JObject:
    let value = node.getOrDefault(key)
    if value.isNil:
      return defaultValue
    case value.kind
    of JBool:
      return value.getBool()
    of JInt:
      return value.getInt() != 0
    else:
      discard
  defaultValue

proc jsonGetInt64(node: JsonNode, key: string, defaultValue = 0'i64): int64 =
  if node.kind == JObject:
    let value = node.getOrDefault(key)
    if value.isNil:
      return defaultValue
    case value.kind
    of JInt:
      return value.getBiggestInt()
    of JFloat:
      return int64(value.getFloat())
    of JString:
      return parseIntFlexible(value.getStr())
    else:
      discard
  defaultValue

proc toJson(info: NodeResourceInfo): JsonNode =
  %* {
    "os": {
      "name": info.os.name,
      "version": info.os.version,
      "kernel": info.os.kernel,
      "arch": info.os.arch,
    },
    "cpu": {
      "model": info.cpu.model,
      "coresLogical": info.cpu.coresLogical,
      "frequencyMHz": info.cpu.frequencyMHz,
    },
    "memory": {
      "totalBytes": info.memory.totalBytes,
      "availableBytes": info.memory.availableBytes,
    },
    "gpu": {
      "model": info.gpu.model,
      "vendor": info.gpu.vendor,
      "vramTotalBytes": info.gpu.vramTotalBytes,
      "vramAvailableBytes": info.gpu.vramAvailableBytes,
      "computeClass": info.gpu.computeClass,
      "computeScore": info.gpu.computeScore,
      "availableKnown": info.gpu.availableKnown,
    },
    "disk": {
      "mountPath": info.disk.mountPath,
      "type": info.disk.`type`,
      "totalBytes": info.disk.totalBytes,
      "availableBytes": info.disk.availableBytes,
    },
    "bandwidth": {
      "uplinkBps": info.bandwidth.uplinkBps,
      "downlinkBps": info.bandwidth.downlinkBps,
    },
    "updatedAtMs": info.updatedAtMs,
  }

proc validateNodeResourceInfo(
    info: NodeResourceInfo, nowMs: int64, maxClockSkewMs: int64
): bool =
  if info.updatedAtMs <= 0:
    return false
  if abs(nowMs - info.updatedAtMs) > maxClockSkewMs:
    return false

  if info.cpu.coresLogical < 0 or info.cpu.coresLogical > 1_000_000:
    return false
  if info.cpu.frequencyMHz < 0 or info.cpu.frequencyMHz > 100_000_000:
    return false
  if info.gpu.computeScore < 0 or info.gpu.computeScore > 1_000_000:
    return false

  let byteFields = [
    info.memory.totalBytes,
    info.memory.availableBytes,
    info.gpu.vramTotalBytes,
    info.gpu.vramAvailableBytes,
    info.disk.totalBytes,
    info.disk.availableBytes,
    info.bandwidth.uplinkBps,
    info.bandwidth.downlinkBps,
  ]
  for value in byteFields:
    if value < 0 or value > MaxNodeResourceBytes:
      return false

  if info.memory.totalBytes > 0 and info.memory.availableBytes > info.memory.totalBytes:
    return false
  if info.gpu.availableKnown and info.gpu.vramTotalBytes > 0 and
      info.gpu.vramAvailableBytes > info.gpu.vramTotalBytes:
    return false
  if info.disk.totalBytes > 0 and info.disk.availableBytes > info.disk.totalBytes:
    return false
  true

proc decodeNodeResourceInfo(
    payload: seq[byte], nowMs: int64, maxClockSkewMs: int64
): Option[NodeResourceInfo] =
  try:
    let root = parseJson(bytesToString(payload))
    if root.kind != JObject:
      return none(NodeResourceInfo)

    let osNode = root.getOrDefault("os")
    let cpuNode = root.getOrDefault("cpu")
    let memNode = root.getOrDefault("memory")
    let gpuNode = root.getOrDefault("gpu")
    let diskNode = root.getOrDefault("disk")
    let bwNode = root.getOrDefault("bandwidth")

    var info = NodeResourceInfo(
      os: NodeOsInfo(
        name: jsonGetStr(osNode, "name"),
        version: jsonGetStr(osNode, "version"),
        kernel: jsonGetStr(osNode, "kernel"),
        arch: jsonGetStr(osNode, "arch"),
      ),
      cpu: NodeCpuInfo(
        model: jsonGetStr(cpuNode, "model"),
        coresLogical: int(jsonGetInt64(cpuNode, "coresLogical")),
        frequencyMHz: int(jsonGetInt64(cpuNode, "frequencyMHz")),
      ),
      memory: NodeMemoryInfo(
        totalBytes: jsonGetInt64(memNode, "totalBytes"),
        availableBytes: jsonGetInt64(memNode, "availableBytes"),
      ),
      gpu: NodeGpuInfo(
        model: jsonGetStr(gpuNode, "model"),
        vendor: jsonGetStr(gpuNode, "vendor"),
        vramTotalBytes: jsonGetInt64(gpuNode, "vramTotalBytes"),
        vramAvailableBytes: jsonGetInt64(gpuNode, "vramAvailableBytes"),
        computeClass: jsonGetStr(gpuNode, "computeClass"),
        computeScore: int(jsonGetInt64(gpuNode, "computeScore")),
        availableKnown: jsonGetBool(gpuNode, "availableKnown"),
      ),
      disk: NodeDiskInfo(
        mountPath: jsonGetStr(diskNode, "mountPath"),
        `type`: jsonGetStr(diskNode, "type"),
        totalBytes: jsonGetInt64(diskNode, "totalBytes"),
        availableBytes: jsonGetInt64(diskNode, "availableBytes"),
      ),
      bandwidth: NodeBandwidthInfo(
        uplinkBps: jsonGetInt64(bwNode, "uplinkBps"),
        downlinkBps: jsonGetInt64(bwNode, "downlinkBps"),
      ),
      updatedAtMs: jsonGetInt64(root, "updatedAtMs"),
    )
    if not validateNodeResourceInfo(info, nowMs, maxClockSkewMs):
      return none(NodeResourceInfo)
    if info.gpu.vendor.len == 0 or info.gpu.computeClass.len == 0 or info.gpu.computeScore == 0:
      let inferred = inferGpuIdentity(info.gpu.model, info.gpu.vendor)
      if info.gpu.vendor.len == 0:
        info.gpu.vendor = inferred.vendor
      if info.gpu.computeClass.len == 0:
        info.gpu.computeClass = inferred.computeClass
      if info.gpu.computeScore == 0:
        info.gpu.computeScore = inferred.computeScore
    some(info)
  except CatchableError:
    none(NodeResourceInfo)

proc new*(
    T: typedesc[NodeResourceRegistry],
    ttl: Duration = 5.minutes,
    maxEntries = 256,
): T =
  T(
    ttlMs: max(0'i64, durationToMillis(ttl)),
    maxEntries: max(1, maxEntries),
    entries: initTable[PeerId, NodeResourceSnapshot](),
  )

proc cleanup*(
    registry: NodeResourceRegistry, nowMs: int64 = nowMillis()
) =
  if registry.isNil:
    return
  var expired: seq[PeerId] = @[]
  for peerId, snapshot in registry.entries.pairs():
    if registry.ttlMs > 0 and nowMs - snapshot.lastSeenMs > registry.ttlMs:
      expired.add(peerId)
  for peerId in expired:
    registry.entries.del(peerId)

proc len*(registry: NodeResourceRegistry): int =
  if registry.isNil:
    return 0
  registry.entries.len

proc evictOldest(registry: NodeResourceRegistry) =
  var oldestPeer: Option[PeerId]
  var oldestSeen = high(int64)
  for peerId, snapshot in registry.entries.pairs():
    if snapshot.lastSeenMs < oldestSeen:
      oldestSeen = snapshot.lastSeenMs
      oldestPeer = some(peerId)
  if oldestPeer.isSome():
    registry.entries.del(oldestPeer.get())

proc put*(
    registry: NodeResourceRegistry,
    peerId: PeerId,
    info: NodeResourceInfo,
    nowMs: int64 = nowMillis(),
) =
  if registry.isNil:
    return
  registry.cleanup(nowMs)
  registry.entries[peerId] = NodeResourceSnapshot(
    peerId: peerId, resource: info, lastSeenMs: nowMs
  )
  while registry.entries.len > registry.maxEntries:
    registry.cleanup(nowMs)
    if registry.entries.len <= registry.maxEntries:
      break
    registry.evictOldest()

proc get*(
    registry: NodeResourceRegistry, peerId: PeerId, nowMs: int64 = nowMillis()
): Option[NodeResourceSnapshot] =
  if registry.isNil:
    return none(NodeResourceSnapshot)
  registry.cleanup(nowMs)
  try:
    some(registry.entries[peerId])
  except KeyError:
    none(NodeResourceSnapshot)

proc snapshots*(
    registry: NodeResourceRegistry, nowMs: int64 = nowMillis()
): seq[NodeResourceSnapshot] =
  if registry.isNil:
    return @[]
  registry.cleanup(nowMs)
  for snapshot in registry.entries.values():
    result.add(snapshot)

proc init*(
    _: type NodeResourceServiceConfig,
    codec = NodeResourceCodec,
    syncInterval = 30.seconds,
    requestTimeout = 5.seconds,
    localRefreshInterval = 30.seconds,
    registryTtl = 5.minutes,
    registryMaxEntries = 256,
    maxRequestBytes = 64,
    maxResponseBytes = DefaultNodeResourceMaxMessageBytes,
    syncBatchSize = 1,
    maxClockSkew = 24.hours,
): NodeResourceServiceConfig =
  NodeResourceServiceConfig(
    codec: codec,
    syncInterval: syncInterval,
    requestTimeout: requestTimeout,
    localRefreshInterval: localRefreshInterval,
    registryTtl: registryTtl,
    registryMaxEntries: max(1, registryMaxEntries),
    maxRequestBytes: max(1, maxRequestBytes),
    maxResponseBytes: max(1, maxResponseBytes),
    syncBatchSize: max(1, syncBatchSize),
    maxClockSkew: maxClockSkew,
  )

proc new*(
    T: typedesc[NodeResourceService],
    dataDir = "",
    config: NodeResourceServiceConfig = NodeResourceServiceConfig.init(),
    source: NodeResourceCollectorSource = NodeResourceCollectorSource(),
): T =
  let effectiveDataDir =
    if dataDir.len > 0:
      dataDir
    else:
      try:
        getCurrentDir()
      except CatchableError:
        "/"
  T(
    dataDir: effectiveDataDir,
    config: config,
    registry: NodeResourceRegistry.new(
      ttl = config.registryTtl, maxEntries = config.registryMaxEntries
    ),
    source: effectiveSource(source),
  )

proc getNodeResourceService*(switch: Switch): NodeResourceService =
  if switch.isNil:
    return nil
  for service in switch.services:
    if service of NodeResourceService:
      return NodeResourceService(service)

proc refreshLocalSnapshot(
    svc: NodeResourceService, force = false
) =
  if svc.isNil:
    return
  let nowMs = svc.source.nowMs()
  let refreshEvery = durationToMillis(svc.config.localRefreshInterval)
  if not force and svc.localInfo.updatedAtMs > 0 and refreshEvery > 0 and
      nowMs - svc.lastLocalRefreshMs < refreshEvery:
    return
  svc.localInfo = collectNodeResourceInfo(svc.dataDir, svc.source)
  svc.lastLocalRefreshMs = nowMs

proc openNodeResourceConn(
    svc: NodeResourceService, peerId: PeerId
): Future[Connection] {.async: (raises: [CancelledError]).} =
  try:
    if svc.switch.isConnected(peerId):
      return await svc.switch.dial(peerId, @[svc.config.codec])

    let protocols = svc.switch.peerStore[ProtoBook][peerId]
    if svc.config.codec notin protocols:
      return nil
    let addrs = svc.switch.peerStore.getAddresses(peerId)
    if addrs.len == 0:
      return nil
    return await svc.switch.dial(peerId, addrs, @[svc.config.codec], forceDial = false)
  except CancelledError as exc:
    raise exc
  except CatchableError:
    return nil

proc requestResourceFromPeer(
    svc: NodeResourceService, peerId: PeerId
): Future[Option[NodeResourceInfo]] {.async: (raises: [CancelledError]).} =
  let conn = await svc.openNodeResourceConn(peerId)
  if conn.isNil:
    return none(NodeResourceInfo)

  defer:
    try:
      await conn.close()
    except CatchableError:
      discard

  try:
    await conn.writeLp(stringToBytes(NodeResourceRequestToken))
    let readFuture = conn.readLp(svc.config.maxResponseBytes)
    let completed = await withTimeout(readFuture, svc.config.requestTimeout)
    if not completed:
      if not readFuture.finished():
        readFuture.cancelSoon()
      return none(NodeResourceInfo)
    let payload = readFuture.read()
    return decodeNodeResourceInfo(
      payload,
      svc.source.nowMs(),
      min(MaxNodeResourceTimestampSkewMs, durationToMillis(svc.config.maxClockSkew)),
    )
  except CancelledError as exc:
    raise exc
  except CatchableError:
    none(NodeResourceInfo)

proc syncCandidates(svc: NodeResourceService): seq[PeerId] =
  if svc.isNil or svc.switch.isNil:
    return @[]
  var seen = initTable[string, bool]()
  template addPeer(candidate: PeerId) =
    let key = $candidate
    if key.len == 0 or seen.hasKey(key):
      discard
    elif candidate == svc.switch.peerInfo.peerId:
      discard
    else:
      seen[key] = true
      result.add(candidate)

  for peerId in svc.switch.connectedPeers(In):
    addPeer(peerId)
  for peerId in svc.switch.connectedPeers(Out):
    addPeer(peerId)
  let protoBook = svc.switch.peerStore[ProtoBook]
  if not protoBook.isNil:
    for peerId, protos in protoBook.book.pairs():
      if svc.config.codec in protos:
        addPeer(peerId)

proc hostNodeResourceSyncTick*(
    svc: NodeResourceService
): Future[void] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return
  svc.refreshLocalSnapshot()
  let nowMs = svc.source.nowMs()
  svc.registry.cleanup(nowMs)
  let peers = svc.syncCandidates()
  if peers.len == 0:
    return
  let batch = min(svc.config.syncBatchSize, peers.len)
  for offset in 0 ..< batch:
    let idx = (svc.syncCursor + offset) mod peers.len
    let peerId = peers[idx]
    let infoOpt = await svc.requestResourceFromPeer(peerId)
    if infoOpt.isSome():
      svc.registry.put(peerId, infoOpt.get(), nowMs)
  svc.syncCursor = (svc.syncCursor + batch) mod peers.len

proc hostNodeResourceUpdateAll*(
    svc: NodeResourceService
): Future[void] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return
  svc.refreshLocalSnapshot(force = true)
  let nowMs = svc.source.nowMs()
  svc.registry.cleanup(nowMs)
  for peerId in svc.syncCandidates():
    let infoOpt = await svc.requestResourceFromPeer(peerId)
    if infoOpt.isSome():
      svc.registry.put(peerId, infoOpt.get(), nowMs)

proc syncLoop(svc: NodeResourceService) {.async: (raises: [CancelledError]).} =
  heartbeat "node-resource-sync", svc.config.syncInterval:
    await svc.hostNodeResourceSyncTick()

proc buildProtocol(svc: NodeResourceService): LPProtocol =
  let handler =
    proc(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
      try:
        let payload = await conn.readLp(svc.config.maxRequestBytes)
        if bytesToString(payload) != NodeResourceRequestToken:
          return
        svc.refreshLocalSnapshot(force = true)
        await conn.writeLp(stringToBytes($toJson(svc.localInfo)))
      except CancelledError as exc:
        raise exc
      except CatchableError:
        discard
  LPProtocol.new(@[svc.config.codec], handler)

method setup*(
    svc: NodeResourceService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(svc).setup(switch)
  if hasBeenSetup:
    svc.switch = switch
    svc.registry = NodeResourceRegistry.new(
      ttl = svc.config.registryTtl, maxEntries = svc.config.registryMaxEntries
    )
    svc.source = effectiveSource(svc.source)
    svc.refreshLocalSnapshot(force = true)
    if svc.protocol.isNil:
      svc.protocol = svc.buildProtocol()
    if switch.isStarted() and not svc.protocol.started:
      await svc.protocol.start()
    if not svc.mounted:
      try:
        switch.mount(svc.protocol)
        svc.mounted = true
      except CatchableError as exc:
        warn "failed to mount node-resource protocol", error = exc.msg
        discard await procCall Service(svc).stop(switch)
        return false
    await svc.run(switch)
  hasBeenSetup

method run*(
    svc: NodeResourceService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  if svc.running:
    return
  svc.running = true
  if svc.config.syncInterval > ZeroDuration:
    svc.runner = svc.syncLoop()

method stop*(
    svc: NodeResourceService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(svc).stop(switch)
  if hasBeenStopped:
    svc.running = false
    if not svc.runner.isNil:
      await svc.runner.cancelAndWait()
      svc.runner = nil
    if not svc.protocol.isNil and svc.protocol.started:
      await svc.protocol.stop()
  hasBeenStopped

proc startNodeResourceService*(
    switch: Switch,
    dataDir = "",
    config: NodeResourceServiceConfig = NodeResourceServiceConfig.init(),
    source: NodeResourceCollectorSource = NodeResourceCollectorSource(),
): Future[NodeResourceService] {.async: (raises: [CancelledError]).} =
  if switch.isNil:
    return nil
  var svc = getNodeResourceService(switch)
  if svc.isNil:
    svc = NodeResourceService.new(dataDir = dataDir, config = config, source = source)
    switch.services.add(svc)
  if switch.isStarted():
    discard await svc.setup(switch)
  svc

proc getLocalNodeResource*(svc: NodeResourceService): NodeResourceInfo =
  if svc.isNil:
    return defaultResourceInfo()
  svc.refreshLocalSnapshot()
  svc.localInfo

proc getLocalNodeResource*(switch: Switch): Option[NodeResourceInfo] =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return none(NodeResourceInfo)
  some(svc.getLocalNodeResource())

proc getRemoteNodeResource*(
    svc: NodeResourceService, peerId: PeerId
): Option[NodeResourceSnapshot] =
  if svc.isNil:
    return none(NodeResourceSnapshot)
  svc.registry.get(peerId, svc.source.nowMs())

proc getRemoteNodeResource*(
    switch: Switch, peerId: PeerId
): Option[NodeResourceSnapshot] =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return none(NodeResourceSnapshot)
  svc.getRemoteNodeResource(peerId)

proc allSnapshots(svc: NodeResourceService): seq[NodeResourceSnapshot] =
  if svc.isNil:
    return @[]
  let local = svc.getLocalNodeResource()
  if not svc.switch.isNil:
    result.add(
      NodeResourceSnapshot(
        peerId: svc.switch.peerInfo.peerId,
        resource: local,
        lastSeenMs: local.updatedAtMs,
      )
    )
  result.add(svc.registry.snapshots(svc.source.nowMs()))

proc nodeResourceTotals*(svc: NodeResourceService): NodeResourceTotals =
  if svc.isNil:
    return
  for snapshot in svc.allSnapshots():
    let resource = snapshot.resource
    inc result.peers
    if hasGpu(resource):
      inc result.peersWithGpu
    result.cpuCoresLogical += int64(resource.cpu.coresLogical)
    result.memoryTotalBytes += resource.memory.totalBytes
    result.memoryAvailableBytes += resource.memory.availableBytes
    result.gpuVramTotalBytes += resource.gpu.vramTotalBytes
    if resource.gpu.availableKnown:
      result.gpuVramAvailableBytes += resource.gpu.vramAvailableBytes
    result.diskTotalBytes += resource.disk.totalBytes
    result.diskAvailableBytes += resource.disk.availableBytes
    result.bandwidthUplinkBps += resource.bandwidth.uplinkBps
    result.bandwidthDownlinkBps += resource.bandwidth.downlinkBps

proc nodeResourceTotals*(switch: Switch): NodeResourceTotals =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return
  svc.nodeResourceTotals()

proc nodeResourceSnapshots*(svc: NodeResourceService): seq[NodeResourceSnapshot] =
  if svc.isNil:
    return @[]
  svc.allSnapshots()

proc nodeResourceSnapshots*(switch: Switch): seq[NodeResourceSnapshot] =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return @[]
  svc.nodeResourceSnapshots()

proc formatBytes(bytes: int64): string =
  let units = ["B", "KiB", "MiB", "GiB", "TiB"]
  var value = float64(max(0'i64, bytes))
  var idx = 0
  while value >= 1024.0 and idx < units.high:
    value /= 1024.0
    inc idx
  if idx == 0:
    $int64(value) & " " & units[idx]
  else:
    formatFloat(value, ffDecimal, 1) & " " & units[idx]

proc formatBps(bps: int64): string =
  let units = ["bps", "Kbps", "Mbps", "Gbps", "Tbps"]
  var value = float64(max(0'i64, bps))
  var idx = 0
  while value >= 1000.0 and idx < units.high:
    value /= 1000.0
    inc idx
  if idx == 0:
    $int64(value) & " " & units[idx]
  else:
    formatFloat(value, ffDecimal, 1) & " " & units[idx]

proc nodeResourceTotalsText*(svc: NodeResourceService): string =
  let totals = svc.nodeResourceTotals()
  "peers=" & $totals.peers & " gpuPeers=" & $totals.peersWithGpu &
    " cpuCores=" & $totals.cpuCoresLogical &
    " memAvail=" & formatBytes(totals.memoryAvailableBytes) &
    " gpuVramAvail=" & formatBytes(totals.gpuVramAvailableBytes) &
    " diskAvail=" & formatBytes(totals.diskAvailableBytes) &
    " downlink=" & formatBps(totals.bandwidthDownlinkBps)

proc nodeResourceTotalsText*(switch: Switch): string =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return "peers=0 gpuPeers=0 cpuCores=0 memAvail=0 B gpuVramAvail=0 B diskAvail=0 B downlink=0 bps"
  svc.nodeResourceTotalsText()

proc compareSnapshots(a, b: NodeResourceSnapshot): int =
  if a.resource.gpu.computeScore != b.resource.gpu.computeScore:
    return cmp(b.resource.gpu.computeScore, a.resource.gpu.computeScore)
  if a.resource.memory.availableBytes != b.resource.memory.availableBytes:
    return cmp(b.resource.memory.availableBytes, a.resource.memory.availableBytes)
  if a.resource.disk.availableBytes != b.resource.disk.availableBytes:
    return cmp(b.resource.disk.availableBytes, a.resource.disk.availableBytes)
  cmp(b.resource.bandwidth.downlinkBps, a.resource.bandwidth.downlinkBps)

proc findResourceCandidates*(
    svc: NodeResourceService,
    requireGpu = false,
    minMemoryBytes = 0'i64,
    minVramBytes = 0'i64,
    minDiskAvailableBytes = 0'i64,
    minBandwidthBps = 0'i64,
): seq[NodeResourceSnapshot] =
  if svc.isNil:
    return @[]
  for snapshot in svc.allSnapshots():
    let resource = snapshot.resource
    if requireGpu and not hasGpu(resource):
      continue
    if resource.memory.availableBytes < minMemoryBytes:
      continue
    if resource.gpu.vramTotalBytes < minVramBytes:
      continue
    if resource.disk.availableBytes < minDiskAvailableBytes:
      continue
    if resource.bandwidth.downlinkBps < minBandwidthBps:
      continue
    result.add(snapshot)
  result.sort(compareSnapshots)

proc findResourceCandidates*(
    switch: Switch,
    requireGpu = false,
    minMemoryBytes = 0'i64,
    minVramBytes = 0'i64,
    minDiskAvailableBytes = 0'i64,
    minBandwidthBps = 0'i64,
): seq[NodeResourceSnapshot] =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return @[]
  svc.findResourceCandidates(
    requireGpu = requireGpu,
    minMemoryBytes = minMemoryBytes,
    minVramBytes = minVramBytes,
    minDiskAvailableBytes = minDiskAvailableBytes,
    minBandwidthBps = minBandwidthBps,
  )

proc hostNodeResourceUpdateAll*(
    switch: Switch
): Future[void] {.async: (raises: [CancelledError]).} =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return
  await svc.hostNodeResourceUpdateAll()

proc hostNodeResourceSyncTick*(
    switch: Switch
): Future[void] {.async: (raises: [CancelledError]).} =
  let svc = getNodeResourceService(switch)
  if svc.isNil:
    return
  await svc.hostNodeResourceSyncTick()

{.pop.}
