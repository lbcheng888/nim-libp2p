{.used.}

import std/[options, os, sequtils, strutils, tables]
import chronos
import unittest2

import ./helpers
import ../libp2p/[builders, switch]
import ../libp2p/protocols/protocol
import ../libp2p/services/noderesourceservice
import ../libp2p/stream/connection
import ../libp2p/stream/lpstream

const
  GiB = 1024'i64 * 1024 * 1024
  TiB = 1024'i64 * 1024 * 1024 * 1024
  FixedNow = 1_700_000_000_000'i64
  FixtureDir = currentSourcePath.parentDir / "fixtures" / "node_resource"

proc fixture(path: string): string =
  readFile(FixtureDir / path).strip()

proc execKey(command: string, args: seq[string]): string =
  command & "|" & args.join("|")

proc toBytes(text: string): seq[byte] =
  text.toSeq().mapIt(byte(it))

proc buildSource(
    platform: string,
    readPairs: openArray[(string, string)] = [],
    dirPairs: openArray[(string, seq[string])] = [],
    execPairs: openArray[(string, string)] = [],
    envPairs: openArray[(string, string)] = [],
    totalBytes = 0'i64,
    availableBytes = 0'i64,
    nowMs = FixedNow,
): NodeResourceCollectorSource =
  var readMap = initTable[string, string]()
  var dirMap = initTable[string, seq[string]]()
  var execMap = initTable[string, string]()
  var envMap = initTable[string, string]()
  for entry in readPairs:
    readMap[entry[0]] = entry[1]
  for entry in dirPairs:
    dirMap[entry[0]] = entry[1]
  for entry in execPairs:
    execMap[entry[0]] = entry[1]
  for entry in envPairs:
    envMap[entry[0]] = entry[1]

  result.platform = platform
  result.readText = proc(path: string): string =
    readMap.getOrDefault(path, "").strip()
  result.listDirs = proc(path: string): seq[string] =
    dirMap.getOrDefault(path, @[])
  result.exec = proc(command: string, args: seq[string]): string =
    execMap.getOrDefault(execKey(command, args), "").strip()
  result.envLookup = proc(name: string): string =
    envMap.getOrDefault(name, "").strip()
  result.statVolume = proc(path: string): NodeResourceDiskStat =
    NodeResourceDiskStat(totalBytes: totalBytes, availableBytes: availableBytes)
  result.resolvePath = proc(path: string): string =
    if path.len > 0:
      path
    else:
      "/"
  result.nowMs = proc(): int64 =
    nowMs

proc makeMemorySwitch(): Switch =
  newStandardSwitch(transport = TransportType.Memory)

proc makeInfo(
    updatedAtMs: int64,
    peerHasGpu = true,
    computeScore = 100,
    memoryAvailableBytes = 32 * GiB,
    vramBytes = 16 * GiB,
    diskAvailableBytes = 256 * GiB,
    downlinkBps = 1_000_000_000'i64,
): NodeResourceInfo =
  NodeResourceInfo(
    os: NodeOsInfo(name: "Linux", version: "test", kernel: "test", arch: "amd64"),
    cpu: NodeCpuInfo(model: "test-cpu", coresLogical: 16, frequencyMHz: 3200),
    memory: NodeMemoryInfo(totalBytes: 64 * GiB, availableBytes: memoryAvailableBytes),
    gpu:
      if peerHasGpu:
        NodeGpuInfo(
          model: "test-gpu",
          vendor: "NVIDIA",
          vramTotalBytes: vramBytes,
          vramAvailableBytes: vramBytes,
          computeClass: "test",
          computeScore: computeScore,
          availableKnown: true,
        )
      else:
        NodeGpuInfo(model: "unknown", vendor: "unknown"),
    disk: NodeDiskInfo(
      mountPath: "/data",
      `type`: "ext4",
      totalBytes: TiB,
      availableBytes: diskAvailableBytes,
    ),
    bandwidth: NodeBandwidthInfo(
      uplinkBps: downlinkBps div 2, downlinkBps: downlinkBps
    ),
    updatedAtMs: updatedAtMs,
  )

suite "Node resource service":
  teardown:
    checkTrackers()

  test "collector_linux parses fixture CPU memory disk gpu bandwidth":
    let source = buildSource(
      "linux",
      readPairs = [
        ("/proc/cpuinfo", fixture("linux/proc_cpuinfo.txt")),
        ("/proc/meminfo", fixture("linux/proc_meminfo.txt")),
        ("/proc/mounts", fixture("linux/proc_mounts.txt")),
        ("/proc/net/dev", fixture("linux/proc_net_dev.txt")),
        ("/proc/net/route", fixture("linux/proc_net_route.txt")),
        (
          "/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq",
          fixture("linux/sys_cpuinfo_max_freq.txt"),
        ),
        (
          "/proc/driver/nvidia/gpus/0000:01:00.0/information",
          fixture("linux/nvidia_information.txt"),
        ),
        ("/sys/class/net/eth0/speed", fixture("linux/sys_net_eth0_speed.txt")),
      ],
      dirPairs = [
        (
          "/proc/driver/nvidia/gpus",
          @["/proc/driver/nvidia/gpus/0000:01:00.0"],
        )
      ],
      totalBytes = 4 * TiB,
      availableBytes = 3 * TiB,
    )

    let info = collectNodeResourceInfo("/mnt/models/cache", source)
    check info.os.name == "Linux"
    check info.cpu.model == "AMD EPYC 7B13"
    check info.cpu.coresLogical == 4
    check info.cpu.frequencyMHz == 2450
    check info.memory.totalBytes == 131_900_024'i64 * 1024
    check info.memory.availableBytes == 101_234_568'i64 * 1024
    check info.disk.mountPath == "/mnt/models"
    check info.disk.`type` == "ext4"
    check info.disk.totalBytes == 4 * TiB
    check info.disk.availableBytes == 3 * TiB
    check info.gpu.model == "NVIDIA H100 SXM5 80GB"
    check info.gpu.vendor == "NVIDIA"
    check info.gpu.computeClass == "nvidia-hopper"
    check info.gpu.computeScore == 1000
    check info.bandwidth.uplinkBps == 10_000_000_000'i64
    check info.bandwidth.downlinkBps == 10_000_000_000'i64

  test "collector_macos parses fixture CPU memory gpu":
    let dataDir = "/Volumes/Models/cache"
    let source = buildSource(
      "macos",
      execPairs = [
        (execKey("sw_vers", @["-productVersion"]), fixture("mac/sw_vers_product_version.txt")),
        (execKey("sysctl", @["-n", "kern.osrelease"]), fixture("mac/sysctl_kern_osrelease.txt")),
        (execKey("sysctl", @["-n", "hw.machine"]), fixture("mac/sysctl_hw_machine.txt")),
        (
          execKey("sysctl", @["-n", "machdep.cpu.brand_string"]),
          fixture("mac/sysctl_cpu_brand.txt"),
        ),
        (execKey("sysctl", @["-n", "hw.logicalcpu"]), fixture("mac/sysctl_hw_logicalcpu.txt")),
        (execKey("sysctl", @["-n", "hw.cpufrequency"]), fixture("mac/sysctl_hw_cpufrequency.txt")),
        (execKey("sysctl", @["-n", "hw.memsize"]), fixture("mac/sysctl_hw_memsize.txt")),
        (execKey("vm_stat", @[]), fixture("mac/vm_stat.txt")),
        (
          execKey("system_profiler", @["SPDisplaysDataType"]),
          fixture("mac/system_profiler_displays.txt"),
        ),
        (execKey("df", @["-P", dataDir]), fixture("mac/df_models.txt")),
      ],
      totalBytes = TiB,
      availableBytes = 512 * GiB,
    )

    let info = collectNodeResourceInfo(dataDir, source)
    check info.os.name == "macOS"
    check info.os.version == "15.3"
    check info.os.kernel == "24.3.0"
    check info.os.arch == "arm64"
    check info.cpu.model == "Apple M3 Max"
    check info.cpu.coresLogical == 16
    check info.cpu.frequencyMHz == 4050
    check info.memory.totalBytes == 68_719_476_736'i64
    check info.memory.availableBytes == 2_621_440_000'i64
    check info.disk.mountPath == "/Volumes/Models"
    check info.disk.totalBytes == TiB
    check info.disk.availableBytes == 512 * GiB
    check info.gpu.model == "Apple M3 Max"
    check info.gpu.vendor == "Apple"
    check info.gpu.vramTotalBytes == 48 * GiB
    check info.gpu.computeClass == "apple-mseries"
    check info.gpu.computeScore == 450

  test "registry eviction and ttl cleanup work":
    let registry = NodeResourceRegistry.new(ttl = 100.milliseconds, maxEntries = 2)
    let peer0 = makeMemorySwitch().peerInfo.peerId
    let peer1 = makeMemorySwitch().peerInfo.peerId
    let peer2 = makeMemorySwitch().peerInfo.peerId

    registry.put(peer0, makeInfo(FixedNow), nowMs = 1_000)
    registry.put(peer1, makeInfo(FixedNow + 1), nowMs = 1_050)
    registry.put(peer2, makeInfo(FixedNow + 2), nowMs = 1_090)

    check registry.get(peer0, nowMs = 1_090).isNone()
    check registry.get(peer1, nowMs = 1_090).isSome()
    check registry.get(peer2, nowMs = 1_090).isSome()

    registry.cleanup(nowMs = 1_250)
    check registry.len == 0

  test "dataDir volume semantics follow data volume mount":
    let source = buildSource(
      "linux",
      readPairs = [
        ("/proc/mounts", fixture("linux/proc_mounts_volume.txt"))
      ],
      totalBytes = 500 * GiB,
      availableBytes = 200 * GiB,
    )

    let info = collectNodeResourceInfo("/srv/models/cache", source)
    check info.disk.mountPath == "/srv/models"
    check info.disk.totalBytes == 500 * GiB
    check info.disk.availableBytes == 200 * GiB

  asyncTest "node-resource protocol smoke":
    let client = makeMemorySwitch()
    let server = makeMemorySwitch()
    await client.start()
    await server.start()
    defer:
      await client.stop()
      await server.stop()

    discard await startNodeResourceService(
      client,
      dataDir = "/client",
      source = buildSource(
        "linux",
        envPairs = [
          ("CPU_CORES", "8"),
          ("MEMORY_BYTES", $(32 * GiB)),
          ("MEMORY_AVAILABLE_BYTES", $(24 * GiB)),
          ("DISK_TOTAL_BYTES", $(256 * GiB)),
          ("DISK_AVAILABLE_BYTES", $(200 * GiB)),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    let serverSvc = await startNodeResourceService(
      server,
      dataDir = "/srv/models",
      source = buildSource(
        "linux",
        envPairs = [
          ("CPU_CORES", "64"),
          ("CPU_FREQ_MHZ", "3200"),
          ("MEMORY_BYTES", $(256 * GiB)),
          ("MEMORY_AVAILABLE_BYTES", $(192 * GiB)),
          ("GPU_MODEL", "NVIDIA H100 SXM5 80GB"),
          ("GPU_VRAM_BYTES", $(80 * GiB)),
          ("GPU_VRAM_AVAILABLE_BYTES", $(70 * GiB)),
          ("GPU_COMPUTE_SCORE", "1000"),
          ("DISK_TOTAL_BYTES", $(4 * TiB)),
          ("DISK_AVAILABLE_BYTES", $(3 * TiB)),
          ("BANDWIDTH_UP_BPS", "10000000000"),
          ("BANDWIDTH_DOWN_BPS", "25000000000"),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    check serverSvc.getLocalNodeResource().gpu.computeScore == 1000

    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)
    await hostNodeResourceUpdateAll(client)

    let remote = client.getRemoteNodeResource(server.peerInfo.peerId)
    check remote.isSome()
    let snapshot = remote.get()
    check snapshot.resource.cpu.coresLogical == 64
    check snapshot.resource.gpu.computeScore == 1000
    check snapshot.resource.memory.availableBytes == 192 * GiB
    check snapshot.resource.disk.availableBytes == 3 * TiB
    check snapshot.resource.bandwidth.downlinkBps == 25_000_000_000'i64

  asyncTest "node-resource sync smoke":
    let a = makeMemorySwitch()
    let b = makeMemorySwitch()
    let c = makeMemorySwitch()
    await a.start()
    await b.start()
    await c.start()
    defer:
      await a.stop()
      await b.stop()
      await c.stop()

    discard await startNodeResourceService(
      a,
      dataDir = "/a",
      source = buildSource(
        "linux",
        envPairs = [
          ("CPU_CORES", "8"),
          ("MEMORY_AVAILABLE_BYTES", $(20 * GiB)),
          ("DISK_AVAILABLE_BYTES", $(100 * GiB)),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 50.milliseconds),
    )
    discard await startNodeResourceService(
      b,
      dataDir = "/b",
      source = buildSource(
        "linux",
        envPairs = [
          ("CPU_CORES", "16"),
          ("MEMORY_AVAILABLE_BYTES", $(40 * GiB)),
          ("GPU_MODEL", "RTX 4090"),
          ("GPU_VRAM_BYTES", $(24 * GiB)),
          ("GPU_COMPUTE_SCORE", "800"),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 50.milliseconds),
    )
    discard await startNodeResourceService(
      c,
      dataDir = "/c",
      source = buildSource(
        "linux",
        envPairs = [
          ("CPU_CORES", "32"),
          ("MEMORY_AVAILABLE_BYTES", $(60 * GiB)),
          ("GPU_MODEL", "A100"),
          ("GPU_VRAM_BYTES", $(40 * GiB)),
          ("GPU_COMPUTE_SCORE", "900"),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 50.milliseconds),
    )

    await a.connect(b.peerInfo.peerId, b.peerInfo.addrs)
    await a.connect(c.peerInfo.peerId, c.peerInfo.addrs)

    checkUntilTimeoutCustom(2.seconds, 50.milliseconds):
      a.getRemoteNodeResource(b.peerInfo.peerId).isSome()
      a.getRemoteNodeResource(c.peerInfo.peerId).isSome()

  asyncTest "invalid payload rejection":
    let client = makeMemorySwitch()
    let server = makeMemorySwitch()
    await client.start()
    await server.start()
    defer:
      await client.stop()
      await server.stop()

    discard await startNodeResourceService(
      client,
      dataDir = "/client",
      source = buildSource("linux"),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )

    let invalidProto = LPProtocol.new(
      @[NodeResourceCodec],
      proc(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
        try:
          discard await conn.readLp(64)
          await conn.writeLp(
            toBytes(
              """{"os":{"name":"Linux"},"cpu":{"coresLogical":8},"memory":{"totalBytes":-1,"availableBytes":0},"gpu":{"model":"bad","vendor":"NVIDIA","vramTotalBytes":0,"vramAvailableBytes":0,"computeClass":"bad","computeScore":0,"availableKnown":false},"disk":{"mountPath":"/","type":"ext4","totalBytes":1,"availableBytes":1},"bandwidth":{"uplinkBps":0,"downlinkBps":0},"updatedAtMs":9999999999999}"""
            )
          )
        except LPStreamError:
          discard
    )
    await invalidProto.start()
    server.mount(invalidProto)

    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)
    await hostNodeResourceUpdateAll(client)

    check client.getRemoteNodeResource(server.peerInfo.peerId).isNone()

  asyncTest "candidate selection sorts by gpu memory disk bandwidth":
    let scheduler = makeMemorySwitch()
    let worker1 = makeMemorySwitch()
    let worker2 = makeMemorySwitch()
    let worker3 = makeMemorySwitch()
    await scheduler.start()
    await worker1.start()
    await worker2.start()
    await worker3.start()
    defer:
      await scheduler.stop()
      await worker1.stop()
      await worker2.stop()
      await worker3.stop()

    discard await startNodeResourceService(
      scheduler,
      dataDir = "/scheduler",
      source = buildSource(
        "linux",
        envPairs = [
          ("CPU_CORES", "8"),
          ("MEMORY_AVAILABLE_BYTES", $(16 * GiB)),
          ("DISK_AVAILABLE_BYTES", $(64 * GiB)),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      worker1,
      dataDir = "/w1",
      source = buildSource(
        "linux",
        envPairs = [
          ("GPU_MODEL", "A100"),
          ("GPU_VRAM_BYTES", $(40 * GiB)),
          ("GPU_COMPUTE_SCORE", "900"),
          ("MEMORY_AVAILABLE_BYTES", $(48 * GiB)),
          ("DISK_AVAILABLE_BYTES", $(400 * GiB)),
          ("BANDWIDTH_DOWN_BPS", "8000000000"),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      worker2,
      dataDir = "/w2",
      source = buildSource(
        "linux",
        envPairs = [
          ("GPU_MODEL", "A100"),
          ("GPU_VRAM_BYTES", $(40 * GiB)),
          ("GPU_COMPUTE_SCORE", "900"),
          ("MEMORY_AVAILABLE_BYTES", $(96 * GiB)),
          ("DISK_AVAILABLE_BYTES", $(300 * GiB)),
          ("BANDWIDTH_DOWN_BPS", "6000000000"),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      worker3,
      dataDir = "/w3",
      source = buildSource(
        "linux",
        envPairs = [
          ("GPU_MODEL", "RTX 4090"),
          ("GPU_VRAM_BYTES", $(24 * GiB)),
          ("GPU_COMPUTE_SCORE", "800"),
          ("MEMORY_AVAILABLE_BYTES", $(128 * GiB)),
          ("DISK_AVAILABLE_BYTES", $(500 * GiB)),
          ("BANDWIDTH_DOWN_BPS", "12000000000"),
        ],
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )

    await scheduler.connect(worker1.peerInfo.peerId, worker1.peerInfo.addrs)
    await scheduler.connect(worker2.peerInfo.peerId, worker2.peerInfo.addrs)
    await scheduler.connect(worker3.peerInfo.peerId, worker3.peerInfo.addrs)
    await hostNodeResourceUpdateAll(scheduler)

    let ranked = scheduler.findResourceCandidates(
      requireGpu = true,
      minMemoryBytes = 32 * GiB,
      minVramBytes = 24 * GiB,
      minDiskAvailableBytes = 200 * GiB,
      minBandwidthBps = 5_000_000_000'i64,
    )
    check ranked.len == 3
    check ranked[0].peerId == worker2.peerInfo.peerId
    check ranked[1].peerId == worker1.peerInfo.peerId
    check ranked[2].peerId == worker3.peerInfo.peerId
