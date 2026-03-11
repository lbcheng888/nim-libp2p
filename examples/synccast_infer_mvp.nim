import std/[options, strutils, tables]

import chronos
import libp2p/[builders, cid, multicodec, multihash]
import libp2p/protocols/bitswap/store
import libp2p/services/[noderesourceservice, synccastcontrolservice,
  distributedinferenceservice]

const GiB = 1024'i64 * 1024 * 1024

proc resourceSource(
    envPairs: openArray[(string, string)],
    nowMs = 1_700_200_000_000'i64,
): NodeResourceCollectorSource =
  var envMap = initTable[string, string]()
  for entry in envPairs:
    envMap[entry[0]] = entry[1]
  result.platform = "linux"
  result.readText = proc(path: string): string =
    ""
  result.listDirs = proc(path: string): seq[string] =
    @[]
  result.exec = proc(command: string, args: seq[string]): string =
    ""
  result.envLookup = proc(name: string): string =
    envMap.getOrDefault(name, "")
  result.statVolume = proc(path: string): NodeResourceDiskStat =
    NodeResourceDiskStat(totalBytes: 512 * GiB, availableBytes: 256 * GiB)
  result.resolvePath = proc(path: string): string =
    if path.len > 0: path else: "/"
  result.nowMs = proc(): int64 =
    nowMs

proc rawCid(data: seq[byte]): Cid =
  let mh = MultiHash.digest("sha2-256", data).tryGet()
  Cid.init(CIDv1, multiCodec("raw"), mh).tryGet()

proc main() {.async.} =
  let workerStore = MemoryBlockStore.new()
  let anchor = newStandardSwitch(transport = TransportType.Memory)
  let worker =
    newStandardSwitchBuilder(transport = TransportType.Memory)
      .withBitswap(workerStore)
      .build()
  let client = newStandardSwitch(transport = TransportType.Memory)

  let artifact = @[byte 0x1, 0x2, 0x3, 0x4]
  let artifactCid = rawCid(artifact)
  await workerStore.putBlock(artifactCid, artifact)

  await anchor.start()
  await worker.start()
  await client.start()
  defer:
    await anchor.stop()
    await worker.stop()
    await client.stop()

  discard await startNodeResourceService(
    anchor,
    dataDir = "/anchor",
    source = resourceSource(
      [
        ("MEMORY_AVAILABLE_BYTES", $(48 * GiB)),
        ("BANDWIDTH_DOWN_BPS", "12000000000"),
      ]
    ),
    config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
  )
  discard await startNodeResourceService(
    worker,
    dataDir = "/worker",
    source = resourceSource(
      [
        ("MEMORY_AVAILABLE_BYTES", $(64 * GiB)),
        ("GPU_MODEL", "A100"),
        ("GPU_VRAM_BYTES", $(40 * GiB)),
        ("GPU_COMPUTE_SCORE", "900"),
        ("BANDWIDTH_DOWN_BPS", "10000000000"),
      ]
    ),
    config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
  )
  discard await startNodeResourceService(
    client,
    dataDir = "/client",
    source = resourceSource(
      [
        ("MEMORY_AVAILABLE_BYTES", $(16 * GiB)),
        ("BANDWIDTH_DOWN_BPS", "4000000000"),
      ]
    ),
    config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
  )

  discard await startSynccastControlService(anchor)
  discard await startSynccastControlService(client)
  discard await startDistributedInferenceService(client)
  discard await startDistributedInferenceService(worker, store = workerStore)

  await client.connect(anchor.peerInfo.peerId, anchor.peerInfo.addrs)
  await client.connect(worker.peerInfo.peerId, worker.peerInfo.addrs)
  await anchor.connect(worker.peerInfo.peerId, worker.peerInfo.addrs)
  await hostNodeResourceUpdateAll(anchor)
  await hostNodeResourceUpdateAll(client)

  let routeOpt = await client.negotiateSynccastRoute(
    anchor.peerInfo.peerId,
    SynccastSessionSpec(
      roomId: "demo-room",
      sessionId: "demo-session",
      desiredRole: srWorker,
      requireGpu: true,
      minMemoryBytes: 16 * GiB,
      minBandwidthBps: 5_000_000_000'i64,
      stickyKey: "demo-sticky",
    ),
  )
  if routeOpt.isNone():
    echo "route negotiation failed"
    return

  let route = routeOpt.get()
  echo "selected worker: ", route.selectedPeer
  echo "route reason: ", route.reason

  let result = await client.submitInferenceTask(
    TaskSpec(
      taskId: "demo-task",
      stickyKey: "demo-sticky",
      modelId: "demo-model",
      artifactCids: @[$artifactCid],
      inputCids: @[$artifactCid],
      requireGpu: true,
      minMemoryBytes: 16 * GiB,
      minVramBytes: 16 * GiB,
      minBandwidthBps: 5_000_000_000'i64,
    )
  )
  echo "task status: ", result.status
  echo "task worker: ", result.workerPeer
  if result.errorMessage.len > 0:
    echo "task error: ", result.errorMessage
  else:
    echo "output cids: ", result.outputCids.join(", ")

when isMainModule:
  waitFor main()
