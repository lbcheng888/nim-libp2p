{.used.}

import std/[options, sequtils, strutils, tables]

import chronos
import unittest2

import ./helpers
import ../libp2p/[builders, switch, delegatedrouting]
import ../libp2p/cid
import ../libp2p/delegatedrouting/store
import ../libp2p/multicodec
import ../libp2p/multihash
import ../libp2p/peerid
import ../libp2p/peerstore
import ../libp2p/protocols/bitswap/store
import ../libp2p/protocols/protocol
import ../libp2p/providers/bitswapadvertiser
import ../libp2p/services/[noderesourceservice, synccastcontrolservice,
  distributedinferenceservice]
import ../libp2p/stream/connection
import ../libp2p/stream/lpstream

const
  GiB = 1024'i64 * 1024 * 1024

proc toBytes(text: string): seq[byte] =
  text.toSeq().mapIt(byte(it))

proc resourceSource(
    envPairs: openArray[(string, string)],
    nowMs = 1_700_100_000_000'i64,
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

proc makeSwitch(store: BitswapBlockStore = nil): Switch =
  var builder = newStandardSwitchBuilder(transport = TransportType.Memory)
  if not store.isNil():
    builder = builder.withBitswap(store)
  builder.build()

proc rawCid(data: seq[byte]): Cid =
  let mh = MultiHash.digest("sha2-256", data).tryGet()
  Cid.init(CIDv1, multiCodec("raw"), mh).tryGet()

suite "Synccast + distributed inference service":
  teardown:
    checkTrackers()

  asyncTest "synccast hello negotiate smoke":
    let anchor = makeSwitch()
    let client = makeSwitch()
    await anchor.start()
    await client.start()
    defer:
      await anchor.stop()
      await client.stop()

    discard await startNodeResourceService(
      anchor,
      dataDir = "/anchor",
      source = resourceSource(
        [
          ("CPU_CORES", "16"),
          ("MEMORY_AVAILABLE_BYTES", $(64 * GiB)),
          ("BANDWIDTH_DOWN_BPS", "12000000000"),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      client,
      dataDir = "/client",
      source = resourceSource(
        [
          ("CPU_CORES", "8"),
          ("MEMORY_AVAILABLE_BYTES", $(16 * GiB)),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startSynccastControlService(anchor)
    discard await startSynccastControlService(client)

    await client.connect(anchor.peerInfo.peerId, anchor.peerInfo.addrs)

    let routeOpt = await client.negotiateSynccastRoute(
      anchor.peerInfo.peerId,
      SynccastSessionSpec(
        roomId: "room-a",
        sessionId: "sess-1",
        desiredRole: srRelay,
        minMemoryBytes: 8 * GiB,
        minBandwidthBps: 1_000_000_000'i64,
      ),
    )
    check routeOpt.isSome()
    let route = routeOpt.get()
    check route.selectedPeer == anchor.peerInfo.peerId
    check route.selectedRole == srRelay
    check client.getSynccastRoute(route.routeId).isSome()

  asyncTest "synccast routing prefers sticky worker and relay bandwidth":
    let client = makeSwitch()
    let anchor = makeSwitch()
    let workerA = makeSwitch()
    let workerB = makeSwitch()
    let relayA = makeSwitch()
    let relayB = makeSwitch()
    for sw in [client, anchor, workerA, workerB, relayA, relayB]:
      await sw.start()
    defer:
      for sw in [client, anchor, workerA, workerB, relayA, relayB]:
        await sw.stop()

    discard await startNodeResourceService(
      client,
      dataDir = "/client",
      source = resourceSource([("MEMORY_AVAILABLE_BYTES", $(16 * GiB))]),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      anchor,
      dataDir = "/anchor",
      source = resourceSource([("MEMORY_AVAILABLE_BYTES", $(48 * GiB))]),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      workerA,
      dataDir = "/worker-a",
      source = resourceSource(
        [
          ("MEMORY_AVAILABLE_BYTES", $(48 * GiB)),
          ("GPU_MODEL", "A100"),
          ("GPU_VRAM_BYTES", $(40 * GiB)),
          ("GPU_COMPUTE_SCORE", "850"),
          ("BANDWIDTH_DOWN_BPS", "6000000000"),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      workerB,
      dataDir = "/worker-b",
      source = resourceSource(
        [
          ("MEMORY_AVAILABLE_BYTES", $(40 * GiB)),
          ("GPU_MODEL", "H100"),
          ("GPU_VRAM_BYTES", $(80 * GiB)),
          ("GPU_COMPUTE_SCORE", "1000"),
          ("BANDWIDTH_DOWN_BPS", "7000000000"),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      relayA,
      dataDir = "/relay-a",
      source = resourceSource(
        [
          ("MEMORY_AVAILABLE_BYTES", $(32 * GiB)),
          ("BANDWIDTH_DOWN_BPS", "15000000000"),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      relayB,
      dataDir = "/relay-b",
      source = resourceSource(
        [
          ("MEMORY_AVAILABLE_BYTES", $(64 * GiB)),
          ("BANDWIDTH_DOWN_BPS", "10000000000"),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )

    discard await startSynccastControlService(anchor)
    discard await startSynccastControlService(client)

    await client.connect(anchor.peerInfo.peerId, anchor.peerInfo.addrs)
    for peer in [workerA, workerB, relayA, relayB]:
      await anchor.connect(peer.peerInfo.peerId, peer.peerInfo.addrs)
    await hostNodeResourceUpdateAll(anchor)

    let relayRoute = await client.negotiateSynccastRoute(
      anchor.peerInfo.peerId,
      SynccastSessionSpec(
        roomId: "room-b",
        sessionId: "relay-1",
        desiredRole: srRelay,
        minMemoryBytes: 8 * GiB,
        minBandwidthBps: 5_000_000_000'i64,
      ),
    )
    check relayRoute.isSome()
    check relayRoute.get().selectedPeer == relayA.peerInfo.peerId

    let firstWorkerRoute = await client.negotiateSynccastRoute(
      anchor.peerInfo.peerId,
      SynccastSessionSpec(
        roomId: "room-b",
        sessionId: "worker-1",
        desiredRole: srWorker,
        requireGpu: true,
        minMemoryBytes: 32 * GiB,
        minBandwidthBps: 5_000_000_000'i64,
        stickyKey: "sticky-turn-1",
      ),
    )
    check firstWorkerRoute.isSome()
    check firstWorkerRoute.get().selectedPeer == workerB.peerInfo.peerId

    let secondWorkerRoute = await client.negotiateSynccastRoute(
      anchor.peerInfo.peerId,
      SynccastSessionSpec(
        roomId: "room-b",
        sessionId: "worker-2",
        desiredRole: srWorker,
        requireGpu: true,
        minMemoryBytes: 32 * GiB,
        minBandwidthBps: 5_000_000_000'i64,
        stickyKey: "sticky-turn-1",
      ),
    )
    check secondWorkerRoute.isSome()
    check secondWorkerRoute.get().selectedPeer == workerB.peerInfo.peerId

    await workerB.stop()
    discard anchor.peerStore[ProtoBook].del(workerB.peerInfo.peerId)
    await hostNodeResourceUpdateAll(anchor)

    let fallbackRoute = await client.negotiateSynccastRoute(
      anchor.peerInfo.peerId,
      SynccastSessionSpec(
        roomId: "room-b",
        sessionId: "worker-3",
        desiredRole: srWorker,
        requireGpu: true,
        minMemoryBytes: 32 * GiB,
        minBandwidthBps: 5_000_000_000'i64,
        stickyKey: "sticky-turn-1",
      ),
    )
    check fallbackRoute.isSome()
    check fallbackRoute.get().selectedPeer == workerA.peerInfo.peerId

  asyncTest "distributed inference retries rejected worker and completes on fallback":
    let coordinator = makeSwitch()
    let rejectWorker = makeSwitch()
    let workerStore = MemoryBlockStore.new()
    let runWorker = makeSwitch(workerStore)
    for sw in [coordinator, rejectWorker, runWorker]:
      await sw.start()
    defer:
      for sw in [coordinator, rejectWorker, runWorker]:
        await sw.stop()

    let artifact = @[byte 1, 3, 5, 7]
    let artifactCid = rawCid(artifact)
    await workerStore.putBlock(artifactCid, artifact)

    discard await startNodeResourceService(
      coordinator,
      dataDir = "/coordinator",
      source = resourceSource([("MEMORY_AVAILABLE_BYTES", $(16 * GiB))]),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      rejectWorker,
      dataDir = "/reject",
      source = resourceSource(
        [
          ("MEMORY_AVAILABLE_BYTES", $(64 * GiB)),
          ("GPU_MODEL", "H100"),
          ("GPU_VRAM_BYTES", $(80 * GiB)),
          ("GPU_COMPUTE_SCORE", "1000"),
          ("BANDWIDTH_DOWN_BPS", "12000000000"),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      runWorker,
      dataDir = "/run",
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
    discard await startDistributedInferenceService(
      coordinator,
      config = DistributedInferenceConfig.init(requestTimeout = 2.seconds),
    )
    discard await startDistributedInferenceService(rejectWorker)
    discard await startDistributedInferenceService(
      runWorker,
      executor = proc(task: TaskSpec): Future[TaskResult] {.async, gcsafe.} =
        await sleepAsync(400.milliseconds)
        TaskResult(
          taskId: task.taskId,
          status: trsSuccess,
          outputCids: task.inputCids,
        ),
    )

    await coordinator.connect(rejectWorker.peerInfo.peerId, rejectWorker.peerInfo.addrs)
    await coordinator.connect(runWorker.peerInfo.peerId, runWorker.peerInfo.addrs)
    await hostNodeResourceUpdateAll(coordinator)

    let task = TaskSpec(
      taskId: "task-retry-1",
      stickyKey: "session-1",
      modelId: "model-a",
      artifactCids: @[$artifactCid],
      inputCids: @[$artifactCid],
      requireGpu: true,
      minMemoryBytes: 16 * GiB,
      minVramBytes: 16 * GiB,
      minBandwidthBps: 5_000_000_000'i64,
    )

    let submitFuture = coordinator.submitInferenceTask(task)
    checkUntilTimeoutCustom(2.seconds, 50.milliseconds):
      block:
        let leaseOpt = coordinator.getInferenceLease("task-retry-1")
        leaseOpt.isSome() and leaseOpt.get().state == ilsRunning

    let result = await submitFuture
    check result.status == trsSuccess
    check result.workerPeer == runWorker.peerInfo.peerId
    let leaseOpt = coordinator.getInferenceLease("task-retry-1")
    check leaseOpt.isSome()
    check leaseOpt.get().state == ilsCompleted

  asyncTest "prewarm covers local hits provider hints and delegated providers":
    let sharedProviders = DelegatedRoutingStore.new()
    let providerBase = MemoryBlockStore.new()
    let providerStore = AutoProviderBlockStore.new(providerBase, minInterval = 0.seconds)
    let workerStore = MemoryBlockStore.new()
    let provider = makeSwitch(providerStore)
    let worker = makeSwitch(workerStore)
    await provider.start()
    await worker.start()
    defer:
      await provider.stop()
      await worker.stop()

    provider.delegatedRoutingStore = sharedProviders
    worker.delegatedRoutingStore = sharedProviders

    discard await startNodeResourceService(
      provider,
      dataDir = "/provider",
      source = resourceSource([("MEMORY_AVAILABLE_BYTES", $(32 * GiB))]),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      worker,
      dataDir = "/worker",
      source = resourceSource([("MEMORY_AVAILABLE_BYTES", $(32 * GiB))]),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    let workerSvc = await startDistributedInferenceService(worker, store = workerStore)
    await worker.connect(provider.peerInfo.peerId, provider.peerInfo.addrs)

    let localData = @[byte 2, 4, 6]
    let localCid = rawCid(localData)
    await workerStore.putBlock(localCid, localData)
    let localWarm = await workerSvc.prewarmArtifacts(
      TaskSpec(
        taskId: "warm-local",
        modelId: "model-local",
        artifactCids: @[$localCid],
      )
    )
    check localWarm.readyCids == @[$localCid]
    check localWarm.missingCids.len == 0
    check localWarm.providerUsed.len == 0

    let hintedData = @[byte 7, 7, 7]
    let hintedCid = rawCid(hintedData)
    await providerStore.putBlock(hintedCid, hintedData)
    let hintWarm = await workerSvc.prewarmArtifacts(
      TaskSpec(
        taskId: "warm-hint",
        modelId: "model-hint",
        artifactCids: @[$hintedCid],
        providerHints: @[provider.peerInfo.peerId],
      )
    )
    check hintWarm.readyCids == @[$hintedCid]
    check hintWarm.missingCids.len == 0
    check hintWarm.providerUsed.len == 1
    check hintWarm.providerUsed[0] == provider.peerInfo.peerId

    let delegatedData = @[byte 9, 9, 1]
    let delegatedCid = rawCid(delegatedData)
    await providerStore.putBlock(delegatedCid, delegatedData)
    checkUntilTimeoutCustom(2.seconds, 50.milliseconds):
      (await sharedProviders.getProviders($delegatedCid)).len > 0

    let delegatedWarm = await workerSvc.prewarmArtifacts(
      TaskSpec(
        taskId: "warm-delegated",
        modelId: "model-delegated",
        artifactCids: @[$delegatedCid],
      )
    )
    check delegatedWarm.readyCids == @[$delegatedCid]
    check delegatedWarm.missingCids.len == 0
    check delegatedWarm.providerUsed.len == 1
    check delegatedWarm.providerUsed[0] == provider.peerInfo.peerId

  asyncTest "builder auto mounts node-resource and malformed payloads are rejected":
    let synccastSwitch =
      newStandardSwitchBuilder(transport = TransportType.Memory)
        .withSynccastControlService()
        .build()
    let inferenceSwitch =
      newStandardSwitchBuilder(transport = TransportType.Memory)
        .withDistributedInferenceService()
        .build()
    await synccastSwitch.start()
    await inferenceSwitch.start()
    defer:
      await synccastSwitch.stop()
      await inferenceSwitch.stop()

    check synccastSwitch.getNodeResourceService() != nil
    check synccastSwitch.getSynccastControlService() != nil
    check inferenceSwitch.getNodeResourceService() != nil
    check inferenceSwitch.getDistributedInferenceService() != nil

    let synccastClient = makeSwitch()
    let badControl = makeSwitch()
    await synccastClient.start()
    await badControl.start()
    defer:
      await synccastClient.stop()
      await badControl.stop()

    discard await startNodeResourceService(
      synccastClient,
      dataDir = "/client",
      source = resourceSource([("MEMORY_AVAILABLE_BYTES", $(16 * GiB))]),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startSynccastControlService(synccastClient)
    let invalidControl = LPProtocol.new(
      @[SynccastControlCodec],
      proc(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
        try:
          discard await conn.readLp(1024)
          await conn.writeLp(toBytes("not-json"))
        except LPStreamError:
          discard
    )
    await invalidControl.start()
    badControl.mount(invalidControl)
    await synccastClient.connect(badControl.peerInfo.peerId, badControl.peerInfo.addrs)
    let badRoute = await synccastClient.negotiateSynccastRoute(
      badControl.peerInfo.peerId,
      SynccastSessionSpec(
        roomId: "bad-room",
        sessionId: "bad-session",
        desiredRole: srRelay,
      ),
    )
    check badRoute.isNone()

    let coordinator = makeSwitch()
    let invalidWorker = makeSwitch()
    await coordinator.start()
    await invalidWorker.start()
    defer:
      await coordinator.stop()
      await invalidWorker.stop()

    discard await startNodeResourceService(
      coordinator,
      dataDir = "/coordinator",
      source = resourceSource([("MEMORY_AVAILABLE_BYTES", $(16 * GiB))]),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startNodeResourceService(
      invalidWorker,
      dataDir = "/invalid",
      source = resourceSource(
        [
          ("MEMORY_AVAILABLE_BYTES", $(64 * GiB)),
          ("GPU_MODEL", "A100"),
          ("GPU_VRAM_BYTES", $(40 * GiB)),
          ("GPU_COMPUTE_SCORE", "900"),
        ]
      ),
      config = NodeResourceServiceConfig.init(syncInterval = 0.seconds),
    )
    discard await startDistributedInferenceService(coordinator)
    let invalidDispatch = LPProtocol.new(
      @[DistributedInferenceCodec],
      proc(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
        try:
          discard await conn.readLp(2048)
          await conn.writeLp(toBytes("{bad-json"))
        except LPStreamError:
          discard
    )
    await invalidDispatch.start()
    invalidWorker.mount(invalidDispatch)
    await coordinator.connect(invalidWorker.peerInfo.peerId, invalidWorker.peerInfo.addrs)
    await hostNodeResourceUpdateAll(coordinator)

    let invalidResult = await coordinator.submitInferenceTask(
      TaskSpec(
        taskId: "bad-dispatch",
        modelId: "model-bad",
        artifactCids: @[],
        requireGpu: true,
        minMemoryBytes: 8 * GiB,
      )
    )
    check invalidResult.status == trsFailed
    check invalidResult.errorMessage.len > 0
