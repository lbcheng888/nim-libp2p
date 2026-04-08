{.used.}

from std/times import epochTime

import chronos
import unittest2

import ./helpers
import ../libp2p/[builders, crypto/crypto, lsmr, peerstore, switch]
import ../libp2p/services/lsmrservice

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc makeSignedWitness(
    anchorKey: PrivateKey,
    subjectPeerId: PeerId,
    networkId: string,
    operatorId: string,
    regionDigit: uint8,
    rttMs: uint32,
    attestedPrefix: LsmrPath = @[],
): SignedLsmrWitness =
  let anchorPeerId = PeerId.init(anchorKey).tryGet()
  let targetPrefix =
    if attestedPrefix.len > 0:
      attestedPrefix
    else:
      @[regionDigit, latencyDigitForRtt(rttMs)]
  let witness = LsmrWitness(
    subjectPeerId: subjectPeerId,
    anchorPeerId: anchorPeerId,
    networkId: networkId,
    regionDigit: regionDigit,
    latencyDigit: targetPrefix[^1],
    rttMs: rttMs,
    operatorId: operatorId,
    issuedAtMs: nowMillis(),
    expiresAtMs: nowMillis() + 30 * 60 * 1000,
    attestedPrefix: targetPrefix,
    prefixDepth: uint8(targetPrefix.len),
    directionMask: 0x1ff'u32,
    epochId: 1'u64,
    version: DefaultLsmrVersion,
  )
  SignedLsmrWitness.init(anchorKey, witness).tryGet()

proc makeSignedCoordinateRecord(
    peerKey: PrivateKey,
    peerId: PeerId,
    networkId: string,
    attestedPrefix: LsmrPath,
    witnesses: seq[SignedLsmrWitness],
    confidence = 80'u8,
): SignedLsmrCoordinateRecord =
  var record = LsmrCoordinateRecord(
    peerId: peerId,
    networkId: networkId,
    seqNo: uint64(nowMillis()),
    attestedPrefix: attestedPrefix,
    localSuffix: @[],
    epochId: 1'u64,
    issuedAtMs: nowMillis(),
    expiresAtMs: nowMillis() + 30 * 60 * 1000,
    confidence: confidence,
    witnesses: witnesses,
    parentDigest: computeExpectedParentDigest(networkId, attestedPrefix, 1'u64),
    migrationDigest: computeStaticMigrationDigest(peerId, networkId, attestedPrefix, 1'u64),
    coverageMask: expectedCoverageMask(attestedPrefix),
    certifiedDepth: uint8(attestedPrefix.len),
    recordDigest: "",
    heTuParity:
      computeHeTuParity(
        1'u64,
        attestedPrefix,
        expectedCoverageMask(attestedPrefix),
        computeExpectedParentDigest(networkId, attestedPrefix, 1'u64),
        computeStaticMigrationDigest(peerId, networkId, attestedPrefix, 1'u64),
        DefaultLsmrVersion,
      ),
    version: DefaultLsmrVersion,
  )
  record.recordDigest = computeRecordDigest(record)
  SignedLsmrCoordinateRecord.init(peerKey, record).tryGet()

proc installCoordinateChainState(
    sw: Switch,
    records: seq[SignedLsmrCoordinateRecord],
    writeLocalMetadata = false,
) =
  if records.len == 0:
    return
  let tip = records[^1]
  if writeLocalMetadata:
    sw.peerInfo.metadata[LsmrCoordinateMetadataKey] = tip.encode().tryGet()
  sw.peerStore[LsmrChainBook][tip.data.peerId] = records
  sw.peerStore[LsmrBook][tip.data.peerId] = tip
  sw.peerStore[ActiveLsmrBook][tip.data.peerId] = tip

suite "LSMR Dayan Service":
  teardown:
    checkTrackers()

  asyncTest "dayan zhen relays across kun dui li":
    let rng = newRng()
    let networkId = "lsmr-dayan-zhen-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()
    let anchors = @[
      LsmrAnchor(peerId: anchorPeer, operatorId: "root", regionDigit: 5'u8)
    ]

    let keyA = PrivateKey.random(ECDSA, rng[]).tryGet()
    let keyB = PrivateKey.random(ECDSA, rng[]).tryGet()
    let keyC = PrivateKey.random(ECDSA, rng[]).tryGet()

    let nodeA = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(keyA)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1, anchors = anchors))
      .build()
    let nodeB = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(keyB)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1, anchors = anchors))
      .build()
    let nodeC = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(keyC)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1, anchors = anchors))
      .build()

    await nodeA.start()
    await nodeB.start()
    await nodeC.start()
    let svcA = await startLsmrService(nodeA)
    let svcB = await startLsmrService(nodeB)
    let svcC = await startLsmrService(nodeC)
    defer:
      await nodeC.stop()
      await nodeB.stop()
      await nodeA.stop()

    let anchorRoot =
      makeSignedCoordinateRecord(
        anchorKey,
        anchorPeer,
        networkId,
        @[5'u8],
        @[makeSignedWitness(anchorKey, anchorPeer, networkId, "root", 5'u8, 5'u32, @[5'u8])],
      )
    for node in [nodeA, nodeB, nodeC]:
      installCoordinateChainState(node, @[anchorRoot])

    let aParent =
      makeSignedCoordinateRecord(
        keyA,
        nodeA.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, nodeA.peerInfo.peerId, networkId, "op-a-root", 5'u8, 5'u32, @[5'u8, 1'u8])],
      )
    let aDeep =
      makeSignedCoordinateRecord(
        keyA,
        nodeA.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8, 9'u8],
        @[makeSignedWitness(keyA, nodeA.peerInfo.peerId, networkId, "op-a", 5'u8, 5'u32, @[5'u8, 1'u8, 9'u8])],
      )
    installCoordinateChainState(nodeA, @[aParent, aDeep], writeLocalMetadata = true)

    let bParent =
      makeSignedCoordinateRecord(
        keyB,
        nodeB.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, nodeB.peerInfo.peerId, networkId, "op-b-root", 5'u8, 6'u32, @[5'u8, 1'u8])],
      )
    let bDeep =
      makeSignedCoordinateRecord(
        keyB,
        nodeB.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8, 8'u8],
        @[makeSignedWitness(keyB, nodeB.peerInfo.peerId, networkId, "op-b", 5'u8, 6'u32, @[5'u8, 1'u8, 8'u8])],
      )
    installCoordinateChainState(nodeB, @[bParent, bDeep], writeLocalMetadata = true)

    let cParent =
      makeSignedCoordinateRecord(
        keyC,
        nodeC.peerInfo.peerId,
        networkId,
        @[5'u8, 5'u8],
        @[makeSignedWitness(anchorKey, nodeC.peerInfo.peerId, networkId, "op-c-root", 5'u8, 7'u32, @[5'u8, 5'u8])],
      )
    let cDeep =
      makeSignedCoordinateRecord(
        keyC,
        nodeC.peerInfo.peerId,
        networkId,
        @[5'u8, 5'u8, 4'u8],
        @[makeSignedWitness(keyC, nodeC.peerInfo.peerId, networkId, "op-c", 5'u8, 7'u32, @[5'u8, 5'u8, 4'u8])],
      )
    installCoordinateChainState(nodeC, @[cParent, cDeep], writeLocalMetadata = true)

    await nodeA.connect(nodeB.peerInfo.peerId, nodeB.peerInfo.addrs)
    await nodeB.connect(nodeC.peerInfo.peerId, nodeC.peerInfo.addrs)

    var localMsgs: seq[LsmrDaYanMessage] = @[]
    var regionalMsgs: seq[LsmrDaYanMessage] = @[]
    var globalMsgs: seq[LsmrDaYanMessage] = @[]

    discard svcA.subscribeDaYan(
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8, 1'u8, 9'u8],
        minResolution = 3'u8,
        maxResolution = 3'u8,
        semanticPrefix = "sem:",
        acceptUrgent = true,
        acceptBackground = false,
      ),
      proc(message: LsmrDaYanMessage): Future[void] {.async, gcsafe.} =
        localMsgs.add(message),
    )
    discard svcB.subscribeDaYan(
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8, 1'u8],
        minResolution = 2'u8,
        maxResolution = 2'u8,
        semanticPrefix = "sem:",
        acceptUrgent = true,
        acceptBackground = false,
      ),
      proc(message: LsmrDaYanMessage): Future[void] {.async, gcsafe.} =
        regionalMsgs.add(message),
    )
    discard svcC.subscribeDaYan(
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8],
        minResolution = 1'u8,
        maxResolution = 2'u8,
        semanticPrefix = "sem:",
        acceptUrgent = true,
        acceptBackground = false,
      ),
      proc(message: LsmrDaYanMessage): Future[void] {.async, gcsafe.} =
        globalMsgs.add(message),
    )

    check await svcB.publishLocalStateToPeer(nodeA.peerInfo.peerId)
    check await svcC.publishLocalStateToPeer(nodeB.peerInfo.peerId)

    let payload = @[byte 0x01, byte 0x02, byte 0x03]
    let topic =
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:alarm",
        guardDigest = "guard:safe",
        mode = LsmrDaYanMode.ldymZhen,
      )
    check (await svcA.publishDaYan(topic, payload)) == 1

    checkUntilTimeoutCustom(3.seconds, 50.milliseconds):
      localMsgs.len == 1
      regionalMsgs.len == 1
      globalMsgs.len == 1

    check localMsgs[0].stage == LsmrDaYanStage.ldysKunLocal
    check localMsgs[0].payloadKind == LsmrDaYanPayloadKind.ldypkCsgDelta
    check localMsgs[0].payload == payload
    check regionalMsgs[0].stage == LsmrDaYanStage.ldysDuiRegional
    check regionalMsgs[0].payloadKind == LsmrDaYanPayloadKind.ldypkCsgDelta
    check regionalMsgs[0].payload == payload
    check regionalMsgs[0].resolution == 2'u8
    check globalMsgs[0].stage == LsmrDaYanStage.ldysLiGlobal
    check globalMsgs[0].payloadKind == LsmrDaYanPayloadKind.ldypkSemanticFingerprint
    check globalMsgs[0].payload.len == 0
    check globalMsgs[0].semanticFingerprint == topic.semanticFingerprint
    check globalMsgs[0].resolution == 2'u8

  asyncTest "dayan xun background relay and unsubscribe remove route":
    let rng = newRng()
    let networkId = "lsmr-dayan-xun-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()
    let anchors = @[
      LsmrAnchor(peerId: anchorPeer, operatorId: "root", regionDigit: 5'u8)
    ]

    let keyA = PrivateKey.random(ECDSA, rng[]).tryGet()
    let keyB = PrivateKey.random(ECDSA, rng[]).tryGet()

    let nodeA = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(keyA)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1, anchors = anchors))
      .build()
    let nodeB = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(keyB)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1, anchors = anchors))
      .build()

    await nodeA.start()
    await nodeB.start()
    let svcA = await startLsmrService(nodeA)
    let svcB = await startLsmrService(nodeB)
    defer:
      await nodeB.stop()
      await nodeA.stop()

    let anchorRoot =
      makeSignedCoordinateRecord(
        anchorKey,
        anchorPeer,
        networkId,
        @[5'u8],
        @[makeSignedWitness(anchorKey, anchorPeer, networkId, "root", 5'u8, 5'u32, @[5'u8])],
      )
    for node in [nodeA, nodeB]:
      installCoordinateChainState(node, @[anchorRoot])

    let aParent =
      makeSignedCoordinateRecord(
        keyA,
        nodeA.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, nodeA.peerInfo.peerId, networkId, "op-a-root", 5'u8, 5'u32, @[5'u8, 1'u8])],
      )
    let aDeep =
      makeSignedCoordinateRecord(
        keyA,
        nodeA.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8, 9'u8],
        @[makeSignedWitness(keyA, nodeA.peerInfo.peerId, networkId, "op-a", 5'u8, 5'u32, @[5'u8, 1'u8, 9'u8])],
      )
    installCoordinateChainState(nodeA, @[aParent, aDeep], writeLocalMetadata = true)

    let bParent =
      makeSignedCoordinateRecord(
        keyB,
        nodeB.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, nodeB.peerInfo.peerId, networkId, "op-b-root", 5'u8, 6'u32, @[5'u8, 1'u8])],
      )
    let bDeep =
      makeSignedCoordinateRecord(
        keyB,
        nodeB.peerInfo.peerId,
        networkId,
        @[5'u8, 1'u8, 8'u8],
        @[makeSignedWitness(keyB, nodeB.peerInfo.peerId, networkId, "op-b", 5'u8, 6'u32, @[5'u8, 1'u8, 8'u8])],
      )
    installCoordinateChainState(nodeB, @[bParent, bDeep], writeLocalMetadata = true)

    await nodeA.connect(nodeB.peerInfo.peerId, nodeB.peerInfo.addrs)

    var received: seq[LsmrDaYanMessage] = @[]
    let subscriptionId =
      svcB.subscribeDaYan(
        LsmrDaYanSubscription.init(
          topologyPrefix = @[5'u8, 1'u8],
          minResolution = 2'u8,
          maxResolution = 2'u8,
          semanticPrefix = "sem:",
          acceptUrgent = false,
          acceptBackground = true,
        ),
        proc(message: LsmrDaYanMessage): Future[void] {.async, gcsafe.} =
          received.add(message),
      )
    check subscriptionId.len > 0
    check await svcB.publishLocalStateToPeer(nodeA.peerInfo.peerId)

    let topic =
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:state",
        mode = LsmrDaYanMode.ldymXun,
      )
    check (await svcA.publishDaYan(topic, @[byte 0xaa, byte 0xbb])) == 0
    checkUntilTimeoutCustom(3.seconds, 50.milliseconds):
      received.len == 1
    check received[0].mode == LsmrDaYanMode.ldymXun
    check received[0].stage == LsmrDaYanStage.ldysDuiRegional

    check svcB.unsubscribeDaYan(subscriptionId)
    check await svcB.publishLocalStateToPeer(nodeA.peerInfo.peerId)
    let before = received.len
    check (await svcA.publishDaYan(topic, @[byte 0xcc])) == 0
    await sleepAsync(300.milliseconds)
    check received.len == before

  asyncTest "dayan message replay only delivers once":
    let rng = newRng()
    let networkId = "lsmr-dayan-replay-" & $nowMillis()
    let sourceKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let localKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let sourcePeer = PeerId.init(sourceKey).tryGet()

    let node = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(localKey)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1))
      .build()

    await node.start()
    let svc = await startLsmrService(node)
    defer:
      await node.stop()

    var received: seq[LsmrDaYanMessage] = @[]
    discard svc.subscribeDaYan(
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8, 1'u8],
        minResolution = 2'u8,
        maxResolution = 2'u8,
        semanticPrefix = "sem:",
      ),
      proc(message: LsmrDaYanMessage): Future[void] {.async, gcsafe.} =
        received.add(message),
    )

    let topic =
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:delta",
        mode = LsmrDaYanMode.ldymZhen,
      )
    let envelope =
      LsmrDaYanEnvelope.init(
        messageId = "replay-1",
        originPeerId = sourcePeer,
        topic = topic,
        stage = LsmrDaYanStage.ldysDuiRegional,
        payloadKind = LsmrDaYanPayloadKind.ldypkCsgDelta,
        resolution = 2'u8,
        payload = @[byte 0x42],
      )

    check await svc.acceptDaYanEnvelope(sourcePeer, envelope)
    check not await svc.acceptDaYanEnvelope(sourcePeer, envelope)
    checkUntilTimeoutCustom(2.seconds, 50.milliseconds):
      received.len == 1
    check received[0].messageId == "replay-1"
