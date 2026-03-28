{.used.}

import std/[options, sequtils, tables]
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

proc makeSignedMigrationRecord(
    peerKey: PrivateKey,
    peerId: PeerId,
    networkId: string,
    fromPrefix, toPrefix: LsmrPath,
    oldRecordDigest: string,
    witnesses: seq[SignedLsmrWitness],
    epochId = 1'u64,
): SignedLsmrMigrationRecord =
  var record = LsmrMigrationRecord(
    peerId: peerId,
    networkId: networkId,
    epochId: epochId,
    fromPrefix: fromPrefix,
    toPrefix: toPrefix,
    oldRecordDigest: oldRecordDigest,
    newParentDigest: computeExpectedParentDigest(networkId, toPrefix, epochId),
    issuedAtMs: nowMillis(),
    witnesses: witnesses,
    migrationDigest: "",
    version: DefaultLsmrVersion,
  )
  record.migrationDigest = computeMigrationDigest(record)
  SignedLsmrMigrationRecord.init(peerKey, record).tryGet()

proc makeSignedIsolationEvidence(
    signerKey: PrivateKey,
    subjectPeerId: PeerId,
    networkId: string,
    coordinateDigest, conflictingDigest: string,
    witnesses: seq[SignedLsmrWitness],
    epochId = 1'u64,
): SignedLsmrIsolationEvidence =
  var evidence = LsmrIsolationEvidence(
    peerId: subjectPeerId,
    networkId: networkId,
    epochId: epochId,
    reason: LsmrIsolationReason.lirMultiLocation,
    detail: "test_evidence",
    coordinateDigest: coordinateDigest,
    conflictingDigest: conflictingDigest,
    issuedAtMs: nowMillis(),
    witnesses: witnesses,
    evidenceDigest: "",
    version: DefaultLsmrVersion,
  )
  evidence.evidenceDigest = computeIsolationDigest(evidence)
  SignedLsmrIsolationEvidence.init(signerKey, evidence).tryGet()

suite "LSMR":
  teardown:
    checkTrackers()

  test "coordinate record roundtrip preserves parity and luo shu distance":
    let rng = newRng()
    let peerKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let peerId = PeerId.init(peerKey).tryGet()
    let witness =
      makeSignedWitness(anchorKey, peerId, "roundtrip-net", "op-a", 5'u8, 12'u32)
    let signed =
      makeSignedCoordinateRecord(
        peerKey, peerId, "roundtrip-net", @[5'u8, 1'u8], @[witness]
      )

    check signed.checkValid().isOk()
    let encoded = signed.encode().tryGet()
    let decoded = SignedLsmrCoordinateRecord.decode(encoded).tryGet()
    check decoded.data.attestedPrefix == @[5'u8, 1'u8]
    check decoded.data.witnesses.len == 1
    check lsmrDistance(@[5'u8, 8'u8], @[5'u8, 2'u8]) == 4

  test "coordinate record rejects inconsistent he tu parity":
    let rng = newRng()
    let peerKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let peerId = PeerId.init(peerKey).tryGet()
    let witness =
      makeSignedWitness(anchorKey, peerId, "bad-parity-net", "op-a", 5'u8, 10'u32)
    var record = LsmrCoordinateRecord(
      peerId: peerId,
      networkId: "bad-parity-net",
      seqNo: uint64(nowMillis()),
      attestedPrefix: @[5'u8, 1'u8],
      localSuffix: @[],
      epochId: 1'u64,
      issuedAtMs: nowMillis(),
      expiresAtMs: nowMillis() + 30 * 60 * 1000,
      confidence: 80'u8,
      witnesses: @[witness],
      parentDigest: computeExpectedParentDigest("bad-parity-net", @[5'u8, 1'u8], 1'u64),
      migrationDigest:
        computeStaticMigrationDigest(peerId, "bad-parity-net", @[5'u8, 1'u8], 1'u64),
      coverageMask: expectedCoverageMask(@[5'u8, 1'u8]),
      certifiedDepth: 2'u8,
      recordDigest: "",
      heTuParity: 123'u32,
      version: DefaultLsmrVersion,
    )
    record.recordDigest = computeRecordDigest(record)
    let signed = SignedLsmrCoordinateRecord.init(peerKey, record).tryGet()
    check signed.checkValid().isErr()

  test "object coordinate mapping is deterministic":
    let digest = @[byte 0x00, byte 0x08, byte 0x11, byte 0xff]
    let mapped = objectPathForDigest(digest, 6)
    check mapped.len == 6
    check mapped == objectPathForDigest(digest, 6)
    let coord = objectCoordinate("cid:test", digest, 4)
    check coord.objectId == "cid:test"
    check coord.depth == 4'u8
    check coord.path == objectPathForDigest(digest, 4)

  test "topology neighbor view groups same cell and directional peers":
    let rng = newRng()
    let networkId = "lsmr-neighbor-view"
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()

    let localKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let localPeer = PeerId.init(localKey).tryGet()
    let sameKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let samePeer = PeerId.init(sameKey).tryGet()
    let eastKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let eastPeer = PeerId.init(eastKey).tryGet()
    let southKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let southPeer = PeerId.init(southKey).tryGet()

    let sw = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(localKey)
      .build()
    sw.peerStore[ActiveLsmrBook][localPeer] =
      makeSignedCoordinateRecord(
        localKey,
        localPeer,
        networkId,
        @[5'u8, 5'u8],
        @[makeSignedWitness(anchorKey, localPeer, networkId, "op-a", 5'u8, 8'u32, @[5'u8, 5'u8])],
      )
    sw.peerStore[ActiveLsmrBook][samePeer] =
      makeSignedCoordinateRecord(
        sameKey,
        samePeer,
        networkId,
        @[5'u8, 5'u8],
        @[makeSignedWitness(anchorKey, samePeer, networkId, "op-a", 5'u8, 9'u32, @[5'u8, 5'u8])],
      )
    sw.peerStore[ActiveLsmrBook][eastPeer] =
      makeSignedCoordinateRecord(
        eastKey,
        eastPeer,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, eastPeer, networkId, "op-a", 5'u8, 10'u32, @[5'u8, 1'u8])],
      )
    sw.peerStore[ActiveLsmrBook][southPeer] =
      makeSignedCoordinateRecord(
        southKey,
        southPeer,
        networkId,
        @[5'u8, 9'u8],
        @[makeSignedWitness(anchorKey, southPeer, networkId, "op-a", 5'u8, 12'u32, @[5'u8, 9'u8])],
      )

    let view = sw.peerStore.topologyNeighborView(localPeer, depth = 2)
    check view.isSome()
    check samePeer in view.get().sameCellPeers
    check eastPeer in view.get().parentPrefixPeers
    check southPeer in view.get().parentPrefixPeers
    check view.get().directionalPeers.anyIt(it.directionDigit == 1'u8 and eastPeer in it.peers)
    check view.get().directionalPeers.anyIt(it.directionDigit == 9'u8 and southPeer in it.peers)

  test "route next hop follows directional geometry before global ranking":
    let rng = newRng()
    let networkId = "lsmr-route-next-hop"
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()

    let localKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let localPeer = PeerId.init(localKey).tryGet()
    let eastKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let eastPeer = PeerId.init(eastKey).tryGet()
    let southKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let southPeer = PeerId.init(southKey).tryGet()

    let sw = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(localKey)
      .build()
    sw.peerStore[ActiveLsmrBook][localPeer] =
      makeSignedCoordinateRecord(
        localKey,
        localPeer,
        networkId,
        @[5'u8, 5'u8],
        @[makeSignedWitness(anchorKey, localPeer, networkId, "op-a", 5'u8, 8'u32, @[5'u8, 5'u8])],
      )
    sw.peerStore[ActiveLsmrBook][eastPeer] =
      makeSignedCoordinateRecord(
        eastKey,
        eastPeer,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, eastPeer, networkId, "op-a", 5'u8, 10'u32, @[5'u8, 1'u8])],
      )
    sw.peerStore[ActiveLsmrBook][southPeer] =
      makeSignedCoordinateRecord(
        southKey,
        southPeer,
        networkId,
        @[5'u8, 9'u8],
        @[makeSignedWitness(anchorKey, southPeer, networkId, "op-a", 5'u8, 12'u32, @[5'u8, 9'u8])],
      )

    let nextHop = sw.peerStore.routeNextHop(localPeer, @[5'u8, 9'u8, 1'u8])
    check nextHop.isSome()
    check nextHop.get() == southPeer

  test "validated migration record allows non descendant coordinate update":
    let rng = newRng()
    let networkId = "lsmr-migration-validation"
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let committeeKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let committeePeer = PeerId.init(committeeKey).tryGet()
    let peerKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let peerId = PeerId.init(peerKey).tryGet()

    let sw = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(peerKey)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1))
      .build()

    let committeeRecord =
      makeSignedCoordinateRecord(
        committeeKey,
        committeePeer,
        networkId,
        @[5'u8, 5'u8],
        @[makeSignedWitness(anchorKey, committeePeer, networkId, "committee", 5'u8, 8'u32, @[5'u8, 5'u8])],
      )
    sw.peerStore[ActiveLsmrBook][committeePeer] = committeeRecord

    let oldRecord =
      makeSignedCoordinateRecord(
        peerKey,
        peerId,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, peerId, networkId, "committee", 5'u8, 10'u32, @[5'u8, 1'u8])],
      )
    sw.peerStore[ActiveLsmrBook][peerId] = oldRecord

    let migrationWitness =
      makeSignedWitness(committeeKey, peerId, networkId, "committee", 5'u8, 9'u32, @[5'u8, 9'u8])
    let migration =
      makeSignedMigrationRecord(
        peerKey,
        peerId,
        networkId,
        @[5'u8, 1'u8],
        @[5'u8, 9'u8],
        oldRecord.data.recordDigest,
        @[migrationWitness],
        epochId = 2'u64,
      )
    discard installMigrationRecord(sw, migration)

    let migratedRecord =
      makeSignedCoordinateRecord(
        peerKey,
        peerId,
        networkId,
        @[5'u8, 9'u8],
        @[migrationWitness],
      )
    var migratedData = migratedRecord.data
    migratedData.epochId = 2'u64
    migratedData.parentDigest = computeExpectedParentDigest(networkId, @[5'u8, 9'u8], 2'u64)
    migratedData.migrationDigest = migration.data.migrationDigest
    migratedData.heTuParity =
      computeHeTuParity(
        2'u64,
        @[5'u8, 9'u8],
        expectedCoverageMask(@[5'u8, 9'u8]),
        migratedData.parentDigest,
        migratedData.migrationDigest,
        DefaultLsmrVersion,
      )
    migratedData.recordDigest = computeRecordDigest(migratedData)
    let signedMigrated = SignedLsmrCoordinateRecord.init(peerKey, migratedData).tryGet()
    sw.peerStore[LsmrBook][peerId] = signedMigrated

    check promoteValidatedCoordinateRecord(sw, LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1), peerId)
    check sw.peerStore[ActiveLsmrBook][peerId].data.certifiedPrefix() == @[5'u8, 9'u8]

  asyncTest "coordinate sync imports active certs migrations and isolations":
    let rng = newRng()
    let networkId = "lsmr-coordinate-sync-" & $nowMillis()
    let serverKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let serverPeer = PeerId.init(serverKey).tryGet()
    let clientKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let subjectKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let subjectPeer = PeerId.init(subjectKey).tryGet()
    let signerKey = PrivateKey.random(ECDSA, rng[]).tryGet()

    let server = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(serverKey)
      .withLsmr(LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1, serveWitness = true, operatorId = "root"))
      .build()
    let client = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(clientKey)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          minWitnessQuorum = 1,
          anchors = @[LsmrAnchor(peerId: serverPeer, addrs: server.peerInfo.addrs, operatorId: "root", regionDigit: 5'u8)],
        )
      )
      .build()

    await server.start()
    await client.start()
    discard await startLsmrService(server)
    let clientSvc = await startLsmrService(client)
    defer:
      await client.stop()
      await server.stop()

    let serverWitness =
      makeSignedWitness(serverKey, serverPeer, networkId, "root", 5'u8, 5'u32, @[5'u8])
    let serverRecord =
      makeSignedCoordinateRecord(serverKey, serverPeer, networkId, @[5'u8], @[serverWitness])
    server.peerStore[LsmrBook][serverPeer] = serverRecord
    server.peerStore[ActiveLsmrBook][serverPeer] = serverRecord

    let subjectWitness =
      makeSignedWitness(serverKey, subjectPeer, networkId, "root", 5'u8, 10'u32, @[5'u8, 1'u8])
    let migration =
      makeSignedMigrationRecord(
        subjectKey,
        subjectPeer,
        networkId,
        @[5'u8, 1'u8],
        @[5'u8, 9'u8],
        "old-record",
        @[subjectWitness],
      )
    discard installMigrationRecord(server, migration)
    let isolation =
      makeSignedIsolationEvidence(
        signerKey,
        subjectPeer,
        networkId,
        "coord-a",
        "coord-b",
        @[subjectWitness],
      )
    discard installIsolationEvidence(server, isolation)

    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)
    check await clientSvc.syncCoordinatesFromPeer(serverPeer)
    check client.peerStore[ActiveLsmrBook].contains(serverPeer)
    check client.peerStore[LsmrMigrationBook][subjectPeer].len == 1
    check client.peerStore[LsmrIsolationBook][subjectPeer].len == 1

  asyncTest "near field observation seeds witness sources without static anchors":
    let rng = newRng()
    let networkId = "lsmr-near-field-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()

    let anchor = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(anchorKey)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          serveWitness = true,
          operatorId = "near-op",
          regionDigit = 5'u8,
          minWitnessQuorum = 1,
        )
      )
      .build()
    let client = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          minWitnessQuorum = 1,
          enableNearFieldBootstrap = true,
          nearFieldProviders = @[NearFieldBootstrapProvider.nfbpBle],
        )
      )
      .build()

    await anchor.start()
    await client.start()
    discard await startLsmrService(anchor)
    discard await startLsmrService(client)
    defer:
      await client.stop()
      await anchor.stop()

    let observation = NearFieldObservation(
      provider: NearFieldBootstrapProvider.nfbpBle,
      networkId: networkId,
      peerId: anchorPeer,
      addrs: anchor.peerInfo.addrs,
      operatorId: "near-op",
      regionDigit: 5'u8,
      attestedPrefix: @[5'u8],
      serveDepth: 1'u8,
      directionMask: 0x1ff'u32,
      canIssueRootCert: true,
      rssi: -42'i32,
      observedAtMs: nowMillis(),
      ttlMs: 30 * 60 * 1000,
    )
    check observeNearFieldBootstrap(client, observation)
    check await client.refreshCoordinateRecord()
    let localRecord = localCoordinateRecord(client)
    check localRecord.isSome()
    check localRecord.get().data.certifiedPrefix()[0] == 5'u8

  asyncTest "witness refresh publishes local coordinate record and identify imports it":
    let rng = newRng()
    let networkId = "lsmr-identify-" & $nowMillis()

    let anchorKeyA = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorKeyB = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorKeyC = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeerA = PeerId.init(anchorKeyA).tryGet()
    let anchorPeerB = PeerId.init(anchorKeyB).tryGet()
    let anchorPeerC = PeerId.init(anchorKeyC).tryGet()

    let anchorA = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(anchorKeyA)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          serveWitness = true,
          operatorId = "op-a",
          regionDigit = 5'u8,
          minWitnessQuorum = 2,
        )
      )
      .build()
    let anchorB = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(anchorKeyB)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          serveWitness = true,
          operatorId = "op-b",
          regionDigit = 5'u8,
          minWitnessQuorum = 2,
        )
      )
      .build()
    let anchorC = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(anchorKeyC)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          serveWitness = true,
          operatorId = "op-c",
          regionDigit = 8'u8,
          minWitnessQuorum = 2,
        )
      )
      .build()

    await anchorA.start()
    await anchorB.start()
    await anchorC.start()
    discard await startLsmrService(anchorA)
    discard await startLsmrService(anchorB)
    discard await startLsmrService(anchorC)

    let client = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          anchors = @[
            LsmrAnchor(
              peerId: anchorPeerA,
              addrs: anchorA.peerInfo.addrs,
              operatorId: "op-a",
              regionDigit: 5'u8,
            ),
            LsmrAnchor(
              peerId: anchorPeerB,
              addrs: anchorB.peerInfo.addrs,
              operatorId: "op-b",
              regionDigit: 5'u8,
            ),
            LsmrAnchor(
              peerId: anchorPeerC,
              addrs: anchorC.peerInfo.addrs,
              operatorId: "op-c",
              regionDigit: 8'u8,
            ),
          ],
          minWitnessQuorum = 2,
        )
      )
      .build()

    let observer = newStandardSwitch(transport = TransportType.Memory)

    await client.start()
    await observer.start()
    discard await startLsmrService(client)
    defer:
      await observer.stop()
      await client.stop()
      await anchorC.stop()
      await anchorB.stop()
      await anchorA.stop()

    let lsmrSvc = getLsmrService(client)
    check not lsmrSvc.isNil

    check await client.refreshCoordinateRecord()
    let localRecord = localCoordinateRecord(client)
    check localRecord.isSome()
    check localRecord.get().data.attestedPrefix.len == 1
    check localRecord.get().data.attestedPrefix[0] == 5'u8
    check client.peerStore[ActiveLsmrBook].contains(client.peerInfo.peerId)
    check client.peerInfo.metadata.hasKey(LsmrCoordinateMetadataKey)

    await observer.connect(client.peerInfo.peerId, client.peerInfo.addrs)
    checkUntilTimeout:
      observer.peerStore[LsmrBook].contains(client.peerInfo.peerId)
    check observer.peerStore[LsmrBook][client.peerInfo.peerId].data.attestedPrefix[0] == 5'u8
