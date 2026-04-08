{.used.}

import chronos
import unittest2

import ./helpers
import ./testlsmrservice_network_helpers
import ../libp2p/[builders, crypto/crypto, lsmr, peerstore, switch]
import ../libp2p/services/lsmrservice

suite "LSMR Service Network Sync Migration":
  teardown:
    checkTrackers()

  asyncTest "coordinate sync promotes migrated coordinates after raw install":
    let rng = newRng()
    let networkId = "lsmr-sync-promote-" & $nowMillis()
    let serverKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let serverPeer = PeerId.init(serverKey).tryGet()
    let clientKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let committeeKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let committeePeer = PeerId.init(committeeKey).tryGet()
    let subjectKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let subjectPeer = PeerId.init(subjectKey).tryGet()

    let server = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(serverKey)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          minWitnessQuorum = 1,
          serveWitness = true,
          operatorId = "root",
        )
      )
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
      await client.disconnect(server.peerInfo.peerId)
      await server.disconnect(client.peerInfo.peerId)
      checkUntilTimeout:
        client.connManager.connCount(server.peerInfo.peerId) == 0
      checkUntilTimeout:
        server.connManager.connCount(client.peerInfo.peerId) == 0
      await client.stop()
      await server.stop()

    let serverWitness =
      makeSignedWitness(serverKey, serverPeer, networkId, "root", 5'u8, 5'u32, @[5'u8])
    let serverRecord =
      makeSignedCoordinateRecord(serverKey, serverPeer, networkId, @[5'u8], @[serverWitness])
    server.peerStore[LsmrChainBook][serverPeer] = @[serverRecord]
    server.peerStore[LsmrBook][serverPeer] = serverRecord
    server.peerStore[ActiveLsmrBook][serverPeer] = serverRecord

    let committeeWitness =
      makeSignedWitness(serverKey, committeePeer, networkId, "root", 5'u8, 8'u32, @[5'u8, 5'u8])
    let committeeRecord =
      makeSignedCoordinateRecord(
        committeeKey,
        committeePeer,
        networkId,
        @[5'u8, 5'u8],
        @[committeeWitness],
      )
    server.peerStore[LsmrChainBook][committeePeer] = @[committeeRecord]
    server.peerStore[LsmrBook][committeePeer] = committeeRecord
    server.peerStore[ActiveLsmrBook][committeePeer] = committeeRecord

    let migrationWitness =
      makeSignedWitness(committeeKey, subjectPeer, networkId, "committee", 5'u8, 9'u32, @[5'u8, 9'u8])
    let migration =
      makeSignedMigrationRecord(
        subjectKey,
        subjectPeer,
        networkId,
        @[5'u8, 1'u8],
        @[5'u8, 9'u8],
        "old-record",
        @[migrationWitness],
        epochId = 2'u64,
      )
    discard installMigrationRecord(server, migration)

    var migratedData =
      makeSignedCoordinateRecord(
        subjectKey,
        subjectPeer,
        networkId,
        @[5'u8, 9'u8],
        @[migrationWitness],
      ).data
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
    let migratedRecord = SignedLsmrCoordinateRecord.init(subjectKey, migratedData).tryGet()
    server.peerStore[LsmrChainBook][subjectPeer] = @[migratedRecord]
    server.peerStore[LsmrBook][subjectPeer] = migratedRecord
    server.peerStore[ActiveLsmrBook][subjectPeer] = migratedRecord

    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)
    check await clientSvc.syncCoordinatesFromPeer(serverPeer)
    check client.peerStore[LsmrMigrationBook][subjectPeer].len == 1
    check client.peerStore[ActiveLsmrBook].contains(committeePeer)
    check client.peerStore[ActiveLsmrBook].contains(subjectPeer)
    check client.peerStore[ActiveLsmrBook][subjectPeer].data.certifiedPrefix() == @[5'u8, 9'u8]
