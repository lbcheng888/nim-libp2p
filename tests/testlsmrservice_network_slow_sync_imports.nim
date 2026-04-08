{.used.}

import chronos
import unittest2

import ./helpers
import ./testlsmrservice_network_helpers
import ../libp2p/[builders, crypto/crypto, lsmr, peerstore, switch]
import ../libp2p/services/lsmrservice

suite "LSMR Service Network Slow Sync Imports":
  teardown:
    checkTrackers()

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
          anchors = @[LsmrAnchor(
            peerId: serverPeer,
            addrs: server.peerInfo.addrs,
            operatorId: "root",
            regionDigit: 5'u8,
          )],
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
