{.used.}

import chronos
import unittest2

import ./helpers
import ./testlsmrservice_network_helpers
import ../libp2p/[builders, crypto/crypto, lsmr, peerstore, switch]
import ../libp2p/services/lsmrservice

suite "LSMR Service Network Slow Sync Foreign Rooted":
  teardown:
    checkTrackers()

  asyncTest "coordinate sync promotes foreign rooted chain with explicit root anchor":
    let rng = newRng()
    let networkId = "lsmr-sync-foreign-rooted-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()
    let subjectKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let subjectPeer = PeerId.init(subjectKey).tryGet()
    let clientKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let clientConfig =
      LsmrConfig.init(
        networkId = networkId,
        minWitnessQuorum = 1,
        anchors = @[LsmrAnchor(
          peerId: anchorPeer,
          operatorId: "root-a",
          regionDigit: 5'u8,
          attestedPrefix: @[5'u8],
          canIssueRootCert: true,
        )],
      )

    let server = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(subjectKey)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          minWitnessQuorum = 1,
          serveWitness = true,
          operatorId = "subject-op",
        )
      )
      .build()
    let client = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(clientKey)
      .withLsmr(clientConfig)
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

    let rootWitness =
      makeSignedWitness(anchorKey, subjectPeer, networkId, "root-a", 5'u8, 5'u32, @[5'u8])
    let rootRecord =
      makeSignedCoordinateRecord(subjectKey, subjectPeer, networkId, @[5'u8], @[rootWitness])
    let deepWitness =
      makeSignedWitness(subjectKey, subjectPeer, networkId, "subject-op", 5'u8, 6'u32, @[5'u8, 1'u8])
    let deepRecord =
      makeSignedCoordinateRecord(subjectKey, subjectPeer, networkId, @[5'u8, 1'u8], @[deepWitness])

    server.peerStore[LsmrChainBook][subjectPeer] = @[rootRecord, deepRecord]
    server.peerStore[LsmrBook][subjectPeer] = deepRecord
    server.peerStore[ActiveLsmrBook][subjectPeer] = deepRecord

    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)
    check await clientSvc.syncCoordinatesFromPeer(subjectPeer)
    await sleepAsync(100.milliseconds)

    if not client.peerStore[ActiveLsmrBook].contains(subjectPeer):
      client.peerStore[LsmrBook][subjectPeer] = rootRecord
      let rootSnapshot = validateLsmrPeer(client, clientConfig, subjectPeer)
      client.peerStore[LsmrBook][subjectPeer] = deepRecord
      let deepSnapshot = validateLsmrPeer(client, clientConfig, subjectPeer)
      checkpoint "root trust=" & $rootSnapshot.trust & " reason=" & rootSnapshot.reason &
        " deep trust=" & $deepSnapshot.trust & " reason=" & deepSnapshot.reason
    check client.peerStore[ActiveLsmrBook].contains(subjectPeer)
    check client.peerStore[ActiveLsmrBook][subjectPeer].data.certifiedPrefix() == @[5'u8, 1'u8]
