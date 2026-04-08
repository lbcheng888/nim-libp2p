{.used.}

import chronos
import unittest2

import ./helpers
import ./testlsmrservice_network_helpers
import ../libp2p/[builders, crypto/crypto, lsmr, peerstore, switch]
import ../libp2p/services/lsmrservice

suite "LSMR Service Network Sync Self Chain":
  teardown:
    checkTrackers()

  asyncTest "coordinate sync promotes same peer chain from root to deep":
    let rng = newRng()
    let networkId = "lsmr-sync-self-chain-" & $nowMillis()
    let serverKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let serverPeer = PeerId.init(serverKey).tryGet()
    let clientKey = PrivateKey.random(ECDSA, rng[]).tryGet()

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

    let rootWitness =
      makeSignedWitness(serverKey, serverPeer, networkId, "root", 5'u8, 5'u32, @[5'u8])
    let rootRecord =
      makeSignedCoordinateRecord(serverKey, serverPeer, networkId, @[5'u8], @[rootWitness])
    let deepWitness =
      makeSignedWitness(serverKey, serverPeer, networkId, "root", 5'u8, 6'u32, @[5'u8, 1'u8])
    let deepRecord =
      makeSignedCoordinateRecord(serverKey, serverPeer, networkId, @[5'u8, 1'u8], @[deepWitness])

    server.peerStore[LsmrChainBook][serverPeer] = @[rootRecord, deepRecord]
    server.peerStore[LsmrBook][serverPeer] = deepRecord
    server.peerStore[ActiveLsmrBook][serverPeer] = deepRecord

    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)
    check await clientSvc.syncCoordinatesFromPeer(serverPeer)
    await sleepAsync(100.milliseconds)
    check client.peerStore[ActiveLsmrBook].contains(serverPeer)
    check client.peerStore[ActiveLsmrBook][serverPeer].data.certifiedPrefix() == @[5'u8, 1'u8]
