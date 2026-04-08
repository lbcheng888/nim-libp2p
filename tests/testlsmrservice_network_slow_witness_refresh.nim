{.used.}

import std/options

import chronos
import unittest2

import ./helpers
import ./testlsmrservice_network_helpers
import ../libp2p/[builders, crypto/crypto, lsmr, peerstore, switch]
import ../libp2p/protocols/identify
import ../libp2p/services/lsmrservice

suite "LSMR Service Network Slow Witness Refresh":
  teardown:
    checkTrackers()

  asyncTest "witness refresh publishes local coordinate record and identify snapshot imports it":
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

    await client.start()
    discard await startLsmrService(client)
    var anchorsStopped = false
    defer:
      await client.stop()
      if not anchorsStopped:
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

    await client.disconnect(anchorPeerA)
    await client.disconnect(anchorPeerB)
    await client.disconnect(anchorPeerC)
    await anchorA.disconnect(client.peerInfo.peerId)
    await anchorB.disconnect(client.peerInfo.peerId)
    await anchorC.disconnect(client.peerInfo.peerId)
    checkUntilTimeout:
      client.connManager.connCount(anchorPeerA) == 0 and
        client.connManager.connCount(anchorPeerB) == 0 and
        client.connManager.connCount(anchorPeerC) == 0
    checkUntilTimeout:
      anchorA.connManager.connCount(client.peerInfo.peerId) == 0 and
        anchorB.connManager.connCount(client.peerInfo.peerId) == 0 and
        anchorC.connManager.connCount(client.peerInfo.peerId) == 0
    await anchorC.stop()
    await anchorB.stop()
    await anchorA.stop()
    anchorsStopped = true

    let observer = newStandardSwitch(transport = TransportType.Memory)
    observer.peerStore.updatePeerInfo(
      IdentifyInfo(
        peerId: client.peerInfo.peerId,
        addrs: client.peerInfo.addrs,
        metadata: client.peerInfo.metadata,
      )
    )
    check observer.peerStore[LsmrBook].contains(client.peerInfo.peerId)
    check observer.peerStore[LsmrBook][client.peerInfo.peerId].data.attestedPrefix[0] == 5'u8
