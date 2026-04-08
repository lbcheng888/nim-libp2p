{.used.}

import std/options

import chronos
import unittest2

import ./helpers
import ./testlsmrservice_network_helpers
import ../libp2p/[builders, crypto/crypto, lsmr, switch]
import ../libp2p/services/lsmrservice

suite "LSMR Service Network Slow Near Field":
  teardown:
    checkTrackers()

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
      await client.disconnect(anchor.peerInfo.peerId)
      await anchor.disconnect(client.peerInfo.peerId)
      checkUntilTimeout:
        client.connManager.connCount(anchor.peerInfo.peerId) == 0
      checkUntilTimeout:
        anchor.connManager.connCount(client.peerInfo.peerId) == 0
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
