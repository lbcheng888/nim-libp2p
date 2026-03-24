{.used.}

import std/[options, sequtils, strutils]
from std/times import epochTime

import chronos
import unittest2

import ./helpers
import ../libp2p/[builders, crypto/crypto, errors, peerstore, switch]
import ../libp2p/services/[hpservice, mobilemeshservice, noderesourceservice]

proc makeMemorySwitch(): Switch =
  newStandardSwitch(transport = TransportType.Memory)

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

suite "Mobile mesh service":
  teardown:
    checkTrackers()

  asyncTest "mobile sightings feed peerstore and candidates stay sorted":
    let sw = makeMemorySwitch()
    await sw.start()
    defer:
      await sw.stop()

    let svc = await startMobileMeshService(
      sw,
      MobileMeshServiceConfig.init(
        cleanupInterval = ZeroDuration,
        sightingTtl = 500.milliseconds,
        wakeTokenTtl = 500.milliseconds,
        peerStoreTtl = 5.minutes,
        maxSightings = 2,
        maxWakeTokens = 2,
      ),
    )

    let peerA = makeMemorySwitch().peerInfo.peerId
    let peerB = makeMemorySwitch().peerInfo.peerId
    let peerC = makeMemorySwitch().peerInfo.peerId
    let addrB = MultiAddress.init("/ip4/10.0.0.2/udp/4001/quic-v1").tryGet()
    let addrC = MultiAddress.init("/ip4/10.0.0.3/udp/4001/quic-v1").tryGet()
    let staleTs = nowMillis() - 5_000

    check svc.recordMobilePeerSighting(
      MobilePeerSighting(
        peerId: peerA,
        medium: mdmMdnsFallback,
        addrs: @[MultiAddress.init("/ip4/10.0.0.1/udp/4001/quic-v1").tryGet()],
        observedAtMs: staleTs,
        proximityScore: 5,
      )
    )
    check svc.recordMobilePeerSighting(
      MobilePeerSighting(
        peerId: peerB,
        medium: mdmWifiAware,
        addrs: @[addrB],
        proximityScore: 90,
      )
    )
    check svc.recordMobilePeerSighting(
      MobilePeerSighting(
        peerId: peerC,
        medium: mdmBleProximity,
        addrs: @[addrC, addrC],
        proximityScore: 70,
      )
    )

    let candidates = svc.mobileNearfieldCandidates()
    check candidates.len == 2
    check candidates[0].peerId == peerB
    check candidates[1].peerId == peerC
    check sw.peerStore.getAddresses(peerB) == @[addrB]
    check sw.peerStore.getAddresses(peerC) == @[addrC]

    check svc.setMobilePeerWakeState(peerC, mpwsHibernated)
    check svc.mobilePeerState(peerC) == some(mpwsHibernated)

  asyncTest "wake token registry rejects stale entries and sorts by expiry":
    let sw = makeMemorySwitch()
    await sw.start()
    defer:
      await sw.stop()

    let svc = await startMobileMeshService(
      sw,
      MobileMeshServiceConfig.init(
        cleanupInterval = ZeroDuration,
        wakeTokenTtl = 500.milliseconds,
        maxWakeTokens = 2,
      ),
    )

    let peerA = makeMemorySwitch().peerInfo.peerId
    let peerB = makeMemorySwitch().peerInfo.peerId
    let peerC = makeMemorySwitch().peerInfo.peerId
    let now = nowMillis()

    check svc.registerMobileWakeToken(
      MobileWakeToken(
        peerId: peerA,
        provider: "fcm",
        token: "stale",
        updatedAtMs: now - 5_000,
        expiresAtMs: now - 1_000,
      )
    )
    check svc.registerMobileWakeToken(
      MobileWakeToken(
        peerId: peerB,
        provider: "fcm",
        token: "fresh-b",
        updatedAtMs: now,
        expiresAtMs: now + 10_000,
      )
    )
    check svc.registerMobileWakeToken(
      MobileWakeToken(
        peerId: peerC,
        provider: "apns",
        token: "fresh-c",
        updatedAtMs: now,
        expiresAtMs: now + 20_000,
      )
    )

    let tokens = svc.mobileWakeCandidates()
    check tokens.len == 2
    check tokens[0].peerId == peerC
    check tokens[1].peerId == peerB

  asyncTest "mobile discovery snapshot exposes dial hints phantom peers and transport health":
    let sw = makeMemorySwitch()
    await sw.start()
    defer:
      await sw.stop()

    let svc = await startMobileMeshService(
      sw,
      MobileMeshServiceConfig.init(
        cleanupInterval = ZeroDuration,
        sightingTtl = 5.minutes,
        wakeTokenTtl = 5.minutes,
      ),
    )

    let peerA = makeMemorySwitch().peerInfo.peerId
    let peerB = makeMemorySwitch().peerInfo.peerId
    let nanAddr = MultiAddress.init("/nan/id/android-demo/quic-v1").tryGet()
    let nearlinkAddr = MultiAddress.init("/nearlink/mac/11AA22BB33/quic-v1").tryGet()

    check svc.recordMobilePeerSighting(
      MobilePeerSighting(
        peerId: peerA,
        medium: mdmWifiAware,
        addrs: @[nanAddr],
        proximityScore: 95,
        note: "nan-ready",
      )
    )
    check svc.recordMobilePeerSighting(
      MobilePeerSighting(
        peerId: peerB,
        medium: mdmNearLink,
        addrs: @[nearlinkAddr],
        proximityScore: 90,
        note: "nearlink-ready",
      )
    )
    check svc.registerMobileWakeToken(
      MobileWakeToken(
        peerId: peerB,
        provider: "pushkit",
        token: "wake-peer-b",
        updatedAtMs: nowMillis(),
        expiresAtMs: nowMillis() + 60_000,
      )
    )
    check svc.setPhantomPeerState(peerB, ppsWaking, mdmNearLink)
    check svc.setTransportHealth(
      TransportHealthSnapshot(
        state: ppsMigrating,
        activeMedium: mdmWifiAware,
        lastKnownMedium: mdmBleProximity,
        directQuicOnly: true,
        connectionMigrationEnabled: true,
        note: "wifi_to_cell",
      )
    )

    let snapshot = svc.mobileDiscoverySnapshot()
    check snapshot.nearfield.len == 2
    check snapshot.dialHints.len == 2
    check snapshot.dialHints[0].peerId == peerA
    check snapshot.dialHints[0].preferredAddrs == @[nanAddr]
    check snapshot.dialHints[1].peerId == peerB
    check snapshot.dialHints[1].wakeState == mpwsActive
    check snapshot.phantomPeers.len == 2
    check snapshot.phantomPeers[0].peerId == peerB
    check snapshot.transportHealth.state == ppsMigrating
    check snapshot.transportHealth.connectionMigrationEnabled
    check snapshot.transportHealth.directQuicOnly

  test "mobile full p2p builder wires required services":
    let builder = SwitchBuilder.new().withRng(newRng())
    let hpCfg = HPServiceConfig.init(
      directProbeTimeout = 3.seconds,
      maxProbeCandidates = 4,
      keepaliveInterval = 7.seconds,
      relayCloseDelay = 1.seconds,
      upgradeDelay = 200.milliseconds,
      dcutrConnectTimeout = 12.seconds,
      maxDcutrDialableAddrs = 5,
    )

    when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
      let sw =
        builder.withMobileFullP2PProfile(dataDir = "/tmp/mobile-full", hpConfig = hpCfg).build()
      check sw.getNodeResourceService() != nil
      check sw.getMobileMeshService() != nil
      check sw.services.anyIt(it of HPService)
      let hpSvc = HPService(sw.services.filterIt(it of HPService)[0])
      check hpSvc.config.directProbeTimeout == 3.seconds
      check hpSvc.config.maxProbeCandidates == 4
      check hpSvc.config.keepaliveInterval == 7.seconds
      check hpSvc.config.relayCloseDelay == 1.seconds
      check hpSvc.config.upgradeDelay == 200.milliseconds
      check hpSvc.config.dcutrConnectTimeout == 12.seconds
      check hpSvc.config.maxDcutrDialableAddrs == 5
      check sw.transports.len == 2
      check sw.peerInfo.listenAddrs.len == 1
      check ($sw.peerInfo.listenAddrs[0]).contains("/quic")
    else:
      expect LPError:
        discard builder.withMobileFullP2PProfile(dataDir = "/tmp/mobile-full").build()
