{.used.}

import std/[json, options, os, sequtils]
from std/times import epochTime

import chronos
import unittest2

import ./helpers
import ../libp2p/[builders, crypto/crypto, lsmr, switch]
import ../libp2p/nameresolving/nameresolver
import ../libp2p/protocols/connectivity/autonatv2/[service, types]
import ../libp2p/protocols/kademlia/[kademlia, keys]
import ../libp2p/protocols/connectivity/relay/utils
import ../libp2p/protocols/rendezvous/rendezvous
import ../libp2p/services/hpservice
import ../libp2p/services/lsmrservice
import ../libp2p/services/wanbootstrapservice
import ./kademlia/utils

proc makeMemorySwitch(): Switch =
  newStandardSwitch(transport = TransportType.Memory)

proc makeMemorySwitch(resolver: NameResolver): Switch =
  newStandardSwitchBuilder(
    transport = TransportType.Memory, nameResolver = Opt.some(resolver)
  ).build()

proc tcpListenAddress(): MultiAddress =
  MultiAddress.init("/ip4/127.0.0.1/tcp/0").tryGet()

proc makeTcpSwitch(): Switch =
  newStandardSwitch(addrs = @[tcpListenAddress()])

proc makeTcpSwitch(resolver: NameResolver): Switch =
  newStandardSwitchBuilder(
    addrs = @[tcpListenAddress()], nameResolver = Opt.some(resolver)
  ).build()

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc withPeerId(ma: MultiAddress, peerId: PeerId): MultiAddress =
  MultiAddress.init($ma & "/p2p/" & $peerId).tryGet()

proc hasKadKey(kad: KadDHT, key: Key): bool =
  for bucket in kad.rtable.buckets:
    for entry in bucket.peers:
      if entry.nodeId == key:
        return true
  false

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

suite "WAN bootstrap service":
  teardown:
    checkTrackers()

  asyncTest "progressive selection prefers direct hints and enforces caps":
    let sw = makeMemorySwitch()
    await sw.start()
    defer:
      await sw.stop()

    let svc = await startWanBootstrapService(
      sw,
      WanBootstrapConfig.init(
        maxBootstrapCandidates = 4,
        recentStableSlots = 1,
        randomPoolCap = 8,
      ),
    )

    let peerA = makeMemorySwitch().peerInfo.peerId
    let peerB = makeMemorySwitch().peerInfo.peerId
    let peerC = makeMemorySwitch().peerInfo.peerId
    let peerD = makeMemorySwitch().peerInfo.peerId
    let baseTs = nowMillis() + 5_000

    check svc.registerBootstrapHint(
      peerA,
      @[MultiAddress.init("/ip4/10.0.0.1/tcp/4001").tryGet()],
      source = "invite",
    )
    check svc.registerBootstrapHint(
      peerB,
      @[MultiAddress.init("/ip4/10.0.0.2/tcp/4001").tryGet()],
      source = "rendezvous",
    )
    check svc.registerBootstrapHint(
      peerC,
      @[MultiAddress.init("/ip4/10.0.0.3/tcp/4001").tryGet()],
      source = "mdns",
    )
    check svc.registerBootstrapHint(
      peerD,
      @[MultiAddress.init("/ip4/10.0.0.4/tcp/4001").tryGet()],
      source = "dnsaddr",
    )

    check svc.markBootstrapSeen(peerA, baseTs + 4)
    check svc.markBootstrapSeen(peerB, baseTs + 3)
    check svc.markBootstrapSeen(peerC, baseTs + 2)
    check svc.markBootstrapSeen(peerD, baseTs + 1)

    let selected = svc.selectBootstrapCandidates()
    check selected.len == 4
    check selected[0].peerId == peerA
    check selected[0].selectionReason == "direct_hint"
    check selected[1].peerId == peerB
    check selected[1].selectionReason == "hint"
    check selected[2].peerId == peerC
    check selected[2].selectionReason == "recent_stable"
    check selected[3].peerId == peerD
    check selected[3].selectionReason == "random_pool"

  asyncTest "lsmr bias prefers nearer candidate within same trust tier":
    let rng = newRng()
    let networkId = "wan-bootstrap-lsmr-" & $nowMillis()
    let anchorKeyNear = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorKeyFar = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeerNear = PeerId.init(anchorKeyNear).tryGet()
    let anchorPeerFar = PeerId.init(anchorKeyFar).tryGet()

    let sw = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          anchors = @[
            LsmrAnchor(peerId: anchorPeerNear, operatorId: "op-near", regionDigit: 5'u8),
            LsmrAnchor(peerId: anchorPeerFar, operatorId: "op-far", regionDigit: 8'u8),
          ],
          minWitnessQuorum = 1,
          enableBootstrapBias = true,
        )
      )
      .withWanBootstrapService(
        WanBootstrapConfig.init(
          networkId = networkId,
          maxBootstrapCandidates = 2,
          recentStableSlots = 2,
          randomPoolCap = 2,
        )
      )
      .build()
    await sw.start()
    defer:
      await sw.stop()

    let svc = getWanBootstrapService(sw)
    check not svc.isNil
    check not getLsmrService(sw).isNil

    let localWitness =
      makeSignedWitness(
        anchorKeyNear, sw.peerInfo.peerId, networkId, "op-near", 5'u8, 5'u32, @[5'u8, 5'u8]
      )
    let localRecord =
      makeSignedCoordinateRecord(
        sw.peerInfo.privateKey, sw.peerInfo.peerId, networkId, @[5'u8, 5'u8], @[localWitness]
      )
    sw.peerInfo.metadata[LsmrCoordinateMetadataKey] = localRecord.encode().tryGet()
    sw.peerStore[LsmrBook][sw.peerInfo.peerId] = localRecord

    let peerNearKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let peerFarKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let peerNear = PeerId.init(peerNearKey).tryGet()
    let peerFar = PeerId.init(peerFarKey).tryGet()

    let nearRecord =
      makeSignedCoordinateRecord(
        peerNearKey,
        peerNear,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKeyNear, peerNear, networkId, "op-near", 5'u8, 10'u32, @[5'u8, 1'u8])],
      )
    let farRecord =
      makeSignedCoordinateRecord(
        peerFarKey,
        peerFar,
        networkId,
        @[8'u8, 1'u8],
        @[makeSignedWitness(anchorKeyFar, peerFar, networkId, "op-far", 8'u8, 10'u32, @[8'u8, 1'u8])],
      )

    sw.peerStore[LsmrBook][peerNear] = nearRecord
    sw.peerStore[LsmrBook][peerFar] = farRecord
    check svc.registerBootstrapHint(
      peerNear,
      @[MultiAddress.init("/ip4/10.0.1.1/tcp/4001").tryGet()],
      source = "mdns",
    )
    check svc.registerBootstrapHint(
      peerFar,
      @[MultiAddress.init("/ip4/10.0.1.2/tcp/4001").tryGet()],
      source = "mdns",
    )
    let ts = nowMillis() + 10_000
    check svc.markBootstrapSeen(peerNear, ts)
    check svc.markBootstrapSeen(peerFar, ts)

    let selected = svc.selectBootstrapCandidates(2)
    check selected.len == 2
    check selected[0].peerId == peerNear
    check selected[1].peerId == peerFar

  asyncTest "near field refreshed local coordinate drives lsmr primary candidate selection":
    let rng = newRng()
    let networkId = "wan-bootstrap-near-field-" & $nowMillis()

    let anchorKeyNear = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorKeyFar = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeerNear = PeerId.init(anchorKeyNear).tryGet()

    let anchorNear = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(anchorKeyNear)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          serveWitness = true,
          operatorId = "op-near",
          regionDigit = 5'u8,
          minWitnessQuorum = 1,
        )
      )
      .build()

    let clientLsmrConfig = LsmrConfig.init(
      networkId = networkId,
      minWitnessQuorum = 1,
      enableBootstrapBias = true,
      enableNearFieldBootstrap = true,
      nearFieldProviders = @[NearFieldBootstrapProvider.nfbpBle],
    )
    let client = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withLsmr(clientLsmrConfig)
      .build()

    await anchorNear.start()
    await client.start()
    discard await startLsmrService(anchorNear)
    discard await startLsmrService(client)
    defer:
      await client.stop()
      await anchorNear.stop()

    let observation = NearFieldObservation(
      provider: NearFieldBootstrapProvider.nfbpBle,
      networkId: networkId,
      peerId: anchorPeerNear,
      addrs: anchorNear.peerInfo.addrs,
      operatorId: "op-near",
      regionDigit: 5'u8,
      attestedPrefix: @[5'u8],
      serveDepth: 1'u8,
      directionMask: 0x1ff'u32,
      canIssueRootCert: true,
      rssi: -40'i32,
      observedAtMs: nowMillis(),
      ttlMs: 30 * 60 * 1000,
    )
    check observeNearFieldBootstrap(client, observation)
    check await client.refreshCoordinateRecord()

    let localRecord = activeCoordinateRecord(client, client.peerInfo.peerId)
    check localRecord.isSome()
    check localRecord.get().data.certifiedPrefix()[0] == 5'u8

    var wanCfg = WanBootstrapConfig.init(
      networkId = networkId,
      maxBootstrapCandidates = 2,
      recentStableSlots = 2,
      randomPoolCap = 2,
    )
    wanCfg.routingMode = RoutingPlaneMode.dualStack
    wanCfg.primaryPlane = PrimaryRoutingPlane.lsmr
    wanCfg.lsmrConfig = some(clientLsmrConfig)

    let svc = await startWanBootstrapService(client, wanCfg)
    check not svc.isNil
    check svc.config.primaryPlane == PrimaryRoutingPlane.lsmr

    let peerNearKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let peerFarKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let peerNear = PeerId.init(peerNearKey).tryGet()
    let peerFar = PeerId.init(peerFarKey).tryGet()

    client.peerStore[ActiveLsmrBook][peerNear] =
      makeSignedCoordinateRecord(
        peerNearKey,
        peerNear,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKeyNear, peerNear, networkId, "op-near", 5'u8, 10'u32, @[5'u8, 1'u8])],
      )
    client.peerStore[ActiveLsmrBook][peerFar] =
      makeSignedCoordinateRecord(
        peerFarKey,
        peerFar,
        networkId,
        @[8'u8, 1'u8],
        @[makeSignedWitness(anchorKeyFar, peerFar, networkId, "op-far", 8'u8, 10'u32, @[8'u8, 1'u8])],
      )

    check svc.registerBootstrapHint(
      peerNear,
      @[MultiAddress.init("/ip4/10.0.2.1/tcp/4001").tryGet()],
      source = "invite",
    )
    check svc.registerBootstrapHint(
      peerFar,
      @[MultiAddress.init("/ip4/10.0.2.2/tcp/4001").tryGet()],
      source = "invite",
    )
    let seenAt = nowMillis() + 20_000
    check svc.markBootstrapSeen(peerNear, seenAt)
    check svc.markBootstrapSeen(peerFar, seenAt)

    let selected = svc.selectBootstrapCandidates(2)
    check selected.len == 2
    check selected[0].peerId == peerNear
    check selected[0].selectionReason == "lsmr_certified"
    check selected[1].peerId == peerFar

    let status = svc.routingPlaneStatus()
    check status.mode == RoutingPlaneMode.dualStack
    check status.primary == PrimaryRoutingPlane.lsmr
    check status.shadowMode
    check status.lsmrActiveCertificates >= 3
    check selected[0].lsmrDistance < selected[1].lsmrDistance

  asyncTest "authoritative bootstrap peers suppress stale non-bootstrap candidates before first connection":
    let sw = makeMemorySwitch()
    await sw.start()
    defer:
      await sw.stop()

    let peerBootstrapA = makeMemorySwitch().peerInfo.peerId
    let peerBootstrapB = makeMemorySwitch().peerInfo.peerId
    let stalePeer = makeMemorySwitch().peerInfo.peerId

    let svc = await startWanBootstrapService(
      sw,
      WanBootstrapConfig.init(
        authoritativePeerIds = @[$peerBootstrapA, $peerBootstrapB],
        maxBootstrapCandidates = 6,
      ),
    )

    check svc.registerBootstrapHint(
      peerBootstrapA,
      @[MultiAddress.init("/ip6/2001:db8::1/tcp/4001").tryGet()],
      source = "bootstrap",
    )
    check svc.registerBootstrapHint(
      peerBootstrapB,
      @[MultiAddress.init("/ip6/2001:db8::2/udp/4001/quic-v1").tryGet()],
      source = "bootstrap",
    )
    check svc.registerBootstrapHint(
      stalePeer,
      @[MultiAddress.init("/ip4/10.0.0.9/tcp/4001").tryGet()],
      source = "journal",
    )

    let selected = svc.selectBootstrapCandidates()
    check selected.len == 2
    check selected.allIt($it.peerId in [ $peerBootstrapA, $peerBootstrapB ])
    check not selected.anyIt(it.peerId == stalePeer)

  asyncTest "joinViaBootstrap connects to hinted memory peer":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let svc = await startWanBootstrapService(client)
    check svc.registerBootstrapHint(
      server.peerInfo.peerId, server.peerInfo.addrs, source = "invite"
    )

    let result = await client.joinViaBootstrap()
    check result.ok
    check result.attemptedCount == 1
    check result.connectedCount == 1
    check result.attempts.len == 1
    check result.attempts[0].candidate.peerId == server.peerInfo.peerId
    checkUntilTimeout:
      client.isConnected(server.peerInfo.peerId)

  asyncTest "joinViaBootstrap stops after first stable connection when stopAfterConnected is one":
    let primary = makeMemorySwitch()
    let secondary = makeMemorySwitch()
    let client = makeMemorySwitch()
    await primary.start()
    await secondary.start()
    await client.start()
    defer:
      await client.stop()
      await secondary.stop()
      await primary.stop()

    let svc = await startWanBootstrapService(
      client,
      WanBootstrapConfig.init(
        maxBootstrapCandidates = 3,
        maxConcurrentBootstrapDials = 1,
        stopAfterConnected = 1,
      ),
    )
    check svc.registerBootstrapHint(
      primary.peerInfo.peerId, primary.peerInfo.addrs, source = "invite"
    )
    check svc.registerBootstrapHint(
      secondary.peerInfo.peerId, secondary.peerInfo.addrs, source = "dnsaddr"
    )

    let result = await client.joinViaBootstrap()
    check result.ok
    check result.connectedCount == 1
    check result.attemptedCount == 1
    check result.attempts.len == 1
    check result.attempts[0].candidate.peerId == primary.peerInfo.peerId
    checkUntilTimeout:
      client.isConnected(primary.peerInfo.peerId)
    check not client.isConnected(secondary.peerInfo.peerId)

  asyncTest "joinViaBootstrap retries later candidate addrs when the first addr fails":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let svc = await startWanBootstrapService(client)
    let badAddr = MultiAddress.init("/memory/999999").tryGet()
    check svc.registerBootstrapHint(
      server.peerInfo.peerId, @[badAddr, server.peerInfo.addrs[0]], source = "invite"
    )

    let result = await client.joinViaBootstrap()
    check result.ok
    check result.attemptedCount == 1
    check result.connectedCount == 1
    check result.attempts.len == 1
    check result.attempts[0].candidate.peerId == server.peerInfo.peerId
    check result.attempts[0].connected
    check result.attempts[0].attemptedAddr == $server.peerInfo.addrs[0]
    check client.isConnected(server.peerInfo.peerId)

  asyncTest "attestPeer can reject a connected bootstrap peer":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let rejectAttester: WanBootstrapAttester =
      proc(
          switch: Switch,
          peerId: PeerId,
          addrs: seq[MultiAddress],
          sources: seq[string],
          currentTrust: WanBootstrapTrust,
          stage: string,
      ): Future[WanBootstrapTrust] {.async: (raises: []).} =
        check peerId == server.peerInfo.peerId
        check addrs.len >= 1
        check "Hints" in sources
        check stage.len >= 5 and stage[0 .. 4] == "local"
        return WanBootstrapTrust.rejected
    let svc = await startWanBootstrapService(
      client, WanBootstrapConfig.init(attestPeer = rejectAttester)
    )
    check svc.registerBootstrapHint(
      server.peerInfo.peerId, server.peerInfo.addrs, source = "invite"
    )

    let result = await client.joinViaBootstrap()
    check not result.ok
    check result.attemptedCount == 1
    check result.connectedCount == 0
    check result.attempts.len == 1
    check not result.attempts[0].connected
    check not client.isConnected(server.peerInfo.peerId)
    let snapshot = svc.bootstrapStateSnapshot()
    check snapshot.metrics.attestRejected == 1
    check not snapshot.selectedCandidates.anyIt(it.peerId == server.peerInfo.peerId)
    check snapshot.hasLastJoinResult
    check snapshot.lastJoinResult.attempts.len == 1
    check snapshot.lastJoinResult.attempts[0].candidate.peerId == server.peerInfo.peerId
    check not snapshot.lastJoinResult.attempts[0].connected

  asyncTest "attestPeer can promote a connected peer to trusted":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let trustAttester: WanBootstrapAttester =
      proc(
          switch: Switch,
          peerId: PeerId,
          addrs: seq[MultiAddress],
          sources: seq[string],
          currentTrust: WanBootstrapTrust,
          stage: string,
      ): Future[WanBootstrapTrust] {.async: (raises: []).} =
        check peerId == server.peerInfo.peerId
        check currentTrust == WanBootstrapTrust.quarantine
        check stage.len >= 5 and stage[0 .. 4] == "local"
        return WanBootstrapTrust.trusted
    let svc = await startWanBootstrapService(
      client, WanBootstrapConfig.init(attestPeer = trustAttester)
    )
    check svc.registerBootstrapHint(
      server.peerInfo.peerId, server.peerInfo.addrs, source = "invite"
    )

    let result = await client.joinViaBootstrap()
    check result.ok
    check client.isConnected(server.peerInfo.peerId)
    let snapshot = svc.bootstrapStateSnapshot()
    check snapshot.metrics.attestTrusted == 1
    check snapshot.selectedCandidates.anyIt(
      it.peerId == server.peerInfo.peerId and it.trust == WanBootstrapTrust.trusted
    )

  asyncTest "journal persists and reloads bootstrap candidates":
    let peer = makeMemorySwitch().peerInfo.peerId
    let journalPath =
      getTempDir() / ("nim-libp2p-wan-bootstrap-" & $nowMillis() & ".json")
    if fileExists(journalPath):
      removeFile(journalPath)

    block:
      let sw = makeMemorySwitch()
      await sw.start()
      defer:
        await sw.stop()

      let svc = await startWanBootstrapService(
        sw, WanBootstrapConfig.init(journalPath = journalPath)
      )
      check svc.registerBootstrapHint(
        peer,
        @[MultiAddress.init("/ip4/10.0.0.50/tcp/4010").tryGet()],
        source = "invite",
      )
      check svc.saveBootstrapJournal()

    let restored = makeMemorySwitch()
    await restored.start()
    defer:
      await restored.stop()
      if fileExists(journalPath):
        removeFile(journalPath)

    let restoredSvc = await startWanBootstrapService(
      restored, WanBootstrapConfig.init(journalPath = journalPath)
    )
    let selected = restoredSvc.selectBootstrapCandidates()
    check selected.len >= 1
    check selected.anyIt(it.peerId == peer)

  asyncTest "snapshot protocol returns remote bootstrap snapshot":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let serverSvc = await startWanBootstrapService(server)
    let clientSvc = await startWanBootstrapService(client)
    let peer = makeMemorySwitch().peerInfo.peerId

    check serverSvc.registerBootstrapHint(
      peer,
      @[MultiAddress.init("/ip4/10.0.0.60/tcp/4011").tryGet()],
      source = "manual",
    )
    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

    let snapshot = await clientSvc.requestBootstrapSnapshot(server.peerInfo.peerId)
    check snapshot.len >= 1
    check snapshot.anyIt(it.peerId == peer)

  asyncTest "dnsaddr fallback and rendezvous refresh hydrate candidates":
    let resolver = MockResolver.new()
    let networkId = "wan-bootstrap-test-" & $nowMillis()
    let anchorNode = makeMemorySwitch()
    let publicNodeSwitch = makeMemorySwitch()
    let client = makeMemorySwitch(NameResolver(resolver))

    await anchorNode.start()
    await publicNodeSwitch.start()
    await client.start()
    defer:
      await client.stop()
      await publicNodeSwitch.stop()
      await anchorNode.stop()

    let anchorSeed = withPeerId(anchorNode.peerInfo.addrs[0], anchorNode.peerInfo.peerId)
    let dnsSeed =
      MultiAddress.init("/dnsaddr/bootstrap.example/p2p/" & $anchorNode.peerInfo.peerId).tryGet()
    resolver.txtResponses["_dnsaddr.bootstrap.example"] = @["dnsaddr=" & $anchorSeed]

    let anchorSvc = await startWanBootstrapService(
      anchorNode, bootstrapRoleConfig(WanBootstrapRole.anchor, networkId = networkId)
    )
    let publicSvc = await startWanBootstrapService(
      publicNodeSwitch,
      bootstrapRoleConfig(WanBootstrapRole.publicNode, networkId = networkId),
    )
    let clientSvc = await startWanBootstrapService(
      client,
      bootstrapRoleConfig(
        WanBootstrapRole.mobileEdge, networkId = networkId, fallbackDnsAddrs = @[dnsSeed]
      ),
    )

    var publicJoin = WanBootstrapJoinResult(timestampMs: nowMillis())
    proc publicConnectedToAnchor(): Future[bool] {.async: (raises: [CancelledError]).} =
      publicJoin = await publicSvc.joinViaBootstrap()
      publicJoin.ok and publicNodeSwitch.isConnected(anchorNode.peerInfo.peerId)

    check publicSvc.registerBootstrapHint(
      anchorNode.peerInfo.peerId, anchorNode.peerInfo.addrs, source = "invite"
    )
    checkUntilTimeout:
      await publicConnectedToAnchor()
    check publicJoin.ok
    let publicRefresh = await publicSvc.refreshBootstrapState()
    check publicRefresh >= 1 or publicSvc.bootstrapStateSnapshot().connectedPeerCount >= 1
    discard anchorSvc

    let clientJoin = await clientSvc.joinViaBootstrap()
    check clientJoin.ok
    check client.isConnected(anchorNode.peerInfo.peerId)
    checkUntilTimeout:
      clientSvc.selectBootstrapCandidates().anyIt(it.peerId == publicNodeSwitch.peerInfo.peerId)

  asyncTest "state snapshot captures join and refresh activity":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    let importedPeer = makeMemorySwitch().peerInfo.peerId
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let serverSvc = await startWanBootstrapService(server)
    let clientSvc = await startWanBootstrapService(client)
    check serverSvc.registerBootstrapHint(
      importedPeer,
      @[MultiAddress.init("/ip4/10.0.0.88/tcp/4088").tryGet()],
      source = "manual",
    )
    check clientSvc.registerBootstrapHint(
      server.peerInfo.peerId, server.peerInfo.addrs, source = "invite"
    )

    let joinResult = await clientSvc.joinViaBootstrap()
    check joinResult.ok
    let refreshImported = await clientSvc.refreshBootstrapState()
    check refreshImported >= 1

    let snapshot = clientSvc.bootstrapStateSnapshot()
    check snapshot.networkId == clientSvc.config.networkId
    check snapshot.connectedPeerCount >= 1
    check snapshot.hasLastJoinResult
    check snapshot.lastJoinResult.ok
    check snapshot.lastJoinResult.attemptedCount == joinResult.attemptedCount
    check snapshot.metrics.joinRuns == 1
    check snapshot.metrics.joinAttempts == joinResult.attemptedCount.int64
    check snapshot.metrics.joinConnected == joinResult.connectedCount.int64
    check snapshot.lastRefresh.timestampMs > 0
    check snapshot.lastRefresh.snapshotImported >= 1
    check snapshot.selectedCandidates.anyIt(it.peerId == importedPeer)
    let stateJson = clientSvc.bootstrapStateJson()
    check stateJson{"hasLastJoinResult"}.getBool()
    check stateJson{"metrics", "joinRuns"}.getBiggestInt() == 1

  asyncTest "joinViaBootstrap bootstraps mounted kademlia after connect":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    let serverKad = KadDHT.new(server, PermissiveValidator(), CandSelector())
    let clientKad = KadDHT.new(client, PermissiveValidator(), CandSelector())
    server.mount(serverKad)
    client.mount(clientKad)
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let clientSvc = await startWanBootstrapService(
      client, WanBootstrapConfig.init(enableKadAfterJoin = true)
    )
    check clientSvc.registerBootstrapHint(
      server.peerInfo.peerId, server.peerInfo.addrs, source = "invite"
    )

    let result = await clientSvc.joinViaBootstrap()
    check result.ok
    checkUntilTimeout:
      clientKad.hasKadKey(serverKad.rtable.selfId)
    let snapshot = clientSvc.bootstrapStateSnapshot()
    check snapshot.metrics.kadBootstrapRuns == 1
    check snapshot.metrics.kadBootstrapProtocols >= 1

  asyncTest "tcp multi client bootstrap discovers public peers through anchor":
    let resolver = MockResolver.new()
    let networkId = "wan-bootstrap-tcp-" & $nowMillis()
    let anchorNode = makeTcpSwitch()
    let publicNodeSwitch = makeTcpSwitch()
    let clientA = makeTcpSwitch(NameResolver(resolver))
    let clientB = makeTcpSwitch(NameResolver(resolver))

    await anchorNode.start()
    await publicNodeSwitch.start()
    await clientA.start()
    await clientB.start()
    defer:
      await clientB.stop()
      await clientA.stop()
      await publicNodeSwitch.stop()
      await anchorNode.stop()

    let anchorSeed = withPeerId(anchorNode.peerInfo.addrs[0], anchorNode.peerInfo.peerId)
    let dnsSeed =
      MultiAddress.init("/dnsaddr/bootstrap.example/p2p/" & $anchorNode.peerInfo.peerId).tryGet()
    resolver.txtResponses["_dnsaddr.bootstrap.example"] = @["dnsaddr=" & $anchorSeed]

    let anchorSvc = await startWanBootstrapService(
      anchorNode, bootstrapRoleConfig(WanBootstrapRole.anchor, networkId = networkId)
    )
    let publicSvc = await startWanBootstrapService(
      publicNodeSwitch,
      bootstrapRoleConfig(WanBootstrapRole.publicNode, networkId = networkId),
    )
    let clientASvc = await startWanBootstrapService(
      clientA,
      bootstrapRoleConfig(
        WanBootstrapRole.mobileEdge, networkId = networkId, fallbackDnsAddrs = @[dnsSeed]
      ),
    )
    let clientBSvc = await startWanBootstrapService(
      clientB,
      bootstrapRoleConfig(
        WanBootstrapRole.mobileEdge, networkId = networkId, fallbackDnsAddrs = @[dnsSeed]
      ),
    )

    check publicSvc.registerBootstrapHint(
      anchorNode.peerInfo.peerId, anchorNode.peerInfo.addrs, source = "invite"
    )
    let publicJoin = await publicSvc.joinViaBootstrap()
    check publicJoin.ok
    check publicNodeSwitch.isConnected(anchorNode.peerInfo.peerId)
    check (await publicSvc.refreshBootstrapState()) >= 1
    discard anchorSvc

    let joinAFut = clientASvc.joinViaBootstrap()
    let joinBFut = clientBSvc.joinViaBootstrap()
    await allFutures(@[joinAFut, joinBFut])
    let joinA = joinAFut.read()
    let joinB = joinBFut.read()
    check joinA.ok
    check joinB.ok
    check clientA.isConnected(anchorNode.peerInfo.peerId)
    check clientB.isConnected(anchorNode.peerInfo.peerId)
    checkUntilTimeout:
      clientASvc.selectBootstrapCandidates().anyIt(it.peerId == publicNodeSwitch.peerInfo.peerId)
      clientBSvc.selectBootstrapCandidates().anyIt(it.peerId == publicNodeSwitch.peerInfo.peerId)

    let stateA = clientASvc.bootstrapStateSnapshot()
    let stateB = clientBSvc.bootstrapStateSnapshot()
    check stateA.metrics.joinRuns >= 1
    check stateB.metrics.joinRuns >= 1
    check stateA.connectedPeerCount >= 1
    check stateB.connectedPeerCount >= 1
    check stateA.candidateCount >= 2
    check stateB.candidateCount >= 2

  test "mobile/public role config stays seedless without explicit fallback":
    let mobileCfg = bootstrapRoleConfig(WanBootstrapRole.mobileEdge, networkId = "seedless-mobile")
    let publicCfg = bootstrapRoleConfig(WanBootstrapRole.publicNode, networkId = "seedless-public")
    check mobileCfg.fallbackDnsAddrs.len == 0
    check publicCfg.fallbackDnsAddrs.len == 0
    check not mobileCfg.enableDnsaddrFallback
    check not publicCfg.enableDnsaddrFallback

  test "dual stack role config preserves legacy primary by default":
    let lsmrCfg =
      LsmrConfig.init(
        networkId = "dual-config",
        localSuffix = @[1'u8, 8'u8],
        serveDepth = 3,
      )
    let cfg = bootstrapDualStackRoleConfig(
      WanBootstrapRole.publicNode,
      lsmrCfg,
      networkId = "dual-config",
    )
    check cfg.routingMode == RoutingPlaneMode.dualStack
    check cfg.primaryPlane == PrimaryRoutingPlane.legacy
    check cfg.lsmrConfig.isSome()
    check cfg.legacyDiscoveryEnabled
    check not cfg.legacyCompatibilityOnly

  asyncTest "legacy public bootstrap dnsaddrs are ignored when fallback is disabled":
    let sw = makeMemorySwitch()
    await sw.start()
    defer:
      await sw.stop()

    let svc = await startWanBootstrapService(
      sw,
      bootstrapRoleConfig(WanBootstrapRole.mobileEdge, networkId = "seedless-filter"),
    )
    let legacyPeer = makeMemorySwitch().peerInfo.peerId
    let legacyAddr = MultiAddress.init(
      "/dnsaddr/bootstrap.libp2p.io/p2p/" & $legacyPeer
    ).tryGet()
    check not svc.registerBootstrapHint(legacyPeer, @[legacyAddr], source = "journal")
    check svc.selectBootstrapCandidates().len == 0

  asyncTest "third node learns first peer from second snapshot without static fallback":
    let networkId = "wan-bootstrap-snapshot-chain-" & $nowMillis()
    let node1 = makeMemorySwitch()
    let node2 = makeMemorySwitch()
    let node3 = makeMemorySwitch()

    await node1.start()
    await node2.start()
    await node3.start()
    defer:
      await node3.stop()
      await node2.stop()
      await node1.stop()

    discard await startWanBootstrapService(
      node1,
      bootstrapRoleConfig(WanBootstrapRole.publicNode, networkId = networkId),
    )
    let node2Svc = await startWanBootstrapService(
      node2,
      bootstrapRoleConfig(WanBootstrapRole.mobileEdge, networkId = networkId),
    )
    let node3Svc = await startWanBootstrapService(
      node3,
      bootstrapRoleConfig(WanBootstrapRole.mobileEdge, networkId = networkId),
    )

    check node2Svc.registerBootstrapHint(
      node1.peerInfo.peerId, node1.peerInfo.addrs, source = "manual"
    )
    let node2Join = await node2Svc.joinViaBootstrap()
    check node2Join.ok
    check node2Join.attemptedCount == 1
    check node2Join.attempts[0].candidate.peerId == node1.peerInfo.peerId
    check node2.isConnected(node1.peerInfo.peerId)

    check node3Svc.registerBootstrapHint(
      node2.peerInfo.peerId, node2.peerInfo.addrs, source = "manual"
    )
    let node3Join = await node3Svc.joinViaBootstrap()
    check node3Join.ok
    check node3Join.attemptedCount == 1
    check node3Join.attempts[0].candidate.peerId == node2.peerInfo.peerId
    check node3.isConnected(node2.peerInfo.peerId)
    checkUntilTimeout:
      node3Svc.selectBootstrapCandidates().anyIt(it.peerId == node1.peerInfo.peerId)

    let node3State = node3Svc.bootstrapStateSnapshot()
    check node3State.metrics.snapshotImported >= 1
    check node3State.selectedCandidates.anyIt(it.peerId == node1.peerInfo.peerId)
    check not node3State.selectedCandidates.anyIt(
      $it.peerId == "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
    )

  asyncTest "seeded join uses explicit seed addrs without peerstore fallback":
    let server = makeMemorySwitch()
    let client = makeMemorySwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let svc = await startWanBootstrapService(client)
    let badAddr = MultiAddress.init("/memory/999999").tryGet()
    check svc.registerBootstrapHint(
      server.peerInfo.peerId, @[badAddr], source = "stale"
    )

    let result = await client.joinViaSeedBootstrap(
      server.peerInfo.peerId,
      server.peerInfo.addrs,
      source = "manual",
    )
    check result.ok
    check result.attemptedCount == 1
    check result.connectedCount == 1
    check result.attempts.len == 1
    check result.attempts[0].stage == "seeded_direct_hint"
    check result.attempts[0].candidate.peerId == server.peerInfo.peerId
    check result.attempts[0].attemptedAddr == $server.peerInfo.addrs[0]
    check client.isConnected(server.peerInfo.peerId)

  asyncTest "seeded join preserves explicit exact addr order":
    let server = makeTcpSwitch()
    let client = makeTcpSwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let svc = await startWanBootstrapService(client)
    let badExact = MultiAddress.init(
      "/ip4/127.0.0.1/tcp/1/p2p/" & $server.peerInfo.peerId
    ).tryGet()
    let goodExact = withPeerId(server.peerInfo.addrs[0], server.peerInfo.peerId)

    let result = await client.joinViaSeedBootstrap(
      server.peerInfo.peerId,
      @[goodExact, badExact],
      source = "manual",
    )
    check result.ok
    check result.attemptedCount == 1
    check result.connectedCount == 1
    check result.attempts.len == 1
    check result.attempts[0].stage == "seeded_direct_hint"
    check result.attempts[0].candidate.peerId == server.peerInfo.peerId
    check result.attempts[0].attemptedAddr == $goodExact
    check client.isConnected(server.peerInfo.peerId)

  asyncTest "seeded join same-family policy forbids cross-family fallback":
    let server = makeTcpSwitch()
    let client = makeTcpSwitch()
    await server.start()
    await client.start()
    defer:
      await client.stop()
      await server.stop()

    let svc = await startWanBootstrapService(
      client,
      WanBootstrapConfig.init(
        exactSeedDialPolicy = ExactSeedDialPolicy.single,
      ),
    )
    let badIpv6Exact = MultiAddress.init(
      "/ip6/2001:db8::1/tcp/4001/p2p/" & $server.peerInfo.peerId
    ).tryGet()
    let goodExact = withPeerId(server.peerInfo.addrs[0], server.peerInfo.peerId)

    let result = await svc.joinViaSeedBootstrap(
      server.peerInfo.peerId,
      @[badIpv6Exact, goodExact],
      source = "manual",
    )
    check not result.ok
    check result.attemptedCount == 1
    check result.connectedCount == 0
    check result.attempts.len == 1
    check result.attempts[0].attemptedAddr == $badIpv6Exact
    check not client.isConnected(server.peerInfo.peerId)

  asyncTest "first three nodes prefer direct chain before fallback":
    let networkId = "wan-bootstrap-chain-" & $nowMillis()
    let node1 = makeMemorySwitch()
    let node2 = makeMemorySwitch()
    let node3 = makeMemorySwitch()
    let node3Fallback = makeMemorySwitch()
    var node2Stopped = false

    await node1.start()
    await node2.start()
    await node3.start()
    await node3Fallback.start()
    defer:
      await node3Fallback.stop()
      await node3.stop()
      if not node2Stopped:
        await node2.stop()
      await node1.stop()

    discard await startWanBootstrapService(
      node1,
      bootstrapRoleConfig(WanBootstrapRole.publicNode, networkId = networkId),
    )
    let node2Svc = await startWanBootstrapService(
      node2,
      bootstrapRoleConfig(WanBootstrapRole.mobileEdge, networkId = networkId),
    )
    check node2Svc.registerBootstrapHint(
      node1.peerInfo.peerId, node1.peerInfo.addrs, source = "manual"
    )

    let node2Join = await node2Svc.joinViaBootstrap()
    check node2Join.ok
    check node2Join.attemptedCount == 1
    check node2Join.attempts.len == 1
    check node2Join.attempts[0].candidate.peerId == node1.peerInfo.peerId
    check node2Join.attempts[0].connected
    check node2.isConnected(node1.peerInfo.peerId)

    let node3Svc = await startWanBootstrapService(
      node3,
      bootstrapRoleConfig(WanBootstrapRole.mobileEdge, networkId = networkId),
    )
    check node3Svc.registerBootstrapHint(
      node2.peerInfo.peerId, node2.peerInfo.addrs, source = "manual"
    )
    check node3Svc.registerBootstrapHint(
      node1.peerInfo.peerId, node1.peerInfo.addrs, source = "bootstrap"
    )

    let node3Join = await node3Svc.joinViaBootstrap()
    check node3Join.ok
    check node3Join.attemptedCount == 1
    check node3Join.attempts.len == 1
    check node3Join.attempts[0].candidate.peerId == node2.peerInfo.peerId
    check node3Join.attempts[0].connected
    check node3.isConnected(node2.peerInfo.peerId)

    await node2.stop()
    node2Stopped = true

    let node3FallbackSvc = await startWanBootstrapService(
      node3Fallback,
      bootstrapRoleConfig(WanBootstrapRole.mobileEdge, networkId = networkId),
    )
    check node3FallbackSvc.registerBootstrapHint(
      node2.peerInfo.peerId, node2.peerInfo.addrs, source = "manual"
    )
    check node3FallbackSvc.registerBootstrapHint(
      node1.peerInfo.peerId, node1.peerInfo.addrs, source = "bootstrap"
    )

    let fallbackJoin = await node3FallbackSvc.joinViaBootstrap()
    check fallbackJoin.ok
    check fallbackJoin.attemptedCount == 2
    check fallbackJoin.attempts.len == 2
    check fallbackJoin.attempts[0].candidate.peerId == node2.peerInfo.peerId
    check not fallbackJoin.attempts[0].connected
    check fallbackJoin.attempts[1].candidate.peerId == node1.peerInfo.peerId
    check fallbackJoin.attempts[1].connected

  test "builder wires wan bootstrap service":
    let cfg = WanBootstrapConfig.init(
      maxBootstrapCandidates = 5, maxConcurrentBootstrapDials = 2
    )
    let sw =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withWanBootstrapService(cfg)
        .build()
    let svc = sw.getWanBootstrapService()
    check svc != nil
    check svc.config.maxBootstrapCandidates == 5
    check svc.config.maxConcurrentBootstrapDials == 2

  asyncTest "builder anchor profile mounts relay hop, snapshot, rendezvous and autonat server":
    let cfg = bootstrapRoleConfig(
      WanBootstrapRole.anchor, networkId = "builder-anchor-profile-" & $nowMillis()
    )
    let sw =
      newStandardSwitchBuilder(transport = TransportType.Memory)
        .withWanBootstrapProfile(cfg)
        .build()
    await sw.start()
    defer:
      await sw.stop()

    let svc = sw.getWanBootstrapService()
    check svc != nil
    check svc.config.mountRendezvousProtocol
    check sw.services.anyIt(it of WanBootstrapService)
    checkUntilTimeout:
      cfg.snapshotCodec in sw.peerInfo.protocols
      RelayV2HopCodec in sw.peerInfo.protocols
      $AutonatV2Codec.DialRequest in sw.peerInfo.protocols
      RendezVousCodecs().allIt(it in sw.peerInfo.protocols)

  asyncTest "builder public profile mounts autonat client without relay defaults":
    let cfg = bootstrapRoleConfig(
      WanBootstrapRole.publicNode, networkId = "builder-public-profile-" & $nowMillis()
    )
    let sw =
      newStandardSwitchBuilder(transport = TransportType.Memory)
        .withWanBootstrapProfile(cfg)
        .build()
    await sw.start()
    defer:
      await sw.stop()

    let svc = sw.getWanBootstrapService()
    check svc != nil
    check sw.services.anyIt(it of WanBootstrapService)
    check sw.services.anyIt(it of AutonatV2Service)
    checkUntilTimeout:
      cfg.snapshotCodec in sw.peerInfo.protocols
      $AutonatV2Codec.DialBack in sw.peerInfo.protocols
    check RelayV2HopCodec notin sw.peerInfo.protocols
    check RelayV2StopCodec notin sw.peerInfo.protocols

  asyncTest "builder mobile profile enables relay client and autonat client":
    let cfg = bootstrapRoleConfig(
      WanBootstrapRole.mobileEdge, networkId = "builder-mobile-profile-" & $nowMillis()
    )
    let hpCfg = HPServiceConfig.init(
      directProbeTimeout = 4.seconds,
      maxProbeCandidates = 3,
      keepaliveInterval = 8.seconds,
      relayCloseDelay = 1.seconds,
      upgradeDelay = 300.milliseconds,
      dcutrConnectTimeout = 13.seconds,
      maxDcutrDialableAddrs = 6,
    )
    let sw =
      newStandardSwitchBuilder(transport = TransportType.Memory)
        .withWanBootstrapProfile(cfg, hpConfig = hpCfg)
        .build()
    await sw.start()
    defer:
      await sw.stop()

    let svc = sw.getWanBootstrapService()
    check svc != nil
    check svc.config.maxConcurrentBootstrapDials == 2
    check sw.services.anyIt(it of WanBootstrapService)
    check sw.services.anyIt(it of AutonatV2Service)
    check sw.services.anyIt(it of HPService)
    let hpSvc = HPService(sw.services.filterIt(it of HPService)[0])
    check hpSvc.config.directProbeTimeout == 4.seconds
    check hpSvc.config.maxProbeCandidates == 3
    check hpSvc.config.keepaliveInterval == 8.seconds
    check hpSvc.config.relayCloseDelay == 1.seconds
    check hpSvc.config.upgradeDelay == 300.milliseconds
    check hpSvc.config.dcutrConnectTimeout == 13.seconds
    check hpSvc.config.maxDcutrDialableAddrs == 6
    checkUntilTimeout:
      cfg.snapshotCodec in sw.peerInfo.protocols
      $AutonatV2Codec.DialBack in sw.peerInfo.protocols
      RelayV2StopCodec in sw.peerInfo.protocols
    check RelayV2HopCodec notin sw.peerInfo.protocols

  asyncTest "builder dual stack profile mounts lsmr without changing legacy primary":
    let lsmrCfg =
      LsmrConfig.init(
        networkId = "builder-dual-" & $nowMillis(),
        localSuffix = @[5'u8, 1'u8],
        serveDepth = 3,
      )
    let cfg = bootstrapDualStackRoleConfig(
      WanBootstrapRole.publicNode,
      lsmrCfg,
      networkId = lsmrCfg.networkId,
    )
    let sw =
      newStandardSwitchBuilder(transport = TransportType.Memory)
        .withWanBootstrapDualStackProfile(cfg, lsmrCfg)
        .build()
    await sw.start()
    defer:
      await sw.stop()

    let svc = sw.getWanBootstrapService()
    check svc != nil
    check svc.config.routingMode == RoutingPlaneMode.dualStack
    check svc.config.primaryPlane == PrimaryRoutingPlane.legacy
    check svc.config.lsmrConfig.isSome()
    check sw.services.anyIt(it of LsmrService)
    check sw.services.anyIt(it of WanBootstrapService)

  asyncTest "lsmr primary selection ignores legacy only candidates":
    let rng = newRng()
    let networkId = "wan-bootstrap-lsmr-primary-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()
    let sw = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withLsmr(
        LsmrConfig.init(
          networkId = networkId,
          anchors = @[LsmrAnchor(peerId: anchorPeer, operatorId: "op-root", regionDigit: 5'u8)],
          minWitnessQuorum = 1,
        )
      )
      .withWanBootstrapService(
        WanBootstrapConfig.init(
          networkId = networkId,
          routingMode = RoutingPlaneMode.dualStack,
          primaryPlane = PrimaryRoutingPlane.lsmr,
          lsmrConfig = some(
            LsmrConfig.init(
              networkId = networkId,
              anchors = @[LsmrAnchor(peerId: anchorPeer, operatorId: "op-root", regionDigit: 5'u8)],
              minWitnessQuorum = 1,
            )
          ),
        )
      )
      .build()
    await sw.start()
    defer:
      await sw.stop()

    let svc = getWanBootstrapService(sw)
    let localWitness =
      makeSignedWitness(anchorKey, sw.peerInfo.peerId, networkId, "op-root", 5'u8, 6'u32, @[5'u8, 5'u8])
    let localRecord =
      makeSignedCoordinateRecord(
        sw.peerInfo.privateKey, sw.peerInfo.peerId, networkId, @[5'u8, 5'u8], @[localWitness]
      )
    sw.peerInfo.metadata[LsmrCoordinateMetadataKey] = localRecord.encode().tryGet()
    sw.peerStore[LsmrBook][sw.peerInfo.peerId] = localRecord
    sw.peerStore[ActiveLsmrBook][sw.peerInfo.peerId] = localRecord

    let trustedKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let trustedPeer = PeerId.init(trustedKey).tryGet()
    let trustedRecord =
      makeSignedCoordinateRecord(
        trustedKey,
        trustedPeer,
        networkId,
        @[5'u8, 1'u8],
        @[makeSignedWitness(anchorKey, trustedPeer, networkId, "op-root", 5'u8, 9'u32, @[5'u8, 1'u8])],
      )
    sw.peerStore[LsmrBook][trustedPeer] = trustedRecord
    sw.peerStore[ActiveLsmrBook][trustedPeer] = trustedRecord
    check svc.registerBootstrapHint(
      trustedPeer,
      @[MultiAddress.init("/ip4/10.0.9.1/tcp/4001").tryGet()],
      source = "mdns",
    )

    let legacyOnlyPeer = makeMemorySwitch().peerInfo.peerId
    check svc.registerBootstrapHint(
      legacyOnlyPeer,
      @[MultiAddress.init("/ip4/10.0.9.2/tcp/4001").tryGet()],
      source = "invite",
    )

    let selected = svc.selectBootstrapCandidates(2)
    check selected.len == 1
    check selected[0].peerId == trustedPeer
    check selected[0].selectionReason == "lsmr_certified"

  asyncTest "lsmr primary falls back to bootstrap candidates before any certificate exists":
    let rng = newRng()
    let networkId = "wan-bootstrap-lsmr-fallback-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()
    let lsmrCfg = LsmrConfig.init(
      networkId = networkId,
      anchors = @[LsmrAnchor(peerId: anchorPeer, operatorId: "op-root", regionDigit: 5'u8)],
      minWitnessQuorum = 1,
    )
    let sw = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withLsmr(lsmrCfg)
      .withWanBootstrapService(
        WanBootstrapConfig.init(
          networkId = networkId,
          routingMode = RoutingPlaneMode.dualStack,
          primaryPlane = PrimaryRoutingPlane.lsmr,
          lsmrConfig = some(lsmrCfg),
          authoritativePeerIds = @[$anchorPeer],
        )
      )
      .build()
    await sw.start()
    defer:
      await sw.stop()

    let svc = getWanBootstrapService(sw)
    check not svc.isNil
    check svc.registerBootstrapHint(
      anchorPeer,
      @[MultiAddress.init("/ip4/10.0.9.9/tcp/4001/p2p/" & $anchorPeer).tryGet()],
      source = "journal",
    )

    let selected = svc.selectBootstrapCandidates(1)
    check selected.len == 1
    check selected[0].peerId == anchorPeer
    check selected[0].selectionReason == "hint"

  asyncTest "lsmr primary promotes connected authoritative bootstrap into join state":
    let rng = newRng()
    let networkId = "wan-bootstrap-lsmr-promote-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()
    let lsmrCfg = LsmrConfig.init(
      networkId = networkId,
      anchors = @[LsmrAnchor(peerId: anchorPeer, operatorId: "op-root", regionDigit: 5'u8)],
      minWitnessQuorum = 1,
    )
    let anchor = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(anchorKey)
      .build()
    let client = newStandardSwitchBuilder(transport = TransportType.Memory).build()

    await anchor.start()
    await client.start()
    defer:
      await client.stop()
      await anchor.stop()

    let anchorSvc = await startWanBootstrapService(
      anchor,
      bootstrapRoleConfig(
        WanBootstrapRole.anchor,
        networkId = networkId,
        authoritativePeerIds = @[$anchorPeer],
      ),
    )
    let clientSvc = await startWanBootstrapService(
      client,
      WanBootstrapConfig.init(
        networkId = networkId,
        role = WanBootstrapRole.mobileEdge,
        routingMode = RoutingPlaneMode.dualStack,
        primaryPlane = PrimaryRoutingPlane.lsmr,
        lsmrConfig = some(lsmrCfg),
        authoritativePeerIds = @[$anchorPeer],
        enableRendezvous = false,
        enableSnapshotProtocol = false,
        enableKadAfterJoin = false,
      ),
    )
    check not anchorSvc.isNil
    check not clientSvc.isNil
    let anchorSeed = withPeerId(anchor.peerInfo.addrs[0], anchorPeer)
    check clientSvc.registerBootstrapHint(anchorPeer, @[anchorSeed], source = "journal")

    await client.connect(anchor.peerInfo.peerId, anchor.peerInfo.addrs)
    let joinResult = await clientSvc.promoteConnectedBootstrapPeer(
      anchorPeer,
      @[anchorSeed],
      "manual_connected_seed",
    )
    check joinResult.ok
    check joinResult.attemptedCount == 1
    check joinResult.connectedCount == 1

    let snapshot = clientSvc.bootstrapStateSnapshot()
    check snapshot.metrics.joinRuns >= 1
    check snapshot.hasLastJoinResult
    check snapshot.lastJoinResult.ok
    check snapshot.lastJoinResult.connectedCount >= 1

  asyncTest "lsmr bootstrap handoff installs local cert and publishes it to anchor":
    let rng = newRng()
    let networkId = "wan-bootstrap-lsmr-handoff-" & $nowMillis()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let anchorPeer = PeerId.init(anchorKey).tryGet()
    let anchorLsmrCfg =
      LsmrConfig.init(
        networkId = networkId,
        serveWitness = true,
        operatorId = "unimaker-root",
        regionDigit = 5'u8,
        minWitnessQuorum = 1,
      )
    let clientLsmrCfg =
      LsmrConfig.init(
        networkId = networkId,
        minWitnessQuorum = 1,
        anchors = @[
          LsmrAnchor(
            peerId: anchorPeer,
            addrs: @[],
            operatorId: "unimaker-root",
            regionDigit: 5'u8,
            attestedPrefix: @[5'u8],
            serveDepth: 2'u8,
            directionMask: 0x1ff'u32,
            canIssueRootCert: true,
          )
        ],
      )
    let anchor = newStandardSwitchBuilder(transport = TransportType.Memory)
      .withPrivateKey(anchorKey)
      .build()
    let client = newStandardSwitchBuilder(transport = TransportType.Memory).build()

    await anchor.start()
    await client.start()
    defer:
      await client.stop()
      await anchor.stop()

    let anchorCfg =
      bootstrapDualStackRoleConfig(
        role = WanBootstrapRole.anchor,
        lsmrConfig = anchorLsmrCfg,
        networkId = networkId,
        authoritativePeerIds = @[$anchorPeer],
        primaryPlane = PrimaryRoutingPlane.lsmr,
      )
    var clientCfg =
      bootstrapDualStackRoleConfig(
        role = WanBootstrapRole.mobileEdge,
        lsmrConfig = clientLsmrCfg,
        networkId = networkId,
        authoritativePeerIds = @[$anchorPeer],
        primaryPlane = PrimaryRoutingPlane.lsmr,
      )
    clientCfg.enableRendezvous = false
    clientCfg.enableSnapshotProtocol = false
    clientCfg.enableKadAfterJoin = false

    let anchorSvc = await startWanBootstrapService(anchor, anchorCfg)
    let clientSvc = await startWanBootstrapService(client, clientCfg)
    let anchorLsmrSvc = getLsmrService(anchor)
    let clientLsmrSvc = getLsmrService(client)
    check not anchorSvc.isNil
    check not clientSvc.isNil
    check not anchorLsmrSvc.isNil
    check not clientLsmrSvc.isNil

    let anchorSeed = withPeerId(anchor.peerInfo.addrs[0], anchorPeer)
    check clientSvc.registerBootstrapHint(anchorPeer, @[anchorSeed], source = "journal")

    await client.connect(anchor.peerInfo.peerId, anchor.peerInfo.addrs)
    let joinResult = await clientSvc.promoteConnectedBootstrapPeer(
      anchorPeer,
      @[anchorSeed],
      "manual_connected_seed",
    )
    check joinResult.ok
    check joinResult.attemptedCount == 1
    check joinResult.connectedCount == 1

    checkUntilTimeout:
      localCoordinateRecord(client).isSome()
    let clientLocal = localCoordinateRecord(client)
    check clientLocal.isSome()
    check clientLocal.get().data.certifiedPrefix() == @[5'u8]
    check client.peerStore[ActiveLsmrBook].contains(client.peerInfo.peerId)
    check clientLsmrSvc.routingPlaneStatus().lsmrLocalCertReady
    check clientLsmrSvc.routingPlaneStatus().lsmrWitnessSuccess >= 1
    check clientSvc.bootstrapStateSnapshot().routingStatus.lsmrLastRefreshReason == "ok"

    checkUntilTimeout:
      anchor.peerStore[ActiveLsmrBook].contains(client.peerInfo.peerId)
    check anchor.peerStore[ActiveLsmrBook][client.peerInfo.peerId].data.certifiedPrefix() == @[5'u8]
