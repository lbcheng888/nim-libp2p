{.used.}

import std/[json, jsonutils, net, options, os, strutils]
from std/times import epochTime

import chronos
import chronos/apps/http/httpclient
import unittest2

import ./helpers
import ../libp2p/[crypto/crypto, lsmr, peerstore]
import ../libp2p/rwad/identity
import ../libp2p/fabric/[node, types]
from ../libp2p/fabric/rpc/server import FabricRpcServer, newFabricRpcServer, start, stop

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc pickPort(host: string = "127.0.0.1"): Port =
  var sock: Socket
  var created = false
  try:
    let isIpv6 = host.contains(":")
    sock = newSocket(
      domain = if isIpv6: Domain.AF_INET6 else: Domain.AF_INET,
      sockType = SockType.SOCK_STREAM,
      protocol = Protocol.IPPROTO_TCP,
      buffered = true,
      inheritable = false,
    )
    created = true
    sock.setSockOpt(OptReuseAddr, true)
    sock.bindAddr(Port(0), host)
    sock.listen()
    let (_, port) = sock.getLocalAddr()
    return port
  finally:
    if created:
      close(sock)

proc httpGetJson(url: string): Future[JsonNode] {.async.} =
  let session = HttpSessionRef.new()
  defer:
    await session.closeWait()
  let reqRes = HttpClientRequestRef.get(session, url)
  doAssert reqRes.isOk(), $reqRes.error
  let response = await reqRes.get().send()
  defer:
    await response.closeWait()
  doAssert response.status == 200, "unexpected GET status " & $response.status
  parseJson(stringOf(await response.getBodyBytes()))

proc httpPostJson(url: string, payload: string): Future[JsonNode] {.async.} =
  let session = HttpSessionRef.new()
  defer:
    await session.closeWait()
  let reqRes = HttpClientRequestRef.post(
    session,
    url,
    headers = @[("Content-Type", "application/json")],
    body = payload,
  )
  doAssert reqRes.isOk(), $reqRes.error
  let response = await reqRes.get().send()
  defer:
    await response.closeWait()
  doAssert response.status == 200, "unexpected POST status " & $response.status
  parseJson(stringOf(await response.getBodyBytes()))

proc mustNewFabricNode(cfg: FabricNodeConfig): FabricNode {.raises: [].} =
  try:
    newFabricNode(cfg)
  except Exception as exc:
    raiseAssert("newFabricNode failed: " & exc.msg)

proc makeSignedWitness(
    anchorKey: PrivateKey,
    subjectPeerId: PeerId,
    networkId: string,
    operatorId: string,
    regionDigit: uint8,
    rttMs: uint32,
): SignedLsmrWitness =
  let anchorPeerId = PeerId.init(anchorKey).tryGet()
  let witness = LsmrWitness(
    subjectPeerId: subjectPeerId,
    anchorPeerId: anchorPeerId,
    networkId: networkId,
    regionDigit: regionDigit,
    latencyDigit: latencyDigitForRtt(rttMs),
    rttMs: rttMs,
    operatorId: operatorId,
    issuedAtMs: nowMillis(),
    expiresAtMs: nowMillis() + 30 * 60 * 1000,
    prefixDepth: 2'u8,
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
    confidence: 80'u8,
    witnesses: witnesses,
    parentDigest: "",
    migrationDigest: "",
    coverageMask: 0x1ff'u32,
    certifiedDepth: uint8(attestedPrefix.len),
    recordDigest: "",
    heTuParity: computeHeTuParity(1'u64, attestedPrefix, 0x1ff'u32, "", "", DefaultLsmrVersion),
    version: DefaultLsmrVersion,
  )
  record.recordDigest = computeRecordDigest(record)
  SignedLsmrCoordinateRecord.init(peerKey, record).tryGet()

proc makeConfig(
    baseDir: string,
    routingMode: RoutingPlaneMode,
    primaryPlane: PrimaryRoutingPlane,
    lsmrCfg: LsmrConfig,
): FabricNodeConfig =
  let identity = newNodeIdentity()
  createDir(baseDir)
  saveIdentity(baseDir / "identity.json", identity)
  saveGenesis(baseDir / "genesis.json", createDefaultGenesis(identity, "fabric-routing-planes"))
  FabricNodeConfig(
    dataDir: baseDir,
    identityPath: baseDir / "identity.json",
    genesisPath: baseDir / "genesis.json",
    listenAddrs: @["/ip4/127.0.0.1/tcp/0"],
    bootstrapAddrs: @[],
    legacyBootstrapAddrs: @[],
    lsmrBootstrapAddrs: @[],
    lsmrPath: @[5'u8],
    routingMode: routingMode,
    primaryPlane: primaryPlane,
    lsmrConfig: some(lsmrCfg),
  )

suite "Fabric routing planes":
  teardown:
    checkTrackers()

  test "dual stack legacy primary keeps legacy discovery mounted":
    let baseDir = getTempDir() / ("fabric-routing-legacy-" & $nowMillis())
    let cfg = makeConfig(
      baseDir,
      RoutingPlaneMode.dualStack,
      PrimaryRoutingPlane.legacy,
      LsmrConfig.init(networkId = "fabric-routing-legacy", minWitnessQuorum = 1),
    )
    var node: FabricNode
    {.cast(gcsafe).}:
      node = mustNewFabricNode(cfg)
    defer:
      waitFor node.stop()

    let status = node.fabricStatus()
    check not node.network.isNil
    check not node.network.rendezvous.isNil
    check not node.network.kad.isNil
    check status.routingStatus.mode == RoutingPlaneMode.dualStack
    check status.routingStatus.primary == PrimaryRoutingPlane.legacy
    check status.routingStatus.shadowMode
    check status.routingStatus.lsmrActiveCertificates == 0

  test "dual stack lsmr primary surfaces certified coordinate counts":
    let baseDir = getTempDir() / ("fabric-routing-lsmr-" & $nowMillis())
    let networkId = "fabric-routing-lsmr"
    let cfg = makeConfig(
      baseDir,
      RoutingPlaneMode.dualStack,
      PrimaryRoutingPlane.lsmr,
      LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1),
    )
    var node: FabricNode
    {.cast(gcsafe).}:
      node = mustNewFabricNode(cfg)
    defer:
      waitFor node.stop()

    let rng = newRng()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let remoteKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let remotePeer = PeerId.init(remoteKey).tryGet()
    let witness = makeSignedWitness(anchorKey, remotePeer, networkId, "fabric-op", 5'u8, 8'u32)
    node.network.switch.peerStore[ActiveLsmrBook][remotePeer] =
      makeSignedCoordinateRecord(remoteKey, remotePeer, networkId, @[5'u8, 1'u8], @[witness])

    let status = node.fabricStatus()
    check not node.network.rendezvous.isNil
    check not node.network.kad.isNil
    check status.routingStatus.mode == RoutingPlaneMode.dualStack
    check status.routingStatus.primary == PrimaryRoutingPlane.lsmr
    check status.routingStatus.shadowMode
    check status.routingStatus.lsmrActiveCertificates == 1

  asyncTest "rpc fabric.status returns routing status for dual stack legacy primary":
    let baseDir = getTempDir() / ("fabric-rpc-routing-legacy-" & $nowMillis())
    let cfg = makeConfig(
      baseDir,
      RoutingPlaneMode.dualStack,
      PrimaryRoutingPlane.legacy,
      LsmrConfig.init(networkId = "fabric-rpc-routing-legacy", minWitnessQuorum = 1),
    )
    var node: FabricNode
    {.cast(gcsafe).}:
      node = mustNewFabricNode(cfg)
    let port = pickPort()
    let server = newFabricRpcServer(node, "127.0.0.1", int(port))
    try:
      await server.start()
      let url = "http://127.0.0.1:" & $port & "/"
      let rpcPayload = %*{
        "jsonrpc": "2.0",
        "id": 1,
        "method": "fabric.status",
        "params": {}
      }
      let rpcResponse = await httpPostJson(url, $rpcPayload)
      let rpcStatus = jsonTo(rpcResponse["result"], FabricStatusSnapshot)
      check rpcStatus.routingStatus.mode == RoutingPlaneMode.dualStack
      check rpcStatus.routingStatus.primary == PrimaryRoutingPlane.legacy
      check rpcStatus.routingStatus.shadowMode

      let health = await httpGetJson("http://127.0.0.1:" & $port & "/healthz")
      let healthStatus = jsonTo(health, FabricStatusSnapshot)
      check healthStatus.routingStatus.mode == RoutingPlaneMode.dualStack
      check healthStatus.routingStatus.primary == PrimaryRoutingPlane.legacy
      check healthStatus.routingStatus.shadowMode
    finally:
      await server.stop()
      await node.stop()

  asyncTest "rpc fabric.status returns routing status for dual stack lsmr primary":
    let baseDir = getTempDir() / ("fabric-rpc-routing-lsmr-" & $nowMillis())
    let networkId = "fabric-rpc-routing-lsmr"
    let cfg = makeConfig(
      baseDir,
      RoutingPlaneMode.dualStack,
      PrimaryRoutingPlane.lsmr,
      LsmrConfig.init(networkId = networkId, minWitnessQuorum = 1),
    )
    var node: FabricNode
    {.cast(gcsafe).}:
      node = mustNewFabricNode(cfg)
    let rng = newRng()
    let anchorKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let remoteKey = PrivateKey.random(ECDSA, rng[]).tryGet()
    let remotePeer = PeerId.init(remoteKey).tryGet()
    let witness = makeSignedWitness(anchorKey, remotePeer, networkId, "fabric-rpc-op", 5'u8, 8'u32)
    node.network.switch.peerStore[ActiveLsmrBook][remotePeer] =
      makeSignedCoordinateRecord(remoteKey, remotePeer, networkId, @[5'u8, 1'u8], @[witness])
    let port = pickPort()
    let server = newFabricRpcServer(node, "127.0.0.1", int(port))
    try:
      await server.start()
      let url = "http://127.0.0.1:" & $port & "/"
      let rpcPayload = %*{
        "jsonrpc": "2.0",
        "id": 1,
        "method": "fabric.status",
        "params": {}
      }
      let rpcResponse = await httpPostJson(url, $rpcPayload)
      let rpcStatus = jsonTo(rpcResponse["result"], FabricStatusSnapshot)
      check rpcStatus.routingStatus.mode == RoutingPlaneMode.dualStack
      check rpcStatus.routingStatus.primary == PrimaryRoutingPlane.lsmr
      check rpcStatus.routingStatus.shadowMode
      check rpcStatus.routingStatus.lsmrActiveCertificates == 1

      let health = await httpGetJson("http://127.0.0.1:" & $port & "/healthz")
      let healthStatus = jsonTo(health, FabricStatusSnapshot)
      check healthStatus.routingStatus.mode == RoutingPlaneMode.dualStack
      check healthStatus.routingStatus.primary == PrimaryRoutingPlane.lsmr
      check healthStatus.routingStatus.shadowMode
      check healthStatus.routingStatus.lsmrActiveCertificates == 1
    finally:
      await server.stop()
      await node.stop()
