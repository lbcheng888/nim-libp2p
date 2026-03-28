import std/[json, options, os, sequtils, tables, times, unittest]

import chronos

import ../libp2p/lsmr
import ../libp2p/fabric
import ../libp2p/rwad/identity
import ../libp2p/rwad/types

proc makeGenesis(validators: seq[NodeIdentity], observer: NodeIdentity): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "fabric-network"
  result = GenesisSpec(
    params: params,
    validators: @[],
    balances: @[KeyValueU64(key: params.rewardPoolAccount, value: 5_000_000)],
    names: @[],
    contents: @[],
    mintedSupply: 5_000_000,
  )
  for identity in validators:
    result.validators.add(ValidatorInfo(
      address: identity.account,
      peerId: peerIdString(identity.peerId),
      publicKey: encodePublicKeyHex(identity.publicKey),
      stake: 10_000,
      delegatedStake: 0,
      active: true,
      jailed: false,
      slashedAmount: 0,
      lastUpdatedAt: 0,
    ))
    result.balances.add(KeyValueU64(key: identity.account, value: 1_000_000))
  result.balances.add(KeyValueU64(key: observer.account, value: 1_000_000))

proc signedTx(identity: NodeIdentity, nonce: uint64, suffix: string): Tx =
  result = Tx(
    txId: "",
    kind: txContentPublishIntent,
    sender: "",
    senderPublicKey: "",
    nonce: nonce,
    epoch: 0,
    timestamp: int64(nonce),
    payload: %*{
      "intent": {
        "contentId": "net-post-" & suffix,
        "author": identity.account,
        "title": "net-post-" & suffix,
        "body": "network body " & suffix,
        "kind": "text",
        "manifestCid": "net-manifest-" & suffix,
        "createdAt": int(nonce),
        "accessPolicy": "public",
        "previewCid": "net-preview-" & suffix,
        "seqNo": int(nonce)
      }
    },
    signature: "",
  )
  signTx(identity, result)

proc waitUntil(
    predicate: proc(): bool {.closure, gcsafe.}, timeoutMs = 30_000, intervalMs = 100
): Future[void] {.async: (raises: [CancelledError, Exception]).} =
  let attempts = max(1, timeoutMs div intervalMs)
  for _ in 0 ..< attempts:
    if predicate():
      return
    await sleepAsync(intervalMs)
  raise newException(ValueError, "condition timed out")

proc stopAll(nodes: seq[FabricNode]) {.async.} =
  for idx in countdown(nodes.len - 1, 0):
    try:
      await nodes[idx].stop()
    except CatchableError:
      discard

proc rawAddrs(node: FabricNode): seq[string] =
  if node.isNil or node.network.isNil or node.network.switch.isNil:
    return @[]
  for addr in node.network.switch.peerInfo.addrs:
    result.add($addr)

proc rootAnchor(identity: NodeIdentity, operatorId: string): LsmrAnchor =
  LsmrAnchor(
    peerId: identity.peerId,
    addrs: @[],
    operatorId: operatorId,
    regionDigit: 5'u8,
    attestedPrefix: @[5'u8],
    serveDepth: 2'u8,
    directionMask: 0x1ff'u32,
    canIssueRootCert: true,
  )

proc makeLsmrConfig(
    networkId, operatorId: string,
    localSuffix: LsmrPath,
    anchors: seq[LsmrAnchor],
): LsmrConfig =
  LsmrConfig.init(
    networkId = networkId,
    anchors = anchors,
    serveWitness = true,
    operatorId = operatorId,
    regionDigit = 5'u8,
    localSuffix = localSuffix,
    serveDepth = 2,
    minWitnessQuorum = 1,
  )

proc makeNodeConfig(
    baseDir: string,
    identity: NodeIdentity,
    genesis: GenesisSpec,
    localSuffix: LsmrPath,
    bootstrapAddrs: seq[string],
    anchors: seq[LsmrAnchor],
): FabricNodeConfig =
  createDir(baseDir)
  saveIdentity(baseDir / "identity.json", identity)
  saveGenesis(baseDir / "genesis.json", genesis)
  FabricNodeConfig(
    dataDir: baseDir,
    identityPath: baseDir / "identity.json",
    genesisPath: baseDir / "genesis.json",
    listenAddrs: @["/ip4/127.0.0.1/tcp/0"],
    bootstrapAddrs: @[],
    legacyBootstrapAddrs: @[],
    lsmrBootstrapAddrs: bootstrapAddrs,
    lsmrPath: @[5'u8] & localSuffix,
    routingMode: RoutingPlaneMode.lsmrOnly,
    primaryPlane: PrimaryRoutingPlane.lsmr,
    lsmrConfig: some(makeLsmrConfig(genesis.params.chainId, operatorId = identity.account, localSuffix = localSuffix, anchors = anchors)),
  )

suite "Fabric 3 validators + 1 observer":
  test "observer catches up to checkpoint projection results":
    let v0 = newNodeIdentity()
    let v1 = newNodeIdentity()
    let v2 = newNodeIdentity()
    let observerId = newNodeIdentity()
    let genesis = makeGenesis(@[v0, v1, v2], observerId)

    let n0 = newFabricNode(v0, genesis, @[5'u8])
    let n1 = newFabricNode(v1, genesis, @[1'u8])
    let n2 = newFabricNode(v2, genesis, @[9'u8])
    let n3 = newFabricNode(observerId, genesis, @[3'u8])

    for pair in @[(n0, n1), (n0, n2), (n0, n3), (n1, n2), (n1, n3), (n2, n3)]:
      connectPeer(pair[0], pair[1])

    var nonce0 = 0'u64
    var nonce1 = 0'u64
    var nonce2 = 0'u64

    for idx in 0 ..< 20:
      inc nonce0
      let tx0 = signedTx(v0, nonce0, "v0-" & $idx)
      let out0 = n0.submitTx(tx0)
      check out0.accepted
      check out0.certified

      inc nonce1
      let tx1 = signedTx(v1, nonce1, "v1-" & $idx)
      let out1 = n1.submitTx(tx1)
      check out1.accepted
      check out1.certified

      inc nonce2
      let tx2 = signedTx(v2, nonce2, "v2-" & $idx)
      let out2 = n2.submitTx(tx2)
      check out2.accepted
      check out2.certified

    inc nonce0
    let last = n0.submitTx(signedTx(v0, nonce0, "v0-final"))
    check last.accepted
    check last.certified

    let checkpoint0 = n0.getCheckpoint()
    let checkpoint1 = n1.getCheckpoint()
    let checkpoint2 = n2.getCheckpoint()
    let checkpoint3 = n3.getCheckpoint()
    check checkpoint0.isSome()
    check checkpoint1.isSome()
    check checkpoint2.isSome()
    check checkpoint3.isSome()
    check checkpoint0.get().checkpointId == checkpoint1.get().checkpointId
    check checkpoint0.get().checkpointId == checkpoint2.get().checkpointId
    check checkpoint0.get().checkpointId == checkpoint3.get().checkpointId
    check checkpoint3.get().candidate.era == 0

    let observerProjection = n3.queryProjection("content")
    check observerProjection.kind == JObject
    check observerProjection["contents"].kind == JArray
    check observerProjection["contents"].len == 60

    let observerDetail = n3.contentDetail("net-post-v0-0")
    check observerDetail.isSome()
    check observerDetail.get().title == "net-post-v0-0"

    let observerFeed = n3.feedSnapshot(10)
    check observerFeed.len == 10

  test "lsmr only real network converges without kad or rendezvous":
    let baseDir = getTempDir() / ("fabric-network-lsmr-" & $getTime().toUnix())
    createDir(baseDir)
    let v0 = newNodeIdentity()
    let v1 = newNodeIdentity()
    let v2 = newNodeIdentity()
    let observerId = newNodeIdentity()
    let genesis = makeGenesis(@[v0, v1, v2], observerId)
    let anchors = @[rootAnchor(v0, "root-a"), rootAnchor(v1, "root-b")]
    var nodes: seq[FabricNode] = @[]
    try:
      let n0 = newFabricNode(makeNodeConfig(baseDir / "node-0", v0, genesis, @[1'u8], @[], anchors))
      nodes.add(n0)
      waitFor n0.start()

      let n1 = newFabricNode(makeNodeConfig(baseDir / "node-1", v1, genesis, @[9'u8], n0.rawAddrs(), anchors))
      nodes.add(n1)
      waitFor n1.start()

      let bootstrap = n0.rawAddrs() & n1.rawAddrs()
      let n2 = newFabricNode(makeNodeConfig(baseDir / "node-2", v2, genesis, @[3'u8], bootstrap, anchors))
      nodes.add(n2)
      waitFor n2.start()

      let n3 = newFabricNode(makeNodeConfig(baseDir / "node-3", observerId, genesis, @[7'u8], bootstrap, anchors))
      nodes.add(n3)
      waitFor n3.start()

      for node in nodes:
        check not node.network.isNil
        check node.network.kad.isNil
        check node.network.rendezvous.isNil
        check node.fabricStatus().routingStatus.mode == RoutingPlaneMode.lsmrOnly
        check node.fabricStatus().routingStatus.primary == PrimaryRoutingPlane.lsmr

      waitFor sleepAsync(2000)

      var nonce0 = 0'u64
      for idx in 0 ..< 61:
        inc nonce0
        check n0.submitTx(signedTx(v0, nonce0, "lsmr-v0-" & $idx)).accepted

      waitFor waitUntil(
        proc(): bool {.gcsafe.} =
          var ready = true
          {.cast(gcsafe).}:
            for node in nodes:
              let checkpoint = node.getCheckpoint()
              if checkpoint.isNone():
                ready = false
                break
            if ready:
              let projection = nodes[^1].queryProjection("content")
              ready = projection.kind == JObject and
                projection.hasKey("contents") and
                projection["contents"].kind == JArray and
                projection["contents"].len == 61
          ready,
        timeoutMs = 90_000,
      )

      let checkpoint0 = nodes[0].getCheckpoint()
      let checkpoint1 = nodes[1].getCheckpoint()
      let checkpoint2 = nodes[2].getCheckpoint()
      let checkpoint3 = nodes[3].getCheckpoint()
      check checkpoint0.isSome()
      check checkpoint1.isSome()
      check checkpoint2.isSome()
      check checkpoint3.isSome()
      check checkpoint0.get().checkpointId == checkpoint1.get().checkpointId
      check checkpoint0.get().checkpointId == checkpoint2.get().checkpointId
      check checkpoint0.get().checkpointId == checkpoint3.get().checkpointId

      let observerProjection = nodes[^1].queryProjection("content")
      check observerProjection["contents"].len == 61
      let observerDetail = nodes[^1].contentDetail("net-post-lsmr-v0-0")
      check observerDetail.isSome()
      check observerDetail.get().title == "net-post-lsmr-v0-0"
      check nodes[^1].feedSnapshot(10).len == 10
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)
