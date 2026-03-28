import std/[json, options, os, sequtils, tables, times, unittest]

import chronos

import ../libp2p/switch
import ../libp2p/lsmr
import ../libp2p/rwad

const ValidatorCount = 3

proc makeGenesis(identities: seq[NodeIdentity], validatorCount = ValidatorCount): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "rwad-network"
  var validators: seq[ValidatorInfo] = @[]
  var balances = @[KeyValueU64(key: params.rewardPoolAccount, value: 500_000)]
  for idx, identity in identities:
    if idx < min(validatorCount, identities.len):
      validators.add(ValidatorInfo(
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
    balances.add(KeyValueU64(key: identity.account, value: 500_000))
  GenesisSpec(
    params: params,
    validators: validators,
    balances: balances,
    names: @[],
    contents: @[],
    mintedSupply: 1_000_000 + uint64(identities.len * 500_000),
  )

proc signedPublish(identity: NodeIdentity, nonce: uint64, contentId: string): Tx =
  result = Tx(
    txId: "",
    kind: txContentPublishIntent,
    sender: "",
    senderPublicKey: "",
    nonce: nonce,
    epoch: 0,
    timestamp: nonce.int64,
    payload: %*{
      "intent": {
        "contentId": contentId,
        "author": identity.account,
        "title": "network post",
        "body": "hello rwad network",
        "kind": "text",
        "manifestCid": "manifest-" & contentId,
        "createdAt": nonce,
        "accessPolicy": "public",
        "previewCid": "preview-" & contentId,
        "seqNo": nonce
      },
      "sourceGraph": {
        "sourceUri": "snapshot",
        "parentContentIds": [],
        "relatedNames": [],
        "importedFrom": []
      }
    },
    signature: "",
  )
  signTx(identity, result)

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

proc waitUntil(
    predicate: proc(): bool {.closure, gcsafe.}, timeoutMs = 15_000, intervalMs = 100
): Future[void] {.async: (raises: [CancelledError, Exception]).} =
  let attempts = max(1, timeoutMs div intervalMs)
  for _ in 0 ..< attempts:
    if predicate():
      return
    await sleepAsync(intervalMs)
  raise newException(ValueError, "condition timed out")

proc connectPair(fromNode, toNode: RwadNode) {.async: (raises: []).} =
  fromNode.network.switch.peerStore.setAddresses(
    toNode.network.switch.peerInfo.peerId,
    toNode.network.switch.peerInfo.addrs,
  )
  for attempt in 0 ..< 20:
    try:
      await fromNode.network.switch.connect(
        toNode.network.switch.peerInfo.peerId,
        toNode.network.switch.peerInfo.addrs,
      )
      return
    except CancelledError:
      return
    except CatchableError:
      if attempt + 1 < 20:
        try:
          await sleepAsync(250)
        except CancelledError:
          return

proc connectAll(nodes: seq[RwadNode]) {.async: (raises: []).} =
  if nodes.len <= 1:
    return
  var futures: seq[Future[void]] = @[]
  for i in 0 ..< nodes.len:
    for j in i + 1 ..< nodes.len:
      futures.add(connectPair(nodes[i], nodes[j]))
      futures.add(connectPair(nodes[j], nodes[i]))
  if futures.len > 0:
    try:
      await allFutures(futures)
    except CancelledError:
      discard

proc stopAll(nodes: seq[RwadNode]) {.async.} =
  for idx in countdown(nodes.len - 1, 0):
    try:
      await nodes[idx].stop()
    except CatchableError:
      discard

proc makeNodeConfig(
    nodeDir: string,
    identity: NodeIdentity,
    genesis: GenesisSpec,
    localSuffix: LsmrPath,
    anchors: seq[LsmrAnchor],
): RwadNodeConfig =
  createDir(nodeDir)
  saveIdentity(nodeDir / "identity.json", identity)
  saveGenesis(nodeDir / "genesis.json", genesis)
  RwadNodeConfig(
    dataDir: nodeDir,
    listenAddrs: @["/ip4/127.0.0.1/tcp/0"],
    bootstrapAddrs: @[],
    legacyBootstrapAddrs: @[],
    lsmrBootstrapAddrs: @[],
    identityPath: nodeDir / "identity.json",
    genesisPath: nodeDir / "genesis.json",
    consensusTickMs: 0,
    routingMode: RoutingPlaneMode.lsmrOnly,
    primaryPlane: PrimaryRoutingPlane.lsmr,
    lsmrConfig: some(makeLsmrConfig(genesis.params.chainId, identity.account, localSuffix, anchors)),
  )

suite "RWAD network":
  test "three validators and one observer converge on the same finalized published content":
    let baseDir = getTempDir() / ("rwad-network-" & $getTime().toUnix())
    createDir(baseDir)
    let identities = @[newNodeIdentity(), newNodeIdentity(), newNodeIdentity(), newNodeIdentity()]
    let genesis = makeGenesis(identities)
    var nodes: seq[RwadNode] = @[]
    try:
      for idx, identity in identities:
        let nodeDir = baseDir / ("node-" & $idx)
        createDir(nodeDir)
        saveIdentity(nodeDir / "identity.json", identity)
        saveGenesis(nodeDir / "genesis.json", genesis)
        nodes.add(newRwadNode(RwadNodeConfig(
          dataDir: nodeDir,
          listenAddrs: @["/ip4/127.0.0.1/tcp/0"],
          bootstrapAddrs: @[],
          identityPath: nodeDir / "identity.json",
          genesisPath: nodeDir / "genesis.json",
          consensusTickMs: 0,
        )))

      for node in nodes:
        waitFor node.start()
      waitFor connectAll(nodes)
      waitFor sleepAsync(1000)

      check nodes[^1].identity.account notin nodes[^1].state.validators
      check nodes[0].state.activeValidators().len == ValidatorCount

      let contentId = "network-post-1"
      let submit = nodes[0].submitTx(signedPublish(identities[0], 1, contentId))
      check submit.accepted

      waitFor waitUntil(
        proc(): bool {.gcsafe.} =
          var ready = true
          {.cast(gcsafe).}:
            for node in nodes:
              if node.state.height < 1 or contentId notin node.state.contents:
                ready = false
                break
              if not node.feedSnapshot(10).anyIt(it.contentId == contentId):
                ready = false
                break
          ready
      )

      let expectedBlockId = nodes[0].state.lastBlockId
      check expectedBlockId.len > 0
      for node in nodes:
        check node.state.height >= 1
        check node.state.activeValidators().len == ValidatorCount
        check node.state.lastBlockId == expectedBlockId
        check node.contentDetail(contentId).contentId == contentId
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)

  test "lsmr only three validators and one observer converge without kad or rendezvous":
    let baseDir = getTempDir() / ("rwad-network-lsmr-" & $getTime().toUnix())
    createDir(baseDir)
    let identities = @[newNodeIdentity(), newNodeIdentity(), newNodeIdentity(), newNodeIdentity()]
    let genesis = makeGenesis(identities)
    let anchors = @[rootAnchor(identities[0], "root-a"), rootAnchor(identities[1], "root-b")]
    var nodes: seq[RwadNode] = @[]
    try:
      let suffixes: seq[LsmrPath] = @[@[1'u8], @[9'u8], @[3'u8], @[7'u8]]
      for idx, identity in identities:
        nodes.add(newRwadNode(makeNodeConfig(
          baseDir / ("node-" & $idx),
          identity,
          genesis,
          suffixes[idx],
          anchors,
        )))

      for node in nodes:
        waitFor node.start()
      waitFor connectAll(nodes)
      waitFor sleepAsync(1000)

      for node in nodes:
        check not node.network.isNil
        check node.network.kad.isNil
        check node.network.rendezvous.isNil
        check node.chainStatus().routingStatus.mode == RoutingPlaneMode.lsmrOnly
        check node.chainStatus().routingStatus.primary == PrimaryRoutingPlane.lsmr

      waitFor waitUntil(
        proc(): bool {.gcsafe.} =
          var ready = true
          {.cast(gcsafe).}:
            for node in nodes:
              if node.chainStatus().routingStatus.lsmrActiveCertificates == 0:
                ready = false
                break
          ready,
        timeoutMs = 30_000,
      )

      let contentId = "network-post-lsmr-1"
      let submit = nodes[0].submitTx(signedPublish(identities[0], 1, contentId))
      check submit.accepted

      waitFor waitUntil(
        proc(): bool {.gcsafe.} =
          var ready = true
          {.cast(gcsafe).}:
            for node in nodes:
              if node.state.height < 1 or contentId notin node.state.contents:
                ready = false
                break
              if not node.feedSnapshot(10).anyIt(it.contentId == contentId):
                ready = false
                break
          ready
      )

      let expectedBlockId = nodes[0].state.lastBlockId
      check expectedBlockId.len > 0
      for node in nodes:
        check node.state.height >= 1
        check node.state.activeValidators().len == ValidatorCount
        check node.state.lastBlockId == expectedBlockId
        check node.contentDetail(contentId).contentId == contentId
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)
