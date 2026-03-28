import std/[json, options, os, sequtils, times, unittest]

import chronos

import ../libp2p/lsmr
import ../libp2p/fabric
import ../libp2p/rwad/identity
import ../libp2p/rwad/types

proc makeGenesis(validators: seq[NodeIdentity], observer: NodeIdentity): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "fabric-network-lsmr"
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
    lsmrConfig: some(makeLsmrConfig(genesis.params.chainId, identity.account, localSuffix, anchors)),
  )

proc dumpNodeState(nodes: seq[FabricNode]) =
  for idx, node in nodes:
    let status = node.fabricStatus()
    let checkpoint = node.getCheckpoint()
    let projection = node.queryProjection("content")
    let contentCount =
      if projection.kind == JObject and projection.hasKey("contents") and projection["contents"].kind == JArray:
        projection["contents"].len
      else:
        -1
    echo "FABRIC_LSMR node=", idx,
      " account=", status.localAccount,
      " events=", status.eventCount,
      " certs=", status.certifiedEventCount,
      " checkpoints=", status.checkpointCount,
      " latestEra=", status.latestCheckpointEra,
      " activeCerts=", status.routingStatus.lsmrActiveCertificates,
      " checkpoint=", checkpoint.isSome(),
      " contentCount=", contentCount

suite "Fabric lsmr only network":
  test "real network converges without kad or rendezvous":
    let baseDir = getTempDir() / ("fabric-network-lsmr-only-" & $getTime().toUnix())
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

      waitFor sleepAsync(2000)

      when defined(fabric_diag):
        for idx, node in nodes:
          let status = node.fabricStatus()
          echo "routing node=", idx,
            " account=", status.localAccount,
            " activeCerts=", status.routingStatus.lsmrActiveCertificates

      var nonce0 = 0'u64
      let targetTxCount =
        when defined(fabric_diag):
          4
        else:
          61
      for idx in 0 ..< targetTxCount:
        inc nonce0
        let submit = n0.submitTx(signedTx(v0, nonce0, "lsmr-v0-" & $idx))
        when defined(fabric_diag):
          let event = n0.getEvent(submit.eventId)
          if event.isSome():
            echo "event#", idx,
              " witnessAccounts=", event.get().witnessSet,
              " witnessRoutes=", event.get().witnessRoutes,
              " participantRoutes=", event.get().participantRoutes
        check submit.accepted

      try:
        waitFor waitUntil(
          proc(): bool {.gcsafe.} =
            var ready = true
            {.cast(gcsafe).}:
              for node in nodes:
                if node.getCheckpoint().isNone():
                  ready = false
                  break
              if ready:
                let projection = nodes[^1].queryProjection("content")
                ready = projection.kind == JObject and
                  projection.hasKey("contents") and
                  projection["contents"].kind == JArray and
                  projection["contents"].len == targetTxCount
            ready,
          timeoutMs = 90_000,
        )
      except CatchableError:
        dumpNodeState(nodes)
        raise

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
      check observerProjection["contents"].len == targetTxCount
      let observerDetail = nodes[^1].contentDetail("net-post-lsmr-v0-0")
      check observerDetail.isSome()
      check observerDetail.get().title == "net-post-lsmr-v0-0"
      check nodes[^1].feedSnapshot(10).len == 10
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)
