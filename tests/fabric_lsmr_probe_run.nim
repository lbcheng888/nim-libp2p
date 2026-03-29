import std/[algorithm, json, options, os, strutils, tables, times]

import chronos

import ../libp2p/lsmr
import ../libp2p/fabric
import ../libp2p/multiaddress
import ../libp2p/peerstore
import ../libp2p/rwad/identity
import ../libp2p/rwad/types

proc makeGenesis(validators: seq[NodeIdentity], observer: NodeIdentity): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "fabric-network-lsmr-probe"
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
        "contentId": "probe-post-" & suffix,
        "author": identity.account,
        "title": "probe-post-" & suffix,
        "body": "probe body " & suffix,
        "kind": "text",
        "manifestCid": "probe-manifest-" & suffix,
        "createdAt": int(nonce),
        "accessPolicy": "public",
        "previewCid": "probe-preview-" & suffix,
        "seqNo": int(nonce)
      }
    },
    signature: "",
  )
  signTx(identity, result)

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

proc parseAddrs(values: seq[string]): seq[MultiAddress] =
  for value in values:
    let parsed = MultiAddress.init(value)
    if parsed.isOk():
      result.add(parsed.get())

proc rootAnchor(identity: NodeIdentity, operatorId: string, addrs: seq[string] = @[]): LsmrAnchor =
  LsmrAnchor(
    peerId: identity.peerId,
    addrs: parseAddrs(addrs),
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

proc dumpNodeState(nodes: seq[FabricNode]) {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    for idx, node in nodes:
      try:
        let status = node.fabricStatus()
        let checkpoint = node.getCheckpoint()
        let projection = node.queryProjection("content")
        let contentCount =
          if projection.kind == JObject and projection.hasKey("contents") and projection["contents"].kind == JArray:
            projection["contents"].len
          else:
            -1
        echo "NODE ", idx,
          " account=", status.localAccount,
          " peerId=", peerIdString(node.identity.peerId),
          " events=", status.eventCount,
          " certs=", status.certifiedEventCount,
          " checkpoints=", status.checkpointCount,
          " latestEra=", status.latestCheckpointEra,
          " activeCerts=", status.routingStatus.lsmrActiveCertificates,
          " checkpoint=", checkpoint.isSome(),
          " contentCount=", contentCount
        if not node.network.isNil and not node.network.switch.isNil:
          let peerStore = node.network.switch.peerStore
          if not peerStore.isNil:
            var active: seq[string] = @[]
            for peerId, record in peerStore[ActiveLsmrBook].book:
              active.add($peerId & "=" & $record.data.certifiedPrefix())
            active.sort()
            echo "ACTIVE ", idx, " ", active.join(" | ")
            let view = peerStore.topologyNeighborView(node.identity.peerId)
            if view.isSome():
              let item = view.get()
              var sameCell: seq[string] = @[]
              for peerId in item.sameCellPeers:
                sameCell.add($peerId)
              sameCell.sort()
              var parentPeers: seq[string] = @[]
              for peerId in item.parentPrefixPeers:
                parentPeers.add($peerId)
              parentPeers.sort()
              var buckets: seq[string] = @[]
              for bucket in item.directionalPeers:
                var peers: seq[string] = @[]
                for peerId in bucket.peers:
                  peers.add($peerId)
                peers.sort()
                buckets.add($bucket.directionDigit & ":" & peers.join(","))
              buckets.sort()
              echo "VIEW ", idx,
                " selfPrefix=", item.selfPrefix,
                " sameCell=", sameCell.join(","),
                " parents=", parentPeers.join(","),
                " dirs=", buckets.join(" | ")
      except CatchableError as exc:
        echo "NODE ", idx, " status-error=", exc.msg

proc monitorNodeState(nodes: seq[FabricNode], totalMs: int, intervalMs = 1000) {.async: (raises: [CancelledError]).} =
  let effectiveInterval = max(1, intervalMs)
  let rounds =
    if totalMs <= 0: 1
    else: max(1, (totalMs + effectiveInterval - 1) div effectiveInterval)
  for round in 0 ..< rounds:
    echo "TICK ", round, " ms=", round * effectiveInterval
    dumpNodeState(nodes)
    if round + 1 < rounds:
      await sleepAsync(effectiveInterval)

proc nodeActiveCertCount(node: FabricNode): int {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.fabricStatus().routingStatus.lsmrActiveCertificates
    except CatchableError:
      return -1

proc nodeEventCount(node: FabricNode): int {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.fabricStatus().eventCount
    except CatchableError:
      return -1

proc nodeContentCount(node: FabricNode): int {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      let projection = node.queryProjection("content")
      if projection.kind == JObject and
          projection.hasKey("contents") and
          projection["contents"].kind == JArray:
        return projection["contents"].len
    except CatchableError:
      discard
    return -1

proc awaitOverlayReady(
    nodes: seq[FabricNode], minActiveCerts: int, timeoutMs: int, intervalMs = 100
) {.async: (raises: [CancelledError]).} =
  let startedAtMs = int64(epochTime() * 1000)
  while true:
    var ready = true
    for node in nodes:
      if node.isNil:
        ready = false
        break
      if node.nodeActiveCertCount() < minActiveCerts:
        ready = false
        break
    if ready:
      echo "OVERLAY-READY activeCerts>=", minActiveCerts
      return
    let elapsedMs = int(int64(epochTime() * 1000) - startedAtMs)
    if timeoutMs >= 0 and elapsedMs >= timeoutMs:
      echo "OVERLAY-TIMEOUT activeCerts>=", minActiveCerts, " elapsedMs=", elapsedMs
      dumpNodeState(nodes)
      return
    await sleepAsync(intervalMs)

proc awaitEventConvergence(
    nodes: seq[FabricNode], expectedEventCount: int, timeoutMs: int, intervalMs = 250
) {.async: (raises: [CancelledError]).} =
  let startedAtMs = int64(epochTime() * 1000)
  var nextDumpAtMs = startedAtMs
  while true:
    var ready = nodes.len > 0
    for idx, node in nodes:
      if node.isNil or node.nodeEventCount() < expectedEventCount:
        ready = false
        break
      if idx == nodes.high and node.nodeContentCount() < expectedEventCount:
        ready = false
        break
    if ready:
      echo "EVENTS-READY count>=", expectedEventCount
      return
    let nowMs = int64(epochTime() * 1000)
    if nowMs >= nextDumpAtMs:
      echo "POST-SUBMIT"
      dumpNodeState(nodes)
      nextDumpAtMs = nowMs + 1000
    let elapsedMs = int(nowMs - startedAtMs)
    if timeoutMs >= 0 and elapsedMs >= timeoutMs:
      echo "EVENTS-TIMEOUT count>=", expectedEventCount, " elapsedMs=", elapsedMs
      dumpNodeState(nodes)
      return
    await sleepAsync(intervalMs)

proc main() =
  let txCount =
    if paramCount() >= 1: parseInt(paramStr(1))
    else: 12
  let settleMs =
    if paramCount() >= 2: parseInt(paramStr(2))
    else: 15000
  let baseDir = getTempDir() / ("fabric-network-lsmr-probe-" & $getTime().toUnix())
  createDir(baseDir)
  let v0 = newNodeIdentity()
  let v1 = newNodeIdentity()
  let v2 = newNodeIdentity()
  let observerId = newNodeIdentity()
  let genesis = makeGenesis(@[v0, v1, v2], observerId)
  var nodes: seq[FabricNode] = @[]
  let n0 = newFabricNode(makeNodeConfig(
    baseDir / "node-0",
    v0,
    genesis,
    @[1'u8],
    @[],
    @[rootAnchor(v0, "root-a"), rootAnchor(v1, "root-b")],
  ))
  nodes.add(n0)
  waitFor n0.start()
  echo "START 0 peer=", peerIdString(n0.identity.peerId), " addrs=", n0.rawAddrs().join(",")
  let anchor0Addrs = n0.rawAddrs()

  let n1 = newFabricNode(makeNodeConfig(
    baseDir / "node-1",
    v1,
    genesis,
    @[9'u8],
    anchor0Addrs,
    @[rootAnchor(v0, "root-a", anchor0Addrs), rootAnchor(v1, "root-b")],
  ))
  nodes.add(n1)
  waitFor n1.start()
  echo "START 1 peer=", peerIdString(n1.identity.peerId), " addrs=", n1.rawAddrs().join(",")
  let anchor1Addrs = n1.rawAddrs()

  let anchors = @[
    rootAnchor(v0, "root-a", anchor0Addrs),
    rootAnchor(v1, "root-b", anchor1Addrs),
  ]
  let bootstrap = anchor0Addrs & anchor1Addrs
  let n2 = newFabricNode(makeNodeConfig(baseDir / "node-2", v2, genesis, @[3'u8], bootstrap, anchors))
  nodes.add(n2)
  waitFor n2.start()
  echo "START 2 peer=", peerIdString(n2.identity.peerId), " addrs=", n2.rawAddrs().join(",")

  let n3 = newFabricNode(makeNodeConfig(baseDir / "node-3", observerId, genesis, @[7'u8], bootstrap, anchors))
  nodes.add(n3)
  waitFor n3.start()
  echo "START 3 peer=", peerIdString(n3.identity.peerId), " addrs=", n3.rawAddrs().join(",")

  echo "BASE ", baseDir
  waitFor awaitOverlayReady(nodes, nodes.len, 5000)
  echo "PRE-SUBMIT"
  dumpNodeState(nodes)

  var nonce0 = 0'u64
  for idx in 0 ..< txCount:
    inc nonce0
    let submit = n0.submitTx(signedTx(v0, nonce0, "v0-" & $idx))
    echo "SUBMIT ", idx, " accepted=", submit.accepted, " certified=", submit.certified, " event=", submit.eventId

  waitFor awaitEventConvergence(nodes, txCount, settleMs)
  dumpNodeState(nodes)
  waitFor stopAll(nodes)

main()
