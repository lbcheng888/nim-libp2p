import std/[json, options, os, sequtils, times, unittest]

import chronos

import ../libp2p/fabric
import ../libp2p/peerinfo
import ../libp2p/rwad/identity
import ../libp2p/rwad/types

proc makeGenesis(validators: seq[NodeIdentity], chainId = "fabric-test"): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = chainId
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

proc publishPayload(identity: NodeIdentity, nonce: uint64): JsonNode =
  %*{
    "intent": {
      "contentId": "post-" & $nonce,
      "author": identity.account,
      "title": "post-" & $nonce,
      "body": "fabric body " & $nonce,
      "kind": "text",
      "manifestCid": "manifest-" & $nonce,
      "createdAt": int(nonce),
      "accessPolicy": "public",
      "previewCid": "preview-" & $nonce,
      "seqNo": int(nonce)
    }
  }

proc makeConfig(
    root: string,
    name: string,
    identity: NodeIdentity,
    genesis: GenesisSpec,
    path: seq[uint8],
    bootstrapAddrs: seq[string] = @[],
): FabricNodeConfig =
  let dir = root / name
  createDir(dir)
  let identityPath = dir / "identity.json"
  let genesisPath = dir / "genesis.json"
  saveIdentity(identityPath, identity)
  saveGenesis(genesisPath, genesis)
  FabricNodeConfig(
    dataDir: dir,
    identityPath: identityPath,
    genesisPath: genesisPath,
    listenAddrs: @["/ip4/127.0.0.1/tcp/0"],
    bootstrapAddrs: bootstrapAddrs,
    lsmrPath: path,
  )

proc waitUntil(
    predicate: proc(): bool {.closure, gcsafe.}, timeoutMs = 30_000, intervalMs = 100
): Future[void] {.async: (raises: [CancelledError, Exception]).} =
  let attempts = max(1, timeoutMs div intervalMs)
  for _ in 0 ..< attempts:
    if predicate():
      return
    await sleepAsync(intervalMs)
  raise newException(ValueError, "condition timed out")

proc assertQuiesced(node: FabricNode, expectedCommittedNonce: uint64) =
  let snapshot = node.submitFence()
  check snapshot.pendingInbound == 0
  check snapshot.localOfferedNonce == expectedCommittedNonce
  check snapshot.localCommittedNonce == expectedCommittedNonce
  check snapshot.pendingEvents == 0
  check snapshot.pendingAttestations == 0
  check snapshot.pendingEventCertificates == 0
  check snapshot.pendingCertificationEvents == 0
  check snapshot.pendingWitnessPulls == 0
  check snapshot.pendingIndexFlushes == 0
  check snapshot.eventOutstanding == 0
  check snapshot.attOutstanding == 0
  check snapshot.certOutstanding == 0

proc submitCommittedLocalTx(
    node: FabricNode,
    kind: TxKind,
    payload: JsonNode,
    nonce: uint64,
    timestamp: int64,
): SubmitEventResult =
  result = node.submitLocalTx(kind, payload, timestamp = timestamp)
  check result.accepted
  waitFor waitUntil(
    proc(): bool {.gcsafe.} =
      {.cast(gcsafe).}:
        node.fabricStatus().localCommittedNonce >= nonce
    ,
    timeoutMs = 30_000,
  )

suite "Fabric execution":
  test "all tx kinds map to system contracts and era checkpoints roll deterministically":
    let alice = newNodeIdentity()
    let observer = newNodeIdentity()
    let genesis = makeGenesis(@[alice])
    let root = getTempDir() / ("fabric-execution-" & $epochTime().int64)
    createDir(root)

    var nodes: seq[FabricNode] = @[]
    defer:
      for node in nodes:
        try:
          waitFor node.stop()
        except CatchableError:
          discard
      if dirExists(root):
        removeDir(root)

    let node = newFabricNode(makeConfig(root, "author", alice, genesis, @[5'u8]))
    nodes.add(node)
    waitFor node.start()

    let bootstrap = node.network.switch.peerInfo.fullAddrs().tryGet().mapIt($it)
    let observerNode = newFabricNode(makeConfig(root, "observer", observer, genesis, @[1'u8], bootstrap))
    nodes.add(observerNode)
    waitFor observerNode.start()
    waitFor sleepAsync(2000)

    for kind in TxKind:
      let item = getContractRoot(kind)
      check item.contractRoot.len > 0
      check item.entrypoint.len > 0

    for idx in 0 ..< 61:
      let nonce = uint64(idx + 1)
      discard node.submitCommittedLocalTx(
        txContentPublishIntent,
        publishPayload(alice, nonce),
        nonce,
        timestamp = int64(nonce),
      )

    waitFor waitUntil(
      proc(): bool {.gcsafe.} =
        {.cast(gcsafe).}:
          let checkpoint = observerNode.getCheckpoint()
          if checkpoint.isNone():
            return false
          let projection = observerNode.queryProjection("content")
          projection.kind == JObject and
            projection.hasKey("contents") and
            projection["contents"].kind == JArray and
            projection["contents"].len == 60
      ,
      timeoutMs = 90_000,
    )

    node.assertQuiesced(61'u64)
    observerNode.assertQuiesced(0'u64)

    let checkpoint = observerNode.getCheckpoint()
    check checkpoint.isSome()
    check checkpoint.get().candidate.era == 0

    let projection = observerNode.queryProjection("content")
    check projection.kind == JObject
    check projection["contents"].kind == JArray
    check projection["contents"].len == 60

    let detail = observerNode.contentDetail("post-1")
    check detail.isSome()
    check detail.get().title == "post-1"
