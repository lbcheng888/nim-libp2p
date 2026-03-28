import std/[json, options, os, sequtils, tables, times]

import chronos

import ../libp2p/switch
import ../libp2p/rwad

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

const ValidatorCount = 3

proc makeGenesis(identities: seq[NodeIdentity], validatorCount = ValidatorCount): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "rwad-vote-probe"
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
        "title": "vote probe",
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

proc shortAddr(value: string): string =
  if value.len <= 8:
    value
  else:
    value[0 .. 7]

proc main() =
  let baseDir = getTempDir() / ("rwad-vote-probe-" & $getTime().toUnix())
  createDir(baseDir)
  let identities = @[newNodeIdentity(), newNodeIdentity(), newNodeIdentity(), newNodeIdentity()]
  let genesis = makeGenesis(identities)
  let validatorIdentities = identities[0 ..< min(ValidatorCount, identities.len)]
  var nodes: seq[RwadNode] = @[]
  var peerIds = initTable[string, string]()
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
      peerIds[identity.account] = peerIdString(identity.peerId)

    for node in nodes:
      waitFor node.start()
    waitFor connectAll(nodes)
    waitFor sleepAsync(1000)

    discard nodes[0].submitTx(signedPublish(identities[0], 1, "vote-probe-1"))

    var ready = false
    for _ in 0 ..< 100:
      ready = true
      for node in nodes:
        let round1 = node.consensus.batches.values.toSeq().filterIt(it.round == 1)
        if round1.len < validatorIdentities.len:
          ready = false
          break
      if ready:
        break
      waitFor sleepAsync(100)
    if not ready:
      raise newException(ValueError, "round 1 batches did not converge")

    waitFor sleepAsync(3000)

    let proposerNode = nodes[0]
    let round1 = proposerNode.consensus.batches.values.toSeq().filterIt(it.round == 1)
    var batchesByProposer = initTable[string, Batch]()
    for batch in round1:
      batchesByProposer[batch.proposer] = batch

    for proposer in validatorIdentities.mapIt(it.account):
      let batch = batchesByProposer[proposer]
      echo "BATCH proposer=", shortAddr(proposer), " batch=", batch.batchId[0 .. 7], " txs=", batch.transactions.len
      var proposerIdx = -1
      for idx, identity in identities:
        if identity.account == proposer:
          proposerIdx = idx
          break
      doAssert proposerIdx >= 0
      let proposerNodeRef = nodes[proposerIdx]
      for voter in validatorIdentities.mapIt(it.account):
        let local = proposerNodeRef.store.loadVote(batch.batchId, voter).isSome()
        let fetched = waitFor proposerNodeRef.network.fetchRaw(
          peerIds[voter],
          "vote:" & batch.batchId & ":" & voter,
          timeout = chronos.seconds(1),
          maxAttempts = 1,
        )
        echo "  voter=", shortAddr(voter), " local=", local, " remote=", fetched.isSome()
  finally:
    if nodes.len > 0:
      waitFor stopAll(nodes)

main()
