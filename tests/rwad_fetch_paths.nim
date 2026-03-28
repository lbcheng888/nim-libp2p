import std/[base64, json, options, os, sequtils, tables, unittest]

import chronos

import ../libp2p/switch
import ../libp2p/rwad

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc makeGenesis(identities: seq[NodeIdentity]): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "rwad-fetch-paths"
  var validators: seq[ValidatorInfo] = @[]
  var balances = @[KeyValueU64(key: params.rewardPoolAccount, value: 500_000)]
  for identity in identities:
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
        "title": "fetch probe",
        "body": "hello rwad",
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

proc waitUntil(
    predicate: proc(): bool {.closure, gcsafe.}, timeoutMs = 5_000, intervalMs = 100
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

proc stopAll(nodes: seq[RwadNode]) {.async.} =
  for idx in countdown(nodes.len - 1, 0):
    try:
      await nodes[idx].stop()
    except CatchableError:
      discard

suite "RWAD fetch paths":
  test "batchbyround and submitbatch move a batch across nodes":
    let baseDir = getTempDir() / "rwad-fetch-paths"
    if dirExists(baseDir):
      removeDir(baseDir)
    createDir(baseDir)
    let identities = @[newNodeIdentity(), newNodeIdentity()]
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

      let submit = nodes[0].submitTx(signedPublish(identities[0], 1, "fetch-probe-1"))
      check submit.accepted

      let batch = nodes[0].consensus.batches.values.toSeq().filterIt(
        it.proposer == identities[0].account and it.round == 1
      )[0]
      check nodes[1].getBatch(batch.batchId).isNone()

      waitFor connectPair(nodes[0], nodes[1])
      waitFor connectPair(nodes[1], nodes[0])
      waitFor sleepAsync(1000)

      let byRound = waitFor nodes[1].network.fetchRaw(
        peerIdString(identities[0].peerId),
        "batchbyround:" & identities[0].account & ":1",
        timeout = 1.seconds,
        maxAttempts = 1,
      )
      check byRound.isSome()
      if byRound.isSome():
        let fetched = decodeObj[Batch](stringOf(byRound.get()))
        check fetched.batchId == batch.batchId

      let submitAck = waitFor nodes[0].network.fetchRaw(
        peerIdString(identities[1].peerId),
        "submitbatch:" & base64.encode(encodeObj(batch)),
        timeout = 1.seconds,
        maxAttempts = 1,
      )
      check submitAck.isSome()

      waitFor waitUntil(
        proc(): bool {.gcsafe.} =
          var present = false
          {.cast(gcsafe).}:
            present = nodes[1].getBatch(batch.batchId).isSome() and
              nodes[1].store.loadVote(batch.batchId, identities[1].account).isSome()
          present
      )
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)

  test "submitvote delivers a validator vote to the proposer":
    let baseDir = getTempDir() / "rwad-submit-vote"
    if dirExists(baseDir):
      removeDir(baseDir)
    createDir(baseDir)
    let identities = @[newNodeIdentity(), newNodeIdentity()]
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

      waitFor connectPair(nodes[0], nodes[1])
      waitFor connectPair(nodes[1], nodes[0])
      waitFor sleepAsync(1000)

      let submit = nodes[0].submitTx(signedPublish(identities[0], 1, "submit-vote-probe-1"))
      check submit.accepted

      let batch = nodes[0].consensus.batches.values.toSeq().filterIt(
        it.proposer == identities[0].account and it.round == 1
      )[0]
      check nodes[0].store.loadVote(batch.batchId, identities[1].account).isNone()

      let vote = nodes[1].consensus.localVote(batch)
      let ack = waitFor nodes[1].network.submitVote(
        peerIdString(identities[0].peerId),
        vote,
        timeout = 1.seconds,
      )
      check ack

      waitFor waitUntil(
        proc(): bool {.gcsafe.} =
          var present = false
          {.cast(gcsafe).}:
            present = nodes[0].store.loadVote(batch.batchId, identities[1].account).isSome()
          present
      )
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)

  test "submitvote does not ack when receiver cannot resolve the batch":
    let baseDir = getTempDir() / "rwad-submit-vote-missing-batch"
    if dirExists(baseDir):
      removeDir(baseDir)
    createDir(baseDir)
    let identities = @[newNodeIdentity(), newNodeIdentity()]
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

      waitFor connectPair(nodes[1], nodes[0])
      waitFor sleepAsync(500)

      let missingVote = BatchVote(
        batchId: "missing-batch",
        proposer: "ghost-proposer",
        epoch: 0,
        round: 1,
        batchHash: "missing-batch",
        vote: CertificateVote(
          voter: identities[1].account,
          signature: "deadbeef",
          weight: 1,
        ),
      )

      let ack = waitFor nodes[1].network.submitVote(
        peerIdString(identities[0].peerId),
        missingVote,
        timeout = 1.seconds,
      )
      check not ack
      check nodes[0].store.loadVote("missing-batch", identities[1].account).isNone()
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)
