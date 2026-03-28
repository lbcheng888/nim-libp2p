import std/[json, options, tables, unittest]

import ../libp2p/rwad/types
import ../libp2p/rwad/identity
import ../libp2p/rwad/consensus/engine
import ../libp2p/rwad/execution/state

proc makeGenesis(identity: NodeIdentity): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "rwad-consensus"
  GenesisSpec(
    params: params,
    validators: @[
      ValidatorInfo(
        address: identity.account,
        peerId: peerIdString(identity.peerId),
        publicKey: encodePublicKeyHex(identity.publicKey),
        stake: 10_000,
        delegatedStake: 0,
        active: true,
        jailed: false,
        slashedAmount: 0,
        lastUpdatedAt: 0,
      )
    ],
    balances: @[
      KeyValueU64(key: params.rewardPoolAccount, value: 500_000),
      KeyValueU64(key: identity.account, value: 500_000),
    ],
    names: @[],
    contents: @[],
    mintedSupply: 1_000_000,
  )

proc makeGenesis(identities: seq[NodeIdentity]): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "rwad-consensus-multi"
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

proc signedTx(identity: NodeIdentity, nonce: uint64): Tx =
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
        "contentId": "post-" & $nonce,
        "author": identity.account,
        "title": "post " & $nonce,
        "body": "hello",
        "kind": "text",
        "manifestCid": "manifest-" & $nonce,
        "createdAt": nonce,
        "accessPolicy": "public",
        "previewCid": "",
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

suite "RWAD consensus":
  test "tx signing is deterministic and single-validator rounds finalize":
    let identity = newNodeIdentity()
    let txA = signedTx(identity, 1)
    let txB = signedTx(identity, 1)
    check txA.txId == txB.txId
    check txA.signature == txB.signature

    let state = newChainState(makeGenesis(identity))
    let engine = newConsensusEngine(state, identity)
    engine.addTx(txA)
    let round1 = engine.produceRound()
    check round1.certificate.isSome()
    check engine.verifyCertificate(round1.certificate.get())
    let round2 = engine.produceRound()
    check round2.finalized.len == 1
    check state.height == 1
    check state.contents.hasKey("post-1")
    let blocks = engine.latestBlocks(10)
    check blocks.len == 1
    check blocks[0].transactions.len == 1
    check blocks[0].orderedBatchIds.len >= 1

  test "observed higher-round batches do not skip the local proposal round":
    let identities = @[newNodeIdentity(), newNodeIdentity()]
    let genesis = makeGenesis(identities)
    let engine0 = newConsensusEngine(newChainState(genesis), identities[0])
    let engine1 = newConsensusEngine(newChainState(genesis), identities[1])

    let tx = signedTx(identities[0], 1)
    engine0.addTx(tx)
    let batch1 = engine0.buildBatch()
    discard engine1.ingestBatch(batch1)

    let vote0 = engine0.localVote(batch1)
    let vote1 = engine1.localVote(batch1)
    discard engine0.ingestVote(vote0)
    let cert1 = engine0.ingestVote(vote1)
    check cert1.isSome()
    discard engine1.ingestCertificate(cert1.get())

    discard engine0.ingestCertificate(cert1.get())
    let remoteRound2 = engine0.buildBatch()
    check remoteRound2.round == 2
    discard engine1.ingestBatch(remoteRound2)

    let localRound2 = engine1.buildBatch()
    check localRound2.round == 2
    check localRound2.parents == @[batch1.batchId]
