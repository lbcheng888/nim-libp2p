import std/[json, options, unittest]

import ../libp2p/fabric
import ../libp2p/rwad/identity
import ../libp2p/rwad/types

proc makeGenesis(validators: seq[NodeIdentity], chainId = "fabric-avo"): GenesisSpec =
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

proc signedTx(identity: NodeIdentity, nonce: uint64, idx: int): Tx =
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
        "contentId": "avo-post-" & $idx,
        "author": identity.account,
        "title": "avo-post-" & $idx,
        "body": "avo body " & $idx,
        "kind": "text",
        "manifestCid": "avo-manifest-" & $idx,
        "createdAt": idx + 1,
        "accessPolicy": "public",
        "previewCid": "avo-preview-" & $idx,
        "seqNo": idx + 1
      }
    },
    signature: "",
  )
  signTx(identity, result)

suite "Fabric AVO":
  test "proposals activate on the checkpoint after adoption":
    let alice = newNodeIdentity()
    let observer = newNodeIdentity()
    let genesis = makeGenesis(@[alice])
    let node = newFabricNode(alice, genesis, @[5'u8])
    let observerNode = newFabricNode(observer, genesis, @[1'u8])
    connectPeer(node, observerNode)

    let proposal = node.submitAvoProposal(
      ContentContractRoot,
      "content_publish_intent",
      "local_tps_pressure > 90%",
      "parallel signature verification",
      "proof-bundle-1",
      checkpointStartEra = 0,
      checkpointEndEra = 0,
    )
    check node.approveAvoProposal(proposal.proposalId)

    for idx in 0 ..< 121:
      let outcome = node.submitTx(signedTx(alice, uint64(idx + 1), idx))
      check outcome.accepted
      check outcome.certified

    let checkpoint = observerNode.getCheckpoint()
    check checkpoint.isSome()
    check checkpoint.get().candidate.era == 1
    check proposal.proposalId in checkpoint.get().candidate.activeAvoSet
