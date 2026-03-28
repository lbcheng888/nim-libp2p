import std/[json, options, unittest]

import ../libp2p/fabric
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

proc signedTx(identity: NodeIdentity, kind: TxKind, nonce: uint64, payload: JsonNode): Tx =
  result = Tx(
    txId: "",
    kind: kind,
    sender: "",
    senderPublicKey: "",
    nonce: nonce,
    epoch: 0,
    timestamp: nonce.int64,
    payload: payload,
    signature: "",
  )
  signTx(identity, result)

suite "Fabric execution":
  test "all tx kinds map to system contracts and era checkpoints roll deterministically":
    let alice = newNodeIdentity()
    let observer = newNodeIdentity()
    let genesis = makeGenesis(@[alice])
    let node = newFabricNode(alice, genesis, @[5'u8])
    let observerNode = newFabricNode(observer, genesis, @[1'u8])
    connectPeer(node, observerNode)

    for kind in TxKind:
      let item = getContractRoot(kind)
      check item.contractRoot.len > 0
      check item.entrypoint.len > 0

    for idx in 0 ..< 61:
      let tx = signedTx(alice, txContentPublishIntent, uint64(idx + 1), %*{
        "intent": {
          "contentId": "post-" & $idx,
          "author": alice.account,
          "title": "post-" & $idx,
          "body": "fabric body " & $idx,
          "kind": "text",
          "manifestCid": "manifest-" & $idx,
          "createdAt": idx + 1,
          "accessPolicy": "public",
          "previewCid": "preview-" & $idx,
          "seqNo": idx + 1
        }
      })
      let outcome = node.submitTx(tx)
      check outcome.accepted
      check outcome.certified

    let checkpoint = observerNode.getCheckpoint()
    check checkpoint.isSome()
    check checkpoint.get().candidate.era == 0

    let projection = observerNode.queryProjection("content")
    check projection.kind == JObject
    check projection["contents"].kind == JArray
    check projection["contents"].len == 60

    let detail = observerNode.contentDetail("post-0")
    check detail.isSome()
    check detail.get().title == "post-0"
