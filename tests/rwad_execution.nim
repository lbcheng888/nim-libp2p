import std/[json, options, tables, unittest]

import ../libp2p/rwad/types
import ../libp2p/rwad/identity
import ../libp2p/rwad/execution/state
import ../libp2p/rwad/execution/engine
import ../libp2p/rwad/execution/name

proc makeGenesis(identity: NodeIdentity): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "rwad-test"
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

suite "RWAD execution":
  test "content governance, credits, treasury minting, and names evolve correctly":
    let alice = newNodeIdentity()
    let bob = newNodeIdentity()
    let state = newChainState(makeGenesis(alice))

    let publish = signedTx(alice, txContentPublishIntent, 1, %*{
      "intent": {
        "contentId": "post-1",
        "author": alice.account,
        "title": "normal post",
        "body": "hello rwad",
        "kind": "file_post",
        "manifestCid": "bafy-post-1",
        "createdAt": 1,
        "accessPolicy": "public",
        "previewCid": "preview-1",
        "seqNo": 1
      },
      "sourceGraph": {
        "sourceUri": "file://snapshot",
        "parentContentIds": [],
        "relatedNames": [],
        "importedFrom": []
      }
    })
    discard state.applyTx(publish)
    check state.contents.hasKey("post-1")
    check state.contents["post-1"].distribution.visibleInFeed

    let manifest = signedTx(alice, txContentManifestCommit, 2, %*{
      "contentId": "post-1",
      "manifest": {
        "version": 1,
        "manifestCid": "bafy-post-1",
        "contentHash": "hash-1",
        "totalSize": 64,
        "chunkSize": 64,
        "mime": "text/plain",
        "createdAt": 2,
        "snapshotAt": 2,
        "previewCid": "preview-1",
        "thumbnailCid": "",
        "accessPolicy": "public",
        "originHolder": alice.account,
        "readOnly": true,
        "authorSignature": "sig",
        "chunks": [{"index": 0, "cid": "chunk-0", "chunkHash": "h0", "size": 64, "mime": "text/plain"}],
        "tickets": []
      }
    })
    discard state.applyTx(manifest)
    check state.contents["post-1"].manifest.contentHash == "hash-1"

    let blueReport = signedTx(alice, txContentReportSubmit, 3, %*{
      "contentId": "post-1",
      "channel": "blue",
      "category": "spam",
      "reason": "suspected spam",
      "evidenceHash": "ev-1",
      "depositLocked": 1
    })
    discard state.applyTx(blueReport)
    check not state.contents["post-1"].distribution.visibleInFeed
    check state.contents["post-1"].moderationCaseId.len > 0

    let review = signedTx(alice, txContentReviewDecide, 4, %*{
      "contentId": "post-1",
      "caseId": state.contents["post-1"].moderationCaseId,
      "classification": "C2",
      "decision": "PASS_TO_POOL",
      "tags": ["LOW_DISTRIBUTION"],
      "reason": "keep in detail view"
    })
    discard state.applyTx(review)
    check state.contents["post-1"].distribution.visibleInDetail
    check not state.contents["post-1"].distribution.visibleInFeed

    let tombstone = signedTx(alice, txContentTombstoneIssue, 5, %*{
      "contentId": "post-1",
      "reason": "emergency hide",
      "scope": "content_visibility",
      "reasonCode": "EMERGENCY_HIDE",
      "basisType": "platform_rule",
      "executionReceipt": "exec-1",
      "restoreAllowed": true
    })
    discard state.applyTx(tombstone)
    check not state.contents["post-1"].distribution.visibleInDetail
    check state.tombstones[state.contents["post-1"].tombstoneId].reasonCode == "EMERGENCY_HIDE"

    let restore = signedTx(alice, txContentTombstoneRestore, 6, %*{
      "contentId": "post-1",
      "restoreDecisionId": "restore-1"
    })
    discard state.applyTx(restore)
    check state.contents["post-1"].distribution.visibleInDetail

    let creditIssue = signedTx(alice, txCreditIssue, 7, %*{
      "positionId": "credit-1",
      "owner": alice.account,
      "sourceType": "content",
      "sourceId": "post-1",
      "amount": 120,
      "budgetEpoch": 1
    })
    discard state.applyTx(creditIssue)
    let creditConvert = signedTx(alice, txCreditConvert, 8, %*{
      "positionId": "credit-1",
      "amount": 40
    })
    let rewardBefore = state.rwadBalanceOf(state.params.rewardPoolAccount)
    let aliceBefore = state.rwadBalanceOf(alice.account)
    discard state.applyTx(creditConvert)
    check state.rwadBalanceOf(alice.account) == aliceBefore + 40
    check state.rwadBalanceOf(state.params.rewardPoolAccount) == rewardBefore - 40

    let mintIntent = signedTx(alice, txTreasuryMintIntentCreate, 9, %*{
      "mintIntentId": "mint-1",
      "userAccount": alice.account,
      "inAsset": "USDT",
      "inAmount": 300,
      "basketPolicy": "ETF_1_1_1",
      "ackIrreversible": true
    })
    discard state.applyTx(mintIntent)
    check state.mintIntents["mint-1"].status == misIntentCreated
    for idx, asset in ["BTC", "GOLD", "HKD_STABLE"]:
      let receipt = signedTx(alice, TxKind.txTreasuryExecutionReceiptSubmit, uint64(10 + idx), %*{
        "receiptId": "receipt-" & $idx,
        "mintIntentId": "mint-1",
        "legIndex": idx,
        "assetOut": asset,
        "amountOut": 100,
        "avgPxUsd": 100,
        "venue": "dex",
        "evidenceHash": "e-" & $idx
      })
      discard state.applyTx(receipt)
      if idx < 2:
        check state.mintIntents["mint-1"].status == misDexExecuting
    check state.mintIntents["mint-1"].status == misDexSettled

    let custody = signedTx(alice, txTreasuryCustodySnapshotSubmit, 13, %*{
      "snapshot": {
        "snapshotId": "custody-1",
        "epochId": 1,
        "proofHash": "proof-1",
        "totalCustodyUsd": 300,
        "totalLiabilitiesUsd": 0,
        "goldAllocatedUsd": 100,
        "goldPendingUsd": 0,
        "coveredIntentIds": ["mint-1"],
        "signatures": ["sig-a"],
        "createdAt": 13
      }
    })
    discard state.applyTx(custody)
    check state.mintIntents["mint-1"].status == misCustodyPending
    let reserve = signedTx(alice, txTreasuryReserveProofSubmit, 14, %*{
      "proof": {
        "proofId": "reserve-1",
        "epochId": 1,
        "reserveProofHash": "reserve-hash-1",
        "netNavDeltaUsd": 300,
        "coveredIntentIds": ["mint-1"],
        "custodySnapshotIds": ["custody-1"],
        "signatures": ["sig-a"],
        "submittedAt": 14
      }
    })
    discard state.applyTx(reserve)
    check state.mintIntents["mint-1"].status == misCustodyAttested
    let supplyBeforeMint = state.totalRwadSupply
    let finalize = signedTx(alice, txRwadMintFinalize, 15, %*{
      "mintIntentId": "mint-1",
      "reserveProofHash": "reserve-hash-1",
      "amount": 250
    })
    discard state.applyTx(finalize)
    check state.totalRwadSupply == supplyBeforeMint + 250
    check state.mintIntents["mint-1"].status == misMinted
    expect ValueError:
      discard state.applyTx(finalize)

    let registerName = signedTx(alice, txNameRegister, 16, %*{
      "name": "alice.rwad",
      "targetAccount": alice.account,
      "resolvedCid": "bafy-post-1",
      "targetContentId": "post-1"
    })
    discard state.applyTx(registerName)
    let transferName = signedTx(alice, txNameTransfer, 17, %*{
      "name": "alice.rwad",
      "newOwner": bob.account,
      "targetAccount": bob.account
    })
    discard state.applyTx(transferName)
    let renewName = signedTx(bob, txNameRenew, 1, %*{
      "name": "alice.rwad",
      "ttlSeconds": 1000
    })
    discard state.applyTx(renewName)
    let resolved = state.resolveNameSnapshot("alice.rwad", 20)
    check resolved.isSome()
    check resolved.get().owner == bob.account

  test "strict content, governance, treasury, and name invariants reject invalid input":
    let alice = newNodeIdentity()
    let state = newChainState(makeGenesis(alice))

    let publish = signedTx(alice, txContentPublishIntent, 1, %*{
      "intent": {
        "contentId": "post-2",
        "author": alice.account,
        "title": "governed post",
        "body": "body",
        "kind": "file_post",
        "manifestCid": "bafy-post-2",
        "createdAt": 1,
        "accessPolicy": "public",
        "previewCid": "preview-2",
        "seqNo": 1
      }
    })
    discard state.applyTx(publish)

    let manifest = signedTx(alice, txContentManifestCommit, 2, %*{
      "contentId": "post-2",
      "manifest": {
        "version": 1,
        "manifestCid": "bafy-post-2",
        "contentHash": "hash-2",
        "totalSize": 32,
        "chunkSize": 32,
        "mime": "text/plain",
        "createdAt": 2,
        "snapshotAt": 2,
        "previewCid": "preview-2",
        "thumbnailCid": "",
        "accessPolicy": "public",
        "originHolder": alice.account,
        "readOnly": true,
        "authorSignature": "sig",
        "chunks": [{"index": 0, "cid": "chunk-a", "chunkHash": "ha", "size": 32, "mime": "text/plain"}],
        "tickets": []
      }
    })
    discard state.applyTx(manifest)

    let mutatedManifest = signedTx(alice, txContentManifestCommit, state.nonceOf(alice.account) + 1, %*{
      "contentId": "post-2",
      "manifest": {
        "version": 1,
        "manifestCid": "bafy-post-2",
        "contentHash": "hash-2b",
        "totalSize": 32,
        "chunkSize": 32,
        "mime": "text/plain",
        "createdAt": 3,
        "snapshotAt": 3,
        "previewCid": "preview-2",
        "thumbnailCid": "",
        "accessPolicy": "public",
        "originHolder": alice.account,
        "readOnly": true,
        "authorSignature": "sig",
        "chunks": [{"index": 0, "cid": "chunk-a", "chunkHash": "ha", "size": 32, "mime": "text/plain"}],
        "tickets": []
      }
    })
    expect ValueError:
      discard state.applyTx(mutatedManifest)

    let badBlueReport = signedTx(alice, txContentReportSubmit, state.nonceOf(alice.account) + 1, %*{
      "contentId": "post-2",
      "channel": "blue",
      "category": "spam",
      "reason": "missing deposit",
      "evidenceHash": "ev-bad",
      "depositLocked": 0
    })
    expect ValueError:
      discard state.applyTx(badBlueReport)

    let blueReport = signedTx(alice, txContentReportSubmit, state.nonceOf(alice.account) + 1, %*{
      "contentId": "post-2",
      "channel": "blue",
      "category": "spam",
      "reason": "real report",
      "evidenceHash": "ev-ok",
      "depositLocked": 1
    })
    discard state.applyTx(blueReport)

    let badCommunityReview = signedTx(alice, txContentReviewDecide, state.nonceOf(alice.account) + 1, %*{
      "contentId": "post-2",
      "caseId": state.contents["post-2"].moderationCaseId,
      "classification": "C1",
      "decision": "BLOCK_AND_PRESERVE",
      "tags": ["REQUIRE_PRO_REVIEW"],
      "reason": "community overreach"
    })
    expect ValueError:
      discard state.applyTx(badCommunityReview)

    let tombstoneNoRestore = signedTx(alice, txContentTombstoneIssue, state.nonceOf(alice.account) + 1, %*{
      "contentId": "post-2",
      "reason": "no restore",
      "restoreAllowed": false
    })
    discard state.applyTx(tombstoneNoRestore)
    let restore = signedTx(alice, txContentTombstoneRestore, state.nonceOf(alice.account) + 1, %*{"contentId": "post-2"})
    expect ValueError:
      discard state.applyTx(restore)

    let badIntent = signedTx(alice, txTreasuryMintIntentCreate, state.nonceOf(alice.account) + 1, %*{
      "mintIntentId": "mint-bad",
      "userAccount": alice.account,
      "inAsset": "USDT",
      "inAmount": 100,
      "basketPolicy": "ETF_1_1_1",
      "ackIrreversible": false
    })
    expect ValueError:
      discard state.applyTx(badIntent)

    let mintIntent = signedTx(alice, txTreasuryMintIntentCreate, state.nonceOf(alice.account) + 1, %*{
      "mintIntentId": "mint-2",
      "userAccount": alice.account,
      "inAsset": "USDC",
      "inAmount": 300,
      "basketPolicy": "ETF_1_1_1",
      "basketWeights": ["3333", "3333", "3334"],
      "ackIrreversible": true
    })
    discard state.applyTx(mintIntent)

    let receipt0 = signedTx(alice, txTreasuryExecutionReceiptSubmit, state.nonceOf(alice.account) + 1, %*{
      "receiptId": "receipt-a",
      "mintIntentId": "mint-2",
      "legIndex": 0,
      "assetOut": "BTC",
      "amountOut": 100,
      "avgPxUsd": 100,
      "venue": "dex",
      "evidenceHash": "e-a"
    })
    discard state.applyTx(receipt0)
    let duplicateLeg = signedTx(alice, txTreasuryExecutionReceiptSubmit, state.nonceOf(alice.account) + 1, %*{
      "receiptId": "receipt-b",
      "mintIntentId": "mint-2",
      "legIndex": 0,
      "assetOut": "GOLD",
      "amountOut": 100,
      "avgPxUsd": 100,
      "venue": "dex",
      "evidenceHash": "e-b"
    })
    expect ValueError:
      discard state.applyTx(duplicateLeg)

    for item in [
      %*{"receiptId": "receipt-c", "mintIntentId": "mint-2", "legIndex": 1, "assetOut": "GOLD", "amountOut": 100, "avgPxUsd": 100, "venue": "dex", "evidenceHash": "e-c"},
      %*{"receiptId": "receipt-d", "mintIntentId": "mint-2", "legIndex": 2, "assetOut": "HKD_STABLE", "amountOut": 100, "avgPxUsd": 100, "venue": "dex", "evidenceHash": "e-d"}
    ]:
      discard state.applyTx(signedTx(alice, txTreasuryExecutionReceiptSubmit, state.nonceOf(alice.account) + 1, item))

    let badReserve = signedTx(alice, txTreasuryReserveProofSubmit, state.nonceOf(alice.account) + 1, %*{
      "proof": {
        "proofId": "reserve-bad",
        "epochId": 1,
        "reserveProofHash": "reserve-bad",
        "netNavDeltaUsd": 300,
        "coveredIntentIds": ["mint-2"],
        "custodySnapshotIds": ["missing-custody"],
        "signatures": ["sig-a"],
        "submittedAt": 20
      }
    })
    expect ValueError:
      discard state.applyTx(badReserve)

    let custody = signedTx(alice, txTreasuryCustodySnapshotSubmit, state.nonceOf(alice.account) + 1, %*{
      "snapshot": {
        "snapshotId": "custody-2",
        "epochId": 1,
        "proofHash": "proof-2",
        "totalCustodyUsd": 300,
        "totalLiabilitiesUsd": 0,
        "goldAllocatedUsd": 100,
        "goldPendingUsd": 0,
        "coveredIntentIds": ["mint-2"],
        "signatures": ["sig-a"],
        "createdAt": 21
      }
    })
    discard state.applyTx(custody)
    let reserve = signedTx(alice, txTreasuryReserveProofSubmit, state.nonceOf(alice.account) + 1, %*{
      "proof": {
        "proofId": "reserve-2",
        "epochId": 1,
        "reserveProofHash": "reserve-hash-2",
        "netNavDeltaUsd": 300,
        "coveredIntentIds": ["mint-2"],
        "custodySnapshotIds": ["custody-2"],
        "signatures": ["sig-a"],
        "submittedAt": 22
      }
    })
    discard state.applyTx(reserve)

    let badName = signedTx(alice, txNameRegister, state.nonceOf(alice.account) + 1, %*{
      "name": "Bad Name",
      "targetAccount": alice.account
    })
    expect ValueError:
      discard state.applyTx(badName)

    let validName = signedTx(alice, txNameRegister, state.nonceOf(alice.account) + 1, %*{
      "name": "valid-name.rwad"
    })
    discard state.applyTx(validName)
    let badTransfer = signedTx(alice, txNameTransfer, state.nonceOf(alice.account) + 1, %*{
      "name": "valid-name.rwad",
      "newOwner": ""
    })
    expect ValueError:
      discard state.applyTx(badTransfer)
