import std/[json, options, os, sequtils, tables, times, unittest]

import chronos

import ../libp2p/fabric
import ../libp2p/lsmr
import ../libp2p/peerinfo
import ../libp2p/rwad/identity
import ../libp2p/rwad/types

proc makeGenesis(validators: seq[NodeIdentity], observer: NodeIdentity): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "fabric-network-live"
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
        "contentId": "live-post-" & suffix,
        "author": identity.account,
        "title": "live-post-" & suffix,
        "body": "live body " & suffix,
        "kind": "text",
        "manifestCid": "live-manifest-" & suffix,
        "createdAt": int(nonce),
        "accessPolicy": "public",
        "previewCid": "live-preview-" & suffix,
        "seqNo": int(nonce)
      }
    },
    signature: "",
  )
  signTx(identity, result)

proc makeConfig(
    root: string,
    name: string,
    identity: NodeIdentity,
    genesis: GenesisSpec,
    path: LsmrPath,
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

proc pump(nodes: seq[FabricNode], rounds = 1) =
  for _ in 0 ..< rounds:
    waitFor sleepAsync(200)
    for idx, node in nodes:
      when defined(fabric_diag):
        echo "pump node#", idx, " inbound=", len(node.inbound)
      discard node.fabricStatus()
      when defined(fabric_diag):
        echo "pump done#", idx

proc waitUntilCertified(nodes: seq[FabricNode], author: FabricNode, eventId: string, rounds = 40): bool =
  for _ in 0 ..< rounds:
    pump(nodes, 1)
    if author.getEventCertificate(eventId).isSome():
      return true
  when defined(fabric_diag):
    echo "certify failed event=", eventId
    for idx, node in nodes:
      let eventKnown = node.getEvent(eventId).isSome()
      let certKnown = node.getEventCertificate(eventId).isSome()
      let attCount = len(node.eventAttestations.getOrDefault(eventId, @[]))
      echo " node#", idx,
        " event=", eventKnown,
        " cert=", certKnown,
        " att=", attCount
  false

proc waitUntilCheckpoint(nodes: seq[FabricNode], observer: FabricNode, rounds = 60): Option[CheckpointCertificate] =
  for _ in 0 ..< rounds:
    pump(nodes, 1)
    let checkpoint = observer.getCheckpoint()
    if checkpoint.isSome():
      return checkpoint
  none(CheckpointCertificate)

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

suite "Fabric live network":
  test "observer catches up over real libp2p fabric network":
    let v0 = newNodeIdentity()
    let v1 = newNodeIdentity()
    let v2 = newNodeIdentity()
    let observerId = newNodeIdentity()
    let genesis = makeGenesis(@[v0, v1, v2], observerId)
    let root = getTempDir() / ("fabric-live-" & $epochTime().int64)
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

    let c0 = makeConfig(root, "n0", v0, genesis, @[5'u8])
    let n0 = newFabricNode(c0)
    nodes.add(n0)
    waitFor n0.start()

    let bootstrap = n0.network.switch.peerInfo.fullAddrs().tryGet().mapIt($it)

    let n1 = newFabricNode(makeConfig(root, "n1", v1, genesis, @[1'u8], bootstrap))
    let n2 = newFabricNode(makeConfig(root, "n2", v2, genesis, @[9'u8], bootstrap))
    let n3 = newFabricNode(makeConfig(root, "n3", observerId, genesis, @[3'u8], bootstrap))
    nodes.add(n1)
    nodes.add(n2)
    nodes.add(n3)

    waitFor n1.start()
    waitFor n2.start()
    waitFor n3.start()

    pump(nodes, 15)
    when defined(fabric_diag):
      echo "routing peers:",
        " n0=", len(n0.routingPeers),
        " n1=", len(n1.routingPeers),
        " n2=", len(n2.routingPeers),
        " n3=", len(n3.routingPeers)

    let targetTxCount =
      when defined(fabric_diag):
        4
      else:
        61

    var nonce = 0'u64
    for idx in 0 ..< targetTxCount:
      inc nonce
      let outcome = n0.submitTx(signedTx(v0, nonce, $idx))
      when defined(fabric_diag):
        let event = n0.getEvent(outcome.eventId)
        if event.isSome():
          echo "event#", idx,
            " witnesses=", event.get().witnessSet,
            " participants=", event.get().participants,
            " n0Certs=", n0.eventCertificates.len
      check outcome.accepted
      let certified = waitUntilCertified(nodes, n0, outcome.eventId)
      when defined(fabric_diag):
        echo "certified#", idx, " ok=", certified, " n0CertsAfter=", n0.eventCertificates.len
      check certified

    when not defined(fabric_diag):
      n0.assertQuiesced(nonce)
      n1.assertQuiesced(0'u64)
      n2.assertQuiesced(0'u64)
      n3.assertQuiesced(0'u64)

      let checkpoint = waitUntilCheckpoint(nodes, n3)
      check checkpoint.isSome()
      check checkpoint.get().candidate.era == 0

      let projection = n3.queryProjection("content")
      check projection.kind == JObject
      check projection["contents"].kind == JArray
      check projection["contents"].len == 60

      let detail = n3.contentDetail("live-post-0")
      check detail.isSome()
      check detail.get().title == "live-post-0"
