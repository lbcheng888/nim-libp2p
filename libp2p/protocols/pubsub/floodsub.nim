# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[sets, hashes, tables]
import chronos, chronicles, metrics
import
  ./pubsub,
  ./pubsubpeer,
  ./timedcache,
  ./peertable,
  ./rpc/[message, messages, protobuf],
  ../../crypto/crypto,
  ../../stream/connection,
  ../../peerid,
  ../../peerinfo,
  ../../utility

## Simple flood-based publishing.

logScope:
  topics = "libp2p floodsub"

const FloodSubCodec* = "/floodsub/1.0.0"

type FloodSub* {.public.} = ref object of PubSub
  floodsub*: PeerTable # topic to remote peer map
  seen*: TimedCache[SaltedId]
    # Early filter for messages recently observed on the network
    # We use a salted id because the messages in this cache have not yet
    # been validated meaning that an attacker has greater control over the
    # hash key and therefore could poison the table
  seenSaltSeed*: array[32, byte]
    # Keep raw seed bytes instead of a copied SHA256 context. Copying nimcrypto
    # hash state across publish paths has been unstable on Android arm64.

proc salt*(f: FloodSub, msgId: MessageId): SaltedId =
  func rotl64(value: uint64, shift: int): uint64 =
    let normalized = shift mod 64
    if normalized == 0:
      value
    else:
      (value shl normalized) or (value shr (64 - normalized))

  proc mixLane(state: var uint64, b: byte, rotateBy: int) =
    state = state xor (uint64(b) + 1'u64)
    state = rotl64(state * 0x100000001b3'u64 + 0x9e3779b97f4a7c15'u64, rotateBy)

  var lanes = [
    0x243f6a8885a308d3'u64,
    0x13198a2e03707344'u64,
    0xa4093822299f31d0'u64,
    0x082efa98ec4e6c89'u64,
  ]
  const rotates = [13, 27, 39, 57]
  for index, b in f.seenSaltSeed:
    mixLane(lanes[index mod lanes.len], b, rotates[index mod rotates.len])
  for index, b in msgId:
    mixLane(lanes[index mod lanes.len], b, rotates[index mod rotates.len])
  var digest: MDigest[256]
  for laneIndex in 0 ..< lanes.len:
    var lane = lanes[laneIndex]
    for byteIndex in 0 ..< 8:
      digest.data[laneIndex * 8 + byteIndex] = byte(lane and 0xff'u64)
      lane = lane shr 8
  SaltedId(data: digest)

proc hasSeen*(f: FloodSub, saltedId: SaltedId): bool =
  saltedId in f.seen

proc addSeen*(f: FloodSub, saltedId: SaltedId): bool =
  # Return true if the message has already been seen
  f.seen.put(saltedId)

proc firstSeen*(f: FloodSub, saltedId: SaltedId): Moment =
  f.seen.addedAt(saltedId)

proc handleSubscribe(f: FloodSub, peer: PubSubPeer, topic: string, subscribe: bool) =
  logScope:
    peer
    topic

  # this is a workaround for a race condition
  # that can happen if we disconnect the peer very early
  # in the future we might use this as a test case
  # and eventually remove this workaround
  if subscribe and peer.peerId notin f.peers:
    trace "ignoring unknown peer"
    return

  if subscribe and not (isNil(f.subscriptionValidator)) and
      not (f.subscriptionValidator(topic)):
    # this is a violation, so warn should be in order
    warn "ignoring invalid topic subscription", topic, peer
    return

  if subscribe:
    trace "adding subscription for topic", peer, topic

    # subscribe the peer to the topic
    withMapEntry(f.floodsub, topic, initHashSet[PubSubPeer](), peers):
      peers.incl(peer)
  else:
    f.floodsub.withValue(topic, peers):
      trace "removing subscription for topic", peer, topic

      # unsubscribe the peer from the topic
      peers[].excl(peer)

method unsubscribePeer*(f: FloodSub, peer: PeerId) =
  ## handle peer disconnects
  ##
  trace "unsubscribing floodsub peer", peer
  let pubSubPeer = f.peers.getOrDefault(peer)
  if pubSubPeer.isNil:
    return

  for _, v in f.floodsub.mpairs():
    v.excl(pubSubPeer)

  procCall PubSub(f).unsubscribePeer(peer)

method rpcHandler*(
    f: FloodSub, peer: PubSubPeer, data: seq[byte]
) {.async: (raises: [CancelledError, PeerMessageDecodeError, PeerRateLimitError]).} =
  var rpcMsg = decodeRpcMsg(data).valueOr:
    debug "failed to decode msg from peer", peer, err = error
    raise newException(PeerMessageDecodeError, "Peer msg couldn't be decoded")

  trace "decoded msg from peer", peer, payload = rpcMsg.shortLog
  # trigger hooks
  peer.recvObservers(rpcMsg)

  for i in 0 ..< min(f.topicsHigh, rpcMsg.subscriptions.len):
    template sub(): untyped =
      rpcMsg.subscriptions[i]

    f.handleSubscribe(peer, sub.topic, sub.subscribe)

  for msg in rpcMsg.messages: # for every message
    let msgIdResult = f.msgIdProvider(msg)
    if msgIdResult.isErr:
      debug "Dropping message due to failed message id generation",
        error = msgIdResult.error
      # TODO: descore peers due to error during message validation (malicious?)
      continue

    let
      msgId = msgIdResult.get
      saltedId = f.salt(msgId)

    if f.addSeen(saltedId):
      trace "Dropping already-seen message", msgId, peer
      continue

    if (msg.signature.len > 0 or f.verifySignature) and not msg.verify():
      # always validate if signature is present or required
      debug "Dropping message due to failed signature verification", msgId, peer
      continue

    if msg.seqno.len > 0 and msg.seqno.len != 8:
      # if we have seqno should be 8 bytes long
      debug "Dropping message due to invalid seqno length", msgId, peer
      continue

    # g.anonymize needs no evaluation when receiving messages
    # as we have a "lax" policy and allow signed messages

    let validation = await f.validate(msg)
    case validation
    of ValidationResult.Reject:
      debug "Dropping message after validation, reason: reject", msgId, peer
      continue
    of ValidationResult.Ignore:
      debug "Dropping message after validation, reason: ignore", msgId, peer
      continue
    of ValidationResult.Accept:
      discard

    var toSendPeers = initHashSet[PubSubPeer]()
    let topic = msg.topic
    if topic notin f.topics:
      debug "Dropping message due to topic not in floodsub topics", topic, msgId, peer
      continue

    f.floodsub.withValue(topic, peers):
      toSendPeers.incl(peers[])

    await handleData(f, topic, msg.data)

    # In theory, if topics are the same in all messages, we could batch - we'd
    # also have to be careful to only include validated messages
    f.broadcast(toSendPeers, RPCMsg(messages: @[msg]), isHighPriority = false)
    trace "Forwared message to peers", peers = toSendPeers.len

  f.updateMetrics(rpcMsg)

method init*(f: FloodSub) =
  proc handler(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    ## main protocol handler that gets triggered on every
    ## connection for a protocol string
    ## e.g. ``/floodsub/1.0.0``, etc...
    ##
    try:
      await f.handleConn(conn, proto)
    except CancelledError as exc:
      trace "Unexpected cancellation in floodsub handler", conn, description = exc.msg
      raise exc

  f.handler = handler
  f.codec = FloodSubCodec

method publish*(
    f: FloodSub,
    topic: string,
    data: seq[byte],
    publishParams: Option[PublishParams] = none(PublishParams),
): Future[int] {.async: (raises: []).} =
  # base returns always 0
  discard await procCall PubSub(f).publish(topic, data)

  trace "Publishing message on topic", data = data.shortLog, topic

  if topic.len <= 0: # data could be 0/empty
    debug "Empty topic, skipping publish", topic
    return 0

  let peers = f.floodsub.getOrDefault(topic)

  if peers.len == 0:
    debug "No peers for topic, skipping publish", topic
    return 0

  let
    msg =
      if f.anonymize:
        Message.init(none(PeerInfo), data, topic, none(uint64), false)
      else:
        inc f.msgSeqno
        Message.init(some(f.peerInfo), data, topic, some(f.msgSeqno), f.sign)
    msgId = f.msgIdProvider(msg).valueOr:
      trace "Error generating message id, skipping publish", error = error
      return 0

  trace "Created new message", payload = shortLog(msg), peers = peers.len, topic, msgId

  if f.addSeen(f.salt(msgId)):
    # custom msgid providers might cause this
    trace "Dropping already-seen message", msgId, topic
    return 0

  # Try to send to all peers that are known to be interested
  f.broadcast(peers, RPCMsg(messages: @[msg]), isHighPriority = true)

  when defined(libp2p_expensive_metrics):
    libp2p_pubsub_messages_published.inc(labelValues = [topic])

  trace "Published message to peers", msgId, topic

  return peers.len

method initPubSub*(f: FloodSub) {.raises: [InitializationError].} =
  procCall PubSub(f).initPubSub()
  f.seen = TimedCache[SaltedId].init(2.minutes)
  hmacDrbgGenerate(f.rng[], f.seenSaltSeed)

  f.init()
