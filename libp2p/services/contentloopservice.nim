{.push raises: [].}

import ../features

when not libp2pFetchEnabled:
  {.fatal: "contentloopservice requires fetch support".}

import std/[json, options, strutils, tables]
from std/times import epochTime
import chronos
import chronicles

import ../peerid
import ../switch
import ../protocols/pubsub/gossipsub
import ../protocols/feed/feedservice
import ../protocols/fetch/fetch
import ../protocols/fetch/protobuf
import ../protocols/dm/dmservice

const
  DefaultContentReceiptCodec* = "/unimaker/content-receipt/1.0.0"
  ContentReceiptOp = "content-receipt"

logScope:
  topics = "libp2p contentloop"

type
  ContentLoopConfig* = object
    topicPrefix*: string
    fetchTimeout*: Duration
    maxFetchBytes*: int
    receiptTimeout*: Duration
    receiptCodec*: string
    followHints*: bool

  ContentAnnouncement* = object
    id*: string
    mediaType*: string
    cover*: seq[byte]
    summary*: string
    contentKey*: string
    cid*: string
    authorDid*: string
    providers*: seq[PeerId]
    bootstrapPeers*: seq[PeerId]
    receiptRequested*: bool
    extra*: JsonNode

  ContentLoopItem* = object
    publisher*: PeerId
    topic*: string
    announcement*: ContentAnnouncement
    payload*: seq[byte]
    fetchedFrom*: Option[PeerId]
    fetchError*: string

  ContentReceiptStatus* {.pure.} = enum
    crFetched,
    crFetchFailed,
    crAccepted,
    crRejected

  ContentReceipt* = object
    id*: string
    contentKey*: string
    status*: ContentReceiptStatus
    consumer*: PeerId
    producer*: PeerId
    message*: string
    timestampMs*: int64
    extra*: JsonNode

  ContentLoopHandler* = proc(item: ContentLoopItem): Future[void] {.
    gcsafe, raises: [], closure
  .}

  ContentReceiptHandler* = proc(receipt: ContentReceipt): Future[void] {.
    gcsafe, raises: [], closure
  .}

  ContentLoopService* = ref object
    switch*: Switch
    gossip*: GossipSub
    selfPeer*: PeerId
    config*: ContentLoopConfig
    feedService*: FeedService
    fetchService*: FetchService
    receiptService*: DirectMessageService
    itemHandler*: ContentLoopHandler
    receiptHandler*: ContentReceiptHandler
    contentStore: Table[string, seq[byte]]
    receiptLog: seq[ContentReceipt]
    started: bool

proc init*(
    _: type ContentLoopConfig,
    topicPrefix = "/content-feed",
    fetchTimeout = 10.seconds,
    maxFetchBytes = DefaultFetchMessageSize,
    receiptTimeout = 12.seconds,
    receiptCodec = DefaultContentReceiptCodec,
    followHints = true,
): ContentLoopConfig =
  ContentLoopConfig(
    topicPrefix: topicPrefix,
    fetchTimeout: fetchTimeout,
    maxFetchBytes: maxFetchBytes,
    receiptTimeout: receiptTimeout,
    receiptCodec: receiptCodec,
    followHints: followHints,
  )

proc bytesToString(data: seq[byte]): string =
  if data.len == 0:
    return ""
  result = newString(data.len)
  copyMem(addr result[0], unsafeAddr data[0], data.len)

proc stringToBytes(text: string): seq[byte] =
  if text.len == 0:
    return @[]
  result = newSeq[byte](text.len)
  copyMem(addr result[0], text.cstring, text.len)

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc jsonGetStr(node: JsonNode, key: string, defaultValue = ""): string =
  if node.kind == JObject:
    let value = node.getOrDefault(key)
    if not value.isNil and value.kind == JString:
      return value.getStr()
  defaultValue

proc jsonGetBool(node: JsonNode, key: string, defaultValue = false): bool =
  if node.kind == JObject:
    let value = node.getOrDefault(key)
    if value.isNil:
      return defaultValue
    case value.kind
    of JBool:
      return value.getBool()
    of JInt:
      return value.getInt() != 0
    else:
      discard
  defaultValue

proc parsePeerList(node: JsonNode, key: string): seq[PeerId] =
  if node.kind != JObject:
    return
  let values = node.getOrDefault(key)
  if values.isNil or values.kind != JArray:
    return

  for child in values:
    if child.kind != JString:
      continue
    var peer: PeerId
    if peer.init(child.getStr()):
      result.add(peer)

proc encodePeerList(peers: seq[PeerId]): JsonNode =
  result = newJArray()
  for peer in peers:
    if peer.data.len > 0:
      result.add(newJString($peer))

proc parseExtra(entry: FeedEntry): JsonNode =
  if not entry.extra.isNil and entry.extra.kind == JObject:
    return entry.extra
  if entry.raw.len == 0:
    return newJObject()
  try:
    let node = parseJson(bytesToString(entry.raw))
    if node.kind == JObject:
      return node
  except CatchableError:
    discard
  newJObject()

proc normalizeAnnouncement(
    announcement: ContentAnnouncement
): ContentAnnouncement =
  result = announcement
  if result.contentKey.len == 0:
    if result.cid.len > 0:
      result.contentKey = result.cid
    elif result.id.len > 0:
      result.contentKey = result.id
    else:
      result.contentKey = "content-" & $nowMillis()
  if result.id.len == 0:
    result.id = result.contentKey

proc toAnnouncement(entry: FeedEntry): ContentAnnouncement =
  let extra = parseExtra(entry)
  result = normalizeAnnouncement(
    ContentAnnouncement(
      id: entry.id,
      mediaType: entry.mediaType,
      cover: entry.cover,
      summary: entry.summary,
      contentKey: jsonGetStr(extra, "contentKey"),
      cid: jsonGetStr(extra, "cid"),
      authorDid: jsonGetStr(extra, "authorDid"),
      providers: parsePeerList(extra, "providers"),
      bootstrapPeers: parsePeerList(extra, "bootstrapPeers"),
      receiptRequested: jsonGetBool(extra, "receiptRequested"),
      extra: extra,
    )
  )

proc toFeedEntry(announcement: ContentAnnouncement): FeedEntry =
  let normalized = normalizeAnnouncement(announcement)
  var extra =
    if normalized.extra.isNil or normalized.extra.kind != JObject:
      newJObject()
    else:
      normalized.extra

  extra["contentKey"] = %normalized.contentKey
  extra["receiptRequested"] = %normalized.receiptRequested
  if normalized.cid.len > 0:
    extra["cid"] = %normalized.cid
  else:
    extra["cid"] = newJNull()
  if normalized.authorDid.len > 0:
    extra["authorDid"] = %normalized.authorDid
  else:
    extra["authorDid"] = newJNull()
  if normalized.providers.len > 0:
    extra["providers"] = encodePeerList(normalized.providers)
  else:
    extra["providers"] = newJArray()
  if normalized.bootstrapPeers.len > 0:
    extra["bootstrapPeers"] = encodePeerList(normalized.bootstrapPeers)
  else:
    extra["bootstrapPeers"] = newJArray()

  FeedEntry(
    id: normalized.id,
    mediaType: normalized.mediaType,
    cover: normalized.cover,
    summary: normalized.summary,
    extra: extra,
  )

proc receiptStatusToString(status: ContentReceiptStatus): string =
  case status
  of crFetched:
    "fetched"
  of crFetchFailed:
    "fetch_failed"
  of crAccepted:
    "accepted"
  of crRejected:
    "rejected"

proc parseReceiptStatus(value: string): Option[ContentReceiptStatus] =
  case value.toLowerAscii()
  of "fetched":
    some(crFetched)
  of "fetch_failed":
    some(crFetchFailed)
  of "accepted":
    some(crAccepted)
  of "rejected":
    some(crRejected)
  else:
    none(ContentReceiptStatus)

proc encodeReceipt(receipt: ContentReceipt): seq[byte] =
  var extra =
    if receipt.extra.isNil or receipt.extra.kind != JObject:
      newJObject()
    else:
      receipt.extra

  extra["op"] = %ContentReceiptOp
  extra["id"] = %receipt.id
  extra["contentKey"] = %receipt.contentKey
  extra["status"] = %receiptStatusToString(receipt.status)
  extra["message"] = %receipt.message
  extra["timestamp_ms"] = %receipt.timestampMs
  if receipt.consumer.data.len > 0:
    extra["consumer"] = %($receipt.consumer)
  else:
    extra["consumer"] = newJNull()
  if receipt.producer.data.len > 0:
    extra["producer"] = %($receipt.producer)
  else:
    extra["producer"] = newJNull()

  stringToBytes($extra)

proc decodeReceipt(payload: seq[byte]): Option[ContentReceipt] =
  if payload.len == 0:
    return none(ContentReceipt)
  try:
    let node = parseJson(bytesToString(payload))
    if node.kind != JObject or jsonGetStr(node, "op") != ContentReceiptOp:
      return none(ContentReceipt)
    let status = parseReceiptStatus(jsonGetStr(node, "status"))
    if status.isNone:
      return none(ContentReceipt)

    var consumer: PeerId
    discard consumer.init(jsonGetStr(node, "consumer"))
    var producer: PeerId
    discard producer.init(jsonGetStr(node, "producer"))

    let contentKey = jsonGetStr(node, "contentKey")
    let id = jsonGetStr(node, "id", contentKey)
    if id.len == 0:
      return none(ContentReceipt)

    some(
      ContentReceipt(
        id: id,
        contentKey: contentKey,
        status: status.get(),
        consumer: consumer,
        producer: producer,
        message: jsonGetStr(node, "message"),
        timestampMs:
          block:
            let tsNode = node.getOrDefault("timestamp_ms")
            if not tsNode.isNil and tsNode.kind == JInt:
              tsNode.getInt().int64
            else:
              0'i64,
        extra: node,
      )
    )
  except CatchableError:
    none(ContentReceipt)

proc addUniquePeer(peers: var seq[PeerId], peer: PeerId) =
  if peer.data.len == 0:
    return
  for existing in peers:
    if existing == peer:
      return
  peers.add(peer)

proc contentKeys*(svc: ContentLoopService): seq[string] =
  if svc.isNil:
    return @[]
  for key in svc.contentStore.keys:
    result.add(key)

proc receipts*(svc: ContentLoopService): seq[ContentReceipt] =
  if svc.isNil:
    return @[]
  svc.receiptLog

proc hasContent*(svc: ContentLoopService, key: string): bool =
  if svc.isNil:
    return false
  svc.contentStore.hasKey(key)

proc getContent*(svc: ContentLoopService, key: string): Option[seq[byte]] =
  if svc.isNil or not svc.contentStore.hasKey(key):
    return none(seq[byte])
  some(svc.contentStore.getOrDefault(key))

proc storeContent*(svc: ContentLoopService, key: string, payload: seq[byte]) =
  if svc.isNil or key.len == 0:
    return
  svc.contentStore[key] = payload

proc notifyReceipt(
    svc: ContentLoopService, receipt: ContentReceipt
) {.async: (raises: []), gcsafe.} =
  if svc.isNil:
    return
  svc.receiptLog.add(receipt)
  if not svc.receiptHandler.isNil:
    try:
      await svc.receiptHandler(receipt)
    except CatchableError as exc:
      debug "content receipt handler failed", id = receipt.id, msg = exc.msg

proc sendReceipt*(
    svc: ContentLoopService, peer: PeerId, receipt: ContentReceipt
): Future[(bool, string)] {.async.} =
  if svc.isNil or svc.receiptService.isNil:
    return (false, "content loop service unavailable")

  var normalized = receipt
  if normalized.id.len == 0:
    normalized.id =
      if normalized.contentKey.len > 0: normalized.contentKey else: "receipt-" & $nowMillis()
  if normalized.timestampMs == 0:
    normalized.timestampMs = nowMillis()
  if normalized.consumer.data.len == 0:
    normalized.consumer = svc.selfPeer
  let payload = encodeReceipt(normalized)
  let messageId =
    normalized.id & ":" & receiptStatusToString(normalized.status) & ":" &
    $normalized.timestampMs
  await svc.receiptService.send(
    peer,
    payload,
    ackRequested = true,
    messageId = messageId,
    timeout = svc.config.receiptTimeout,
  )

proc publishContent*(
    svc: ContentLoopService,
    announcement: ContentAnnouncement,
    payload: seq[byte] = @[],
): Future[int] {.async.} =
  if svc.isNil:
    return 0

  var normalized = normalizeAnnouncement(announcement)
  if payload.len > 0:
    svc.storeContent(normalized.contentKey, payload)
  if svc.hasContent(normalized.contentKey):
    normalized.providers.addUniquePeer(svc.selfPeer)
  let entry = toFeedEntry(normalized)
  await svc.feedService.publishFeedItem(entry)

proc candidateProviders(
    publisher: PeerId, announcement: ContentAnnouncement
): seq[PeerId] =
  result.addUniquePeer(publisher)
  for provider in announcement.providers:
    result.addUniquePeer(provider)

proc followHints(
    svc: ContentLoopService, announcement: ContentAnnouncement
) {.async: (raises: []), gcsafe.} =
  if svc.isNil or not svc.config.followHints or svc.feedService.isNil:
    return
  var hinted: seq[PeerId] = @[]
  for peer in announcement.bootstrapPeers:
    hinted.addUniquePeer(peer)
  for peer in announcement.providers:
    hinted.addUniquePeer(peer)
  for peer in hinted:
    if peer == svc.selfPeer:
      continue
    try:
      await svc.feedService.subscribeToPeer(peer)
    except CatchableError as exc:
      debug "content loop hint subscribe failed", peer = $peer, msg = exc.msg

proc fetchContent(
    svc: ContentLoopService,
    publisher: PeerId,
    announcement: ContentAnnouncement,
): Future[(seq[byte], Option[PeerId], string)] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    return (@[], none(PeerId), "content loop service unavailable")
  if announcement.contentKey.len == 0:
    return (@[], none(PeerId), "missing content key")

  let local = svc.getContent(announcement.contentKey)
  if local.isSome:
    return (local.get(), none(PeerId), "")

  var lastError = "content not found"
  for provider in candidateProviders(publisher, announcement):
    try:
      let resp = await fetch(
        svc.switch,
        provider,
        announcement.contentKey,
        timeout = svc.config.fetchTimeout,
        maxResponseBytes = svc.config.maxFetchBytes,
      )
      case resp.status
      of fsOk:
        svc.storeContent(announcement.contentKey, resp.data)
        return (resp.data, some(provider), "")
      of fsNotFound:
        lastError = "content not found at " & $provider
      of fsTooLarge:
        lastError = "content too large at " & $provider
      of fsError:
        lastError = "content fetch failed at " & $provider
    except CancelledError as exc:
      raise exc
    except FetchError as err:
      lastError = err.msg
  (@[], none(PeerId), lastError)

proc handleFeedItem(
    svc: ContentLoopService, item: FeedItem
) {.async: (raises: []), gcsafe.} =
  if svc.isNil:
    return

  let announcement = toAnnouncement(item.entry)
  await svc.followHints(announcement)

  var loopItem = ContentLoopItem(
    publisher: item.publisher,
    topic: item.topic,
    announcement: announcement,
    fetchedFrom: none(PeerId),
  )
  try:
    let (payload, fetchedFrom, fetchError) =
      await svc.fetchContent(item.publisher, announcement)
    loopItem.payload = payload
    loopItem.fetchedFrom = fetchedFrom
    loopItem.fetchError = fetchError
  except CancelledError:
    return
  except CatchableError as exc:
    loopItem.fetchError = exc.msg

  if not svc.itemHandler.isNil:
    try:
      await svc.itemHandler(loopItem)
    except CatchableError as exc:
      debug "content item handler failed", id = announcement.id, msg = exc.msg

  if announcement.receiptRequested:
    let receipt = ContentReceipt(
      id: announcement.id,
      contentKey: announcement.contentKey,
      status:
        (if loopItem.fetchError.len == 0: crFetched else: crFetchFailed),
      consumer: svc.selfPeer,
      producer: item.publisher,
      message: loopItem.fetchError,
    )
    try:
      discard await svc.sendReceipt(item.publisher, receipt)
    except CatchableError as exc:
      debug "content loop auto receipt failed",
        id = announcement.id, peer = $item.publisher, msg = exc.msg

proc handleReceiptMessage(
    svc: ContentLoopService, msg: DirectMessage
) {.async: (raises: []), gcsafe.} =
  if svc.isNil:
    return
  let receiptOpt = decodeReceipt(msg.payload)
  if receiptOpt.isNone:
    return
  var receipt = receiptOpt.get()
  if receipt.consumer.data.len == 0:
    receipt.consumer = msg.sender
  await svc.notifyReceipt(receipt)

proc fetchHandler(
    svc: ContentLoopService, key: string
): Future[FetchResponse] {.async, gcsafe.} =
  if svc.isNil or key.len == 0 or not svc.contentStore.hasKey(key):
    return FetchResponse(status: fsNotFound, data: @[])
  let payload = svc.contentStore.getOrDefault(key)
  if svc.config.maxFetchBytes > 0 and payload.len > svc.config.maxFetchBytes:
    return FetchResponse(status: fsTooLarge, data: @[])
  FetchResponse(status: fsOk, data: payload)

proc newContentLoopService*(
    switch: Switch,
    gossip: GossipSub,
    selfPeer: PeerId,
    handler: ContentLoopHandler = nil,
    receiptHandler: ContentReceiptHandler = nil,
    config: ContentLoopConfig = ContentLoopConfig.init(),
): ContentLoopService =
  var svc: ContentLoopService

  let feedHandler = proc(item: FeedItem) {.async: (raises: []), gcsafe, closure.} =
    await svc.handleFeedItem(item)

  let fetchCb: FetchHandler = proc(key: string): Future[FetchResponse] {.
      async, gcsafe, closure
  .} =
    await svc.fetchHandler(key)

  let receiptCb = proc(msg: DirectMessage) {.async, gcsafe, closure.} =
    await svc.handleReceiptMessage(msg)

  svc = ContentLoopService(
    switch: switch,
    gossip: gossip,
    selfPeer: selfPeer,
    config: config,
    itemHandler: handler,
    receiptHandler: receiptHandler,
    contentStore: initTable[string, seq[byte]](),
    receiptLog: @[],
  )

  svc.feedService = newFeedService(
    gossip,
    selfPeer,
    feedHandler,
    topicPrefix = config.topicPrefix,
  )
  svc.fetchService = FetchService.new(
    fetchCb,
    FetchConfig.init(
      maxRequestBytes = DefaultFetchMessageSize,
      maxResponseBytes = config.maxFetchBytes,
      handlerTimeout = config.fetchTimeout,
    ),
  )
  svc.receiptService = newDirectMessageService(
    switch,
    selfPeer,
    receiptCb,
    codec = config.receiptCodec,
  )
  svc

proc start*(svc: ContentLoopService) {.async.} =
  if svc.isNil or svc.started:
    return
  await svc.fetchService.start()
  svc.switch.mount(svc.fetchService)
  await svc.receiptService.start()
  svc.switch.mount(svc.receiptService)
  svc.started = true

proc stop*(svc: ContentLoopService) {.async.} =
  if svc.isNil or not svc.started:
    return
  await svc.fetchService.stop()
  await svc.receiptService.stop()
  svc.started = false

proc subscribeToPeer*(svc: ContentLoopService, peer: PeerId): Future[void] {.async.} =
  if svc.isNil or svc.feedService.isNil:
    return
  await svc.feedService.subscribeToPeer(peer)

proc unsubscribeFromPeer*(svc: ContentLoopService, peer: PeerId) =
  if svc.isNil or svc.feedService.isNil:
    return
  try:
    svc.feedService.unsubscribeFromPeer(peer)
  except CatchableError as exc:
    debug "content loop unsubscribe failed", peer = $peer, msg = exc.msg
