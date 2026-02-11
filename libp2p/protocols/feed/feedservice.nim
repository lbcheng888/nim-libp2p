import std/[json, options, tables, strutils, base64, sequtils, sets]
import chronos
import chronicles
import ../../peerid
import ../pubsub/gossipsub

type
  FeedEntry* = object
    id*: string
    mediaType*: string
    cover*: seq[byte]
    summary*: string
    extra*: JsonNode
    raw*: seq[byte]

  FeedItem* = object
    publisher*: PeerId
    topic*: string
    entry*: FeedEntry

  FeedHandler* = proc(item: FeedItem) {.async: (raises: []), gcsafe, closure.}

  FeedDecoder* = proc(payload: seq[byte]): Option[FeedEntry] {.raises: [], gcsafe, closure.}

  ContentFetcher* = proc(entry: FeedEntry) {.async: (raises: []), gcsafe, closure.}

  TopicSubscription = ref object
    handler: TopicHandler

  FeedService* = ref object
    gossip: GossipSub
    topicPrefix: string
    handler: FeedHandler
    decoder: FeedDecoder
    subscriptions: Table[string, TopicSubscription]
    selfPeer: PeerId
    seenIds: HashSet[string]
    contentFetcher: ContentFetcher
    deduplicate: bool

proc defaultDecode(payload: seq[byte]): Option[FeedEntry] {.raises: [], gcsafe.} =
  try:
    let rawText = payload.mapIt(char(it)).join("")
    let node = parseJson(rawText)
    if node.kind != JObject:
      return none(FeedEntry)

    let id =
      if node.hasKey("id") and node["id"].kind == JString:
        node["id"].getStr()
      else:
        ""
    let mediaType =
      if node.hasKey("mediaType") and node["mediaType"].kind == JString:
        node["mediaType"].getStr()
      else:
        ""
    let summary =
      if node.hasKey("summary") and node["summary"].kind == JString:
        node["summary"].getStr()
      else:
        ""
    var coverBytes: seq[byte] = @[]
    if node.hasKey("cover") and node["cover"].kind == JString:
      try:
        let decoded = decode(node["cover"].getStr())
        coverBytes = decoded.toSeq().mapIt(byte(it))
      except CatchableError:
        discard

    some(
      FeedEntry(
        id: id,
        mediaType: mediaType,
        cover: coverBytes,
        summary: summary,
        extra: node,
        raw: payload,
      )
    )
  except CatchableError:
    none(FeedEntry)

proc encodeEntry*(entry: FeedEntry): seq[byte] =
  var node =
    if entry.extra.isNil or entry.extra.kind != JObject:
      newJObject()
    else:
      entry.extra

  node["id"] = newJString(entry.id)
  node["mediaType"] = newJString(entry.mediaType)
  node["summary"] = newJString(entry.summary)
  if entry.cover.len > 0:
    node["cover"] = newJString(encode(entry.cover))
  else:
    node.delete("cover")
  let text = $node
  text.toSeq().mapIt(byte(it))

proc toTopic*(prefix: string, peer: PeerId): string =
  prefix & "/" & $peer

proc newFeedService*(
    gossip: GossipSub,
    selfPeer: PeerId,
    handler: FeedHandler,
    decoder: FeedDecoder = nil,
    topicPrefix = "/content-feed"
): FeedService =
  FeedService(
    gossip: gossip,
    topicPrefix: topicPrefix,
    handler: handler,
    decoder: (if decoder.isNil: FeedDecoder(defaultDecode) else: decoder),
    subscriptions: initTable[string, TopicSubscription](),
    selfPeer: selfPeer,
    seenIds: initHashSet[string](),
    contentFetcher: nil,
    deduplicate: true,
  )

proc subscribeToPeer*(
    svc: FeedService, peer: PeerId
) {.async.} =
  if svc.isNil:
    return
  let topic = toTopic(svc.topicPrefix, peer)
  if svc.subscriptions.hasKey(topic):
    return

  let handler = proc(t: string, payload: seq[byte]) {.async.} =
    var entryOpt: Option[FeedEntry]
    try:
      entryOpt =
        if svc.decoder.isNil:
          defaultDecode(payload)
        else:
          svc.decoder(payload)
    except CatchableError as exc:
      debug "feed decoder raised exception", topic = t, msg = exc.msg
      entryOpt = none(FeedEntry)
    if entryOpt.isNone:
      entryOpt = some(
        FeedEntry(
          id: "",
          mediaType: "",
          summary: "",
          cover: @[],
          extra: nil,
          raw: payload,
        )
      )
    var entry: FeedEntry
    try:
      entry = entryOpt.get()
    except ValueError:
      return

    if svc.deduplicate:
      if entry.id.len > 0 and entry.id in svc.seenIds:
        debug "feed entry ignored due to duplicate id", topic = t, id = entry.id
        return
      if entry.id.len > 0:
        svc.seenIds.incl(entry.id)

    if not svc.contentFetcher.isNil:
      try:
        await svc.contentFetcher(entry)
      except CancelledError:
        return
      except CatchableError as exc:
        debug "feed content fetcher failed", topic = t, id = entry.id, msg = exc.msg
    var publisher: PeerId
    let segments = t.split('/')
    if segments.len > 0 and segments[^1].len > 0 and publisher.init(segments[^1]):
      discard
    else:
      publisher = peer

    let item = FeedItem(publisher: publisher, topic: t, entry: entry)
    if not svc.handler.isNil:
      try:
        await svc.handler(item)
      except CatchableError as exc:
        debug "feed handler raised exception", topic = t, msg = exc.msg

  svc.gossip.subscribe(topic, handler)
  svc.subscriptions[topic] = TopicSubscription(handler: handler)

proc unsubscribeFromPeer*(svc: FeedService, peer: PeerId) =
  if svc.isNil:
    return
  let topic = toTopic(svc.topicPrefix, peer)
  if svc.subscriptions.hasKey(topic):
    let sub = svc.subscriptions[topic]
    svc.gossip.unsubscribe(topic, sub.handler)
    svc.subscriptions.del(topic)

proc publishFeedItem*(
    svc: FeedService, entry: FeedEntry
): Future[int] {.async.} =
  if svc.isNil:
    return 0
  let payload = encodeEntry(entry)
  let topic = toTopic(svc.topicPrefix, svc.selfPeer)
  await svc.gossip.publish(topic, payload)

proc setContentFetcher*(svc: FeedService, fetcher: ContentFetcher) =
  if svc.isNil:
    return
  svc.contentFetcher = fetcher

proc setDeduplication*(svc: FeedService, enabled: bool) =
  if svc.isNil:
    return
  svc.deduplicate = enabled
  if not enabled:
    svc.seenIds.clear()

proc resetSeen*(svc: FeedService) =
  if svc.isNil:
    return
  svc.seenIds.clear()
