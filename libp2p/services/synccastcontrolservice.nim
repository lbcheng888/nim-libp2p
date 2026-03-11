{.push raises: [].}

import std/[algorithm, json, options, sequtils, strutils, tables]
from std/times import epochTime

import chronos, chronicles

import ../peerid
import ../peerstore
import ../protocols/protocol
import ../stream/connection
import ../stream/lpstream
import ../switch
import ./noderesourceservice

const
  SynccastControlCodec* = "/cheng-synccast/control/1.0.0"
  DefaultSynccastMaxMessageBytes* = 32 * 1024

logScope:
  topics = "libp2p synccast-control"

type
  SynccastRole* {.pure.} = enum
    srPublisher,
    srWorker,
    srRelay,
    srSfu

  SynccastSessionSpec* = object
    roomId*: string
    sessionId*: string
    desiredRole*: SynccastRole
    requireGpu*: bool
    minMemoryBytes*: int64
    minBandwidthBps*: int64
    preferDatagram*: bool
    stickyKey*: string

  SynccastRoute* = object
    routeId*: string
    anchorPeer*: PeerId
    selectedPeer*: PeerId
    selectedRole*: SynccastRole
    fallbackPeers*: seq[PeerId]
    transport*: string
    leaseTtlMs*: int64
    reason*: string

  SynccastControlConfig* = object
    codec*: string
    requestTimeout*: Duration
    maxRequestBytes*: int
    maxResponseBytes*: int
    routeLeaseTtl*: Duration

  SynccastControlService* = ref object of Service
    switch*: Switch
    config*: SynccastControlConfig
    protocol: LPProtocol
    mounted: bool
    routes: Table[string, SynccastRoute]
    stickyRoutes: Table[string, SynccastRoute]

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

proc durationToMillis(value: Duration): int64 =
  value.nanoseconds div 1_000_000

proc roleToString(role: SynccastRole): string =
  case role
  of srPublisher:
    "publisher"
  of srWorker:
    "worker"
  of srRelay:
    "relay"
  of srSfu:
    "sfu"

proc parseRole(text: string): Option[SynccastRole] =
  case text.toLowerAscii()
  of "publisher":
    some(srPublisher)
  of "worker":
    some(srWorker)
  of "relay":
    some(srRelay)
  of "sfu":
    some(srSfu)
  else:
    none(SynccastRole)

proc jsonGetStr(node: JsonNode, key: string, defaultValue = ""): string =
  if not node.isNil and node.kind == JObject:
    let value = node.getOrDefault(key)
    if not value.isNil and value.kind == JString:
      return value.getStr()
  defaultValue

proc jsonGetBool(node: JsonNode, key: string, defaultValue = false): bool =
  if node.isNil or node.kind != JObject:
    return defaultValue
  let value = node.getOrDefault(key)
  if value.isNil:
    return defaultValue
  case value.kind
  of JBool:
    value.getBool()
  of JInt:
    value.getInt() != 0
  else:
    defaultValue

proc jsonGetInt64(node: JsonNode, key: string, defaultValue = 0'i64): int64 =
  if node.isNil or node.kind != JObject:
    return defaultValue
  let value = node.getOrDefault(key)
  if value.isNil:
    return defaultValue
  case value.kind
  of JInt:
    value.getInt().int64
  of JFloat:
    int64(value.getFloat())
  else:
    defaultValue

proc parsePeerList(node: JsonNode, key: string): seq[PeerId] =
  if node.kind != JObject:
    return @[]
  let values = node.getOrDefault(key)
  if values.isNil or values.kind != JArray:
    return @[]
  for child in values:
    if child.kind != JString:
      continue
    var peerId: PeerId
    if peerId.init(child.getStr()):
      result.add(peerId)

proc encodePeerList(peers: openArray[PeerId]): JsonNode =
  result = newJArray()
  for peerId in peers:
    if peerId.data.len > 0:
      result.add(%($peerId))

proc isValidSpec(spec: SynccastSessionSpec): bool =
  spec.roomId.len > 0 and spec.sessionId.len > 0 and spec.minMemoryBytes >= 0 and
    spec.minBandwidthBps >= 0

proc toJson(spec: SynccastSessionSpec): JsonNode =
  %*{
    "roomId": spec.roomId,
    "sessionId": spec.sessionId,
    "desiredRole": roleToString(spec.desiredRole),
    "requireGpu": spec.requireGpu,
    "minMemoryBytes": spec.minMemoryBytes,
    "minBandwidthBps": spec.minBandwidthBps,
    "preferDatagram": spec.preferDatagram,
    "stickyKey": spec.stickyKey,
  }

proc decodeSpec(node: JsonNode): Option[SynccastSessionSpec] =
  if node.isNil or node.kind != JObject:
    return none(SynccastSessionSpec)
  let roleOpt = parseRole(jsonGetStr(node, "desiredRole"))
  if roleOpt.isNone():
    return none(SynccastSessionSpec)
  let spec = SynccastSessionSpec(
    roomId: jsonGetStr(node, "roomId"),
    sessionId: jsonGetStr(node, "sessionId"),
    desiredRole: roleOpt.get(),
    requireGpu: jsonGetBool(node, "requireGpu"),
    minMemoryBytes: jsonGetInt64(node, "minMemoryBytes"),
    minBandwidthBps: jsonGetInt64(node, "minBandwidthBps"),
    preferDatagram: jsonGetBool(node, "preferDatagram"),
    stickyKey: jsonGetStr(node, "stickyKey"),
  )
  if not isValidSpec(spec):
    return none(SynccastSessionSpec)
  some(spec)

proc toJson(route: SynccastRoute): JsonNode =
  result = %*{
    "routeId": route.routeId,
    "selectedRole": roleToString(route.selectedRole),
    "transport": route.transport,
    "leaseTtlMs": route.leaseTtlMs,
    "reason": route.reason,
  }
  if route.anchorPeer.data.len > 0:
    result["anchorPeer"] = %($route.anchorPeer)
  else:
    result["anchorPeer"] = newJNull()
  if route.selectedPeer.data.len > 0:
    result["selectedPeer"] = %($route.selectedPeer)
  else:
    result["selectedPeer"] = newJNull()
  result["fallbackPeers"] = encodePeerList(route.fallbackPeers)

proc decodeRoute(node: JsonNode): Option[SynccastRoute] =
  if node.isNil or node.kind != JObject:
    return none(SynccastRoute)
  let roleOpt = parseRole(jsonGetStr(node, "selectedRole"))
  if roleOpt.isNone():
    return none(SynccastRoute)
  var anchorPeer: PeerId
  discard anchorPeer.init(jsonGetStr(node, "anchorPeer"))
  var selectedPeer: PeerId
  discard selectedPeer.init(jsonGetStr(node, "selectedPeer"))
  let route = SynccastRoute(
    routeId: jsonGetStr(node, "routeId"),
    anchorPeer: anchorPeer,
    selectedPeer: selectedPeer,
    selectedRole: roleOpt.get(),
    fallbackPeers: parsePeerList(node, "fallbackPeers"),
    transport: jsonGetStr(node, "transport", "stream"),
    leaseTtlMs: jsonGetInt64(node, "leaseTtlMs"),
    reason: jsonGetStr(node, "reason"),
  )
  if route.routeId.len == 0 or route.selectedPeer.data.len == 0:
    return none(SynccastRoute)
  some(route)

proc encodeHello(spec: SynccastSessionSpec): seq[byte] =
  stringToBytes($(%*{"op": "hello", "spec": toJson(spec)}))

proc encodeNegotiated(route: SynccastRoute): seq[byte] =
  stringToBytes($(%*{"op": "negotiated", "route": toJson(route)}))

proc encodeError(reason: string): seq[byte] =
  stringToBytes($(%*{"op": "error", "reason": reason}))

proc decodeHello(payload: seq[byte]): Option[SynccastSessionSpec] =
  if payload.len == 0:
    return none(SynccastSessionSpec)
  try:
    let node = parseJson(bytesToString(payload))
    if node.kind != JObject or jsonGetStr(node, "op") != "hello":
      return none(SynccastSessionSpec)
    decodeSpec(node.getOrDefault("spec"))
  except CatchableError:
    none(SynccastSessionSpec)

proc decodeNegotiated(payload: seq[byte]): Option[SynccastRoute] =
  if payload.len == 0:
    return none(SynccastRoute)
  try:
    let node = parseJson(bytesToString(payload))
    if node.kind != JObject or jsonGetStr(node, "op") != "negotiated":
      return none(SynccastRoute)
    decodeRoute(node.getOrDefault("route"))
  except CatchableError:
    none(SynccastRoute)

proc decodeErrorReason(payload: seq[byte]): string =
  if payload.len == 0:
    return ""
  try:
    let node = parseJson(bytesToString(payload))
    if node.kind == JObject and jsonGetStr(node, "op") == "error":
      return jsonGetStr(node, "reason")
  except CatchableError:
    discard
  ""

proc init*(
    _: type SynccastControlConfig,
    codec = SynccastControlCodec,
    requestTimeout = 5.seconds,
    maxRequestBytes = DefaultSynccastMaxMessageBytes,
    maxResponseBytes = DefaultSynccastMaxMessageBytes,
    routeLeaseTtl = 60.seconds,
): SynccastControlConfig =
  SynccastControlConfig(
    codec: codec,
    requestTimeout: requestTimeout,
    maxRequestBytes: max(1, maxRequestBytes),
    maxResponseBytes: max(1, maxResponseBytes),
    routeLeaseTtl: routeLeaseTtl,
  )

proc new*(
    T: typedesc[SynccastControlService],
    config: SynccastControlConfig = SynccastControlConfig.init(),
): T =
  T(
    config: config,
    routes: initTable[string, SynccastRoute](),
    stickyRoutes: initTable[string, SynccastRoute](),
  )

proc getSynccastControlService*(switch: Switch): SynccastControlService =
  if switch.isNil:
    return nil
  for service in switch.services:
    if service of SynccastControlService:
      return SynccastControlService(service)

proc storeRoute(
    svc: SynccastControlService, spec: SynccastSessionSpec, route: SynccastRoute
) =
  if svc.isNil or route.routeId.len == 0:
    return
  svc.routes[route.routeId] = route
  if spec.stickyKey.len > 0:
    svc.stickyRoutes[spec.stickyKey] = route

proc getSynccastRoute*(
    svc: SynccastControlService, routeId: string
): Option[SynccastRoute] =
  if svc.isNil or routeId.len == 0:
    return none(SynccastRoute)
  try:
    some(svc.routes[routeId])
  except KeyError:
    none(SynccastRoute)

proc getSynccastRoute*(
    switch: Switch, routeId: string
): Option[SynccastRoute] =
  let svc = getSynccastControlService(switch)
  if svc.isNil:
    return none(SynccastRoute)
  svc.getSynccastRoute(routeId)

proc peerIsEligible(svc: SynccastControlService, peerId: PeerId): bool =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0:
    return false
  if peerId == svc.switch.peerInfo.peerId:
    return true
  if svc.switch.isConnected(peerId):
    return true
  let protoBook = svc.switch.peerStore[ProtoBook]
  svc.config.codec in protoBook[peerId] or NodeResourceCodec in protoBook[peerId]

proc relaySorter(a, b: NodeResourceSnapshot): int =
  if a.resource.bandwidth.downlinkBps != b.resource.bandwidth.downlinkBps:
    return cmp(b.resource.bandwidth.downlinkBps, a.resource.bandwidth.downlinkBps)
  if a.resource.memory.availableBytes != b.resource.memory.availableBytes:
    return cmp(b.resource.memory.availableBytes, a.resource.memory.availableBytes)
  if a.resource.gpu.computeScore != b.resource.gpu.computeScore:
    return cmp(b.resource.gpu.computeScore, a.resource.gpu.computeScore)
  cmp($a.peerId, $b.peerId)

proc pickCandidates(
    svc: SynccastControlService, spec: SynccastSessionSpec
): seq[NodeResourceSnapshot] =
  let resourceSvc = getNodeResourceService(svc.switch)
  if resourceSvc.isNil:
    return @[]
  case spec.desiredRole
  of srPublisher:
    let local = resourceSvc.getLocalNodeResource()
    result = @[
      NodeResourceSnapshot(
        peerId: svc.switch.peerInfo.peerId,
        resource: local,
        lastSeenMs: local.updatedAtMs,
      )
    ]
  of srWorker:
    result = resourceSvc.findResourceCandidates(
      requireGpu = spec.requireGpu,
      minMemoryBytes = spec.minMemoryBytes,
      minBandwidthBps = spec.minBandwidthBps,
    )
  of srRelay, srSfu:
    for snapshot in resourceSvc.nodeResourceSnapshots():
      if snapshot.resource.memory.availableBytes < spec.minMemoryBytes:
        continue
      if snapshot.resource.bandwidth.downlinkBps < spec.minBandwidthBps:
        continue
      if spec.requireGpu and snapshot.resource.gpu.computeScore <= 0:
        continue
      result.add(snapshot)
    result.sort(relaySorter)
  result.keepItIf(svc.peerIsEligible(it.peerId))

proc chooseRoute(
    svc: SynccastControlService, spec: SynccastSessionSpec
): Future[Option[SynccastRoute]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or not isValidSpec(spec):
    return none(SynccastRoute)
  let resourceSvc = getNodeResourceService(svc.switch)
  if resourceSvc.isNil:
    return none(SynccastRoute)

  await resourceSvc.hostNodeResourceUpdateAll()
  var candidates = svc.pickCandidates(spec)
  if candidates.len == 0:
    return none(SynccastRoute)

  var reason = "selected-by-resource"
  if spec.stickyKey.len > 0 and svc.stickyRoutes.hasKey(spec.stickyKey):
    let sticky = svc.stickyRoutes.getOrDefault(spec.stickyKey)
    if candidates.anyIt(it.peerId == sticky.selectedPeer):
      var stickyIdx = -1
      for idx, candidate in candidates.pairs():
        if candidate.peerId == sticky.selectedPeer:
          stickyIdx = idx
          break
      if stickyIdx >= 0:
        let selected = candidates[stickyIdx]
        candidates.delete(stickyIdx)
        candidates.insert(selected, 0)
        reason = "sticky-reuse"

  let selected = candidates[0]
  let route = SynccastRoute(
    routeId:
      spec.roomId & ":" & spec.sessionId & ":" & $selected.peerId & ":" & $nowMillis(),
    anchorPeer: svc.switch.peerInfo.peerId,
    selectedPeer: selected.peerId,
    selectedRole:
      (if spec.desiredRole == srSfu: srRelay else: spec.desiredRole),
    fallbackPeers: candidates[1 .. ^1].mapIt(it.peerId),
    transport: "stream",
    leaseTtlMs: durationToMillis(svc.config.routeLeaseTtl),
    reason: reason,
  )
  svc.storeRoute(spec, route)
  some(route)

proc buildProtocol(svc: SynccastControlService): LPProtocol =
  let handler: LPProtoHandler =
    proc(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
      try:
        let payload = await conn.readLp(svc.config.maxRequestBytes)
        let specOpt = decodeHello(payload)
        if specOpt.isNone():
          await conn.writeLp(encodeError("invalid synccast hello"))
          return
        let routeOpt = await svc.chooseRoute(specOpt.get())
        if routeOpt.isSome():
          await conn.writeLp(encodeNegotiated(routeOpt.get()))
        else:
          await conn.writeLp(encodeError("no synccast route available"))
      except CancelledError as exc:
        raise exc
      except LPStreamError:
        discard
      except CatchableError as exc:
        debug "synccast control handler failed", error = exc.msg
        try:
          await conn.writeLp(encodeError(exc.msg))
        except CatchableError:
          discard
  LPProtocol.new(@[svc.config.codec], handler)

proc openConn(
    svc: SynccastControlService, peerId: PeerId
): Future[Connection] {.async: (raises: [CancelledError]).} =
  try:
    if svc.switch.isConnected(peerId):
      return await svc.switch.dial(peerId, @[svc.config.codec])

    let protocols = svc.switch.peerStore[ProtoBook][peerId]
    if svc.config.codec notin protocols:
      return nil
    let addrs = svc.switch.peerStore.getAddresses(peerId)
    if addrs.len == 0:
      return nil
    return await svc.switch.dial(peerId, addrs, @[svc.config.codec], forceDial = false)
  except CancelledError as exc:
    raise exc
  except CatchableError:
    return nil

proc negotiateSynccastRoute*(
    svc: SynccastControlService, anchorPeer: PeerId, spec: SynccastSessionSpec
): Future[Option[SynccastRoute]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or not isValidSpec(spec):
    return none(SynccastRoute)
  let conn = await svc.openConn(anchorPeer)
  if conn.isNil:
    return none(SynccastRoute)

  defer:
    try:
      await conn.close()
    except CatchableError:
      discard

  try:
    await conn.writeLp(encodeHello(spec))
    let readFuture = conn.readLp(svc.config.maxResponseBytes)
    let completed = await withTimeout(readFuture, svc.config.requestTimeout)
    if not completed:
      if not readFuture.finished():
        readFuture.cancelSoon()
      return none(SynccastRoute)
    let payload = readFuture.read()
    let routeOpt = decodeNegotiated(payload)
    if routeOpt.isSome():
      svc.storeRoute(spec, routeOpt.get())
      return routeOpt
    let errReason = decodeErrorReason(payload)
    if errReason.len > 0:
      debug "synccast negotiation rejected", peer = $anchorPeer, reason = errReason
    none(SynccastRoute)
  except CancelledError as exc:
    raise exc
  except CatchableError:
    none(SynccastRoute)

proc negotiateSynccastRoute*(
    switch: Switch, anchorPeer: PeerId, spec: SynccastSessionSpec
): Future[Option[SynccastRoute]] {.async: (raises: [CancelledError]).} =
  let svc = getSynccastControlService(switch)
  if svc.isNil:
    return none(SynccastRoute)
  await svc.negotiateSynccastRoute(anchorPeer, spec)

method setup*(
    svc: SynccastControlService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(svc).setup(switch)
  if hasBeenSetup:
    svc.switch = switch
    if svc.protocol.isNil:
      svc.protocol = svc.buildProtocol()
    if switch.isStarted() and not svc.protocol.started:
      await svc.protocol.start()
    if not svc.mounted:
      try:
        switch.mount(svc.protocol)
        svc.mounted = true
      except CatchableError as exc:
        warn "failed to mount synccast control protocol", error = exc.msg
        discard await procCall Service(svc).stop(switch)
        return false
    await svc.run(switch)
  hasBeenSetup

method run*(
    svc: SynccastControlService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  discard

method stop*(
    svc: SynccastControlService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(svc).stop(switch)
  if hasBeenStopped and not svc.protocol.isNil and svc.protocol.started:
    await svc.protocol.stop()
  hasBeenStopped

proc startSynccastControlService*(
    switch: Switch,
    config: SynccastControlConfig = SynccastControlConfig.init(),
): Future[SynccastControlService] {.async: (raises: [CancelledError]).} =
  if switch.isNil:
    return nil
  discard await startNodeResourceService(switch)
  var svc = getSynccastControlService(switch)
  if svc.isNil:
    svc = SynccastControlService.new(config = config)
    switch.services.add(svc)
  if switch.isStarted():
    discard await svc.setup(switch)
  svc

{.pop.}
