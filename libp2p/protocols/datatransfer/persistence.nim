# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[json, options, os]
import chronos
import chronicles
import pkg/results

logScope:
  topics = "libp2p datatransfer persistence"

type
  DataTransferExtensionRecord* = object
    name*: string
    data*: string

  DataTransferRequestRecord* = object
    isPull*: bool
    voucherType*: string
    voucher*: string
    selector*: string
    baseCid*: Option[string]

  DataTransferChannelRecord* = object
    peerId*: string
    transferId*: uint64
    direction*: string
    status*: string
    paused*: bool
    remotePaused*: bool
    retryMaxAttempts*: int
    retryDelayNs*: int64
    lastStatus*: Option[string]
    lastError*: Option[string]
    createdAtNs*: int64
    updatedAtNs*: int64
    request*: Option[DataTransferRequestRecord]
    extensions*: seq[DataTransferExtensionRecord]

  DataTransferChannelState* = object
    nextTransferId*: uint64
    channels*: seq[DataTransferChannelRecord]

  DataTransferChannelPersistence* = ref object of RootObj

  DataTransferChannelFilePersistence* = ref object of DataTransferChannelPersistence
    path: string

proc fieldOrNil(node: JsonNode, field: string): JsonNode =
  if node.kind != JObject:
    return nil
  for key, value in node.pairs():
    if key == field:
      return value
  nil

method load*(
    persistence: DataTransferChannelPersistence
): Result[DataTransferChannelState, string] {.base, gcsafe.} =
  err("data transfer persistence load not implemented")

method save*(
    persistence: DataTransferChannelPersistence, state: DataTransferChannelState
): Result[void, string] {.base, gcsafe.} =
  err("data transfer persistence save not implemented")

proc parseExtension(node: JsonNode): DataTransferExtensionRecord =
  if node.kind != JObject:
    return
  let nameNode = fieldOrNil(node, "name")
  let dataNode = fieldOrNil(node, "data")
  if not nameNode.isNil and nameNode.kind == JString:
    result.name = nameNode.getStr()
  if not dataNode.isNil and dataNode.kind == JString:
    result.data = dataNode.getStr()

proc parseRequest(node: JsonNode): DataTransferRequestRecord =
  if node.kind != JObject:
    return
  let pullNode = fieldOrNil(node, "isPull")
  if not pullNode.isNil and pullNode.kind == JBool:
    result.isPull = pullNode.getBool()
  let voucherTypeNode = fieldOrNil(node, "voucherType")
  if not voucherTypeNode.isNil and voucherTypeNode.kind == JString:
    result.voucherType = voucherTypeNode.getStr()
  let voucherNode = fieldOrNil(node, "voucher")
  if not voucherNode.isNil and voucherNode.kind == JString:
    result.voucher = voucherNode.getStr()
  let selectorNode = fieldOrNil(node, "selector")
  if not selectorNode.isNil and selectorNode.kind == JString:
    result.selector = selectorNode.getStr()
  let baseCidNode = fieldOrNil(node, "baseCid")
  if not baseCidNode.isNil and baseCidNode.kind == JString:
    result.baseCid = some(baseCidNode.getStr())

proc parseChannel(node: JsonNode): Option[DataTransferChannelRecord] =
  if node.kind != JObject:
    return none(DataTransferChannelRecord)

  let peerNode = fieldOrNil(node, "peerId")
  let idNode = fieldOrNil(node, "transferId")
  if peerNode.isNil or peerNode.kind != JString or idNode.isNil or idNode.kind notin {JInt, JFloat}:
    return none(DataTransferChannelRecord)

  var record = DataTransferChannelRecord()
  record.peerId = peerNode.getStr()
  record.transferId = uint64(idNode.getBiggestInt(0))

  let directionNode = fieldOrNil(node, "direction")
  if not directionNode.isNil and directionNode.kind == JString:
    record.direction = directionNode.getStr()

  let statusNode = fieldOrNil(node, "status")
  if not statusNode.isNil and statusNode.kind == JString:
    record.status = statusNode.getStr()

  let pausedNode = fieldOrNil(node, "paused")
  if not pausedNode.isNil and pausedNode.kind == JBool:
    record.paused = pausedNode.getBool()

  let remotePausedNode = fieldOrNil(node, "remotePaused")
  if not remotePausedNode.isNil and remotePausedNode.kind == JBool:
    record.remotePaused = remotePausedNode.getBool()

  let retryNode = fieldOrNil(node, "retryMaxAttempts")
  if not retryNode.isNil and retryNode.kind in {JInt, JFloat}:
    record.retryMaxAttempts = retryNode.getInt()

  let delayNode = fieldOrNil(node, "retryDelayNs")
  if not delayNode.isNil and delayNode.kind in {JInt, JFloat}:
    record.retryDelayNs = delayNode.getBiggestInt(0)

  let lastStatusNode = fieldOrNil(node, "lastStatus")
  if not lastStatusNode.isNil and lastStatusNode.kind == JString:
    record.lastStatus = some(lastStatusNode.getStr())

  let lastErrorNode = fieldOrNil(node, "lastError")
  if not lastErrorNode.isNil and lastErrorNode.kind == JString:
    record.lastError = some(lastErrorNode.getStr())

  let createdNode = fieldOrNil(node, "createdAtNs")
  if not createdNode.isNil and createdNode.kind in {JInt, JFloat}:
    record.createdAtNs = createdNode.getBiggestInt(0)

  let updatedNode = fieldOrNil(node, "updatedAtNs")
  if not updatedNode.isNil and updatedNode.kind in {JInt, JFloat}:
    record.updatedAtNs = updatedNode.getBiggestInt(0)

  let requestNode = fieldOrNil(node, "request")
  if not requestNode.isNil:
    record.request = some(parseRequest(requestNode))

  let extensionsNode = fieldOrNil(node, "extensions")
  if not extensionsNode.isNil and extensionsNode.kind == JArray:
    for extNode in extensionsNode:
      let rec = parseExtension(extNode)
      if rec.name.len > 0 or rec.data.len > 0:
        record.extensions.add(rec)

  some(record)

proc encodeRequest(record: DataTransferRequestRecord): JsonNode =
  result = newJObject()
  if record.isPull:
    result["isPull"] = %record.isPull
  if record.voucherType.len > 0:
    result["voucherType"] = %record.voucherType
  if record.voucher.len > 0:
    result["voucher"] = %record.voucher
  if record.selector.len > 0:
    result["selector"] = %record.selector
  if record.baseCid.isSome():
    let value = record.baseCid.get()
    if value.len > 0:
      result["baseCid"] = %value

proc encodeExtension(record: DataTransferExtensionRecord): JsonNode =
  result = newJObject()
  if record.name.len > 0:
    result["name"] = %record.name
  if record.data.len > 0:
    result["data"] = %record.data

proc encodeChannel(record: DataTransferChannelRecord): JsonNode =
  result = newJObject()
  result["peerId"] = %record.peerId
  result["transferId"] = %record.transferId
  if record.direction.len > 0:
    result["direction"] = %record.direction
  if record.status.len > 0:
    result["status"] = %record.status
  if record.paused:
    result["paused"] = %record.paused
  if record.remotePaused:
    result["remotePaused"] = %record.remotePaused
  if record.retryMaxAttempts != 0:
    result["retryMaxAttempts"] = %record.retryMaxAttempts
  if record.retryDelayNs != 0:
    result["retryDelayNs"] = %record.retryDelayNs
  if record.lastStatus.isSome():
    result["lastStatus"] = %record.lastStatus.get()
  if record.lastError.isSome():
    result["lastError"] = %record.lastError.get()
  if record.createdAtNs != 0:
    result["createdAtNs"] = %record.createdAtNs
  if record.updatedAtNs != 0:
    result["updatedAtNs"] = %record.updatedAtNs
  if record.request.isSome():
    result["request"] = encodeRequest(record.request.get())
  if record.extensions.len > 0:
    var exts = newJArray()
    for ext in record.extensions:
      exts.add(encodeExtension(ext))
    result["extensions"] = exts

method load*(
    persistence: DataTransferChannelFilePersistence
): Result[DataTransferChannelState, string] =
  if persistence.isNil or persistence.path.len == 0:
    return err("data transfer persistence path is not configured")

  if not fileExists(persistence.path):
    return ok(
      DataTransferChannelState(nextTransferId: 1, channels: @[])
    )

  let raw =
    try:
      readFile(persistence.path)
    except CatchableError as exc:
      return err("failed to read data transfer persistence: " & exc.msg)

  if raw.len == 0:
    return ok(
      DataTransferChannelState(nextTransferId: 1, channels: @[])
    )

  var root: JsonNode
  try:
    root = parseJson(raw)
  except CatchableError as exc:
    return err("failed to parse data transfer persistence: " & exc.msg)

  if root.kind != JObject:
    return err("data transfer persistence root must be a JSON object")

  var state = DataTransferChannelState(nextTransferId: 1, channels: @[])

  let nextIdNode = fieldOrNil(root, "nextTransferId")
  if not nextIdNode.isNil and nextIdNode.kind in {JInt, JFloat}:
    let candidate = nextIdNode.getBiggestInt(1)
    if candidate > 0:
      state.nextTransferId = uint64(candidate)

  let channelsNode = fieldOrNil(root, "channels")
  if not channelsNode.isNil and channelsNode.kind == JArray:
    for chNode in channelsNode:
      let recOpt = parseChannel(chNode)
      if recOpt.isSome:
        state.channels.add(recOpt.get())

  ok(state)

proc ensureParentDir(path: string) =
  let dir = splitFile(path).dir
  if dir.len == 0:
    return
  try:
    createDir(dir)
  except CatchableError as exc:
    warn "failed creating data transfer persistence directory", dir, error = exc.msg

method save*(
    persistence: DataTransferChannelFilePersistence, state: DataTransferChannelState
): Result[void, string] =
  if persistence.isNil or persistence.path.len == 0:
    return err("data transfer persistence path is not configured")

  var root = newJObject()
  root["nextTransferId"] = %state.nextTransferId
  var channels = newJArray()
  for record in state.channels:
    channels.add(encodeChannel(record))
  root["channels"] = channels

  let serialized = root.pretty()
  ensureParentDir(persistence.path)
  var tempPath = persistence.path & ".tmp"
  try:
    writeFile(tempPath, serialized)
    try:
      moveFile(tempPath, persistence.path)
    except CatchableError as exc:
      raise exc
    except Exception as exc:
      raise newException(IOError, exc.msg)
    ok()
  except CatchableError as exc:
    try:
      if fileExists(tempPath):
        removeFile(tempPath)
    except CatchableError:
      discard
    err("failed to save data transfer persistence: " & exc.msg)

proc new*(
    _: type DataTransferChannelFilePersistence, path: string
): DataTransferChannelFilePersistence =
  DataTransferChannelFilePersistence(path: path)

{.pop.}
