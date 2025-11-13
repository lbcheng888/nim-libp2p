# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}
{.push gcsafe.}

import std/[options]
import results

const
  SelectorKeyEnvelope = "selector"
  SelectorKeyMatcher = "."
  SelectorKeyExploreAll = "a"
  SelectorKeyExploreFields = "f"
  SelectorKeyExploreRecursive = "R"
  SelectorKeyExploreUnion = "|"
  SelectorKeyExploreRecursiveEdge = "@"
  SelectorKeyFields = "f>"
  SelectorKeyLimit = "l"
  SelectorKeySequence = ":>"
  SelectorKeyNext = ">"
  SelectorKeyLimitNone = "none"
  SelectorKeyLimitDepth = "depth"

type
  GraphSyncTraversalMode* {.pure.} = enum
    gstTraverseAll
    gstTraverseNone

  GraphSyncTraversal* = object
    mode*: GraphSyncTraversalMode
    depthLimit*: int ## -1 表示无限深度

  DagCborKind = enum
    dckInt
    dckString
    dckBytes
    dckArray
    dckMap
    dckBool
    dckNull

  DagCborNode = ref object
    case kind: DagCborKind
    of dckInt:
      intVal: int64
    of dckString:
      strVal: string
    of dckBytes:
      bytesVal: seq[byte]
    of dckArray:
      arrayVal: seq[DagCborNode]
    of dckMap:
      mapVal: seq[(DagCborNode, DagCborNode)]
    of dckBool:
      boolVal: bool
    of dckNull:
      discard

proc newIntNode(value: int64): DagCborNode =
  DagCborNode(kind: dckInt, intVal: value)

proc newStringNode(value: string): DagCborNode =
  DagCborNode(kind: dckString, strVal: value)

proc newBytesNode(value: seq[byte]): DagCborNode =
  DagCborNode(kind: dckBytes, bytesVal: value)

proc newArrayNode(value: seq[DagCborNode]): DagCborNode =
  DagCborNode(kind: dckArray, arrayVal: value)

proc newMapNode(value: seq[(DagCborNode, DagCborNode)]): DagCborNode =
  DagCborNode(kind: dckMap, mapVal: value)

proc newBoolNode(value: bool): DagCborNode =
  DagCborNode(kind: dckBool, boolVal: value)

proc newNullNode(): DagCborNode =
  DagCborNode(kind: dckNull)

proc readUint64(
    data: openArray[byte], pos: var int, addInfo: int
): Result[uint64, string] =
  var needed = 0
  var value: uint64 = 0

  case addInfo
  of 0..23:
    return ok(uint64(addInfo))
  of 24:
    needed = 1
  of 25:
    needed = 2
  of 26:
    needed = 4
  of 27:
    needed = 8
  else:
    return err("unsupported CBOR additional info: " & $addInfo)

  if pos + needed > data.len:
    return err("unexpected end of CBOR buffer")

  for i in 0..<needed:
    value = (value shl 8) or uint64(data[pos + i])
  pos += needed
  ok(value)

proc decodeNode(data: openArray[byte], pos: var int): Result[DagCborNode, string]

proc decodeTag(
    data: openArray[byte], pos: var int, addInfo: int
): Result[DagCborNode, string] =
  let tagValRes = readUint64(data, pos, addInfo)
  if tagValRes.isErr():
    return err(tagValRes.error)
  # 忽略 tag，继续解析后续节点
  decodeNode(data, pos)

proc decodeSimple(
    addInfo: int, data: openArray[byte], pos: var int
): Result[DagCborNode, string] =
  case addInfo
  of 20:
    ok(newBoolNode(false))
  of 21:
    ok(newBoolNode(true))
  of 22, 23:
    ok(newNullNode())
  else:
    err("unsupported CBOR simple value: " & $addInfo)

proc decodeNode(
    data: openArray[byte], pos: var int
): Result[DagCborNode, string] =
  if pos >= data.len:
    return err("unexpected end of CBOR buffer")

  let initial = data[pos]
  inc pos
  let major = int(initial shr 5)
  let addInfo = int(initial and 0x1F)

  case major
  of 0:
    let valRes = readUint64(data, pos, addInfo)
    if valRes.isErr():
      return err(valRes.error)
    ok(newIntNode(int64(valRes.get())))
  of 1:
    let valRes = readUint64(data, pos, addInfo)
    if valRes.isErr():
      return err(valRes.error)
    let raw = int64(valRes.get())
    ok(newIntNode(-1 - raw))
  of 2, 3:
    let lenRes = readUint64(data, pos, addInfo)
    if lenRes.isErr():
      return err(lenRes.error)
    let length = int(lenRes.get())
    if length < 0 or pos + length > data.len:
      return err("invalid CBOR string length")
    var tmp = newSeq[byte](length)
    if length > 0:
      for i in 0..<length:
        tmp[i] = data[pos + i]
    pos += length
    if major == 2:
      ok(newBytesNode(tmp))
    else:
      var str = newString(length)
      for i in 0..<length:
        str[i] = char(tmp[i])
      ok(newStringNode(str))
  of 4:
    let lenRes = readUint64(data, pos, addInfo)
    if lenRes.isErr():
      return err(lenRes.error)
    let length = int(lenRes.get())
    if length < 0:
      return err("invalid CBOR array length")
    var items: seq[DagCborNode] = @[]
    items.setLen(length)
    for i in 0..<length:
      let itemRes = decodeNode(data, pos)
      if itemRes.isErr():
        return err(itemRes.error)
      items[i] = itemRes.get()
    ok(newArrayNode(items))
  of 5:
    let lenRes = readUint64(data, pos, addInfo)
    if lenRes.isErr():
      return err(lenRes.error)
    let length = int(lenRes.get())
    if length < 0:
      return err("invalid CBOR map length")
    var entries: seq[(DagCborNode, DagCborNode)] = @[]
    entries.setLen(length)
    for i in 0..<length:
      let keyRes = decodeNode(data, pos)
      if keyRes.isErr():
        return err(keyRes.error)
      let valueRes = decodeNode(data, pos)
      if valueRes.isErr():
        return err(valueRes.error)
      entries[i] = (keyRes.get(), valueRes.get())
    ok(newMapNode(entries))
  of 6:
    decodeTag(data, pos, addInfo)
  of 7:
    decodeSimple(addInfo, data, pos)
  else:
    err("unsupported CBOR major type: " & $major)

proc decodeDagCbor(data: openArray[byte]): Result[DagCborNode, string] =
  var pos = 0
  let nodeRes = decodeNode(data, pos)
  if nodeRes.isErr():
    return err(nodeRes.error)
  if pos != data.len:
    return err("unexpected trailing bytes in CBOR payload")
  ok(nodeRes.get())

proc getMapField(node: DagCborNode, key: string): Option[DagCborNode] =
  if node.kind != dckMap:
    return none(DagCborNode)
  for (k, v) in node.mapVal:
    if k.kind == dckString and k.strVal == key:
      return some(v)
  none(DagCborNode)

proc expectMap(node: DagCborNode, ctx: string): Result[seq[(DagCborNode, DagCborNode)], string] =
  if node.kind != dckMap:
    return err(ctx & " must be a CBOR map")
  ok(node.mapVal)

proc expectSingleEntry(node: DagCborNode, ctx: string): Result[(string, DagCborNode), string] =
  if node.kind != dckMap:
    return err(ctx & " must be a CBOR map")
  if node.mapVal.len != 1:
    return err(ctx & " must be a single-entry map")
  let (keyNode, valueNode) = node.mapVal[0]
  if keyNode.kind != dckString:
    return err(ctx & " key must be a string")
  ok((keyNode.strVal, valueNode))

proc parseLimit(node: DagCborNode): Result[int, string] =
  let entryRes = expectSingleEntry(node, "selector recursion limit")
  if entryRes.isErr():
    return err(entryRes.error)
  let (limitKey, limitVal) = entryRes.get()
  case limitKey
  of SelectorKeyLimitNone:
    ok(-1)
  of SelectorKeyLimitDepth:
    if limitVal.kind != dckInt:
      return err("depth recursion limit must be an integer")
    if limitVal.intVal < 0:
      return err("depth recursion limit cannot be negative")
    if limitVal.intVal > int64(high(int)):
      return err("depth recursion limit exceeds local integer range")
    ok(int(limitVal.intVal))
  else:
    err("unsupported recursion limit key: " & limitKey)

proc hasTraverseAllPath(node: DagCborNode): bool =
  if node.kind != dckMap or node.mapVal.len != 1:
    return false
  let (keyNode, valueNode) = node.mapVal[0]
  if keyNode.kind != dckString:
    return false
  case keyNode.strVal
  of SelectorKeyExploreRecursiveEdge:
    true
  of SelectorKeyExploreAll:
    let nextOpt = getMapField(valueNode, SelectorKeyNext)
    if nextOpt.isNone():
      return false
    hasTraverseAllPath(nextOpt.get())
  of SelectorKeyExploreUnion:
    if valueNode.kind != dckArray:
      return false
    for member in valueNode.arrayVal:
      if hasTraverseAllPath(member):
        return true
    false
  of SelectorKeyExploreRecursive:
    let seqOpt = getMapField(valueNode, SelectorKeySequence)
    if seqOpt.isNone():
      return false
    hasTraverseAllPath(seqOpt.get())
  of SelectorKeyExploreFields:
    let fieldsOpt = getMapField(valueNode, SelectorKeyFields)
    if fieldsOpt.isNone():
      return false
    let fieldsNode = fieldsOpt.get()
    if fieldsNode.kind != dckMap:
      return false
    for (_, fieldSelector) in fieldsNode.mapVal:
      if hasTraverseAllPath(fieldSelector):
        return true
    false
  else:
    false

proc mergeTraversals(
    first: GraphSyncTraversal, second: GraphSyncTraversal
): GraphSyncTraversal =
  if first.mode == gstTraverseAll and second.mode == gstTraverseAll:
    var limit = first.depthLimit
    if limit < 0 or second.depthLimit < 0:
      limit = -1
    else:
      limit = max(first.depthLimit, second.depthLimit)
    GraphSyncTraversal(mode: gstTraverseAll, depthLimit: limit)
  elif first.mode == gstTraverseAll:
    first
  elif second.mode == gstTraverseAll:
    second
  else:
    GraphSyncTraversal(mode: gstTraverseNone, depthLimit: 0)

proc parseExploreRecursive(node: DagCborNode): Result[GraphSyncTraversal, string] =
  let mapRes = expectMap(node, "ExploreRecursive body")
  if mapRes.isErr():
    return err(mapRes.error)

  let limitOpt = getMapField(node, SelectorKeyLimit)
  if limitOpt.isNone():
    return err("ExploreRecursive selector missing limit field")
  let limitRes = parseLimit(limitOpt.get())
  if limitRes.isErr():
    return err(limitRes.error)

  let sequenceOpt = getMapField(node, SelectorKeySequence)
  if sequenceOpt.isNone():
    return err("ExploreRecursive selector missing sequence field")

  if not hasTraverseAllPath(sequenceOpt.get()):
    return err("ExploreRecursive sequence unsupported: missing ExploreAll path to recursion edge")

  ok(GraphSyncTraversal(mode: gstTraverseAll, depthLimit: limitRes.get()))

proc parseSelectorNode(node: DagCborNode): Result[GraphSyncTraversal, string] =
  let entryRes = expectSingleEntry(node, "selector union")
  if entryRes.isErr():
    return err(entryRes.error)
  let (key, valueNode) = entryRes.get()

  case key
  of SelectorKeyExploreRecursive:
    parseExploreRecursive(valueNode)
  of SelectorKeyExploreAll:
    let nextOpt = getMapField(valueNode, SelectorKeyNext)
    if nextOpt.isNone():
      return err("ExploreAll selector missing next field")
    let nextRes = parseSelectorNode(nextOpt.get())
    if nextRes.isErr():
      return err(nextRes.error)
    let nextTraversal = nextRes.get()
    if nextTraversal.mode == gstTraverseAll:
      if nextTraversal.depthLimit < 0:
        return ok(GraphSyncTraversal(mode: gstTraverseAll, depthLimit: -1))
      return ok(
        GraphSyncTraversal(
          mode: gstTraverseAll,
          depthLimit: nextTraversal.depthLimit + 1
        )
      )
    if nextTraversal.depthLimit < 0:
      return ok(GraphSyncTraversal(mode: gstTraverseAll, depthLimit: -1))
    ok(GraphSyncTraversal(mode: gstTraverseAll, depthLimit: 1))
  of SelectorKeyExploreFields:
    let fieldsOpt = getMapField(valueNode, SelectorKeyFields)
    if fieldsOpt.isNone():
      return err("ExploreFields selector missing fields map")
    let fieldsNode = fieldsOpt.get()
    if fieldsNode.kind != dckMap:
      return err("ExploreFields fields entry must be a CBOR map")
    var traversal = GraphSyncTraversal(mode: gstTraverseNone, depthLimit: 0)
    for (_, fieldSelector) in fieldsNode.mapVal:
      let fieldRes = parseSelectorNode(fieldSelector)
      if fieldRes.isErr():
        return err(fieldRes.error)
      traversal = mergeTraversals(traversal, fieldRes.get())
    ok(traversal)
  of SelectorKeyExploreUnion:
    if valueNode.kind != dckArray:
      return err("ExploreUnion selector body must be a CBOR array")
    var traversal = GraphSyncTraversal(mode: gstTraverseNone, depthLimit: 0)
    for member in valueNode.arrayVal:
      let memberRes = parseSelectorNode(member)
      if memberRes.isErr():
        return err(memberRes.error)
      traversal = mergeTraversals(traversal, memberRes.get())
    ok(traversal)
  of SelectorKeyMatcher:
    ok(GraphSyncTraversal(mode: gstTraverseNone, depthLimit: 0))
  of SelectorKeyExploreRecursiveEdge:
    err("ExploreRecursiveEdge selector cannot appear outside ExploreRecursive context")
  else:
    err("unsupported selector type: " & key)

proc parseSelectorEnvelope(node: DagCborNode): Result[GraphSyncTraversal, string] =
  if node.kind != dckMap:
    return err("selector payload must be a CBOR map")
  let innerOpt = getMapField(node, SelectorKeyEnvelope)
  if innerOpt.isSome():
    return parseSelectorNode(innerOpt.get())
  parseSelectorNode(node)

proc parseSelector*(selector: seq[byte]): Result[GraphSyncTraversal, string] =
  if selector.len == 0:
    return ok(GraphSyncTraversal(mode: gstTraverseAll, depthLimit: -1))

  let decodeRes = decodeDagCbor(selector)
  if decodeRes.isErr():
    return err("failed to decode selector: " & decodeRes.error)

  parseSelectorEnvelope(decodeRes.get())

{.pop.}
{.pop.}
