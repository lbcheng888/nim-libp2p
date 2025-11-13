import std/[json, options, sequtils, strutils, tables, times]
import nimcrypto/sha2
import stew/byteutils

const
  EventsTopic* = "bridge.events"
  SignaturesTopic* = "bridge.signatures"
  EventSchemaVersion* = 1

type
  BridgeMode* = enum
    bmWatcher, bmSigner, bmExecutor

  LockEventMessage* = object
    schemaVersion*: int
    eventId*: string
    watcherPeer*: string
    asset*: string
    amount*: float
    sourceChain*: string
    targetChain*: string
    sourceHeight*: int64
    targetHeight*: int64
    proofKey*: string
    proofBlob*: string
    proofDigest*: string
    eventHash*: string

  SignatureMessage* = object
    eventId*: string
    signerPeer*: string
    proofKey*: string
    proofDigest*: string
    signature*: string

  EventStatus* = enum
    esPending, esSigned, esExecuted

  EventRecord* = object
    event*: LockEventMessage
    status*: EventStatus
    signatures*: OrderedTable[string, SignatureMessage]
    updatedAt*: int64

proc nowEpoch*(): int64 =
  now().toTime().toUnix()

proc eventToJson*(ev: LockEventMessage): JsonNode =
  %*{
    "schemaVersion": ev.schemaVersion,
    "eventId": ev.eventId,
    "watcherPeer": ev.watcherPeer,
    "asset": ev.asset,
    "amount": ev.amount,
    "sourceChain": ev.sourceChain,
    "targetChain": ev.targetChain,
    "sourceHeight": ev.sourceHeight,
    "targetHeight": ev.targetHeight,
    "proofKey": ev.proofKey,
    "proofBlob": ev.proofBlob,
    "proofDigest": ev.proofDigest,
    "eventHash": ev.eventHash,
  }

proc signatureToJson*(sig: SignatureMessage): JsonNode =
  %*{
    "eventId": sig.eventId,
    "signerPeer": sig.signerPeer,
    "proofKey": sig.proofKey,
    "proofDigest": sig.proofDigest,
    "signature": sig.signature,
  }

proc eventFromJson*(node: JsonNode): Option[LockEventMessage] =
  if node.kind != JObject or not node.hasKey("eventId"):
    return none(LockEventMessage)
  let ev = LockEventMessage(
    schemaVersion:
      if node.hasKey("schemaVersion"): node["schemaVersion"].getInt()
      else: EventSchemaVersion,
    eventId: node["eventId"].getStr(),
    watcherPeer: node["watcherPeer"].getStr(),
    asset: node["asset"].getStr(),
    amount: node["amount"].getFloat(),
    sourceChain: node["sourceChain"].getStr(),
    targetChain: node["targetChain"].getStr(),
    sourceHeight: if node.hasKey("sourceHeight"): node["sourceHeight"].getInt() else: 0,
    targetHeight: if node.hasKey("targetHeight"): node["targetHeight"].getInt() else: 0,
    proofKey: node["proofKey"].getStr(),
    proofBlob: node["proofBlob"].getStr(),
    proofDigest:
      if node.hasKey("proofDigest"): node["proofDigest"].getStr()
      else: "",
    eventHash:
      if node.hasKey("eventHash"): node["eventHash"].getStr() else: "",
  )
  some(ev)

proc signatureFromJson*(node: JsonNode): Option[SignatureMessage] =
  if node.kind != JObject or not node.hasKey("eventId"):
    return none(SignatureMessage)
  some(
    SignatureMessage(
      eventId: node["eventId"].getStr(),
      signerPeer: node["signerPeer"].getStr(),
      proofKey: node["proofKey"].getStr(),
      proofDigest: node["proofDigest"].getStr(),
      signature: node["signature"].getStr(),
    )
  )

proc encodeEvent*(ev: LockEventMessage): seq[byte] =
  ($eventToJson(ev)).toBytes()

proc decodeEvent*(data: seq[byte]): Option[LockEventMessage] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    eventFromJson(parsed)
  except CatchableError:
    none(LockEventMessage)

proc encodeSignature*(sig: SignatureMessage): seq[byte] =
  ($signatureToJson(sig)).toBytes()

proc decodeSignature*(data: seq[byte]): Option[SignatureMessage] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    signatureFromJson(parsed)
  except CatchableError:
    none(SignatureMessage)

proc computeEventHash*(ev: LockEventMessage): string =
  let payload = ($eventToJson(
    LockEventMessage(
      schemaVersion: ev.schemaVersion,
      eventId: ev.eventId,
      watcherPeer: ev.watcherPeer,
      asset: ev.asset,
      amount: ev.amount,
      sourceChain: ev.sourceChain,
      targetChain: ev.targetChain,
      sourceHeight: ev.sourceHeight,
      targetHeight: ev.targetHeight,
      proofKey: ev.proofKey,
      proofBlob: ev.proofBlob,
      proofDigest: ev.proofDigest,
      eventHash: "",
    )
  )).toBytes()
  let digest = sha256.digest(payload)
  result = digest.data.toHex()

proc recordToJson*(rec: EventRecord): JsonNode =
  var sigList: seq[JsonNode]
  for sig in rec.signatures.values:
    sigList.add(signatureToJson(sig))
  %*{
    "event": eventToJson(rec.event),
    "status": ord(rec.status),
    "signatures": sigList,
    "updatedAt": rec.updatedAt,
  }

proc recordFromJson*(node: JsonNode): Option[EventRecord] =
  if node.kind != JObject or not node.hasKey("event"):
    return none(EventRecord)
  let eventOpt = eventFromJson(node["event"])
  if eventOpt.isNone():
    return none(EventRecord)
  var sigTable = initOrderedTable[string, SignatureMessage]()
  if node.hasKey("signatures") and node["signatures"].kind == JArray:
    for child in node["signatures"]:
      let sigOpt = signatureFromJson(child)
      if sigOpt.isSome():
        let sig = sigOpt.get()
        sigTable[sig.signerPeer] = sig
  let statusVal =
    if node.hasKey("status"):
      node["status"].getInt()
    else:
      0
  if statusVal < ord(low(EventStatus)) or statusVal > ord(high(EventStatus)):
    return none(EventRecord)
  let updatedVal =
    if node.hasKey("updatedAt"): node["updatedAt"].getInt()
    else: nowEpoch()
  some(
    EventRecord(
      event: eventOpt.get(),
      status: EventStatus(statusVal),
      signatures: sigTable,
      updatedAt: updatedVal,
    )
  )
