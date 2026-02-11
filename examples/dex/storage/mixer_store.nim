import std/[json, options, os, tables, times, strutils, sequtils]
import chronos
import stew/byteutils
import pebble/wal/[writer as pebble_wal_writer, reader as pebble_wal_reader, types as pebble_wal_types]
import pebble/runtime/resource_manager as pebble_rm
import ../types

const
  DefaultMixerWal = "mixer.wal"
  WalVersion = 1

type
  MixerWalKind = enum
    mwkSessionUpdate, mwkSettlement

  MixerWalEvent = object
    kind: MixerWalKind
    payload: JsonNode
    timestamp: int64

  MixerSessionRecord* = object
    sessionId*: string
    role*: string # "idle", "coordinator", "participant"
    participants*: seq[string]
    intents*: Table[string, MixerIntent]
    commitments*: Table[string, string]
    signatures*: Table[string, string]
    mySecret*: string # Hex encoded
    status*: string
    updatedAt*: int64

  MixerStore* = ref object
    wal: pebble_wal_writer.WalWriter
    walPath: string
    lock: AsyncLock
    # In-memory cache
    currentSession*: Option[MixerSessionRecord]
    settlements*: seq[SettlementProof]

proc nowMs(): int64 =
  int64(epochTime() * 1000.0)

proc ensureDir(path: string) =
  if path.len == 0: return
  if not dirExists(path): createDir(path)

# --- Serialization Helpers ---

proc intentToJson(i: MixerIntent): JsonNode =
  %*{
    "sessionHint": i.sessionHint,
    "targetAsset": encodeAssetDef(i.targetAsset),
    "amount": i.amount,
    "maxDelayMs": i.maxDelayMs,
    "proofDigest": i.proofDigest,
    "hopCount": i.hopCount,
    "initiatorPk": i.initiatorPk,
    "signature": i.signature
  }

proc jsonToIntent(n: JsonNode): MixerIntent =
  MixerIntent(
    sessionHint: n["sessionHint"].getBiggestInt().uint64,
    targetAsset: decodeAssetDef(n["targetAsset"]),
    amount: n["amount"].getFloat(),
    maxDelayMs: n["maxDelayMs"].getBiggestInt().uint32,
    proofDigest: n["proofDigest"].getStr(),
    hopCount: n["hopCount"].getInt().uint8,
    initiatorPk: n["initiatorPk"].getStr(),
    signature: n["signature"].getStr()
  )

proc sessionToJson(s: MixerSessionRecord): JsonNode =
  var intentsJson = newJObject()
  for k, v in s.intents:
    intentsJson[k] = intentToJson(v)

  var commitsJson = newJObject()
  for k, v in s.commitments:
    commitsJson[k] = %v

  var sigsJson = newJObject()
  for k, v in s.signatures:
    sigsJson[k] = %v
  
  %*{
    "sessionId": s.sessionId,
    "role": s.role,
    "participants": %s.participants,
    "intents": intentsJson,
    "commitments": commitsJson,
    "signatures": sigsJson,
    "mySecret": s.mySecret,
    "status": s.status,
    "updatedAt": s.updatedAt
  }

proc jsonToSession(n: JsonNode): MixerSessionRecord =
  var intents = initTable[string, MixerIntent]()
  if n.hasKey("intents"):
    for k, v in n["intents"]:
      intents[k] = jsonToIntent(v)

  var commits = initTable[string, string]()
  if n.hasKey("commitments"):
    for k, v in n["commitments"]:
      commits[k] = v.getStr()

  var sigs = initTable[string, string]()
  if n.hasKey("signatures"):
    for k, v in n["signatures"]:
      sigs[k] = v.getStr()
  
  MixerSessionRecord(
    sessionId: n["sessionId"].getStr(),
    role: n["role"].getStr(),
    participants: n["participants"].getElems().mapIt(it.getStr()),
    intents: intents,
    commitments: commits,
    signatures: sigs,
    mySecret: n["mySecret"].getStr(),
    status: n["status"].getStr(),
    updatedAt: n["updatedAt"].getBiggestInt()
  )

proc settlementToJson(s: SettlementProof): JsonNode =
  %*{
    "sessionId": s.sessionId,
    "chain": s.chain,
    "txId": s.txId,
    "proofDigest": s.proofDigest,
    "timestamp": s.timestamp
  }

proc jsonToSettlement(n: JsonNode): SettlementProof =
  let chainStr = n["chain"].getStr()
  let chain = try: parseEnum[ChainId](chainStr) except: ChainBTC
  SettlementProof(
    sessionId: n["sessionId"].getStr(),
    chain: chain,
    txId: n["txId"].getStr(),
    proofDigest: n["proofDigest"].getStr(),
    timestamp: n["timestamp"].getBiggestInt()
  )

proc walEventToJson(e: MixerWalEvent): JsonNode =
  %*{
    "version": WalVersion,
    "timestamp": e.timestamp,
    "kind": $e.kind,
    "payload": e.payload
  }

# --- WAL Operations ---

proc appendWal(store: MixerStore, event: MixerWalEvent) =
  if store.wal.isNil: return
  try:
    let node = walEventToJson(event)
    discard store.wal.append(($node).toBytes())
  except CatchableError:
    discard

proc applyEvent(store: MixerStore, event: MixerWalEvent) =
  case event.kind
  of mwkSessionUpdate:
    store.currentSession = some(jsonToSession(event.payload))
  of mwkSettlement:
    store.settlements.add(jsonToSettlement(event.payload))

proc replayWal(store: MixerStore) =
  if not fileExists(store.walPath): return
  var reader = pebble_wal_reader.newWalReader(store.walPath)
  defer: reader.close()
  
  while true:
    let entry = reader.readNext()
    if entry.isNone: break
    try:
      let node = parseJson(string.fromBytes(entry.get().payload))
      let kindStr = node["kind"].getStr()
      var kind: MixerWalKind
      if kindStr == "mwkSessionUpdate": kind = mwkSessionUpdate
      elif kindStr == "mwkSettlement": kind = mwkSettlement
      else: continue
      
      let event = MixerWalEvent(
        kind: kind,
        payload: node["payload"],
        timestamp: node["timestamp"].getBiggestInt()
      )
      store.applyEvent(event)
    except CatchableError:
      continue

# --- Public API ---

proc initMixerStore*(dataDir: string): MixerStore =
  ensureDir(dataDir)
  let walPath = dataDir / DefaultMixerWal
  let wal = pebble_wal_writer.newWalWriter(
    path = walPath,
    format = pebble_wal_types.wireRecyclable,
    maxSegmentBytes = 1 * 1024 * 1024, # 1MB segments
    autoSyncBytes = 4 * 1024
  )
  
  new(result)
  result.wal = wal
  result.walPath = walPath
  result.lock = newAsyncLock()
  result.settlements = @[]
  result.replayWal()

proc close*(store: MixerStore) =
  if not store.wal.isNil:
    store.wal.close()
    store.wal = nil

proc saveSession*(store: MixerStore, session: MixerSessionRecord) {.async: (raises: [Exception]).} =
  await store.lock.acquire()
  defer: store.lock.release()
  
  store.currentSession = some(session)
  try:
    store.appendWal(MixerWalEvent(
      kind: mwkSessionUpdate,
      payload: sessionToJson(session),
      timestamp: nowMs()
    ))
  except CatchableError:
    # Log or ignore failure to persist, don't crash flow
    discard

proc saveSettlement*(store: MixerStore, proof: SettlementProof) {.async: (raises: [Exception]).} =
  await store.lock.acquire()
  defer: store.lock.release()
  
  store.settlements.add(proof)
  try:
    store.appendWal(MixerWalEvent(
      kind: mwkSettlement,
      payload: settlementToJson(proof),
      timestamp: nowMs()
    ))
  except CatchableError:
    discard

proc getSession*(store: MixerStore): Option[MixerSessionRecord] =
  store.currentSession

