import std/[options, os, strutils]

import pebble/kv

import ../codec
import ../types
import ../execution/state

type
  ChainStore* = ref object
    root*: string
    kv*: PebbleKV

proc keyHeight(height: uint64): string =
  align($height, 20, '0')

proc eventKey(prefix, suffix: string): string =
  prefix & ":" & suffix

proc blobTicketReadKey(manifestCid, ticketId, reader: string): string =
  "blobticketreads:" & manifestCid & ":" & ticketId & ":" & reader

proc newChainStore*(root: string): ChainStore =
  createDir(root)
  let kvRoot = root / "pebble"
  createDir(kvRoot)
  ChainStore(root: root, kv: newPebbleKV(kvRoot))

proc close*(store: ChainStore) =
  if not store.isNil and not store.kv.isNil:
    store.kv.close()

proc putJson[T](store: ChainStore, space: KeySpace, key: string, value: T) =
  store.kv.put(space, key, encodeObj(value))

proc getJson[T](store: ChainStore, space: KeySpace, key: string): Option[T] =
  let payload = store.kv.get(space, key)
  if payload.isNone():
    return none(T)
  some(decodeObj[T](payload.get()))

proc persistTx*(store: ChainStore, tx: Tx) =
  store.putJson(ksTask, "tx:" & tx.txId, tx)

proc persistBatch*(store: ChainStore, batch: Batch) =
  store.putJson(ksEvent, eventKey("batch", keyHeight(batch.round) & ":" & batch.batchId), batch)
  store.putJson(ksTask, "batch:" & batch.batchId, batch)
  store.putJson(ksTask, "batchround:" & batch.proposer & ":" & keyHeight(batch.round), batch)

proc persistVote*(store: ChainStore, vote: BatchVote) =
  store.putJson(ksTask, "vote:" & vote.batchId & ":" & vote.vote.voter, vote)

proc persistCertificate*(store: ChainStore, cert: BatchCertificate) =
  store.putJson(
    ksEvent,
    eventKey("cert", keyHeight(cert.round) & ":" & cert.certificateId),
    cert,
  )
  store.putJson(ksTask, "cert:" & cert.certificateId, cert)
  store.putJson(ksTask, "certbatch:" & cert.batchId, cert)

proc persistAudit*(store: ChainStore, event: AuditEvent) =
  store.putJson(
    ksEvent,
    eventKey("audit", $event.createdAt & ":" & event.eventId),
    event,
  )

proc persistCheckpoint*(store: ChainStore, snapshot: ChainStateSnapshot) =
  store.putJson(ksTask, "checkpoint:" & keyHeight(snapshot.height), snapshot)
  store.kv.put(ksIndex, "checkpoint:latest", keyHeight(snapshot.height))

proc persistBlock*(store: ChainStore, blk: FinalizedBlock, snapshot: ChainStateSnapshot) =
  store.putJson(ksEvent, eventKey("block", keyHeight(blk.height) & ":" & blk.blockId), blk)
  store.putJson(ksTask, "block:" & keyHeight(blk.height), blk)
  store.persistCheckpoint(snapshot)

proc loadLatestCheckpoint*(store: ChainStore): Option[ChainStateSnapshot] =
  let latest = store.kv.get(ksIndex, "checkpoint:latest")
  if latest.isNone():
    return none(ChainStateSnapshot)
  getJson[ChainStateSnapshot](store, ksTask, "checkpoint:" & latest.get())

proc loadBlocksAfter*(store: ChainStore, height: uint64): seq[FinalizedBlock] =
  let page = store.kv.scanPrefix(ksEvent, prefix = "block:", limit = 0)
  for item in page.entries:
    let blk = decodeObj[FinalizedBlock](item.value)
    if blk.height > height:
      result.add(blk)

proc loadBlock*(store: ChainStore, height: uint64): Option[FinalizedBlock] =
  getJson[FinalizedBlock](store, ksTask, "block:" & keyHeight(height))

proc loadBatch*(store: ChainStore, batchId: string): Option[Batch] =
  getJson[Batch](store, ksTask, "batch:" & batchId)

proc loadBatchByRound*(store: ChainStore, proposer: string, round: uint64): Option[Batch] =
  getJson[Batch](store, ksTask, "batchround:" & proposer & ":" & keyHeight(round))

proc loadVote*(store: ChainStore, batchId, voter: string): Option[BatchVote] =
  getJson[BatchVote](store, ksTask, "vote:" & batchId & ":" & voter)

proc loadCertificate*(store: ChainStore, certificateId: string): Option[BatchCertificate] =
  getJson[BatchCertificate](store, ksTask, "cert:" & certificateId)

proc loadCertificateByBatch*(store: ChainStore, batchId: string): Option[BatchCertificate] =
  getJson[BatchCertificate](store, ksTask, "certbatch:" & batchId)

proc loadTx*(store: ChainStore, txId: string): Option[Tx] =
  getJson[Tx](store, ksTask, "tx:" & txId)

proc storeBlobHead*(store: ChainStore, manifest: ContentManifest) =
  store.putJson(ksTask, "blobhead:" & manifest.manifestCid, manifest)

proc loadBlobHead*(store: ChainStore, manifestCid: string): Option[ContentManifest] =
  getJson[ContentManifest](store, ksTask, "blobhead:" & manifestCid)

proc storeBlobChunk*(store: ChainStore, manifestCid: string, index: int, bytes: seq[byte]) =
  store.kv.putBytes(ksTask, "blobchunk:" & manifestCid & ":" & $index, bytes)

proc loadBlobChunk*(store: ChainStore, manifestCid: string, index: int): Option[seq[byte]] =
  store.kv.getBytes(ksTask, "blobchunk:" & manifestCid & ":" & $index)

proc blobTicketReadCount*(store: ChainStore, manifestCid, ticketId, reader: string): int {.raises: [].} =
  try:
    let payload = store.kv.get(ksIndex, blobTicketReadKey(manifestCid, ticketId, reader))
    if payload.isNone():
      return 0
    try:
      parseInt(payload.get())
    except ValueError:
      return 0
  except Exception:
    return 0

proc recordBlobTicketRead*(store: ChainStore, manifestCid, ticketId, reader: string): int {.raises: [].} =
  try:
    result = store.blobTicketReadCount(manifestCid, ticketId, reader) + 1
    store.kv.put(ksIndex, blobTicketReadKey(manifestCid, ticketId, reader), $result)
  except Exception:
    result = 0

proc auditLog*(store: ChainStore, category = ""): seq[AuditEvent] =
  let page = store.kv.scanPrefix(ksEvent, prefix = "audit:", limit = 0)
  for item in page.entries:
    let event = decodeObj[AuditEvent](item.value)
    if category.len == 0 or event.category == category:
      result.add(event)
