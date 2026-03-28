import std/[json, jsonutils, options]
import nimcrypto/[hash, sha2]
import nimcrypto/utils

import ./types

proc bytesOf(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc jsonString*(node: JsonNode): string =
  $node

proc hashHex*(payload: openArray[byte]): string =
  var ctx: sha256
  ctx.init()
  if payload.len > 0:
    ctx.update(payload)
  let digest = ctx.finish()
  toHex(digest.data)

proc hashHex*(payload: string): string =
  hashHex(bytesOf(payload))

proc encodeObj*[T](value: T): string =
  jsonString(toJson(value))

proc decodeObj*[T](payload: string): T =
  jsonTo(parseJson(payload), T)

proc txSigningNode*(tx: Tx): JsonNode =
  result = newJObject()
  result["kind"] = %slug(tx.kind)
  result["sender"] = %tx.sender
  result["senderPublicKey"] = %tx.senderPublicKey
  result["nonce"] = %tx.nonce
  result["epoch"] = %tx.epoch
  result["timestamp"] = %tx.timestamp
  result["payload"] = tx.payload

proc txSigningBytes*(tx: Tx): seq[byte] =
  bytesOf(jsonString(txSigningNode(tx)))

proc computeTxId*(tx: Tx): string =
  hashHex(txSigningBytes(tx))

proc batchSigningNode*(batch: Batch): JsonNode =
  result = newJObject()
  result["proposer"] = %batch.proposer
  result["proposerPublicKey"] = %batch.proposerPublicKey
  result["epoch"] = %batch.epoch
  result["round"] = %batch.round
  result["parents"] = %batch.parents
  result["txIds"] = %batch.txIds
  result["createdAt"] = %batch.createdAt

proc batchSigningBytes*(batch: Batch): seq[byte] =
  bytesOf(jsonString(batchSigningNode(batch)))

proc computeBatchId*(batch: Batch): string =
  hashHex(batchSigningBytes(batch))

proc certificateSigningNode*(cert: BatchCertificate): JsonNode =
  result = newJObject()
  result["batchId"] = %cert.batchId
  result["proposer"] = %cert.proposer
  result["epoch"] = %cert.epoch
  result["round"] = %cert.round
  result["batchHash"] = %cert.batchHash
  result["quorumWeight"] = %cert.quorumWeight
  result["votes"] = toJson(cert.votes)

proc certificateSigningBytes*(cert: BatchCertificate): seq[byte] =
  bytesOf(jsonString(certificateSigningNode(cert)))

proc computeCertificateId*(cert: BatchCertificate): string =
  hashHex(certificateSigningBytes(cert))

proc checkpointSigningNode*(checkpoint: Checkpoint): JsonNode =
  result = newJObject()
  result["height"] = %checkpoint.height
  result["epoch"] = %checkpoint.epoch
  result["stateRoot"] = %checkpoint.stateRoot
  result["validatorRoot"] = %checkpoint.validatorRoot
  result["createdAt"] = %checkpoint.createdAt

proc computeCheckpointId*(checkpoint: Checkpoint): string =
  hashHex(jsonString(checkpointSigningNode(checkpoint)))

proc blockSigningNode*(blk: FinalizedBlock): JsonNode =
  result = newJObject()
  result["height"] = %blk.height
  result["epoch"] = %blk.epoch
  result["round"] = %blk.round
  result["leaderBatchId"] = %blk.leaderBatchId
  result["orderedBatchIds"] = %blk.orderedBatchIds
  result["stateRoot"] = %blk.stateRoot
  result["checkpointId"] = %blk.checkpoint.checkpointId
  result["createdAt"] = %blk.createdAt

proc computeBlockId*(blk: FinalizedBlock): string =
  hashHex(jsonString(blockSigningNode(blk)))

proc eventNode*(event: AuditEvent): JsonNode =
  result = newJObject()
  result["category"] = %event.category
  result["subjectId"] = %event.subjectId
  result["action"] = %event.action
  result["payload"] = event.payload
  result["createdAt"] = %event.createdAt

proc computeEventId*(event: AuditEvent): string =
  hashHex(jsonString(eventNode(event)))
