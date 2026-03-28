import std/[options, strutils, tables]

import ./state
import ../types

proc normalizeName*(name: string): string =
  name.strip().toLowerAscii()

proc validateNameSyntax(name: string) =
  if name.len == 0:
    raise newException(ValueError, "name is empty")
  if name.len > 253:
    raise newException(ValueError, "name is too long")
  let labels = name.split('.')
  if labels.len == 0:
    raise newException(ValueError, "name must include at least one label")
  for label in labels:
    if label.len == 0 or label.len > 63:
      raise newException(ValueError, "invalid name label length")
    if label[0] == '-' or label[^1] == '-':
      raise newException(ValueError, "name labels cannot start or end with hyphen")
    for ch in label:
      if not (ch in {'a'..'z'} or ch in {'0'..'9'} or ch == '-'):
        raise newException(ValueError, "name contains invalid characters")

proc refreshNameStatus*(record: var NameRecord, nowTs: int64) =
  if record.frozen:
    record.status = nsFrozen
  elif nowTs <= record.expiresAt:
    record.status = nsActive
  elif nowTs <= record.graceUntil:
    record.status = nsGrace
  else:
    record.status = nsExpired

proc applyNameRegister*(
    state: ChainState,
    name: string,
    owner: string,
    targetAccount: string,
    targetContentId: string,
    resolvedCid: string,
    nowTs: int64,
    ttlSeconds: int64,
) =
  let normalized = normalizeName(name)
  validateNameSyntax(normalized)
  if targetAccount.len == 0 and targetContentId.len == 0 and resolvedCid.len == 0:
    raise newException(ValueError, "name registration requires a resolution target")
  if state.nameRecords.hasKey(normalized):
    var existing = state.nameRecords[normalized]
    existing.refreshNameStatus(nowTs)
    if existing.status in {nsActive, nsGrace, nsFrozen}:
      raise newException(ValueError, "name already reserved: " & normalized)
  let ttl = if ttlSeconds > 0: ttlSeconds else: state.params.nameTtlSeconds
  let record = NameRecord(
    name: normalized,
    owner: owner,
    targetAccount: targetAccount,
    targetContentId: targetContentId,
    resolvedCid: resolvedCid,
    registeredAt: nowTs,
    expiresAt: nowTs + ttl,
    graceUntil: nowTs + ttl + state.params.nameGraceSeconds,
    status: nsActive,
    frozen: false,
  )
  state.nameRecords[normalized] = record

proc applyNameRenew*(state: ChainState, name: string, owner: string, nowTs: int64, ttlSeconds: int64) =
  let normalized = normalizeName(name)
  if not state.nameRecords.hasKey(normalized):
    raise newException(ValueError, "unknown name: " & normalized)
  var record = state.nameRecords[normalized]
  record.refreshNameStatus(nowTs)
  if record.owner != owner:
    raise newException(ValueError, "only owner can renew")
  if record.frozen:
    raise newException(ValueError, "name is frozen")
  if record.status == nsExpired:
    raise newException(ValueError, "grace window expired")
  let ttl = if ttlSeconds > 0: ttlSeconds else: state.params.nameTtlSeconds
  let base = max(record.expiresAt, nowTs)
  record.expiresAt = base + ttl
  record.graceUntil = record.expiresAt + state.params.nameGraceSeconds
  record.status = nsActive
  state.nameRecords[normalized] = record

proc applyNameTransfer*(
    state: ChainState,
    name: string,
    owner: string,
    newOwner: string,
    targetAccount: string,
    nowTs: int64,
) =
  let normalized = normalizeName(name)
  if not state.nameRecords.hasKey(normalized):
    raise newException(ValueError, "unknown name: " & normalized)
  var record = state.nameRecords[normalized]
  record.refreshNameStatus(nowTs)
  if record.owner != owner:
    raise newException(ValueError, "only owner can transfer")
  if record.status notin {nsActive, nsGrace}:
    raise newException(ValueError, "name is not transferable")
  if record.frozen:
    raise newException(ValueError, "name is frozen")
  if newOwner.len == 0:
    raise newException(ValueError, "new owner is required")
  record.owner = newOwner
  if targetAccount.len > 0:
    record.targetAccount = targetAccount
  record.status = nsActive
  state.nameRecords[normalized] = record

proc resolveNameSnapshot*(state: ChainState, name: string, nowTs: int64): Option[NameRecordSnapshot] =
  let normalized = normalizeName(name)
  if not state.nameRecords.hasKey(normalized):
    return none(NameRecordSnapshot)
  var record = state.nameRecords[normalized]
  record.refreshNameStatus(nowTs)
  state.nameRecords[normalized] = record
  some(NameRecordSnapshot(
    name: record.name,
    owner: record.owner,
    targetAccount: record.targetAccount,
    targetContentId: record.targetContentId,
    resolvedCid: record.resolvedCid,
    expiresAt: record.expiresAt,
    graceUntil: record.graceUntil,
    status: record.status,
  ))
