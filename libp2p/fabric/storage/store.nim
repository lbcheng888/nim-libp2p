import std/[options, os, strutils]

import pebble/kv

import ../../rwad/codec
import ../../rwad/execution/state
import ../polar
import ../types

type
  FabricStore* = ref object
    root*: string
    kv*: PebbleKV

proc bytesOf(value: string): seq[byte] {.gcsafe.} =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc stringOf(data: openArray[byte]): string {.gcsafe.} =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc keyHeight(height: uint64): string =
  align($height, 20, '0')

proc newFabricStore*(root: string): FabricStore =
  createDir(root)
  let kvRoot = root / "pebble"
  createDir(kvRoot)
  FabricStore(root: root, kv: newPebbleKV(kvRoot))

proc close*(store: FabricStore) =
  if not store.isNil and not store.kv.isNil:
    store.kv.close()

proc putJson[T](store: FabricStore, space: KeySpace, key: string, value: T) =
  store.kv.put(space, key, encodeObj(value))

proc getJson[T](store: FabricStore, space: KeySpace, key: string): Option[T] =
  let payload = store.kv.get(space, key)
  if payload.isNone():
    return none(T)
  some(decodeObj[T](payload.get()))

proc persistEvent*(store: FabricStore, event: FabricEvent) =
  when defined(fabric_submit_diag):
    stderr.writeLine(
      "store-stage event begin id=", event.eventId,
      " era=", $event.clock.era,
      " tick=", $event.clock.tick,
    )
  store.putJson(ksEvent, "event:" & keyHeight(event.clock.era) & ":" & event.eventId, event)
  when defined(fabric_submit_diag):
    stderr.writeLine("store-stage event ksEvent id=", event.eventId)
  store.putJson(ksTask, "event:" & event.eventId, event)
  when defined(fabric_submit_diag):
    stderr.writeLine("store-stage event ksTask id=", event.eventId)

proc persistAttestation*(store: FabricStore, att: EventAttestation) =
  store.putJson(
    ksEvent,
    "attestation:" & att.eventId & ":" & $att.role & ":" & att.signer,
    att,
  )

proc persistEventCertificate*(store: FabricStore, cert: EventCertificate) =
  store.putJson(ksEvent, "eventcert:" & keyHeight(cert.clock.era) & ":" & cert.eventId, cert)
  store.putJson(ksTask, "eventcert:" & cert.eventId, cert)

proc persistIsolation*(store: FabricStore, record: IsolationRecord) =
  store.putJson(ksEvent, "isolation:" & record.eventId, record)

proc persistCheckpointCandidate*(store: FabricStore, candidate: CheckpointCandidate) =
  store.putJson(
    ksEvent,
    "checkpoint_candidate:" & keyHeight(candidate.era) & ":" & candidate.candidateId,
    candidate,
  )
  store.putJson(ksTask, "checkpoint_candidate:" & candidate.candidateId, candidate)

proc persistCheckpointVote*(store: FabricStore, vote: CheckpointVote) =
  store.putJson(ksEvent, "checkpoint_vote:" & vote.candidateId & ":" & vote.validator, vote)

proc persistCheckpoint*(store: FabricStore, cert: CheckpointCertificate, snapshot: ChainStateSnapshot) =
  store.putJson(ksEvent, "checkpoint:" & keyHeight(cert.candidate.era) & ":" & cert.checkpointId, cert)
  store.putJson(ksTask, "checkpoint:" & cert.checkpointId, cert)
  store.kv.putBytes(ksTask, "snapshot:" & cert.checkpointId, encodePolarSnapshot(snapshot))
  store.kv.put(ksIndex, "checkpoint:latest", cert.checkpointId)

proc persistCheckpointRaw*(store: FabricStore, cert: CheckpointCertificate, checkpointRaw, snapshotRaw: seq[byte]) {.gcsafe.} =
  let checkpointText = stringOf(checkpointRaw)
  store.kv.put(ksEvent, "checkpoint:" & keyHeight(cert.candidate.era) & ":" & cert.checkpointId, checkpointText)
  store.kv.put(ksTask, "checkpoint:" & cert.checkpointId, checkpointText)
  store.kv.putBytes(ksTask, "snapshot:" & cert.checkpointId, snapshotRaw)
  store.kv.put(ksIndex, "checkpoint:latest", cert.checkpointId)

proc persistAvoProposal*(store: FabricStore, proposal: AvoProposal) =
  store.putJson(ksTask, "avo_proposal:" & proposal.proposalId, proposal)

proc persistAvoApproval*(store: FabricStore, proposalId, validator: string) =
  store.kv.put(ksTask, "avo_approval:" & proposalId & ":" & validator, validator)

proc persistAvoAdoption*(store: FabricStore, adoption: AvoAdoptionRecord) =
  store.putJson(ksTask, "avo_adoption:" & adoption.proposalId, adoption)

proc loadEvent*(store: FabricStore, eventId: string): Option[FabricEvent] =
  getJson[FabricEvent](store, ksTask, "event:" & eventId)

proc loadEventCertificate*(store: FabricStore, eventId: string): Option[EventCertificate] =
  getJson[EventCertificate](store, ksTask, "eventcert:" & eventId)

proc loadLatestCheckpoint*(store: FabricStore): Option[CheckpointCertificate] =
  let latest = store.kv.get(ksIndex, "checkpoint:latest")
  if latest.isNone():
    return none(CheckpointCertificate)
  getJson[CheckpointCertificate](store, ksTask, "checkpoint:" & latest.get())

proc loadCheckpoint*(store: FabricStore, checkpointId: string): Option[CheckpointCertificate] =
  getJson[CheckpointCertificate](store, ksTask, "checkpoint:" & checkpointId)

proc loadCheckpointSnapshot*(store: FabricStore, checkpointId: string): Option[ChainStateSnapshot] =
  let payload = store.kv.getBytes(ksTask, "snapshot:" & checkpointId)
  if payload.isNone():
    return none(ChainStateSnapshot)
  some(decodePolarSnapshot(payload.get()))

proc loadEventRaw*(store: FabricStore, eventId: string): Option[seq[byte]] {.gcsafe.} =
  let payload = store.kv.get(ksTask, "event:" & eventId)
  if payload.isNone():
    return none(seq[byte])
  some(bytesOf(payload.get()))

proc loadAttestationRaw*(
    store: FabricStore, eventId: string, role: AttestationRole, signer: string
): Option[seq[byte]] {.gcsafe.} =
  let payload = store.kv.get(
    ksEvent,
    "attestation:" & eventId & ":" & $role & ":" & signer,
  )
  if payload.isNone():
    return none(seq[byte])
  some(bytesOf(payload.get()))

proc loadEventCertificateRaw*(store: FabricStore, eventId: string): Option[seq[byte]] {.gcsafe.} =
  let payload = store.kv.get(ksTask, "eventcert:" & eventId)
  if payload.isNone():
    return none(seq[byte])
  some(bytesOf(payload.get()))

proc loadCheckpointCandidateRaw*(
    store: FabricStore, candidateId: string
): Option[seq[byte]] {.gcsafe.} =
  let payload = store.kv.get(ksTask, "checkpoint_candidate:" & candidateId)
  if payload.isNone():
    return none(seq[byte])
  some(bytesOf(payload.get()))

proc loadCheckpointVoteRaw*(
    store: FabricStore, candidateId, validator: string
): Option[seq[byte]] {.gcsafe.} =
  let payload = store.kv.get(ksEvent, "checkpoint_vote:" & candidateId & ":" & validator)
  if payload.isNone():
    return none(seq[byte])
  some(bytesOf(payload.get()))

proc loadCheckpointRaw*(store: FabricStore, checkpointId: string): Option[seq[byte]] {.gcsafe.} =
  let payload = store.kv.get(ksTask, "checkpoint:" & checkpointId)
  if payload.isNone():
    return none(seq[byte])
  some(bytesOf(payload.get()))

proc loadLatestCheckpointRaw*(store: FabricStore): Option[seq[byte]] {.gcsafe.} =
  let latest = store.kv.get(ksIndex, "checkpoint:latest")
  if latest.isNone():
    return none(seq[byte])
  store.loadCheckpointRaw(latest.get())

proc loadCheckpointSnapshotRaw*(store: FabricStore, checkpointId: string): Option[seq[byte]] {.gcsafe.} =
  store.kv.getBytes(ksTask, "snapshot:" & checkpointId)

proc loadCheckpointBundleRaw*(
    store: FabricStore, checkpointId: string
): Option[seq[byte]] {.gcsafe.} =
  let cert = store.loadCheckpoint(checkpointId)
  let snapshot = store.loadCheckpointSnapshotRaw(checkpointId)
  if cert.isNone() or snapshot.isNone():
    return none(seq[byte])
  some(bytesOf(encodeObj(CheckpointBundle(
    certificate: cert.get(),
    snapshot: snapshot.get(),
  ))))

proc scanPrefix[T](store: FabricStore, space: KeySpace, prefix: string): seq[T] =
  let page = store.kv.scanPrefix(space, prefix = prefix, limit = 0)
  for item in page.entries:
    result.add(decodeObj[T](item.value))

proc loadAllEvents*(store: FabricStore): seq[FabricEvent] =
  scanPrefix[FabricEvent](store, ksEvent, "event:")

proc loadAllAttestations*(store: FabricStore): seq[EventAttestation] =
  scanPrefix[EventAttestation](store, ksEvent, "attestation:")

proc loadAllEventCertificates*(store: FabricStore): seq[EventCertificate] =
  scanPrefix[EventCertificate](store, ksEvent, "eventcert:")

proc loadAllIsolations*(store: FabricStore): seq[IsolationRecord] =
  scanPrefix[IsolationRecord](store, ksEvent, "isolation:")

proc loadAllCheckpointCandidates*(store: FabricStore): seq[CheckpointCandidate] =
  scanPrefix[CheckpointCandidate](store, ksEvent, "checkpoint_candidate:")

proc loadAllCheckpointVotes*(store: FabricStore): seq[CheckpointVote] =
  scanPrefix[CheckpointVote](store, ksEvent, "checkpoint_vote:")

proc loadAllCheckpoints*(store: FabricStore): seq[CheckpointCertificate] =
  scanPrefix[CheckpointCertificate](store, ksEvent, "checkpoint:")

proc loadAllAvoProposals*(store: FabricStore): seq[AvoProposal] =
  scanPrefix[AvoProposal](store, ksTask, "avo_proposal:")

proc loadAllAvoAdoptions*(store: FabricStore): seq[AvoAdoptionRecord] =
  scanPrefix[AvoAdoptionRecord](store, ksTask, "avo_adoption:")

proc loadAvoApprovals*(store: FabricStore): seq[(string, string)] =
  let page = store.kv.scanPrefix(ksTask, prefix = "avo_approval:", limit = 0)
  for item in page.entries:
    let parts = item.id.split(":")
    if parts.len == 3:
      result.add((parts[1], parts[2]))
