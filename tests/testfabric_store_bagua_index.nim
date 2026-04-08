{.used.}

import std/[algorithm, json, os, sequtils]
from std/times import epochTime

import unittest2

import ../libp2p/lsmr
import ../libp2p/fabric/storage/store
import ../libp2p/fabric/types
import ../libp2p/rwad/types

proc testRoot(name: string): string =
  getTempDir() / (name & "-" & $int64(epochTime() * 1_000_000))

proc sampleTx(nonce: uint64): Tx =
  Tx(
    txId: "tx-" & $nonce,
    kind: txContentPublishIntent,
    sender: "alice",
    senderPublicKey: "alice-pub",
    nonce: nonce,
    epoch: 1,
    timestamp: 1_717_171_717'i64,
    payload: newJObject(),
    signature: "sig",
  )

proc sampleEvent(eventId: string, clock: GanzhiClock): FabricEvent =
  FabricEvent(
    eventId: eventId,
    threadId: EventThreadId(
      contractRoot: "content",
      owner: "alice",
      lane: "main",
    ),
    author: "alice",
    authorPublicKey: "alice-pub",
    authorPeerId: "peer-alice",
    contractRoot: "content",
    entrypoint: "publish",
    args: newJObject(),
    legacyTx: sampleTx(1),
    participants: @["alice", "bob"],
    participantRoutes: @[],
    witnessRoutes: @[],
    witnessSet: @["validator-1"],
    parents: @[],
    clock: clock,
    routingPath: @[5'u8, 1'u8],
    createdAt: 1_717_171_717'i64,
  )

suite "Fabric Store Bagua Index":
  test "persist raw objects writes anti-entropy bagua leaves":
    let root = testRoot("fabric-store-bagua-index")
    if dirExists(root):
      removeDir(root)

    var store = newFabricStore(root)
    let clock = GanzhiClock(era: 7'u64, tick: 3'u8)
    let event = sampleEvent("evt-1", clock)
    let attParticipant = EventAttestation(
      eventId: event.eventId,
      signer: "bob",
      signerPublicKey: "bob-pub",
      signerPeerId: "peer-bob",
      role: arParticipant,
      weight: 1'u64,
      issuedAt: 1_717_171_718'i64,
      signature: "att-p",
    )
    let attWitness = EventAttestation(
      eventId: event.eventId,
      signer: "validator-1",
      signerPublicKey: "validator-pub",
      signerPeerId: "peer-validator",
      role: arWitness,
      weight: 1'u64,
      issuedAt: 1_717_171_719'i64,
      signature: "att-w",
    )
    let cert = EventCertificate(
      certificateId: "cert-1",
      eventId: event.eventId,
      threadId: event.threadId,
      clock: clock,
      participantAttestations: @[attParticipant],
      witnessAttestations: @[attWitness],
      reachableWitnesses: 1,
      witnessThreshold: 1,
      createdAt: 1_717_171_720'i64,
    )
    let candidate = CheckpointCandidate(
      candidateId: "cand-1",
      era: 7'u64,
      frontierDigest: "frontier",
      projectionRoots: @[],
      activeContractRoots: @[],
      activeAvoSet: @[],
      adoptedProposalIds: @[],
      isolatedEventIds: @[],
      snapshotRoot: "snapshot",
      validatorSetRoot: "validators",
      createdAt: 1_717_171_721'i64,
    )
    let vote = CheckpointVote(
      candidateId: candidate.candidateId,
      validator: "validator-1",
      validatorPublicKey: "validator-pub",
      validatorPeerId: "peer-validator",
      weight: 1'u64,
      issuedAt: 1_717_171_722'i64,
      signature: "vote",
    )

    store.persistEvent(event)
    store.persistAttestation(clock, attParticipant)
    store.persistAttestation(clock, attWitness)
    store.persistEventCertificate(cert)
    store.persistCheckpointCandidate(candidate)
    store.persistCheckpointVote(candidate.era, vote)
    store.close()

    store = newFabricStore(root)

    let eventRoot = store.loadAntiEntropyRootLeaves("event")
    check eventRoot.len == 2
    check eventRoot.allIt(it.clock == clock)
    check eventRoot.allIt(it.payload == "event:" & event.eventId)
    check eventRoot.mapIt(it.baguaKey.bagua).sorted(system.cmp[LsmrBagua]) ==
      @[LsmrBagua.lbgZhen, LsmrBagua.lbgXun].sorted(system.cmp[LsmrBagua])

    let attParticipantTick =
      store.loadAntiEntropyTickLeaves(
        "att_participant",
        clock.era,
        uint64(clock.tick),
      )
    check attParticipantTick.len == 2
    check attParticipantTick.allIt(
      it.payload == "attestation:" & event.eventId & ":" & $arParticipant & ":bob"
    )

    let eventCertEra = store.loadAntiEntropyEraLeaves("eventcert", clock.era)
    check eventCertEra.len == 2
    check eventCertEra.allIt(it.payload == "eventcert:" & event.eventId)
    check eventCertEra.mapIt(it.baguaKey.bagua).sorted(system.cmp[LsmrBagua]) ==
      @[LsmrBagua.lbgKan, LsmrBagua.lbgLi].sorted(system.cmp[LsmrBagua])

    let voteRoot = store.loadAntiEntropyRootLeaves("checkpoint_vote")
    check voteRoot.len == 2
    check voteRoot.allIt(
      it.payload == "checkpoint-vote:" & candidate.candidateId & ":" & vote.validator
    )

    store.close()
    if dirExists(root):
      removeDir(root)
