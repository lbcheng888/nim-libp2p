# Nim-Libp2p
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/tables
import results

const
  coinjoinFeatureEnabled* = defined(libp2p_coinjoin)

type
  CoinJoinErrorKind* = enum
    cjDisabled,
    cjNotImplemented,
    cjInvalidInput,
    cjInternal

  CoinJoinError* = object
    kind*: CoinJoinErrorKind
    msg*: string

  CoinJoinResult*[T] = Result[T, CoinJoinError]

proc newCoinJoinError*(kind: CoinJoinErrorKind, msg = ""): CoinJoinError =
  CoinJoinError(kind: kind, msg: msg)

proc errDisabled*[T](feature: string): CoinJoinResult[T] =
  err(newCoinJoinError(
    cjDisabled, feature & " requires -d:libp2p_coinjoin to be enabled"
  ))

proc errNotImplemented*[T](feature: string): CoinJoinResult[T] =
  err(newCoinJoinError(
    cjNotImplemented, feature & " is not yet implemented"
  ))

proc coinJoinSupportEnabled*(): bool {.inline.} =
  coinjoinFeatureEnabled

type
  CoinJoinAmount* = array[32, byte]
  CoinJoinBlind* = array[32, byte]
  CoinJoinPoint* = array[33, byte]
  OnionSeed* = array[32, byte]
  OnionSecret* = array[32, byte]
  OnionMac* = array[32, byte]
  OnionProof* = OnionMac

  CoinJoinCommitment* = object
    point*: CoinJoinPoint
    blind*: CoinJoinBlind

  OnionRouteCtx* = object
    seed*: OnionSeed
    hopSecrets*: seq[OnionSecret]
    payload*: seq[byte]
    mac*: OnionMac

  ShuffleProof* = object
    challenge*: CoinJoinPoint
    responses*: seq[CoinJoinPoint]

  NonceState* = enum
    nsNone, nsReceived, nsUsed

  MusigSessionCtx* = object
    sessionId*: uint64
    participantIds*: seq[string]
    aggregatedNonce*: CoinJoinPoint
    usedNonces*: Table[string, NonceState]
    # Musig2 State
    partialSignatures*: Table[string, seq[byte]] # participantId -> 32-byte partial sig
    sessionCache*: seq[byte] # Opaque MusigSession serialized
    keyAggCache*: seq[byte] # Opaque MusigKeyAggCache serialized
    aggNonceBytes*: seq[byte] # 132 bytes if needed, or reuse aggregatedNonce field with conversion

  OnionPacket* = object
    routeId*: uint64
    payload*: seq[byte]
    mac*: OnionMac

  CoinJoinPhase* = enum
    cjpDiscovery,
    cjpCommit,
    cjpShuffle,
    cjpSignature,
    cjpFinalize

  SessionEffectKind* = enum
    sekNone,
    sekBroadcastMessage,
    sekBlameReport,
    sekFinalizeTx

  SessionEffect* = object
    kind*: SessionEffectKind
    data*: seq[byte]

  CoinJoinSessionState* = object
    id*: uint64
    phase*: CoinJoinPhase
    participants*: seq[string]
    pendingEffects*: seq[SessionEffect]
