# Nim-Libp2p CoinJoin skeleton tests

import unittest2, results, libp2p/crypto/coinjoin
import ./test_commitments
import ./test_shuffle
import ./test_signature

proc zeroBytes32(): array[32, byte] =
  default(array[32, byte])

suite "CoinJoin skeleton API":
  test "feature flag exposed":
    check coinJoinSupportEnabled() == defined(libp2p_coinjoin)

  test "commitment helpers return structured result":
    let amount = default(CoinJoinAmount)
    let blind = default(CoinJoinBlind)
    let res = pedersenCommit(amount, blind)
    if coinJoinSupportEnabled():
      check res.isErr or res.isOk
    else:
      check res.isErr and res.error.kind == cjDisabled

  test "shuffle proof API guarded":
    let res = proveShuffle(@[], @[])
    if coinJoinSupportEnabled():
      check res.isErr or res.isOk
    else:
      check res.isErr and res.error.kind == cjDisabled

  test "signature aggregation guard":
    var ctx = MusigSessionCtx(sessionId: 1, participantIds: @["p1", "p2"])
    let res = startAggregation(ctx, ctx.participantIds)
    if coinJoinSupportEnabled():
      check res.isErr or res.isOk
    else:
      check res.isErr and res.error.kind == cjDisabled

  test "onion helper guard":
    let seed = zeroBytes32()
    let participant = [byte 0xAA]
    let res = deriveHopSecrets(seed, 1, participant, 1)
    if coinJoinSupportEnabled():
      check res.isErr or res.isOk
    else:
      check res.isErr and res.error.kind == cjDisabled

  test "session state initializes":
    var state = initSessionState(1, @["alice", "bob"])
    check state.id == 1
    check state.phase == cjpDiscovery
    let stepRes = stepSession(state, SessionEffect(kind: sekNone, data: @[]))
    if coinJoinSupportEnabled():
      check stepRes.isErr or stepRes.isOk
    else:
      check stepRes.isErr and stepRes.error.kind == cjDisabled
