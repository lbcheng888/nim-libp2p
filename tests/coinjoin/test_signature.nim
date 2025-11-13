# Nim-Libp2p CoinJoin signature tests (skeleton)

import unittest2, libp2p/crypto/coinjoin

suite "CoinJoin signature prototype":
  test "startAggregation enforces feature flag":
    var ctx = MusigSessionCtx(sessionId: 0, participantIds: @[])
    let res = startAggregation(ctx, @["p1", "p2"])
    if coinJoinSupportEnabled():
      check res.isErr or res.isOk
    else:
      check res.isErr and res.error.kind == cjDisabled

  test "processPartialSignature validates inputs":
    var ctx = MusigSessionCtx(sessionId: 1, participantIds: @["p1"])
    let res = processPartialSignature(ctx, "", @[])
    if coinJoinSupportEnabled():
      check res.isErr and res.error.kind == cjInvalidInput
    else:
      check res.isErr and res.error.kind == cjDisabled
