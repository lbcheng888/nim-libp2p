# Nim-Libp2p
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import ./types

proc startAggregation*(
    ctx: var MusigSessionCtx, participantIds: seq[string]
): CoinJoinResult[void] =
  if not coinJoinSupportEnabled():
    return errDisabled[void]("startAggregation")
  ctx.participantIds = participantIds
  ctx.sessionId.inc
  errNotImplemented[void]("startAggregation")

proc processPartialSignature*(
    ctx: var MusigSessionCtx, participantId: string, payload: seq[byte]
): CoinJoinResult[void] =
  if not coinJoinSupportEnabled():
    return errDisabled[void]("processPartialSignature")
  if participantId.len == 0 or payload.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "participantId/payload missing"))
  errNotImplemented[void]("processPartialSignature")

proc finalizeAggregation*(
    ctx: var MusigSessionCtx
): CoinJoinResult[seq[byte]] =
  if not coinJoinSupportEnabled():
    return errDisabled[seq[byte]]("finalizeAggregation")
  errNotImplemented[seq[byte]]("finalizeAggregation")
