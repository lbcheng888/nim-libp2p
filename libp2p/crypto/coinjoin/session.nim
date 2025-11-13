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

proc initSessionState*(sessionId: uint64, participants: seq[string]): CoinJoinSessionState =
  CoinJoinSessionState(
    id: sessionId,
    phase: cjpDiscovery,
    participants: participants,
    pendingEffects: @[]
  )

proc resetSession*(
    state: var CoinJoinSessionState
): CoinJoinResult[void] =
  if not coinJoinSupportEnabled():
    return errDisabled[void]("resetSession")
  state.phase = cjpDiscovery
  state.pendingEffects.setLen(0)
  errNotImplemented[void]("resetSession")

proc stepSession*(
    state: var CoinJoinSessionState, effect: SessionEffect
): CoinJoinResult[SessionEffect] =
  if not coinJoinSupportEnabled():
    return errDisabled[SessionEffect]("stepSession")
  state.pendingEffects.add(effect)
  errNotImplemented[SessionEffect]("stepSession")
