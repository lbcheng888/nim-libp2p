import std/tables

import ./state
import ../types

proc applyCreditIssue*(
    state: ChainState,
    position: CreditPosition,
) =
  if state.creditPositions.hasKey(position.positionId):
    raise newException(ValueError, "duplicate credit position: " & position.positionId)
  state.creditPositions[position.positionId] = position

proc applyCreditFreeze*(state: ChainState, positionId: string, reason: string) =
  if not state.creditPositions.hasKey(positionId):
    raise newException(ValueError, "unknown credit position: " & positionId)
  var position = state.creditPositions[positionId]
  position.status = csFrozen
  position.freezeReason = reason
  state.creditPositions[positionId] = position

proc applyCreditExpire*(state: ChainState, positionId: string) =
  if not state.creditPositions.hasKey(positionId):
    raise newException(ValueError, "unknown credit position: " & positionId)
  var position = state.creditPositions[positionId]
  position.status = csExpired
  position.available = 0
  state.creditPositions[positionId] = position

proc applyCreditConvert*(
    state: ChainState,
    positionId: string,
    amount: uint64,
) =
  if not state.creditPositions.hasKey(positionId):
    raise newException(ValueError, "unknown credit position: " & positionId)
  var position = state.creditPositions[positionId]
  if position.status != csOpen:
    raise newException(ValueError, "credit position is not convertible")
  if amount == 0 or amount > position.available:
    raise newException(ValueError, "invalid convert amount")
  if state.rwadBalanceOf(state.params.rewardPoolAccount) < amount:
    raise newException(ValueError, "reward pool depleted")
  position.available -= amount
  if position.available == 0:
    position.status = csConverted
  state.creditPositions[positionId] = position
  state.adjustRwadBalance(state.params.rewardPoolAccount, -int64(amount))
  state.adjustRwadBalance(position.owner, int64(amount))

proc creditBalanceSnapshot*(state: ChainState, owner: string): CreditBalanceSnapshot =
  state.creditBalanceSnapshot(owner)
