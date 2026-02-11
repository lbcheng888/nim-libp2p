## Nim 版 pacing 调度器，实现 BBR 每轮发送量与 ACK 聚合突发的协调。

import std/options

import ./bbr_model
import ./common

const
  MaxBurstAllowanceBytes = 512'u64 * 1024'u64  ## 上限 512KB，避免 ACK 聚合导致短时过大发送。

type
  PacingState* = object
    pacingEnabled*: bool
    lastSendTimeUs*: uint64
    burstAllowanceBytes*: uint64

proc initPacingState*(pacingEnabled: bool = true): PacingState =
  PacingState(
    pacingEnabled: pacingEnabled,
    lastSendTimeUs: 0,
    burstAllowanceBytes: 0)

proc reset*(state: var PacingState) =
  ## 清空节奏控制状态，但保留 enable 配置。
  state.lastSendTimeUs = 0
  state.burstAllowanceBytes = 0

proc setPacingEnabled*(state: var PacingState, enabled: bool) =
  state.pacingEnabled = enabled
  if not enabled:
    state.reset()

proc recordAckHeight*(state: var PacingState, ackHeightBytes: uint64) =
  ## ACK 聚合带来的额外窗口，可用于一次性突发发送。
  if ackHeightBytes == 0:
    return
  let allowance = state.burstAllowanceBytes + ackHeightBytes
  if allowance < state.burstAllowanceBytes:
    state.burstAllowanceBytes = MaxBurstAllowanceBytes
  else:
    state.burstAllowanceBytes = min(allowance, MaxBurstAllowanceBytes)

proc allowance*(state: var PacingState, model: BbrModel, nowUs: uint64): uint64 =
  ## 计算当前允许立即发送的字节数，综合考虑 CWND 与 pacing。
  let cwndAllowance = model.sendAllowance()
  if cwndAllowance == 0:
    if nowUs != 0:
      state.lastSendTimeUs = nowUs
    return 0

  if not state.pacingEnabled or nowUs == 0 or model.pacingRateBps == 0:
    return cwndAllowance

  var deltaUs =
    if state.lastSendTimeUs == 0 or nowUs <= state.lastSendTimeUs:
      let minRtt = (
        if model.minRtt == high(uint64): 0'u64
        else: model.minRtt)
      if minRtt == 0:
        MicroSecsInSec div 1000
      else:
        minRtt
    else:
      nowUs - state.lastSendTimeUs
  if deltaUs == 0:
    deltaUs = 1

  var pacingBytes =
    (model.pacingRateBps * deltaUs) div MicroSecsInSec
  if pacingBytes < model.sendQuantum:
    pacingBytes = model.sendQuantum

  let burst = min(state.burstAllowanceBytes, cwndAllowance)
  var total = pacingBytes + burst
  if total < pacingBytes:
    total = high(uint64)
  if total > cwndAllowance:
    total = cwndAllowance
  total

proc commitSend*(state: var PacingState, nowUs: uint64, sentBytes: uint64) =
  ## 记录一次实际发送，更新最后发送时间与剩余突发额度。
  if nowUs != 0:
    state.lastSendTimeUs = nowUs
  if sentBytes >= state.burstAllowanceBytes:
    state.burstAllowanceBytes = 0
  else:
    state.burstAllowanceBytes -= sentBytes

proc onLoss*(state: var PacingState) =
  ## 丢包时适度降低突发额度，避免持续放量。
  state.burstAllowanceBytes = state.burstAllowanceBytes div 2

proc nextSendTime*(state: PacingState, model: BbrModel, nowUs: uint64): Option[uint64] =
  ## 返回下一次 pacing 触发的时间点，用于时间轮调度。
  if not state.pacingEnabled or model.pacingRateBps == 0 or nowUs == 0:
    return none(uint64)
  let interval = model.pacingIntervalUs()
  if interval == 0:
    return none(uint64)
  let reference =
    if state.lastSendTimeUs == 0 or state.lastSendTimeUs < nowUs:
      nowUs
    else:
      state.lastSendTimeUs
  some(reference + interval)
