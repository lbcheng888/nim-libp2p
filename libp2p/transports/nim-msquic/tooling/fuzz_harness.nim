## 模糊测试脚手架：复用 Nim 核心模型生成并验证初始握手。

import std/strformat

import "../core/mod"

type
  FuzzStatus* = enum
    frSuccess
    frEmptyPayload
    frHandshakeFailed
    frError

  FuzzIterationResult* = object
    status*: FuzzStatus
    payloadLength*: int
    packetSummary*: string
    error*: string

proc clientConnection(): QuicConnection =
  let clientCid = initConnectionId(@[0xF0'u8, 0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8, 0x05'u8])
  let serverCid = initConnectionId(@[0x0F'u8, 0xAA'u8, 0xBB'u8, 0xCC'u8, 0xDD'u8, 0xEE'u8])
  newConnection(crClient, clientCid, serverCid, QuicVersion(1))

proc packetSummary(packet: PreparedPacket): string =
  case packet.header.form
  of pfLongHeader:
    fmt"{packet.header.longHdr.packetType}-pn{packet.header.longHdr.packetNumber}-len{packet.payloadLength}"
  of pfShortHeader:
    fmt"short-pn{packet.header.shortHdr.packetNumber}-len{packet.payloadLength}"

proc fuzzInitialFlight*(payload: seq[uint8]): FuzzIterationResult =
  var conn = clientConnection()
  var data = payload
  if data.len == 0:
    data = @[0x01'u8]
  conn.queueInitialCrypto(data)
  if not conn.hasInitialCryptoPending():
    return FuzzIterationResult(
      status: frEmptyPayload,
      payloadLength: data.len,
      packetSummary: "",
      error: "")
  try:
    let packetHandle = conn.buildInitialPacket()
    let packet = packetHandle.toSnapshot()
    conn.onHandshakeAck(ceInitial)
    let success = conn.handshakeSucceeded()
    FuzzIterationResult(
      status: (if success: frSuccess else: frHandshakeFailed),
      payloadLength: data.len,
      packetSummary: packetSummary(packet),
      error: "")
  except CatchableError as exc:
    FuzzIterationResult(
      status: frError,
      payloadLength: data.len,
      packetSummary: "",
      error: exc.msg)

proc fuzzPayloads*(payloads: openArray[seq[uint8]]): seq[FuzzIterationResult] =
  result = @[]
  for payload in payloads:
    result.add fuzzInitialFlight(payload)

proc fuzzWithMutator*(iterations: Natural; seed: seq[uint8];
    mutator: proc (payload: var seq[uint8])): seq[FuzzIterationResult] =
  var current = seed
  result = @[]
  for _ in 0 ..< iterations:
    var candidate = current
    mutator(candidate)
    result.add fuzzInitialFlight(candidate)
    current = candidate
