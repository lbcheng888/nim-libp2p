## 对应 `src/core/packet*.c/h` 的关键结构建模。

import ./common, ./frame_model

when compiles((proc () {.noGc.} = discard)):
  {.pragma: quicHotPath, noGc, raises: [].}
else:
  {.pragma: quicHotPath, gcsafe, raises: [].}

const
  MaxCryptoSpanLength = high(uint16).int

type
  CryptoSpan* = object
    data*: ptr uint8
    length*: uint16

  PacketForm* = enum
    pfLongHeader
    pfShortHeader

  CryptoEpoch* = enum
    ceInitial
    ceZeroRtt
    ceHandshake
    ceOneRtt

  PacketNumberSpaceState* = object
    epoch*: CryptoEpoch
    largestSent*: uint64
    largestAcked*: uint64
    lossTime*: uint64

  PacketHeaderInvariant* = object
    isLongHeader*: bool
    version*: QuicVersion
    destinationCid*: ConnectionId
    sourceCid*: ConnectionId

  LongHeaderType* = enum
    lhtInitial
    lhtZeroRtt
    lhtHandshake
    lhtRetry

  PacketHeaderLong* = object
    invariant*: PacketHeaderInvariant
    packetType*: LongHeaderType
    tokenLength*: QuicVarInt
    packetNumber*: uint32

  PacketHeaderShort* = object
    invariant*: PacketHeaderInvariant
    keyPhase*: bool
    spinBit*: bool
    packetNumber*: uint16

  QuicPacketHeader* = object
    case form*: PacketForm
    of pfLongHeader:
      longHdr*: PacketHeaderLong
    of pfShortHeader:
      shortHdr*: PacketHeaderShort

  FramePayload* = object
    frameType*: QuicFrameType
    cryptoOffset*: uint64
    cryptoData*: CryptoSpan

  PreparedPacket* = object
    header*: QuicPacketHeader
    frames*: seq[FramePayload]
    payloadLength*: uint16

proc initPacketNumberSpace*(epoch: CryptoEpoch): PacketNumberSpaceState {.quicHotPath.} =
  PacketNumberSpaceState(epoch: epoch, largestSent: 0, largestAcked: 0, lossTime: 0)

proc recordSentPacket*(space: var PacketNumberSpaceState, packetNumber: uint64) {.quicHotPath.} =
  ## 更新发送的最大包号。
  if packetNumber > space.largestSent:
    space.largestSent = packetNumber

proc recordAckedPacket*(space: var PacketNumberSpaceState, packetNumber: uint64) {.quicHotPath.} =
  ## 更新已确认的最大包号。
  if packetNumber > space.largestAcked:
    space.largestAcked = packetNumber

proc emptyCryptoSpan*(): CryptoSpan {.inline.} =
  CryptoSpan(data: nil, length: 0)

proc makeCryptoSpan*(dataPtr: ptr uint8, length: Natural): CryptoSpan =
  ## 从裸指针与长度构建 Span，限制长度不超过 65535。
  if length == 0:
    return CryptoSpan(data: nil, length: 0)
  if dataPtr.isNil:
    raise newException(ValueError, "crypto span pointer must not be nil")
  if length > MaxCryptoSpanLength:
    raise newException(ValueError, "crypto span length overflow")
  CryptoSpan(data: dataPtr, length: uint16(length))

proc cryptoFrame*(offset: uint64, span: CryptoSpan): FramePayload =
  ## 构建携带 Crypto 载荷的帧描述。
  FramePayload(frameType: qftCrypto, cryptoOffset: offset, cryptoData: span)

proc computePayloadLength*(frames: openArray[FramePayload]): uint16 {.quicHotPath.} =
  ## 目前仅统计 Crypto 帧长度，满足最小握手路径需求。
  var total = 0
  for frame in frames:
    if frame.frameType == qftCrypto:
      total.inc(frame.cryptoData.length.int)
  uint16(total)

proc recalcPayloadLength*(packet: var PreparedPacket) {.quicHotPath.} =
  ## 根据帧载荷重新计算包的净载荷长度。
  packet.payloadLength = computePayloadLength(packet.frames)

proc cryptoSpanToSeq*(span: CryptoSpan): seq[uint8] =
  ## 将零拷贝 Span 转换为 seq，供测试或日志使用。
  if span.length == 0 or span.data.isNil:
    return @[]
  result = newSeq[uint8](span.length.int)
  copyMem(addr result[0], span.data, span.length.int)
