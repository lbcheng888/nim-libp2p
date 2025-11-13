## 对应 `src/core/frame.h` 中的帧定义及基础结构。

import std/options
import ./common

type
  QuicFrameType* = enum
    qftPadding               = 0x0'u64
    qftPing                  = 0x1'u64
    qftAck                   = 0x2'u64
    qftAck1                  = 0x3'u64
    qftResetStream           = 0x4'u64
    qftStopSending           = 0x5'u64
    qftCrypto                = 0x6'u64
    qftNewToken              = 0x7'u64
    qftStream                = 0x8'u64
    qftStream1               = 0x9'u64
    qftStream2               = 0xAu64
    qftStream3               = 0xBu64
    qftStream4               = 0xCu64
    qftStream5               = 0xDu64
    qftStream6               = 0xEu64
    qftStream7               = 0xFu64
    qftMaxData               = 0x10'u64
    qftMaxStreamData         = 0x11'u64
    qftMaxStreams            = 0x12'u64
    qftMaxStreams1           = 0x13'u64
    qftDataBlocked           = 0x14'u64
    qftStreamDataBlocked     = 0x15'u64
    qftStreamsBlocked        = 0x16'u64
    qftStreamsBlocked1       = 0x17'u64
    qftNewConnectionId       = 0x18'u64
    qftRetireConnectionId    = 0x19'u64
    qftPathChallenge         = 0x1Au64
    qftPathResponse          = 0x1Bu64
    qftConnectionClose       = 0x1Cu64
    qftConnectionClose1      = 0x1Du64
    qftHandshakeDone         = 0x1Eu64
    qftImmediateAck          = 0x1Fu64
    qftReliableResetStream   = 0x21'u64
    qftDatagram              = 0x30'u64
    qftDatagram1             = 0x31'u64
    qftAckFrequency          = 0xAF'u64
    qftTimestamp             = 0x2F5'u64

  AckBlock* = object
    gap*: QuicVarInt                ## `QUIC_ACK_BLOCK_EX.Gap`
    ackLength*: QuicVarInt          ## `QUIC_ACK_BLOCK_EX.AckBlock`

  AckDescriptor* = object
    largestAcknowledged*: QuicVarInt
    ackDelay*: QuicVarInt
    firstAckBlock*: QuicVarInt
    additionalBlocks*: seq[AckBlock]

  StreamFrameFlags* = set[QuicFrameFlag]

  QuicFrameFlag* = enum
    qffFin
    qffLen
    qffOffset

  StreamFramePayload* = object
    streamId*: uint64
    offset*: uint64
    length*: uint64
    fin*: bool
    dataLength*: uint32

  CryptoFramePayload* = object
    offset*: uint64
    length*: uint64

  MaxDataFramePayload* = object
    maximumData*: uint64

  MaxStreamDataFramePayload* = object
    streamId*: uint64
    maximumStreamData*: uint64

  MaxStreamsFramePayload* = object
    bidi*: bool
    maximumStreams*: uint64

  DataBlockedFramePayload* = object
    dataLimit*: uint64

  StreamDataBlockedFramePayload* = object
    streamId*: uint64
    streamDataLimit*: uint64

  StreamsBlockedFramePayload* = object
    bidi*: bool
    streamLimit*: uint64

  NewConnectionIdFramePayload* = object
    sequence*: uint64
    retirePriorTo*: uint64
    cid*: ConnectionId
    statelessResetToken*: array[16, uint8]

  RetireConnectionIdFramePayload* = object
    sequence*: uint64

  PathChallengeFramePayload* = object
    data*: array[8, uint8]

  PathResponseFramePayload* = object
    data*: array[8, uint8]

  ConnectionClosePayload* = object
    errorCode*: uint64
    frameType*: Option[QuicFrameType]
    reasonPhrase*: string

  HandshakeDonePayload* = object
    empty*: bool

  DatagramFramePayload* = object
    containsLength*: bool
    length*: uint16

  AckFrequencyFramePayload* = object
    sequenceNumber*: uint32
    packetTolerance*: uint32
    maxAckDelay*: uint32
    ignoreOrder*: bool

  TimestampFramePayload* = object
    timestampValue*: uint64

  ReliableResetStreamPayload* = object
    streamId*: uint64
    applicationErrorCode*: uint16
    finalSize*: uint64
    reliableOffset*: uint64

  ResetStreamPayload* = object
    streamId*: uint64
    applicationErrorCode*: uint16
    finalSize*: uint64

  StopSendingPayload* = object
    streamId*: uint64
    errorCode*: uint16

  NewTokenPayload* = object
    token*: seq[uint8]

  ImmediateAckPayload* = object
    ackDelay*: uint16

  QuicFramePayloadKind* = enum
    fpNone
    fpAck
    fpStream
    fpCrypto
    fpMaxData
    fpMaxStreamData
    fpMaxStreams
    fpDataBlocked
    fpStreamDataBlocked
    fpStreamsBlocked
    fpNewConnectionId
    fpRetireConnectionId
    fpPathChallenge
    fpPathResponse
    fpConnectionClose
    fpHandshakeDone
    fpNewToken
    fpDatagram
    fpAckFrequency
    fpTimestamp
    fpResetStream
    fpStopSending
    fpReliableResetStream
    fpImmediateAck

  QuicFramePayload* = object
    case kind*: QuicFramePayloadKind
    of fpNone:
      discard
    of fpAck:
      ack*: AckDescriptor
    of fpStream:
      stream*: StreamFramePayload
    of fpCrypto:
      crypto*: CryptoFramePayload
    of fpMaxData:
      maxData*: MaxDataFramePayload
    of fpMaxStreamData:
      maxStreamData*: MaxStreamDataFramePayload
    of fpMaxStreams:
      maxStreams*: MaxStreamsFramePayload
    of fpDataBlocked:
      dataBlocked*: DataBlockedFramePayload
    of fpStreamDataBlocked:
      streamDataBlocked*: StreamDataBlockedFramePayload
    of fpStreamsBlocked:
      streamsBlocked*: StreamsBlockedFramePayload
    of fpNewConnectionId:
      newConnectionId*: NewConnectionIdFramePayload
    of fpRetireConnectionId:
      retireConnectionId*: RetireConnectionIdFramePayload
    of fpPathChallenge:
      pathChallenge*: PathChallengeFramePayload
    of fpPathResponse:
      pathResponse*: PathResponseFramePayload
    of fpConnectionClose:
      connectionClose*: ConnectionClosePayload
    of fpHandshakeDone:
      handshakeDone*: HandshakeDonePayload
    of fpNewToken:
      newToken*: NewTokenPayload
    of fpDatagram:
      datagram*: DatagramFramePayload
    of fpAckFrequency:
      ackFrequency*: AckFrequencyFramePayload
    of fpTimestamp:
      timestamp*: TimestampFramePayload
    of fpResetStream:
      resetStream*: ResetStreamPayload
    of fpStopSending:
      stopSending*: StopSendingPayload
    of fpReliableResetStream:
      reliableReset*: ReliableResetStreamPayload
    of fpImmediateAck:
      immediateAck*: ImmediateAckPayload

  QuicFrame* = object
    frameType*: QuicFrameType
    payload*: QuicFramePayload

proc initAckDescriptor*(
    largestAcked, ackDelay, firstBlock: QuicVarInt,
    blocks: seq[AckBlock] = @[]): AckDescriptor =
  AckDescriptor(
    largestAcknowledged: largestAcked,
    ackDelay: ackDelay,
    firstAckBlock: firstBlock,
    additionalBlocks: blocks)

proc framePayloadKind*(frameType: QuicFrameType): QuicFramePayloadKind =
  case frameType
  of qftAck, qftAck1: fpAck
  of qftStream, qftStream1, qftStream2, qftStream3, qftStream4,
     qftStream5, qftStream6, qftStream7: fpStream
  of qftCrypto: fpCrypto
  of qftMaxData: fpMaxData
  of qftMaxStreamData: fpMaxStreamData
  of qftMaxStreams, qftMaxStreams1: fpMaxStreams
  of qftDataBlocked: fpDataBlocked
  of qftStreamDataBlocked: fpStreamDataBlocked
  of qftStreamsBlocked, qftStreamsBlocked1: fpStreamsBlocked
  of qftNewConnectionId: fpNewConnectionId
  of qftRetireConnectionId: fpRetireConnectionId
  of qftPathChallenge: fpPathChallenge
  of qftPathResponse: fpPathResponse
  of qftConnectionClose, qftConnectionClose1: fpConnectionClose
  of qftHandshakeDone: fpHandshakeDone
  of qftNewToken: fpNewToken
  of qftDatagram, qftDatagram1: fpDatagram
  of qftAckFrequency: fpAckFrequency
  of qftTimestamp: fpTimestamp
  of qftResetStream: fpResetStream
  of qftStopSending: fpStopSending
  of qftReliableResetStream: fpReliableResetStream
  of qftImmediateAck: fpImmediateAck
  else: fpNone
