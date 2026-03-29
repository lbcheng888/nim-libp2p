## Pure Nim QUIC Protocol Definitions (RFC 9000)
## This module handles packet wire formats, header encoding/decoding, and basic frame structures.

import std/[algorithm, endians]

type
  QuicPacketType* = enum
    ptInitial = 0x0
    pt0RTT = 0x1
    ptHandshake = 0x2
    ptRetry = 0x3
    ptShort = 0x4 # Generic short header type

  ConnectionId* = seq[byte]

  QuicLongHeader* = object
    packetType*: QuicPacketType
    version*: uint32
    destConnectionId*: ConnectionId
    srcConnectionId*: ConnectionId
    token*: seq[byte]
    payloadLength*: uint64 # Variable Length Integer
    packetNumber*: uint32  # Truncated, usually 4 bytes for Initial

  QuicPacket* = object
    isLongHeader*: bool
    longHeader*: QuicLongHeader
    payload*: seq[byte]

# --- Helpers ---

proc writeUint32*(buf: var seq[byte], val: uint32) =
  var tmp: uint32
  bigEndian32(addr tmp, unsafeAddr val)
  let p = cast[ptr array[4, byte]](addr tmp)
  buf.add(p[])

proc writeVarInt*(buf: var seq[byte], val: uint64) =
  # RFC 9000 Variable Length Integer Encoding
  if val <= 63:
    buf.add(byte(val))
  elif val <= 16383:
    var tmp = uint16(val) or 0x4000
    var outVal: uint16
    bigEndian16(addr outVal, addr tmp)
    let p = cast[ptr array[2, byte]](addr outVal)
    buf.add(p[])
  elif val <= 1073741823:
    var tmp = uint32(val) or 0x80000000'u32
    var outVal: uint32
    bigEndian32(addr outVal, addr tmp)
    let p = cast[ptr array[4, byte]](addr outVal)
    buf.add(p[])
  else:
    var tmp = val or 0xC000000000000000'u64
    var outVal: uint64
    bigEndian64(addr outVal, addr tmp)
    let p = cast[ptr array[8, byte]](addr outVal)
    buf.add(p[])

# --- Encoding ---

proc encodeInitialPacket*(destCid, srcCid: ConnectionId, token: seq[byte], 
                          payload: seq[byte], packetNumber: uint32): seq[byte] =
  # RFC 9000 Section 17.2.2. Initial Packet
  # Header Form: Long Header
  # Type: Initial (0x0)
  
  # First Byte: 11000000 (0xC0) | (Type << 4) | (Packet Number Length - 1)
  # For Initial, Type is 0.
  # We'll use 4-byte packet number encoding (0x3), so low bits are 11.
  # Result: 11000011 -> 0xC3
  var firstByte = 0xC3'u8
  result.add(firstByte)

  # Version (4 bytes) - Using QUIC v1 (0x00000001)
  result.writeUint32(1'u32)

  # Destination Connection ID Length (1 byte) + DCID
  result.add(byte(destCid.len))
  result.add(destCid)

  # Source Connection ID Length (1 byte) + SCID
  result.add(byte(srcCid.len))
  result.add(srcCid)

  # Token Length (VarInt) + Token
  result.writeVarInt(uint64(token.len))
  result.add(token)

  # Length (VarInt)
  # Length field includes Packet Number length + Payload length
  let pnLen = 4'u64
  # Long-header payload length covers packet number + ciphertext + AEAD tag.
  let lengthVal = pnLen + uint64(payload.len) + 16'u64
  result.writeVarInt(lengthVal)

  # Packet Number (4 bytes, unencrypted for skeleton)
  # Note: Real QUIC encrypts this. We are writing it raw for the skeleton to establish structure.
  var pnRaw = packetNumber # In 4 bytes
  # We need to write it in Network Byte Order, masked by header protection in real world
  # For skeleton, just write big endian
  result.writeUint32(pnRaw)

  # Payload (Frames)
  result.add(payload)

proc encodeHandshakePacket*(destCid, srcCid: ConnectionId;
    payload: seq[byte]; packetNumber: uint32): seq[byte] =
  var firstByte = 0xE3'u8
  result.add(firstByte)
  result.writeUint32(1'u32)
  result.add(byte(destCid.len))
  result.add(destCid)
  result.add(byte(srcCid.len))
  result.add(srcCid)
  let pnLen = 4'u64
  let lengthVal = pnLen + uint64(payload.len) + 16'u64
  result.writeVarInt(lengthVal)
  result.writeUint32(packetNumber)
  result.add(payload)

proc encodeShortHeaderPacket*(destCid: ConnectionId, packetNumber: uint32, 
                              payload: seq[byte]): seq[byte] =
  # RFC 9000 Section 17.3.1. Short Header
  # First Byte: 01000000 (0x40) | (Type=0 for 1-RTT? No, Fixed Bit 1)
  # Short Header: 0 | 1 | Spin | Reserved | KeyPhase | PacketNumberLength
  # Fixed Bit = 1 (0x40). 
  # Spin = 0 (for now)
  # KeyPhase = 0
  # PN Length = 2 bytes (0x01) -> 3? No, 0=1byte, 1=2bytes, 2=3bytes, 3=4bytes.
  # Let's use 4-byte PN (0x03).
  # Byte: 01000011 -> 0x43
  var firstByte = 0x43'u8
  result.add(firstByte)
  
  result.add(destCid)
  
  # Packet Number (4 bytes)
  result.writeUint32(packetNumber)
  
  # Payload
  result.add(payload)

# --- Frame Encoding ---

type
  FrameType* = enum
    ftPadding = 0x00
    ftPing = 0x01
    ftAck = 0x02 # ... 0x03
    ftResetStream = 0x04
    ftStopSending = 0x05
    ftCrypto = 0x06
    ftNewToken = 0x07
    ftStream = 0x08 # 0x08..0x0F
    ftMaxData = 0x10
    ftMaxStreamData = 0x11
    # ... others omitted for brevity
  
  CryptoFrame* = object
    offset*: uint64
    length*: uint64
    data*: seq[byte]

  StreamFrame* = object
    streamId*: uint64
    offset*: uint64
    length*: uint64
    fin*: bool
    data*: seq[byte]

  AckRange* = object
    smallest*: uint64
    largest*: uint64

  AckFrame* = object
    largestAcked*: uint64
    delay*: uint64
    rangeCount*: uint64
    firstRange*: uint64
    ranges*: seq[AckRange]

  ResetStreamFrame* = object
    streamId*: uint64
    applicationErrorCode*: uint64
    finalSize*: uint64

  StopSendingFrame* = object
    streamId*: uint64
    applicationErrorCode*: uint64

  MaxDataFrame* = object
    maximumData*: uint64

  MaxStreamDataFrame* = object
    streamId*: uint64
    maximumStreamData*: uint64

  DatagramFrame* = object
    containsLength*: bool
    length*: uint64
    data*: seq[byte]

  PathChallengeFrame* = object
    data*: array[8, byte]

  PathResponseFrame* = object
    data*: array[8, byte]

  NewConnectionIdFrame* = object
    sequence*: uint64
    retirePriorTo*: uint64
    connectionId*: ConnectionId
    statelessResetToken*: array[16, byte]

  RetireConnectionIdFrame* = object
    sequence*: uint64

  HandshakeDoneFrame* = object
    present*: bool

  ConnectionCloseFrame* = object
    applicationClose*: bool
    errorCode*: uint64
    hasFrameType*: bool
    frameType*: uint64
    reasonPhrase*: string

proc encodeStreamFrame*(streamId: uint64, data: seq[byte], offset: uint64, fin: bool): seq[byte] =
  # STREAM Frame Layout:
  # Type (0x08..0x0F):
  #   0x08 | OFF=1 | LEN=1 | FIN=1
  #   OFF (0x04): Data has offset? (If 0, offset is 0)
  #   LEN (0x02): Length field present? (If 0, data extends to end of packet)
  #   FIN (0x01): Unknown size? No, FIN bit.
  
  var typeByte = 0x08'u8
  if offset > 0:
    typeByte = typeByte or 0x04'u8
  # We always include Length for safety in this implementation
  typeByte = typeByte or 0x02'u8 
  if fin:
    typeByte = typeByte or 0x01'u8
    
  result.add(typeByte)
  result.writeVarInt(streamId)
  if offset > 0:
    result.writeVarInt(offset)
  
  # Length
  result.writeVarInt(uint64(data.len))
  result.add(data)

proc encodeDatagramFrame*(data: seq[byte], includeLength = true): seq[byte] =
  if includeLength:
    result.add(0x31'u8)
    result.writeVarInt(uint64(data.len))
  else:
    result.add(0x30'u8)
  result.add(data)

proc encodeResetStreamFrame*(streamId: uint64; applicationErrorCode: uint64;
    finalSize: uint64): seq[byte] =
  result.add(0x04'u8)
  result.writeVarInt(streamId)
  result.writeVarInt(applicationErrorCode)
  result.writeVarInt(finalSize)

proc encodeStopSendingFrame*(streamId: uint64; applicationErrorCode: uint64): seq[byte] =
  result.add(0x05'u8)
  result.writeVarInt(streamId)
  result.writeVarInt(applicationErrorCode)

proc encodeMaxDataFrame*(maximumData: uint64): seq[byte] =
  result.add(0x10'u8)
  result.writeVarInt(maximumData)

proc encodeMaxStreamDataFrame*(streamId: uint64; maximumStreamData: uint64): seq[byte] =
  result.add(0x11'u8)
  result.writeVarInt(streamId)
  result.writeVarInt(maximumStreamData)

proc encodePathChallengeFrame*(challenge: array[8, byte]): seq[byte] =
  result.add(0x1A'u8)
  for b in challenge:
    result.add(b)

proc encodePathResponseFrame*(response: array[8, byte]): seq[byte] =
  result.add(0x1B'u8)
  for b in response:
    result.add(b)

proc encodeNewConnectionIdFrame*(sequence: uint64; retirePriorTo: uint64;
    connectionId: ConnectionId; statelessResetToken: array[16, byte]): seq[byte] =
  result.add(0x18'u8)
  result.writeVarInt(sequence)
  result.writeVarInt(retirePriorTo)
  result.add(byte(connectionId.len))
  result.add(connectionId)
  for b in statelessResetToken:
    result.add(b)

proc encodeRetireConnectionIdFrame*(sequence: uint64): seq[byte] =
  result.add(0x19'u8)
  result.writeVarInt(sequence)

proc encodeHandshakeDoneFrame*(): seq[byte] =
  @[0x1E'u8]

proc encodeConnectionCloseFrame*(errorCode: uint64; reasonPhrase = "";
    frameType = 0'u64; applicationClose = true): seq[byte] =
  result.add(if applicationClose: 0x1D'u8 else: 0x1C'u8)
  result.writeVarInt(errorCode)
  if not applicationClose:
    result.writeVarInt(frameType)
  result.writeVarInt(uint64(reasonPhrase.len))
  for ch in reasonPhrase:
    result.add(byte(ord(ch)))

# --- Decoding ---

proc readVarInt*(data: openArray[byte], pos: var int): uint64 =
  # RFC 9000 Variable Length Integer Decoding
  if pos >= data.len: return 0
  let first = data[pos]
  let prefix = first shr 6
  let length = 1 shl prefix
  
  if pos + length > data.len: return 0
  
  var val: uint64 = 0
  case length
  of 1: val = uint64(first and 0x3F)
  of 2:
    var tmp: uint16
    let p = cast[ptr array[2, byte]](unsafeAddr data[pos])
    var buf = p[]
    buf[0] = buf[0] and 0x3F
    bigEndian16(addr tmp, addr buf)
    val = uint64(tmp)
  of 4:
    var tmp: uint32
    let p = cast[ptr array[4, byte]](unsafeAddr data[pos])
    var buf = p[]
    buf[0] = buf[0] and 0x3F
    bigEndian32(addr tmp, addr buf)
    val = uint64(tmp)
  of 8:
    var tmp: uint64
    let p = cast[ptr array[8, byte]](unsafeAddr data[pos])
    var buf = p[]
    buf[0] = buf[0] and 0x3F
    bigEndian64(addr tmp, addr buf)
    val = tmp
  else: discard

  pos += length
  return val

proc decodePacket*(data: seq[byte]): QuicPacket =
  if data.len == 0: return
  
  var pos = 0
  let firstByte = data[pos]
  pos += 1
  
  result.isLongHeader = (firstByte and 0x80) != 0
  
  if result.isLongHeader:
    # Long Header Parsing
    # Version
    if pos + 4 > data.len: return
    var ver: uint32
    bigEndian32(addr ver, unsafeAddr data[pos])
    result.longHeader.version = ver
    pos += 4
    
    # DCID Len
    if pos >= data.len: return
    let dcidLen = int(data[pos])
    pos += 1
    if pos + dcidLen > data.len: return
    result.longHeader.destConnectionId = data[pos ..< pos + dcidLen]
    pos += dcidLen
    
    # SCID Len
    if pos >= data.len: return
    let scidLen = int(data[pos])
    pos += 1
    if pos + scidLen > data.len: return
    result.longHeader.srcConnectionId = data[pos ..< pos + scidLen]
    pos += scidLen
    
    # Token (only for Initial)
    # Type extraction: (FirstByte & 0x30) >> 4
    # Initial is 0x0
    let packetType = QuicPacketType((firstByte and 0x30) shr 4)
    result.longHeader.packetType = packetType
    
    if packetType == ptInitial:
      let tokenLen = int(readVarInt(data, pos))
      if pos + tokenLen > data.len: return
      result.longHeader.token = data[pos ..< pos + tokenLen]
      pos += tokenLen
      
    # Payload Length
    let lenVal = readVarInt(data, pos)
    result.longHeader.payloadLength = lenVal
    
    # Rest is Packet Number + Payload (encrypted in real QUIC)
    # For skeleton, we just slurp the rest
    if pos < data.len:
      result.payload = data[pos ..^ 1]
      
  else:
    # Short Header (Not implemented fully in Phase 2)
    result.longHeader.packetType = ptShort
    result.payload = data

type
  UnprotectedHeader* = object
    firstByte*: byte
    version*: uint32
    destConnectionId*: ConnectionId
    srcConnectionId*: ConnectionId
    payloadOffset*: int
    payloadLength*: uint64
    packetLength*: int

proc parseUnprotectedHeader*(data: openArray[byte]): UnprotectedHeader =
  if data.len == 0: return
  
  var pos = 0
  result.firstByte = data[pos]
  pos += 1
  
  if (result.firstByte and 0x80) != 0:
    # Long Header
    if pos + 4 > data.len: return
    var ver: uint32
    bigEndian32(addr ver, unsafeAddr data[pos])
    result.version = ver
    pos += 4
    
    # DCID Len
    if pos >= data.len: return
    let dcidLen = int(data[pos])
    pos += 1
    if pos + dcidLen > data.len: return
    result.destConnectionId = data[pos ..< pos + dcidLen]
    pos += dcidLen
    
    # SCID Len
    if pos >= data.len: return
    let scidLen = int(data[pos])
    pos += 1
    if pos + scidLen > data.len: return
    result.srcConnectionId = data[pos ..< pos + scidLen]
    pos += scidLen

    # Token Length (Initial Only)
    let packetType = QuicPacketType((result.firstByte and 0x30) shr 4)
    if packetType == ptInitial:
      let tokenLen = int(readVarInt(data, pos)) # readVarInt updates `pos`
      pos += tokenLen
      
    # Payload Length (VarInt)
    # RFC 9001: We need to parse Length to know where payload ends, or just trust buffer?
    # Length field is part of the Unprotected Header?
    # Actually, the Length field itself is NOT protected, but it tells us the size.
    # We parse it here to be helpful.
    let payloadLen = readVarInt(data, pos)
    result.payloadLength = payloadLen

    # Now `pos` is pointing to Packet Number (Protected)
    result.payloadOffset = pos
    let totalLen = pos + int(payloadLen)
    if totalLen > 0 and totalLen <= data.len:
      result.packetLength = totalLen
    else:
      result.packetLength = data.len
  else:
    result.packetLength = data.len

proc splitCoalescedPackets*(data: openArray[byte]): seq[seq[byte]] =
  if data.len == 0:
    return
  var pos = 0
  while pos < data.len:
    let header = parseUnprotectedHeader(data.toOpenArray(pos, data.high))
    let remainingLen = data.len - pos
    var packetLen = header.packetLength
    if packetLen <= 0 or packetLen > remainingLen:
      packetLen = remainingLen
    let nextPos = pos + packetLen
    if nextPos <= pos or nextPos > data.len:
      break
    result.add(@(data.toOpenArray(pos, nextPos - 1)))
    pos = nextPos

proc parseCryptoFrame*(data: openArray[byte], pos: var int): CryptoFrame =
  # Assumes caller checked Type == 0x06
  # Offset (VarInt)
  result.offset = readVarInt(data, pos)
  # Length (VarInt)
  result.length = readVarInt(data, pos)
  # Data
  if pos + int(result.length) <= data.len:
    result.data = @(data[pos ..< pos + int(result.length)])
    pos += int(result.length)

proc parseStreamFrame*(data: openArray[byte], pos: var int): StreamFrame =
  ## Parse a STREAM frame starting at the frame type byte.
  if pos >= data.len:
    return
  let typeByte = data[pos]
  inc pos

  result.fin = (typeByte and 0x01'u8) != 0
  let hasLen = (typeByte and 0x02'u8) != 0
  let hasOffset = (typeByte and 0x04'u8) != 0

  result.streamId = readVarInt(data, pos)
  if hasOffset:
    result.offset = readVarInt(data, pos)
  else:
    result.offset = 0

  if hasLen:
    result.length = readVarInt(data, pos)
  else:
    result.length = uint64(max(0, data.len - pos))

  if result.length == 0:
    result.data = @[]
    return

  if pos + int(result.length) <= data.len:
    result.data = @(data[pos ..< pos + int(result.length)])
    pos += int(result.length)
  else:
    result.length = 0
    result.data = @[]

proc parseAckFrame*(data: openArray[byte], pos: var int): AckFrame =
  ## Parse an ACK frame and materialize explicit ACK ranges.
  if pos >= data.len:
    return
  let frameType = data[pos]
  if frameType != 0x02'u8 and frameType != 0x03'u8:
    return
  inc pos
  result.largestAcked = readVarInt(data, pos)
  result.delay = readVarInt(data, pos)
  result.rangeCount = readVarInt(data, pos)
  result.firstRange = readVarInt(data, pos)
  result.ranges = @[]
  var currentLargest = result.largestAcked
  let firstSmallest =
    if result.firstRange > currentLargest: 0'u64
    else: currentLargest - result.firstRange
  result.ranges.add(AckRange(smallest: firstSmallest, largest: currentLargest))
  if result.rangeCount > 0:
    for _ in 0 ..< int(result.rangeCount):
      let gap = readVarInt(data, pos)
      let ackRangeLength = readVarInt(data, pos)
      if currentLargest <= gap + 1:
        break
      currentLargest = currentLargest - gap - 2
      let smallest =
        if ackRangeLength > currentLargest: 0'u64
        else: currentLargest - ackRangeLength
      result.ranges.add(AckRange(smallest: smallest, largest: currentLargest))

proc parseDatagramFrame*(data: openArray[byte], pos: var int): DatagramFrame =
  if pos >= data.len:
    return
  let frameType = data[pos]
  if frameType != 0x30'u8 and frameType != 0x31'u8:
    return
  inc pos
  result.containsLength = frameType == 0x31'u8
  if result.containsLength:
    result.length = readVarInt(data, pos)
  else:
    result.length = uint64(max(0, data.len - pos))
  if result.length == 0:
    result.data = @[]
    return
  if pos + int(result.length) <= data.len:
    result.data = @(data[pos ..< pos + int(result.length)])
    pos += int(result.length)
  else:
    result.length = 0
    result.data = @[]

proc parseResetStreamFrame*(data: openArray[byte], pos: var int): ResetStreamFrame =
  if pos >= data.len or data[pos] != 0x04'u8:
    return
  inc pos
  result.streamId = readVarInt(data, pos)
  result.applicationErrorCode = readVarInt(data, pos)
  result.finalSize = readVarInt(data, pos)

proc parseStopSendingFrame*(data: openArray[byte], pos: var int): StopSendingFrame =
  if pos >= data.len or data[pos] != 0x05'u8:
    return
  inc pos
  result.streamId = readVarInt(data, pos)
  result.applicationErrorCode = readVarInt(data, pos)

proc parseMaxDataFrame*(data: openArray[byte], pos: var int): MaxDataFrame =
  if pos >= data.len or data[pos] != 0x10'u8:
    return
  inc pos
  result.maximumData = readVarInt(data, pos)

proc parseMaxStreamDataFrame*(data: openArray[byte], pos: var int): MaxStreamDataFrame =
  if pos >= data.len or data[pos] != 0x11'u8:
    return
  inc pos
  result.streamId = readVarInt(data, pos)
  result.maximumStreamData = readVarInt(data, pos)

proc parsePathChallengeFrame*(data: openArray[byte], pos: var int): PathChallengeFrame =
  if pos >= data.len or data[pos] != 0x1A'u8:
    return
  inc pos
  if pos + 8 > data.len:
    return
  for i in 0 .. 7:
    result.data[i] = data[pos + i]
  pos += 8

proc parsePathResponseFrame*(data: openArray[byte], pos: var int): PathResponseFrame =
  if pos >= data.len or data[pos] != 0x1B'u8:
    return
  inc pos
  if pos + 8 > data.len:
    return
  for i in 0 .. 7:
    result.data[i] = data[pos + i]
  pos += 8

proc parseNewConnectionIdFrame*(data: openArray[byte], pos: var int): NewConnectionIdFrame =
  if pos >= data.len or data[pos] != 0x18'u8:
    return
  inc pos
  result.sequence = readVarInt(data, pos)
  result.retirePriorTo = readVarInt(data, pos)
  if pos >= data.len:
    return
  let cidLen = int(data[pos])
  inc pos
  if cidLen < 0 or pos + cidLen + 16 > data.len:
    result.connectionId = @[]
    return
  result.connectionId = @(data[pos ..< pos + cidLen])
  pos += cidLen
  for i in 0 .. 15:
    result.statelessResetToken[i] = data[pos + i]
  pos += 16

proc parseRetireConnectionIdFrame*(data: openArray[byte], pos: var int): RetireConnectionIdFrame =
  if pos >= data.len or data[pos] != 0x19'u8:
    return
  inc pos
  result.sequence = readVarInt(data, pos)

proc parseHandshakeDoneFrame*(data: openArray[byte], pos: var int): HandshakeDoneFrame =
  if pos >= data.len or data[pos] != 0x1E'u8:
    return
  inc pos
  result.present = true

proc parseConnectionCloseFrame*(data: openArray[byte], pos: var int): ConnectionCloseFrame =
  if pos >= data.len:
    return
  let frameType = data[pos]
  if frameType != 0x1C'u8 and frameType != 0x1D'u8:
    return
  inc pos
  result.applicationClose = frameType == 0x1D'u8
  result.errorCode = readVarInt(data, pos)
  result.hasFrameType = not result.applicationClose
  if result.hasFrameType:
    result.frameType = readVarInt(data, pos)
  let reasonLen = int(readVarInt(data, pos))
  if reasonLen < 0 or pos + reasonLen > data.len:
    return
  result.reasonPhrase = newString(reasonLen)
  for i in 0 ..< reasonLen:
    result.reasonPhrase[i] = char(data[pos + i])
  pos += reasonLen

proc encodeAckFrame*(largestAcked: uint64, delay: uint64): seq[byte] =
  # RFC 9000 Section 19.3.1. ACK Frame
  # Type (0x02)
  result.add(0x02'u8)
  
  # Largest Acknowledged (VarInt)
  result.writeVarInt(largestAcked)
  
  # ACK Delay (VarInt)
  result.writeVarInt(delay)
  
  # ACK Range Count (VarInt)
  # 0 means just the First Block
  result.writeVarInt(0)
  
  # First ACK Range (VarInt)
  # Number of packets preceding Largest Acked that are also acked.
  # For cumulative ack from 0 to LargestAcked, Range = LargestAcked.
  result.writeVarInt(largestAcked) 
  # Note: This implies 0..LargestAcked are ALL received.

proc encodeAckFrame*(ranges: openArray[AckRange], delay: uint64): seq[byte] =
  ## Encode explicit ACK ranges using RFC 9000 Section 19.3.1 framing.
  if ranges.len == 0:
    return encodeAckFrame(0'u64, delay)

  var ordered = @ranges
  if ordered.len > 1:
    ordered.sort(proc(a, b: AckRange): int = cmp(b.largest, a.largest))

  let largestAcked = ordered[0].largest
  let firstRange =
    if ordered[0].largest >= ordered[0].smallest:
      ordered[0].largest - ordered[0].smallest
    else:
      0'u64

  result.add(0x02'u8)
  result.writeVarInt(largestAcked)
  result.writeVarInt(delay)
  result.writeVarInt(uint64(max(0, ordered.len - 1)))
  result.writeVarInt(firstRange)

  var previousSmallest = ordered[0].smallest
  for i in 1 ..< ordered.len:
    let current = ordered[i]
    let gap =
      if previousSmallest > current.largest:
        previousSmallest - current.largest - 2'u64
      else:
        0'u64
    let ackRangeLength =
      if current.largest >= current.smallest:
        current.largest - current.smallest
      else:
        0'u64
    result.writeVarInt(gap)
    result.writeVarInt(ackRangeLength)
    previousSmallest = current.smallest
