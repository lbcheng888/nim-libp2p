## Pure Nim QUIC Protocol Definitions (RFC 9000)
## This module handles packet wire formats, header encoding/decoding, and basic frame structures.

import std/endians

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
  let lengthVal = pnLen + uint64(payload.len)
  result.writeVarInt(lengthVal)

  # Packet Number (4 bytes, unencrypted for skeleton)
  # Note: Real QUIC encrypts this. We are writing it raw for the skeleton to establish structure.
  var pnRaw = packetNumber # In 4 bytes
  # We need to write it in Network Byte Order, masked by header protection in real world
  # For skeleton, just write big endian
  result.writeUint32(pnRaw)

  # Payload (Frames)
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

  AckFrame* = object
    largestAcked*: uint64
    delay*: uint64
    rangeCount*: uint64
    firstRange*: uint64

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

proc parseUnprotectedHeader*(data: seq[byte]): UnprotectedHeader =
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
    let _ = readVarInt(data, pos)
    
    # Now `pos` is pointing to Packet Number (Protected)
    result.payloadOffset = pos

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

