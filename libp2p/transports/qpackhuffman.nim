type
  QpackHuffmanNode = ref object
    children: array[256, QpackHuffmanNode]
    isLeaf: bool
    codeLen: uint8
    sym: byte

const
  QpackHuffmanEosCode = 0x3fffffff'u32
  QpackHuffmanEosBits = 30'u8
  QpackHuffmanPadByte = byte(QpackHuffmanEosCode shr (QpackHuffmanEosBits - 8))

  QpackHuffmanCodes*: array[256, uint32] = [
    0x1ff8'u32, 0x7fffd8'u32, 0xfffffe2'u32, 0xfffffe3'u32, 0xfffffe4'u32,
    0xfffffe5'u32, 0xfffffe6'u32, 0xfffffe7'u32, 0xfffffe8'u32, 0xffffea'u32,
    0x3ffffffc'u32, 0xfffffe9'u32, 0xfffffea'u32, 0x3ffffffd'u32, 0xfffffeb'u32,
    0xfffffec'u32, 0xfffffed'u32, 0xfffffee'u32, 0xfffffef'u32, 0xffffff0'u32,
    0xffffff1'u32, 0xffffff2'u32, 0x3ffffffe'u32, 0xffffff3'u32, 0xffffff4'u32,
    0xffffff5'u32, 0xffffff6'u32, 0xffffff7'u32, 0xffffff8'u32, 0xffffff9'u32,
    0xffffffa'u32, 0xffffffb'u32, 0x14'u32, 0x3f8'u32, 0x3f9'u32, 0xffa'u32,
    0x1ff9'u32, 0x15'u32, 0xf8'u32, 0x7fa'u32, 0x3fa'u32, 0x3fb'u32, 0xf9'u32,
    0x7fb'u32, 0xfa'u32, 0x16'u32, 0x17'u32, 0x18'u32, 0x0'u32, 0x1'u32, 0x2'u32,
    0x19'u32, 0x1a'u32, 0x1b'u32, 0x1c'u32, 0x1d'u32, 0x1e'u32, 0x1f'u32,
    0x5c'u32, 0xfb'u32, 0x7ffc'u32, 0x20'u32, 0xffb'u32, 0x3fc'u32, 0x1ffa'u32,
    0x21'u32, 0x5d'u32, 0x5e'u32, 0x5f'u32, 0x60'u32, 0x61'u32, 0x62'u32,
    0x63'u32, 0x64'u32, 0x65'u32, 0x66'u32, 0x67'u32, 0x68'u32, 0x69'u32,
    0x6a'u32, 0x6b'u32, 0x6c'u32, 0x6d'u32, 0x6e'u32, 0x6f'u32, 0x70'u32,
    0x71'u32, 0x72'u32, 0xfc'u32, 0x73'u32, 0xfd'u32, 0x1ffb'u32, 0x7fff0'u32,
    0x1ffc'u32, 0x3ffc'u32, 0x22'u32, 0x7ffd'u32, 0x3'u32, 0x23'u32, 0x4'u32,
    0x24'u32, 0x5'u32, 0x25'u32, 0x26'u32, 0x27'u32, 0x6'u32, 0x74'u32,
    0x75'u32, 0x28'u32, 0x29'u32, 0x2a'u32, 0x7'u32, 0x2b'u32, 0x76'u32,
    0x2c'u32, 0x8'u32, 0x9'u32, 0x2d'u32, 0x77'u32, 0x78'u32, 0x79'u32,
    0x7a'u32, 0x7b'u32, 0x7ffe'u32, 0x7fc'u32, 0x3ffd'u32, 0x1ffd'u32,
    0xffffffc'u32, 0xfffe6'u32, 0x3fffd2'u32, 0xfffe7'u32, 0xfffe8'u32,
    0x3fffd3'u32, 0x3fffd4'u32, 0x3fffd5'u32, 0x7fffd9'u32, 0x3fffd6'u32,
    0x7fffda'u32, 0x7fffdb'u32, 0x7fffdc'u32, 0x7fffdd'u32, 0x7fffde'u32,
    0xffffeb'u32, 0x7fffdf'u32, 0xffffec'u32, 0xffffed'u32, 0x3fffd7'u32,
    0x7fffe0'u32, 0xffffee'u32, 0x7fffe1'u32, 0x7fffe2'u32, 0x7fffe3'u32,
    0x7fffe4'u32, 0x1fffdc'u32, 0x3fffd8'u32, 0x7fffe5'u32, 0x3fffd9'u32,
    0x7fffe6'u32, 0x7fffe7'u32, 0xffffef'u32, 0x3fffda'u32, 0x1fffdd'u32,
    0xfffe9'u32, 0x3fffdb'u32, 0x3fffdc'u32, 0x7fffe8'u32, 0x7fffe9'u32,
    0x1fffde'u32, 0x7fffea'u32, 0x3fffdd'u32, 0x3fffde'u32, 0xfffff0'u32,
    0x1fffdf'u32, 0x3fffdf'u32, 0x7fffeb'u32, 0x7fffec'u32, 0x1fffe0'u32,
    0x1fffe1'u32, 0x3fffe0'u32, 0x1fffe2'u32, 0x7fffed'u32, 0x3fffe1'u32,
    0x7fffee'u32, 0x7fffef'u32, 0xfffea'u32, 0x3fffe2'u32, 0x3fffe3'u32,
    0x3fffe4'u32, 0x7ffff0'u32, 0x3fffe5'u32, 0x3fffe6'u32, 0x7ffff1'u32,
    0x3ffffe0'u32, 0x3ffffe1'u32, 0xfffeb'u32, 0x7fff1'u32, 0x3fffe7'u32,
    0x7ffff2'u32, 0x3fffe8'u32, 0x1ffffec'u32, 0x3ffffe2'u32, 0x3ffffe3'u32,
    0x3ffffe4'u32, 0x7ffffde'u32, 0x7ffffdf'u32, 0x3ffffe5'u32, 0xfffff1'u32,
    0x1ffffed'u32, 0x7fff2'u32, 0x1fffe3'u32, 0x3ffffe6'u32, 0x7ffffe0'u32,
    0x7ffffe1'u32, 0x3ffffe7'u32, 0x7ffffe2'u32, 0xfffff2'u32, 0x1fffe4'u32,
    0x1fffe5'u32, 0x3ffffe8'u32, 0x3ffffe9'u32, 0xffffffd'u32, 0x7ffffe3'u32,
    0x7ffffe4'u32, 0x7ffffe5'u32, 0xfffec'u32, 0xfffff3'u32, 0xfffed'u32,
    0x1fffe6'u32, 0x3fffe9'u32, 0x1fffe7'u32, 0x1fffe8'u32, 0x7ffff3'u32,
    0x3fffea'u32, 0x3fffeb'u32, 0x1ffffee'u32, 0x1ffffef'u32, 0xfffff4'u32,
    0xfffff5'u32, 0x3ffffea'u32, 0x7ffff4'u32, 0x3ffffeb'u32, 0x7ffffe6'u32,
    0x3ffffec'u32, 0x3ffffed'u32, 0x7ffffe7'u32, 0x7ffffe8'u32, 0x7ffffe9'u32,
    0x7ffffea'u32, 0x7ffffeb'u32, 0xffffffe'u32, 0x7ffffec'u32, 0x7ffffed'u32,
    0x7ffffee'u32, 0x7ffffef'u32, 0x7fffff0'u32, 0x3ffffee'u32
  ]

  QpackHuffmanCodeLen*: array[256, uint8] = [
    13'u8, 23'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8, 24'u8,
    30'u8, 28'u8, 28'u8, 30'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8,
    28'u8, 28'u8, 30'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8, 28'u8,
    28'u8, 28'u8, 6'u8, 10'u8, 10'u8, 12'u8, 13'u8, 6'u8, 8'u8, 11'u8,
    10'u8, 10'u8, 8'u8, 11'u8, 8'u8, 6'u8, 6'u8, 6'u8, 5'u8, 5'u8, 5'u8,
    6'u8, 6'u8, 6'u8, 6'u8, 6'u8, 6'u8, 6'u8, 7'u8, 8'u8, 15'u8, 6'u8,
    12'u8, 10'u8, 13'u8, 6'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8,
    7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8,
    7'u8, 7'u8, 7'u8, 7'u8, 8'u8, 7'u8, 8'u8, 13'u8, 19'u8, 13'u8, 14'u8,
    6'u8, 15'u8, 5'u8, 6'u8, 5'u8, 6'u8, 5'u8, 6'u8, 6'u8, 6'u8, 5'u8,
    7'u8, 7'u8, 6'u8, 6'u8, 6'u8, 5'u8, 6'u8, 7'u8, 6'u8, 5'u8, 5'u8,
    6'u8, 7'u8, 7'u8, 7'u8, 7'u8, 7'u8, 15'u8, 11'u8, 14'u8, 13'u8, 28'u8,
    20'u8, 22'u8, 20'u8, 20'u8, 22'u8, 22'u8, 22'u8, 23'u8, 22'u8, 23'u8,
    23'u8, 23'u8, 23'u8, 23'u8, 24'u8, 23'u8, 24'u8, 24'u8, 22'u8, 23'u8,
    24'u8, 23'u8, 23'u8, 23'u8, 23'u8, 21'u8, 22'u8, 23'u8, 22'u8, 23'u8,
    23'u8, 24'u8, 22'u8, 21'u8, 20'u8, 22'u8, 22'u8, 23'u8, 23'u8, 21'u8,
    23'u8, 22'u8, 22'u8, 24'u8, 21'u8, 22'u8, 23'u8, 23'u8, 21'u8, 21'u8,
    22'u8, 21'u8, 23'u8, 22'u8, 23'u8, 23'u8, 20'u8, 22'u8, 22'u8, 22'u8,
    23'u8, 22'u8, 22'u8, 23'u8, 26'u8, 26'u8, 20'u8, 19'u8, 22'u8, 23'u8,
    22'u8, 25'u8, 26'u8, 26'u8, 26'u8, 27'u8, 27'u8, 26'u8, 24'u8, 25'u8,
    19'u8, 21'u8, 26'u8, 27'u8, 27'u8, 26'u8, 27'u8, 24'u8, 21'u8, 21'u8,
    26'u8, 26'u8, 28'u8, 27'u8, 27'u8, 27'u8, 20'u8, 24'u8, 20'u8, 21'u8,
    22'u8, 21'u8, 21'u8, 23'u8, 22'u8, 22'u8, 25'u8, 25'u8, 24'u8, 24'u8,
    26'u8, 23'u8, 26'u8, 27'u8, 26'u8, 26'u8, 27'u8, 27'u8, 27'u8, 27'u8,
    27'u8, 28'u8, 27'u8, 27'u8, 27'u8, 27'u8, 27'u8, 26'u8
  ]

proc newInternalNode(): QpackHuffmanNode =
  QpackHuffmanNode()

proc newLeafNode(sym: byte, codeLen: uint8): QpackHuffmanNode =
  QpackHuffmanNode(isLeaf: true, sym: sym, codeLen: codeLen)

proc buildQpackHuffmanRoot(): QpackHuffmanNode =
  result = newInternalNode()
  for sym in 0 .. 255:
    let code = QpackHuffmanCodes[sym]
    var codeLen = QpackHuffmanCodeLen[sym]
    var current = result
    while codeLen > 8:
      codeLen -= 8
      let idx = int((code shr codeLen) and 0xff'u32)
      if current.children[idx].isNil:
        current.children[idx] = newInternalNode()
      current = current.children[idx]
    let shift = 8 - int(codeLen)
    let start = int((code shl shift) and 0xff'u32)
    let leaf = newLeafNode(byte(sym), codeLen)
    for idx in start ..< start + (1 shl shift):
      current.children[idx] = leaf

let QpackHuffmanRoot = buildQpackHuffmanRoot()

proc qpackHuffmanEncodedLen*(value: string): uint64 {.gcsafe, raises: [].} =
  var bits = 0'u64
  for ch in value:
    bits += uint64(QpackHuffmanCodeLen[ord(ch)])
  (bits + 7) div 8

proc qpackHuffmanEncode*(value: string): seq[byte] {.gcsafe, raises: [].} =
  var bitCount = 0'u
  var buffer = 0'u64
  for ch in value:
    let codeLen = int(QpackHuffmanCodeLen[ord(ch)])
    buffer = (buffer shl codeLen) or uint64(QpackHuffmanCodes[ord(ch)])
    bitCount += codeLen.uint
    if bitCount >= 32'u:
      bitCount = bitCount mod 32'u
      let chunk = uint32(buffer shr bitCount)
      result.add(byte(chunk shr 24))
      result.add(byte(chunk shr 16))
      result.add(byte(chunk shr 8))
      result.add(byte(chunk))
  let remainder = bitCount mod 8'u
  if remainder > 0'u:
    let padding = 8'u - remainder
    buffer = (buffer shl padding) or uint64(QpackHuffmanPadByte shr remainder)
    bitCount += padding
  case bitCount div 8'u
  of 0'u:
    discard
  of 1'u:
    result.add(byte(buffer))
  of 2'u:
    let chunk = uint16(buffer)
    result.add(byte(chunk shr 8))
    result.add(byte(chunk))
  of 3'u:
    let chunk = uint16(buffer shr 8)
    result.add(byte(chunk shr 8))
    result.add(byte(chunk))
    result.add(byte(buffer))
  else:
    let chunk = uint32(buffer)
    result.add(byte(chunk shr 24))
    result.add(byte(chunk shr 16))
    result.add(byte(chunk shr 8))
    result.add(byte(chunk))

proc qpackHuffmanDecode*(data: openArray[byte]): string {.raises: [ValueError].} =
  var current = QpackHuffmanRoot
  var buffer = 0'u64
  var cbits = 0'u8
  var sbits = 0'u8

  for b in data:
    buffer = (buffer shl 8) or uint64(b)
    cbits += 8
    sbits += 8
    while cbits >= 8:
      let idx = int((buffer shr (cbits - 8)) and 0xff'u64)
      current = current.children[idx]
      if current.isNil:
        raise newException(ValueError, "invalid QPACK Huffman payload")
      if current.isLeaf:
        result.add(char(current.sym))
        cbits -= current.codeLen
        current = QpackHuffmanRoot
        sbits = cbits
      else:
        cbits -= 8

  while cbits > 0:
    let idx = int((buffer shl (8 - cbits)) and 0xff'u64)
    current = current.children[idx]
    if current.isNil:
      raise newException(ValueError, "invalid QPACK Huffman payload")
    if not current.isLeaf or current.codeLen > cbits:
      break
    result.add(char(current.sym))
    cbits -= current.codeLen
    current = QpackHuffmanRoot
    sbits = cbits

  if sbits > 7:
    raise newException(ValueError, "invalid QPACK Huffman padding")

  if cbits > 0:
    let mask = (1'u64 shl cbits) - 1'u64
    if (buffer and mask) != mask:
      raise newException(ValueError, "invalid QPACK Huffman padding")
