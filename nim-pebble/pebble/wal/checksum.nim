# CRC32C（Castagnoli）校验实现，保持与 Go 版 internal/crc 包兼容。

const
  crc32cPoly = 0x82F63B78'u32

proc buildTable(): array[256, uint32] =
  var tbl: array[256, uint32]
  var i = 0
  while i < 256:
    var crc = uint32(i)
    var bit = 0
    while bit < 8:
      if (crc and 1'u32) != 0:
        crc = (crc shr 1) xor crc32cPoly
      else:
        crc = crc shr 1
      inc bit
    tbl[i] = crc
    inc i
  tbl

const
  crc32cTable* = buildTable()

proc extend*(crc: uint32; data: openArray[byte]): uint32 =
  ## 基于 LevelDB/Pebble 约定的增量扩展接口。
  var c = crc xor 0xFFFFFFFF'u32
  for b in data:
    let idx = (c xor uint32(b)) and 0xFF'u32
    c = crc32cTable[int(idx)] xor (c shr 8)
  result = c xor 0xFFFFFFFF'u32

proc fromByte*(value: byte): array[1, byte] =
  [value]

proc fromUInt32*(value: uint32): array[4, byte] =
  [
    byte(value and 0xFF'u32),
    byte((value shr 8) and 0xFF'u32),
    byte((value shr 16) and 0xFF'u32),
    byte((value shr 24) and 0xFF'u32)
  ]

proc fromUInt64*(value: uint64): array[8, byte] =
  [
    byte(value and 0xFF'u64),
    byte((value shr 8) and 0xFF'u64),
    byte((value shr 16) and 0xFF'u64),
    byte((value shr 24) and 0xFF'u64),
    byte((value shr 32) and 0xFF'u64),
    byte((value shr 40) and 0xFF'u64),
    byte((value shr 48) and 0xFF'u64),
    byte((value shr 56) and 0xFF'u64)
  ]

proc maskChecksum*(crc: uint32): uint32 =
  ## 参考 Go internal/crc.Value() 的掩码规则。
  ((crc shr 15) or (crc shl 17)) + 0xA282EAD8'u32

proc value*(data: openArray[byte]): uint32 =
  ## 便捷方法：一次性计算掩码后的 CRC。
  maskChecksum(extend(0'u32, data))
