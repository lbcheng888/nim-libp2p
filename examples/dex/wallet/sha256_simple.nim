# Simple SHA-256 implementation
import std/[endians]

type
  Sha256State* = object
    count: uint64
    state: array[8, uint32]
    buf: array[64, byte]

const K: array[64, uint32] = [
  0x428a2f98'u32, 0x71374491'u32, 0xb5c0fbcf'u32, 0xe9b5dba5'u32,
  0x3956c25b'u32, 0x59f111f1'u32, 0x923f82a4'u32, 0xab1c5ed5'u32,
  0xd807aa98'u32, 0x12835b01'u32, 0x243185be'u32, 0x550c7dc3'u32,
  0x72be5d74'u32, 0x80deb1fe'u32, 0x9bdc06a7'u32, 0xc19bf174'u32,
  0xe49b69c1'u32, 0xefbe4786'u32, 0x0fc19dc6'u32, 0x240ca1cc'u32,
  0x2de92c6f'u32, 0x4a7484aa'u32, 0x5cb0a9dc'u32, 0x76f988da'u32,
  0x983e5152'u32, 0xa831c66d'u32, 0xb00327c8'u32, 0xbf597fc7'u32,
  0xc6e00bf3'u32, 0xd5a79147'u32, 0x06ca6351'u32, 0x14292967'u32,
  0x27b70a85'u32, 0x2e1b2138'u32, 0x4d2c6dfc'u32, 0x53380d13'u32,
  0x650a7354'u32, 0x766a0abb'u32, 0x81c2c92e'u32, 0x92722c85'u32,
  0xa2bfe8a1'u32, 0xa81a664b'u32, 0xc24b8b70'u32, 0xc76c51a3'u32,
  0xd192e819'u32, 0xd6990624'u32, 0xf40e3585'u32, 0x106aa070'u32,
  0x19a4c116'u32, 0x1e376c08'u32, 0x2748774c'u32, 0x34b0bcb5'u32,
  0x391c0cb3'u32, 0x4ed8aa4a'u32, 0x5b9cca4f'u32, 0x682e6ff3'u32,
  0x748f82ee'u32, 0x78a5636f'u32, 0x84c87814'u32, 0x8cc70208'u32,
  0x90befffa'u32, 0xa4506ceb'u32, 0xbef9a3f7'u32, 0xc67178f2'u32
]

proc rotr(x: uint32, n: int): uint32 {.inline.} = (x shr n) or (x shl (32 - n))
proc ch(x, y, z: uint32): uint32 {.inline.} = (x and y) or ((not x) and z)
proc maj(x, y, z: uint32): uint32 {.inline.} = (x and y) or (x and z) or (y and z)
proc sigma0(x: uint32): uint32 {.inline.} = rotr(x, 2) xor rotr(x, 13) xor rotr(x, 22)
proc sigma1(x: uint32): uint32 {.inline.} = rotr(x, 6) xor rotr(x, 11) xor rotr(x, 25)
proc gamma0(x: uint32): uint32 {.inline.} = rotr(x, 7) xor rotr(x, 18) xor (x shr 3)
proc gamma1(x: uint32): uint32 {.inline.} = rotr(x, 17) xor rotr(x, 19) xor (x shr 10)

proc transform(ctx: var Sha256State) =
  var w: array[64, uint32]
  for i in 0..15:
    w[i] = (uint32(ctx.buf[i*4]) shl 24) or (uint32(ctx.buf[i*4+1]) shl 16) or
           (uint32(ctx.buf[i*4+2]) shl 8) or uint32(ctx.buf[i*4+3])
  for i in 16..63:
    w[i] = gamma1(w[i-2]) + w[i-7] + gamma0(w[i-15]) + w[i-16]

  var a = ctx.state[0]
  var b = ctx.state[1]
  var c = ctx.state[2]
  var d = ctx.state[3]
  var e = ctx.state[4]
  var f = ctx.state[5]
  var g = ctx.state[6]
  var h = ctx.state[7]

  for i in 0..63:
    let t1 = h + sigma1(e) + ch(e, f, g) + K[i] + w[i]
    let t2 = sigma0(a) + maj(a, b, c)
    h = g
    g = f
    f = e
    e = d + t1
    d = c
    c = b
    b = a
    a = t1 + t2

  ctx.state[0] += a
  ctx.state[1] += b
  ctx.state[2] += c
  ctx.state[3] += d
  ctx.state[4] += e
  ctx.state[5] += f
  ctx.state[6] += g
  ctx.state[7] += h

proc init*(ctx: var Sha256State) =
  ctx.count = 0
  ctx.state = [0x6a09e667'u32, 0xbb67ae85'u32, 0x3c6ef372'u32, 0xa54ff53a'u32,
               0x510e527f'u32, 0x9b05688c'u32, 0x1f83d9ab'u32, 0x5be0cd19'u32]

proc update*(ctx: var Sha256State, data: openArray[byte]) =
  if data.len == 0: return
  var i = 0
  var left = data.len
  while left > 0:
    let offset = int(ctx.count mod 64)
    let chunk = min(64 - offset, left)
    copyMem(addr ctx.buf[offset], unsafeAddr data[i], chunk)
    ctx.count += uint64(chunk)
    i += chunk
    left -= chunk
    if (ctx.count mod 64) == 0:
      transform(ctx)

proc final*(ctx: var Sha256State, result: var array[32, byte]) =
  let count = ctx.count
  let offset = int(count mod 64)
  ctx.buf[offset] = 0x80
  if offset >= 56:
    for i in offset+1 .. 63: ctx.buf[i] = 0
    transform(ctx)
    for i in 0 .. 55: ctx.buf[i] = 0
  else:
    for i in offset+1 .. 55: ctx.buf[i] = 0
  
  # Big endian length
  let bitlen = count * 8
  ctx.buf[56] = byte((bitlen shr 56) and 0xff)
  ctx.buf[57] = byte((bitlen shr 48) and 0xff)
  ctx.buf[58] = byte((bitlen shr 40) and 0xff)
  ctx.buf[59] = byte((bitlen shr 32) and 0xff)
  ctx.buf[60] = byte((bitlen shr 24) and 0xff)
  ctx.buf[61] = byte((bitlen shr 16) and 0xff)
  ctx.buf[62] = byte((bitlen shr 8) and 0xff)
  ctx.buf[63] = byte(bitlen and 0xff)
  transform(ctx)

  for i in 0..7:
    result[i*4] = byte((ctx.state[i] shr 24) and 0xff)
    result[i*4+1] = byte((ctx.state[i] shr 16) and 0xff)
    result[i*4+2] = byte((ctx.state[i] shr 8) and 0xff)
    result[i*4+3] = byte(ctx.state[i] and 0xff)







