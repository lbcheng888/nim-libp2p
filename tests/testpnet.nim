when defined(libp2p_run_pnet_tests):
  {.used.}

  # Nim-LibP2P
  # Copyright (c) 2025
  # Licensed under either of MIT or Apache-2.0

  import std/[sequtils, strformat, base64]
  import unittest2
  import chronos

  import ./utils/async_tests

  import ../libp2p/pnet
  import ../libp2p/crypto/[crypto, xsalsa20]
  import ../libp2p/stream/bridgestream

  proc toKey(bytes: seq[byte]): PrivateNetworkKey =
    assert bytes.len == 32
    var key: PrivateNetworkKey
    for i in 0 ..< 32:
      key[i] = bytes[i]
    key

  suite "pnet":
    test "loadPrivateNetworkKey base16/base64/bin":
      var raw = newSeq[byte](32)
      for i in 0 ..< raw.len:
        raw[i] = byte(i)
      let key = toKey(raw)
      let hexContent = fmt"{SwarmKeyHeader}\n{EncodingBase16}\n{toHex(raw)}\n"
      check loadPrivateNetworkKeyFromString(hexContent) ==
        Result[PrivateNetworkKey, string].ok(key)

      let base64Content = fmt"{SwarmKeyHeader}\n{EncodingBase64}\n{base64.encode(raw)}\n"
      check loadPrivateNetworkKeyFromString(base64Content) ==
        Result[PrivateNetworkKey, string].ok(key)

      var binContent = SwarmKeyHeader & "\n" & EncodingBin & "\n"
      for b in raw:
        binContent.add(char(b))
      check loadPrivateNetworkKeyFromString(binContent) ==
        Result[PrivateNetworkKey, string].ok(key)

    test "xsalsa20 reference vectors":
      let nonce = "24-byte nonce for xsalsa"
      let keyData = "this is 32-byte key for xsalsa20"
      var key: array[32, byte]
      for i in 0 ..< key.len:
        key[i] = byte(keyData[i])
      var nonceArr: array[24, byte]
      for i in 0 ..< nonceArr.len:
        nonceArr[i] = byte(nonce[i])

      var stream = initXSalsa20Stream(key, nonceArr)
      var input = "Hello world!".toSeq.mapIt(byte(it))
      var output = newSeq[byte](input.len)
      stream.xorKeyStream(output, input)
      check output == @[
        byte 0x00, 0x2d, 0x45, 0x13, 0x84, 0x3f, 0xc2, 0x40, 0xc4, 0x01, 0xe5, 0x41]

      stream = initXSalsa20Stream(key, nonceArr)
      var zeros = newSeq[byte](64)
      var out2 = newSeq[byte](64)
      stream.xorKeyStream(out2, zeros)
      check out2 == @[
        byte 0x48, 0x48, 0x29, 0x7f, 0xeb, 0x1f, 0xb5, 0x2f, 0xb6, 0x6d, 0x81, 0x60, 0x9b, 0xd5,
        0x47, 0xfa, 0xbc, 0xbe, 0x70, 0x26, 0xed, 0xc8, 0xb5, 0xe5, 0xe4, 0x49, 0xd0, 0x88,
        0xbf, 0xa6, 0x9c, 0x08, 0x8f, 0x5d, 0x8d, 0xa1, 0xd7, 0x91, 0x26, 0x7c, 0x2c, 0x19,
        0x5a, 0x7f, 0x8c, 0xae, 0x9c, 0x4b, 0x40, 0x50, 0xd0, 0x8c, 0xe6, 0xd3, 0xa1, 0x51,
        0xec, 0x26, 0x5f, 0x3a, 0x58, 0xe4, 0x76, 0x48]

    asyncTest "protected connection roundtrip":
      var raw = newSeq[byte](32)
      for i in 0 ..< raw.len:
        raw[i] = byte(i + 1)
      let key = toKey(raw)
      var rng = newRng()
      check not rng.isNil
      let protector = newConnectionProtector(key, rng)

      let (rawA, rawB) = bridgedConnections(
        closeTogether = false, dirA = Direction.Out, dirB = Direction.In
      )
      let connA = protector.protect(rawA)
      let connB = protector.protect(rawB)

      let payload = @[byte 0x01, byte 0x02, byte 0x03, byte 0x04, byte 0x05]

      await connA.write(payload)
      var buf = newSeqUninit[byte](payload.len)
      await connB.readExactly(addr buf[0], buf.len)
      check buf == payload

      # Test opposite direction to ensure both send nonces correctly.
      let payload2 = @[byte 0xaa, byte 0xbb, byte 0xcc]
      await connB.write(payload2)
      var buf2 = newSeqUninit[byte](payload2.len)
      await connA.readExactly(addr buf2[0], buf2.len)
      check buf2 == payload2

      await connA.close()
      await connB.close()

else:
  import ./helpers
  suite "pnet":
    test "pnet tests disabled":
      skip()
