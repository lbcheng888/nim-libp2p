import unittest2

import ../libp2p/transports/qpackhuffman

suite "QPACK Huffman":
  test "round-trips common HTTP/3 values":
    let samples = [
      "",
      ":method",
      ":scheme",
      "webtransport",
      "sec-webtransport-http3-draft",
      "application/json",
      "/nim-tsnet-control/v1/register",
      "64.176.84.12",
      "relay",
    ]

    for sample in samples:
      let encoded = qpackHuffmanEncode(sample)
      check qpackHuffmanEncodedLen(sample) == encoded.len.uint64
      check qpackHuffmanDecode(encoded) == sample

  test "rejects invalid padding":
    expect(ValueError):
      discard qpackHuffmanDecode(@[0x00'u8])
