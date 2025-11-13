{.used.}

# Nim-LibP2P

import unittest2
import pkg/results

import ../libp2p/[
  routing_record,
  multiaddress,
  peerid,
  signed_envelope,
  crypto/crypto,
]

suite "peer record":
  test "encode/decode roundtrip":
    let address = MultiAddress.init("/ip4/127.0.0.1/tcp/4001").tryGet()
    let rng = newRng()
    let privateKey = PrivateKey.random(rng[]).expect("private key")
    let peerId = PeerId.init(privateKey).expect("peer id")

    let record = PeerRecord.init(peerId, @[address], seqNo = 42)
    let encoded = record.encode()
    let decoded = PeerRecord.decode(encoded).expect("decode record")

    check decoded.peerId == record.peerId
    check decoded.seqNo == record.seqNo
    check decoded.addresses.len == 1
    check decoded.addresses[0].address == address

  test "signed peer record":
    let address = MultiAddress.init("/ip4/10.0.0.2/udp/3000/quic-v1").tryGet()
    let rng = newRng()
    let privateKey = PrivateKey.random(rng[]).expect("private key")
    let peerId = PeerId.init(privateKey).expect("peer id")

    let record = PeerRecord.init(peerId, @[address])
    let signed = SignedPayload[PeerRecord].init(privateKey, record).expect("signed record")
    let encoded = signed.encode().expect("encode signed record")
    let decoded = SignedPayload[PeerRecord].decode(encoded).expect("decode signed record")

    check decoded.data.peerId == peerId
    check decoded.data.addresses.len == 1
    check decoded.data.addresses[0].address == address
    check peerId.match(decoded.envelope.publicKey)
