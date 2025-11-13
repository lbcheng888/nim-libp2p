# Peer Record 与 Signed Peer Record

- 模块：`libp2p/routing_record.nim`
- 规范：对应 [libp2p Peer Record](https://github.com/libp2p/specs/blob/master/peer-id/peer-record.md)，用于在 PubSub、DHT 等协议中传播带签名的地址信息。

## 生成 PeerRecord
```nim
import libp2p/[routing_record, multiaddress, peerid, crypto/crypto]

let rng = newRng()
let privKey = PrivateKey.random(rng[]).expect("key")
let peerId = PeerId.init(privKey).expect("peer id")
let addr = MultiAddress.init("/ip4/127.0.0.1/tcp/4001").tryGet()

let record = PeerRecord.init(peerId, @[addr])
let encoded = record.encode()
let decoded = PeerRecord.decode(encoded).expect("decode")
```

## 签名封装
```nim
import libp2p/signed_envelope

let signed = SignedPayload[PeerRecord].init(privKey, record).expect("sign")
let payload = signed.encode().expect("encode signed")
let verified = SignedPayload[PeerRecord].decode(payload).expect("decode signed")

doAssert verified.data.peerId == peerId
doAssert verified.envelope.publicKey.match(privKey)
```

`SignedPayload` 自动验证签名并检查 `payloadDomain` / `payloadType`。

## 测试
- `tests/testpeerrecord.nim` 覆盖编码/解码与 SignedPeerRecord roundtrip。
