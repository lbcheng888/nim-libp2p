{.used.}

# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

import std/random
import unittest2
import ../libp2p/crypto/crypto
import ../libp2p/protocols/pubsub/gossipsub/sharding
import pkg/results

proc randomBytes(len: int): seq[byte] =
  var res = newSeq[byte](len)
  for i in 0 ..< len:
    res[i] = byte(rand(255))
  res

suite "gossipsub sharding":
  setup:
    randomize(42)

  test "encode/decode without loss":
    let data = randomBytes(128 * 3 + 17)
    let rng = newRng()
    check not rng.isNil
    let cfg = ShardConfig(chunkSize: 128, redundancy: 2)
    let packets = encodeShards(data, cfg, rng, SessionId(1))
    var session: ShardSession
    for packet in packets:
      let shardOpt = decodeShardPayload(packet)
      check shardOpt.isSome()
      let shard = shardOpt.get()
      if session.isNil:
        session = newShardSession(shard)
      check session.acceptShard(shard)
      let assembled = session.tryAssemble()
      if assembled.isSome():
        let payload = assembled.get()
        check payload.len == data.len
        check payload == data
        break

  test "decode with missing identity chunk":
    let data = randomBytes(64 * 5 + 5)
    let rng = newRng()
    check not rng.isNil
    let cfg = ShardConfig(chunkSize: 64, redundancy: 3)
    var packets = encodeShards(data, cfg, rng, SessionId(99))
    # Drop one of the identity shards
    packets.delete(0)
    var session: ShardSession
    var assembledData = Opt[seq[byte]].err()
    for packet in packets:
      let shardOpt = decodeShardPayload(packet)
      check shardOpt.isSome()
      let shard = shardOpt.get()
      if session.isNil:
        session = newShardSession(shard)
      discard session.acceptShard(shard)
      let assembled = session.tryAssemble()
      if assembled.isSome():
        assembledData = assembled
        break
    check assembledData.isSome()
    check assembledData.value().len == data.len
    check assembledData.value() == data
