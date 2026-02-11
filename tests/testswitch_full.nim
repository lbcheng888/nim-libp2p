 # Nim-Libp2p
 # Copyright (c) 2023 Status Research & Development GmbH
 # Licensed under either of
 #  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
 #  * MIT license ([LICENSE-MIT](LICENSE-MIT))
 # at your option.
 # This file may not be copied, modified, or distributed except according to
 # those terms.

import results, sequtils
import chronos
import stew/byteutils
import
  ../libp2p/[
    errors,
    dial,
    switch,
    multistream,
    builders,
    stream/bufferstream,
    stream/connection,
    multicodec,
    multiaddress,
    peerinfo,
    crypto/crypto,
    protocols/protocol,
    protocols/secure/secure,
    muxers/muxer,
    muxers/mplex/lpchannel,
    stream/lpstream,
    nameresolving/mockresolver,
    nameresolving/nameresolver,
    stream/chronosstream,
    utils/semaphore,
    transports/tcptransport,
    transports/wstransport,
    transports/quictransport,
  ]
import ./helpers

const TestCodec = "/test/proto/1.0.0"

type TestProto = ref object of LPProtocol

suite "Switch":
  teardown:
    checkTrackers()

  asyncTest "e2e use switch dial proto string":
    let done: Future[void].Raising([]) =
      cast[Future[void].Raising([])](newFuture[void]())
    proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        check "Hello!" == msg
        await conn.writeLp("Hello!")
      except LPStreamError:
        check false
      finally:
        await conn.close()
        done.complete()

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let switch1 = newStandardSwitch()
    switch1.mount(testProto)

    let switch2 = newStandardSwitch()
    await switch1.start()
    await switch2.start()

    let conn =
      await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, TestCodec)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await conn.writeLp("Hello!")
    let msg = string.fromBytes(await conn.readLp(1024))
    check "Hello!" == msg
    await conn.close()

    await allFuturesThrowing(done.wait(5.seconds), switch1.stop(), switch2.stop())

    check not switch1.isConnected(switch2.peerInfo.peerId)
    check not switch2.isConnected(switch1.peerInfo.peerId)

  asyncTest "e2e use switch dial proto string with custom matcher":
    let done: Future[void].Raising([]) =
      cast[Future[void].Raising([])](newFuture[void]())
    proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        check "Hello!" == msg
        await conn.writeLp("Hello!")
      except LPStreamError:
        check false
      finally:
        await conn.close()
        done.complete()

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let callProto = TestCodec & "/pew"

    proc match(proto: string): bool {.gcsafe.} =
      proto == callProto

    let switch1 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    switch1.mount(testProto, match)

    let switch2 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    await switch1.start()
    await switch2.start()

    let conn =
      await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, callProto)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await conn.writeLp("Hello!")
    let msg = string.fromBytes(await conn.readLp(1024))
    check "Hello!" == msg
    await conn.close()

    await allFuturesThrowing(done.wait(5.seconds), switch1.stop(), switch2.stop())

    check not switch1.isConnected(switch2.peerInfo.peerId)
    check not switch2.isConnected(switch1.peerInfo.peerId)

  asyncTest "e2e should not leak bufferstreams and connections on channel close":
    let done: Future[void].Raising([]) =
      cast[Future[void].Raising([])](newFuture[void]())
    proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        check "Hello!" == msg
        await conn.writeLp("Hello!")
      except LPStreamError:
        check false
      finally:
        await conn.close()
        done.complete()

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let switch1 = newStandardSwitch()
    switch1.mount(testProto)

    let switch2 = newStandardSwitch()
    await switch1.start()
    await switch2.start()

    let conn =
      await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, TestCodec)

    await conn.writeLp("Hello!")
    await conn.close()

    await allFuturesThrowing(done.wait(5.seconds), switch1.stop(), switch2.stop())

