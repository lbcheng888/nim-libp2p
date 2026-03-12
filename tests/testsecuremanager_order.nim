import chronos
import unittest2
import stew/byteutils

import ../libp2p/[
  builders,
  switch,
  stream/connection,
  protocols/protocol,
  protocols/secure/secure,
  protocols/secure/noise,
  protocols/secure/tls,
]
import ./helpers

const TestCodec = "/test/secure-order/1.0.0"

type TestProto = ref object of LPProtocol

proc negotiatedSecureCodec(conn: Connection): string =
  var current = conn
  while not current.isNil:
    if current of TLSConnection:
      return TLSCodec
    if current of NoiseConnection:
      return NoiseCodec
    current = current.getWrapped()
  ""

suite "Secure Manager Order":
  teardown:
    checkTrackers()

  asyncTest "builder preserves secure manager preference order":
    let negotiatedServer = newFuture[string]()
    let done = newFuture[void]()

    proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
      try:
        if not negotiatedServer.finished:
          negotiatedServer.complete(negotiatedSecureCodec(conn))
        await conn.writeLp("pong")
      except LPStreamError:
        check false
      finally:
        await conn.close()
        if not done.finished:
          done.complete()

    let testProto = TestProto()
    testProto.codec = TestCodec
    testProto.handler = handle

    let listener = newStandardSwitch(
      secureManagers = [SecureProtocol.Tls, SecureProtocol.Noise]
    )
    listener.mount(testProto)

    let dialer = newStandardSwitch(
      secureManagers = [SecureProtocol.Tls, SecureProtocol.Noise]
    )

    await listener.start()
    await dialer.start()

    let conn =
      await dialer.dial(listener.peerInfo.peerId, listener.peerInfo.addrs, TestCodec)

    check negotiatedSecureCodec(conn) == TLSCodec
    try:
      check string.fromBytes(await conn.readLp(1024)) == "pong"
    except LPStreamError:
      check false

    check await negotiatedServer.withTimeout(5.seconds)
    check negotiatedServer.read() == TLSCodec

    await conn.close()
    await allFuturesThrowing(done.wait(5.seconds), listener.stop(), dialer.stop())
