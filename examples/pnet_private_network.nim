{.used.}

import chronos
import stew/byteutils
import libp2p

const
  DemoSwarmKey = """
/key/swarm/psk/1.0.0/
/base16/
4F8E0A501DC7A2432F45935781D24C8B81F5FEA5F0E4A6C7B4A3A22ECBF1D1E3
"""
  DemoCodec = "/pnet/example/1.0.0"

type PrivateEchoProto = ref object of LPProtocol

proc new(T: type PrivateEchoProto): T =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(1024)
      echo "pnet listener received: ", string.fromBytes(payload)
      await conn.writeLp("ack from protected switch")
    except LPStreamError as exc:
      echo "exception in handler: ", exc.msg
    finally:
      await conn.close()

  T.new(codecs = @[DemoCodec], handler = handle)

proc buildProtectedSwitch(
    ma: MultiAddress, rng: ref HmacDrbgContext
): Switch =
  SwitchBuilder
    .new()
    .withRng(rng)
    .withAddress(ma)
    .withTcpTransport()
    .withMplex()
    .withNoise()
    .withPnetFromString(DemoSwarmKey)
    .build()

proc main() {.async.} =
  let
    rng = newRng()
    listenAddr = MultiAddress.init("/ip4/127.0.0.1/tcp/0").tryGet()
    dialAddr = MultiAddress.init("/ip4/127.0.0.1/tcp/0").tryGet()
    listener = buildProtectedSwitch(listenAddr, rng)
    dialer = buildProtectedSwitch(dialAddr, rng)
    proto = PrivateEchoProto.new()

  listener.mount(proto)

  await listener.start()
  await dialer.start()

  let conn = await dialer.dial(listener.peerInfo.peerId, listener.peerInfo.addrs, DemoCodec)
  await conn.writeLp("hello from the protected network")
  echo "dialer received: ", string.fromBytes(await conn.readLp(1024))
  await conn.close()

  await allFutures(listener.stop(), dialer.stop())

waitFor(main())
