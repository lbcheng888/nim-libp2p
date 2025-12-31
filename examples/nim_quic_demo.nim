import chronos, chronicles
import ../libp2p/builders
import ../libp2p/transports/msquictransport
import ../libp2p/protocols/protocol

const
  TestTopic = "nim-quic-demo"

proc createNode(addrs: seq[MultiAddress] = @[]): Future[Switch] {.async.} =
  let
    rng = newRng()
    seckey = PrivateKey.random(ECDSA, rng[]).get()
  
  let builder = SwitchBuilder.new()
    .withRng(rng)
    .withPrivateKey(seckey)
    .withAddresses(addrs)
    .withMsQuicTransport() # Use the experimental MsQuic transport builder
    .withNoise()           # Satisfy 'secureManagers' requirement (though QUIC has its own)
    .withMplex()           # Satisfy 'muxers' requirement (though QUIC has its own)

  let switch = builder.build()
  return switch

proc runDemo() {.async.} =
  # 1. Setup Server
  # Using IPv4 loopback, any port (0), QUIC-V1
  let ma = MultiAddress.init("/ip4/127.0.0.1/udp/0/quic-v1").get()
  let server = await createNode(addrs = @[ma])
  
  proc handleStream(stream: Connection, proto: string) {.async: (raises: [CancelledError]), closure, gcsafe.} =
    try:
      let msg = cast[string](await stream.readLp(1024))
      notice "Server received", msg
      await stream.writeLp("Hello from Server over QUIC!")
      await stream.close()
    except CatchableError as exc:
      error "Stream handling failed", msg = exc.msg
  
  let proto = LPProtocol(codecs: @[TestTopic])
  proto.handler = handleStream
  server.mount(proto)
  await server.start()

  notice "Server started", peerId = server.peerInfo.peerId, addrs = server.peerInfo.addrs

  # 2. Setup Client
  # Client needs a bind address to initialize the transport
  let clientMa = MultiAddress.init("/ip4/127.0.0.1/udp/0/quic-v1").get()
  let client = await createNode(addrs = @[clientMa])

  notice "Client created", transports = client.transports.len
  await client.start()
  notice "Client started", running = client.transports[0].running
  
  # 3. Client connects to Server
  let serverAddr = server.peerInfo.addrs[0]
  notice "Client dialing...", serverAddr = serverAddr

  try:
    let conn = await client.dial(server.peerInfo.peerId, @[serverAddr], TestTopic)
    notice "Client connected!"

    # 4. Exchange Data
    await conn.writeLp("Hello from Client over Nim QUIC!")
    let response = cast[string](await conn.readLp(1024))
    notice "Client received", response
    
    await conn.close()

  except CatchableError as exc:
    error "Client demo failed", msg = exc.msg

  # 5. Cleanup
  await client.stop()
  await server.stop()
  notice "Demo finished successfully"

when isMainModule:
  waitFor(runDemo())
