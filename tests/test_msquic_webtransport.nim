when defined(libp2p_msquic_experimental):
  {.used.}

  import chronos
  import ./helpers
  import ../libp2p/[builders, multiaddress, switch]
  import ../libp2p/transports/msquicdriver as msdriver

  suite "MsQuic WebTransport handshake":
    teardown:
      checkTrackers()

    asyncTest "MsQuic switch completes WebTransport handshake":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
        return
      handle.shutdown()

      let listenAddr = MultiAddress.init("/ip4/127.0.0.1/udp/0/quic-v1/webtransport").tryGet()
      let server = newStandardSwitch(
        transport = TransportType.QUIC,
        addrs = @[listenAddr]
      )
      let client = newStandardSwitch(transport = TransportType.QUIC)

      await server.start()
      defer:
        await server.stop()

      await client.start()
      defer:
        await client.stop()

      await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

      checkUntilTimeout:
        server.webtransportSessions().len == 1
        server.webtransportSessions()[0].ready

      checkUntilTimeout:
        client.webtransportSessions().len == 1
        client.webtransportSessions()[0].ready

      let serverSession = server.webtransportSessions()[0]
      check serverSession.path == "/.well-known/libp2p-webtransport"
      check serverSession.sessionId != 0'u64
      check serverSession.draft.len > 0
      check serverSession.ready
else:
  import ./helpers
  suite "MsQuic WebTransport handshake":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
