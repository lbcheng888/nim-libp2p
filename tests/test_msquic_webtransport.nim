when defined(libp2p_msquic_experimental):
  {.used.}

  import std/[options, sequtils, strutils]
  import chronos
  import ./helpers
  import ../libp2p/[builders, multiaddress, switch]
  import ../libp2p/transports/msquicdriver as msdriver
  import ../libp2p/transports/msquictransport as quictransport
  import ../libp2p/transports/qpackhuffman

  suite "MsQuic WebTransport handshake":
    teardown:
      checkTrackers()

    asyncTest "MsQuic switch completes WebTransport handshake":
      let (handle, initErr) = initMsQuicTransportForAsync()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
        return
      shutdownMsQuicTransportForAsync(handle)

      let listenAddr = MultiAddress.init("/ip4/127.0.0.1/udp/0/quic-v1/webtransport").tryGet()
      let server = newStandardSwitch(
        transport = TransportType.QUIC,
        addrs = @[listenAddr]
      )
      let client = newStandardSwitch(transport = TransportType.QUIC)

      await server.start()
      defer:
        await server.stop()

      check server.peerInfo.addrs.anyIt(($it).contains("/webtransport"))

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

      checkUntilTimeout:
        server.msquicTransportStats().len == 1
        client.msquicTransportStats().len == 1
        server.msquicTransportStats()[0].connectionCount == 1
        client.msquicTransportStats()[0].connectionCount == 1
        server.msquicTransportStats()[0].webtransportReady == 1
        client.msquicTransportStats()[0].webtransportReady == 1
        server.msquicTransportStats()[0].datagramEnabled == 1
        client.msquicTransportStats()[0].datagramEnabled == 1

      let serverStats = server.msquicTransportStats()
      let clientStats = client.msquicTransportStats()
      check serverStats.len == 1
      check clientStats.len == 1

      let serverTransport = serverStats[0]
      let clientTransport = clientStats[0]

      check serverTransport.listenerCount >= 1
      check clientTransport.listenerCount >= 1
      check serverTransport.connectionCount == 1
      check clientTransport.connectionCount == 1
      check serverTransport.webtransportReady == 1
      check clientTransport.webtransportReady == 1
      check serverTransport.webtransportPending == 0
      check clientTransport.webtransportPending == 0
      check serverTransport.datagramEnabled == 1
      check clientTransport.datagramEnabled == 1
      check serverTransport.datagramDisabled == 0
      check clientTransport.datagramDisabled == 0
      check serverTransport.webtransportActiveSlots == 1'u32
      check serverTransport.webtransportSlotLimit >= 1'u32
      check serverTransport.currentCerthash.len > 0
      check serverTransport.certhashHistory.len >= 1
      check serverTransport.readyHandshakeAverage >= 0.0
      check clientTransport.readyHandshakeAverage >= 0.0
      check serverTransport.runtime.loaded
      check clientTransport.runtime.loaded
      when defined(libp2p_msquic_builtin):
        check serverTransport.runtime.kind == quictransport.qrkMsQuicBuiltin
        check clientTransport.runtime.kind == quictransport.qrkMsQuicBuiltin
      else:
        check serverTransport.runtime.kind in {
          quictransport.qrkMsQuicBuiltin,
          quictransport.qrkMsQuicNative
        }
        check clientTransport.runtime.kind in {
          quictransport.qrkMsQuicBuiltin,
          quictransport.qrkMsQuicNative
        }

      check serverTransport.connections.len == 1
      check clientTransport.connections.len == 1

      let serverConn = serverTransport.connections[0]
      let clientConn = clientTransport.connections[0]

      check serverConn.isWebtransport
      check clientConn.isWebtransport
      check serverConn.handshakeComplete
      check clientConn.handshakeComplete
      check serverConn.webtransportReady
      check clientConn.webtransportReady
      check serverConn.datagramSendEnabled
      check clientConn.datagramSendEnabled
      check serverConn.webtransportPath == "/.well-known/libp2p-webtransport"
      check clientConn.webtransportPath == "/.well-known/libp2p-webtransport"
      check serverConn.webtransportSessionId != 0'u64
      check clientConn.webtransportSessionId != 0'u64
      check serverConn.handshakeStartMs.isSome
      check serverConn.handshakeReadyMs.isSome
      check clientConn.handshakeStartMs.isSome
      check clientConn.handshakeReadyMs.isSome
      when defined(libp2p_msquic_builtin):
        check serverConn.activePathId.isSome
        check clientConn.activePathId.isSome
        check serverConn.knownPathCount.isSome
        check clientConn.knownPathCount.isSome
        check serverConn.pathStates.len >= 1
        check clientConn.pathStates.len >= 1
        check serverConn.pathStates.anyIt(it.active)
        check clientConn.pathStates.anyIt(it.active)
      if serverConn.handshakeStartMs.isSome and serverConn.handshakeReadyMs.isSome:
        check serverConn.handshakeReadyMs.get >= serverConn.handshakeStartMs.get
      if clientConn.handshakeStartMs.isSome and clientConn.handshakeReadyMs.isSome:
        check clientConn.handshakeReadyMs.get >= clientConn.handshakeStartMs.get

    asyncTest "MsQuic WebTransport accepts Huffman-compressed QPACK headers":
      let (handle, initErr) = initMsQuicTransportForAsync()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
        return
      shutdownMsQuicTransportForAsync(handle)

      let listenAddr = MultiAddress.init("/ip4/127.0.0.1/udp/0/quic-v1/webtransport").tryGet()
      let server = newStandardSwitch(
        transport = TransportType.QUIC,
        addrs = @[listenAddr]
      )
      let client = newStandardSwitch(transport = TransportType.QUIC)
      let huffmanPath = "/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      check qpackHuffmanEncodedLen(huffmanPath) < huffmanPath.len.uint64

      server.setWebtransportPath(huffmanPath)
      client.setWebtransportPath(huffmanPath)

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
      let clientSession = client.webtransportSessions()[0]
      check serverSession.path == huffmanPath
      check clientSession.path == huffmanPath
      check serverSession.ready
      check clientSession.ready
else:
  import ./helpers
  suite "MsQuic WebTransport handshake":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
