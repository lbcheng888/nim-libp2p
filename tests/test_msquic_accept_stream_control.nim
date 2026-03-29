import std/[json, options, strutils, syncio, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/msquicstream
import ../libp2p/transports/quicruntime as quicrt
import ../libp2p/stream/lpstream

when defined(libp2p_msquic_experimental):
  proc bytesToString(data: openArray[byte]): string =
    if data.len == 0:
      return ""
    result = newString(data.len)
    copyMem(addr result[0], unsafeAddr data[0], data.len)

  proc stringToBytesLocal(text: string): seq[byte] =
    if text.len == 0:
      return @[]
    result = newSeq[byte](text.len)
    copyMem(addr result[0], unsafeAddr text[0], text.len)

  proc appendUint32BE(buffer: var seq[byte]; value: uint32) =
    buffer.add(byte((value shr 24) and 0xff'u32))
    buffer.add(byte((value shr 16) and 0xff'u32))
    buffer.add(byte((value shr 8) and 0xff'u32))
    buffer.add(byte(value and 0xff'u32))

  proc readUint32BE(buffer: openArray[byte]; offset: int): uint32 =
    (uint32(buffer[offset]) shl 24) or
      (uint32(buffer[offset + 1]) shl 16) or
      (uint32(buffer[offset + 2]) shl 8) or
      uint32(buffer[offset + 3])

  proc jsonField(node: JsonNode; key: string): string =
    if node.isNil or node.kind != JObject or not node.hasKey(key):
      return ""
    let value = node.getOrDefault(key)
    if value.kind == JString:
      return value.getStr()
    ""

  proc wrapStream(
      handle: msdriver.MsQuicTransportHandle,
      state: msdriver.MsQuicStreamState,
      dir: Direction
  ): MsQuicStream =
    newMsQuicStream(state, handle, dir)

  proc writeJsonFrame(stream: MsQuicStream; payload: JsonNode): Future[void] {.async.} =
    let encoded = if payload.isNil: "{}" else: $payload
    let body = stringToBytesLocal(encoded)
    var frame = newSeqOfCap[byte](4 + body.len)
    appendUint32BE(frame, uint32(body.len))
    frame.add(body)
    await stream.write(frame)

  proc readJsonFrame(stream: MsQuicStream): Future[JsonNode] {.async.} =
    var payload = stream.takeCachedBytes()
    var frameLen = -1
    if payload.len >= 4:
      frameLen = int(readUint32BE(payload, 0))
    while true:
      let chunk = await stream.read()
      if chunk.len == 0:
        break
      payload.add(chunk)
      if frameLen < 0 and payload.len >= 4:
        frameLen = int(readUint32BE(payload, 0))
      if frameLen >= 0 and payload.len >= 4 + frameLen:
        let frameEnd = 4 + frameLen
        let node = parseJson(bytesToString(payload.toOpenArray(4, frameEnd - 1)))
        if payload.len > frameEnd:
          stream.restoreCachedBytes(payload[frameEnd ..< payload.len])
        else:
          stream.restoreCachedBytes(@[])
        return node
    raise newException(IOError, "stream closed before a complete JSON frame was received")

  proc waitJsonFrame(stream: MsQuicStream; label: string): JsonNode =
    checkpoint(label)
    try:
      waitFor readJsonFrame(stream)
    except CatchableError as exc:
      raise newException(IOError, label & ": " & exc.msg)

  proc readJsonFrameLabelled(
      stream: MsQuicStream;
      label: string
  ): Future[JsonNode] {.async.} =
    try:
      return await readJsonFrame(stream)
    except CatchableError as exc:
      raise newException(IOError, label & ": " & exc.msg)

  proc readJsonFrameWithin(
      stream: MsQuicStream;
      timeout: Duration;
      label: string
  ): Future[JsonNode] {.async.} =
    let readFut = readJsonFrameLabelled(stream, label)
    let timeoutFut = sleepAsync(timeout)
    let winner = await race(cast[FutureBase](readFut), cast[FutureBase](timeoutFut))
    if winner == cast[FutureBase](timeoutFut):
      readFut.cancelSoon()
      raise newException(IOError, label & ": timeout")
    timeoutFut.cancelSoon()
    await readFut

  suite "MsQuic accept-style control stream":
    test "raw driver same bidi stream supports delayed incoming then ready":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            msdriver.shutdown(handle)

        let (listenerOpt, listenerErr) = startLoopbackListener(handle)
        if listenerErr.len > 0 or listenerOpt.isNone:
          echo "MsQuic listener unavailable: ", listenerErr
          skip()
        else:
          let listener = listenerOpt.get()
          defer:
            discard msdriver.stopListener(handle, listener.listener)
            msdriver.closeListener(handle, listener.listener, listener.state)

          let (clientConnPtr, clientConnStateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or clientConnStateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let clientConnState = clientConnStateOpt.get()
            let (serverConnPtr, serverConnStateOpt, acceptErr) =
              acceptPendingConnection(listener.state)
            if acceptErr.len > 0 or serverConnStateOpt.isNone:
              echo "MsQuic accept unavailable: ", acceptErr
              skip()
            else:
              let serverConnState = serverConnStateOpt.get()
              defer:
                discard msdriver.shutdownConnection(handle, serverConnPtr)
                msdriver.closeConnection(handle, serverConnPtr, serverConnState)
                discard msdriver.shutdownConnection(handle, clientConnPtr)
                msdriver.closeConnection(handle, clientConnPtr, clientConnState)

              discard nextConnectionEventOfKind(clientConnState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverConnState, quicrt.qceConnected)

              let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
                handle,
                clientConnPtr,
                connectionState = clientConnState
              )
              check streamErr.len == 0
              check streamStateOpt.isSome
              if streamErr.len > 0 or streamStateOpt.isNone:
                skip()
              else:
                let clientStreamState = streamStateOpt.get()
                defer:
                  msdriver.closeStream(handle, streamPtr, clientStreamState)
                check msdriver.startStream(handle, streamPtr).len == 0
                discard nextStreamEventOfKind(clientStreamState, quicrt.qseStartComplete)

                let serverInboundFut = msdriver.awaitPendingStreamState(serverConnState)
                check waitFor msdriver.writeStreamAndWait(
                  clientStreamState,
                  stringToBytesLocal("""{"mode":"accept"}""")
                ).withTimeout(3.seconds)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverStreamState = serverInboundFut.read()
                check serverStreamState != nil
                if serverStreamState.isNil:
                  skip()
                else:
                  let serverAccept = msdriver.readStream(serverStreamState)
                  check waitFor serverAccept.withTimeout(3.seconds)
                  check bytesToString(serverAccept.read()) == """{"mode":"accept"}"""

                  let clientAck = msdriver.readStream(clientStreamState)
                  check waitFor msdriver.writeStreamAndWait(
                    serverStreamState,
                    stringToBytesLocal("""{"ok":true,"mode":"accept"}""")
                  ).withTimeout(3.seconds)
                  check waitFor clientAck.withTimeout(3.seconds)
                  check bytesToString(clientAck.read()) == """{"ok":true,"mode":"accept"}"""

                  let serverAwait = msdriver.readStream(serverStreamState)
                  check waitFor msdriver.writeStreamAndWait(
                    clientStreamState,
                    stringToBytesLocal("""{"op":"await"}""")
                  ).withTimeout(3.seconds)
                  check waitFor serverAwait.withTimeout(3.seconds)
                  check bytesToString(serverAwait.read()) == """{"op":"await"}"""

                  let serverReady = msdriver.readStream(serverStreamState)
                  let clientIncoming = msdriver.readStream(clientStreamState)
                  waitFor sleepAsync(250.milliseconds)
                  check waitFor msdriver.writeStreamAndWait(
                    serverStreamState,
                    stringToBytesLocal("""{"op":"incoming","sessionId":"session-1"}""")
                  ).withTimeout(3.seconds)
                  check waitFor clientIncoming.withTimeout(3.seconds)
                  check bytesToString(clientIncoming.read()) ==
                    """{"op":"incoming","sessionId":"session-1"}"""

                  check waitFor msdriver.writeStreamAndWait(
                    clientStreamState,
                    stringToBytesLocal("""{"op":"ready","sessionId":"session-1"}""")
                  ).withTimeout(3.seconds)
                  check waitFor serverReady.withTimeout(3.seconds)
                  check bytesToString(serverReady.read()) ==
                    """{"op":"ready","sessionId":"session-1"}"""

    test "wrapped stream same bidi stream supports delayed incoming then ready":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            msdriver.shutdown(handle)

        let (listenerOpt, listenerErr) = startLoopbackListener(handle)
        if listenerErr.len > 0 or listenerOpt.isNone:
          echo "MsQuic listener unavailable: ", listenerErr
          skip()
        else:
          let listener = listenerOpt.get()
          defer:
            discard msdriver.stopListener(handle, listener.listener)
            msdriver.closeListener(handle, listener.listener, listener.state)

          let (clientConnPtr, clientConnStateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or clientConnStateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let clientConnState = clientConnStateOpt.get()
            let (serverConnPtr, serverConnStateOpt, acceptErr) =
              acceptPendingConnection(listener.state)
            if acceptErr.len > 0 or serverConnStateOpt.isNone:
              echo "MsQuic accept unavailable: ", acceptErr
              skip()
            else:
              let serverConnState = serverConnStateOpt.get()
              defer:
                discard msdriver.shutdownConnection(handle, serverConnPtr)
                msdriver.closeConnection(handle, serverConnPtr, serverConnState)
                discard msdriver.shutdownConnection(handle, clientConnPtr)
                msdriver.closeConnection(handle, clientConnPtr, clientConnState)

              discard nextConnectionEventOfKind(clientConnState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverConnState, quicrt.qceConnected)

              let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
                handle,
                clientConnPtr,
                connectionState = clientConnState
              )
              check streamErr.len == 0
              check streamStateOpt.isSome
              if streamErr.len > 0 or streamStateOpt.isNone:
                skip()
              else:
                let clientStreamState = streamStateOpt.get()
                defer:
                  msdriver.closeStream(handle, streamPtr, clientStreamState)
                check msdriver.startStream(handle, streamPtr).len == 0
                discard nextStreamEventOfKind(clientStreamState, quicrt.qseStartComplete)

                let clientStream = wrapStream(handle, clientStreamState, Direction.Out)
                let serverInboundFut = msdriver.awaitPendingStreamState(serverConnState)
                check waitFor writeJsonFrame(clientStream, %*{"mode":"accept"}).withTimeout(3.seconds)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverStreamState = serverInboundFut.read()
                check serverStreamState != nil
                if serverStreamState.isNil:
                  skip()
                else:
                  let serverStream = wrapStream(handle, serverStreamState, Direction.In)

                  let acceptMsg = waitJsonFrame(serverStream, "server read accept")
                  check jsonField(acceptMsg, "mode") == "accept"

                  check waitFor writeJsonFrame(
                    serverStream,
                    %*{"ok": true, "payload": {"mode":"accept"}}
                  ).withTimeout(3.seconds)
                  let acceptAck = waitJsonFrame(clientStream, "client read accept ack")
                  check acceptAck{"ok"}.getBool(false)

                  check waitFor writeJsonFrame(clientStream, %*{"op":"await"}).withTimeout(3.seconds)
                  let awaitMsg = waitJsonFrame(serverStream, "server read await")
                  check jsonField(awaitMsg, "op") == "await"

                  check waitFor writeJsonFrame(
                    serverStream,
                    %*{
                      "op":"incoming",
                      "route":"test-route",
                      "source":"/ip4/100.64.1.2/udp/9999/quic-v1/tsnet",
                      "sessionId":"session-1"
                    }
                  ).withTimeout(3.seconds)
                  let incoming = waitJsonFrame(clientStream, "client read incoming")
                  check jsonField(incoming, "op") == "incoming"
                  check jsonField(incoming, "sessionId") == "session-1"

                  check waitFor writeJsonFrame(
                    clientStream,
                    %*{"op":"ready","sessionId":"session-1"}
                  ).withTimeout(3.seconds)
                  let ready = waitJsonFrame(serverStream, "server read ready")
                  check jsonField(ready, "op") == "ready"
                  check jsonField(ready, "sessionId") == "session-1"

    test "wrapped stream timeout on incoming wait does not consume later payload":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            msdriver.shutdown(handle)

        let (listenerOpt, listenerErr) = startLoopbackListener(handle)
        if listenerErr.len > 0 or listenerOpt.isNone:
          echo "MsQuic listener unavailable: ", listenerErr
          skip()
        else:
          let listener = listenerOpt.get()
          defer:
            discard msdriver.stopListener(handle, listener.listener)
            msdriver.closeListener(handle, listener.listener, listener.state)

          let (clientConnPtr, clientConnStateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or clientConnStateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let clientConnState = clientConnStateOpt.get()
            let (serverConnPtr, serverConnStateOpt, acceptErr) =
              acceptPendingConnection(listener.state)
            if acceptErr.len > 0 or serverConnStateOpt.isNone:
              echo "MsQuic accept unavailable: ", acceptErr
              skip()
            else:
              let serverConnState = serverConnStateOpt.get()
              defer:
                discard msdriver.shutdownConnection(handle, serverConnPtr)
                msdriver.closeConnection(handle, serverConnPtr, serverConnState)
                discard msdriver.shutdownConnection(handle, clientConnPtr)
                msdriver.closeConnection(handle, clientConnPtr, clientConnState)

              discard nextConnectionEventOfKind(clientConnState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverConnState, quicrt.qceConnected)

              let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
                handle,
                clientConnPtr,
                connectionState = clientConnState
              )
              check streamErr.len == 0
              check streamStateOpt.isSome
              if streamErr.len > 0 or streamStateOpt.isNone:
                skip()
              else:
                let clientStreamState = streamStateOpt.get()
                defer:
                  msdriver.closeStream(handle, streamPtr, clientStreamState)
                check msdriver.startStream(handle, streamPtr).len == 0
                discard nextStreamEventOfKind(clientStreamState, quicrt.qseStartComplete)

                let clientStream = wrapStream(handle, clientStreamState, Direction.Out)
                let serverInboundFut = msdriver.awaitPendingStreamState(serverConnState)
                check waitFor writeJsonFrame(clientStream, %*{"mode":"accept"}).withTimeout(3.seconds)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverStreamState = serverInboundFut.read()
                check serverStreamState != nil
                if serverStreamState.isNil:
                  skip()
                else:
                  let serverStream = wrapStream(handle, serverStreamState, Direction.In)
                  discard waitJsonFrame(serverStream, "server read accept")
                  check waitFor writeJsonFrame(
                    serverStream,
                    %*{"ok": true, "payload": {"mode":"accept"}}
                  ).withTimeout(3.seconds)
                  discard waitJsonFrame(clientStream, "client read accept ack")

                  check waitFor writeJsonFrame(clientStream, %*{"op":"await"}).withTimeout(3.seconds)
                  discard waitJsonFrame(serverStream, "server read await")

                  var timedOut = false
                  try:
                    discard waitFor readJsonFrameWithin(
                      clientStream,
                      150.milliseconds,
                      "client wait incoming timeout"
                    )
                  except CatchableError as exc:
                    timedOut = exc.msg.contains("timeout")
                  check timedOut

                  check waitFor writeJsonFrame(
                    serverStream,
                    %*{
                      "op":"incoming",
                      "route":"test-route",
                      "source":"/ip4/100.64.1.2/udp/9999/quic-v1/tsnet",
                      "sessionId":"session-2"
                    }
                  ).withTimeout(3.seconds)
                  let incoming = waitJsonFrame(clientStream, "client read incoming after timeout")
                  check jsonField(incoming, "op") == "incoming"
                  check jsonField(incoming, "sessionId") == "session-2"

                  check waitFor writeJsonFrame(
                    clientStream,
                    %*{"op":"ready","sessionId":"session-2"}
                  ).withTimeout(3.seconds)
                  let ready = waitJsonFrame(serverStream, "server read ready after timeout")
                  check jsonField(ready, "op") == "ready"
                  check jsonField(ready, "sessionId") == "session-2"

else:
  suite "MsQuic accept-style control stream":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
