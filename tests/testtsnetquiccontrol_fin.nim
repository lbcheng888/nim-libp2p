{.used.}

when defined(libp2p_msquic_experimental):
  import std/[json, options]
  import unittest2
  import chronos

  import ./msquic_test_helpers
  import ../libp2p/transports/msquicdriver as msdriver
  import ../libp2p/transports/msquicstream
  import ../libp2p/transports/quicruntime as quicrt
  import ../libp2p/transports/tsnet/quiccontrol
  import ../libp2p/stream/lpstream
  import "../libp2p/transports/nim-msquic/tls/common" as mstlstypes

  const
    TestCertificate = """-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUFlk8X0WMC1uH+ONhqdMOH/xpTwIwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI1MTEwNzEzMTUzNloXDTI2MTEw
NzEzMTUzNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAxzZ1uprFN6z0AutrOtzHXyAAVtodC708bVWSr/cj3Nbm
Bk1jAobnZVQD/tZhkqhx+LMpXaJ62PrLqfOHH6Lr1npNTgRToLEqtuXF7tnuXiWs
iBTyA3zbP5OmzPy2eZmmUojtpFvb2t7IMViVOTtsgUlM6vNkqk6cLAeIRBNy0Uti
HKn06z6Do4NNvm/bylQsHOg1xmBsrj5VCb+IBw2aBJm1sq8I5fn106ocoF+aXAnF
i/PemsNj9RyIpYAF8/1M3hcXfRjcfYU6q/QtQLlEeZBHUozKjyGTlu3snU9ssLyz
qdDft7tTJ82g+N+FNbGUqKRKykc6q5mguFR993f0PQIDAQABo1MwUTAdBgNVHQ4E
FgQUsc2QNJqJyhA9hoxW+BCalOL28jkwHwYDVR0jBBgwFoAUsc2QNJqJyhA9hoxW
+BCalOL28jkwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAtImN
3+maaA/fWAb3k0slLirqV51w7hYaza0g/8xB9gqAJQL2rLfCWXZOlOB/fLCghIPt
lBcIokfj7qJq0vRAzs7aHO77wGeWnjt2a5dD0PpMlJGqWZzt3Pp/d8CGNGtm9HGS
zJNY7BHiLQXfbThvx6k/Bdwwu5Wsiv5XbZbhGh1q5wusy09i8M+NYuvjDAPuLPOJ
tlz1zTO7a2YWG1dbMxemzbz90ZJoqrsiHw3AAtiO7k15YqmvLVpu+GLS7wIOIviP
/+R8wdJd6+/oL8XADgbPMwkytgghBDWD1zhbuUuO28ELevgV/N9Wy1WvYhhZiPZ2
figoosWp8NMAFoiMwA==
-----END CERTIFICATE-----"""

    TestPrivateKey = """-----BEGIN PRIVATE KEY-----
MIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQDHNnW6msU3rPQC
62s63MdfIABW2h0LvTxtVZKv9yPc1uYGTWMChudlVAP+1mGSqHH4syldonrY+sup
84cfouvWek1OBFOgsSq25cXu2e5eJayIFPIDfNs/k6bM/LZ5maZSiO2kW9va3sgx
WJU5O2yBSUzq82SqTpwsB4hEE3LRS2IcqfTrPoOjg02+b9vKVCwc6DXGYGyuPlUJ
v4gHDZoEmbWyrwjl+fXTqhygX5pcCcWL896aw2P1HIilgAXz/UzeFxd9GNx9hTqr
9C1AuUR5kEdSjMqPIZOW7eydT2ywvLOp0N+3u1MnzaD434U1sZSopErKRzqrmaC4
VH33d/Q9AgMBAAECggEAHzufm6EWWJNKMob8ad8hdv2KcBOESEnkBnRLKkGCIuai
a8yIQGYsM0vH0JWF+LtmGwrj6mVGA2zWr4+Z2NDvTtIf+qJdBi2gt8owjTEn2STo
9vDpvLg/m6knlq3sYgY/+GK1d3ZbcuZI1su/oZh6e46le5SrbLQkchbAO4QdFUkI
h88McLhZakWCWbCSd7xQg42JTauhuHeo+Y916ktpABO9RPOUzTSd8YvByK9HAX5t
ax174FOZVWAu57mVIZ/3pD3RKMextny+3IucGR9r4Qv2Dndo9e4FZzjl+KTtpT4t
1Stumr9V7knjFbsd2UdRe+jiGe3yAmd1QsXvCXANQQKBgQDlO6XK54uKMjWkVc5B
DpEE7n6lvef5t3BAnVEemIs3vE4KIYcXOkYgwAsXtdzZvYjtMdZL0MEGXX0rJd/D
DwC6Ig4hzmLBGN3rOt+UTvnEXncyU22ZNLur31A+5jlxb+TuXR8wGDhjwVp1Y/aw
1tyn4Aid6dT1qS3jdMTQzu/3UQKBgQDeeW5LjjsbD+nDPY4xHJyag/GZ60v9wx9E
J6fNxRSNTFaZYwrZgjV15dwtSsFYoVuTIAQ8NvmUQY3EsrbWdb1IaIiwJ7n03syy
5P9anTL0q7UrwhGg424eZjYtcKdztlh6mKC/41TGphyhtmPYOO6eOLDaRrh7KmXE
ogiRZNkLLQJ/ZEm0PxEN+2f8D+l6UvwMdhvhTKHI23dlpN8unjQetEOt4MDKWV8l
Ty61q6nk9V32ic9D8edii2ZbXIU1YCEwMD618BRbIB/A9yjKqBflLgQmId5eFKj9
cjRA50PR3c8WWTJkcqYmBX6SFMmnI7bc0pUxL+UdRly9tsVfVfszAQKBgGT+bRKB
m9VaMP1/2Sf0XCdM1IXSKiolxPDUq7meyQin6fwx2QAKuygtU/l/oSwR/BdbBnEr
Z7tk0u3DT3sl8eqIAd0t+53s8rIXgNBq4nHt7Q3TSNtnw1qrfda8+FdwJNRqqzbR
BXA0gnTq7oJ+vdw30hkU17SZ957/C7KtPFZ1AoGATM42Hj6UCOwN28kI8H97g3hB
8qL4HyummV+S0xTkmpqaaeTlig6Nn3NO/XlIeM4JYcyMjCTkwDXKgthB5rgJGAka
9MEr/XFzQFG5qMgXwDriPkD9VdyAZAatsNHKyAQo3Cmq5DXJl59OZ15W9jlqKaQZ
/z8swJdkQ7mkgfDuX14=
-----END PRIVATE KEY-----"""

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

  proc writeJsonFrame(stream: MsQuicStream; payload: JsonNode): Future[void] {.async.} =
    let encoded = if payload.isNil: "{}" else: $payload
    let body = stringToBytesLocal(encoded)
    var frame = newSeqOfCap[byte](4 + body.len)
    appendUint32BE(frame, uint32(body.len))
    frame.add(body)
    await stream.write(frame)

  proc readJsonFrameAfterEof(stream: MsQuicStream): Future[JsonNode] {.async.} =
    var payload = stream.takeCachedBytes()
    while true:
      try:
        let chunk = await stream.read()
        if chunk.len == 0:
          break
        payload.add(chunk)
      except LPStreamEOFError:
        break
    if payload.len < 4:
      raise newException(IOError, "stream closed before frame header")
    let frameLen = int(readUint32BE(payload, 0))
    if frameLen < 0 or payload.len < 4 + frameLen:
      raise newException(IOError, "stream closed before complete frame payload")
    parseJson(bytesToString(payload.toOpenArray(4, 3 + frameLen)))

  proc serverTlsConfig(): mstlstypes.TlsConfig =
    mstlstypes.TlsConfig(
      role: mstlstypes.tlsServer,
      alpns: @[NimTsnetQuicAlpn],
      transportParameters: @[],
      serverName: none(string),
      certificatePem: some(TestCertificate),
      privateKeyPem: some(TestPrivateKey),
      certificateFile: none(string),
      privateKeyFile: none(string),
      privateKeyPassword: none(string),
      pkcs12File: none(string),
      pkcs12Data: none(seq[uint8]),
      pkcs12Password: none(string),
      certificateHash: none(mstlstypes.TlsCertificateHash),
      certificateStore: none(string),
      certificateStoreFlags: 0'u32,
      certificateContext: none(pointer),
      caCertificateFile: none(string),
      resumptionTicket: none(seq[uint8]),
      enableZeroRtt: false,
      useSharedSessionCache: false,
      disableCertificateValidation: false,
      requireClientAuth: false,
      enableOcsp: false,
      indicateCertificateReceived: false,
      deferCertificateValidation: false,
      useBuiltinCertificateValidation: false,
      allowedCipherSuites: none(uint32),
      tempDirectory: none(string)
    )

  proc safeShutdownConnection(
      handle: msdriver.MsQuicTransportHandle,
      connPtr: pointer
  ) {.gcsafe, raises: [].} =
    try:
      {.cast(gcsafe).}:
        discard msdriver.shutdownConnection(handle, connPtr)
    except CatchableError:
      discard

  proc safeCloseConnection(
      handle: msdriver.MsQuicTransportHandle,
      connPtr: pointer,
      connState: msdriver.MsQuicConnectionState
  ) {.gcsafe, raises: [].} =
    try:
      {.cast(gcsafe).}:
        msdriver.closeConnection(handle, connPtr, connState)
    except CatchableError:
      discard

  proc serveSingleKeyRequestAfterEof(
      handle: msdriver.MsQuicTransportHandle,
      listenerState: msdriver.MsQuicListenerState
  ): Future[void] {.async.} =
    let event = await listenerState.nextQuicListenerEvent()
    doAssert event.kind == quicrt.qleNewConnection
    let connPtr = event.connection
    doAssert not connPtr.isNil
    let connStateOpt = msdriver.takePendingConnection(listenerState, connPtr)
    doAssert connStateOpt.isSome
    let connState = connStateOpt.get()
    defer:
      safeShutdownConnection(handle, connPtr)
      safeCloseConnection(handle, connPtr, connState)
    discard await connState.nextQuicConnectionEvent()
    let streamState = await msdriver.awaitPendingStreamState(connState)
    doAssert not streamState.isNil
    let stream = newMsQuicStream(streamState, handle, Direction.In)
    let request = await readJsonFrameAfterEof(stream)
    check request{"route"}.getStr() == "key"
    await writeJsonFrame(stream, %*{
      "ok": true,
      "payload": {
        "publicKey": "mkey:test-control",
        "legacyPublicKey": "mkey:test-legacy",
        "capabilityVersion": request{"capabilityVersion"}.getInt(130)
      }
    })
    await stream.closeWrite()
    await sleepAsync(50.milliseconds)

  suite "Tsnet QUIC control EOF semantics":
    test "client key request succeeds when server waits for EOF before decoding":
      var cfg = msdriver.MsQuicTransportConfig(
        alpns: @[NimTsnetQuicAlpn],
        appName: "nim-tsnet-control-quic"
      )
      when compiles(quicrt.useBuiltinRuntime(cfg)):
        quicrt.useBuiltinRuntime(cfg)
      let (handle, initErr) = msdriver.initMsQuicTransport(cfg)
      if handle.isNil or initErr.len > 0:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          msdriver.shutdown(handle)
        let credErr = msdriver.loadCredential(handle, serverTlsConfig())
        if credErr.len > 0:
          echo "MsQuic credential unavailable: ", credErr
          skip()
        else:
          let (listenerOpt, listenerErr) = startLoopbackListener(handle)
          if listenerErr.len > 0 or listenerOpt.isNone:
            echo "MsQuic listener unavailable: ", listenerErr
            skip()
          else:
            let listener = listenerOpt.get()
            defer:
              discard msdriver.stopListener(handle, listener.listener)
              msdriver.closeListener(handle, listener.listener, listener.state)

            let serverFut = serveSingleKeyRequestAfterEof(handle, listener.state)
            asyncSpawn serverFut

            let controlUrl = "https://localhost:" & $listener.port
            let fetched = fetchControlServerKeyQuicSafe(controlUrl)
            check fetched.isOk()
            if fetched.isOk():
              check fetched.get().publicKey == "mkey:test-control"
            check waitFor serverFut.withTimeout(3.seconds)
else:
  import unittest2

  suite "Tsnet QUIC control EOF semantics":
    test "msquic runtime unavailable":
      skip()
