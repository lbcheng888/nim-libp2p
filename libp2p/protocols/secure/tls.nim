# Nim-LibP2P
# Copyright (c) 2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/base64
import chronos
import chronos/streams/[asyncstream, tlsstream]
import chronicles
import stew/endians2
import bearssl/[abi/bearssl_x509, certs/cacert, ssl]
import ../../stream/[connection, chronosstream]
import ../../peerid
import ../../peerinfo
import ../../utility
import ../../errors
import ../protocol
import secure
import ../../transports/tls/certificate
import ../../crypto/crypto
import ../../utils/sequninit

when defined(libp2p_dump):
  import ../../debugutils

logScope:
  topics = "libp2p tls"

const
  TLSCodec* = "/tls/1.0.0"
  HandshakeTimeout = 1.minutes
  TlsReadChunk = 16_384
  Libp2pAlpn* = "libp2p"

{.emit: """
static void libp2p_set_alpn(br_ssl_engine_context *ctx, const char *const *names, size_t num) {
  br_ssl_engine_set_protocol_names(ctx, names, num);
}
""".}

proc libp2pSetAlpn(
    ctx: ptr SslEngineContext, names: cstringArray, num: csize_t
) {.importc: "libp2p_set_alpn", header: "bearssl_ssl.h".}

template applyAlpn(
    engine: var SslEngineContext, alpnPtr: cstringArray, count: Natural
) =
  libp2pSetAlpn(addr engine, alpnPtr, csize_t(count))

type
  TLSError* = object of LPStreamError
  TLSHandshakeError* = object of TLSError
  TLSPeerMismatchError* = object of TLSError

  TLSCredentials = object
    certificateDer: seq[byte]
    tlsCertificate: TLSCertificate
    tlsPrivateKey: TLSPrivateKey

  TLSObj = object of Secure
    rng: ref HmacDrbgContext
    localPrivateKey: PrivateKey
    creds: TLSCredentials
    alpnProtocols: seq[string]
    alpnPtr: cstringArray

  TLS* = ref TLSObj

  TLSConnection* = ref object of SecureConn
    tlsStream: TLSAsyncStream
    reader: AsyncStreamReader
    writer: AsyncStreamWriter

proc freeAlpn(p: var TLSObj) =
  if p.alpnPtr != nil:
    deallocCStringArray(p.alpnPtr)
    p.alpnPtr = nil

proc freeAlpn(p: TLS) =
  if not p.isNil:
    p[].freeAlpn()

proc refreshAlpn(p: TLS) =
  p.freeAlpn()
  if p.alpnProtocols.len == 0:
    raise newException(Defect, "TLS ALPN list cannot be empty")
  p.alpnPtr = allocCStringArray(p.alpnProtocols)

proc setAlpn*(p: TLS, protocols: openArray[string]) =
  p.alpnProtocols = @protocols
  p.refreshAlpn()

proc `=destroy`(p: var TLSObj) =
  p.freeAlpn()

template mapTlsExceptions(body: untyped): untyped =
  try:
    body
  except AsyncStreamIncompleteError:
    raise newLPStreamIncompleteError()
  except AsyncStreamLimitError:
    raise newLPStreamLimitError()
  except AsyncStreamUseClosedError:
    raise newLPStreamEOFError()
  except AsyncStreamError:
    raise newLPStreamEOFError()

func toPem(header: string, data: openArray[byte]): string =
  let encoded = encode(data)
  result = "-----BEGIN " & header & "-----\n"
  var i = 0
  while i < encoded.len:
    let j = min(i + 64, encoded.len)
    result.add(encoded[i ..< j])
    result.add('\n')
    i = j
  result.add("-----END " & header & "-----\n")

proc ensureCredentials(p: TLS) {.raises: [TLSHandshakeError].} =
  if not isNil(p.creds.tlsCertificate):
    return

  try:
    let pubkey = p.localPrivateKey.getPublicKey().expect("valid private key")
    let keyPair = KeyPair(seckey: p.localPrivateKey, pubkey: pubkey)
    let certX509 = generateX509(keyPair, encodingFormat = EncodingFormat.DER)

    let certPem = toPem("CERTIFICATE", certX509.certificate)
    let privPem = toPem("PRIVATE KEY", certX509.privateKey)

    p.creds = TLSCredentials(
      certificateDer: certX509.certificate,
      tlsCertificate: TLSCertificate.init(certPem),
      tlsPrivateKey: TLSPrivateKey.init(privPem),
    )
  except TLSCertificateError as exc:
    raise (ref TLSHandshakeError)(msg: "Failed to generate TLS certificate: " & exc.msg, parent: exc)
  except TLSStreamProtocolError as exc:
    raise (ref TLSHandshakeError)(msg: "Failed to initialise TLS material: " & exc.msg, parent: exc)

proc sendCertificate(
    writer: AsyncStreamWriter, certificate: seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  if certificate.len == 0 or certificate.len > uint16.high.int:
    raise (ref TLSHandshakeError)(msg: "Invalid certificate payload length")

  var frame = newSeqUninit[byte](2 + certificate.len)
  let sizeBytes = certificate.len.uint16.toBytesBE()
  frame[0] = sizeBytes[0]
  frame[1] = sizeBytes[1]
  if certificate.len > 0:
    copyMem(addr frame[2], unsafeAddr certificate[0], certificate.len)

  mapTlsExceptions(await writer.write(frame))

proc receiveCertificate(
    reader: AsyncStreamReader
): Future[seq[byte]] {.async: (raises: [CancelledError, LPStreamError]).} =
  var lengthBuf: array[2, byte]
  mapTlsExceptions(await reader.readExactly(addr lengthBuf[0], lengthBuf.len))
  let length = uint16.fromBytesBE(lengthBuf).int
  if length == 0:
    raise (ref TLSHandshakeError)(msg: "Received empty certificate payload")

  result = newSeqUninit[byte](length)
  var offset = 0
  while offset < length:
    let read =
      mapTlsExceptions(await reader.readOnce(addr result[offset], length - offset))
    if read <= 0:
      raise newLPStreamEOFError()
    offset += read

proc verifyPeerCertificate(
    p: TLS, certificateDer: seq[byte], expected: Opt[PeerId]
): PeerId {.raises: [TLSHandshakeError, TLSPeerMismatchError].} =
  var parsed: P2pCertificate
  try:
    parsed = parse(certificateDer)
  except CertificateParsingError as exc:
    raise (ref TLSHandshakeError)(
      msg: "Failed to parse peer certificate: " & exc.msg, parent: exc
    )

  if not parsed.verify():
    raise (ref TLSHandshakeError)(msg: "Peer certificate verification failed")

  var pid: PeerId
  try:
    pid = parsed.peerId()
  except LPError as exc:
    raise (ref TLSHandshakeError)(
      msg: "Failed to extract peer id from certificate: " & exc.msg, parent: exc
    )
  expected.withValue(exp):
    if pid != exp:
      raise (ref TLSPeerMismatchError)(
        msg: "PeerId mismatch during TLS handshake: expected " &
          $exp & ", received " & $pid
      )
  pid

method readMessage*(
    sconn: TLSConnection
): Future[seq[byte]] {.async: (raises: [CancelledError, LPStreamError]).} =
  var buffer = newSeqUninit[byte](TlsReadChunk)
  let readBytes =
    mapTlsExceptions(await sconn.reader.readOnce(addr buffer[0], buffer.len))

  if readBytes == 0:
    raise newLPStreamEOFError()

  buffer.setLen(readBytes)
  when defined(libp2p_dump):
    dumpMessage(sconn, FlowDirection.Incoming, buffer)
  sconn.activity = true
  result = buffer

method write*(
    sconn: TLSConnection, message: seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  if message.len == 0:
    return

  when defined(libp2p_dump):
    dumpMessage(sconn, FlowDirection.Outgoing, message)

  mapTlsExceptions(await sconn.writer.write(message))
  sconn.activity = true

method closeImpl*(s: TLSConnection) {.async: (raises: []).} =
  try:
    if not isNil(s.reader):
      s.reader.close()
    if not isNil(s.writer):
      s.writer.close()
  except CatchableError:
    discard

  await procCall SecureConn(s).closeImpl()

method init*(p: TLS) {.gcsafe.} =
  procCall Secure(p).init()
  p.codec = TLSCodec

proc new*(
    T: typedesc[TLS], rng: ref HmacDrbgContext, privateKey: PrivateKey
): T =
  var tls = T(rng: rng, localPrivateKey: privateKey, alpnProtocols: @[Libp2pAlpn])
  tls.refreshAlpn()
  tls.init()
  tls

proc makeClientStream(
    p: TLS, reader: AsyncStreamReader, writer: AsyncStreamWriter
): TLSAsyncStream {.raises: [TLSHandshakeError].} =
  try:
    if p.alpnPtr.isNil:
      p.refreshAlpn()
    let stream = newTLSClientAsyncStream(
      reader,
      writer,
      serverName = "",
      minVersion = TLSVersion.TLS12,
      maxVersion = TLSVersion.TLS12,
      flags = {TLSFlags.NoVerifyHost, TLSFlags.NoVerifyServerName, TLSFlags.TolerateNoClientAuth},
    )
    applyAlpn(stream.ccontext.eng, p.alpnPtr, p.alpnProtocols.len)
    stream
  except TLSStreamInitError as exc:
    raise (ref TLSHandshakeError)(
      msg: "Failed to initialize TLS client stream: " & exc.msg, parent: exc
    )

proc makeServerStream(
    p: TLS, reader: AsyncStreamReader, writer: AsyncStreamWriter
): TLSAsyncStream {.raises: [TLSHandshakeError].} =
  try:
    if p.alpnPtr.isNil:
      p.refreshAlpn()
    let stream = newTLSServerAsyncStream(
      reader,
      writer,
      privateKey = p.creds.tlsPrivateKey,
      certificate = p.creds.tlsCertificate,
      minVersion = TLSVersion.TLS12,
      maxVersion = TLSVersion.TLS12,
      flags = {TLSFlags.NoRenegotiation, TLSFlags.FailOnAlpnMismatch, TLSFlags.TolerateNoClientAuth},
    )
    applyAlpn(stream.scontext.eng, p.alpnPtr, p.alpnProtocols.len)
    stream
  except TLSStreamInitError as exc:
    raise (ref TLSHandshakeError)(
      msg: "Failed to initialize TLS server stream: " & exc.msg, parent: exc
    )
  except TLSStreamProtocolError as exc:
    raise (ref TLSHandshakeError)(
      msg: "Failed to configure TLS server stream: " & exc.msg, parent: exc
    )

method handshake*(
    p: TLS, conn: Connection, initiator: bool, peerId: Opt[PeerId]
): Future[SecureConn] {.async: (raises: [CancelledError, LPStreamError]).} =
  trace "Starting TLS handshake", conn, initiator
  ensureCredentials(p)
  conn.clearNegotiatedMuxer()

  let baseConn = cast[ChronosStream](conn)
  if baseConn.isNil:
    raise (ref TLSHandshakeError)(
      msg: "Underlying connection is not ChronosStream; TLS unsupported"
    )

  let originalTimeout = conn.timeout
  conn.timeout = HandshakeTimeout
  defer:
    conn.timeout = originalTimeout

  let transport = baseConn.transport()
  let reader = newAsyncStreamReader(transport)
  let writer = newAsyncStreamWriter(transport)
  var tlsStream: TLSAsyncStream

  try:
    tlsStream =
      if initiator:
        makeClientStream(p, reader, writer)
      else:
        makeServerStream(p, reader, writer)

    await tlsStream.handshake()

    let sendFut = sendCertificate(tlsStream.writer, p.creds.certificateDer)
    let remoteCertificate = await receiveCertificate(tlsStream.reader)
    await sendFut

    let negotiatedAlpn =
      if initiator:
        sslEngineGetSelectedProtocol(tlsStream.ccontext.eng)
      else:
        sslEngineGetSelectedProtocol(tlsStream.scontext.eng)
    var negotiated =
      if negotiatedAlpn.isNil: ""
      else: $negotiatedAlpn
    if negotiated.len == 0:
      negotiated = Libp2pAlpn
    if negotiated.len == 0 or not p.alpnProtocols.contains(negotiated):
      raise (ref TLSHandshakeError)(
        msg: "TLS ALPN negotiation failed: expected one of " & $p.alpnProtocols &
          ", negotiated '" & negotiated & "'"
      )
    if negotiated != Libp2pAlpn:
      conn.setNegotiatedMuxer(negotiated)
    else:
      conn.clearNegotiatedMuxer()

    let remotePeerId = p.verifyPeerCertificate(remoteCertificate, peerId)
    conn.peerId = remotePeerId
    conn.protocol = TLSCodec

    let tlsConn =
      TLSConnection.new(conn, conn.peerId, conn.observedAddr, conn.localAddr)
    tlsConn.tlsStream = tlsStream
    tlsConn.reader = tlsStream.reader
    tlsConn.writer = tlsStream.writer
    tlsConn.setNegotiatedMuxer(conn.negotiatedMuxer)
    if conn.dir == Direction.Out:
      tlsConn.transportDir = Direction.Out
    else:
      tlsConn.transportDir = Direction.In
    trace "TLS handshake completed", initiator, peer = shortLog(conn.peerId)
    return tlsConn
  except CancelledError as exc:
    trace "TLS handshake cancelled", conn
    raise exc
  except LPStreamError as exc:
    trace "TLS handshake failed", conn, error = exc.msg
    raise exc
  except CatchableError as exc:
    trace "TLS handshake internal error", conn, error = exc.msg
    raise (ref TLSHandshakeError)(msg: exc.msg, parent: exc)
