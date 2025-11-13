# Nim-LibP2P
# Copyright (c) 2025
#
# 基于 OpenSSL QUIC API 的传输适配层。当前实现覆盖上下文配置、证书载入、
# 客户端/服务端握手、基础流创建与读写。数据报面向未来扩展暂以空实现返回。

import std/[options, sequtils, strutils, os, posix, times]
import chronos
import chronicles
import results
import nativesockets
import chronos/transports/common
import chronos/internal/asyncengine as asyncengine
import chronos/oserrno
import chronos/timer as ctimer
import chronos/osdefs as osdefs
import ../crypto/crypto

const
  DefaultReadBuffer = 64 * 1024
  OpenSSLHeader = (parentDir(currentSourcePath()) / "openssl_compat.h").replace('\\', '/')

# --------------------------
# OpenSSL FFI 定义
# --------------------------

type
  SSL_CTX* = pointer
  SSL* = pointer
  SSL_METHOD* = pointer
  BIO_ADDR* = pointer
  X509* = pointer
  EVP_PKEY* = pointer

proc OSSL_QUIC_client_method*(): SSL_METHOD {.importc: "OSSL_QUIC_client_method", header: OpenSSLHeader.}
proc OSSL_QUIC_server_method*(): SSL_METHOD {.importc: "OSSL_QUIC_server_method", header: OpenSSLHeader.}
proc SSL_CTX_new*(sslMethod: SSL_METHOD): SSL_CTX {.importc: "SSL_CTX_new", header: OpenSSLHeader.}
proc SSL_CTX_free*(ctx: SSL_CTX) {.importc: "SSL_CTX_free", header: OpenSSLHeader.}
proc SSL_new*(ctx: SSL_CTX): SSL {.importc: "SSL_new", header: OpenSSLHeader.}
proc SSL_free*(ssl: SSL) {.importc: "SSL_free", header: OpenSSLHeader.}
proc SSL_new_listener*(ctx: SSL_CTX, flags: uint64): SSL {.importc: "SSL_new_listener", header: OpenSSLHeader.}
proc SSL_accept_connection*(listener: SSL, flags: uint64): SSL {.importc: "SSL_accept_connection", header: OpenSSLHeader.}
proc SSL_listen*(listener: SSL): cint {.importc: "SSL_listen", header: OpenSSLHeader.}
proc SSL_connect*(ssl: SSL): cint {.importc: "SSL_connect", header: OpenSSLHeader.}
proc SSL_handle_events*(ssl: SSL): cint {.importc: "SSL_handle_events", header: OpenSSLHeader.}
proc SSL_get_error*(ssl: SSL, retCode: cint): cint {.importc: "SSL_get_error", header: OpenSSLHeader.}
proc SSL_set_fd*(ssl: SSL, fd: cint): cint {.importc: "SSL_set_fd", header: OpenSSLHeader.}
proc SSL_set_blocking_mode*(ssl: SSL, blocking: cint): cint {.importc: "SSL_set_blocking_mode", header: OpenSSLHeader.}
proc SSL_CTX_set_alpn_protos*(ctx: SSL_CTX, protos: ptr cuchar, len: cuint): cint {.importc: "SSL_CTX_set_alpn_protos", header: OpenSSLHeader.}
proc SSL_set_alpn_protos*(ssl: SSL, protos: ptr cuchar, len: cuint): cint {.importc: "SSL_set_alpn_protos", header: OpenSSLHeader.}
proc SSL_CTX_use_certificate*(ctx: SSL_CTX, cert: X509): cint {.importc: "SSL_CTX_use_certificate", header: OpenSSLHeader.}
proc SSL_CTX_use_PrivateKey*(ctx: SSL_CTX, key: EVP_PKEY): cint {.importc: "SSL_CTX_use_PrivateKey", header: OpenSSLHeader.}
proc SSL_set_tlsext_host_name*(ssl: SSL, name: cstring): cint {.importc: "SSL_set_tlsext_host_name", header: OpenSSLHeader.}
proc SSL_set1_host*(ssl: SSL, name: cstring): cint {.importc: "SSL_set1_host", header: OpenSSLHeader.}
proc SSL_set1_initial_peer_addr*(ssl: SSL, peer: BIO_ADDR): cint {.importc: "SSL_set1_initial_peer_addr", header: OpenSSLHeader.}
proc SSL_get_event_timeout*(ssl: SSL, tv: ptr Timeval, isInfinite: ptr cint): cint {.importc: "SSL_get_event_timeout", header: OpenSSLHeader.}
proc SSL_get_peer_addr*(ssl: SSL, peer: BIO_ADDR): cint {.importc: "SSL_get_peer_addr", header: OpenSSLHeader.}
proc SSL_do_handshake*(ssl: SSL): cint {.importc: "SSL_do_handshake", header: OpenSSLHeader.}
proc SSL_is_init_finished*(ssl: SSL): cint {.importc: "SSL_is_init_finished", header: OpenSSLHeader.}
proc SSL_get1_peer_certificate*(ssl: SSL): X509 {.importc: "SSL_get1_peer_certificate", header: OpenSSLHeader.}
proc SSL_new_stream*(ssl: SSL, flags: uint64): SSL {.importc: "SSL_new_stream", header: OpenSSLHeader.}
proc SSL_accept_stream*(ssl: SSL, flags: uint64): SSL {.importc: "SSL_accept_stream", header: OpenSSLHeader.}
proc SSL_get_stream_id*(ssl: SSL): uint64 {.importc: "SSL_get_stream_id", header: OpenSSLHeader.}
proc SSL_get_stream_type*(ssl: SSL): cint {.importc: "SSL_get_stream_type", header: OpenSSLHeader.}
proc SSL_stream_conclude*(ssl: SSL, flags: uint64): cint {.importc: "SSL_stream_conclude", header: OpenSSLHeader.}
proc SSL_get_stream_read_state*(ssl: SSL): cint {.
    importc: "SSL_get_stream_read_state", header: OpenSSLHeader.}
proc SSL_get_stream_write_state*(ssl: SSL): cint {.
    importc: "SSL_get_stream_write_state", header: OpenSSLHeader.}
proc SSL_get_stream_read_error_code*(ssl: SSL, appError: ptr uint64): cint {.
    importc: "SSL_get_stream_read_error_code", header: OpenSSLHeader.}
proc SSL_get_stream_write_error_code*(ssl: SSL, appError: ptr uint64): cint {.
    importc: "SSL_get_stream_write_error_code", header: OpenSSLHeader.}
proc SSL_shutdown*(ssl: SSL): cint {.importc: "SSL_shutdown", header: OpenSSLHeader.}
proc SSL_read_ex*(ssl: SSL, buf: pointer, num: csize_t, readBytes: ptr csize_t): cint {.importc: "SSL_read_ex", header: OpenSSLHeader.}
proc SSL_write_ex*(ssl: SSL, buf: pointer, num: csize_t, written: ptr csize_t): cint {.importc: "SSL_write_ex", header: OpenSSLHeader.}

proc BIO_ADDR_new*(): BIO_ADDR {.importc: "BIO_ADDR_new", header: OpenSSLHeader.}
proc BIO_ADDR_free*(bio: BIO_ADDR) {.importc: "BIO_ADDR_free", header: OpenSSLHeader.}
proc BIO_ADDR_rawmake*(bio: BIO_ADDR, family: cint, addrPtr: pointer, addrLen: csize_t, port: cushort): cint {.importc: "BIO_ADDR_rawmake", header: OpenSSLHeader.}
proc BIO_ADDR_family*(bio: BIO_ADDR): cint {.importc: "BIO_ADDR_family", header: OpenSSLHeader.}
proc BIO_ADDR_rawaddress*(bio: BIO_ADDR, p: pointer, l: ptr csize_t): cint {.importc: "BIO_ADDR_rawaddress", header: OpenSSLHeader.}
proc BIO_ADDR_rawport*(bio: BIO_ADDR): cushort {.importc: "BIO_ADDR_rawport", header: OpenSSLHeader.}

proc BIO_new_mem_buf*(data: pointer, len: cint): pointer {.importc: "BIO_new_mem_buf", header: OpenSSLHeader.}
proc BIO_free*(bio: pointer): cint {.importc: "BIO_free", header: OpenSSLHeader.}

proc PEM_read_bio_X509*(bio: pointer, certOut: ptr X509, cb: pointer, u: pointer): X509 {.importc: "PEM_read_bio_X509", header: OpenSSLHeader.}
proc PEM_read_bio_PrivateKey*(bio: pointer, keyOut: ptr EVP_PKEY, cb: pointer, u: pointer): EVP_PKEY {.importc: "PEM_read_bio_PrivateKey", header: OpenSSLHeader.}
proc d2i_X509*(certOut: ptr X509, data: ptr ptr cuchar, len: clong): X509 {.importc: "d2i_X509", header: OpenSSLHeader.}
proc d2i_AutoPrivateKey*(keyOut: ptr EVP_PKEY, data: ptr ptr cuchar, len: clong): EVP_PKEY {.importc: "d2i_AutoPrivateKey", header: OpenSSLHeader.}
proc i2d_X509*(cert: X509, bufOut: ptr ptr cuchar): cint {.importc: "i2d_X509", header: OpenSSLHeader.}
proc X509_free*(cert: X509) {.importc: "X509_free", header: OpenSSLHeader.}
proc EVP_PKEY_free*(key: EVP_PKEY) {.importc: "EVP_PKEY_free", header: OpenSSLHeader.}

proc ERR_get_error*(): culong {.importc: "ERR_get_error", header: OpenSSLHeader.}
proc ERR_error_string_n*(e: culong, buf: cstring, len: csize_t) {.importc: "ERR_error_string_n", header: OpenSSLHeader.}
proc ERR_peek_error*(): culong {.importc: "ERR_peek_error", header: OpenSSLHeader.}
proc ERR_get_error_all*(
    file: ptr cstring, line: ptr cint, funcName: ptr cstring, data: ptr cstring, flags: ptr cint
): culong {.importc: "ERR_get_error_all", header: OpenSSLHeader.}
proc ERR_print_errors_fp*(fp: pointer) {.importc: "ERR_print_errors_fp", header: OpenSSLHeader.}
type AlpnSelectCb* = proc(
    ssl: SSL,
    outProto: ptr ptr cuchar,
    outLen: ptr cuchar,
    inProto: ptr cuchar,
    inLen: cuint,
    userData: pointer
): cint {.cdecl.}

proc nim_SSL_CTX_set_alpn_select_cb*(
    ctx: SSL_CTX, cb: AlpnSelectCb, arg: pointer
) {.importc: "nim_SSL_CTX_set_alpn_select_cb", header: OpenSSLHeader.}
proc SSL_select_next_proto*(
    outProto: ptr ptr cuchar,
    outLen: ptr cuchar,
    server: ptr cuchar,
    serverLen: cuint,
    client: ptr cuchar,
    clientLen: cuint
): cint {.importc: "SSL_select_next_proto", header: OpenSSLHeader.}
proc nim_SSL_poll_stream*(
    ssl: SSL, events: uint64, revents: ptr uint64
): cint {.importc: "nim_SSL_poll_stream", header: OpenSSLHeader.}
proc SSL_set_default_stream_mode*(ssl: SSL, mode: uint32): cint {.
    importc: "SSL_set_default_stream_mode", header: OpenSSLHeader.}

type
  SslPollItem* {.importc: "nim_ssl_poll_item", header: OpenSSLHeader.} = object

proc nim_ssl_poll_item_setup*(holder: ptr SslPollItem, ssl: SSL, events: uint64) {.
    importc: "nim_ssl_poll_item_setup", header: OpenSSLHeader.}
proc nim_ssl_poll_item_events*(holder: ptr SslPollItem): uint64 {.
    importc: "nim_ssl_poll_item_events", header: OpenSSLHeader.}
proc nim_ssl_poll_item_set_events*(holder: ptr SslPollItem, events: uint64) {.
    importc: "nim_ssl_poll_item_set_events", header: OpenSSLHeader.}
proc nim_ssl_poll_item_revents*(holder: ptr SslPollItem): uint64 {.
    importc: "nim_ssl_poll_item_revents", header: OpenSSLHeader.}
proc nim_ssl_poll_item_reset*(holder: ptr SslPollItem) {.
    importc: "nim_ssl_poll_item_reset", header: OpenSSLHeader.}
proc nim_SSL_poll*(
    items: ptr SslPollItem,
    numItems: csize_t,
    timeout: ptr Timeval,
    flags: uint64,
    result: ptr csize_t
): cint {.importc: "nim_SSL_poll", header: OpenSSLHeader.}

const
  ERR_TXT_STRING* = 1
  SSL_ERROR_NONE* = 0
  SSL_ERROR_WANT_READ* = 2
  SSL_ERROR_WANT_WRITE* = 3
  SSL_ERROR_WANT_ACCEPT* = 8
  SSL_ERROR_SSL* = 1
  SSL_ERROR_SYSCALL* = 5
  SSL_ERROR_ZERO_RETURN* = 6
  SSL_ACCEPT_CONNECTION_NO_BLOCK* = (1'u64 shl 0)
  OPENSSL_NPN_NEGOTIATED* = 1
  SSL_TLSEXT_ERR_OK* = 0
  SSL_TLSEXT_ERR_NOACK* = 3
  SSL_ACCEPT_STREAM_NO_BLOCK* = (1'u64 shl 0)
  SSL_DEFAULT_STREAM_MODE_AUTO_BIDI* = 1'u32
  SSL_POLL_EVENT_EL* = (1'u64 shl 1)
  SSL_POLL_EVENT_EC* = (1'u64 shl 2)
  SSL_POLL_EVENT_ER* = (1'u64 shl 4)
  SSL_POLL_EVENT_EW* = (1'u64 shl 5)
  SSL_POLL_EVENT_IC* = (1'u64 shl 8)
  SSL_POLL_EVENT_ISB* = (1'u64 shl 9)
  SSL_POLL_EVENT_ISU* = (1'u64 shl 10)
  SSL_POLL_EVENT_IS* = SSL_POLL_EVENT_ISB or SSL_POLL_EVENT_ISU
  SSL_POLL_EVENT_E* = SSL_POLL_EVENT_EL or SSL_POLL_EVENT_EC or SSL_POLL_EVENT_ER or SSL_POLL_EVENT_EW
  SSL_POLL_EVENT_OSB* = (1'u64 shl 11)
  SSL_POLL_EVENT_OSU* = (1'u64 shl 12)
  SSL_POLL_EVENT_OS* = SSL_POLL_EVENT_OSB or SSL_POLL_EVENT_OSU
  SSL_STREAM_STATE_NONE* = 0
  SSL_STREAM_STATE_OK* = 1
  SSL_STREAM_STATE_WRONG_DIR* = 2
  SSL_STREAM_STATE_FINISHED* = 3
  SSL_STREAM_STATE_RESET_LOCAL* = 4
  SSL_STREAM_STATE_RESET_REMOTE* = 5
  SSL_STREAM_STATE_CONN_CLOSED* = 6

# --------------------------
# 自定义类型与错误
# --------------------------

type
  AlpnSelectData = ref object
    encoded: seq[byte]

  QuicError* = object of CatchableError
    streamState*: cint
    hasStreamState*: bool
    streamAppError*: uint64
    hasStreamAppError*: bool
    streamId*: uint64
  QuicConfigError* = object of QuicError
  TimeOutError* = object of QuicError

  certificateVerifierCB* =
    proc(serverName: string, derCertificates: seq[seq[byte]]): bool {.gcsafe.}

  CertificateVerifier* = ref object of RootObj
  CustomCertificateVerifier* = ref object of CertificateVerifier
    cb: certificateVerifierCB

  TLSConfig* = object
    certificate*: seq[byte]
    privateKey*: seq[byte]
    alpn*: seq[string]
    verifier*: Opt[CertificateVerifier]
    alpnData*: AlpnSelectData

  Listener* = ref object of RootObj
    ctx*: SSL_CTX
    ssl*: SSL
    socketFd*: AsyncFD
    local*: TransportAddress
    running*: bool
    tls*: TLSConfig
    eventLock*: AsyncLock

  Connection* = ref object of RootObj
    ssl*: SSL
    socketFd*: AsyncFD
    remote*: TransportAddress
    local*: TransportAddress
    tls*: TLSConfig
    ownsSocket*: bool
    closed*: bool
    eventLock*: AsyncLock
    certificatesCache*: seq[seq[byte]]
    certificatesLoaded*: bool
    msquicState*: pointer

  Stream* = ref object of RootObj
    ssl*: SSL
    connection*: Connection
    eventLock*: AsyncLock
    unidirectional*: bool
    closed*: bool
  QuicClient* = object
    ctx*: SSL_CTX
    tls*: TLSConfig

  QuicServer* = object
    ctx*: SSL_CTX
    tls*: TLSConfig

proc streamIdentifier(stream: Stream): uint64 =
  if stream.isNil or stream.ssl.isNil:
    0
  else:
    SSL_get_stream_id(stream.ssl)

proc streamStateToString*(state: cint): string =
  case state
  of SSL_STREAM_STATE_NONE:
    "none"
  of SSL_STREAM_STATE_OK:
    "ok"
  of SSL_STREAM_STATE_WRONG_DIR:
    "wrong_direction"
  of SSL_STREAM_STATE_FINISHED:
    "finished"
  of SSL_STREAM_STATE_RESET_LOCAL:
    "reset_local"
  of SSL_STREAM_STATE_RESET_REMOTE:
    "reset_remote"
  of SSL_STREAM_STATE_CONN_CLOSED:
    "connection_closed"
  else:
    "unknown(" & $state & ")"

proc newStreamQuicError(
    msg: string, stream: Stream, state: cint, hasAppError: bool, appError: uint64
): ref QuicError =
  let err = newException(QuicError, msg)
  err.streamState = state
  err.hasStreamState = true
  err.hasStreamAppError = hasAppError
  err.streamAppError = appError
  err.streamId = streamIdentifier(stream)
  err

const
  StreamReadPollMask = SSL_POLL_EVENT_ER or SSL_POLL_EVENT_E
  StreamWritePollMask = SSL_POLL_EVENT_EW or SSL_POLL_EVENT_E

# --------------------------
# 工具函数
# --------------------------

proc newQuicError(msg: string): ref QuicError =
  (ref QuicError)(msg: msg)

proc opensslErrorMessage(): string =
  var file, funcName, data: cstring = nil
  var line, flags: cint = 0
  var buf = newString(256)
  let code = ERR_get_error_all(addr file, addr line, addr funcName, addr data, addr flags)
  if code == 0:
    return "unknown OpenSSL error"
  ERR_error_string_n(code, buf.cstring, buf.len.csize_t)
  let zero = buf.find(char(0))
  if zero >= 0:
    buf.setLen(zero)
  if not data.isNil and (flags and ERR_TXT_STRING) != 0:
    buf.add(" - ")
    buf.add($data)
  if not file.isNil:
    buf.add(" @")
    buf.add($file)
    if line != 0:
      buf.add(":")
      buf.add($line)
  if not funcName.isNil:
    buf.add(" in ")
    buf.add($funcName)
  buf

proc encodeAlpn(protos: seq[string]): seq[byte] {.raises: [ref QuicError].} =
  for item in protos:
    let payloadLen = item.len
    if payloadLen == 0 or payloadLen > 255:
      raise newQuicError("invalid ALPN length: " & item)
    result.add(byte(payloadLen))
    for ch in item:
      result.add(byte(ch))

proc loadCertificateIntoCtx(ctx: SSL_CTX, tls: TLSConfig) {.raises: [QuicConfigError].} =
  if tls.certificate.len == 0 or tls.privateKey.len == 0:
    return

  var cert: X509 = nil
  var key: EVP_PKEY = nil
  var certBio: pointer = nil
  var keyBio: pointer = nil

  defer:
    if not certBio.isNil:
      discard BIO_free(certBio)
    if not keyBio.isNil:
      discard BIO_free(keyBio)
    if not cert.isNil:
      X509_free(cert)
    if not key.isNil:
      EVP_PKEY_free(key)

  certBio = BIO_new_mem_buf(unsafeAddr tls.certificate[0], tls.certificate.len.cint)
  if certBio.isNil:
    raise newException(QuicConfigError, "failed to create certificate BIO")
  let isCertPem = tls.certificate.len >= 27 and tls.certificate[0] == byte('-')
  cert =
    if isCertPem:
      PEM_read_bio_X509(certBio, nil, nil, nil)
    else:
      block:
        var certPtr = cast[ptr cuchar](unsafeAddr tls.certificate[0])
        d2i_X509(nil, addr certPtr, tls.certificate.len.clong)
  if cert.isNil:
    raise newException(QuicConfigError, "failed to parse certificate: " & opensslErrorMessage())

  keyBio = BIO_new_mem_buf(unsafeAddr tls.privateKey[0], tls.privateKey.len.cint)
  if keyBio.isNil:
    raise newException(QuicConfigError, "failed to create private key BIO")
  let isKeyPem = tls.privateKey.len >= 27 and tls.privateKey[0] == byte('-')
  key =
    if isKeyPem:
      PEM_read_bio_PrivateKey(keyBio, nil, nil, nil)
    else:
      block:
        var keyPtr = cast[ptr cuchar](unsafeAddr tls.privateKey[0])
        d2i_AutoPrivateKey(nil, addr keyPtr, tls.privateKey.len.clong)
  if key.isNil:
    raise newException(QuicConfigError, "failed to parse private key: " & opensslErrorMessage())

  if SSL_CTX_use_certificate(ctx, cert) != 1:
    raise newException(QuicConfigError, "failed to load certificate: " & opensslErrorMessage())
  if SSL_CTX_use_PrivateKey(ctx, key) != 1:
    raise newException(QuicConfigError, "failed to load private key: " & opensslErrorMessage())

proc applyAlpn(ctx: SSL_CTX, tls: TLSConfig) {.raises: [QuicConfigError].} =
  if tls.alpn.len == 0:
    return
  if tls.alpnData.isNil or tls.alpnData.encoded.len == 0:
    raise newException(QuicConfigError, "ALPN payload is empty")
  if SSL_CTX_set_alpn_protos(
      ctx,
      cast[ptr cuchar](unsafeAddr tls.alpnData.encoded[0]),
      tls.alpnData.encoded.len.cuint,
    ) != 0:
    raise newException(QuicConfigError, "failed to configure ALPN: " & opensslErrorMessage())

proc init*(
    _: typedesc[TLSConfig],
    certificate: seq[byte] = @[],
    key: seq[byte] = @[],
    alpn: seq[string] = @[],
    certificateVerifier: Opt[CertificateVerifier] = Opt.none(CertificateVerifier),
): TLSConfig =
  var alpnData: AlpnSelectData = nil
  if alpn.len > 0:
    let encoded =
      try:
        encodeAlpn(alpn)
      except QuicError as exc:
        raise newException(QuicConfigError, exc.msg)
    alpnData = AlpnSelectData(encoded: encoded)
  TLSConfig(
    certificate: certificate,
    privateKey: key,
    alpn: alpn,
    verifier: certificateVerifier,
    alpnData: alpnData,
  )

proc alpnSelectCallback(
    ssl: SSL,
    outProto: ptr ptr cuchar,
    outLen: ptr cuchar,
    inProto: ptr cuchar,
    inLen: cuint,
    userData: pointer
): cint {.cdecl.} =
  discard ssl
  let payload = cast[AlpnSelectData](userData)
  if payload.isNil or payload.encoded.len == 0:
    return SSL_TLSEXT_ERR_NOACK
  var selected: ptr cuchar
  var selectedLen: cuchar
  let rc = SSL_select_next_proto(
    addr selected,
    addr selectedLen,
    cast[ptr cuchar](unsafeAddr payload.encoded[0]),
    payload.encoded.len.cuint,
    inProto,
    inLen,
  )
  if rc != OPENSSL_NPN_NEGOTIATED:
    return SSL_TLSEXT_ERR_NOACK
  outProto[] = selected
  outLen[] = selectedLen
  SSL_TLSEXT_ERR_OK

proc configureCtx(ctx: SSL_CTX, tls: var TLSConfig, isServer: bool) {.raises: [QuicConfigError].} =
  loadCertificateIntoCtx(ctx, tls)
  try:
    applyAlpn(ctx, tls)
  except QuicError as exc:
    raise newException(QuicConfigError, exc.msg)
  if isServer and not tls.alpnData.isNil and tls.alpnData.encoded.len > 0:
    nim_SSL_CTX_set_alpn_select_cb(
      ctx, cast[AlpnSelectCb](alpnSelectCallback), cast[pointer](tls.alpnData)
    )

proc init*(
    _: typedesc[QuicClient], tlsConfig: TLSConfig, rng: ref HmacDrbgContext = nil
): QuicClient {.raises: [QuicConfigError].} =
  discard rng
  let clientMethod = OSSL_QUIC_client_method()
  if clientMethod.isNil:
    raise newException(QuicConfigError, "failed to obtain OSSL_QUIC_client_method")
  let ctx = SSL_CTX_new(clientMethod)
  if ctx.isNil:
    raise newException(QuicConfigError, "SSL_CTX_new failed (client)")
  var ctxAlive = true
  defer:
    if ctxAlive:
      SSL_CTX_free(ctx)
  var tlsCopy = tlsConfig
  configureCtx(ctx, tlsCopy, false)
  ctxAlive = false
  QuicClient(ctx: ctx, tls: tlsCopy)

proc init*(
    _: typedesc[QuicServer], tlsConfig: TLSConfig, rng: ref HmacDrbgContext = nil
): QuicServer {.raises: [QuicConfigError].} =
  discard rng
  let serverMethod = OSSL_QUIC_server_method()
  if serverMethod.isNil:
    raise newException(QuicConfigError, "failed to obtain OSSL_QUIC_server_method")
  let ctx = SSL_CTX_new(serverMethod)
  if ctx.isNil:
    raise newException(QuicConfigError, "SSL_CTX_new failed (server)")
  var ctxAlive = true
  defer:
    if ctxAlive:
      SSL_CTX_free(ctx)
  var tlsCopy = tlsConfig
  configureCtx(ctx, tlsCopy, true)
  ctxAlive = false
  QuicServer(ctx: ctx, tls: tlsCopy)

proc init*(
    _: typedesc[CustomCertificateVerifier], cb: certificateVerifierCB
): CustomCertificateVerifier =
  CustomCertificateVerifier(cb: cb)

proc newCustomCertificateVerifier*(cb: certificateVerifierCB): CertificateVerifier =
  CustomCertificateVerifier(cb: cb)

proc durationFromTimeval(tv: Timeval): ctimer.Duration =
  let secs = int64(tv.tv_sec)
  let micros = int64(tv.tv_usec)
  ctimer.seconds(secs) + ctimer.microseconds(micros)

proc makeBioAddr(address: TransportAddress): Result[BIO_ADDR, ref QuicError] =
  info "makeBioAddr invoked", family = $address.family, port = int(address.port)
  let bio = BIO_ADDR_new()
  if bio.isNil:
    return err(newQuicError("BIO_ADDR_new failed"))
  var okFlag = false
  try:
    case address.family
    of AddressFamily.IPv4:
      if BIO_ADDR_rawmake(
          bio,
          cint(posix.AF_INET),
          cast[pointer](unsafeAddr address.address_v4[0]),
          address.address_v4.len.csize_t,
          cushort(nativesockets.htons(uint16(address.port))),
        ) != 1:
        return err(newQuicError("BIO_ADDR_rawmake IPv4 failed: " & opensslErrorMessage()))
    of AddressFamily.IPv6:
      if BIO_ADDR_rawmake(
          bio,
          cint(posix.AF_INET6),
          cast[pointer](unsafeAddr address.address_v6[0]),
          address.address_v6.len.csize_t,
          cushort(nativesockets.htons(uint16(address.port))),
        ) != 1:
        return err(newQuicError("BIO_ADDR_rawmake IPv6 failed: " & opensslErrorMessage()))
    else:
      return err(newQuicError("unsupported address family: " & $address.family))
    okFlag = true
  finally:
    if not okFlag:
      BIO_ADDR_free(bio)
  ok(bio)

proc waitReadable(fd: AsyncFD): Future[void] =
  let fut = newFuture[void]("openssl_quic.waitReadable")
  var registered = false

  proc cleanup() {.gcsafe.} =
    if registered:
      discard asyncengine.removeReader2(fd)
      registered = false

  proc callback(_: pointer) {.gcsafe.} =
    cleanup()
    if not fut.finished:
      fut.complete()

  registered = true
  let res = asyncengine.addReader2(fd, callback, nil)
  if res.isErr:
    cleanup()
    fut.fail(newQuicError("failed to register read event: " & osErrorMsg(res.error)))
    return fut

  proc cancellation(_: pointer) {.gcsafe.} =
    cleanup()

  fut.cancelCallback = cancellation
  fut

proc waitWritable(fd: AsyncFD): Future[void] =
  let fut = newFuture[void]("openssl_quic.waitWritable")
  var registered = false

  proc cleanup() {.gcsafe.} =
    if registered:
      discard asyncengine.removeWriter2(fd)
      registered = false

  proc callback(_: pointer) {.gcsafe.} =
    cleanup()
    if not fut.finished:
      fut.complete()

  registered = true
  let res = asyncengine.addWriter2(fd, callback, nil)
  if res.isErr:
    cleanup()
    fut.fail(newQuicError("failed to register write event: " & osErrorMsg(res.error)))
    return fut

  proc cancellation(_: pointer) {.gcsafe.} =
    cleanup()

  fut.cancelCallback = cancellation
  fut

proc releaseLockQuiet(lock: AsyncLock) {.inline.} =
  if lock.isNil:
    return
  try:
    lock.release()
  except AsyncLockError:
    discard

proc waitForSocketEvents(
    ssl: SSL,
    fd: AsyncFD,
    wantRead: bool,
    wantWrite: bool,
    ctx: string,
    lock: AsyncLock,
    lockHeld = false,
): Future[void] {.async: (raises: [QuicError, CancelledError]).} =
  if lock.isNil:
    await sleepAsync(ctimer.milliseconds(10))
    return

  var needRead = wantRead
  var needWrite = wantWrite
  var held = lockHeld

  while true:
    if not held:
      try:
        await lock.acquire()
      except AsyncLockError as exc:
        raise newQuicError(ctx & ": failed to acquire lock: " & exc.msg)
    let releaseLock = not held

    var readFut, writeFut, timerFut: Future[void]
    var futures: seq[Future[void]] = @[]
    try:
      if needRead:
        readFut = waitReadable(fd)
        futures.add(readFut)
      if needWrite:
        writeFut = waitWritable(fd)
        futures.add(writeFut)

      var tv: Timeval
      var isInf: cint
      if SSL_get_event_timeout(ssl, addr tv, addr isInf) == 1 and isInf == 0:
        var timeout = durationFromTimeval(tv)
        if timeout <= ctimer.milliseconds(0):
          timeout = ctimer.milliseconds(1)
        timerFut = sleepAsync(timeout)
        futures.add(timerFut)

      if futures.len == 0:
        return

      var finishedBase: FutureBase
      try:
        finishedBase = await race(futures.mapIt(FutureBase(it)))
      except ValueError as exc:
        raise newQuicError(ctx & ": event wait failed: " & exc.msg)
      for fut in futures:
        if FutureBase(fut) != finishedBase:
          fut.cancelSoon()

      if finishedBase.cancelled():
        raise (ref CancelledError)(msg: ctx & ": wait cancelled")
      if finishedBase.failed():
        var err: ref CatchableError = nil
        try:
          err = finishedBase.readError()
        except CatchableError as exc:
          raise newQuicError(ctx & ": event wait failed: " & exc.msg)
        if not err.isNil:
          raise newQuicError(ctx & ": event wait failed: " & err.msg)
        raise newQuicError(ctx & ": event wait failed")

      let handleRc = SSL_handle_events(ssl)
      if handleRc <= 0:
        let errCode = SSL_get_error(ssl, handleRc)
        case errCode
        of SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE:
          needRead = (errCode == SSL_ERROR_WANT_READ)
          needWrite = (errCode == SSL_ERROR_WANT_WRITE)
          continue
        of SSL_ERROR_NONE:
          return
        else:
          raise newQuicError(ctx & ": SSL_handle_events failed: " & opensslErrorMessage())
      else:
        return
    finally:
      if releaseLock:
        releaseLockQuiet(lock)
    held = false

proc pollForQuicEvents(
    ssl: SSL,
    fd: AsyncFD,
    events: uint64,
    wantRead: bool,
    wantWrite: bool,
    ctx: string,
    lock: AsyncLock,
    lockHeld = false,
    driveSsl: SSL = nil,
): Future[uint64] {.async: (raises: [QuicError, CancelledError]).} =
  if ssl.isNil:
    raise newQuicError(ctx & ": SSL object not initialized")
  let driver = if driveSsl.isNil: ssl else: driveSsl
  var zeroTimeout: Timeval
  zeroTimeout.tv_sec = posix.Time(0)
  zeroTimeout.tv_usec = posix.Suseconds(0)
  var held = lockHeld
  var pollItem: SslPollItem

  while true:
    discard SSL_handle_events(driver)
    nim_ssl_poll_item_setup(addr pollItem, ssl, events)
    var readyCount: csize_t = 0
    let rc = nim_SSL_poll(
      addr pollItem,
      1,
      addr zeroTimeout,
      0'u64,
      addr readyCount,
    )
    if rc < 0:
      nim_ssl_poll_item_reset(addr pollItem)
      raise newQuicError(ctx & ": SSL_poll call failed: " & opensslErrorMessage())
    if readyCount > 0:
      let revents = nim_ssl_poll_item_revents(addr pollItem)
      nim_ssl_poll_item_reset(addr pollItem)
      return revents

    nim_ssl_poll_item_reset(addr pollItem)
    await waitForSocketEvents(
      driver,
      fd,
      wantRead,
      wantWrite,
      ctx,
      lock,
      held,
    )
    held = false

proc driveHandshake(
    conn: Connection,
    attempt: proc(): cint {.gcsafe.},
    ctx: string,
): Future[void] {.async: (raises: [QuicError, CancelledError]).} =
  if conn.isNil or conn.ssl.isNil:
    raise newQuicError(ctx & ": invalid connection")

  while true:
    try:
      await conn.eventLock.acquire()
    except AsyncLockError as exc:
      raise newQuicError(ctx & ": failed to acquire lock: " & exc.msg)
    var rc: cint
    try:
      try:
        rc = attempt()
      except CatchableError as exc:
        raise newQuicError(ctx & ": handshake step failed: " & exc.msg)
      except Exception as exc:
        raise newQuicError(ctx & ": handshake step raised exception: " & exc.msg)
      if rc == 1:
        return
    finally:
      releaseLockQuiet(conn.eventLock)

    let errCode = SSL_get_error(conn.ssl, rc)
    case errCode
    of SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE:
      await waitForSocketEvents(
        conn.ssl,
        conn.socketFd,
        errCode == SSL_ERROR_WANT_READ,
        errCode == SSL_ERROR_WANT_WRITE,
        ctx,
        conn.eventLock,
      )
    of SSL_ERROR_ZERO_RETURN:
      raise newQuicError(ctx & ": peer closed connection during handshake")
    else:
      raise newQuicError(ctx & ": handshake failed: " & opensslErrorMessage())

proc queryLocalAddress(fd: AsyncFD): Result[TransportAddress, ref QuicError] =
  var sa: Sockaddr_storage
  var sl = SockLen(sizeof(sa))
  if getsockname(SocketHandle(fd), cast[ptr SockAddr](addr sa), addr sl) != 0:
    return err(newQuicError("failed to get local address: " & osErrorMsg(osLastError())))
  var ta: TransportAddress
  fromSAddr(addr sa, sl, ta)
  ok(ta)

proc getPeerTransportAddress(ssl: SSL): Result[TransportAddress, ref QuicError] =
  if ssl.isNil:
    return err(newQuicError("SSL object invalid"))
  let peer = BIO_ADDR_new()
  if peer.isNil:
    return err(newQuicError("BIO_ADDR_new failed"))
  defer:
    BIO_ADDR_free(peer)

  if SSL_get_peer_addr(ssl, peer) != 1:
    return err(newQuicError("SSL_get_peer_addr failed: " & opensslErrorMessage()))

  let family = BIO_ADDR_family(peer)
  let port = Port(nativesockets.ntohs(uint16(BIO_ADDR_rawport(peer))))

  case family
  of cint(nativesockets.AF_INET):
    var raw: array[4, byte]
    var rawLen = csize_t(raw.len)
    if BIO_ADDR_rawaddress(peer, addr raw[0], addr rawLen) != 1:
      return err(newQuicError("failed to parse IPv4 address: " & opensslErrorMessage()))
    if rawLen != raw.len.csize_t:
      return err(newQuicError("unexpected IPv4 address length: " & $rawLen))
    var ta = TransportAddress(family: AddressFamily.IPv4, port: port)
    for i in 0 ..< raw.len:
      ta.address_v4[i] = uint8(raw[i])
    ok(ta)
  of cint(nativesockets.AF_INET6):
    var raw: array[16, byte]
    var rawLen = csize_t(raw.len)
    if BIO_ADDR_rawaddress(peer, addr raw[0], addr rawLen) != 1:
      return err(newQuicError("failed to parse IPv6 address: " & opensslErrorMessage()))
    if rawLen != raw.len.csize_t:
      return err(newQuicError("unexpected IPv6 address length: " & $rawLen))
    var ta = TransportAddress(family: AddressFamily.IPv6, port: port)
    for i in 0 ..< raw.len:
      ta.address_v6[i] = uint8(raw[i])
    ok(ta)
  else:
    err(newQuicError("unsupported address family: " & $family))

# --------------------------
# 套接字
# --------------------------

proc createUdpSocket(address: TransportAddress, bindLocal: bool): Result[(AsyncFD, TransportAddress), ref QuicError] =
  let domain = getDomain(address)
  let sock = createAsyncSocket2(domain, SockType.SOCK_DGRAM, Protocol.IPPROTO_UDP).valueOr:
    return err(newQuicError("failed to create UDP socket: " & osErrorMsg(error)))

  var local =
    if bindLocal:
      address
    else:
      TransportAddress(family: AddressFamily.None)

  if bindLocal:
    var sa: Sockaddr_storage
    var sl: SockLen
    toSAddr(address, sa, sl)
    if osdefs.bindSocket(SocketHandle(sock), cast[ptr SockAddr](unsafeAddr sa), sl) != 0:
      let msg = osErrorMsg(osLastError())
      discard asyncengine.unregister2(sock)
      closeSocket(sock)
      return err(newQuicError("failed to bind address: " & msg))

    if address.port == Port(0):
      var saddr: Sockaddr_storage
      var slen = SockLen(sizeof(saddr))
      if posix.getsockname(SocketHandle(sock), cast[ptr SockAddr](addr saddr), addr slen) != 0:
        let msg = osErrorMsg(osLastError())
        discard asyncengine.unregister2(sock)
        closeSocket(sock)
        return err(newQuicError("failed to query local address: " & msg))
      fromSAddr(addr saddr, slen, local)

  ok((sock, local))

# --------------------------
# 监听与拨号
# --------------------------

proc listen*(
    server: QuicServer, address: TransportAddress
): Listener {.raises: [QuicError, TransportOsError].} =
  let (sock, local) = createUdpSocket(address, true).valueOr:
    raise newException(QuicError, error.msg)

  let ssl = SSL_new_listener(server.ctx, 0)
  if ssl.isNil:
    discard asyncengine.unregister2(sock)
    closeSocket(sock)
    raise newException(QuicError, "failed to create QUIC listener: " & opensslErrorMessage())

  if SSL_set_fd(ssl, cint(sock)) != 1:
    SSL_free(ssl)
    discard asyncengine.unregister2(sock)
    closeSocket(sock)
    raise newException(QuicError, "failed to bind listener socket: " & opensslErrorMessage())

  discard SSL_set_blocking_mode(ssl, 0)

  if SSL_listen(ssl) != 1:
    SSL_free(ssl)
    discard asyncengine.unregister2(sock)
    closeSocket(sock)
    raise newException(QuicError, "SSL_listen failed: " & opensslErrorMessage())

  Listener(
    ctx: server.ctx,
    ssl: ssl,
    socketFd: sock,
    local: local,
    running: true,
    tls: server.tls,
    eventLock: newAsyncLock(),
  )

proc close*(conn: Connection) {.async: (raises: []).} =
  if conn.isNil or conn.ssl.isNil:
    return
  conn.closed = true
  SSL_free(conn.ssl)
  conn.ssl = nil
  if conn.ownsSocket and conn.socketFd != asyncInvalidSocket:
    discard asyncengine.unregister2(conn.socketFd)
    closeSocket(conn.socketFd)
    conn.socketFd = asyncInvalidSocket


proc close*(listener: Listener) {.async: (raises: []).} =
  if listener.isNil:
    return
  listener.running = false
  if not listener.ssl.isNil:
    SSL_free(listener.ssl)
    listener.ssl = nil
  if listener.socketFd != asyncInvalidSocket:
    discard asyncengine.unregister2(listener.socketFd)
    closeSocket(listener.socketFd)
    listener.socketFd = asyncInvalidSocket


proc stop*(listener: Listener) {.async: (raises: []).} =
  await listener.close()

proc localAddress*(listener: Listener): TransportAddress =
  if listener.isNil: TransportAddress(family: AddressFamily.None) else: listener.local

proc dial*(
    client: QuicClient, address: TransportAddress
): Future[Connection] {.async: (raises: [QuicError, TransportOsError, CancelledError]).} =
  let (sock, _) = createUdpSocket(address, false).valueOr:
    raise newException(QuicError, error.msg)

  let ssl = SSL_new(client.ctx)
  if ssl.isNil:
    discard asyncengine.unregister2(sock)
    closeSocket(sock)
    raise newException(QuicError, "SSL_new failed: " & opensslErrorMessage())

  var peerAddr = makeBioAddr(address).valueOr:
    discard asyncengine.unregister2(sock)
    closeSocket(sock)
    SSL_free(ssl)
    raise newException(QuicError, error.msg)
  defer:
    BIO_ADDR_free(peerAddr)

  if SSL_set_fd(ssl, cint(sock)) != 1:
    discard asyncengine.unregister2(sock)
    closeSocket(sock)
    SSL_free(ssl)
    raise newException(QuicError, "SSL_set_fd failed: " & opensslErrorMessage())

  discard SSL_set_blocking_mode(ssl, 0)
  discard SSL_set_default_stream_mode(ssl, SSL_DEFAULT_STREAM_MODE_AUTO_BIDI)

  if SSL_set1_initial_peer_addr(ssl, peerAddr) != 1:
    discard asyncengine.unregister2(sock)
    closeSocket(sock)
    SSL_free(ssl)
    raise newException(QuicError, "failed to set initial peer address: " & opensslErrorMessage())

  let conn = Connection(
    ssl: ssl,
    socketFd: sock,
    remote: address,
    local: TransportAddress(family: AddressFamily.None),
    tls: client.tls,
    ownsSocket: true,
    closed: false,
    eventLock: newAsyncLock(),
    certificatesCache: @[],
    certificatesLoaded: false,
  )

  try:
    await driveHandshake(conn, proc(): cint = SSL_connect(conn.ssl), "client handshake")
  except CatchableError as exc:
    await conn.close()
    raise newException(QuicError, "client handshake failed: " & exc.msg)

  conn.local = queryLocalAddress(conn.socketFd).valueOr:
    await conn.close()
    raise newException(QuicError, error.msg)

  conn.remote = getPeerTransportAddress(conn.ssl).valueOr:
    debugEcho "OpenSSL QUIC: unable to get peer address; using dial target as fallback"
    address

  debugEcho "OpenSSL QUIC: client handshake complete local=" & conn.local.host() &
            " port=" & $int(conn.local.port)
  conn

proc accept*(
    listener: Listener
): Future[Connection] {.async: (raises: [QuicError, CancelledError]).} =
  if listener.isNil or listener.ssl.isNil:
    raise newException(QuicError, "listener not initialized")

  const ListenerPollEvents = SSL_POLL_EVENT_IC or SSL_POLL_EVENT_EL
  var child: SSL = nil

  while true:
    if not listener.running:
      raise newException(QuicError, "listener stopped")

    let revents = await pollForQuicEvents(
      listener.ssl,
      listener.socketFd,
      ListenerPollEvents,
      true,
      false,
      "listener poll",
      listener.eventLock,
    )
    if (revents and SSL_POLL_EVENT_EL) != 0:
      raise newException(
        QuicError,
        "listener detected error event: " & $(revents and SSL_POLL_EVENT_EL),
      )
    if (revents and SSL_POLL_EVENT_IC) == 0:
      continue

    try:
      await listener.eventLock.acquire()
    except AsyncLockError as exc:
      raise newException(QuicError, "failed to acquire listener lock: " & exc.msg)
    try:
      discard SSL_handle_events(listener.ssl)
      child = SSL_accept_connection(listener.ssl, SSL_ACCEPT_CONNECTION_NO_BLOCK)
    finally:
      releaseLockQuiet(listener.eventLock)

    if not child.isNil:
      break

    let errCode = SSL_get_error(listener.ssl, 0)
    case errCode
    of SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE, SSL_ERROR_WANT_ACCEPT:
      continue
    of SSL_ERROR_SSL:
      if ERR_peek_error() == 0:
        continue
      let msg = opensslErrorMessage()
      raise newException(
        QuicError,
        "failed to accept connection: errCode=" & $errCode & " msg=" & msg,
      )
    of SSL_ERROR_ZERO_RETURN:
      raise newException(QuicError, "listener closed connection")
    else:
      let msg = opensslErrorMessage()
      raise newException(
        QuicError,
        "failed to accept connection: errCode=" & $errCode & " msg=" & msg,
      )

  let conn = Connection(
    ssl: child,
    socketFd: listener.socketFd,
    remote: TransportAddress(family: AddressFamily.None),
    local: listener.local,
    tls: listener.tls,
    ownsSocket: false,
    closed: false,
    eventLock: listener.eventLock,
    certificatesCache: @[],
    certificatesLoaded: false,
  )

  discard SSL_set_blocking_mode(child, 0)
  discard SSL_set_default_stream_mode(child, SSL_DEFAULT_STREAM_MODE_AUTO_BIDI)

  try:
    await driveHandshake(conn, proc(): cint = SSL_do_handshake(conn.ssl), "server handshake")
  except CatchableError as exc:
    SSL_free(child)
    raise newException(QuicError, "server handshake failed: " & exc.msg)

  conn.remote = getPeerTransportAddress(conn.ssl).valueOr:
    debugEcho "OpenSSL QUIC: unable to parse remote address; using empty address as fallback"
    TransportAddress(family: AddressFamily.None)

  debugEcho "OpenSSL QUIC: new connection handshake complete remote=" & conn.remote.host() &
            " port=" & $int(conn.remote.port)
  return conn

# --------------------------
# Stream & Connection I/O
# --------------------------

proc newStream(conn: Connection, raw: SSL, unidirectional: bool): Stream =
  Stream(
    ssl: raw,
    connection: conn,
    eventLock: newAsyncLock(),
    unidirectional: unidirectional,
    closed: false,
  )

proc streamUnidirectional(raw: SSL): bool =
  let stype = SSL_get_stream_type(raw)
  stype == 1 or stype == 2

proc incomingStream*(
    conn: Connection
): Future[Stream] {.async: (raises: [QuicError, CancelledError]).} =
  if conn.isNil or conn.ssl.isNil:
    raise newException(QuicError, "connection not initialized")

  const StreamPollEvents = SSL_POLL_EVENT_IS or SSL_POLL_EVENT_E

  while true:
    let revents = await pollForQuicEvents(
      conn.ssl,
      conn.socketFd,
      StreamPollEvents,
      true,
      false,
      "waiting for remote stream events",
      conn.eventLock,
    )
    if (revents and SSL_POLL_EVENT_E) != 0:
      raise newException(QuicError, "remote stream error detected")
    if (revents and SSL_POLL_EVENT_IS) == 0:
      continue

    try:
      await conn.eventLock.acquire()
    except AsyncLockError as exc:
      raise newQuicError("failed to acquire lock while waiting for remote stream: " & exc.msg)
    var raw: SSL = nil
    try:
      discard SSL_handle_events(conn.ssl)
      raw = SSL_accept_stream(conn.ssl, 0)
      if not raw.isNil:
        let stream = newStream(conn, raw, streamUnidirectional(raw))
        releaseLockQuiet(conn.eventLock)
        return stream
    finally:
      if raw.isNil:
        releaseLockQuiet(conn.eventLock)

    let errCode = SSL_get_error(conn.ssl, 0)
    case errCode
    of SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE, SSL_ERROR_WANT_ACCEPT:
      continue
    of SSL_ERROR_SSL:
      if ERR_peek_error() == 0:
        continue
      let msg = opensslErrorMessage()
      raise newException(
        QuicError,
        "failed to accept remote stream: errCode=" & $errCode & " msg=" & msg,
      )
    of SSL_ERROR_ZERO_RETURN:
      raise newException(QuicError, "remote stream closed")
    else:
      let msg = opensslErrorMessage()
      debugEcho "OpenSSL QUIC: SSL_accept_stream err=", errCode, " msg=", msg
      raise newException(QuicError, "failed to accept stream: " & msg)

proc openStream*(
    conn: Connection, unidirectional = false
): Future[Stream] {.async: (raises: [QuicError, CancelledError]).} =
  if conn.isNil or conn.ssl.isNil:
    raise newException(QuicError, "connection not initialized")

  let flags =
    if unidirectional:
      uint64(1) # SSL_STREAM_FLAG_UNI
    else:
      uint64(0)
  let desiredEvents =
    if unidirectional: SSL_POLL_EVENT_OSU else: SSL_POLL_EVENT_OSB
  let eventMask = desiredEvents or SSL_POLL_EVENT_E

  while true:
    let revents = await pollForQuicEvents(
      conn.ssl,
      conn.socketFd,
      eventMask,
      true,
      true,
      "stream creation poll",
      conn.eventLock,
    )
    if (revents and SSL_POLL_EVENT_E) != 0:
      raise newException(QuicError, "connection error detected while creating stream")
    if (revents and desiredEvents) == 0:
      continue

    try:
      await conn.eventLock.acquire()
    except AsyncLockError as exc:
      raise newQuicError("failed to acquire lock while creating stream: " & exc.msg)
    var raw: SSL = nil
    try:
      discard SSL_handle_events(conn.ssl)
      raw = SSL_new_stream(conn.ssl, flags)
      if not raw.isNil:
        let stream = newStream(conn, raw, unidirectional)
        releaseLockQuiet(conn.eventLock)
        return stream
    finally:
      if raw.isNil:
        releaseLockQuiet(conn.eventLock)

    let errCode = SSL_get_error(conn.ssl, 0)
    case errCode
    of SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE:
      continue
    of SSL_ERROR_SSL:
      if ERR_peek_error() == 0:
        continue
      let msg = opensslErrorMessage()
      raise newException(QuicError, "failed to create stream: " & msg)
    else:
      raise newException(QuicError, "failed to create stream: " & opensslErrorMessage())

proc isUnidirectional*(stream: Stream): bool =
  if stream.isNil:
    false
  else:
    stream.unidirectional

proc id*(stream: Stream): uint64 =
  if stream.isNil or stream.ssl.isNil:
    0
  else:
    SSL_get_stream_id(stream.ssl)

proc read*(
    stream: Stream
): Future[seq[byte]] {.async: (raises: [QuicError, CancelledError]).} =
  if stream.isNil or stream.ssl.isNil:
    raise newException(QuicError, "stream not initialized")
  let conn = stream.connection
  if conn.isNil:
    raise newException(QuicError, "stream connection not initialized")

  var buffer = newSeq[byte](DefaultReadBuffer)

  while true:
    try:
      await stream.eventLock.acquire()
    except AsyncLockError as exc:
      raise newException(QuicError, "failed to acquire stream read lock: " & exc.msg)
    var rc: cint
    var errCode: cint
    var got: csize_t = 0
    try:
      rc = SSL_read_ex(
        stream.ssl,
        cast[pointer](unsafeAddr buffer[0]),
        buffer.len.csize_t,
        addr got,
      )
      errCode = SSL_get_error(stream.ssl, rc)
      if rc == 1:
        if got == 0:
          return @[]
        result = newSeq[byte](int(got))
        copyMem(addr result[0], unsafeAddr buffer[0], int(got))
        return result
    finally:
      releaseLockQuiet(stream.eventLock)

    case errCode
    of SSL_ERROR_WANT_READ:
      debug "quic stream read awaiting event",
        streamId = streamIdentifier(stream),
        mode = "read"
      discard await pollForQuicEvents(
        stream.ssl,
        conn.socketFd,
        StreamReadPollMask,
        true,
        true,
        "stream read",
        conn.eventLock,
        driveSsl = conn.ssl,
      )
    of SSL_ERROR_WANT_WRITE:
      debug "quic stream read awaiting event",
        streamId = streamIdentifier(stream),
        mode = "write"
      discard await pollForQuicEvents(
        stream.ssl,
        conn.socketFd,
        StreamWritePollMask,
        true,
        true,
        "stream read waiting for write event",
        conn.eventLock,
        driveSsl = conn.ssl,
      )
    of SSL_ERROR_SSL:
      let state = SSL_get_stream_read_state(stream.ssl)
      var appCode: uint64 = 0
      let hasApp = SSL_get_stream_read_error_code(stream.ssl, addr appCode) == 1
      debug "quic stream read ssl_error",
        streamId = streamIdentifier(stream),
        state = streamStateToString(state),
        appCode = (if hasApp: $appCode else: "none")
      case state
      of SSL_STREAM_STATE_FINISHED:
        return @[]
      of SSL_STREAM_STATE_RESET_REMOTE:
        raise newStreamQuicError("stream reset by remote", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_RESET_LOCAL:
        raise newStreamQuicError("stream reset locally", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_CONN_CLOSED:
        raise newStreamQuicError("connection closed; cannot read stream", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_WRONG_DIR:
        raise newStreamQuicError("incorrect stream direction; read refused", stream, state, hasApp, appCode)
      else:
        let msg = opensslErrorMessage()
        raise newStreamQuicError(
          "stream read failed: " & msg & " state=" & streamStateToString(state),
          stream,
          state,
          hasApp,
          appCode,
        )
    of SSL_ERROR_ZERO_RETURN:
      return @[]
    else:
      raise newException(QuicError, "stream read failed: " & opensslErrorMessage())

proc write*(
    stream: Stream, data: seq[byte]
): Future[void] {.async: (raises: [QuicError, CancelledError]).} =
  if stream.isNil or stream.ssl.isNil:
    raise newException(QuicError, "stream not initialized")
  let conn = stream.connection
  if conn.isNil:
    raise newException(QuicError, "stream connection not initialized")
  if data.len == 0:
    return

  var offset = 0
  while offset < data.len:
    try:
      await stream.eventLock.acquire()
    except AsyncLockError as exc:
      raise newException(QuicError, "failed to acquire stream write lock: " & exc.msg)
    var rc: cint
    var errCode: cint
    var written: csize_t = 0
    try:
      rc = SSL_write_ex(
        stream.ssl,
        cast[pointer](unsafeAddr data[offset]),
        (data.len - offset).csize_t,
        addr written,
      )
      errCode = SSL_get_error(stream.ssl, rc)
      if rc == 1:
        if written == 0:
          discard
        else:
          offset += int(written)
          continue
    finally:
      releaseLockQuiet(stream.eventLock)

    case errCode
    of SSL_ERROR_WANT_READ:
      debug "quic stream write awaiting event",
        streamId = streamIdentifier(stream),
        mode = "read"
      discard await pollForQuicEvents(
        stream.ssl,
        conn.socketFd,
        StreamReadPollMask,
        true,
        true,
        "stream write wait for read event",
        conn.eventLock,
        driveSsl = conn.ssl,
      )
    of SSL_ERROR_WANT_WRITE:
      debug "quic stream write awaiting event",
        streamId = streamIdentifier(stream),
        mode = "write"
      discard await pollForQuicEvents(
        stream.ssl,
        conn.socketFd,
        StreamWritePollMask,
        true,
        true,
        "stream write",
        conn.eventLock,
        driveSsl = conn.ssl,
      )
    of SSL_ERROR_SSL:
      let state = SSL_get_stream_write_state(stream.ssl)
      var appCode: uint64 = 0
      let hasApp = SSL_get_stream_write_error_code(stream.ssl, addr appCode) == 1
      debug "quic stream write ssl_error",
        streamId = streamIdentifier(stream),
        state = streamStateToString(state),
        appCode = (if hasApp: $appCode else: "none")
      case state
      of SSL_STREAM_STATE_FINISHED:
        raise newStreamQuicError("stream finished writing; refusing extra data", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_RESET_REMOTE:
        raise newStreamQuicError("stream write failed: remote reset", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_RESET_LOCAL:
        raise newStreamQuicError("stream write failed: local reset", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_CONN_CLOSED:
        raise newStreamQuicError("connection closed; write failed", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_WRONG_DIR:
        raise newStreamQuicError("incorrect stream direction; write refused", stream, state, hasApp, appCode)
      else:
        let msg = opensslErrorMessage()
        raise newStreamQuicError(
          "stream write failed: " & msg & " state=" & streamStateToString(state),
          stream,
          state,
          hasApp,
          appCode,
        )
    else:
      raise newException(QuicError, "stream write failed: " & opensslErrorMessage())

proc closeWrite*(
    stream: Stream
): Future[void] {.async: (raises: [QuicError, CancelledError]).} =
  if stream.isNil or stream.ssl.isNil or stream.closed:
    return
  let conn = stream.connection
  if conn.isNil:
    raise newException(QuicError, "stream connection not initialized")

  while true:
    try:
      await stream.eventLock.acquire()
    except AsyncLockError as exc:
      raise newException(QuicError, "failed to acquire close-write lock: " & exc.msg)
    var rc = SSL_stream_conclude(stream.ssl, 0)
    var errCode = SSL_get_error(stream.ssl, rc)
    releaseLockQuiet(stream.eventLock)

    case errCode
    of SSL_ERROR_NONE:
      return
    of SSL_ERROR_WANT_READ:
      discard await pollForQuicEvents(
        stream.ssl,
        conn.socketFd,
        StreamReadPollMask,
        true,
        true,
        "close write wait for read event",
        conn.eventLock,
        driveSsl = conn.ssl,
      )
    of SSL_ERROR_WANT_WRITE:
      discard await pollForQuicEvents(
        stream.ssl,
        conn.socketFd,
        StreamWritePollMask,
        true,
        true,
        "close write",
        conn.eventLock,
        driveSsl = conn.ssl,
      )
    of SSL_ERROR_SSL:
      let state = SSL_get_stream_write_state(stream.ssl)
      var appCode: uint64 = 0
      let hasApp = SSL_get_stream_write_error_code(stream.ssl, addr appCode) == 1
      debug "quic stream closeWrite ssl_error",
        streamId = streamIdentifier(stream),
        state = streamStateToString(state),
        appCode = (if hasApp: $appCode else: "none")
      case state
      of SSL_STREAM_STATE_FINISHED:
        return
      of SSL_STREAM_STATE_RESET_REMOTE:
        raise newStreamQuicError("close write failed: remote reset", stream, state, hasApp, appCode)
      of SSL_STREAM_STATE_RESET_LOCAL:
        return
      of SSL_STREAM_STATE_CONN_CLOSED:
        return
      else:
        let msg = opensslErrorMessage()
        raise newStreamQuicError(
          "close write failed: " & msg & " state=" & streamStateToString(state),
          stream,
          state,
          hasApp,
          appCode,
        )
    else:
      raise newException(QuicError, "close write failed: " & opensslErrorMessage())

proc close*(
    stream: Stream
): Future[void] {.async: (raises: [QuicError, CancelledError]).} =
  if stream.isNil or stream.closed:
    return
  if stream.ssl.isNil:
    stream.closed = true
    return
  let conn = stream.connection
  if conn.isNil:
    stream.closed = true
    return

  try:
    await stream.closeWrite()
  except QuicError:
    discard
  stream.closed = true
  SSL_free(stream.ssl)
  stream.ssl = nil

# --------------------------
# 连接接口扩展
# --------------------------

proc localAddress*(conn: Connection): TransportAddress =
  if conn.isNil:
    TransportAddress(family: AddressFamily.None)
  else:
    conn.local

proc remoteAddress*(conn: Connection): TransportAddress =
  if conn.isNil:
    TransportAddress(family: AddressFamily.None)
  else:
    conn.remote

proc certificates*(conn: Connection): seq[seq[byte]] =
  if conn.isNil or conn.ssl.isNil:
    return @[]
  if conn.certificatesLoaded:
    return conn.certificatesCache

  let cert = SSL_get1_peer_certificate(conn.ssl)
  if cert.isNil:
    conn.certificatesLoaded = true
    conn.certificatesCache = @[]
    return conn.certificatesCache
  defer:
    X509_free(cert)

  let length = i2d_X509(cert, nil)
  if length <= 0:
    conn.certificatesLoaded = true
    conn.certificatesCache = @[]
    return conn.certificatesCache

  var buf = newSeq[byte](length)
  var ptrOut = cast[ptr cuchar](unsafeAddr buf[0])
  discard i2d_X509(cert, addr ptrOut)

  conn.certificatesCache = @[buf]
  conn.certificatesLoaded = true
  conn.certificatesCache

proc sendDatagram*(conn: Connection, data: seq[byte]): bool =
  discard conn
  discard data
  false

proc incomingDatagram*(conn: Connection): Future[seq[byte]] {.async: (raises: []).} =
  discard conn
  @[]

# --------------------------
# 资源释放
# --------------------------

proc destroy*(listener: Listener) =
  discard
