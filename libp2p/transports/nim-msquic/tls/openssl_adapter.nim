## 基于 OpenSSL(quictls) 的 Nim TLS 适配层，覆盖 B3 原子任务要求。

import std/base64
import std/dynlib
import std/options
import std/sets
import std/strutils
import std/tables
import std/times

import ./common

type
  SSL_CTX* = distinct pointer
  SSL* = distinct pointer
  SSL_METHOD* = distinct pointer
  SSL_SESSION* = distinct pointer
  BIO* = distinct pointer
  X509* = distinct pointer
  EVP_PKEY* = distinct pointer

  SslEncryptionLevel = enum
    sslEncInitial = 0
    sslEncEarlyData = 1
    sslEncHandshake = 2
    sslEncApplication = 3

  SslSetSecretsCb = proc (ssl: SSL, level: SslEncryptionLevel,
                          readSecret: ptr uint8, writeSecret: ptr uint8,
                          secretLen: csize_t): cint {.cdecl.}
  SslAddHandshakeCb = proc (ssl: SSL, level: SslEncryptionLevel,
                            data: ptr uint8, len: csize_t): cint {.cdecl.}
  SslFlushFlightCb = proc (ssl: SSL): cint {.cdecl.}
  SslSendAlertCb = proc (ssl: SSL, level: SslEncryptionLevel,
                         alert: uint8): cint {.cdecl.}
  SslKeyUpdateProc = proc (ssl: SSL, updateType: cint): cint {.cdecl.}
  SslUseCertificateProc = proc (ssl: SSL, cert: X509): cint {.cdecl.}
  SslUsePrivateKeyProc = proc (ssl: SSL, key: EVP_PKEY): cint {.cdecl.}
  SslCheckPrivateKeyProc = proc (ssl: SSL): cint {.cdecl.}
  SslCtxGet0CertificateProc = proc (ctx: SSL_CTX): X509 {.cdecl.}
  SslGet1PeerCertificateProc = proc (ssl: SSL): X509 {.cdecl.}
  I2dX509Proc = proc (cert: X509, buf: ptr ptr uint8): cint {.cdecl.}

  SSL_QUIC_METHOD = object
    set_encryption_secrets*: SslSetSecretsCb
    add_handshake_data*: SslAddHandshakeCb
    flush_flight*: SslFlushFlightCb
    send_alert*: SslSendAlertCb

  OpenSslSymbolError = object of CatchableError

  TlsFlightBuffer = object
    level: TlsEncryptionLevel
    data: seq[uint8]

  TlsPendingSecret = object
    level: TlsEncryptionLevel
    direction: TlsDirection
    aead: TlsAeadAlgorithm
    hash: TlsHashAlgorithm
    material: seq[uint8]

  SessionCacheKey = object
    role: TlsRole
    serverName: string
    alpn: string

  SessionCacheEntry = object
    session: seq[uint8]
    expiration: int64

  TlsContext* = ref object
    config: TlsConfig
    sslCtx: SSL_CTX
    ssl: SSL
    quicMethod: SSL_QUIC_METHOD
    transportParameters: seq[uint8]
    pendingFlights: seq[TlsFlightBuffer]
    pendingSecrets: seq[TlsPendingSecret]
    pendingSecretViews: seq[TlsSecretView]
    pendingFlags: HashSet[TlsProcessFlag]
    negotiatedAlpn: seq[uint8]
    peerTransportParameters: Option[seq[uint8]]
    sessionResumed: bool
    earlyDataState: TlsEarlyDataState
    alertCode: uint16
    cachedSession: Option[seq[uint8]]
    destroyed: bool

template isNull(p: untyped): bool =
  cast[pointer](p) == nil

const
  SSL_KEY_UPDATE_NOT_REQUESTED = 0
  SSL_KEY_UPDATE_REQUESTED = 1

var
  libSsl {.threadvar.}: LibHandle
  libCrypto {.threadvar.}: LibHandle
  sslInitialized {.threadvar.}: bool

const SessionLifetimeMs = 5'i64 * 60 * 1000

var gSessionCache = initTable[SessionCacheKey, SessionCacheEntry]()

proc applyResumptionTicket*(ctx: TlsContext, ticket: openArray[uint8]): bool

proc epochMillis(): int64 =
  let msSinceEpoch = epochTime() * 1000.0
  if msSinceEpoch <= low(int64).float:
    return low(int64)
  if msSinceEpoch >= high(int64).float:
    return high(int64)
  int64(msSinceEpoch)

proc sessionCacheKey(config: TlsConfig): SessionCacheKey =
  SessionCacheKey(
    role: config.role,
    serverName: (if config.serverName.isSome: config.serverName.get() else: ""),
    alpn: (if config.alpns.len > 0: config.alpns[0] else: "")
  )

proc fetchCachedSession(config: TlsConfig): Option[seq[uint8]] =
  let key = sessionCacheKey(config)
  if not gSessionCache.hasKey(key):
    return none(seq[uint8])
  let entry = gSessionCache[key]
  if entry.expiration < epochMillis():
    gSessionCache.del(key)
    return none(seq[uint8])
  some(entry.session)

proc storeCachedSession(config: TlsConfig, ticket: seq[uint8]) =
  if ticket.len == 0:
    return
  let key = sessionCacheKey(config)
  gSessionCache[key] = SessionCacheEntry(session: ticket, expiration: epochMillis() + SessionLifetimeMs)

proc clearSharedSessionCache*() =
  gSessionCache.clear()

proc raiseMissingSymbol(name: string): void =
  raise newException(OpenSslSymbolError, "缺失 OpenSSL 符号: " & name)

proc loadSymbol[T](handle: LibHandle, name: string): T =
  let sym = handle.symAddr(name)
  if sym.isNil:
    raiseMissingSymbol(name)
  cast[T](sym)

type
  OpenSslInitProc = proc (opts: culong, settings: pointer): cint {.cdecl.}
  ErrGetErrorProc = proc (): culong {.cdecl.}
  ErrStringProc = proc (err: culong, buf: cstring, len: csize_t): void {.cdecl.}
  TlsMethodProc = proc (): SSL_METHOD {.cdecl.}
  SslCtxNewProc = proc (sslMethod: SSL_METHOD): SSL_CTX {.cdecl.}
  SslCtxFreeProc = proc (ctx: SSL_CTX): void {.cdecl.}
  SslCtxSetOptProc = proc (ctx: SSL_CTX, option: culong): culong {.cdecl.}
  SslCtxSetProtoProc = proc (ctx: SSL_CTX, version: cint): cint {.cdecl.}
  SslCtxUseCertProc = proc (ctx: SSL_CTX, cert: X509): cint {.cdecl.}
  SslCtxUseKeyProc = proc (ctx: SSL_CTX, pkey: EVP_PKEY): cint {.cdecl.}
  SslCtxCheckKeyProc = proc (ctx: SSL_CTX): cint {.cdecl.}
  SslCtxSetMaxEarlyDataProc = proc (ctx: SSL_CTX, maxEarly: culong): cint {.cdecl.}
  SslCtxSetAlpnSelectCbProc = proc (ctx: SSL_CTX, cb: proc (ssl: SSL, outData: ptr ptr uint8,
      outLen: ptr uint8, inData: ptr uint8, inLen: uint32, arg: pointer): cint {.cdecl, gcsafe, noSideEffect.},
      arg: pointer): void {.cdecl.}
  SslNewProc = proc (ctx: SSL_CTX): SSL {.cdecl.}
  SslFreeProc = proc (ssl: SSL): void {.cdecl.}
  SslSetAppDataProc = proc (ssl: SSL, data: pointer): void {.cdecl.}
  SslGetAppDataProc = proc (ssl: SSL): pointer {.cdecl.}
  SslSetConnectStateProc = proc (ssl: SSL): void {.cdecl.}
  SslSetAcceptStateProc = proc (ssl: SSL): void {.cdecl.}
  SslSetQuicMethodProc = proc (ssl: SSL, quicMethod: ptr SSL_QUIC_METHOD): cint {.cdecl.}
  SslSetQuicTransportParamsProc = proc (ssl: SSL, params: ptr uint8, len: csize_t): cint {.cdecl.}
  SslGetPeerQuicTransportParamsProc = proc (ssl: SSL, params: ptr ptr uint8, len: ptr csize_t): void {.cdecl.}
  SslProvideQuicDataProc = proc (ssl: SSL, level: SslEncryptionLevel, data: ptr uint8,
                                 len: csize_t): cint {.cdecl.}
  SslDoHandshakeProc = proc (ssl: SSL): cint {.cdecl.}
  SslGetErrorProc = proc (ssl: SSL, ret: cint): cint {.cdecl.}
  SslIsInitFinishedProc = proc (ssl: SSL): cint {.cdecl.}
  SslGetCurrentCipherProc = proc (ssl: SSL): pointer {.cdecl.}
  SslCipherGetIdProc = proc (cipher: pointer): culong {.cdecl.}
  SslGetAlpnSelectedProc = proc (ssl: SSL, data: ptr ptr uint8, len: ptr uint32): void {.cdecl.}
  SslSetAlpnProtosProc = proc (ssl: SSL, data: ptr uint8, len: uint32): cint {.cdecl.}
  SslSetServerNameProc = proc (ssl: SSL, name: cstring): cint {.cdecl.}
  SslSetEarlyDataEnabledProc = proc (ssl: SSL, enable: cint): cint {.cdecl.}
  SslGetEarlyDataStatusProc = proc (ssl: SSL): cint {.cdecl.}
  SslGet1SessionProc = proc (ssl: SSL): SSL_SESSION {.cdecl.}
  SslSessionFreeProc = proc (session: SSL_SESSION): void {.cdecl.}
  SslSessionReusedProc = proc (ssl: SSL): cint {.cdecl.}
  I2dSessionProc = proc (session: SSL_SESSION, buf: ptr ptr uint8): cint {.cdecl.}
  D2iSessionProc = proc (session: ptr SSL_SESSION, buf: ptr ptr uint8,
                         len: cint): SSL_SESSION {.cdecl.}
  SslSetSessionProc = proc (ssl: SSL, session: SSL_SESSION): cint {.cdecl.}
  BioNewMemBufProc = proc (data: pointer, len: cint): BIO {.cdecl.}
  BioFreeProc = proc (bio: BIO): cint {.cdecl.}
  PemReadBioX509Proc = proc (bio: BIO, outX509: ptr X509, cb: pointer, u: pointer): X509 {.cdecl.}
  PemReadBioPrivateKeyProc = proc (bio: BIO, outKey: ptr EVP_PKEY, cb: pointer, u: pointer): EVP_PKEY {.cdecl.}
  X509FreeProc = proc (cert: X509): void {.cdecl.}
  PkeyFreeProc = proc (key: EVP_PKEY): void {.cdecl.}
  OpensslFreeProc = proc (mem: pointer): void {.cdecl.}

var
  symOPENSSL_init_ssl: OpenSslInitProc
  symERR_get_error: ErrGetErrorProc
  symERR_error_string_n: ErrStringProc
  symTLS_method: TlsMethodProc
  symSSL_CTX_new: SslCtxNewProc
  symSSL_CTX_free: SslCtxFreeProc
  symSSL_CTX_set_options: SslCtxSetOptProc
  symSSL_CTX_set_min_proto_version: SslCtxSetProtoProc
  symSSL_CTX_set_max_proto_version: SslCtxSetProtoProc
  symSSL_CTX_use_certificate: SslCtxUseCertProc
  symSSL_CTX_use_PrivateKey: SslCtxUseKeyProc
  symSSL_CTX_check_private_key: SslCtxCheckKeyProc
  symSSL_CTX_set_max_early_data: SslCtxSetMaxEarlyDataProc
  symSSL_CTX_set_alpn_select_cb: SslCtxSetAlpnSelectCbProc
  symSSL_new: SslNewProc
  symSSL_free: SslFreeProc
  symSSL_set_app_data: SslSetAppDataProc
  symSSL_get_app_data: SslGetAppDataProc
  symSSL_set_connect_state: SslSetConnectStateProc
  symSSL_set_accept_state: SslSetAcceptStateProc
  symSSL_set_quic_method: SslSetQuicMethodProc
  symSSL_set_quic_transport_params: SslSetQuicTransportParamsProc
  symSSL_get_peer_quic_transport_params: SslGetPeerQuicTransportParamsProc
  symSSL_provide_quic_data: SslProvideQuicDataProc
  symSSL_do_handshake: SslDoHandshakeProc
  symSSL_get_error: SslGetErrorProc
  symSSL_is_init_finished: SslIsInitFinishedProc
  symSSL_get_current_cipher: SslGetCurrentCipherProc
  symSSL_CIPHER_get_id: SslCipherGetIdProc
  symSSL_get0_alpn_selected: SslGetAlpnSelectedProc
  symSSL_set_alpn_protos: SslSetAlpnProtosProc
  symSSL_set_tlsext_host_name: SslSetServerNameProc
  symSSL_set_quic_early_data_enabled: SslSetEarlyDataEnabledProc
  symSSL_get_early_data_status: SslGetEarlyDataStatusProc
  symSSL_get1_session: SslGet1SessionProc
  symSSL_SESSION_free: SslSessionFreeProc
  symSSL_session_reused: SslSessionReusedProc
  symI2d_SSL_SESSION: I2dSessionProc
  symD2i_SSL_SESSION: D2iSessionProc
  symSSL_set_session: SslSetSessionProc
  symSSL_key_update: SslKeyUpdateProc
  symSSL_use_certificate: SslUseCertificateProc
  symSSL_use_PrivateKey: SslUsePrivateKeyProc
  symSSL_check_private_key: SslCheckPrivateKeyProc
  symSSL_CTX_get0_certificate: SslCtxGet0CertificateProc
  symSSL_get1_peer_certificate: SslGet1PeerCertificateProc
  symI2d_X509: I2dX509Proc
  symBIO_new_mem_buf: BioNewMemBufProc
  symBIO_free: BioFreeProc
  symPEM_read_bio_X509: PemReadBioX509Proc
  symPEM_read_bio_PrivateKey: PemReadBioPrivateKeyProc
  symX509_free: X509FreeProc
  symEVP_PKEY_free: PkeyFreeProc
  symOPENSSL_free: OpensslFreeProc

proc ensureOpenSslLoaded() =
  if sslInitialized:
    return

  when defined(windows):
    const sslNames = ["libssl-3-x64.dll", "libssl-1_1-x64.dll", "libssl.dll"]
    const cryptoNames = ["libcrypto-3-x64.dll", "libcrypto-1_1-x64.dll", "libcrypto.dll"]
  elif defined(macosx):
    const sslNames = ["libssl.3.dylib", "libssl.1.1.dylib", "libssl.dylib"]
    const cryptoNames = ["libcrypto.3.dylib", "libcrypto.1.1.dylib", "libcrypto.dylib"]
  else:
    const sslNames = ["libssl.so.3", "libssl.so.1.1", "libssl.so"]
    const cryptoNames = ["libcrypto.so.3", "libcrypto.so.1.1", "libcrypto.so"]

  for name in sslNames:
    libSsl = loadLib(name)
    if libSsl != nil:
      break
  if libSsl.isNil:
    raise newException(OpenSslSymbolError, "无法加载 OpenSSL 库 (ssl)")

  for name in cryptoNames:
    libCrypto = loadLib(name)
    if libCrypto != nil:
      break
  if libCrypto.isNil:
    raise newException(OpenSslSymbolError, "无法加载 OpenSSL 库 (crypto)")

  symOPENSSL_init_ssl = loadSymbol[OpenSslInitProc](libSsl, "OPENSSL_init_ssl")
  symERR_get_error = loadSymbol[ErrGetErrorProc](libCrypto, "ERR_get_error")
  symERR_error_string_n = loadSymbol[ErrStringProc](libCrypto, "ERR_error_string_n")
  symTLS_method = loadSymbol[TlsMethodProc](libSsl, "TLS_method")
  symSSL_CTX_new = loadSymbol[SslCtxNewProc](libSsl, "SSL_CTX_new")
  symSSL_CTX_free = loadSymbol[SslCtxFreeProc](libSsl, "SSL_CTX_free")
  symSSL_CTX_set_options = loadSymbol[SslCtxSetOptProc](libSsl, "SSL_CTX_set_options")
  symSSL_CTX_set_min_proto_version = loadSymbol[SslCtxSetProtoProc](libSsl, "SSL_CTX_set_min_proto_version")
  symSSL_CTX_set_max_proto_version = loadSymbol[SslCtxSetProtoProc](libSsl, "SSL_CTX_set_max_proto_version")
  symSSL_CTX_use_certificate = loadSymbol[SslCtxUseCertProc](libSsl, "SSL_CTX_use_certificate")
  symSSL_CTX_use_PrivateKey = loadSymbol[SslCtxUseKeyProc](libSsl, "SSL_CTX_use_PrivateKey")
  symSSL_CTX_check_private_key = loadSymbol[SslCtxCheckKeyProc](libSsl, "SSL_CTX_check_private_key")
  symSSL_CTX_set_max_early_data = loadSymbol[SslCtxSetMaxEarlyDataProc](libSsl, "SSL_CTX_set_max_early_data")
  symSSL_CTX_set_alpn_select_cb = loadSymbol[SslCtxSetAlpnSelectCbProc](libSsl, "SSL_CTX_set_alpn_select_cb")
  symSSL_new = loadSymbol[SslNewProc](libSsl, "SSL_new")
  symSSL_free = loadSymbol[SslFreeProc](libSsl, "SSL_free")
  symSSL_set_app_data = loadSymbol[SslSetAppDataProc](libSsl, "SSL_set_app_data")
  symSSL_get_app_data = loadSymbol[SslGetAppDataProc](libSsl, "SSL_get_app_data")
  symSSL_set_connect_state = loadSymbol[SslSetConnectStateProc](libSsl, "SSL_set_connect_state")
  symSSL_set_accept_state = loadSymbol[SslSetAcceptStateProc](libSsl, "SSL_set_accept_state")
  symSSL_set_quic_method = loadSymbol[SslSetQuicMethodProc](libSsl, "SSL_set_quic_method")
  symSSL_set_quic_transport_params = loadSymbol[SslSetQuicTransportParamsProc](libSsl, "SSL_set_quic_transport_params")
  symSSL_get_peer_quic_transport_params = loadSymbol[SslGetPeerQuicTransportParamsProc](libSsl, "SSL_get_peer_quic_transport_params")
  symSSL_provide_quic_data = loadSymbol[SslProvideQuicDataProc](libSsl, "SSL_provide_quic_data")
  symSSL_do_handshake = loadSymbol[SslDoHandshakeProc](libSsl, "SSL_do_handshake")
  symSSL_get_error = loadSymbol[SslGetErrorProc](libSsl, "SSL_get_error")
  symSSL_is_init_finished = loadSymbol[SslIsInitFinishedProc](libSsl, "SSL_is_init_finished")
  symSSL_get_current_cipher = loadSymbol[SslGetCurrentCipherProc](libSsl, "SSL_get_current_cipher")
  symSSL_CIPHER_get_id = loadSymbol[SslCipherGetIdProc](libSsl, "SSL_CIPHER_get_id")
  symSSL_get0_alpn_selected = loadSymbol[SslGetAlpnSelectedProc](libSsl, "SSL_get0_alpn_selected")
  symSSL_set_alpn_protos = loadSymbol[SslSetAlpnProtosProc](libSsl, "SSL_set_alpn_protos")
  symSSL_set_tlsext_host_name = loadSymbol[SslSetServerNameProc](libSsl, "SSL_set_tlsext_host_name")
  symSSL_set_quic_early_data_enabled = loadSymbol[SslSetEarlyDataEnabledProc](libSsl, "SSL_set_quic_early_data_enabled")
  symSSL_get_early_data_status = loadSymbol[SslGetEarlyDataStatusProc](libSsl, "SSL_get_early_data_status")
  symSSL_get1_session = loadSymbol[SslGet1SessionProc](libSsl, "SSL_get1_session")
  symSSL_SESSION_free = loadSymbol[SslSessionFreeProc](libSsl, "SSL_SESSION_free")
  symSSL_session_reused = loadSymbol[SslSessionReusedProc](libSsl, "SSL_session_reused")
  symI2d_SSL_SESSION = loadSymbol[I2dSessionProc](libSsl, "i2d_SSL_SESSION")
  symD2i_SSL_SESSION = loadSymbol[D2iSessionProc](libSsl, "d2i_SSL_SESSION")
  symSSL_set_session = loadSymbol[SslSetSessionProc](libSsl, "SSL_set_session")
  symSSL_key_update = loadSymbol[SslKeyUpdateProc](libSsl, "SSL_key_update")
  symSSL_use_certificate = loadSymbol[SslUseCertificateProc](libSsl, "SSL_use_certificate")
  symSSL_use_PrivateKey = loadSymbol[SslUsePrivateKeyProc](libSsl, "SSL_use_PrivateKey")
  symSSL_check_private_key = loadSymbol[SslCheckPrivateKeyProc](libSsl, "SSL_check_private_key")
  symSSL_CTX_get0_certificate = loadSymbol[SslCtxGet0CertificateProc](libSsl, "SSL_CTX_get0_certificate")
  symSSL_get1_peer_certificate = loadSymbol[SslGet1PeerCertificateProc](libSsl, "SSL_get1_peer_certificate")
  symI2d_X509 = loadSymbol[I2dX509Proc](libCrypto, "i2d_X509")
  symBIO_new_mem_buf = loadSymbol[BioNewMemBufProc](libCrypto, "BIO_new_mem_buf")
  symBIO_free = loadSymbol[BioFreeProc](libCrypto, "BIO_free")
  symPEM_read_bio_X509 = loadSymbol[PemReadBioX509Proc](libCrypto, "PEM_read_bio_X509")
  symPEM_read_bio_PrivateKey = loadSymbol[PemReadBioPrivateKeyProc](libCrypto, "PEM_read_bio_PrivateKey")
  symX509_free = loadSymbol[X509FreeProc](libCrypto, "X509_free")
  symEVP_PKEY_free = loadSymbol[PkeyFreeProc](libCrypto, "EVP_PKEY_free")
  symOPENSSL_free = loadSymbol[OpensslFreeProc](libCrypto, "OPENSSL_free")

  let initResult = symOPENSSL_init_ssl(0, nil)
  if initResult != 1:
    raise newException(OpenSslSymbolError, "初始化 OpenSSL 失败")
  sslInitialized = true

proc openSslAvailable*(): bool =
  try:
    ensureOpenSslLoaded()
    true
  except OpenSslSymbolError:
    false
  except CatchableError:
    false

proc sslErrorMessage(): string =
  var buf = newString(256)
  let code = symERR_get_error()
  if code == 0:
    return "unknown error"
  symERR_error_string_n(code, buf.cstring, buf.len.csize_t)
  result = buf.split('\0')[0]

proc toTlsLevel(level: SslEncryptionLevel): TlsEncryptionLevel =
  case level
  of sslEncInitial: telInitial
  of sslEncEarlyData: telZeroRtt
  of sslEncHandshake: telHandshake
  of sslEncApplication: telOneRtt

proc toSslLevel(level: TlsEncryptionLevel): SslEncryptionLevel =
  case level
  of telInitial: sslEncInitial
  of telZeroRtt: sslEncEarlyData
  of telHandshake: sslEncHandshake
  of telOneRtt: sslEncApplication

proc cipherToAlgorithms(cipherId: culong): (TlsAeadAlgorithm, TlsHashAlgorithm) =
  case cipherId.uint64
  of 0x03001301'u64: (taaAes128Gcm, thaSha256)
  of 0x03001302'u64: (taaAes256Gcm, thaSha384)
  of 0x03001303'u64: (taaChacha20Poly1305, thaSha256)
  else:
    raise newException(OpenSslSymbolError, "未知 CipherId: " & $cipherId)

proc toTlsSecret(level: SslEncryptionLevel, direction: TlsDirection,
                 secret: ptr uint8, secretLen: csize_t,
                 aeadAlg: TlsAeadAlgorithm, hashAlg: TlsHashAlgorithm): TlsPendingSecret =
  var material = newSeq[uint8](int secretLen)
  if secretLen != 0:
    copyMem(material[0].addr, secret, int secretLen)
  TlsPendingSecret(
    level: toTlsLevel(level),
    direction: direction,
    aead: aeadAlg,
    hash: hashAlg,
    material: material
  )

proc tlsContext(ssl: SSL): TlsContext =
  result = cast[TlsContext](symSSL_get_app_data(ssl))

proc appendFlight(ctx: TlsContext, level: SslEncryptionLevel,
                  data: ptr uint8, len: csize_t) =
  if len == 0:
    return
  let tlsLevel = toTlsLevel(level)
  if ctx.pendingFlights.len > 0 and ctx.pendingFlights[^1].level == tlsLevel:
    let start = ctx.pendingFlights[^1].data.len
    ctx.pendingFlights[^1].data.setLen(start + int len)
    copyMem(ctx.pendingFlights[^1].data[start].addr, data, int len)
  else:
    var payload = newSeq[uint8](int len)
    copyMem(payload[0].addr, data, int len)
    ctx.pendingFlights.add TlsFlightBuffer(level: tlsLevel, data: payload)
  ctx.pendingFlags.incl tpfHasData

proc setSecretsCallback(ssl: SSL, level: SslEncryptionLevel,
                        readSecret: ptr uint8, writeSecret: ptr uint8,
                        secretLen: csize_t): cint {.cdecl.} =
  let ctx = tlsContext(ssl)
  if ctx.isNil:
    return 0
  let cipher = symSSL_CIPHER_get_id(symSSL_get_current_cipher(ssl))
  let (aeadAlg, hashAlg) = cipherToAlgorithms(cipher)
  let tlsLevel = toTlsLevel(level)
  if readSecret != nil:
    ctx.pendingSecretViews.add TlsSecretView(
      level: tlsLevel,
      direction: tdRead,
      aead: aeadAlg,
      hash: hashAlg,
      dataPtr: cast[pointer](readSecret),
      length: uint16(secretLen)
    )
    ctx.pendingSecrets.add toTlsSecret(level, tdRead, readSecret, secretLen, aeadAlg, hashAlg)
    ctx.pendingFlags.incl tpfReadKeyUpdated
  if writeSecret != nil:
    ctx.pendingSecretViews.add TlsSecretView(
      level: tlsLevel,
      direction: tdWrite,
      aead: aeadAlg,
      hash: hashAlg,
      dataPtr: cast[pointer](writeSecret),
      length: uint16(secretLen)
    )
    ctx.pendingSecrets.add toTlsSecret(level, tdWrite, writeSecret, secretLen, aeadAlg, hashAlg)
    ctx.pendingFlags.incl tpfWriteKeyUpdated
  return 1

proc addHandshakeCallback(ssl: SSL, level: SslEncryptionLevel,
                          data: ptr uint8, len: csize_t): cint {.cdecl.} =
  let ctx = tlsContext(ssl)
  if ctx.isNil:
    return 0
  ctx.appendFlight(level, data, len)
  1

proc flushFlightCallback(ssl: SSL): cint {.cdecl.} =
  let ctx = tlsContext(ssl)
  if ctx.isNil:
    return 0
  1

proc sendAlertCallback(ssl: SSL, level: SslEncryptionLevel, alert: uint8): cint {.cdecl.} =
  let ctx = tlsContext(ssl)
  if ctx.isNil:
    return 0
  ctx.alertCode = (uint16(alert) shl 8) or uint16(level.ord)
  1

proc alpnSelectCallback(ssl: SSL, outData: ptr ptr uint8, outLen: ptr uint8,
                        inData: ptr uint8, inLen: uint32, arg: pointer): cint {.cdecl, gcsafe, noSideEffect.} =
  let ctx = cast[TlsContext](arg)
  if ctx.isNil or ctx.config.alpns.len == 0:
    return 1
  let buffer = cast[ptr UncheckedArray[uint8]](inData)
  let baseAddr = cast[uint](inData)
  var offset = 0
  while offset < int(inLen):
    let length = int(buffer[offset])
    let start = offset + 1
    let stop = start + length
    if stop > int(inLen):
      break
    let candidatePtr = cast[ptr uint8](baseAddr + uint(start))
    for alpn in ctx.config.alpns:
      if alpn.len == length and cmpMem(candidatePtr, cast[ptr uint8](alpn.cstring), length) == 0:
        outData[] = candidatePtr
        outLen[] = uint8(length)
        ctx.negotiatedAlpn = newSeq[uint8](length)
        copyMem(ctx.negotiatedAlpn[0].addr, candidatePtr, length)
        return 0
    offset = stop
  return 1

proc freeSessionData(session: SSL_SESSION) =
  if not isNull(session):
    symSSL_SESSION_free(session)

proc encodeSession(session: SSL_SESSION): Option[seq[uint8]] =
  if isNull(session):
    return none(seq[uint8])
  var bufferPtr: ptr uint8 = nil
  let length = symI2d_SSL_SESSION(session, bufferPtr.addr)
  if length <= 0 or bufferPtr.isNil:
    return none(seq[uint8])
  defer: symOPENSSL_free(bufferPtr)
  var resultSeq = newSeq[uint8](length)
  copyMem(resultSeq[0].addr, bufferPtr, length)
  some(resultSeq)

proc decodeSession(data: openArray[uint8]): Option[SSL_SESSION] =
  if data.len == 0:
    return none(SSL_SESSION)
  var ptrData = cast[ptr uint8](data[0].unsafeAddr)
  let session = symD2i_SSL_SESSION(nil, ptrData.addr, cint(data.len))
  if isNull(session):
    return none(SSL_SESSION)
  some(session)

proc encodeAlpns(alpns: seq[string]): seq[uint8] =
  result = @[]
  for alpn in alpns:
    let length = alpn.len
    if length == 0 or length > 255:
      raise newException(OpenSslSymbolError, "ALPN 长度非法: " & alpn)
    result.add uint8(length)
    for ch in alpn:
      result.add uint8(ch.ord)

proc x509ToDer(x509: X509): seq[uint8] =
  var buffer: ptr uint8 = nil
  let length = symI2d_X509(x509, buffer.addr)
  if length <= 0 or buffer.isNil:
    raise newException(OpenSslSymbolError, "X509 编码失败: " & sslErrorMessage())
  defer:
    if buffer != nil:
      symOPENSSL_free(buffer)
  result = newSeq[uint8](int(length))
  if length > 0:
    copyMem(result[0].addr, buffer, int(length))

proc certificateFingerprint(x509: X509): string =
  let der = x509ToDer(x509)
  encode(der)

proc applyServerCertificate(ctx: TlsContext, certificatePem, privateKeyPem: string,
                            includeSsl: bool) =
  if ctx.isNil or isNull(ctx.sslCtx):
    raise newException(OpenSslSymbolError, "TLS 上下文尚未初始化")
  let certBio = symBIO_new_mem_buf(cast[pointer](certificatePem.cstring), cint(certificatePem.len))
  if isNull(certBio):
    raise newException(OpenSslSymbolError, "创建证书 BIO 失败")
  defer:
    if not isNull(certBio):
      discard symBIO_free(certBio)
  let keyBio = symBIO_new_mem_buf(cast[pointer](privateKeyPem.cstring), cint(privateKeyPem.len))
  if isNull(keyBio):
    raise newException(OpenSslSymbolError, "创建私钥 BIO 失败")
  defer:
    if not isNull(keyBio):
      discard symBIO_free(keyBio)

  let x509 = symPEM_read_bio_X509(certBio, nil, nil, nil)
  if isNull(x509):
    raise newException(OpenSslSymbolError, "解析证书失败: " & sslErrorMessage())
  defer:
    symX509_free(x509)

  let pkey = symPEM_read_bio_PrivateKey(keyBio, nil, nil, nil)
  if isNull(pkey):
    raise newException(OpenSslSymbolError, "解析私钥失败: " & sslErrorMessage())
  defer:
    symEVP_PKEY_free(pkey)

  if symSSL_CTX_use_certificate(ctx.sslCtx, x509) != 1:
    raise newException(OpenSslSymbolError, "加载证书失败: " & sslErrorMessage())
  if includeSsl:
    if isNull(ctx.ssl):
      raise newException(OpenSslSymbolError, "TLS 上下文未初始化 SSL 实例")
    if symSSL_use_certificate(ctx.ssl, x509) != 1:
      raise newException(OpenSslSymbolError, "更新 SSL 证书失败: " & sslErrorMessage())

  if symSSL_CTX_use_PrivateKey(ctx.sslCtx, pkey) != 1:
    raise newException(OpenSslSymbolError, "加载私钥失败: " & sslErrorMessage())
  if includeSsl:
    if symSSL_use_PrivateKey(ctx.ssl, pkey) != 1:
      raise newException(OpenSslSymbolError, "更新 SSL 私钥失败: " & sslErrorMessage())

  if symSSL_CTX_check_private_key(ctx.sslCtx) != 1:
    raise newException(OpenSslSymbolError, "证书与私钥不匹配: " & sslErrorMessage())
  if includeSsl:
    if symSSL_check_private_key(ctx.ssl) != 1:
      raise newException(OpenSslSymbolError, "SSL 证书与私钥不匹配: " & sslErrorMessage())

proc prepareTlsContext(config: TlsConfig): TlsContext =
  ensureOpenSslLoaded()

  let tlsMethod = symTLS_method()
  if isNull(tlsMethod):
    raise newException(OpenSslSymbolError, "TLS_method 返回空指针")
  let ctx = symSSL_CTX_new(tlsMethod)
  if isNull(ctx):
    raise newException(OpenSslSymbolError, "SSL_CTX_new 失败: " & sslErrorMessage())

  var tlsCtx = TlsContext(
    config: config,
    sslCtx: ctx,
    transportParameters: config.transportParameters,
    pendingFlights: @[],
    pendingSecrets: @[],
    pendingSecretViews: @[],
    pendingFlags: initHashSet[TlsProcessFlag](),
    negotiatedAlpn: @[],
    peerTransportParameters: none(seq[uint8]),
    sessionResumed: false,
    earlyDataState: tedsUnknown,
    alertCode: 0,
    cachedSession: none(seq[uint8]),
    destroyed: false
  )

  const TLS1_3_VERSION = 0x0304
  discard symSSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION.cint)
  discard symSSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION.cint)
  discard symSSL_CTX_set_options(ctx, 0x00000010.culong) # SSL_OP_NO_TICKET 清除旧票据再由 QUIC 控制

  if config.role == tlsServer:
    if config.certificatePem.isNone or config.privateKeyPem.isNone:
      raise newException(OpenSslSymbolError, "服务器模式需要证书与私钥")
    applyServerCertificate(tlsCtx, config.certificatePem.get(), config.privateKeyPem.get(), false)

    if config.enableZeroRtt:
      discard symSSL_CTX_set_max_early_data(ctx, culong(0xffffffff'u64))
    symSSL_CTX_set_alpn_select_cb(ctx, alpnSelectCallback, cast[pointer](tlsCtx))

  let ssl = symSSL_new(ctx)
  if isNull(ssl):
    symSSL_CTX_free(ctx)
    raise newException(OpenSslSymbolError, "SSL_new 失败: " & sslErrorMessage())

  tlsCtx.ssl = ssl

  var methodCallbacks = SSL_QUIC_METHOD(
    set_encryption_secrets: setSecretsCallback,
    add_handshake_data: addHandshakeCallback,
    flush_flight: flushFlightCallback,
    send_alert: sendAlertCallback
  )
  tlsCtx.quicMethod = methodCallbacks

  if symSSL_set_quic_method(ssl, tlsCtx.quicMethod.addr) != 1:
    symSSL_free(ssl)
    symSSL_CTX_free(ctx)
    raise newException(OpenSslSymbolError, "SSL_set_quic_method 失败: " & sslErrorMessage())

  if tlsCtx.transportParameters.len > 0:
    if symSSL_set_quic_transport_params(ssl, tlsCtx.transportParameters[0].addr,
        tlsCtx.transportParameters.len.csize_t) != 1:
      symSSL_free(ssl)
      symSSL_CTX_free(ctx)
      raise newException(OpenSslSymbolError, "设置 QUIC TP 失败: " & sslErrorMessage())

  symSSL_set_app_data(ssl, cast[pointer](tlsCtx))

  if config.role == tlsClient:
    symSSL_set_connect_state(ssl)
    let alpnWire = encodeAlpns(config.alpns)
    if alpnWire.len > 0:
      discard symSSL_set_alpn_protos(ssl, alpnWire[0].unsafeAddr, uint32(alpnWire.len))
    if config.serverName.isSome:
      discard symSSL_set_tlsext_host_name(ssl, config.serverName.get().cstring)
    if config.enableZeroRtt:
      discard symSSL_set_quic_early_data_enabled(ssl, cint(1))
    if config.resumptionTicket.isSome:
      let maybeSession = decodeSession(config.resumptionTicket.get())
      if maybeSession.isSome:
        let session = maybeSession.get()
        if symSSL_set_session(ssl, session) == 1:
          tlsCtx.sessionResumed = true
        freeSessionData(session)
    elif config.useSharedSessionCache:
      let cached = fetchCachedSession(config)
      if cached.isSome:
        discard applyResumptionTicket(tlsCtx, cached.get())
  else:
    symSSL_set_accept_state(ssl)
    if config.enableZeroRtt:
      discard symSSL_set_quic_early_data_enabled(ssl, cint(1))

  tlsCtx

proc destroy*(ctx: TlsContext) =
  if ctx.isNil or ctx.destroyed:
    return
  ctx.destroyed = true
  if not isNull(ctx.ssl):
    symSSL_free(ctx.ssl)
    ctx.ssl = SSL(nil)
  if not isNull(ctx.sslCtx):
    symSSL_CTX_free(ctx.sslCtx)
    ctx.sslCtx = SSL_CTX(nil)

proc mapProcessFlags(ctx: TlsContext, result: var TlsProcessResult) =
  for flag in ctx.pendingFlags:
    result.flags.incl flag
  ctx.pendingFlags.clear()

proc drainFlights(ctx: TlsContext, result: var TlsProcessResult) =
  for flight in ctx.pendingFlights.mitems:
    result.chunks.add TlsHandshakeChunk(level: flight.level, data: flight.data)
  ctx.pendingFlights.setLen(0)

proc drainSecrets(ctx: TlsContext, result: var TlsProcessResult) =
  for item in ctx.pendingSecrets.mitems:
    result.secrets.add TlsSecret(
      level: item.level,
      direction: item.direction,
      aead: item.aead,
      hash: item.hash,
      material: item.material
    )
  ctx.pendingSecrets.setLen(0)

proc updateSessionCache(ctx: TlsContext) =
  if ctx.config.role == tlsClient:
    let session = symSSL_get1_session(ctx.ssl)
    if not isNull(session):
      defer: freeSessionData(session)
      let encoded = encodeSession(session)
      ctx.cachedSession = encoded
      if ctx.config.useSharedSessionCache and encoded.isSome:
        storeCachedSession(ctx.config, encoded.get())

proc pullPeerTp(ctx: TlsContext) =
  var ptrParams: ptr uint8 = nil
  var length: csize_t = 0
  symSSL_get_peer_quic_transport_params(ctx.ssl, ptrParams.addr, length.addr)
  if ptrParams != nil and length != 0:
    var data = newSeq[uint8](int length)
    copyMem(data[0].addr, ptrParams, int length)
    ctx.peerTransportParameters = some(data)

proc processHandshake*(ctx: TlsContext, incoming: seq[TlsHandshakeChunk] = @[]): TlsProcessResult =
  if ctx.destroyed:
    raise newException(OpenSslSymbolError, "TLS 上下文已销毁")

  ctx.pendingFlags.clear()
  ctx.pendingFlights.setLen(0)
  ctx.pendingSecrets.setLen(0)
  ctx.pendingSecretViews.setLen(0)
  ctx.earlyDataState = tedsUnknown
  ctx.alertCode = 0
  ctx.negotiatedAlpn.setLen(0)
  ctx.peerTransportParameters = none(seq[uint8])
  ctx.cachedSession = none(seq[uint8])

  for chunk in incoming:
    if chunk.data.len == 0:
      continue
    if symSSL_provide_quic_data(ctx.ssl, toSslLevel(chunk.level),
        chunk.data[0].unsafeAddr, csize_t(chunk.data.len)) != 1:
      raise newException(OpenSslSymbolError, "SSL_provide_quic_data 失败: " & sslErrorMessage())

  let ret = symSSL_do_handshake(ctx.ssl)
  var outcome = TlsProcessResult(
    flags: {},
    chunks: @[],
    secrets: @[],
    secretViews: @[],
    sessionResumed: ctx.sessionResumed,
    earlyDataState: ctx.earlyDataState,
    alertCode: ctx.alertCode,
    negotiatedAlpn: ctx.negotiatedAlpn,
    peerTransportParameters: ctx.peerTransportParameters,
    rawSession: ctx.cachedSession
  )

  if ret != 1:
    let err = symSSL_get_error(ctx.ssl, ret)
    case err
    of 1, 2: discard
    else:
      raise newException(OpenSslSymbolError, "SSL_do_handshake 失败: " & $err & " / " & sslErrorMessage())
  else:
    ctx.pendingFlags.incl tpfHandshakeComplete
    ctx.sessionResumed = symSSL_session_reused(ctx.ssl) == 1
    ctx.earlyDataState =
      case symSSL_get_early_data_status(ctx.ssl)
      of 0: tedsUnsupported
      of 1: tedsAccepted
      of 2: tedsRejected
      else: tedsUnknown
    case ctx.earlyDataState
    of tedsAccepted: ctx.pendingFlags.incl tpfEarlyDataAccepted
    of tedsRejected: ctx.pendingFlags.incl tpfEarlyDataRejected
    else: discard
    updateSessionCache(ctx)
    pullPeerTp(ctx)
    var alpnPtr: ptr uint8 = nil
    var alpnLen: uint32 = 0
    symSSL_get0_alpn_selected(ctx.ssl, alpnPtr.addr, alpnLen.addr)
    if alpnPtr != nil and alpnLen != 0:
      ctx.negotiatedAlpn = newSeq[uint8](int alpnLen)
      copyMem(ctx.negotiatedAlpn[0].addr, alpnPtr, int alpnLen)

  mapProcessFlags(ctx, outcome)
  drainFlights(ctx, outcome)
  drainSecrets(ctx, outcome)
  outcome.secretViews = ctx.pendingSecretViews
  ctx.pendingSecretViews.setLen(0)
  outcome.earlyDataState = ctx.earlyDataState
  outcome.negotiatedAlpn = ctx.negotiatedAlpn
  outcome.sessionResumed = ctx.sessionResumed
  outcome.peerTransportParameters = ctx.peerTransportParameters
  outcome.rawSession = ctx.cachedSession
  outcome.alertCode = ctx.alertCode
  outcome

proc getResumptionTicket*(ctx: TlsContext): Option[seq[uint8]] =
  ctx.cachedSession

proc negotiatedAlpn*(ctx: TlsContext): Option[string] =
  if ctx.negotiatedAlpn.len == 0:
    none(string)
  else:
    var s = newString(ctx.negotiatedAlpn.len)
    copyMem(s[0].addr, ctx.negotiatedAlpn[0].unsafeAddr, ctx.negotiatedAlpn.len)
    some(s)

proc newOpenSslContext*(config: TlsConfig): TlsContext =
  result = prepareTlsContext(config)

proc triggerKeyUpdate*(ctx: TlsContext, requestPeerUpdate = false) =
  if ctx.isNil or ctx.destroyed:
    raise newException(OpenSslSymbolError, "TLS 上下文已销毁")
  let mode = if requestPeerUpdate: SSL_KEY_UPDATE_REQUESTED
             else: SSL_KEY_UPDATE_NOT_REQUESTED
  if symSSL_key_update(ctx.ssl, cint(mode)) != 1:
    raise newException(OpenSslSymbolError, "SSL_key_update 失败: " & sslErrorMessage())

proc applyResumptionTicket*(ctx: TlsContext, ticket: openArray[uint8]): bool =
  if ctx.isNil or ctx.destroyed:
    raise newException(OpenSslSymbolError, "TLS 上下文已销毁")
  let maybeSession = decodeSession(ticket)
  if maybeSession.isNone:
    return false
  let session = maybeSession.get()
  defer:
    freeSessionData(session)
  let status = symSSL_set_session(ctx.ssl, session)
  if status == 1:
    ctx.sessionResumed = true
    return true
  false

proc updateServerCertificate*(ctx: TlsContext, certificatePem, privateKeyPem: string) =
  if ctx.isNil or ctx.destroyed:
    raise newException(OpenSslSymbolError, "TLS 上下文已销毁")
  if ctx.config.role != tlsServer:
    raise newException(OpenSslSymbolError, "仅服务器上下文支持证书更新")
  applyServerCertificate(ctx, certificatePem, privateKeyPem, true)
  ctx.config.certificatePem = some(certificatePem)
  ctx.config.privateKeyPem = some(privateKeyPem)

proc currentCertificateFingerprint*(ctx: TlsContext): Option[string] =
  if ctx.isNil or ctx.destroyed:
    return none(string)
  let cert = symSSL_CTX_get0_certificate(ctx.sslCtx)
  if isNull(cert):
    return none(string)
  some(certificateFingerprint(cert))

proc peerCertificateFingerprint*(ctx: TlsContext): Option[string] =
  if ctx.isNil or ctx.destroyed:
    return none(string)
  let cert = symSSL_get1_peer_certificate(ctx.ssl)
  if isNull(cert):
    return none(string)
  defer:
    symX509_free(cert)
  some(certificateFingerprint(cert))

proc fingerprintFromPem*(certificatePem: string): string =
  ensureOpenSslLoaded()
  let bio = symBIO_new_mem_buf(cast[pointer](certificatePem.cstring), cint(certificatePem.len))
  if isNull(bio):
    raise newException(OpenSslSymbolError, "创建证书 BIO 失败")
  defer:
    discard symBIO_free(bio)
  let x509 = symPEM_read_bio_X509(bio, nil, nil, nil)
  if isNull(x509):
    raise newException(OpenSslSymbolError, "解析证书失败: " & sslErrorMessage())
  defer:
    symX509_free(x509)
  certificateFingerprint(x509)
