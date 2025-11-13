## MsQuic-only transport skeleton.
## 该模块逐步取代依赖 OpenSSL 的 `quictransport.nim`，当前仅完成 MsQuic 运行时管理与生命周期托管。

when not defined(libp2p_msquic_experimental):
  {.error: "MsQuic transport requires -d:libp2p_msquic_experimental".}

import std/[base64, options, sequtils, sets, strutils, times, tables]
import results
import chronos, chronicles

import ../crypto/crypto
import ../multiaddress, ../multicodec, ../peerid
import ../multibase
import ../multihash
import ../upgrademngrs/upgrade
import ../stream/connection
from ../stream/lpstream import LPStreamError
import ./transport as basetransport
import ./msquicdriver as msquicdrv
import ./msquicconnection
import ./msquicstream
import ./webtransport_common
import tls/certificate
import "nim-msquic/tls/common" as mstls
import "nim-msquic/api/event_model" as msevents

export msquicconnection.MsQuicConnection

export msquicdrv.MsQuicTransportConfig

const
  MsQuicDialEventTimeout = chronos.milliseconds(250)
  MsQuicDialMaxEvents = 12
  DefaultWebtransportPath* = "/.well-known/libp2p-webtransport"
  DefaultWebtransportQuery* = "?type=noise"
  DefaultWebtransportDraft* = "draft02"
  DefaultWebtransportMaxSessions* = 128'u32
  DefaultWebtransportCerthashHistory* = 2
  webtransportCodec = multiCodec("webtransport")
  certhashCodec = multiCodec("certhash")
  p2pCodec = multiCodec("p2p")
  p2pCircuitCodec = multiCodec("p2p-circuit")
  MsQuicAddressFamilyUnspecified = 0'u16
  MsQuicAddressFamilyIpv4 = 2'u16
  MsQuicAddressFamilyIpv6 = 23'u16

proc defaultCertGenerator(
    kp: KeyPair
): CertificateX509 {.gcsafe, raises: [TLSCertificateError].} =
  generateX509(kp, encodingFormat = EncodingFormat.PEM)

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc detectAddressFamily(host: string): uint16 =
  if host.len == 0:
    return MsQuicAddressFamilyUnspecified
  if host.contains(':'):
    return MsQuicAddressFamilyIpv6
  let parts = host.split('.')
  if parts.len != 4:
    return MsQuicAddressFamilyUnspecified
  for part in parts:
    if part.len == 0 or part.len > 3:
      return MsQuicAddressFamilyUnspecified
    for ch in part:
      if ch < '0' or ch > '9':
        return MsQuicAddressFamilyUnspecified
    try:
      let value = parseInt(part)
      if value < 0 or value > 255:
        return MsQuicAddressFamilyUnspecified
    except CatchableError:
      return MsQuicAddressFamilyUnspecified
  MsQuicAddressFamilyIpv4

template msquicSafe(body: untyped) =
  {.cast(gcsafe).}:
    body

logScope:
  topics = "libp2p msquictransport"

type
  CertGenerator* = proc(kp: KeyPair): CertificateX509 {.gcsafe, raises: [TLSCertificateError].}

  WebtransportRejectionReason* = enum
    wrrSessionLimit,
    wrrMissingConnectProtocol,
    wrrMissingDatagram,
    wrrMissingSessionAccept,
    wrrConnectRejected,
    wrrInvalidRequest

  WebtransportRejectionStats* = object
    sessionLimit*: uint64
    missingConnectProtocol*: uint64
    missingDatagram*: uint64
    missingSessionAccept*: uint64
    connectRejected*: uint64
    invalidRequest*: uint64

  WebtransportSessionSnapshot* = object
    peerId*: PeerId
    sessionId*: uint64
    ready*: bool
    authority*: string
    path*: string
    draft*: string
    observedAddr*: Opt[MultiAddress]
    localAddr*: Opt[MultiAddress]
    handshakeStart*: Option[Moment]
    handshakeReady*: Option[Moment]

  MsQuicListenerInfo = object
    handle: pointer
    state: msquicdrv.MsQuicListenerState
    webtransport: bool
    baseAddr: Option[MultiAddress]

  MsQuicTransport* = ref object of basetransport.Transport
    cfg*: msquicdrv.MsQuicTransportConfig
    handle: msquicdrv.MsQuicTransportHandle
    listeners: seq[MsQuicListenerInfo]
    listenerFuts: seq[Future[msevents.ListenerEvent]]
    privateKey: PrivateKey
    certGenerator: CertGenerator
    certificate: CertificateX509
    certificateInitialized: bool
    certificateDer: seq[byte]
    webtransportPath: string
    webtransportQuery: string
    webtransportDraft: string
    webtransportMaxSessions: uint32
    webtransportMaxCerthashHistory: int
    webtransportCerthashHistory: seq[string]
    webtransportCerthash: string
    webtransportRejection*: WebtransportRejectionStats
    webtransportSessions: seq[WebtransportSessionSnapshot]
    webtransportSessionLock: AsyncLock
    webtransportActiveSessions: uint32
    pendingCerthashHistory: seq[string]
    tlsOverride: Option[mstls.TlsConfig]
    tlsTempDir: string
    connections: seq[MsQuicConnection]
  MsQuicConnectionSnapshot* = object
    peerId*: string
    protocol*: string
    datagramSendEnabled*: bool
    datagramMaxSend*: uint16
    isWebtransport*: bool
    webtransportReady*: bool
    webtransportAuthority*: string
    webtransportPath*: string
    webtransportSessionId*: uint64
    handshakeStartMs*: Option[int64]
    handshakeReadyMs*: Option[int64]
    observedAddr*: Option[string]
    localAddr*: Option[string]
  MsQuicTransportStats* = object
    listenerCount*: int
    connectionCount*: int
    datagramEnabled*: int
    datagramDisabled*: int
    webtransportReady*: int
    webtransportPending*: int
    webtransportActiveSlots*: uint32
    webtransportSlotLimit*: uint32
    currentCerthash*: string
    certhashHistory*: seq[string]
    rejection*: WebtransportRejectionStats
    readyHandshakeAverage*: float
    pendingHandshakeAverage*: float
    connections*: seq[MsQuicConnectionSnapshot]

  QuicTransport* = MsQuicTransport
  QuicTransportError* = object of basetransport.TransportError
  QuicTransportDialError* = object of basetransport.TransportDialError
  QuicTransportAcceptStopped* = object of QuicTransportError

proc initWebtransportDefaults(transport: MsQuicTransport) =
  transport.webtransportPath = DefaultWebtransportPath
  transport.webtransportQuery = DefaultWebtransportQuery
  transport.webtransportDraft = DefaultWebtransportDraft
  transport.webtransportMaxSessions = DefaultWebtransportMaxSessions
  transport.webtransportMaxCerthashHistory = DefaultWebtransportCerthashHistory
  transport.webtransportCerthashHistory = @[]
  transport.webtransportCerthash = ""
  transport.webtransportRejection = WebtransportRejectionStats()
  transport.webtransportSessions = @[]
  transport.webtransportSessionLock = newAsyncLock()
  transport.webtransportActiveSessions = 0'u32
  transport.pendingCerthashHistory = @[]
  transport.connections = @[]

proc webtransportRejectionReasonLabel*(
    reason: WebtransportRejectionReason
): string =
  case reason
  of wrrSessionLimit: "session_limit"
  of wrrMissingConnectProtocol: "missing_connect_protocol"
  of wrrMissingDatagram: "missing_datagram"
  of wrrMissingSessionAccept: "missing_session_accept"
  of wrrConnectRejected: "connect_status_rejected"
  of wrrInvalidRequest: "invalid_request"

proc enforceWebtransportHistoryLimit(transport: MsQuicTransport) {.gcsafe, raises: [].}

proc setWebtransportCerthashHistoryLimit*(
    transport: MsQuicTransport, limit: int
) =
  let sanitized = if limit < 1: 1 else: limit
  transport.webtransportMaxCerthashHistory = sanitized
  transport.enforceWebtransportHistoryLimit()

proc getWebtransportHistoryLimit(transport: MsQuicTransport): int =
  if transport.webtransportMaxCerthashHistory < 1:
    DefaultWebtransportCerthashHistory
  else:
    transport.webtransportMaxCerthashHistory

proc syncWebtransportCerthash(transport: MsQuicTransport) =
  if transport.webtransportCerthashHistory.len > 0:
    transport.webtransportCerthash = transport.webtransportCerthashHistory[0]
  else:
    transport.webtransportCerthash = ""

proc sanitizeCerthashHistory(
    history: openArray[string]
): seq[string] {.gcsafe, raises: [].} =
  var seen = initHashSet[string]()
  for raw in history:
    let trimmed = raw.strip()
    if trimmed.len == 0:
      continue
    if seen.contains(trimmed):
      continue
    seen.incl(trimmed)
    result.add(trimmed)

proc applyPendingCerthashHistory(transport: MsQuicTransport) {.gcsafe, raises: [].} =
  if transport.pendingCerthashHistory.len == 0:
    transport.syncWebtransportCerthash()
    return
  if transport.webtransportCerthashHistory.len == 0:
    return
  var combined: seq[string] = @[]
  let current = transport.webtransportCerthashHistory[0]
  if current.len > 0:
    combined.add(current)
  for hash in transport.pendingCerthashHistory:
    if hash.len == 0:
      continue
    if combined.anyIt(it == hash):
      continue
    combined.add(hash)
  transport.webtransportCerthashHistory = combined
  transport.enforceWebtransportHistoryLimit()
  transport.pendingCerthashHistory.setLen(0)

proc loadWebtransportCerthashHistory*(
    transport: MsQuicTransport, history: openArray[string]
) =
  transport.pendingCerthashHistory = sanitizeCerthashHistory(history)
  if transport.certificateInitialized:
    transport.applyPendingCerthashHistory()
  else:
    transport.syncWebtransportCerthash()

proc enforceWebtransportHistoryLimit(transport: MsQuicTransport) {.gcsafe, raises: [].} =
  let limit = transport.getWebtransportHistoryLimit()
  if transport.webtransportCerthashHistory.len > limit:
    transport.webtransportCerthashHistory.setLen(limit)
  transport.syncWebtransportCerthash()

proc addWebtransportCerthash(
    transport: MsQuicTransport, hash: string
) {.gcsafe, raises: [].} =
  if hash.len == 0:
    return
  if transport.webtransportCerthashHistory.len == 0 or
      transport.webtransportCerthashHistory[0] != hash:
    transport.webtransportCerthashHistory.insert(hash, 0)
    var idx = 1
    while idx < transport.webtransportCerthashHistory.len:
      if transport.webtransportCerthashHistory[idx] == hash:
        transport.webtransportCerthashHistory.delete(idx)
      else:
        inc idx
  transport.enforceWebtransportHistoryLimit()

proc setWebtransportMaxSessions*(
    transport: MsQuicTransport, value: uint32
) =
  transport.webtransportMaxSessions =
    if value == 0'u32: 1'u32 else: value

proc currentWebtransportMaxSessions*(
    transport: MsQuicTransport
): uint32 =
  transport.webtransportMaxSessions

proc webtransportPath*(transport: MsQuicTransport): string =
  transport.webtransportPath

proc webtransportQuery*(transport: MsQuicTransport): string =
  transport.webtransportQuery

proc webtransportDraft*(transport: MsQuicTransport): string =
  transport.webtransportDraft

proc computeWebtransportRequestTarget(
    transport: MsQuicTransport
): string =
  let basePath =
    if transport.webtransportPath.len == 0:
      DefaultWebtransportPath
    else:
      transport.webtransportPath
  if transport.webtransportQuery.len == 0:
    basePath
  else:
    basePath & transport.webtransportQuery

proc webtransportRequestTarget*(transport: MsQuicTransport): string =
  computeWebtransportRequestTarget(transport)

proc setWebtransportPath*(
    transport: MsQuicTransport, path: string
) =
  var cleaned = path.strip()
  let question = cleaned.find('?')
  var extractedQuery = ""
  if question >= 0:
    extractedQuery = cleaned[(question + 1) ..< cleaned.len]
    cleaned = cleaned[0 ..< question]
  if cleaned.len == 0:
    transport.webtransportPath = DefaultWebtransportPath
  else:
    if not cleaned.startsWith("/"):
      cleaned = "/" & cleaned
    transport.webtransportPath = cleaned
  if question >= 0:
    if extractedQuery.len == 0:
      transport.webtransportQuery = ""
    else:
      transport.webtransportQuery = "?" & extractedQuery

proc setWebtransportQuery*(
    transport: MsQuicTransport, query: string
) =
  var trimmed = query.strip()
  if trimmed.len == 0 or trimmed == "?":
    transport.webtransportQuery = ""
  else:
    if not trimmed.startsWith("?"):
      trimmed = "?" & trimmed
    transport.webtransportQuery = trimmed

proc setWebtransportDraft*(
    transport: MsQuicTransport, draft: string
) =
  let trimmed = draft.strip()
  if trimmed.len == 0:
    transport.webtransportDraft = DefaultWebtransportDraft
  else:
    transport.webtransportDraft = trimmed

proc setCertificateGenerator*(
    transport: MsQuicTransport, generator: CertGenerator
) =
  if generator.isNil:
    transport.certGenerator = defaultCertGenerator
  else:
    transport.certGenerator = generator
  transport.certificateInitialized = false
  transport.certificate = CertificateX509()
  transport.certificateDer = @[]
  transport.tlsOverride = none(mstls.TlsConfig)
  transport.webtransportCerthashHistory.setLen(0)
  transport.pendingCerthashHistory.setLen(0)
  transport.syncWebtransportCerthash()

proc ensureCertificate(
    transport: MsQuicTransport
): CertificateX509 {.raises: [QuicTransportError], gcsafe.}

proc updateWebtransportCerthashFromDer(
    transport: MsQuicTransport, der: seq[byte]
)

proc updateWebtransportCerthashFromPem(
    transport: MsQuicTransport, pem: string
)

proc currentWebtransportCerthash*(
    transport: MsQuicTransport
): string =
  if transport.tlsOverride.isNone:
    discard transport.ensureCertificate()
  transport.webtransportCerthash

proc currentWebtransportCerthashHistory*(
    transport: MsQuicTransport
): seq[string] =
  if transport.tlsOverride.isNone:
    discard transport.ensureCertificate()
  for hash in transport.webtransportCerthashHistory:
    if hash.len == 0:
      continue
    result.add(hash)

proc configureTls*(
    transport: MsQuicTransport,
    mutator: proc(cfg: var mstls.TlsConfig) {.gcsafe.}
) =
  var cfg =
    if transport.tlsOverride.isSome:
      transport.tlsOverride.get()
    else:
      var base = default(mstls.TlsConfig)
      base.role = mstls.tlsServer
      base.useSharedSessionCache = true
      base.enableZeroRtt = false
      base
  mutator(cfg)
  transport.tlsOverride = some(cfg)

proc clearTlsOverrides*(transport: MsQuicTransport) =
  transport.tlsOverride = none(mstls.TlsConfig)

proc setTlsTempDir*(transport: MsQuicTransport, path: string) =
  transport.tlsTempDir = path

proc tlsTempDir*(transport: MsQuicTransport): string =
  transport.tlsTempDir

proc setWebtransportCerthashOverride*(
    transport: MsQuicTransport, certhash: string
) =
  let trimmed = certhash.strip()
  transport.webtransportCerthashHistory.setLen(0)
  if trimmed.len > 0:
    transport.webtransportCerthashHistory.add(trimmed)
  transport.enforceWebtransportHistoryLimit()

proc setTlsCertificatePem*(
    transport: MsQuicTransport,
    certificatePem: string,
    privateKeyPem: string
) =
  transport.configureTls(proc(cfg: var mstls.TlsConfig) {.gcsafe.} =
    cfg.certificatePem = some(certificatePem)
    cfg.privateKeyPem = some(privateKeyPem)
    cfg.certificateFile = none(string)
    cfg.privateKeyFile = none(string)
    cfg.privateKeyPassword = none(string)
    cfg.pkcs12File = none(string)
    cfg.pkcs12Data = none(seq[uint8])
    cfg.pkcs12Password = none(string)
    cfg.certificateHash = none(TlsCertificateHash)
    cfg.certificateStore = none(string)
    cfg.certificateStoreFlags = 0'u32
    cfg.certificateContext = none(pointer)
  )
  transport.updateWebtransportCerthashFromPem(certificatePem)

proc setTlsCertificateFiles*(
    transport: MsQuicTransport,
    certificateFile: string,
    privateKeyFile: string,
    password: string = "",
    webtransportCerthash: string = ""
) =
  transport.configureTls(proc(cfg: var mstls.TlsConfig) {.gcsafe.} =
    cfg.certificateFile = some(certificateFile)
    cfg.privateKeyFile = some(privateKeyFile)
    if password.len > 0:
      cfg.privateKeyPassword = some(password)
    else:
      cfg.privateKeyPassword = none(string)
    cfg.certificatePem = none(string)
    cfg.privateKeyPem = none(string)
    cfg.pkcs12File = none(string)
    cfg.pkcs12Data = none(seq[uint8])
    cfg.pkcs12Password = none(string)
    cfg.certificateHash = none(TlsCertificateHash)
    cfg.certificateStore = none(string)
    cfg.certificateStoreFlags = 0'u32
    cfg.certificateContext = none(pointer)
  )
  if webtransportCerthash.len > 0:
    transport.setWebtransportCerthashOverride(webtransportCerthash)
  else:
    try:
      let pem = readFile(certificateFile)
      transport.updateWebtransportCerthashFromPem(pem)
    except CatchableError as exc:
      trace "MsQuic certhash read failed", path = certificateFile, error = exc.msg

proc setTlsCertificateHash*(
    transport: MsQuicTransport,
    hash: openArray[byte],
    storeName: string = "",
    storeFlags: uint32 = 0'u32,
    webtransportCerthash: string = ""
) =
  if hash.len != TlsCertificateHashLength:
    raise newException(
      ValueError, "TLS certificate hash must contain 20 bytes (SHA-1 digest)"
    )
  var buf: TlsCertificateHash
  for i in 0 ..< TlsCertificateHashLength:
    buf[i] = hash[i]
  transport.configureTls(proc(cfg: var mstls.TlsConfig) {.gcsafe.} =
    cfg.certificateHash = some(buf)
    if storeName.len > 0:
      cfg.certificateStore = some(storeName)
    else:
      cfg.certificateStore = none(string)
    cfg.certificateStoreFlags = storeFlags
    cfg.certificatePem = none(string)
    cfg.privateKeyPem = none(string)
    cfg.certificateFile = none(string)
    cfg.privateKeyFile = none(string)
    cfg.privateKeyPassword = none(string)
    cfg.pkcs12File = none(string)
    cfg.pkcs12Data = none(seq[uint8])
    cfg.pkcs12Password = none(string)
    cfg.certificateContext = none(pointer)
  )
  if webtransportCerthash.len > 0:
    transport.setWebtransportCerthashOverride(webtransportCerthash)

proc setTlsPkcs12File*(
    transport: MsQuicTransport,
    path: string,
    password: string = "",
    webtransportCerthash: string = ""
) =
  transport.configureTls(proc(cfg: var mstls.TlsConfig) {.gcsafe.} =
    cfg.pkcs12File = some(path)
    cfg.pkcs12Data = none(seq[uint8])
    if password.len > 0:
      cfg.pkcs12Password = some(password)
    else:
      cfg.pkcs12Password = none(string)
    cfg.certificatePem = none(string)
    cfg.privateKeyPem = none(string)
    cfg.certificateFile = none(string)
    cfg.privateKeyFile = none(string)
    cfg.privateKeyPassword = none(string)
    cfg.certificateHash = none(TlsCertificateHash)
    cfg.certificateStore = none(string)
    cfg.certificateStoreFlags = 0'u32
    cfg.certificateContext = none(pointer)
  )
  if webtransportCerthash.len > 0:
    transport.setWebtransportCerthashOverride(webtransportCerthash)

proc setTlsPkcs12Data*(
    transport: MsQuicTransport,
    data: openArray[byte],
    password: string = "",
    webtransportCerthash: string = ""
) =
  var bytes = newSeq[uint8](data.len)
  if bytes.len > 0:
    for i in 0 ..< data.len:
      bytes[i] = data[i]
  transport.configureTls(proc(cfg: var mstls.TlsConfig) {.gcsafe.} =
    cfg.pkcs12Data = some(bytes)
    cfg.pkcs12File = none(string)
    if password.len > 0:
      cfg.pkcs12Password = some(password)
    else:
      cfg.pkcs12Password = none(string)
    cfg.certificatePem = none(string)
    cfg.privateKeyPem = none(string)
    cfg.certificateFile = none(string)
    cfg.privateKeyFile = none(string)
    cfg.privateKeyPassword = none(string)
    cfg.certificateHash = none(TlsCertificateHash)
    cfg.certificateStore = none(string)
    cfg.certificateStoreFlags = 0'u32
    cfg.certificateContext = none(pointer)
  )
  if webtransportCerthash.len > 0:
    transport.setWebtransportCerthashOverride(webtransportCerthash)

proc setTlsCertificateContext*(
    transport: MsQuicTransport, context: pointer
) =
  transport.configureTls(proc(cfg: var mstls.TlsConfig) {.gcsafe.} =
    cfg.certificateContext = some(context)
    cfg.certificatePem = none(string)
    cfg.privateKeyPem = none(string)
    cfg.certificateFile = none(string)
    cfg.privateKeyFile = none(string)
    cfg.privateKeyPassword = none(string)
    cfg.pkcs12File = none(string)
    cfg.pkcs12Data = none(seq[uint8])
    cfg.pkcs12Password = none(string)
    cfg.certificateHash = none(TlsCertificateHash)
    cfg.certificateStore = none(string)
    cfg.certificateStoreFlags = 0'u32
  )

proc setTlsFlags*(
    transport: MsQuicTransport,
    disableCertificateValidation: Option[bool] = none(bool),
    requireClientAuth: Option[bool] = none(bool),
    enableOcsp: Option[bool] = none(bool),
    indicateCertificateReceived: Option[bool] = none(bool),
    deferCertificateValidation: Option[bool] = none(bool),
    useBuiltinValidation: Option[bool] = none(bool),
    allowedCipherSuites: Option[uint32] = none(uint32),
    caCertificateFile: Option[string] = none(string)
) =
  transport.configureTls(proc(cfg: var mstls.TlsConfig) {.gcsafe.} =
    disableCertificateValidation.withValue(val):
      cfg.disableCertificateValidation = val
    requireClientAuth.withValue(val):
      cfg.requireClientAuth = val
    enableOcsp.withValue(val):
      cfg.enableOcsp = val
    indicateCertificateReceived.withValue(val):
      cfg.indicateCertificateReceived = val
    deferCertificateValidation.withValue(val):
      cfg.deferCertificateValidation = val
    useBuiltinValidation.withValue(val):
      cfg.useBuiltinCertificateValidation = val
    allowedCipherSuites.withValue(val):
      cfg.allowedCipherSuites = some(val)
    caCertificateFile.withValue(val):
      if val.len > 0:
        cfg.caCertificateFile = some(val)
      else:
        cfg.caCertificateFile = none(string)
  )

proc webtransportSessionSnapshots*(
    transport: MsQuicTransport
): seq[WebtransportSessionSnapshot] =
  for conn in transport.connections:
    if conn.isNil or not conn.isWebtransport:
      continue
    result.add(
      WebtransportSessionSnapshot(
        peerId: conn.peerId,
        sessionId: conn.webtransportSessionId,
        ready: conn.webtransportReady,
        authority: conn.webtransportAuthority,
        path: conn.webtransportPath,
        draft: conn.webtransportDraft,
        observedAddr: conn.observedAddr,
        localAddr: conn.localAddr,
        handshakeStart: conn.webtransportHandshakeStart,
        handshakeReady: conn.webtransportHandshakeReadyAt
      )
    )

proc webtransportRejectionStats*(
    transport: MsQuicTransport
): WebtransportRejectionStats =
  transport.webtransportRejection

proc count*(
    stats: WebtransportRejectionStats, reason: WebtransportRejectionReason
): uint64 =
  case reason
  of wrrSessionLimit: stats.sessionLimit
  of wrrMissingConnectProtocol: stats.missingConnectProtocol
  of wrrMissingDatagram: stats.missingDatagram
  of wrrMissingSessionAccept: stats.missingSessionAccept
  of wrrConnectRejected: stats.connectRejected
  of wrrInvalidRequest: stats.invalidRequest

proc rotateCertificate*(
    transport: MsQuicTransport, keepHistory: int
): Future[void] {.async: (raises: [basetransport.TransportError, QuicTransportError, CancelledError]).} =
  if transport.tlsOverride.isSome:
    raise (ref QuicTransportError)(
      msg: "cannot rotate MsQuic certificate when TLS overrides are active"
    )
  let sanitized = if keepHistory < 1: 1 else: keepHistory
  let wasRunning = transport.running
  let listenAddrs = transport.addrs
  var previousHashes: seq[string] = @[]
  for hash in transport.webtransportCerthashHistory:
    previousHashes.add(hash)
  let previousCertificate = transport.certificate
  let previousCertificateDer = transport.certificateDer
  let previousInitialized = transport.certificateInitialized
  let previousCerthash = transport.webtransportCerthash
  let previousHistoryLimit = transport.webtransportMaxCerthashHistory

  if wasRunning:
    await transport.stop()

  transport.webtransportMaxCerthashHistory = sanitized
  transport.certificateInitialized = false
  transport.certificate = CertificateX509()
  transport.certificateDer = @[]
  transport.webtransportCerthashHistory.setLen(0)
  transport.syncWebtransportCerthash()

  try:
    discard transport.ensureCertificate()
    if previousHashes.len > 0:
      for idx in countdown(previousHashes.high, 0):
        transport.addWebtransportCerthash(previousHashes[idx])
    transport.enforceWebtransportHistoryLimit()
    if wasRunning:
      await transport.start(listenAddrs)
  except QuicTransportError as exc:
    transport.certificateInitialized = previousInitialized
    transport.certificate = previousCertificate
    transport.certificateDer = previousCertificateDer
    transport.webtransportCerthashHistory = previousHashes
    transport.webtransportCerthash = previousCerthash
    transport.webtransportMaxCerthashHistory = previousHistoryLimit
    if wasRunning:
      try:
        await transport.start(listenAddrs)
      except CatchableError as startExc:
        raise (ref QuicTransportError)(
          msg: "failed to restore MsQuic transport after rotation failure: " & startExc.msg,
          parent: startExc
        )
    raise exc
  except CatchableError as exc:
    transport.certificateInitialized = previousInitialized
    transport.certificate = previousCertificate
    transport.certificateDer = previousCertificateDer
    transport.webtransportCerthashHistory = previousHashes
    transport.webtransportCerthash = previousCerthash
    transport.webtransportMaxCerthashHistory = previousHistoryLimit
    if wasRunning:
      try:
        await transport.start(listenAddrs)
      except CatchableError as startExc:
        raise (ref QuicTransportError)(
          msg: "failed to restore MsQuic transport after rotation failure: " & startExc.msg,
          parent: startExc
        )
    raise (ref QuicTransportError)(
      msg: "failed to rotate MsQuic certificate: " & exc.msg,
      parent: exc
    )

proc cleanupMsQuicDial(
    transport: MsQuicTransport,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState
) {.raises: [].} =
  if transport.isNil or connPtr.isNil:
    return
  try:
    msquicSafe:
      discard msquicdrv.shutdownConnection(transport.handle, connPtr)
      msquicdrv.closeConnection(transport.handle, connPtr, connState)
  except Exception as exc:
    trace "MsQuic dial cleanup raised", err = exc.msg

proc resetListenerFutures(self: MsQuicTransport) {.raises: [].} =
  self.listenerFuts.setLen(self.listeners.len)
  for idx in 0 ..< self.listeners.len:
    let state = self.listeners[idx].state
    if state.isNil:
      self.listenerFuts[idx] = Future[msevents.ListenerEvent].init("msquic.listener.closed")
      self.listenerFuts[idx].fail(
        newException(msquicdrv.MsQuicEventQueueClosed, "listener closed")
      )
    else:
      self.listenerFuts[idx] = state.nextListenerEvent()

proc closeAllListeners(self: MsQuicTransport) {.raises: [].} =
  for listener in self.listeners:
    if listener.handle.isNil:
      continue
    try:
      msquicSafe:
        discard msquicdrv.stopListener(self.handle, listener.handle)
    except Exception as exc:
      trace "MsQuic stopListener raised", err = exc.msg
    try:
      msquicSafe:
        msquicdrv.closeListener(self.handle, listener.handle, listener.state)
    except Exception as exc:
      trace "MsQuic closeListener raised", err = exc.msg
  self.listeners.setLen(0)
  self.listenerFuts.setLen(0)

proc extractHostPort(address: MultiAddress): (string, string) =
  let text = address.toString().valueOr:
    return ("", "")
  if text.len == 0:
    return ("", "")
  var host = ""
  var port = ""
  var idx = 0
  let parts = text.split('/')
  while idx < parts.len:
    let part = parts[idx]
    case part
    of "ip4", "ip6", "dns", "dns4", "dns6", "dnsaddr":
      if idx + 1 < parts.len:
        host = parts[idx + 1]
        inc idx
    of "udp", "tcp":
      if idx + 1 < parts.len:
        port = parts[idx + 1]
        inc idx
    else:
      discard
    inc idx
  (host, port)

proc makeClientHandshakeInfo(
    transport: MsQuicTransport,
    baseAddr: MultiAddress,
    hostname: string
): WebtransportHandshakeInfo =
  let (hostValue, portValue) = extractHostPort(baseAddr)
  let selectedHost = if hostname.len > 0: hostname else: hostValue
  var authorityHost = selectedHost
  if authorityHost.contains(':') and not authorityHost.startsWith("["):
    authorityHost = "[" & authorityHost & "]"
  let authority = normaliseAuthority(authorityHost, portValue)
  WebtransportHandshakeInfo(
    mode: wtmClient,
    authority: authority,
    path: transport.webtransportPath,
    origin: normaliseOrigin(authority),
    draft: transport.webtransportDraft,
    maxSessions: transport.webtransportMaxSessions
  )

proc makeServerHandshakeInfo(transport: MsQuicTransport): WebtransportHandshakeInfo =
  WebtransportHandshakeInfo(
    mode: wtmServer,
    authority: "",
    path: transport.webtransportPath,
    origin: "",
    draft: transport.webtransportDraft,
    maxSessions: transport.webtransportMaxSessions
  )

proc defaultRemoteSettings(transport: MsQuicTransport): Http3Settings =
  Http3Settings(
    enableConnectProtocol: true,
    enableDatagram: true,
    maxSessions: transport.webtransportMaxSessions
  )

proc isPemCertificate(data: seq[byte]): bool =
  const header = "-----BEGIN CERTIFICATE-----"
  if data.len < header.len:
    return false
  for idx, ch in header:
    if data[idx] != byte(ch):
      return false
  true

proc certificateToDer(cert: CertificateX509): Result[seq[byte], string] =
  if cert.certificate.len == 0:
    return err("empty certificate payload")
  if isPemCertificate(cert.certificate):
    let text = bytesToString(cert.certificate)
    var body = newStringOfCap(text.len)
    for line in text.splitLines():
      let trimmed = line.strip()
      if trimmed.len == 0 or trimmed.startsWith("-----"):
        continue
      body.add(trimmed)
    try:
      let decoded = base64.decode(body)
      var bytes = newSeq[byte](decoded.len)
      for i, ch in decoded:
        bytes[i] = byte(ch)
      ok(bytes)
    except CatchableError as exc:
      err("invalid PEM payload: " & exc.msg)
  else:
    ok(cert.certificate)

proc computeWebtransportCerthash(data: seq[byte]): Result[string, string] =
  if data.len == 0:
    return err("empty certificate data")
  let mh = MultiHash.digest("sha2-256", data).valueOr:
    return err($error)
  let encoded = MultiBase.encode("base64url", mh.data.buffer).valueOr:
    return err(error)
  ok(encoded)

proc pemTextToDer(pem: string): Result[seq[byte], string] =
  if pem.len == 0:
    return err("empty certificate payload")
  var body = newStringOfCap(pem.len)
  for line in pem.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0 or trimmed.startsWith("-----"):
      continue
    body.add(trimmed)
  try:
    let decoded = base64.decode(body)
    var bytes = newSeq[byte](decoded.len)
    for i, ch in decoded:
      bytes[i] = byte(ch)
    ok(bytes)
  except CatchableError as exc:
    err("invalid PEM payload: " & exc.msg)

proc updateWebtransportCerthashFromDer(
    transport: MsQuicTransport, der: seq[byte]
) =
  let hashRes = computeWebtransportCerthash(der)
  if hashRes.isOk:
    transport.webtransportCerthashHistory.setLen(0)
    transport.addWebtransportCerthash(hashRes.get())
    transport.enforceWebtransportHistoryLimit()
  else:
    trace "MsQuic certhash computation failed", error = hashRes.error

proc updateWebtransportCerthashFromPem(
    transport: MsQuicTransport, pem: string
) =
  let derRes = pemTextToDer(pem)
  if derRes.isOk:
    transport.updateWebtransportCerthashFromDer(derRes.get())
  else:
    trace "MsQuic certhash computation failed", error = derRes.error

proc momentToMillis(moment: Moment): int64 {.inline.} =
  moment.epochNanoSeconds() div 1_000_000

proc connectionToSnapshot(
    conn: MsQuicConnection
): MsQuicConnectionSnapshot =
  var observed = none(string)
  if conn.observedAddr.isSome:
    let observedMa = conn.observedAddr.get()
    observed = some($observedMa)
  var local = none(string)
  if conn.localAddr.isSome:
    let localMa = conn.localAddr.get()
    local = some($localMa)
  var handshakeStartMs = none(int64)
  conn.webtransportHandshakeStart.withValue(startMoment):
    handshakeStartMs = some(momentToMillis(startMoment))
  var handshakeReadyMs = none(int64)
  conn.webtransportHandshakeReadyAt.withValue(readyMoment):
    handshakeReadyMs = some(momentToMillis(readyMoment))
  MsQuicConnectionSnapshot(
    peerId: $conn.peerId,
    protocol: conn.protocol,
    datagramSendEnabled: conn.datagramSendEnabled,
    datagramMaxSend: conn.datagramMaxSend,
    isWebtransport: conn.isWebtransport,
    webtransportReady: conn.webtransportReady,
    webtransportAuthority: conn.webtransportAuthority,
    webtransportPath: conn.webtransportPath,
    webtransportSessionId: conn.webtransportSessionId,
    handshakeStartMs: handshakeStartMs,
    handshakeReadyMs: handshakeReadyMs,
    observedAddr: observed,
    localAddr: local
  )

proc connectionSnapshots*(
    transport: MsQuicTransport
): seq[MsQuicConnectionSnapshot] {.gcsafe.} =
  for conn in transport.connections:
    if conn.isNil:
      continue
    try:
      result.add(connectionToSnapshot(conn))
    except CatchableError as exc:
      trace "snapshotting MsQuic connection failed", error = exc.msg

proc collectMsQuicTransportStats*(
    transport: MsQuicTransport
): MsQuicTransportStats {.gcsafe.} =
  var stats = MsQuicTransportStats()
  stats.listenerCount = transport.listeners.len
  try:
    stats.currentCerthash = transport.currentWebtransportCerthash()
  except QuicTransportError as exc:
    trace "failed to fetch current certhash", error = exc.msg
  try:
    stats.certhashHistory = transport.currentWebtransportCerthashHistory()
  except QuicTransportError as exc:
    trace "failed to fetch certhash history", error = exc.msg
  stats.webtransportActiveSlots = transport.webtransportActiveSessions
  stats.webtransportSlotLimit = transport.currentWebtransportMaxSessions()
  stats.rejection = transport.webtransportRejection
  let snapshots = transport.connectionSnapshots()
  stats.connectionCount = snapshots.len
  var readyDurationTotal = 0.0
  var readyDurationCount = 0
  var pendingDurationTotal = 0.0
  var pendingDurationCount = 0
  let nowSeconds =
    float(Moment.now().epochNanoSeconds()) / 1_000_000_000.0
  for snap in snapshots:
    if snap.datagramSendEnabled:
      inc stats.datagramEnabled
    else:
      inc stats.datagramDisabled
    if snap.isWebtransport:
      if snap.webtransportReady:
        inc stats.webtransportReady
        if snap.handshakeStartMs.isSome and snap.handshakeReadyMs.isSome:
          let deltaMs = snap.handshakeReadyMs.get - snap.handshakeStartMs.get
          if deltaMs >= 0:
            let duration = float(deltaMs) / 1_000.0
            readyDurationTotal += duration
            inc readyDurationCount
      else:
        inc stats.webtransportPending
        if snap.handshakeStartMs.isSome:
          let duration =
            nowSeconds - (float(snap.handshakeStartMs.get) / 1000.0)
          if duration >= 0.0:
            pendingDurationTotal += duration
            inc pendingDurationCount
  if readyDurationCount > 0:
    stats.readyHandshakeAverage = readyDurationTotal / float(readyDurationCount)
  else:
    stats.readyHandshakeAverage = 0.0
  if pendingDurationCount > 0:
    stats.pendingHandshakeAverage = pendingDurationTotal / float(pendingDurationCount)
  else:
    stats.pendingHandshakeAverage = 0.0
  stats.connections = snapshots
  result = stats

proc ensureCertificate(
    transport: MsQuicTransport
): CertificateX509 {.raises: [QuicTransportError], gcsafe.} =
  if not transport.certificateInitialized:
    let pubkey = transport.privateKey.getPublicKey().valueOr:
      raise (ref QuicTransportError)(
        msg: "failed to obtain public key for certificate: " & $error
      )
    let cert =
      try:
        transport.certGenerator(KeyPair(seckey: transport.privateKey, pubkey: pubkey))
      except TLSCertificateError as exc:
        raise (ref QuicTransportError)(
          msg: "failed to generate TLS certificate: " & exc.msg, parent: exc
        )
    transport.certificate = cert
    transport.certificateInitialized = true
    transport.certificateDer = certificateToDer(cert).valueOr:
      trace "MsQuic certificate normalization failed", error = error
      @[]
    if transport.certificateDer.len > 0:
      let hashRes = computeWebtransportCerthash(transport.certificateDer)
      if hashRes.isOk:
        transport.webtransportCerthashHistory.setLen(0)
        transport.addWebtransportCerthash(hashRes.get())
      else:
        transport.webtransportCerthashHistory.setLen(0)
        transport.syncWebtransportCerthash()
        trace "MsQuic certhash computation failed", error = hashRes.error
    else:
      transport.webtransportCerthashHistory.setLen(0)
      transport.syncWebtransportCerthash()
  transport.applyPendingCerthashHistory()
  transport.certificate

proc makeTlsConfig(transport: MsQuicTransport): mstls.TlsConfig =
  if transport.tlsOverride.isSome:
    var cfg = transport.tlsOverride.get()
    if cfg.alpns.len == 0:
      cfg.alpns = transport.cfg.alpns
    if cfg.tempDirectory.isNone and transport.tlsTempDir.len > 0:
      cfg.tempDirectory = some(transport.tlsTempDir)
    if cfg.transportParameters.len == 0:
      cfg.transportParameters = @[]
    cfg
  else:
    let cert = transport.ensureCertificate()
    var alpns = transport.cfg.alpns
    if alpns.len == 0:
      alpns = @["libp2p"]
    var cfg = mstls.TlsConfig(
      role: mstls.tlsServer,
      alpns: alpns,
      transportParameters: @[],
      serverName: none(string),
      certificatePem: some(bytesToString(cert.certificate)),
      privateKeyPem: some(bytesToString(cert.privateKey)),
      resumptionTicket: none(seq[uint8]),
      enableZeroRtt: false,
      useSharedSessionCache: true,
      disableCertificateValidation: true
    )
    if transport.tlsTempDir.len > 0:
      cfg.tempDirectory = some(transport.tlsTempDir)
    cfg

proc splitTransportAddress(
    ma: MultiAddress
): MaResult[(MultiAddress, bool, seq[seq[byte]])] =
  ## 拆分 QUIC 多地址：返回基础地址、是否标注 WebTransport，以及显式的 certhash 序列。
  var prefixParts: seq[MultiAddress]
  let total = ?len(ma)
  var idx = 0
  var hasWebtransport = false
  var certHashes: seq[seq[byte]] = @[]

  while idx < total:
    let part = ?ma[idx]
    let code = ?part.protoCode()

    if hasWebtransport:
      if code == certhashCodec:
        certHashes.add(?part.protoArgument())
        inc idx
        continue
      if code == p2pCodec or code == p2pCircuitCodec:
        break
      break

    if code == webtransportCodec:
      hasWebtransport = true
      inc idx
      continue
    if code == certhashCodec or code == p2pCodec or code == p2pCircuitCodec:
      break
    prefixParts.add(part)
    inc idx

  if prefixParts.len == 0:
    return err("multiaddress: missing transport prefix")

  var base = prefixParts[0]
  for i in 1 ..< prefixParts.len:
    base = concat(base, prefixParts[i]).valueOr:
      return err("multiaddress: " & error)

  ok((base, hasWebtransport, certHashes))

type
  MsQuicStreamCursor = ref object
    stream: MsQuicStream
    buffer: seq[byte]
    eof: bool

proc initCursor(stream: MsQuicStream): MsQuicStreamCursor =
  MsQuicStreamCursor(stream: stream, buffer: @[], eof: false)

proc ensureBytes(
    cursor: MsQuicStreamCursor, count: int
): Future[void] {.async: (raises: [CancelledError, LPStreamError, QuicTransportError]).} =
  if cursor.isNil or cursor.stream.isNil:
    raise (ref QuicTransportError)(msg: "stream cursor is nil")
  while cursor.buffer.len < count and not cursor.eof:
    let chunk = await cursor.stream.read()
    if chunk.len == 0:
      cursor.eof = true
    else:
      cursor.buffer.add(chunk)
  if cursor.buffer.len < count:
    raise (ref QuicTransportError)(
      msg: "unexpected EOF while reading HTTP/3 payload"
    )

proc readBytes(
    cursor: MsQuicStreamCursor, count: int
): Future[seq[byte]] {.async: (raises: [CancelledError, LPStreamError, QuicTransportError]).} =
  await cursor.ensureBytes(count)
  result = cursor.buffer[0 ..< count]
  cursor.buffer.delete(0, count - 1)

proc readByte(
    cursor: MsQuicStreamCursor
): Future[byte] {.async: (raises: [CancelledError, LPStreamError, QuicTransportError]).} =
  let bytes = await cursor.readBytes(1)
  bytes[0]

proc encodeQuicVarInt(value: uint64): seq[byte] =
  if value < 0x40'u64:
    result = @[byte(value)]
  elif value < 0x4000'u64:
    result = @[
      byte(0x40 or ((value shr 8) and 0x3f)),
      byte(value and 0xff),
    ]
  elif value < 0x40000000'u64:
    result = @[
      byte(0x80 or ((value shr 24) and 0x3f)),
      byte((value shr 16) and 0xff),
      byte((value shr 8) and 0xff),
      byte(value and 0xff),
    ]
  elif value < 0x4000000000000000'u64:
    result = @[
      byte(0xc0 or ((value shr 56) and 0x3f)),
      byte((value shr 48) and 0xff),
      byte((value shr 40) and 0xff),
      byte((value shr 32) and 0xff),
      byte((value shr 24) and 0xff),
      byte((value shr 16) and 0xff),
      byte((value shr 8) and 0xff),
      byte(value and 0xff),
    ]
  else:
    raiseAssert "value out of range for QUIC varint"

proc decodeQuicVarIntFrom(
    data: openArray[byte], pos: var int
): uint64 {.raises: [QuicTransportError].} =
  if pos >= data.len:
    raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
  let first = data[pos]
  inc pos
  let prefix = first shr 6
  case prefix
  of 0:
    uint64(first and 0x3f)
  of 1:
    if pos >= data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
    let b1 = data[pos]
    inc pos
    (uint64(first and 0x3f) shl 8) or uint64(b1)
  of 2:
    if pos + 3 > data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
    let b1 = data[pos]
    let b2 = data[pos + 1]
    let b3 = data[pos + 2]
    inc pos, 3
    (uint64(first and 0x3f) shl 24) or
      (uint64(b1) shl 16) or (uint64(b2) shl 8) or uint64(b3)
  else:
    if pos + 7 > data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
    let b1 = data[pos]
    let b2 = data[pos + 1]
    let b3 = data[pos + 2]
    let b4 = data[pos + 3]
    let b5 = data[pos + 4]
    let b6 = data[pos + 5]
    let b7 = data[pos + 6]
    inc pos, 7
    (uint64(first and 0x3f) shl 56) or
      (uint64(b1) shl 48) or
      (uint64(b2) shl 40) or
      (uint64(b3) shl 32) or
      (uint64(b4) shl 24) or
      (uint64(b5) shl 16) or
      (uint64(b6) shl 8) or
      uint64(b7)

proc readQuicVarInt(
    cursor: MsQuicStreamCursor
): Future[uint64] {.async: (raises: [CancelledError, LPStreamError, QuicTransportError]).} =
  await cursor.ensureBytes(1)
  let first = cursor.buffer[0]
  let prefix = first shr 6
  let required =
    case prefix
    of 0: 1
    of 1: 2
    of 2: 4
    else: 8
  await cursor.ensureBytes(required)
  var pos = 0
  let value = decodeQuicVarIntFrom(cursor.buffer[0 ..< required], pos)
  cursor.buffer.delete(0, required - 1)
  value

proc encodePrefixedInt(prefixBase: byte, prefixBits: int, value: uint64): seq[byte] =
  let mask = (1 shl prefixBits) - 1
  var first = prefixBase
  if value < mask.uint64:
    first = first or byte(value)
    result.add(first)
  else:
    first = first or byte(mask)
    result.add(first)
    var extra = value - mask.uint64
    while true:
      var b = byte(extra and 0x7f)
      extra = extra shr 7
      if extra != 0:
        b = b or 0x80
        result.add(b)
      else:
        result.add(b)
        break

proc decodePrefixedInt(
    firstValue: byte, prefixBits: int, data: seq[byte], pos: var int
): uint64 {.raises: [QuicTransportError].} =
  let mask = (1 shl prefixBits) - 1
  var value = uint64(firstValue and byte(mask))
  if value < mask.uint64:
    return value
  var multiplier = 0
  var extra = 0'u64
  while true:
    if pos >= data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding prefixed int")
    let b = data[pos]
    inc pos
    extra += uint64(b and 0x7f) shl multiplier
    if (b and 0x80) == 0:
      break
    multiplier += 7
  mask.uint64 + extra

proc encodeStringLiteralCustom(
    prefixBits: int, prefixBase: byte, value: string
): seq[byte] =
  let mask = (1 shl prefixBits) - 1
  let length = value.len.uint64
  var first = prefixBase
  if length < mask.uint64:
    first = first or byte(length)
    result.add(first)
  else:
    first = first or byte(mask)
    result.add(first)
    var extra = length - mask.uint64
    while true:
      var b = byte(extra and 0x7f)
      extra = extra shr 7
      if extra != 0:
        result.add(b or 0x80)
      else:
        result.add(b)
        break
  for ch in value:
    result.add(byte(ch))

proc encodeStringLiteral(prefixBits: int, value: string): seq[byte] =
  encodeStringLiteralCustom(prefixBits, 0, value)

proc decodeStringLiteral(
    data: seq[byte], pos: var int, prefixBits: int
): string {.raises: [QuicTransportError].} =
  if pos >= data.len:
    raise (ref QuicTransportError)(msg: "unexpected EOF decoding string literal")
  let first = data[pos]
  inc pos
  let huffman = ((first shr prefixBits) and 0x1) == 1
  if huffman:
    raise (ref QuicTransportError)(
      msg: "Huffman-encoded QPACK strings are not supported"
    )
  let mask = (1 shl prefixBits) - 1
  var length = uint64(first and mask.byte)
  if length == mask.uint64:
    var multiplier = 0
    while true:
      if pos >= data.len:
        raise (ref QuicTransportError)(
          msg: "unexpected EOF decoding string literal length"
        )
      let b = data[pos]
      inc pos
      length += uint64(b and 0x7f) shl multiplier
      if (b and 0x80) == 0:
        break
      multiplier += 7
  if pos + int(length) > data.len:
    raise (ref QuicTransportError)(
      msg: "unexpected EOF decoding string literal payload"
    )
  let endPos = pos + int(length)
  var s = newString(int(length))
  var idx = 0
  while pos < endPos:
    s[idx] = char(data[pos])
    inc idx
    inc pos
  s

proc qpackIndexedStatic(index: uint64): seq[byte] =
  encodePrefixedInt(0b11000000'u8, 6, index)

proc qpackLiteralWithNameRefStatic(index: uint64, value: string): seq[byte] =
  result = encodePrefixedInt(0b01010000'u8, 4, index)
  result.add(encodeStringLiteral(7, value))

proc qpackLiteralWithLiteralName(name, value: string): seq[byte] =
  let nameBytes = encodeStringLiteralCustom(3, 0b00100000'u8, name)
  let valueBytes = encodeStringLiteral(7, value)
  result = nameBytes
  result.add(valueBytes)

proc appendBytes(target: var seq[byte], data: seq[byte]) =
  for b in data:
    target.add(b)

proc encodeHeadersFrame(payload: seq[byte]): seq[byte] =
  result = encodeQuicVarInt(http3FrameTypeHeaders)
  result.appendBytes(encodeQuicVarInt(uint64(payload.len)))
  result.appendBytes(payload)

proc qpackStaticHeader(index: uint64): (string, string) {.raises: [QuicTransportError].} =
  case index
  of 0'u64:
    (":authority", "")
  of 1'u64:
    (":path", "/")
  of 15'u64:
    (":method", "CONNECT")
  of 17'u64:
    (":method", "GET")
  of 22'u64:
    (":scheme", "http")
  of 23'u64:
    (":scheme", "https")
  of 24'u64:
    (":status", "103")
  of 25'u64:
    (":status", "200")
  of 26'u64:
    (":status", "304")
  of 27'u64:
    (":status", "404")
  of 28'u64:
    (":status", "503")
  of 63'u64:
    (":status", "100")
  of 64'u64:
    (":status", "204")
  of 65'u64:
    (":status", "206")
  of 66'u64:
    (":status", "302")
  of 67'u64:
    (":status", "400")
  of 68'u64:
    (":status", "403")
  of 69'u64:
    (":status", "421")
  of 70'u64:
    (":status", "425")
  of 71'u64:
    (":status", "500")
  else:
    raise (ref QuicTransportError)(
      msg: "unsupported static table index: " & $index
    )

proc decodeHeadersBlock(
    payload: seq[byte]
): Table[string, string] {.raises: [QuicTransportError].} =
  var pos = 0
  if payload.len < 2:
    raise (ref QuicTransportError)(msg: "invalid QPACK field section")

  let requiredFirst = payload[pos]
  inc pos
  discard decodePrefixedInt(requiredFirst, 8, payload, pos)

  let baseFirst = payload[pos]
  inc pos
  discard decodePrefixedInt(baseFirst and 0x7f, 7, payload, pos)

  result = initTable[string, string]()
  while pos < payload.len:
    let first = payload[pos]
    inc pos
    if (first and 0x80) != 0:
      let isStatic = ((first shr 6) and 0x1) == 1
      let index = decodePrefixedInt(first and 0x3f, 6, payload, pos)
      if not isStatic:
        raise (ref QuicTransportError)(
          msg: "dynamic table references are not supported"
        )
      let entry = qpackStaticHeader(index)
      result[entry[0]] = entry[1]
    elif (first and 0xC0) == 0x40:
      let isStatic = ((first shr 4) and 0x1) == 1
      if not isStatic:
        raise (ref QuicTransportError)(
          msg: "dynamic table references are not supported"
        )
      let index = decodePrefixedInt(first and 0x0f, 4, payload, pos)
      let entry = qpackStaticHeader(index)
      let value = decodeStringLiteral(payload, pos, 7)
      result[entry[0]] = value
    elif (first and 0xE0) == 0x20:
      dec pos
      let name = decodeStringLiteral(payload, pos, 3)
      let value = decodeStringLiteral(payload, pos, 7)
      result[name] = value
    else:
      raise (ref QuicTransportError)(
        msg: "unsupported QPACK representation in headers block"
      )

proc makeClientHeadersBlock(info: WebtransportHandshakeInfo): seq[byte] =
  result.add(@[0x00'u8, 0x00'u8])
  result.appendBytes(qpackIndexedStatic(15)) # :method CONNECT
  result.appendBytes(qpackIndexedStatic(23)) # :scheme https
  result.appendBytes(qpackLiteralWithNameRefStatic(1, info.path))
  result.appendBytes(qpackLiteralWithNameRefStatic(0, info.authority))
  result.appendBytes(qpackLiteralWithLiteralName(":protocol", "webtransport"))
  if info.origin.len > 0:
    result.appendBytes(qpackLiteralWithLiteralName("origin", info.origin))
  result.appendBytes(
    qpackLiteralWithLiteralName("sec-webtransport-http3-draft", info.draft)
  )

proc makeServerHeadersBlock(draft: string): seq[byte] =
  result.add(@[0x00'u8, 0x00'u8])
  result.appendBytes(qpackIndexedStatic(25)) # :status 200
  if draft.len > 0:
    result.appendBytes(
      qpackLiteralWithLiteralName("sec-webtransport-http3-draft", draft)
    )

proc buildSettingsPayload(info: WebtransportHandshakeInfo): seq[byte] =
  result.appendBytes(encodeQuicVarInt(http3SettingsEnableConnectProtocol))
  result.appendBytes(encodeQuicVarInt(1))
  result.appendBytes(encodeQuicVarInt(http3SettingsH3Datagram))
  result.appendBytes(encodeQuicVarInt(1))
  result.appendBytes(
    encodeQuicVarInt(http3SettingsWebtransportMaxSessions)
  )
  result.appendBytes(encodeQuicVarInt(uint64(info.maxSessions)))

proc parseSettingsPayload(payload: seq[byte]): Http3Settings {.
    raises: [QuicTransportError]
.} =
  var pos = 0
  result.maxSessions = 0
  while pos < payload.len:
    let id = decodeQuicVarIntFrom(payload, pos)
    if pos > payload.len:
      raise (ref QuicTransportError)(
        msg: "malformed HTTP/3 settings payload"
      )
    let value = decodeQuicVarIntFrom(payload, pos)
    case id
    of http3SettingsEnableConnectProtocol:
      result.enableConnectProtocol = value != 0
    of http3SettingsH3Datagram:
      result.enableDatagram = value != 0
    of http3SettingsWebtransportMaxSessions:
      result.maxSessions = uint32(value and high(uint32))
    else:
      discard

proc readSettingsFrame(
    cursor: MsQuicStreamCursor
): Future[Http3Settings] {.async: (raises: [CancelledError, LPStreamError, QuicTransportError]).} =
  if cursor.isNil:
    raise (ref QuicTransportError)(msg: "nil stream cursor while reading settings frame")
  while true:
    let frameType = await cursor.readQuicVarInt()
    let length = await cursor.readQuicVarInt()
    let payload = await cursor.readBytes(int(length))
    if frameType == http3FrameTypeSettings:
      return parseSettingsPayload(payload)

proc discardStream(stream: MsQuicStream) {.async: (raises: []).} =
  if stream.isNil:
    return
  var cursor = initCursor(stream)
  try:
    while true:
      let chunk = await cursor.stream.read()
      if chunk.len == 0:
        break
  except CatchableError:
    discard

proc increment(
    stats: var WebtransportRejectionStats,
    reason: WebtransportRejectionReason,
    amount: uint64 = 1
) {.inline.} =
  case reason
  of wrrSessionLimit:
    stats.sessionLimit += amount
  of wrrMissingConnectProtocol:
    stats.missingConnectProtocol += amount
  of wrrMissingDatagram:
    stats.missingDatagram += amount
  of wrrMissingSessionAccept:
    stats.missingSessionAccept += amount
  of wrrConnectRejected:
    stats.connectRejected += amount
  of wrrInvalidRequest:
    stats.invalidRequest += amount

proc classifyWebtransportHandshakeError(
    message: string
): WebtransportRejectionReason =
  if message.contains("session limit"):
    wrrSessionLimit
  elif message.contains("CONNECT protocol"):
    wrrMissingConnectProtocol
  elif message.contains("HTTP/3 datagrams"):
    wrrMissingDatagram
  elif message.contains("does not accept WebTransport sessions"):
    wrrMissingSessionAccept
  elif message.startsWith("webtransport CONNECT rejected"):
    wrrConnectRejected
  else:
    wrrInvalidRequest

proc releaseLockSafe(lock: AsyncLock) {.inline.} =
  try:
    lock.release()
  except AsyncLockError:
    discard

proc recordWebtransportRejection(
    transport: MsQuicTransport, reason: WebtransportRejectionReason
) =
  transport.webtransportRejection.increment(reason)

proc reserveWebtransportSlot(
    transport: MsQuicTransport
): Future[void] {.async: (raises: [CancelledError, QuicTransportError]).} =
  try:
    await transport.webtransportSessionLock.acquire()
  except AsyncLockError as exc:
    raise (ref QuicTransportError)(
      msg: "failed to acquire webtransport session lock", parent: exc
    )
  try:
    if transport.webtransportActiveSessions >= transport.webtransportMaxSessions:
      trace "webtransport session limit reached",
        active = transport.webtransportActiveSessions,
        limit = transport.webtransportMaxSessions
      transport.recordWebtransportRejection(wrrSessionLimit)
      raise (ref QuicTransportError)(
        msg: "webtransport session limit reached"
      )
    inc transport.webtransportActiveSessions
  finally:
    releaseLockSafe(transport.webtransportSessionLock)

proc releaseWebtransportSlot(transport: MsQuicTransport) {.async: (raises: []).} =
  try:
    await transport.webtransportSessionLock.acquire()
  except CancelledError:
    return
  except AsyncLockError as exc:
    trace "failed to acquire webtransport session lock for release",
      error = exc.msg
    return
  try:
    if transport.webtransportActiveSessions > 0'u32:
      dec transport.webtransportActiveSessions
  finally:
    releaseLockSafe(transport.webtransportSessionLock)

proc registerConnection(
    transport: MsQuicTransport, conn: MsQuicConnection
) =
  if transport.isNil or conn.isNil or conn.registered:
    return
  conn.registered = true
  transport.connections.add(conn)

proc unregisterConnection(
    transport: MsQuicTransport, conn: MsQuicConnection
) =
  if transport.isNil or conn.isNil or not conn.registered:
    return
  conn.registered = false
  transport.connections.keepItIf(it != conn)

proc handleConnectionClosed(
    transport: MsQuicTransport, conn: MsQuicConnection
): Future[void] {.async.} =
  if transport.isNil or conn.isNil:
    return
  if conn.webtransportSlotReserved:
    conn.webtransportSlotReserved = false
    await transport.releaseWebtransportSlot()
  transport.unregisterConnection(conn)

proc awaitPeerStream(
    conn: MsQuicConnection
): Future[(pointer, bool)] {.async: (raises: [CancelledError, QuicTransportError]).} =
  let state = conn.connectionState()
  if state.isNil:
    raise (ref QuicTransportError)(msg: "MsQuic connection state unavailable")
  while true:
    let fut =
      try:
        state.nextConnectionEvent()
      except CatchableError as exc:
        raise (ref QuicTransportError)(
          msg: "MsQuic connection event queue unavailable: " & exc.msg
        )
    let event =
      try:
        await fut
      except msquicdrv.MsQuicEventQueueClosed:
        raise (ref QuicTransportError)(
          msg: "MsQuic connection closed while awaiting peer stream"
        )
      except CatchableError as exc:
        raise (ref QuicTransportError)(
          msg: "MsQuic connection event wait failed: " & exc.msg, parent: exc
        )
    case event.kind
    of msevents.cePeerStreamStarted:
      if event.stream.isNil:
        continue
      return (event.stream, event.streamIsUnidirectional)
    of msevents.ceShutdownInitiated, msevents.ceShutdownComplete:
      raise (ref QuicTransportError)(
        msg: "MsQuic connection shutdown during WebTransport handshake"
      )
    else:
      continue

proc performClientWebtransportHandshake(
    transport: MsQuicTransport,
    conn: MsQuicConnection,
    info: WebtransportHandshakeInfo
){.async: (raises: [CancelledError, QuicTransportError]).} =
  var controlSend: MsQuicStream = nil
  var controlRecv: MsQuicStream = nil
  var requestStream: MsQuicStream = nil
  var success = false
  try:
    controlSend = conn.openMsQuicStream(true, Direction.Out)
    let settingsPayload = buildSettingsPayload(info)
    var settingsFrame = encodeQuicVarInt(http3FrameTypeSettings)
    settingsFrame.appendBytes(encodeQuicVarInt(uint64(settingsPayload.len)))
    settingsFrame.appendBytes(settingsPayload)
    var controlPayload = encodeQuicVarInt(http3StreamTypeControl)
    controlPayload.appendBytes(settingsFrame)
    await controlSend.write(controlPayload)
    await controlSend.sendFin()

    let (incomingPtr, incomingUni) = await awaitPeerStream(conn)
    if not incomingUni:
      raise (ref QuicTransportError)(
        msg: "expected unidirectional HTTP/3 control stream from peer"
      )
    controlRecv = conn.adoptMsQuicStream(incomingPtr, true)
    var ctrlCursor = initCursor(controlRecv)
    let streamType = await ctrlCursor.readQuicVarInt()
    if streamType != http3StreamTypeControl:
      raise (ref QuicTransportError)(msg: "expected HTTP/3 control stream type")
    let remoteSettings = await readSettingsFrame(ctrlCursor)
    let validation = settingsValidationError(remoteSettings)
    if validation.isSome:
      raise (ref QuicTransportError)(msg: validation.get())
    conn.setRemoteSettings(remoteSettings)

    requestStream = conn.openMsQuicStream(false, Direction.Out)
    let headersBlock = makeClientHeadersBlock(info)
    let headersFrame = encodeHeadersFrame(headersBlock)
    await requestStream.write(headersFrame)
    await requestStream.sendFin()

    var reqCursor = initCursor(requestStream)
    let respType = await reqCursor.readQuicVarInt()
    if respType != http3FrameTypeHeaders:
      raise (ref QuicTransportError)(msg: "unexpected frame on CONNECT stream")
    let respLen = await reqCursor.readQuicVarInt()
    let payload = await reqCursor.readBytes(int(respLen))
    let headers = decodeHeadersBlock(payload)
    let status = headers.getOrDefault(":status", "")
    if status.len == 0 or status != "200":
      raise (ref QuicTransportError)(
        msg: "webtransport CONNECT rejected with status " &
          (if status.len == 0: "unknown" else: status)
      )
    let draft = headers.getOrDefault("sec-webtransport-http3-draft", info.draft)
    let sessionIdRes = requestStream.streamId()
    if sessionIdRes.isErr:
      raise (ref QuicTransportError)(
        msg: "failed to obtain WebTransport session id: " & sessionIdRes.error
      )
    let sessionId = sessionIdRes.get()
    var updatedInfo = info
    updatedInfo.draft = draft
    conn.setHandshakeInfo(updatedInfo)
    conn.completeWebtransportHandshake(updatedInfo, sessionId)

    conn.webtransportControlSend = some(controlSend)
    conn.webtransportControlRecv = some(controlRecv)
    conn.webtransportRequestStream = some(requestStream)
    success = true
  except QuicTransportError as exc:
    raise exc
  except LPStreamError as exc:
    raise (ref QuicTransportError)(
      msg: "MsQuic stream error: " & exc.msg,
      parent: exc
    )
  except CatchableError as exc:
    raise (ref QuicTransportError)(
      msg: "MsQuic handshake failed: " & exc.msg,
      parent: exc
    )
  finally:
    if not success:
      if not controlSend.isNil:
        try:
          await noCancel controlSend.closeImpl()
        except CatchableError:
          discard
      if not controlRecv.isNil:
        try:
          await noCancel controlRecv.closeImpl()
        except CatchableError:
          discard
      if not requestStream.isNil:
        try:
          await noCancel requestStream.closeImpl()
        except CatchableError:
          discard

proc performServerWebtransportHandshake(
    transport: MsQuicTransport,
    conn: MsQuicConnection,
    info: WebtransportHandshakeInfo
){.async: (raises: [CancelledError, QuicTransportError]).} =
  var controlRecv: MsQuicStream = nil
  var controlSend: MsQuicStream = nil
  var requestStream: MsQuicStream = nil
  var success = false
  try:
    let (incomingPtr, incomingUni) = await awaitPeerStream(conn)
    if not incomingUni:
      raise (ref QuicTransportError)(
        msg: "expected client HTTP/3 control stream"
      )
    controlRecv = conn.adoptMsQuicStream(incomingPtr, true)
    var ctrlCursor = initCursor(controlRecv)
    let streamType = await ctrlCursor.readQuicVarInt()
    if streamType != http3StreamTypeControl:
      raise (ref QuicTransportError)(msg: "expected HTTP/3 control stream type")
    let remoteSettings = await readSettingsFrame(ctrlCursor)
    let validation = settingsValidationError(remoteSettings)
    if validation.isSome:
      raise (ref QuicTransportError)(msg: validation.get())
    conn.setRemoteSettings(remoteSettings)

    while requestStream.isNil:
      let (streamPtr, uni) = await awaitPeerStream(conn)
      if uni:
        let stray = conn.adoptMsQuicStream(streamPtr, true)
        asyncSpawn discardStream(stray)
      else:
        requestStream = conn.adoptMsQuicStream(streamPtr, false)

    var reqCursor = initCursor(requestStream)
    let frameType = await reqCursor.readQuicVarInt()
    if frameType != http3FrameTypeHeaders:
      raise (ref QuicTransportError)(msg: "expected HEADERS frame on CONNECT stream")
    let frameLen = await reqCursor.readQuicVarInt()
    let payload = await reqCursor.readBytes(int(frameLen))
    let headers = decodeHeadersBlock(payload)
    if headers.getOrDefault(":method", "") != "CONNECT":
      raise (ref QuicTransportError)(msg: "CONNECT method missing")
    if headers.getOrDefault(":protocol", "") != "webtransport":
      raise (ref QuicTransportError)(msg: "CONNECT request is not WebTransport")

    var updatedInfo = info
    updatedInfo.authority = headers.getOrDefault(":authority", info.authority)
    updatedInfo.path = headers.getOrDefault(":path", info.path)
    updatedInfo.origin = headers.getOrDefault("origin", info.origin)
    let draft = headers.getOrDefault("sec-webtransport-http3-draft", info.draft)
    updatedInfo.draft = draft
    conn.setHandshakeInfo(updatedInfo)

    controlSend = conn.openMsQuicStream(true, Direction.Out)
    let settingsPayload = buildSettingsPayload(updatedInfo)
    var settingsFrame = encodeQuicVarInt(http3FrameTypeSettings)
    settingsFrame.appendBytes(encodeQuicVarInt(uint64(settingsPayload.len)))
    settingsFrame.appendBytes(settingsPayload)
    var controlPayload = encodeQuicVarInt(http3StreamTypeControl)
    controlPayload.appendBytes(settingsFrame)
    await controlSend.write(controlPayload)
    await controlSend.sendFin()

    let responseBlock = makeServerHeadersBlock(draft)
    let responseFrame = encodeHeadersFrame(responseBlock)
    await requestStream.write(responseFrame)

    let sessionIdRes = requestStream.streamId()
    if sessionIdRes.isErr:
      raise (ref QuicTransportError)(
        msg: "failed to obtain WebTransport session id: " & sessionIdRes.error
      )
    let sessionId = sessionIdRes.get()
    conn.completeWebtransportHandshake(updatedInfo, sessionId)

    conn.webtransportControlSend = some(controlSend)
    conn.webtransportControlRecv = some(controlRecv)
    conn.webtransportRequestStream = some(requestStream)
    success = true
  except QuicTransportError as exc:
    raise exc
  except LPStreamError as exc:
    raise (ref QuicTransportError)(
      msg: "MsQuic stream error: " & exc.msg,
      parent: exc
    )
  except CatchableError as exc:
    raise (ref QuicTransportError)(
      msg: "MsQuic handshake failed: " & exc.msg,
      parent: exc
    )
  finally:
    if not success:
      if not controlSend.isNil:
        try:
          await noCancel controlSend.closeImpl()
        except CatchableError:
          discard
      if not controlRecv.isNil:
        try:
          await noCancel controlRecv.closeImpl()
        except CatchableError:
          discard
      if not requestStream.isNil:
        try:
          await noCancel requestStream.closeImpl()
        except CatchableError:
          discard

proc performMsQuicWebtransportHandshake(
    transport: MsQuicTransport,
    conn: MsQuicConnection
){.async: (raises: [CancelledError, QuicTransportError]).} =
  let infoOpt = conn.handshakeInfo()
  if infoOpt.isNone:
    return
  let info = infoOpt.get()
  conn.beginWebtransportHandshake(info)
  case info.mode
  of wtmClient:
    await performClientWebtransportHandshake(transport, conn, info)
  of wtmServer:
    await performServerWebtransportHandshake(transport, conn, info)

proc awaitMsQuicDial(
    state: msquicdrv.MsQuicConnectionState
): Future[(bool, string)] {.async.} =
  if state.isNil:
    return (false, "MsQuic connection state unavailable")
  var attempt = 0
  while attempt < MsQuicDialMaxEvents:
    inc attempt
    let fut = state.nextConnectionEvent()
    let completed = await fut.withTimeout(MsQuicDialEventTimeout)
    if not completed:
      fut.cancel()
      return (false, "timeout waiting for MsQuic connection event")
    let event =
      try:
        await fut
      except msquicdrv.MsQuicEventQueueClosed:
        return (false, "MsQuic connection event queue closed")
    case event.kind
    of msevents.ceConnected:
      return (true, "connected")
    of msevents.ceShutdownInitiated:
      return (false, "MsQuic connection shutdown initiated")
    of msevents.ceShutdownComplete:
      return (false, "MsQuic connection shutdown complete")
    else:
      discard
  (false, "MsQuic dial exceeded event budget")

proc newMsQuicTransport*(
    upgrader: Upgrade,
    privateKey: PrivateKey,
    cfg: msquicdrv.MsQuicTransportConfig = msquicdrv.MsQuicTransportConfig()
): MsQuicTransport =
  result = MsQuicTransport(
    upgrader: upgrader,
    cfg: cfg,
    handle: nil,
    listeners: @[],
    listenerFuts: @[],
    privateKey: privateKey,
    certGenerator: defaultCertGenerator,
    certificate: CertificateX509(),
    certificateInitialized: false,
    certificateDer: @[],
    webtransportSessions: @[],
    tlsOverride: none(mstls.TlsConfig),
    tlsTempDir: "",
    connections: @[]
  )
  result.initialize()
  result.initWebtransportDefaults()

method start*(
    self: MsQuicTransport, addrs: seq[MultiAddress]
) {.async: (raises: [LPError, basetransport.TransportError, CancelledError]).} =
  if self.running:
    return
  var initHandle: msquicdrv.MsQuicTransportHandle = nil
  var initErr = ""
  try:
    let res = block:
      var tmp: tuple[
        handle: msquicdrv.MsQuicTransportHandle,
        error: string
      ]
      msquicSafe:
        tmp = msquicdrv.initMsQuicTransport(self.cfg)
      tmp
    initHandle = res.handle
    initErr = res.error
  except Exception as exc:
    initErr = "MsQuic runtime init raised: " & exc.msg
  if initHandle.isNil:
    raise newException(
      basetransport.TransportError,
      "MsQuic runtime unavailable: " & (if initErr.len > 0: initErr else: "unknown error")
    )
  try:
    discard self.ensureCertificate()
  except QuicTransportError as exc:
    try:
      msquicSafe:
        msquicdrv.shutdown(initHandle)
    except Exception:
      discard
    raise exc
  let tlsCfg = self.makeTlsConfig()
  let credentialErr = block:
    var msg = ""
    msquicSafe:
      msg = msquicdrv.loadCredential(initHandle, tlsCfg)
    msg
  if credentialErr.len > 0:
    try:
      msquicSafe:
        msquicdrv.shutdown(initHandle)
    except Exception:
      discard
    raise (ref QuicTransportError)(
      msg: "failed to load MsQuic credentials: " & credentialErr
    )
  var created: seq[MsQuicListenerInfo] = @[]
  proc cleanupCreated() {.raises: [].} =
    for listener in created:
      if listener.handle.isNil:
        continue
      try:
        msquicSafe:
          discard msquicdrv.stopListener(initHandle, listener.handle)
      except Exception:
        discard
      try:
        msquicSafe:
          msquicdrv.closeListener(initHandle, listener.handle, listener.state)
      except Exception:
        discard
    created.setLen(0)

  var listenerPlans: seq[(Option[MultiAddress], bool)] = @[]
  if addrs.len == 0:
    listenerPlans.add((none(MultiAddress), false))
  else:
    for ma in addrs:
      let split = splitTransportAddress(ma)
      if split.isErr:
        cleanupCreated()
        raise newException(
          basetransport.TransportError,
          "MsQuic listen address invalid: " & split.error
        )
      let (baseAddr, hasWebtransport, _) = split.get()
      listenerPlans.add((some(baseAddr), hasWebtransport))

  try:
    proc addListener(
        plan: (Option[MultiAddress], bool)
    ) {.raises: [basetransport.TransportError].} =
      let addrOpt = plan[0]
      let res = block:
        var tmp: tuple[
          listener: pointer,
          state: Option[msquicdrv.MsQuicListenerState],
          error: string
        ]
        msquicSafe:
          tmp = msquicdrv.createListener(initHandle)
        tmp
      if res.error.len > 0 or res.state.isNone or res.listener.isNil:
        cleanupCreated()
        raise newException(
          basetransport.TransportError,
          "MsQuic listener unavailable: " & (if res.error.len > 0: res.error else: "unknown error")
        )
      let startErr = block:
        var msg = ""
        msquicSafe:
          msg = msquicdrv.startListener(initHandle, res.listener)
        msg
      if startErr.len > 0:
        cleanupCreated()
        raise newException(basetransport.TransportError, "MsQuic listener start failed: " & startErr)
      created.add(
        MsQuicListenerInfo(
          handle: res.listener,
          state: res.state.get(),
          webtransport: plan[1],
          baseAddr: addrOpt
        )
      )

    for plan in listenerPlans:
      addListener(plan)
  except Exception as exc:
    cleanupCreated()
    raise newException(
      basetransport.TransportError,
      "MsQuic listener setup failed: " & exc.msg,
      exc
    )

  var advertisedAddrs: seq[MultiAddress] = @[]
  for plan in listenerPlans:
    if plan[0].isNone:
      continue
    var advertised = plan[0].get()
    let protocols = advertised.protocols.valueOr:
      @[]
    if not protocols.anyIt(it == multiCodec("quic-v1")):
      let quicSuffix = MultiAddress.init("/quic-v1")
      if quicSuffix.isErr:
        trace "failed to init quic suffix for advertised address", error = quicSuffix.error
      else:
        let appended = concat(advertised, quicSuffix.get())
        if appended.isOk:
          advertised = appended.get()
        else:
          trace "failed to append quic suffix to advertised address", error = appended.error

    if plan[1]:
      let wtSuffix = MultiAddress.init("/webtransport")
      if wtSuffix.isErr:
        trace "failed to init webtransport suffix for advertised address", error = wtSuffix.error
      else:
        let appended = concat(advertised, wtSuffix.get())
        if appended.isOk:
          advertised = appended.get()
        else:
          trace "failed to append webtransport suffix to advertised address",
            error = appended.error
      self.enforceWebtransportHistoryLimit()
      for hash in self.webtransportCerthashHistory:
        if hash.len == 0:
          continue
        let chSuffix = MultiAddress.init("/certhash/" & hash)
        if chSuffix.isErr:
          trace "failed to init certhash suffix for advertised address", error = chSuffix.error
          continue
        let appended = concat(advertised, chSuffix.get())
        if appended.isOk:
          advertised = appended.get()
        else:
          trace "failed to append certhash suffix to advertised address",
            error = appended.error

    advertisedAddrs.add(advertised)

  self.handle = initHandle
  self.listeners = created
  resetListenerFutures(self)
  await procCall basetransport.Transport(self).start(advertisedAddrs)

method stop*(self: MsQuicTransport) {.async: (raises: []).} =
  if not self.running:
    return
  closeAllListeners(self)
  await procCall basetransport.Transport(self).stop()
  if not self.handle.isNil:
    try:
      msquicSafe:
        msquicdrv.shutdown(self.handle)
    except Exception as exc:
      trace "MsQuic shutdown raised", err = exc.msg
    self.handle = nil

method handles*(
    self: MsQuicTransport, address: MultiAddress
): bool {.gcsafe, raises: [].} =
  let protocols = address.protocols.valueOr:
    return false
  protocols.anyIt(it == multiCodec("udp") or it == multiCodec("quic-v1"))

proc ensureRunning(self: MsQuicTransport) =
  if not self.running or self.handle.isNil:
    raise newException(
      basetransport.TransportClosedError,
      "MsQuic transport not running"
    )

method accept*(
    self: MsQuicTransport
): Future[Connection] {.gcsafe, async: (raises: [basetransport.TransportError, CancelledError]).} =
  self.ensureRunning()
  if self.listeners.len == 0:
    raise newException(
      basetransport.TransportError,
      "MsQuic transport has no active listeners"
    )

  if self.listenerFuts.len == 0:
    resetListenerFutures(self)

  while true:
    var finished: Future[msevents.ListenerEvent]
    try:
      finished = await one(self.listenerFuts)
    except CancelledError as exc:
      raise exc
    except ValueError:
      raise newException(
        basetransport.TransportError,
        "MsQuic listener futures empty"
      )
    except Exception as exc:
      trace "MsQuic listener wait raised", err = exc.msg
      continue
    let idx = self.listenerFuts.find(finished)
    if idx < 0:
      continue
    let listenerState = self.listeners[idx].state
    self.listenerFuts[idx] = listenerState.nextListenerEvent()

    var event: msevents.ListenerEvent
    try:
      event = await finished
    except msquicdrv.MsQuicEventQueueClosed:
      if not listenerState.isNil:
        self.listenerFuts[idx] = listenerState.nextListenerEvent()
      continue
    except Exception as exc:
      trace "MsQuic listener future raised", err = exc.msg
      if not listenerState.isNil:
        self.listenerFuts[idx] = listenerState.nextListenerEvent()
      continue

    case event.kind
    of msevents.leNewConnection:
      if event.connection.isNil:
        trace "MsQuic listener delivered nil connection"
        continue
      let attachRes = block:
        var tmp: tuple[
          state: Option[msquicdrv.MsQuicConnectionState],
          error: string
        ]
        try:
          msquicSafe:
            tmp = msquicdrv.attachIncomingConnection(
              self.handle,
              event.connection,
              queueLimit = self.cfg.eventQueueLimit,
              pollInterval = self.cfg.eventPollInterval
            )
        except Exception as exc:
          tmp = (none(msquicdrv.MsQuicConnectionState), "MsQuic attach raised: " & exc.msg)
        tmp
      let stateOpt = attachRes.state
      let err = attachRes.error
      if err.len > 0 or stateOpt.isNone:
        trace "MsQuic attach incoming connection failed", error = err
        cleanupMsQuicDial(self, event.connection, if stateOpt.isSome: stateOpt.get() else: nil)
        continue
      var connection: MsQuicConnection
      let inboundState = stateOpt.get()
      let listenerMeta = self.listeners[idx]
      try:
        let localAddr =
          if listenerMeta.baseAddr.isSome:
            Opt.some(listenerMeta.baseAddr.get())
          else:
            Opt.none(MultiAddress)
        connection = newMsQuicConnection(
          self.handle,
          event.connection,
          inboundState,
          local = localAddr,
          onClosed = proc(c: MsQuicConnection): Future[void] {.async.} =
            await self.handleConnectionClosed(c)
        )
      except LPStreamError as exc:
        trace "MsQuic connection construction failed", error = exc.msg
        cleanupMsQuicDial(self, event.connection, inboundState)
        continue
      self.registerConnection(connection)
      connection.protocol = "msquic"
      if listenerMeta.webtransport:
        let handshakeInfo = makeServerHandshakeInfo(self)
        connection.setHandshakeInfo(handshakeInfo)
        connection.protocol = "msquic-webtransport"
        var slotReserved = false
        try:
          await self.reserveWebtransportSlot()
          slotReserved = true
          connection.webtransportSlotReserved = true
          await performMsQuicWebtransportHandshake(self, connection)
        except CancelledError as exc:
          if slotReserved:
            connection.webtransportSlotReserved = false
            await noCancel self.releaseWebtransportSlot()
          await noCancel connection.close()
          raise exc
        except QuicTransportError as exc:
          let reason = classifyWebtransportHandshakeError(exc.msg)
          if slotReserved:
            connection.webtransportSlotReserved = false
            await noCancel self.releaseWebtransportSlot()
          if not (not slotReserved and reason == wrrSessionLimit):
            self.webtransportRejection.increment(reason)
          await noCancel connection.close()
          continue
      return connection
    of msevents.leStopComplete:
      trace "MsQuic listener stop complete"
      continue
    else:
      continue

method dial*(
    self: MsQuicTransport,
    hostname: string,
    address: MultiAddress,
    peerId: Opt[PeerId] = Opt.none(PeerId)
  ): Future[Connection] {.gcsafe, async: (raises: [basetransport.TransportError, CancelledError]).} =
  self.ensureRunning()

  let split = splitTransportAddress(address)
  if split.isErr:
    raise newException(
      basetransport.TransportDialError,
      "MsQuic dial requires /ip*/udp/* multiaddress: " & split.error
    )
  let (baseAddr, useWebtransport, certHashes) = split.get()
  if certHashes.len > 0:
    trace "MsQuic dial received certhash hints",
      hint_count = certHashes.len

  let (host, portStr) = extractHostPort(baseAddr)
  if host.len == 0 or portStr.len == 0:
    raise newException(
      basetransport.TransportDialError,
      "MsQuic dial requires /ip*/udp/* multiaddress"
    )

  var portInt: int
  try:
    portInt = parseInt(portStr)
  except ValueError:
    raise newException(
      basetransport.TransportDialError,
      "invalid port in multiaddress: " & portStr
    )
  if portInt < 0 or portInt > high(uint16).int:
    raise newException(
      basetransport.TransportDialError,
      "port out of range in multiaddress: " & portStr
    )
  let port = uint16(portInt)
  let sni = if hostname.len > 0: hostname else: host
  let addressFamily = detectAddressFamily(host)
  trace "MsQuic dial resolved address family", host = host, family = addressFamily

  var connPtr: pointer = nil
  var connStateOpt: Option[msquicdrv.MsQuicConnectionState]
  var dialErr = ""
  try:
    let res = block:
      var tmp: tuple[
        connection: pointer,
        state: Option[msquicdrv.MsQuicConnectionState],
        error: string
      ]
      msquicSafe:
        tmp = msquicdrv.dialConnection(
          self.handle,
          sni,
          port,
          addressFamily = addressFamily,
          queueLimit = self.cfg.eventQueueLimit,
          pollInterval = MsQuicDialEventTimeout
        )
      tmp
    connPtr = res.connection
    connStateOpt = res.state
    dialErr = res.error
  except Exception as exc:
    dialErr = "MsQuic dial raised: " & exc.msg
  if dialErr.len > 0 or connPtr.isNil or connStateOpt.isNone:
    raise newException(
      basetransport.TransportDialError,
      "MsQuic dial failed: " & (if dialErr.len > 0: dialErr else: "unknown error")
    )
  let connState = connStateOpt.get()

  var connected = false
  var reason = ""
  try:
    let res = await awaitMsQuicDial(connState)
    connected = res[0]
    reason = res[1]
  except Exception as exc:
    connected = false
    reason = "MsQuic dial wait raised: " & exc.msg
  if not connected:
    cleanupMsQuicDial(self, connPtr, connState)
    warn "MsQuic dial handshake failed", host = host, port = port, reason = reason
    raise newException(
      basetransport.TransportDialError,
      "MsQuic dial failed: " & reason
    )

  var connection: MsQuicConnection
  try:
    connection = newMsQuicConnection(
      self.handle,
      connPtr,
      connState,
      observed = Opt.some(address),
      onClosed = proc(c: MsQuicConnection): Future[void] {.async.} =
        await self.handleConnectionClosed(c)
    )
  except LPStreamError as exc:
    cleanupMsQuicDial(self, connPtr, connState)
    raise newException(
      basetransport.TransportDialError,
      "MsQuic connection setup failed: " & exc.msg,
      exc
    )
  except Exception as exc:
    cleanupMsQuicDial(self, connPtr, connState)
    raise newException(
      basetransport.TransportDialError,
      "MsQuic connection setup raised: " & exc.msg,
      exc
    )

  self.registerConnection(connection)
  if peerId.isSome:
    connection.peerId = peerId.get()
  connection.protocol = "msquic"
  if useWebtransport:
    let handshakeInfo = makeClientHandshakeInfo(self, address, hostname)
    connection.setHandshakeInfo(handshakeInfo)
    connection.protocol = "msquic-webtransport"
    var slotReserved = false
    try:
      await self.reserveWebtransportSlot()
      slotReserved = true
      connection.webtransportSlotReserved = true
      await performMsQuicWebtransportHandshake(self, connection)
    except CancelledError as exc:
      if slotReserved:
        connection.webtransportSlotReserved = false
        await noCancel self.releaseWebtransportSlot()
      await noCancel connection.close()
      raise exc
    except QuicTransportError as exc:
      let reason = classifyWebtransportHandshakeError(exc.msg)
      if slotReserved:
        connection.webtransportSlotReserved = false
        await noCancel self.releaseWebtransportSlot()
      if not (not slotReserved and reason == wrrSessionLimit):
        self.webtransportRejection.increment(reason)
      await noCancel connection.close()
      raise newException(
        basetransport.TransportDialError,
        "MsQuic dial failed: " & exc.msg,
        exc
      )
  connection
