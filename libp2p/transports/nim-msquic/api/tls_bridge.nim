## MsQuic TLS 桥接：把 Nim TLS 配置映射到 QUIC_CREDENTIAL_CONFIG，
## 并提供最小事件回调与资源清理支持。

import std/[options, os, random, strformat, times]

import ../tls/common

type
  QuicCredentialType* = distinct uint32
  QuicCredentialFlags* = distinct uint32
  QuicAllowedCipherSuiteFlags* = distinct uint32
  QuicCertificateStoreFlags* = distinct uint32

  QuicCredentialLoadCompleteHandler* = proc (
      configuration: pointer; context: pointer; status: uint32): uint32 {.cdecl.}

  QuicCertificateFile* {.bycopy.} = object
    PrivateKeyFile*: cstring
    CertificateFile*: cstring

  QuicCertificateFileProtected* {.bycopy.} = object
    PrivateKeyFile*: cstring
    CertificateFile*: cstring
    PrivateKeyPassword*: cstring

  QuicCertificatePkcs12* {.bycopy.} = object
    Asn1Blob*: ptr uint8
    Asn1BlobLength*: uint32
    PrivateKeyPassword*: cstring

  QuicCertificateHash* {.bycopy.} = object
    Length*: uint32
    ShaHash*: array[TlsCertificateHashLength, uint8]

  QuicCertificateHashStore* {.bycopy.} = object
    Hash*: QuicCertificateHash
    Flags*: QuicCertificateStoreFlags
    StoreName*: cstring

  QuicCredentialConfig* {.bycopy.} = object
    Type*: QuicCredentialType
    Flags*: QuicCredentialFlags
    Certificate*: pointer
    Principal*: cstring
    Reserved*: pointer
    AsyncHandler*: QuicCredentialLoadCompleteHandler
    AllowedCipherSuites*: QuicAllowedCipherSuiteFlags
    CaCertificateFile*: cstring

const
  QUIC_CREDENTIAL_TYPE_NONE* = QuicCredentialType(0)
  QUIC_CREDENTIAL_TYPE_CERTIFICATE_HASH* = QuicCredentialType(1)
  QUIC_CREDENTIAL_TYPE_CERTIFICATE_HASH_STORE* = QuicCredentialType(2)
  QUIC_CREDENTIAL_TYPE_CERTIFICATE_CONTEXT* = QuicCredentialType(3)
  QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE* = QuicCredentialType(4)
  QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE_PROTECTED* = QuicCredentialType(5)
  QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12* = QuicCredentialType(6)

  QUIC_CREDENTIAL_FLAG_NONE* = QuicCredentialFlags(0x00000000)
  QUIC_CREDENTIAL_FLAG_CLIENT* = QuicCredentialFlags(0x00000001)
  QUIC_CREDENTIAL_FLAG_LOAD_ASYNCHRONOUS* = QuicCredentialFlags(0x00000002)
  QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION* = QuicCredentialFlags(0x00000004)
  QUIC_CREDENTIAL_FLAG_ENABLE_OCSP* = QuicCredentialFlags(0x00000008)
  QUIC_CREDENTIAL_FLAG_INDICATE_CERTIFICATE_RECEIVED* = QuicCredentialFlags(0x00000010)
  QUIC_CREDENTIAL_FLAG_DEFER_CERTIFICATE_VALIDATION* = QuicCredentialFlags(0x00000020)
  QUIC_CREDENTIAL_FLAG_REQUIRE_CLIENT_AUTHENTICATION* = QuicCredentialFlags(0x00000040)
  QUIC_CREDENTIAL_FLAG_USE_TLS_BUILTIN_CERTIFICATE_VALIDATION* = QuicCredentialFlags(0x00000080)
  QUIC_CREDENTIAL_FLAG_SET_ALLOWED_CIPHER_SUITES* = QuicCredentialFlags(0x00002000)
  QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE* = QuicCredentialFlags(0x00100000)

  QUIC_ALLOWED_CIPHER_SUITE_NONE* = QuicAllowedCipherSuiteFlags(0)
  QUIC_ALLOWED_CIPHER_SUITE_AES_128_GCM_SHA256* = QuicAllowedCipherSuiteFlags(0x1)
  QUIC_ALLOWED_CIPHER_SUITE_AES_256_GCM_SHA384* = QuicAllowedCipherSuiteFlags(0x2)
  QUIC_ALLOWED_CIPHER_SUITE_CHACHA20_POLY1305_SHA256* = QuicAllowedCipherSuiteFlags(0x4)

  QUIC_CERTIFICATE_STORE_FLAG_NONE* = QuicCertificateStoreFlags(0x00000000)

type
  TlsCredentialLoadCallback* = proc (status: uint32) {.gcsafe.}
  TlsSessionTicketHandler* = proc (ticket: seq[uint8]) {.gcsafe.}
  TlsKeyUpdateHandler* = proc (level: TlsEncryptionLevel; direction: TlsDirection) {.gcsafe.}

  TlsBridgeHandlers* = object
    onCredentialLoaded*: TlsCredentialLoadCallback
    onSessionTicket*: TlsSessionTicketHandler
    onKeyUpdate*: TlsKeyUpdateHandler

  TlsCredentialBinding* = ref object
    tls*: TlsConfig
    config*: QuicCredentialConfig
    certificateFile*: QuicCertificateFile
    certificateProtected*: QuicCertificateFileProtected
    certificatePkcs12*: QuicCertificatePkcs12
    certificateHash*: QuicCertificateHash
    certificateHashStore*: QuicCertificateHashStore
    storage*: seq[string]
    binaryStorage*: seq[seq[uint8]]
    tempFiles*: seq[string]
    handlers*: TlsBridgeHandlers

var gRngInitialized = false

template asUInt(x: QuicCredentialFlags): uint32 =
  cast[uint32](x)

template asUInt(x: QuicAllowedCipherSuiteFlags): uint32 =
  cast[uint32](x)

proc ensureRandomSeed() =
  if not gRngInitialized:
    randomize()
    gRngInitialized = true

proc `or`*(a, b: QuicCredentialFlags): QuicCredentialFlags {.inline.} =
  QuicCredentialFlags(asUInt(a) or asUInt(b))

proc `or`*(a, b: QuicAllowedCipherSuiteFlags): QuicAllowedCipherSuiteFlags {.inline.} =
  QuicAllowedCipherSuiteFlags(asUInt(a) or asUInt(b))

proc includesFlag*(flags, flag: QuicCredentialFlags): bool {.inline.} =
  (asUInt(flags) and asUInt(flag)) != 0

proc addStorage(binding: TlsCredentialBinding; value: string): cstring =
  binding.storage.add value
  binding.storage[^1].cstring

proc addBinary(binding: TlsCredentialBinding; data: seq[uint8]): ptr uint8 =
  binding.binaryStorage.add(data)
  if binding.binaryStorage[^1].len == 0:
    nil
  else:
    addr binding.binaryStorage[^1][0]

proc readBinaryFile(path: string): seq[uint8] =
  let content = readFile(path)
  var bytes = newSeq[uint8](content.len)
  if bytes.len > 0:
    copyMem(addr bytes[0], cast[pointer](content.cstring), content.len)
  bytes

proc persistFile(binding: TlsCredentialBinding; prefix, suffix, content: string;
    tempDir: string): string =
  let baseDir =
    if tempDir.len == 0: getTempDir() else: tempDir
  try:
    createDir(baseDir)
  except OSError:
    discard
  var attempt = 0
  var path: string
  while true:
    inc attempt
    ensureRandomSeed()
    let stamp = toUnix(getTime())
    let nonce = rand(0x00FF_FFFF)
    let name = fmt"{prefix}-{stamp}-{nonce}{suffix}"
    path = joinPath(baseDir, name)
    if not fileExists(path):
      writeFile(path, content)
      binding.tempFiles.add(path)
      return path
    if attempt > 6:
      raise newException(OSError, "无法创建唯一的临时凭据文件")

proc prepareServerCredential(binding: TlsCredentialBinding; tempDir: string) =
  let cfg = binding.tls
  if cfg.certificatePem.isNone or cfg.privateKeyPem.isNone:
    raise newException(ValueError, "服务器 TLS 配置缺少证书或私钥")
  let certPath = binding.persistFile("nim-msquic-cert", ".pem",
    cfg.certificatePem.get, tempDir)
  let keyPath = binding.persistFile("nim-msquic-key", ".pem",
    cfg.privateKeyPem.get, tempDir)
  binding.certificateFile.CertificateFile = binding.addStorage(certPath)
  binding.certificateFile.PrivateKeyFile = binding.addStorage(keyPath)
  binding.config.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE
  binding.config.Certificate = cast[pointer](addr binding.certificateFile)

proc handleCredentialLoad*(binding: TlsCredentialBinding; status: uint32) =
  if binding.isNil:
    return
  if not binding.handlers.onCredentialLoaded.isNil:
    binding.handlers.onCredentialLoaded(status)

proc handleSessionTicket*(binding: TlsCredentialBinding;
    ticket: openArray[uint8]) =
  if binding.isNil or binding.handlers.onSessionTicket.isNil:
    return
  var copy = newSeqOfCap[uint8](ticket.len)
  for b in ticket:
    copy.add b
  binding.handlers.onSessionTicket(copy)

proc handleKeyUpdate*(binding: TlsCredentialBinding;
    level: TlsEncryptionLevel; direction: TlsDirection) =
  if binding.isNil or binding.handlers.onKeyUpdate.isNil:
    return
  binding.handlers.onKeyUpdate(level, direction)

proc credentialLoadShim(configuration: pointer; context: pointer;
    status: uint32): uint32 {.cdecl.} =
  discard configuration
  let binding =
    if context.isNil: TlsCredentialBinding(nil)
    else: cast[TlsCredentialBinding](context)
  handleCredentialLoad(binding, status)
  status

proc newTlsCredentialBinding*(cfg: TlsConfig;
    handlers: TlsBridgeHandlers = TlsBridgeHandlers();
    tempDir: string = ""): TlsCredentialBinding =
  new(result)
  result.tls = cfg
  result.handlers = handlers
  result.storage = @[]
  result.binaryStorage = @[]
  result.tempFiles = @[]
  result.config.Type = QUIC_CREDENTIAL_TYPE_NONE
  result.config.Flags = QUIC_CREDENTIAL_FLAG_NONE
  result.config.Certificate = nil
  result.config.Principal = nil
  result.config.Reserved = cast[pointer](result)
  if handlers.onCredentialLoaded.isNil:
    result.config.AsyncHandler = nil
  else:
    result.config.AsyncHandler = credentialLoadShim
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_LOAD_ASYNCHRONOUS
  result.config.AllowedCipherSuites = QUIC_ALLOWED_CIPHER_SUITE_NONE
  result.config.CaCertificateFile = nil

  let effectiveTempDir =
    if cfg.tempDirectory.isSome and cfg.tempDirectory.get().len > 0:
      cfg.tempDirectory.get()
    else:
      tempDir

  if cfg.disableCertificateValidation:
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION
  if cfg.requireClientAuth:
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_REQUIRE_CLIENT_AUTHENTICATION
  if cfg.enableOcsp:
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_ENABLE_OCSP
  if cfg.indicateCertificateReceived:
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_INDICATE_CERTIFICATE_RECEIVED
  if cfg.deferCertificateValidation:
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_DEFER_CERTIFICATE_VALIDATION
  if cfg.useBuiltinCertificateValidation:
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_USE_TLS_BUILTIN_CERTIFICATE_VALIDATION
  if cfg.allowedCipherSuites.isSome:
    result.config.AllowedCipherSuites =
      QuicAllowedCipherSuiteFlags(cfg.allowedCipherSuites.get())
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_SET_ALLOWED_CIPHER_SUITES
  if cfg.caCertificateFile.isSome and cfg.caCertificateFile.get().len > 0:
    result.config.CaCertificateFile = result.addStorage(cfg.caCertificateFile.get())
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE
  else:
    result.config.CaCertificateFile = nil

  case cfg.role
  of tlsClient:
    result.config.Flags = result.config.Flags or QUIC_CREDENTIAL_FLAG_CLIENT
    if cfg.serverName.isSome:
      result.config.Principal = result.addStorage(cfg.serverName.get)
  of tlsServer:
    discard

  var credentialConfigured = false

  if cfg.certificateContext.isSome:
    result.config.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_CONTEXT
    result.config.Certificate = cfg.certificateContext.get()
    credentialConfigured = true
  elif cfg.certificateHash.isSome:
    let hash = cfg.certificateHash.get()
    result.certificateHash.Length = TlsCertificateHashLength.uint32
    result.certificateHash.ShaHash = hash
    if cfg.certificateStore.isSome or cfg.certificateStoreFlags != 0'u32:
      result.certificateHashStore.Hash = result.certificateHash
      result.certificateHashStore.Flags = QuicCertificateStoreFlags(cfg.certificateStoreFlags)
      if cfg.certificateStore.isSome and cfg.certificateStore.get().len > 0:
        result.certificateHashStore.StoreName = result.addStorage(cfg.certificateStore.get())
      else:
        result.certificateHashStore.StoreName = nil
      result.config.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_HASH_STORE
      result.config.Certificate = cast[pointer](addr result.certificateHashStore)
    else:
      result.config.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_HASH
      result.config.Certificate = cast[pointer](addr result.certificateHash)
    credentialConfigured = true
  elif cfg.certificateFile.isSome and cfg.privateKeyFile.isSome:
    let certPtr = result.addStorage(cfg.certificateFile.get())
    let keyPtr = result.addStorage(cfg.privateKeyFile.get())
    result.certificateFile.CertificateFile = certPtr
    result.certificateFile.PrivateKeyFile = keyPtr
    if cfg.privateKeyPassword.isSome and cfg.privateKeyPassword.get().len > 0:
      result.certificateProtected.CertificateFile = certPtr
      result.certificateProtected.PrivateKeyFile = keyPtr
      result.certificateProtected.PrivateKeyPassword =
        result.addStorage(cfg.privateKeyPassword.get())
      result.config.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE_PROTECTED
      result.config.Certificate = cast[pointer](addr result.certificateProtected)
    else:
      result.config.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE
      result.config.Certificate = cast[pointer](addr result.certificateFile)
    credentialConfigured = true
  elif cfg.pkcs12Data.isSome or cfg.pkcs12File.isSome:
    var pkcs12Bytes: seq[uint8]
    if cfg.pkcs12Data.isSome:
      pkcs12Bytes = cfg.pkcs12Data.get()
    else:
      if not cfg.pkcs12File.isSome or cfg.pkcs12File.get().len == 0:
        raise newException(ValueError, "pkcs12File 未提供有效路径")
      pkcs12Bytes = readBinaryFile(cfg.pkcs12File.get())
    let blobPtr = result.addBinary(pkcs12Bytes)
    result.certificatePkcs12.Asn1Blob = blobPtr
    result.certificatePkcs12.Asn1BlobLength = uint32(pkcs12Bytes.len)
    if cfg.pkcs12Password.isSome and cfg.pkcs12Password.get().len > 0:
      result.certificatePkcs12.PrivateKeyPassword =
        result.addStorage(cfg.pkcs12Password.get())
    else:
      result.certificatePkcs12.PrivateKeyPassword = nil
    result.config.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12
    result.config.Certificate = cast[pointer](addr result.certificatePkcs12)
    credentialConfigured = true
  elif cfg.certificatePem.isSome and cfg.privateKeyPem.isSome:
    result.prepareServerCredential(effectiveTempDir)
    credentialConfigured = true

  if not credentialConfigured and cfg.role == tlsServer:
    raise newException(ValueError, "服务器 TLS 配置缺少证书或私钥")

proc credentialConfigPtr*(binding: TlsCredentialBinding): ptr QuicCredentialConfig =
  if binding.isNil:
    return nil
  addr binding.config

proc initialResumptionTicket*(binding: TlsCredentialBinding): Option[seq[uint8]] =
  if binding.isNil:
    return none(seq[uint8])
  binding.tls.resumptionTicket

proc cleanup*(binding: TlsCredentialBinding) =
  if binding.isNil:
    return
  for path in binding.tempFiles:
    try:
      if fileExists(path):
        removeFile(path)
    except CatchableError:
      discard
  binding.tempFiles.setLen(0)
