## Nim TLS 公共类型：定义握手阶段、密钥元数据与处理结果。

import std/options

const
  TlsCertificateHashLength* = 20

type
  TlsCertificateHash* = array[TlsCertificateHashLength, uint8]

type
  TlsRole* = enum
    tlsClient
    tlsServer

  TlsEncryptionLevel* = enum
    telInitial
    telZeroRtt
    telHandshake
    telOneRtt

  TlsProcessFlag* = enum
    tpfHasData
    tpfReadKeyUpdated
    tpfWriteKeyUpdated
    tpfEarlyDataAccepted
    tpfEarlyDataRejected
    tpfHandshakeComplete

  TlsEarlyDataState* = enum
    tedsUnknown
    tedsUnsupported
    tedsRejected
    tedsAccepted

  TlsDirection* = enum
    tdRead
    tdWrite

  TlsAeadAlgorithm* = enum
    taaAes128Gcm
    taaAes256Gcm
    taaChacha20Poly1305

  TlsHashAlgorithm* = enum
    thaSha256
    thaSha384
    thaSha512

  TlsSecret* = object
    level*: TlsEncryptionLevel
    direction*: TlsDirection
    aead*: TlsAeadAlgorithm
    hash*: TlsHashAlgorithm
    material*: seq[uint8]

  TlsSecretView* = object
    level*: TlsEncryptionLevel
    direction*: TlsDirection
    aead*: TlsAeadAlgorithm
    hash*: TlsHashAlgorithm
    dataPtr*: pointer
    length*: uint16

  TlsHandshakeChunk* = object
    level*: TlsEncryptionLevel
    data*: seq[uint8]

  TlsProcessResult* = object
    flags*: set[TlsProcessFlag]
    chunks*: seq[TlsHandshakeChunk]
    secrets*: seq[TlsSecret]
    secretViews*: seq[TlsSecretView]
    sessionResumed*: bool
    earlyDataState*: TlsEarlyDataState
    alertCode*: uint16
    negotiatedAlpn*: seq[uint8]
    peerTransportParameters*: Option[seq[uint8]]
    rawSession*: Option[seq[uint8]]

  TlsConfig* = object
    role*: TlsRole
    alpns*: seq[string]
    transportParameters*: seq[uint8]
    serverName*: Option[string]
    certificatePem*: Option[string]
    privateKeyPem*: Option[string]
    certificateFile*: Option[string]
    privateKeyFile*: Option[string]
    privateKeyPassword*: Option[string]
    pkcs12File*: Option[string]
    pkcs12Data*: Option[seq[uint8]]
    pkcs12Password*: Option[string]
    certificateHash*: Option[TlsCertificateHash]
    certificateStore*: Option[string]
    certificateStoreFlags*: uint32
    certificateContext*: Option[pointer]
    caCertificateFile*: Option[string]
    resumptionTicket*: Option[seq[uint8]]
    enableZeroRtt*: bool
    useSharedSessionCache*: bool
    disableCertificateValidation*: bool
    requireClientAuth*: bool
    enableOcsp*: bool
    indicateCertificateReceived*: bool
    deferCertificateValidation*: bool
    useBuiltinCertificateValidation*: bool
    allowedCipherSuites*: Option[uint32]
    tempDirectory*: Option[string]
