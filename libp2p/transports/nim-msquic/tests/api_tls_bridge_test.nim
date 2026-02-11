import std/[options, os, strformat, strutils, unittest]

import ../api/tls_bridge
import ../tls/common
import ./tls_handshake_test

suite "MsQuic TLS Bridge (F3)":
  test "client config outputs MsQuic credential skeleton":
    let cfg = TlsConfig(
      role: tlsClient,
      alpns: @["h3"],
      transportParameters: @[],
      serverName: some("example.com"),
      enableZeroRtt: true,
      useSharedSessionCache: true
    )
    let binding = newTlsCredentialBinding(cfg)
    binding.cleanup() # 客户端不会创建临时文件，保持幂等
    let cfgPtr = binding.credentialConfigPtr()
    require cfgPtr != nil
    let cCfg = cfgPtr[]
    check cast[uint32](cCfg.Type) == cast[uint32](QUIC_CREDENTIAL_TYPE_NONE)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_CLIENT)
    check cCfg.Certificate.isNil
    check cCfg.Principal != nil
    check $cCfg.Principal == "example.com"
    check cast[pointer](binding) == cCfg.Reserved

  test "server config writes PEM artifacts and triggers handlers":
    var credentialStatus: uint32 = 1
    var credentialHit = 0
    var capturedTicketLen = 0
    var capturedTicketBuf: array[8, uint8]
    var keyUpdateLen = 0
    var keyUpdateEntries: array[4, tuple[level: TlsEncryptionLevel, direction: TlsDirection]]

    var handlers = TlsBridgeHandlers()
    handlers.onCredentialLoaded = proc(status: uint32) {.gcsafe.} =
      credentialStatus = status
      inc credentialHit
    handlers.onSessionTicket = proc(ticket: seq[uint8]) {.gcsafe.} =
      capturedTicketLen = ticket.len
      if capturedTicketLen > capturedTicketBuf.len:
        capturedTicketLen = capturedTicketBuf.len
      for i in 0 ..< capturedTicketLen:
        capturedTicketBuf[i] = ticket[i]
    handlers.onKeyUpdate = proc(level: TlsEncryptionLevel; direction: TlsDirection) {.gcsafe.} =
      if keyUpdateLen < keyUpdateEntries.len:
        keyUpdateEntries[keyUpdateLen] = (level, direction)
        inc keyUpdateLen

    let cfg = TlsConfig(
      role: tlsServer,
      alpns: @["h3"],
      transportParameters: @[],
      certificatePem: some(TestCertificate),
      privateKeyPem: some(TestPrivateKey)
    )

    let binding = newTlsCredentialBinding(cfg, handlers)
    let cfgPtr = binding.credentialConfigPtr()
    require cfgPtr != nil
    let cCfg = cfgPtr[]
    check cast[uint32](cCfg.Type) == cast[uint32](QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE)
    check not cCfg.Certificate.isNil
    let certFilePtr = cast[ptr QuicCertificateFile](cCfg.Certificate)
    require certFilePtr != nil
    check certFilePtr.CertificateFile != nil
    check certFilePtr.PrivateKeyFile != nil
    let certPath = $certFilePtr.CertificateFile
    let keyPath = $certFilePtr.PrivateKeyFile
    check fileExists(certPath)
    check fileExists(keyPath)
    check readFile(certPath).contains("BEGIN CERTIFICATE")
    check readFile(keyPath).contains("PRIVATE KEY")

    handleCredentialLoad(binding, 0'u32)
    check credentialHit == 1
    check credentialStatus == 0

    handleSessionTicket(binding, @[1'u8, 2, 3, 4])
    check capturedTicketLen == 4
    check capturedTicketBuf[0] == 1'u8
    check capturedTicketBuf[1] == 2'u8
    check capturedTicketBuf[2] == 3'u8
    check capturedTicketBuf[3] == 4'u8

    handleKeyUpdate(binding, telOneRtt, tdWrite)
    check keyUpdateLen == 1
    check keyUpdateEntries[0] == (telOneRtt, tdWrite)

    binding.cleanup()
    check not fileExists(certPath)
    check not fileExists(keyPath)
    binding.cleanup() # 再次清理保持幂等

  test "resumption ticket is exposed for client reuse":
    let ticket = @[0x01'u8, 0x02'u8, 0x03'u8]
    let cfg = TlsConfig(
      role: tlsClient,
      alpns: @["hq"],
      transportParameters: @[],
      resumptionTicket: some(ticket)
    )
    let binding = newTlsCredentialBinding(cfg)
    defer:
      binding.cleanup()
    let exposed = initialResumptionTicket(binding)
    check exposed.isSome
    check exposed.get == ticket

  test "server config uses existing PEM artifacts without persistence":
    let tempDir = getTempDir()
    let certPath = joinPath(tempDir, fmt"msquic-test-cert-{epochTime()}.pem")
    let keyPath = joinPath(tempDir, fmt"msquic-test-key-{epochTime()}.pem")
    writeFile(certPath, TestCertificate)
    writeFile(keyPath, TestPrivateKey)
    let cfg = TlsConfig(
      role: tlsServer,
      alpns: @["h3"],
      transportParameters: @[],
      certificateFile: some(certPath),
      privateKeyFile: some(keyPath)
    )
    let binding = newTlsCredentialBinding(cfg)
    let cfgPtr = binding.credentialConfigPtr()
    require cfgPtr != nil
    let cCfg = cfgPtr[]
    check cast[uint32](cCfg.Type) == cast[uint32](QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE)
    let filePtr = cast[ptr QuicCertificateFile](cCfg.Certificate)
    require filePtr != nil
    check $filePtr.CertificateFile == certPath
    check $filePtr.PrivateKeyFile == keyPath
    binding.cleanup()
    check fileExists(certPath)
    check fileExists(keyPath)
    removeFile(certPath)
    removeFile(keyPath)

  test "server config accepts certificate hash store descriptor":
    var hash: TlsCertificateHash
    for i in 0 ..< TlsCertificateHashLength:
      hash[i] = uint8(i)
    let cfg = TlsConfig(
      role: tlsServer,
      certificateHash: some(hash),
      certificateStore: some("My"),
      certificateStoreFlags: 0x2'u32
    )
    let binding = newTlsCredentialBinding(cfg)
    defer: binding.cleanup()
    let cfgPtr = binding.credentialConfigPtr()
    require cfgPtr != nil
    let cCfg = cfgPtr[]
    check cast[uint32](cCfg.Type) == cast[uint32](QUIC_CREDENTIAL_TYPE_CERTIFICATE_HASH_STORE)
    let hashPtr = cast[ptr QuicCertificateHashStore](cCfg.Certificate)
    require hashPtr != nil
    check hashPtr.Hash.Length == TlsCertificateHashLength.uint32
    for i in 0 ..< TlsCertificateHashLength:
      check hashPtr.Hash.ShaHash[i] == uint8(i)
    check $hashPtr.StoreName == "My"

  test "server config accepts PKCS12 inline data":
    let pkcs12 = @[0x30'u8, 0x02, 0x01, 0x00]
    let cfg = TlsConfig(
      role: tlsServer,
      pkcs12Data: some(pkcs12),
      pkcs12Password: some("secret")
    )
    let binding = newTlsCredentialBinding(cfg)
    defer: binding.cleanup()
    let cfgPtr = binding.credentialConfigPtr()
    require cfgPtr != nil
    let cCfg = cfgPtr[]
    check cast[uint32](cCfg.Type) == cast[uint32](QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12)
    let pkcsPtr = cast[ptr QuicCertificatePkcs12](cCfg.Certificate)
    require pkcsPtr != nil
    check pkcsPtr.Asn1BlobLength == pkcs12.len.uint32
    check pkcsPtr.Asn1Blob != nil
    check pkcsPtr.PrivateKeyPassword != nil
    check $pkcsPtr.PrivateKeyPassword == "secret"

  test "credential flags propagate to MsQuic bitmask":
    let cfg = TlsConfig(
      role: tlsServer,
      alpns: @["h3"],
      transportParameters: @[],
      certificatePem: some(TestCertificate),
      privateKeyPem: some(TestPrivateKey),
      disableCertificateValidation: true,
      requireClientAuth: true,
      enableOcsp: true,
      indicateCertificateReceived: true,
      deferCertificateValidation: true,
      useBuiltinCertificateValidation: true,
      allowedCipherSuites: some(uint32(0x2)),
      caCertificateFile: some("ca-chain.pem")
    )
    let binding = newTlsCredentialBinding(cfg)
    defer: binding.cleanup()
    let cfgPtr = binding.credentialConfigPtr()
    require cfgPtr != nil
    let cCfg = cfgPtr[]
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_REQUIRE_CLIENT_AUTHENTICATION)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_ENABLE_OCSP)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_INDICATE_CERTIFICATE_RECEIVED)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_DEFER_CERTIFICATE_VALIDATION)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_USE_TLS_BUILTIN_CERTIFICATE_VALIDATION)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_SET_ALLOWED_CIPHER_SUITES)
    check includesFlag(cCfg.Flags, QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE)
    check cast[uint32](cCfg.AllowedCipherSuites) == 0x2'u32
    check $cCfg.CaCertificateFile == "ca-chain.pem"
