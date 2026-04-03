import std/[sequtils, unittest]

import ../protocol/tls_core

suite "tls_core protocol helpers":
  test "client hello round-trips QUIC transport parameters":
    let params = defaultQuicTransportParameters(@[0xAA'u8, 0xBB'u8, 0xCC'u8, 0xDD'u8])
    let keyShare = generateKeyShare()
    let clientHello = encodeClientHello(params.initialSourceConnectionId, keyShare, params)
    let parsed = parseQuicTransportParameters(clientHello)
    check transportParametersEqual(parsed, params)

  test "client hello carries builtin resumption ticket":
    let params = defaultQuicTransportParameters(@[0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8])
    let keyShare = generateKeyShare()
    let ticket = @[0x10'u8, 0x20'u8, 0x30'u8, 0x40'u8]
    let clientHello = encodeClientHello(params.initialSourceConnectionId, keyShare, params, ticket)
    check extractClientResumptionTicket(clientHello) == ticket
    check clientHelloRequestsEarlyData(clientHello)
    let binder = extractClientResumptionBinder(clientHello)
    check binder.len == 32
    check verifyBuiltinResumptionBinder(clientHello, ticket, binder)

  test "builtin resumption binder rejects tamper":
    let params = defaultQuicTransportParameters(@[0x09'u8, 0x08'u8, 0x07'u8, 0x06'u8])
    let keyShare = generateKeyShare()
    let ticket = @[0x44'u8, 0x55'u8, 0x66'u8, 0x77'u8]
    let clientHello = encodeClientHello(params.initialSourceConnectionId, keyShare, params, ticket)
    var binder = extractClientResumptionBinder(clientHello)
    binder[0] = binder[0] xor 0x5A'u8
    check not verifyBuiltinResumptionBinder(clientHello, ticket, binder)

  test "server hello round-trips QUIC transport parameters":
    let params = defaultQuicTransportParameters(@[0x10'u8, 0x20'u8, 0x30'u8, 0x40'u8])
    let serverHello = encodeServerHello(newSeqWith(32, 0x11'u8), params)
    let parsed = parseQuicTransportParameters(serverHello)
    check transportParametersEqual(parsed, params)

  test "server hello signals builtin resumption acceptance":
    let params = defaultQuicTransportParameters(@[0x10'u8, 0x20'u8, 0x30'u8, 0x40'u8])
    let serverHello = encodeServerHello(newSeqWith(32, 0x11'u8), params, sessionResumed = true)
    check serverHelloSessionResumed(serverHello)
    check not serverHelloZeroRttAccepted(serverHello)

  test "server hello signals zero-rtt acceptance separately":
    let params = defaultQuicTransportParameters(@[0x22'u8, 0x33'u8, 0x44'u8, 0x55'u8])
    let serverHello = encodeServerHello(
      newSeqWith(32, 0x22'u8),
      params,
      sessionResumed = true,
      zeroRttAccepted = true
    )
    check serverHelloSessionResumed(serverHello)
    check serverHelloZeroRttAccepted(serverHello)

  test "new session ticket round-trips ticket payload":
    let wire = encodeNewSessionTicket(@[0xAA'u8, 0xBB'u8, 0xCC'u8], maxEarlyData = 32_768'u32)
    let parsed = parseNewSessionTicket(wire)
    check parsed.present
    check parsed.ticket == @[0xAA'u8, 0xBB'u8, 0xCC'u8]
    check parsed.maxEarlyData == 32_768'u32

  test "builtin zero-rtt material is stable across repeated derivation":
    let ticket = newSeqWith(64, 0x5A'u8)
    let first = deriveBuiltinZeroRttMaterial(ticket)
    check first.key.len == 16
    check first.iv.len == 12
    check first.hp.len == 16
    for _ in 0 ..< 256:
      let current = deriveBuiltinZeroRttMaterial(ticket)
      check current.key == first.key
      check current.iv == first.iv
      check current.hp == first.hp

  test "finished verify succeeds and tamper fails":
    var baseSecret = newSeq[byte](32)
    var transcriptHash = newSeq[byte](32)
    for idx in 0 ..< 32:
      baseSecret[idx] = byte((idx * 7) and 0xFF)
      transcriptHash[idx] = byte((idx * 13) and 0xFF)
    let finished = encodeFinished(baseSecret, transcriptHash)
    check verifyFinished(baseSecret, transcriptHash, finished)

    var tampered = finished
    tampered[^1] = tampered[^1] xor 0x5A'u8
    check not verifyFinished(baseSecret, transcriptHash, tampered)

  test "split handshake messages preserves order":
    let params = defaultQuicTransportParameters(@[0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8])
    let keyShare = generateKeyShare()
    let clientHello = encodeClientHello(params.initialSourceConnectionId, keyShare, params)
    let finished = encodeFinished(newSeqWith(32, 0x22'u8), newSeqWith(32, 0x33'u8))
    let messages = splitHandshakeMessages(clientHello & finished)
    check messages.len == 2
    check messages[0].msgType == 0x01'u8
    check messages[1].msgType == 0x14'u8
