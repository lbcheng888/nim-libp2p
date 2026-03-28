{.push raises: [].}

import std/[base64, json, net, nativesockets, os, strutils, uri]
import nimcrypto/[blake2, hmac]
import nimcrypto/sysrand

import ../../crypto/[chacha20poly1305, curve25519]
import ../../utility

const
  NoiseProtocolName = "Noise_IK_25519_ChaChaPoly_BLAKE2s"
  NoiseProtocolVersionPrefix = "Tailscale Control Protocol v"
  ControlUpgradeValue = "tailscale-control-protocol"
  ControlHandshakeHeader = "X-Tailscale-Handshake"
  Http2ClientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
  Http2FrameData = 0'u8
  Http2FrameHeaders = 1'u8
  Http2FrameSettings = 4'u8
  Http2FramePing = 6'u8
  Http2FrameGoAway = 7'u8
  Http2FrameWindowUpdate = 8'u8
  Http2FrameContinuation = 9'u8
  Http2FlagAck = 0x1'u8
  Http2FlagEndStream = 0x1'u8
  Http2FlagEndHeaders = 0x4'u8
  Http2FlagPadded = 0x8'u8
  Http2FlagPriority = 0x20'u8
  NoiseMsgTypeInitiation = 1'u8
  NoiseMsgTypeResponse = 2'u8
  NoiseMsgTypeRecord = 4'u8
  NoiseHeaderLen = 3
  NoiseInitiationHeaderLen = 5
  NoiseInitPayloadLen = 96
  NoiseInitLen = NoiseInitiationHeaderLen + NoiseInitPayloadLen
  NoiseResponseLen = NoiseHeaderLen + 48
  NoiseMaxMessageSize = 4096
  NoiseCiphertextOverhead = 16
  NoiseMaxPlaintextSize = NoiseMaxMessageSize - NoiseHeaderLen - NoiseCiphertextOverhead
  CurvePrivatePrefix = "privkey:"
  MachinePublicPrefix = "mkey:"

type
  TsnetNoiseState = object
    key: ChaChaPolyKey
    nonce: uint64

  TsnetNoiseConn = ref object
    socket: Socket
    pending: seq[byte]
    plainPending: seq[byte]
    earlyPayloadHandled: bool
    tx: TsnetNoiseState
    rx: TsnetNoiseState

  TsnetHttp2Frame = object
    kind: uint8
    flags: uint8
    streamId: int
    payload: seq[byte]

proc ts2021DebugEnabled(): bool =
  getEnv("NIM_TSNET_TS2021_DEBUG").strip() in ["1", "true", "TRUE", "yes", "YES"]

proc ts2021Debug(message: string) =
  if ts2021DebugEnabled():
    try:
      stderr.writeLine("[ts2021] " & message)
    except IOError:
      discard

proc toBytes(text: string): seq[byte] =
  result = newSeqOfCap[byte](text.len)
  for ch in text:
    result.add(byte(ord(ch)))

proc toString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

proc hexPreview(bytes: openArray[byte], maxLen = 32): string =
  const digits = "0123456789ABCDEF"
  let limit = min(bytes.len, maxLen)
  result = newStringOfCap(limit * 2 + 16)
  for i in 0 ..< limit:
    let value = bytes[i]
    result.add(digits[int(value shr 4)])
    result.add(digits[int(value and 0x0F)])
  if bytes.len > maxLen:
    result.add("...")

proc appendBytes(dst: var seq[byte], src: openArray[byte]) =
  dst.setLen(dst.len + src.len)
  if src.len > 0:
    copyMem(addr dst[dst.len - src.len], unsafeAddr src[0], src.len)

proc sliceToSeq(bytes: openArray[byte]): seq[byte] =
  result = newSeq[byte](bytes.len)
  if bytes.len > 0:
    copyMem(addr result[0], unsafeAddr bytes[0], bytes.len)

proc be16(value: int): array[2, byte] =
  [byte((value shr 8) and 0xFF), byte(value and 0xFF)]

proc be24(value: int): array[3, byte] =
  [
    byte((value shr 16) and 0xFF),
    byte((value shr 8) and 0xFF),
    byte(value and 0xFF)
  ]

proc be31(value: int): array[4, byte] =
  [
    byte((value shr 24) and 0x7F),
    byte((value shr 16) and 0xFF),
    byte((value shr 8) and 0xFF),
    byte(value and 0xFF)
  ]

proc le32(value: int): array[4, byte] =
  [
    byte(value and 0xFF),
    byte((value shr 8) and 0xFF),
    byte((value shr 16) and 0xFF),
    byte((value shr 24) and 0xFF)
  ]

proc parseBe16(bytes: openArray[byte]): int =
  (int(bytes[0]) shl 8) or int(bytes[1])

proc parseBe24(bytes: openArray[byte]): int =
  (int(bytes[0]) shl 16) or (int(bytes[1]) shl 8) or int(bytes[2])

proc parseBe31(bytes: openArray[byte]): int =
  ((int(bytes[0]) and 0x7F) shl 24) or
    (int(bytes[1]) shl 16) or
    (int(bytes[2]) shl 8) or
    int(bytes[3])

proc parseBe32(bytes: openArray[byte]): int =
  (int(bytes[0]) shl 24) or
    (int(bytes[1]) shl 16) or
    (int(bytes[2]) shl 8) or
    int(bytes[3])

proc parseLe32(bytes: openArray[byte]): int =
  int(bytes[0]) or
    (int(bytes[1]) shl 8) or
    (int(bytes[2]) shl 16) or
    (int(bytes[3]) shl 24)

proc decodeHexNibble(ch: char): int =
  case ch
  of '0' .. '9':
    ord(ch) - ord('0')
  of 'a' .. 'f':
    ord(ch) - ord('a') + 10
  of 'A' .. 'F':
    ord(ch) - ord('A') + 10
  else:
    -1

proc decodeHexBytes(text: string, expectedBytes: int): Result[seq[byte], string] =
  let raw = text.strip()
  if raw.len != expectedBytes * 2:
    return err("invalid hex key length")
  var decoded = newSeq[byte](expectedBytes)
  for i in 0 ..< expectedBytes:
    let hi = decodeHexNibble(raw[i * 2])
    let lo = decodeHexNibble(raw[i * 2 + 1])
    if hi < 0 or lo < 0:
      return err("invalid hex key data")
    decoded[i] = byte((hi shl 4) or lo)
  ok(decoded)

proc parseCurvePrivate(text: string): Result[Curve25519Key, string] =
  let value = text.strip()
  if not value.startsWith(CurvePrivatePrefix):
    return err("missing typed tsnet private key")
  let decoded = decodeHexBytes(value.substr(CurvePrivatePrefix.len), Curve25519KeySize).valueOr:
    return err(error)
  ok(intoCurve25519Key(decoded))

proc parseMachinePublic(text: string): Result[Curve25519Key, string] =
  let value = text.strip()
  if not value.startsWith(MachinePublicPrefix):
    return err("missing typed tsnet machine public key")
  let decoded = decodeHexBytes(value.substr(MachinePublicPrefix.len), Curve25519KeySize).valueOr:
    return err(error)
  ok(intoCurve25519Key(decoded))

proc clampCurvePrivate(raw: var Curve25519Key) =
  raw[0] = raw[0] and 0xF8'u8
  raw[31] = raw[31] and 0x7F'u8
  raw[31] = raw[31] or 0x40'u8

proc randomCurvePrivate(): Result[Curve25519Key, string] =
  var raw: Curve25519Key
  if randomBytes(raw) != raw.len:
    return err("failed to generate ts2021 ephemeral key")
  clampCurvePrivate(raw)
  ok(raw)

proc finishBlake2s(ctx: var blake2_256): array[32, byte] =
  result = ctx.finish().data
  ctx.clear()

proc blake2sDigest(data: openArray[byte]): array[32, byte] =
  var ctx: blake2_256
  ctx.init()
  if data.len > 0:
    ctx.update(data)
  finishBlake2s(ctx)

proc blake2sDigest(a, b: openArray[byte]): array[32, byte] =
  var ctx: blake2_256
  ctx.init()
  if a.len > 0:
    ctx.update(a)
  if b.len > 0:
    ctx.update(b)
  finishBlake2s(ctx)

proc hmacBlake2s(key, data: openArray[byte]): array[32, byte] =
  var ctx: HMAC[blake2_256]
  ctx.init(key)
  if data.len > 0:
    ctx.update(data)
  result = ctx.finish().data
  ctx.clear()

proc hkdfExtractBlake2s(salt, ikm: openArray[byte]): array[32, byte] =
  hmacBlake2s(salt, ikm)

proc hkdfExpandBlake2s(
    prk, info: openArray[byte],
    outputLen: int
): Result[seq[byte], string] =
  if outputLen <= 0:
    return ok(newSeq[byte](0))
  let blocks = (outputLen + 31) div 32
  if blocks > 255:
    return err("hkdf expansion too large")
  var t: seq[byte] = @[]
  var expanded: seq[byte] = @[]
  for counter in 1 .. blocks:
    var msg: seq[byte] = @[]
    msg.appendBytes(t)
    msg.appendBytes(info)
    msg.add(byte(counter))
    let hkdfBlock = hmacBlake2s(prk, msg)
    t = @hkdfBlock
    expanded.appendBytes(hkdfBlock)
  expanded.setLen(outputLen)
  ok(expanded)

proc hkdfTwoBlake2s(
    chainingKey, ikm: openArray[byte]
): Result[(array[32, byte], array[32, byte]), string] =
  let prk = hkdfExtractBlake2s(chainingKey, ikm)
  let expanded = hkdfExpandBlake2s(prk, [], 64).valueOr:
    return err(error)
  var ck, k: array[32, byte]
  copyMem(addr ck[0], unsafeAddr expanded[0], 32)
  copyMem(addr k[0], unsafeAddr expanded[32], 32)
  ok((ck, k))

proc hkdfSplitBlake2s(chainingKey: openArray[byte]): Result[(array[32, byte], array[32, byte]), string] =
  let prk = hkdfExtractBlake2s(chainingKey, [])
  let expanded = hkdfExpandBlake2s(prk, [], 64).valueOr:
    return err(error)
  var k1, k2: array[32, byte]
  copyMem(addr k1[0], unsafeAddr expanded[0], 32)
  copyMem(addr k2[0], unsafeAddr expanded[32], 32)
  ok((k1, k2))

proc nonceBytes(counter: uint64): ChaChaPolyNonce =
  var raw: array[12, byte]
  raw[4] = byte((counter shr 56) and 0xFF)
  raw[5] = byte((counter shr 48) and 0xFF)
  raw[6] = byte((counter shr 40) and 0xFF)
  raw[7] = byte((counter shr 32) and 0xFF)
  raw[8] = byte((counter shr 24) and 0xFF)
  raw[9] = byte((counter shr 16) and 0xFF)
  raw[10] = byte((counter shr 8) and 0xFF)
  raw[11] = byte(counter and 0xFF)
  intoChaChaPolyNonce(raw)

proc singleUseEncrypt(
    key: array[32, byte],
    plaintext, aad: openArray[byte]
): seq[byte] =
  var data = @plaintext
  var tag: ChaChaPolyTag
  ChaChaPoly.encrypt(
    intoChaChaPolyKey(key),
    intoChaChaPolyNonce([byte 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    tag,
    data,
    aad
  )
  result = data
  result.appendBytes(tag)

proc singleUseDecrypt(
    key: array[32, byte],
    ciphertext, aad: openArray[byte]
): Result[seq[byte], string] =
  if ciphertext.len < NoiseCiphertextOverhead:
    return err("short handshake ciphertext")
  var data = sliceToSeq(ciphertext.toOpenArray(0, ciphertext.len - NoiseCiphertextOverhead - 1))
  var tag = intoChaChaPolyTag(
    ciphertext.toOpenArray(ciphertext.len - NoiseCiphertextOverhead, ciphertext.high)
  )
  ChaChaPoly.decrypt(
    intoChaChaPolyKey(key),
    intoChaChaPolyNonce([byte 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    tag,
    data,
    aad
  )
  if sliceToSeq(tag) != sliceToSeq(
      ciphertext.toOpenArray(ciphertext.len - NoiseCiphertextOverhead, ciphertext.high)
    ):
    return err("ts2021 handshake decrypt failed")
  ok(data)

proc curveDh(privateKey, publicKey: Curve25519Key): array[32, byte] =
  var point = publicKey
  Curve25519.mul(point, privateKey)
  point

proc writeAll(socket: Socket, data: openArray[byte]): Result[void, string] =
  if data.len == 0:
    return ok()
  try:
    ts2021Debug("writeAll bytes=" & $data.len)
    socket.send(toString(data))
  except CatchableError as exc:
    return err("socket write failed: " & exc.msg)
  except OSError as exc:
    return err("socket write failed: " & exc.msg & " (os=" & $exc.errorCode & ")")
  ok()

proc readSome(socket: Socket, maxBytes: int, timeoutMs = 15000): Result[seq[byte], string] =
  var fds = @[socket.getFd()]
  let ready =
    try:
      ts2021Debug("readSome maxBytes=" & $maxBytes & " timeoutMs=" & $timeoutMs)
      selectRead(fds, timeoutMs)
    except CatchableError as exc:
      return err("socket read failed: " & exc.msg)
  if ready <= 0:
    return err("socket read failed: Call to 'recv' timed out.")
  var chunk = newString(maxBytes)
  let read =
    try:
      socket.recv(chunk, maxBytes)
    except CatchableError as exc:
      return err("socket read failed: " & exc.msg)
  if read <= 0:
    return err("socket closed while reading")
  ts2021Debug("readSome got=" & $read)
  chunk.setLen(read)
  ok(toBytes(chunk))

proc readExact(socket: Socket, pending: var seq[byte], size: int): Result[seq[byte], string] =
  while pending.len < size:
    let chunk = readSome(socket, max(4096, size - pending.len)).valueOr:
      return err(error)
    pending.appendBytes(chunk)
  result = ok(sliceToSeq(pending.toOpenArray(0, size - 1)))
  if size >= pending.len:
    pending.setLen(0)
  else:
    pending = pending[size .. ^1]

proc readUntilHeaders(socket: Socket, pending: var seq[byte]): Result[seq[byte], string] =
  proc headerEndIndex(data: openArray[byte]): int =
    if data.len < 4:
      return -1
    for i in 0 .. data.len - 4:
      if data[i] == 13'u8 and data[i + 1] == 10'u8 and data[i + 2] == 13'u8 and data[i + 3] == 10'u8:
        return i + 4
    -1

  var idx = headerEndIndex(pending)
  while idx < 0:
    let chunk = readSome(socket, 4096).valueOr:
      return err(error)
    pending.appendBytes(chunk)
    idx = headerEndIndex(pending)
  result = ok(sliceToSeq(pending.toOpenArray(0, idx - 1)))
  if idx >= pending.len:
    pending.setLen(0)
  else:
    pending = pending[idx .. ^1]

proc parseHttpStatus(headers: openArray[byte]): Result[int, string] =
  let text = toString(headers)
  let lines = text.split("\r\n")
  if lines.len == 0:
    return err("missing HTTP response status line")
  let parts = lines[0].splitWhitespace()
  if parts.len < 2:
    return err("malformed HTTP response status line")
  try:
    ok(parseInt(parts[1]))
  except ValueError:
    err("invalid HTTP status code")

proc findHeader(headers: openArray[byte], key: string): string =
  let text = toString(headers)
  let wanted = key.toLowerAscii()
  for line in text.split("\r\n"):
    let pos = line.find(':')
    if pos <= 0:
      continue
    if line[0 ..< pos].strip().toLowerAscii() == wanted:
      return line[pos + 1 .. ^1].strip()
  ""

proc noisePrologue(version: int): seq[byte] =
  toBytes(NoiseProtocolVersionPrefix & $version)

proc buildHandshakeRequest(
    machinePrivateText, serverPublicText: string,
    protocolVersion: int
): Result[(seq[byte], Curve25519Key, array[32, byte], array[32, byte]), string] =
  let machinePrivate = parseCurvePrivate(machinePrivateText).valueOr:
    return err(error)
  let serverPublic = parseMachinePublic(serverPublicText).valueOr:
    return err(error)
  let machineEphemeral = randomCurvePrivate().valueOr:
    return err(error)

  var handshakeHash = blake2sDigest(toBytes(NoiseProtocolName))
  var chainingKey = handshakeHash
  handshakeHash = blake2sDigest(handshakeHash, noisePrologue(protocolVersion))
  handshakeHash = blake2sDigest(handshakeHash, serverPublic)

  let ephPublic = curve25519.public(machineEphemeral)
  handshakeHash = blake2sDigest(handshakeHash, ephPublic)

  let es = curveDh(machineEphemeral, serverPublic)
  let esMixed = hkdfTwoBlake2s(chainingKey, es).valueOr:
    return err(error)
  chainingKey = esMixed[0]
  let machinePublicCipher = singleUseEncrypt(esMixed[1], curve25519.public(machinePrivate), handshakeHash)
  handshakeHash = blake2sDigest(handshakeHash, machinePublicCipher)

  let ss = curveDh(machinePrivate, serverPublic)
  let ssMixed = hkdfTwoBlake2s(chainingKey, ss).valueOr:
    return err(error)
  chainingKey = ssMixed[0]
  let tag = singleUseEncrypt(ssMixed[1], [], handshakeHash)
  handshakeHash = blake2sDigest(handshakeHash, tag)

  var init = newSeq[byte](NoiseInitLen)
  let versionBytes = be16(protocolVersion)
  init[0] = versionBytes[0]
  init[1] = versionBytes[1]
  init[2] = NoiseMsgTypeInitiation
  let payloadLen = be16(NoiseInitPayloadLen)
  init[3] = payloadLen[0]
  init[4] = payloadLen[1]
  copyMem(addr init[NoiseInitiationHeaderLen], unsafeAddr ephPublic[0], 32)
  copyMem(addr init[NoiseInitiationHeaderLen + 32], unsafeAddr machinePublicCipher[0], machinePublicCipher.len)
  copyMem(addr init[NoiseInitiationHeaderLen + 32 + machinePublicCipher.len], unsafeAddr tag[0], tag.len)
  ok((init, machineEphemeral, chainingKey, handshakeHash))

proc continueHandshake(
    pending: var seq[byte],
    socket: Socket,
    machinePrivateText: string,
    machineEphemeral: Curve25519Key,
    serverPublicText: string,
    chainingKey, handshakeHash: array[32, byte]
): Result[TsnetNoiseConn, string] =
  ts2021Debug("continueHandshake begin")
  let machinePrivate = parseCurvePrivate(machinePrivateText).valueOr:
    return err(error)
  let serverPublic = parseMachinePublic(serverPublicText).valueOr:
    return err(error)
  let response = readExact(socket, pending, NoiseResponseLen).valueOr:
    return err(error)
  ts2021Debug("continueHandshake responseLen=" & $response.len)
  if response[0] != NoiseMsgTypeResponse:
    return err("unexpected ts2021 handshake response type " & $response[0])
  if parseBe16(response.toOpenArray(1, 2)) != 48:
    return err("unexpected ts2021 handshake response length")
  let controlEphemeral = intoCurve25519Key(response.toOpenArray(3, 34))
  let responseTag = sliceToSeq(response.toOpenArray(35, response.high))

  var h = handshakeHash
  var ck = chainingKey
  h = blake2sDigest(h, controlEphemeral)

  let ee = curveDh(machineEphemeral, controlEphemeral)
  let eeMixed = hkdfTwoBlake2s(ck, ee).valueOr:
    return err(error)
  ck = eeMixed[0]

  let se = curveDh(machinePrivate, controlEphemeral)
  let seMixed = hkdfTwoBlake2s(ck, se).valueOr:
    return err(error)
  ck = seMixed[0]
  discard singleUseDecrypt(seMixed[1], responseTag, h).valueOr:
    return err(error)
  h = blake2sDigest(h, responseTag)

  let split = hkdfSplitBlake2s(ck).valueOr:
    return err(error)
  ok(TsnetNoiseConn(
    socket: socket,
    pending: pending,
    plainPending: @[],
    earlyPayloadHandled: false,
    tx: TsnetNoiseState(key: intoChaChaPolyKey(split[0]), nonce: 0),
    rx: TsnetNoiseState(key: intoChaChaPolyKey(split[1]), nonce: 0)
  ))

proc close(conn: TsnetNoiseConn) =
  if conn.isNil or conn.socket.isNil:
    return
  try:
    conn.socket.close()
  except Exception:
    discard

proc writeNoise(conn: TsnetNoiseConn, payload: openArray[byte]): Result[void, string] =
  if conn.isNil:
    return err("ts2021 noise connection is nil")
  var offset = 0
  while offset < payload.len:
    let chunkLen = min(NoiseMaxPlaintextSize, payload.len - offset)
    var data = sliceToSeq(payload.toOpenArray(offset, offset + chunkLen - 1))
    var tag: ChaChaPolyTag
    ChaChaPoly.encrypt(conn.tx.key, nonceBytes(conn.tx.nonce), tag, data, [])
    inc conn.tx.nonce

    var frame: seq[byte] = @[]
    frame.add(NoiseMsgTypeRecord)
    frame.appendBytes(be16(data.len + tag.len))
    frame.appendBytes(data)
    frame.appendBytes(tag)
    ts2021Debug(
      "writeNoise chunkLen=" & $chunkLen &
      " frameLen=" & $frame.len &
      " nonce=" & $conn.tx.nonce
    )
    let sent = writeAll(conn.socket, frame)
    if sent.isErr():
      return err(sent.error)
    offset.inc(chunkLen)
  ok()

proc readNoiseFrame(conn: TsnetNoiseConn): Result[seq[byte], string] =
  if conn.isNil:
    return err("ts2021 noise connection is nil")
  let header = readExact(conn.socket, conn.pending, NoiseHeaderLen).valueOr:
    return err(error)
  if header[0] != NoiseMsgTypeRecord:
    return err("unexpected ts2021 record type " & $header[0])
  let frameLen = parseBe16(header.toOpenArray(1, 2))
  if frameLen < NoiseCiphertextOverhead or frameLen > NoiseMaxMessageSize - NoiseHeaderLen:
    return err("invalid ts2021 record length")
  let ciphertext = readExact(conn.socket, conn.pending, frameLen).valueOr:
    return err(error)
  var data = sliceToSeq(ciphertext.toOpenArray(0, ciphertext.len - NoiseCiphertextOverhead - 1))
  var tag = intoChaChaPolyTag(ciphertext.toOpenArray(ciphertext.len - NoiseCiphertextOverhead, ciphertext.high))
  let originalTag = sliceToSeq(ciphertext.toOpenArray(ciphertext.len - NoiseCiphertextOverhead, ciphertext.high))
  ChaChaPoly.decrypt(conn.rx.key, nonceBytes(conn.rx.nonce), tag, data, [])
  inc conn.rx.nonce
  if @tag != originalTag:
    return err("failed to decrypt ts2021 record")
  ok(data)

proc ensurePlainBytes(conn: TsnetNoiseConn, size: int): Result[void, string] =
  while conn.plainPending.len < size:
    let chunk = conn.readNoiseFrame().valueOr:
      let pendingInfo =
        if conn.plainPending.len > 0:
          " (plainPending=" & $conn.plainPending.len & " hex=" &
            hexPreview(conn.plainPending) & ")"
        else:
          ""
      return err(error & pendingInfo)
    conn.plainPending.appendBytes(chunk)
  ok()

proc drainPlainBytes(conn: TsnetNoiseConn, size: int): Result[seq[byte], string] =
  let ready = conn.ensurePlainBytes(size)
  if ready.isErr():
    return err(ready.error)
  result = ok(sliceToSeq(conn.plainPending.toOpenArray(0, size - 1)))
  if size >= conn.plainPending.len:
    conn.plainPending.setLen(0)
  else:
    conn.plainPending = conn.plainPending[size .. ^1]

proc skipEarlyPayload(conn: TsnetNoiseConn): Result[void, string] =
  if conn.earlyPayloadHandled:
    return ok()
  let ready = conn.ensurePlainBytes(9)
  if ready.isErr():
    return err(ready.error)
  if conn.plainPending.len >= 5 and
      conn.plainPending[0] == 0xFF'u8 and
      conn.plainPending[1] == 0xFF'u8 and
      conn.plainPending[2] == 0xFF'u8 and
      conn.plainPending[3] == byte(ord('T')) and
      conn.plainPending[4] == byte(ord('S')):
    let payloadLen = parseBe32(conn.plainPending.toOpenArray(5, 8))
    let fullLen = 9 + payloadLen
    let fullReady = conn.ensurePlainBytes(fullLen)
    if fullReady.isErr():
      return err(fullReady.error)
    if fullLen >= conn.plainPending.len:
      conn.plainPending.setLen(0)
    else:
      conn.plainPending = conn.plainPending[fullLen .. ^1]
  conn.earlyPayloadHandled = true
  ok()

proc writeHttp2Frame(
    conn: TsnetNoiseConn,
    kind, flags: uint8,
    streamId: int,
    payload: openArray[byte]
): Result[void, string] =
  var frame: seq[byte] = @[]
  frame.appendBytes(be24(payload.len))
  frame.add(kind)
  frame.add(flags)
  frame.appendBytes(be31(streamId))
  frame.appendBytes(payload)
  conn.writeNoise(frame)

proc writeHttp2WindowUpdate(
    conn: TsnetNoiseConn,
    streamId: int,
    increment: int
): Result[void, string] =
  if increment <= 0:
    return ok()
  conn.writeHttp2Frame(Http2FrameWindowUpdate, 0'u8, streamId, be31(increment))

proc readHttp2Frame(conn: TsnetNoiseConn): Result[TsnetHttp2Frame, string] =
  let early = conn.skipEarlyPayload()
  if early.isErr():
    return err(early.error)
  let header = conn.drainPlainBytes(9).valueOr:
    return err(error)
  let payloadLen = parseBe24(header.toOpenArray(0, 2))
  let payload =
    if payloadLen > 0:
      conn.drainPlainBytes(payloadLen).valueOr:
        return err(error)
    else:
      @[]
  ok(TsnetHttp2Frame(
    kind: header[3],
    flags: header[4],
    streamId: parseBe31(header.toOpenArray(5, 8)),
    payload: payload
  ))

proc encodeHpackInt(dst: var seq[byte], value, prefixBits: int, prefixMask: uint8) =
  let maxPrefix = (1 shl prefixBits) - 1
  if value < maxPrefix:
    dst.add(prefixMask or uint8(value))
    return
  dst.add(prefixMask or uint8(maxPrefix))
  var remaining = value - maxPrefix
  while remaining >= 128:
    dst.add(uint8((remaining and 0x7F) or 0x80))
    remaining = remaining shr 7
  dst.add(uint8(remaining))

proc encodeHpackString(dst: var seq[byte], value: string) =
  encodeHpackInt(dst, value.len, 7, 0x00)
  dst.appendBytes(toBytes(value))

proc encodeRequestHeaders(
    authority, scheme, path: string,
    bodyLen: int
): seq[byte] =
  result = @[]
  encodeHpackInt(result, 3, 7, 0x80) # :method POST
  encodeHpackInt(result, if scheme == "http": 6 else: 7, 7, 0x80) # :scheme
  encodeHpackInt(result, 1, 4, 0x00) # :authority name
  encodeHpackString(result, authority)
  encodeHpackInt(result, 4, 4, 0x00) # :path name
  encodeHpackString(result, path)
  encodeHpackInt(result, 28, 4, 0x00) # content-length
  encodeHpackString(result, $bodyLen)

proc openTs2021Conn(
    controlUrl: string,
    machinePrivateKey: string,
    controlPublicKey: string,
    capabilityVersion: int
): Result[TsnetNoiseConn, string] =
  ts2021Debug("openTs2021Conn controlUrl=" & controlUrl & " version=" & $capabilityVersion)
  let parsed = parseUri(controlUrl)
  let host = parsed.hostname.strip()
  if host.len == 0:
    return err("missing control host")
  let scheme = parsed.scheme.toLowerAscii()
  let port =
    if parsed.port.len > 0:
      parsed.port
    elif scheme == "http":
      "80"
    else:
      "443"
  let path = if parsed.path.len > 0: parsed.path else: "/"
  discard path

  let handshake = buildHandshakeRequest(
    machinePrivateText = machinePrivateKey,
    serverPublicText = controlPublicKey,
    protocolVersion = capabilityVersion
  ).valueOr:
    return err(error)
  ts2021Debug("buildHandshakeRequest ok initLen=" & $handshake[0].len)

  var socket: Socket
  try:
    socket = newSocket(buffered = false)
  except CatchableError as exc:
    return err("failed to create ts2021 control socket: " & exc.msg)
  try:
    socket.connect(host, Port(parseInt(port)))
  except CatchableError as exc:
    return err("failed to connect ts2021 control socket: " & exc.msg)
  ts2021Debug("socket connected host=" & host & " port=" & port)

  when defined(ssl):
    if scheme == "https":
      try:
        let ctx = newContext(verifyMode = CVerifyPeerUseEnvVars)
        ctx.wrapConnectedSocket(socket, handshakeAsClient, host)
      except Exception as exc:
        try:
          socket.close()
        except Exception:
          discard
        return err("failed to establish control TLS: " & exc.msg)
  else:
    if scheme == "https":
      try:
        socket.close()
      except Exception:
        discard
      return err("ssl_required_for_https_control")

  let authority =
    if parsed.port.len > 0: host & ":" & port
    else: host
  let requestText =
    "POST /ts2021 HTTP/1.1\r\n" &
    "Host: " & authority & "\r\n" &
    "Upgrade: " & ControlUpgradeValue & "\r\n" &
    "Connection: upgrade\r\n" &
    ControlHandshakeHeader & ": " & base64.encode(toString(handshake[0])) & "\r\n" &
    "Content-Length: 0\r\n\r\n"
  let writeReq = writeAll(socket, toBytes(requestText))
  if writeReq.isErr():
    try:
      socket.close()
    except Exception:
      discard
    return err(writeReq.error)

  var pending: seq[byte] = @[]
  let headers = readUntilHeaders(socket, pending).valueOr:
    try:
      socket.close()
    except Exception:
      discard
    return err("failed to read ts2021 upgrade response headers: " & error)
  ts2021Debug("upgrade response headers=" & toString(headers).replace("\r\n", " | "))
  let status = parseHttpStatus(headers).valueOr:
    try:
      socket.close()
    except Exception:
      discard
    return err(error)
  if status != 101:
    let message = toString(headers).strip()
    try:
      socket.close()
    except Exception:
      discard
    return err("unexpected ts2021 upgrade response: " & $status & " " & message)
  if findHeader(headers, "Upgrade").toLowerAscii() != ControlUpgradeValue:
    try:
      socket.close()
    except Exception:
      discard
    return err("control server switched to unexpected upgrade protocol")

  let established = continueHandshake(
    pending = pending,
    socket = socket,
    machinePrivateText = machinePrivateKey,
    machineEphemeral = handshake[1],
    serverPublicText = controlPublicKey,
    chainingKey = handshake[2],
    handshakeHash = handshake[3]
  ).valueOr:
    try:
      socket.close()
    except Exception:
      discard
    return err("failed to complete ts2021 control handshake: " & error)
  ts2021Debug("openTs2021Conn handshake complete")
  ok(established)

proc executeHttp2JsonPost(
    controlUrl: string,
    machinePrivateKey: string,
    controlPublicKey: string,
    capabilityVersion: int,
    path: string,
    body: JsonNode,
    stopAfterLengthPrefixedMessage = false
): Result[seq[byte], string] =
  ts2021Debug("executeHttp2JsonPost path=" & path)
  let parsed = parseUri(controlUrl)
  let scheme = parsed.scheme.toLowerAscii()
  let authority =
    if parsed.port.len > 0: parsed.hostname & ":" & parsed.port
    else: parsed.hostname
  let conn = openTs2021Conn(controlUrl, machinePrivateKey, controlPublicKey, capabilityVersion).valueOr:
    return err(error)
  defer:
    conn.close()

  let prefaceWrite = conn.writeNoise(toBytes(Http2ClientPreface))
  if prefaceWrite.isErr():
    return err(prefaceWrite.error)
  ts2021Debug("client preface sent")
  let settingsWrite = conn.writeHttp2Frame(Http2FrameSettings, 0'u8, 0, [])
  if settingsWrite.isErr():
    return err(settingsWrite.error)
  ts2021Debug("client settings sent")

  let payloadText = $body
  let payloadBytes = toBytes(payloadText)
  let headerBlock = encodeRequestHeaders(authority, scheme, path, payloadBytes.len)
  let headersWrite = conn.writeHttp2Frame(
    Http2FrameHeaders,
    Http2FlagEndHeaders,
    1,
    headerBlock
  )
  if headersWrite.isErr():
    return err(headersWrite.error)
  ts2021Debug("request headers sent stream=1")
  let dataWrite = conn.writeHttp2Frame(
    Http2FrameData,
    Http2FlagEndStream,
    1,
    payloadBytes
  )
  if dataWrite.isErr():
    return err(dataWrite.error)
  ts2021Debug("request body sent stream=1 bytes=" & $payloadBytes.len)

  var bodyBytes: seq[byte] = @[]
  while true:
    let frame = conn.readHttp2Frame().valueOr:
      return err(error)
    ts2021Debug(
      "read frame kind=" & $frame.kind &
      " flags=" & $frame.flags &
      " stream=" & $frame.streamId &
      " payload=" & $frame.payload.len
    )
    case frame.kind
    of Http2FrameSettings:
      if (frame.flags and Http2FlagAck) == 0:
        let ack = conn.writeHttp2Frame(Http2FrameSettings, Http2FlagAck, 0, [])
        if ack.isErr():
          return err(ack.error)
    of Http2FramePing:
      if (frame.flags and Http2FlagAck) == 0:
        let ack = conn.writeHttp2Frame(Http2FramePing, Http2FlagAck, 0, frame.payload)
        if ack.isErr():
          return err(ack.error)
    of Http2FrameHeaders, Http2FrameContinuation:
      if frame.streamId == 1 and (frame.flags and Http2FlagEndStream) != 0:
        break
    of Http2FrameData:
      if frame.streamId == 1:
        bodyBytes.appendBytes(frame.payload)
        if frame.payload.len > 0:
          let connUpdate = conn.writeHttp2WindowUpdate(0, frame.payload.len)
          if connUpdate.isErr():
            return err(connUpdate.error)
          let streamUpdate = conn.writeHttp2WindowUpdate(1, frame.payload.len)
          if streamUpdate.isErr():
            return err(streamUpdate.error)
        if stopAfterLengthPrefixedMessage and bodyBytes.len >= 4:
          ts2021Debug(
            "stream body prefix len=" & $bodyBytes.len &
            " first16=" & hexPreview(bodyBytes, 16) &
            " declared=" & $parseLe32(bodyBytes.toOpenArray(0, 3))
          )
        if stopAfterLengthPrefixedMessage and bodyBytes.len >= 4:
          let messageLen = parseLe32(bodyBytes.toOpenArray(0, 3))
          if bodyBytes.len >= 4 + messageLen:
            break
        if (frame.flags and Http2FlagEndStream) != 0:
          break
    of Http2FrameWindowUpdate:
      discard
    of Http2FrameGoAway:
      if bodyBytes.len > 0:
        break
      return err("control server sent GOAWAY before a response body arrived")
    else:
      discard
  ok(bodyBytes)

proc transportMetaString(node: JsonNode, key: string): string =
  if node.isNil or node.kind != JObject:
    return ""
  let value = node{key}
  if value.isNil or value.kind != JString:
    return ""
  value.getStr().strip()

proc sanitizedRequest(node: JsonNode): JsonNode =
  result =
    try:
      parseJson($node)
    except CatchableError:
      node
  if result.kind == JObject:
    for key in ["_tsnetMachinePrivateKey", "_tsnetControlPublicKey"]:
      if result.hasKey(key):
        result[key] = newJNull()

proc transportCapabilityVersion(node: JsonNode): int =
  if node.isNil or node.kind != JObject:
    return 130
  let versionNode = node{"Version"}
  if versionNode.isNil:
    return 130
  case versionNode.kind
  of JInt:
    versionNode.getBiggestInt().int
  of JFloat:
    versionNode.getFloat().int
  else:
    130

proc transportMachinePrivate(node: JsonNode): Result[string, string] =
  let value = transportMetaString(node, "_tsnetMachinePrivateKey")
  if value.len == 0:
    return err("missing ts2021 machine private key metadata")
  ok(value)

proc transportControlPublic(node: JsonNode): Result[string, string] =
  let value = transportMetaString(node, "_tsnetControlPublicKey")
  if value.len == 0:
    return err("missing ts2021 control public key metadata")
  ok(value)

proc registerNodeTs2021*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] =
  let machinePrivate = transportMachinePrivate(request).valueOr:
    return err(error)
  let controlPublic = transportControlPublic(request).valueOr:
    return err(error)
  let sanitized = sanitizedRequest(request)
  let body = executeHttp2JsonPost(
    controlUrl,
    machinePrivate,
    controlPublic,
    transportCapabilityVersion(sanitized),
    "/machine/register",
    sanitized
  ).valueOr:
    return err(error)
  try:
    ok(parseJson(toString(body)))
  except CatchableError as exc:
    err("failed to parse ts2021 register response: " & exc.msg)

proc mapPollTs2021*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] =
  let machinePrivate = transportMachinePrivate(request).valueOr:
    return err(error)
  let controlPublic = transportControlPublic(request).valueOr:
    return err(error)
  let sanitized = sanitizedRequest(request)
  if sanitized.kind == JObject:
    sanitized["Stream"] = %true
    sanitized["KeepAlive"] = %false
    sanitized["Compress"] = %""
  let body = executeHttp2JsonPost(
    controlUrl,
    machinePrivate,
    controlPublic,
    transportCapabilityVersion(sanitized),
    "/machine/map",
    sanitized,
    stopAfterLengthPrefixedMessage = true
  ).valueOr:
    return err(error)
  if body.len < 4:
    return err("short ts2021 map response body")
  let messageLen = parseLe32(body.toOpenArray(0, 3))
  if body.len < 4 + messageLen:
    return err("truncated ts2021 map response payload")
  try:
    ok(parseJson(toString(sliceToSeq(body.toOpenArray(4, 4 + messageLen - 1)))))
  except CatchableError as exc:
    err("failed to parse ts2021 map response: " & exc.msg)
