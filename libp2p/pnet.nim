when defined(libp2p_pnet_disable):
  # Nim-LibP2P
  # Copyright (c) 2025 Status Research & Development GmbH
  # Licensed under either of
  #  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
  #  * MIT license ([LICENSE-MIT](LICENSE-MIT))
  # at your option.
  # This file may not be copied, modified, or distributed except according to
  # those terms.

  {.push raises: [].}

  import std/os
  import pkg/results
  import bearssl/rand

  import ./errors
  import ./stream/connection

  export results

  const
    SwarmKeyHeader* = "/key/swarm/psk/1.0.0/"
    EncodingBase16* = "/base16/"
    EncodingBase64* = "/base64/"
    EncodingBin* = "/bin/"
    ForceEnvKey = "LIBP2P_FORCE_PNET"

  type PrivateNetworkKey* = array[32, byte]
  type ConnectionProtector* = ref object

  var forceCacheComputed = false
  var forceCacheValue = false

  proc forcePrivateNetworkEnabled*(): bool =
    if not forceCacheComputed:
      forceCacheValue = getEnv(ForceEnvKey, "") == "1"
      forceCacheComputed = true
    forceCacheValue

  proc loadPrivateNetworkKey*(path: string): Result[PrivateNetworkKey, string] =
    err("pnet disabled at compile time (-d:libp2p_pnet_disable)")

  proc loadPrivateNetworkKeyFromString*(
      content: string
  ): Result[PrivateNetworkKey, string] =
    err("pnet disabled at compile time (-d:libp2p_pnet_disable)")

  proc newConnectionProtector*(
      key: PrivateNetworkKey, rng: ref HmacDrbgContext
  ): ConnectionProtector =
    discard key
    discard rng
    raise newException(
      LPError, "libp2p built without pnet support (-d:libp2p_pnet_disable)"
    )

  proc protect*(protector: ConnectionProtector, conn: Connection): Connection =
    if not protector.isNil:
      raise newException(
        LPError, "libp2p built without pnet support (-d:libp2p_pnet_disable)"
      )
    conn

else:
  # Nim-LibP2P
  # Copyright (c) 2025 Status Research & Development GmbH
  # Licensed under either of
  #  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
  #  * MIT license ([LICENSE-MIT](LICENSE-MIT))
  # at your option.
  # This file may not be copied, modified, or distributed except according to
  # those terms.

  {.push raises: [].}

  import std/[os, strutils, base64, streams]
  import pkg/[results, chronos, chronicles]
  import pkg/chronos/asyncsync
  import stew/[byteutils]
  import bearssl/rand

  import ./stream/[connection, lpstream]
  import ./crypto/[crypto, xsalsa20]

  export results

  const
    SwarmKeyHeader* = "/key/swarm/psk/1.0.0/"
    EncodingBase16* = "/base16/"
    EncodingBase64* = "/base64/"
    EncodingBin* = "/bin/"
    ForceEnvKey = "LIBP2P_FORCE_PNET"

  logScope:
    topics = "libp2p pnet"

  type
    PrivateNetworkKey* = array[32, byte]

    ConnectionProtector* = ref object
      key*: PrivateNetworkKey
      rng*: ref HmacDrbgContext
      keyId*: string

    ProtectedConnection = ref object of Connection
      base: Connection
      protector: ConnectionProtector
      readInitialized: bool
      writeInitialized: bool
      readStream: XSalsa20Stream
      writeStream: XSalsa20Stream
      readLock: AsyncLock
      writeLock: AsyncLock

  proc toPrivateKey(bytes: openArray[byte]): Result[PrivateNetworkKey, string] =
    if bytes.len != 32:
      return err("PSK must be exactly 32 bytes")

    var key: PrivateNetworkKey
    for i in 0 ..< 32:
      key[i] = bytes[i]
    ok(key)

  proc parseSwarmKey(stream: Stream): Result[PrivateNetworkKey, string] =
    if stream.isNil:
      return err("invalid stream")

    let header =
      try:
        stream.readLine()
      except CatchableError as exc:
        return err("failed reading swarm key header: " & exc.msg)

    if header != SwarmKeyHeader:
      return err("invalid swarm key header, expected " & SwarmKeyHeader)

    var encoding =
      try:
        stream.readLine()
      except CatchableError as exc:
        return err("failed reading swarm key encoding: " & exc.msg)

    encoding = encoding.strip()

    case encoding
    of EncodingBase16:
      let raw =
        try:
          stream.readAll()
        except CatchableError as exc:
          return err("failed reading base16 payload: " & exc.msg)
      let cleaned = raw.strip(chars = {' ', '\t', '\r', '\n'})
      try:
        let decoded = hexToSeqByte(cleaned)
        return toPrivateKey(decoded)
      except CatchableError:
        return err("invalid base16 PSK payload")
    of EncodingBase64:
      let raw =
        try:
          stream.readAll()
        except CatchableError as exc:
          return err("failed reading base64 payload: " & exc.msg)
      let cleaned = raw.strip(chars = {' ', '\t', '\r', '\n'})
      try:
        let decoded = base64.decode(cleaned)
        return toPrivateKey(decoded.toOpenArrayByte(0, decoded.high))
      except CatchableError:
        return err("invalid base64 PSK payload")
    of EncodingBin:
      var bytes = newSeqUninit[byte](32)
      let read =
        try:
          stream.readData(addr bytes[0], bytes.len)
        except CatchableError as exc:
          return err("failed reading binary PSK: " & exc.msg)
      if read != bytes.len:
        return err("binary PSK must be 32 bytes")
      return toPrivateKey(bytes)
    else:
      return err("unknown swarm key encoding: " & encoding)

  proc loadPrivateNetworkKey*(path: string): Result[PrivateNetworkKey, string] =
    var fs =
      try:
        openFileStream(path, fmRead)
      except CatchableError as exc:
        return err("unable to open PSK file: " & exc.msg)
    if fs.isNil:
      return err("unable to open PSK file: " & path)
    defer:
      try:
        fs.close()
      except CatchableError:
        discard
    parseSwarmKey(fs)

  proc loadPrivateNetworkKeyFromString*(
      content: string
  ): Result[PrivateNetworkKey, string] =
    var ss = newStringStream(content)
    defer:
      try:
        ss.close()
      except CatchableError:
        discard
    parseSwarmKey(ss)

  var forceCacheComputed = false
  var forceCacheValue = false

  proc forcePrivateNetworkEnabled*(): bool =
    if not forceCacheComputed:
      forceCacheValue = getEnv(ForceEnvKey, "") == "1"
      forceCacheComputed = true
    forceCacheValue

  proc privateKeyId(key: PrivateNetworkKey): string =
    let digest = byteutils.toHex(key)
    digest[0 ..< min(8, digest.len)]

  proc newConnectionProtector*(
      key: PrivateNetworkKey, rng: ref HmacDrbgContext
  ): ConnectionProtector =
    ConnectionProtector(key: key, rng: rng, keyId: privateKeyId(key))

  method getWrapped*(conn: ProtectedConnection): Connection =
    conn.base

  proc ensureReadInitialized(conn: ProtectedConnection) {.async: (raises: [CancelledError, LPStreamError]).} =
    if conn.readInitialized:
      return

    var nonce: array[24, byte]
    await conn.base.readExactly(addr nonce[0], nonce.len)
    conn.readStream = initXSalsa20Stream(conn.protector.key, nonce)
    conn.readInitialized = true
    trace "pnet read stream initialized",
      keyId = conn.protector.keyId, nonceId = privateKeyId(conn.protector.key)

  proc ensureWriteInitialized(conn: ProtectedConnection) {.async: (raises: [CancelledError, LPStreamError]).} =
    if conn.writeInitialized:
      return
    if conn.protector.rng.isNil:
      raise (ref LPStreamError)(msg: "pnet protector RNG not initialized")

    var nonce: array[24, byte]
    hmacDrbgGenerate(conn.protector.rng[], nonce.toOpenArray(0, nonce.high))
    await conn.base.write(@nonce)
    conn.writeStream = initXSalsa20Stream(conn.protector.key, nonce)
    conn.writeInitialized = true
    trace "pnet write stream initialized",
      keyId = conn.protector.keyId, nonceId = privateKeyId(conn.protector.key)

  method readOnce*(
      conn: ProtectedConnection, pbytes: pointer, nbytes: int
  ): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
    if nbytes == 0:
      return 0
    await conn.readLock.acquire()
    defer:
      try:
        conn.readLock.release()
      except AsyncLockError as exc:
        raiseAssert "pnet read lock must be held: " & exc.msg

    await conn.ensureReadInitialized()

    var tmp = newSeqUninit[byte](nbytes)
    let readBytes = await conn.base.readOnce(addr tmp[0], nbytes)
    if readBytes > 0:
      var decrypted = newSeqUninit[byte](readBytes)
      var encrypted = newSeq[byte](readBytes)
      copyMem(addr encrypted[0], addr tmp[0], readBytes)
      conn.readStream.xorKeyStream(decrypted, encrypted)
      let dst = cast[ptr UncheckedArray[byte]](pbytes)
      copyMem(addr dst[0], addr decrypted[0], readBytes)
      conn.activity = true
    readBytes

  method write*(
      conn: ProtectedConnection, msg: seq[byte]
  ): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
    if msg.len == 0:
      return
    await conn.writeLock.acquire()
    defer:
      try:
        conn.writeLock.release()
      except AsyncLockError as exc:
        raiseAssert "pnet write lock must be held: " & exc.msg

    await conn.ensureWriteInitialized()
    var encrypted = newSeq[byte](msg.len)
    conn.writeStream.xorKeyStream(encrypted, msg)
    await conn.base.write(encrypted)
    conn.activity = true

  method closeImpl*(conn: ProtectedConnection) {.async: (raises: []).} =
    if conn.base != nil:
      await conn.base.close()
    await procCall Connection(conn).closeImpl()

  proc newProtectedConnection(
      base: Connection, protector: ConnectionProtector
  ): ProtectedConnection =
    let pc = ProtectedConnection(
      base: base,
      protector: protector,
      peerId: base.peerId,
      observedAddr: base.observedAddr,
      localAddr: base.localAddr,
      timeout: base.timeout,
      timeoutHandler: base.timeoutHandler,
      dir: base.dir,
      transportDir: base.transportDir,
      readLock: newAsyncLock(),
      writeLock: newAsyncLock(),
      closeEvent: base.closeEvent,
    )
    pc.objName = "ProtectedConnection"
    pc.initStream()
    pc.closeEvent = base.closeEvent
    pc

  proc protect*(protector: ConnectionProtector, conn: Connection): Connection =
    if protector.isNil:
      return conn
    trace "applying pnet protector", conn, keyId = protector.keyId
    newProtectedConnection(conn, protector)
