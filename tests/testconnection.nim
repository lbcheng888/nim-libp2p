{.used.}

# Nim-Libp2p
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import chronos
import std/strutils
import ../libp2p/[stream/connection, stream/bufferstream]
import ../libp2p/varint
import stew/byteutils

import ./helpers

type ScriptedLpStream = ref object of Connection
  payload: seq[byte]
  offset: int
  chunkSize: int
  readRequests: seq[int]
  maxRequested: int

proc lpFrame(payload: string): seq[byte] =
  let body = payload.toBytes()
  let prefix = PB.toBytes(body.len.uint64)
  result = newSeqOfCap[byte](prefix.len + body.len)
  for b in prefix:
    result.add(b)
  result.add(body)

method readOnce*(
    s: ScriptedLpStream, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError], raw: true).} =
  let fut = newFuture[int]()
  s.readRequests.add(nbytes)
  if nbytes > s.maxRequested:
    s.maxRequested = nbytes
  if s.offset >= s.payload.len:
    s.isEof = true
    fut.complete(0)
    return fut
  let toRead = min(s.payload.len - s.offset, min(nbytes, s.chunkSize))
  copyMem(pbytes, unsafeAddr s.payload[s.offset], toRead)
  s.offset += toRead
  if s.offset >= s.payload.len:
    s.isEof = true
  fut.complete(toRead)
  fut

method write*(
    s: ScriptedLpStream, msg: seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError], raw: true).} =
  let fut = newFuture[void]()
  fut.complete()
  fut

method close(s: ScriptedLpStream): Future[void] {.async: (raises: [], raw: true).} =
  s.isClosed = true
  s.isEof = true
  let fut = newFuture[void]()
  fut.complete()
  fut

proc newScriptedLpStream(payload: seq[byte], chunkSize = 5): ScriptedLpStream =
  new result
  result.payload = payload
  result.offset = 0
  result.chunkSize = chunkSize
  result.readRequests = @[]
  result.maxRequested = 0

suite "Connection":
  asyncTest "close":
    var conn = BufferStream.new()
    await conn.close()
    check:
      conn.closed == true

  asyncTest "parent close":
    var buf = BufferStream.new()
    var conn = buf

    await conn.close()
    check:
      conn.closed == true
      buf.closed == true

  asyncTest "child close":
    var buf = BufferStream.new()
    var conn = buf

    await buf.close()
    check:
      conn.closed == true
      buf.closed == true

  asyncTest "readLp caps per-read request size even when maxSize is huge":
    let payload = "x".repeat(2048)
    let conn = newScriptedLpStream(lpFrame(payload), chunkSize = 7)
    let data = await conn.readLp(2 * 1024 * 1024)
    check:
      string.fromBytes(data) == payload
      conn.readRequests.len > 0
      conn.readRequests[0] <= 64
      conn.maxRequested <= 16 * 1024

  asyncTest "readLp preserves coalesced replay across capped reads":
    let conn = newScriptedLpStream(
      lpFrame("hello") & lpFrame("world"),
      chunkSize = 3,
    )
    let first = await conn.readLp(2 * 1024 * 1024)
    let second = await conn.readLp(2 * 1024 * 1024)
    check:
      string.fromBytes(first) == "hello"
      string.fromBytes(second) == "world"
