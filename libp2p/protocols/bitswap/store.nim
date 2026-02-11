# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[tables, options, os]
import chronos, chronicles

import ../../cid

logScope:
  topics = "libp2p bitswap store"

type
  BitswapBlockStore* = ref object of RootObj
    ## Abstract base class used to persist Bitswap blocks locally.

  MemoryBlockStore* = ref object of BitswapBlockStore
    ## Simple in-memory implementation suitable for tests and single-node demos.
    blocks: Table[Cid, seq[byte]]
    lock: AsyncLock

  FileBlockStore* = ref object of BitswapBlockStore
    ## File-system backed Bitswap block store.
    root*: string
    lock: AsyncLock

proc releaseSafe(lock: AsyncLock) =
  try:
    lock.release()
  except AsyncLockError:
    discard

method getBlock*(
    store: BitswapBlockStore, cid: Cid
): Future[Option[seq[byte]]] {.base, async: (raises: [CancelledError]).} =
  raise newException(Defect, "BitswapBlockStore.getBlock not implemented")

method putBlock*(
    store: BitswapBlockStore, cid: Cid, data: seq[byte]
): Future[void] {.base, async: (raises: [CancelledError]).} =
  raise newException(Defect, "BitswapBlockStore.putBlock not implemented")

method hasBlock*(
    store: BitswapBlockStore, cid: Cid
): Future[bool] {.base, async: (raises: [CancelledError]).} =
  raise newException(Defect, "BitswapBlockStore.hasBlock not implemented")

proc new*(_: type MemoryBlockStore): MemoryBlockStore =
  MemoryBlockStore(
    blocks: initTable[Cid, seq[byte]](),
    lock: newAsyncLock(),
  )

proc new*(_: type FileBlockStore, root: string): FileBlockStore {.raises: [IOError].} =
  var path = root.strip()
  if path.len == 0:
    raise newException(IOError, "bitswap file store root directory must not be empty")
  path = expandFilename(path)
  try:
    createDir(path)
  except CatchableError as exc:
    raise newException(IOError, "failed to create bitswap file store directory: " & exc.msg)
  FileBlockStore(root: path, lock: newAsyncLock())

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc stringToBytes(data: string): seq[byte] =
  result = newSeq[byte](data.len)
  for i, c in data:
    result[i] = byte(c)

proc blockPath(store: FileBlockStore, cid: Cid): string =
  joinPath(store.root, $cid)

proc writeAtomic(path: string, data: seq[byte]) =
  let directory = splitFile(path).dir
  if directory.len > 0:
    try:
      createDir(directory)
    except CatchableError as exc:
      warn "failed to ensure block directory", dir = directory, error = exc.msg
      return
  let tmpPath = path & ".tmp"
  try:
    writeFile(tmpPath, bytesToString(data))
    if fileExists(path):
      try:
        removeFile(path)
      except CatchableError as exc:
        warn "failed to clean up existing bitswap block before rewrite",
          file = path, error = exc.msg
        return
    try:
      moveFile(tmpPath, path)
    except CatchableError as exc:
      warn "failed to move bitswap block into place", src = tmpPath, dest = path,
        error = exc.msg
      return
    except Exception as exc:
      warn "failed to move bitswap block into place", src = tmpPath, dest = path,
        error = exc.msg
      return
  except CatchableError as exc:
    warn "failed to persist bitswap block", file = path, error = exc.msg
    try:
      if fileExists(tmpPath):
        removeFile(tmpPath)
    except CatchableError:
      discard

method getBlock*(
    store: MemoryBlockStore, cid: Cid
): Future[Option[seq[byte]]] {.async: (raises: [CancelledError]).} =
  await store.lock.acquire()
  defer:
    releaseSafe(store.lock)
  store.blocks.withValue(cid, value):
    return some(value[])
  none(seq[byte])

method putBlock*(
    store: MemoryBlockStore, cid: Cid, data: seq[byte]
): Future[void] {.async: (raises: [CancelledError]).} =
  await store.lock.acquire()
  defer:
    releaseSafe(store.lock)
  store.blocks[cid] = data

method hasBlock*(
    store: MemoryBlockStore, cid: Cid
): Future[bool] {.async: (raises: [CancelledError]).} =
  await store.lock.acquire()
  defer:
    releaseSafe(store.lock)
  store.blocks.hasKey(cid)

method getBlock*(
    store: FileBlockStore, cid: Cid
): Future[Option[seq[byte]]] {.async: (raises: [CancelledError]).} =
  await store.lock.acquire()
  defer:
    releaseSafe(store.lock)
  let path = store.blockPath(cid)
  if not fileExists(path):
    return none(seq[byte])
  try:
    let contents = readFile(path)
    return some(stringToBytes(contents))
  except CatchableError as exc:
    warn "failed to read bitswap block", file = path, error = exc.msg
    none(seq[byte])

method putBlock*(
    store: FileBlockStore, cid: Cid, data: seq[byte]
): Future[void] {.async: (raises: [CancelledError]).} =
  await store.lock.acquire()
  defer:
    releaseSafe(store.lock)
  let path = store.blockPath(cid)
  writeAtomic(path, data)

method hasBlock*(
    store: FileBlockStore, cid: Cid
): Future[bool] {.async: (raises: [CancelledError]).} =
  await store.lock.acquire()
  defer:
    releaseSafe(store.lock)
  try:
    fileExists(store.blockPath(cid))
  except CatchableError as exc:
    warn "failed to check bitswap block existence", error = exc.msg
    false

{.pop.}
