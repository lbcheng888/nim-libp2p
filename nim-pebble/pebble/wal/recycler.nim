# WAL 段文件回收池，支撑重用与尾部裁剪策略。

import std/[locks, options, sequtils]

type
  RecyclableFile* = object
    path*: string
    size*: int64

  LogRecycler* = ref object
    maxPool*: int
    pool*: seq[RecyclableFile]
    mutex: Lock

proc initLogRecycler*(maxPool = 8): LogRecycler =
  ## 创建一个线程安全的 WAL 文件回收池。
  new(result)
  result.maxPool = max(0, maxPool)
  result.pool = @[]
  initLock(result.mutex)

proc len*(recycler: LogRecycler): int =
  if recycler.isNil():
    return 0
  recycler.pool.len

proc acquire*(recycler: LogRecycler): Option[RecyclableFile] =
  ## 从回收池中取出一个文件，若无则返回 none。
  if recycler.isNil() or recycler.maxPool == 0:
    return none(RecyclableFile)
  recycler.mutex.acquire()
  defer: recycler.mutex.release()
  if recycler.pool.len == 0:
    return none(RecyclableFile)
  let file = recycler.pool[^1]
  result = some(file)
  recycler.pool.setLen(recycler.pool.len - 1)

proc recycle*(recycler: LogRecycler; file: RecyclableFile) =
  ## 将文件重新放入回收池，多余的文件会被丢弃。
  if recycler.isNil() or recycler.maxPool == 0:
    return
  recycler.mutex.acquire()
  defer: recycler.mutex.release()
  if recycler.pool.len >= recycler.maxPool:
    # 丢弃最旧的一个，保持容量上限。
    recycler.pool.delete(0)
  recycler.pool.add(file)

proc clear*(recycler: LogRecycler) =
  if recycler.isNil():
    return
  recycler.mutex.acquire()
  recycler.pool.setLen(0)
  recycler.mutex.release()
