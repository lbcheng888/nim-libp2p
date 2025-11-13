import std/monotimes
import std/strformat

import "../api/api_impl"
import "../api/param_catalog"

type
  BenchHandles = object
    apiPtr: pointer
    table: ptr QuicApiTable
    registration: HQUIC
    configuration: HQUIC
    connection: HQUIC

proc noopConnectionCallback(connectionHandle: HQUIC; context: pointer;
    event: pointer): QUIC_STATUS {.cdecl.} =
  discard connectionHandle
  discard context
  discard event
  QUIC_STATUS_SUCCESS

proc initBenchHandles(): BenchHandles =
  var ctx: BenchHandles
  doAssert MsQuicOpenVersion(2'u32, addr ctx.apiPtr) == QUIC_STATUS_SUCCESS
  ctx.table = cast[ptr QuicApiTable](ctx.apiPtr)

  var regConfig = QuicRegistrationConfigC(
    AppName: cstring("nim-msquic-hotpath-bench"),
    ExecutionProfile: QUIC_EXECUTION_PROFILE(0)
  )
  doAssert ctx.table.RegistrationOpen(addr regConfig, addr ctx.registration) == QUIC_STATUS_SUCCESS

  let alpnString = "hq-bench"
  var alpnBuffer = QuicBuffer(
    Length: uint32(alpnString.len),
    Buffer: cast[ptr uint8](alpnString.cstring)
  )
  doAssert ctx.table.ConfigurationOpen(ctx.registration, addr alpnBuffer, 1'u32,
    nil, 0'u32, nil, addr ctx.configuration) == QUIC_STATUS_SUCCESS

  doAssert ctx.table.ConnectionOpen(ctx.registration, noopConnectionCallback, nil,
    addr ctx.connection) == QUIC_STATUS_SUCCESS
  doAssert ctx.table.ConnectionSetConfiguration(ctx.connection, ctx.configuration) == QUIC_STATUS_SUCCESS

  ctx

proc cleanupBenchHandles(ctx: var BenchHandles) =
  if ctx.connection != nil:
    ctx.table.ConnectionClose(ctx.connection)
    ctx.connection = nil
  if ctx.configuration != nil:
    ctx.table.ConfigurationClose(ctx.configuration)
    ctx.configuration = nil
  if ctx.registration != nil:
    ctx.table.RegistrationClose(ctx.registration)
    ctx.registration = nil
  if ctx.apiPtr != nil:
    MsQuicClose(ctx.apiPtr)
    ctx.apiPtr = nil

proc runBench(name: string; iterations: int; body: proc () {.gcsafe.}) =
  let start = getMonoTime()
  for _ in 0 ..< iterations:
    body()
  let elapsed = getMonoTime() - start
  let totalNs = elapsed.inNanoseconds
  let msValue = totalNs.float64 / 1_000_000.0
  let nsPerOp = if iterations > 0: totalNs div iterations else: 0
  echo fmt"{name}: {iterations} 次, {msValue:.3f} ms, {nsPerOp} ns/次"

when isMainModule:
  var ctx = initBenchHandles()
  defer:
    cleanupBenchHandles(ctx)

  let setContextIters = 1_000_000
  let datagramIters = 500_000
  let closeReasonIters = 50_000

  var contextSlot: int64 = 0
  runBench("SetContext(表函数)", setContextIters, proc () {.gcsafe.} =
    ctx.table.SetContext(ctx.connection, addr contextSlot)
  )

  runBench("SetContext(C Shim)", setContextIters, proc () {.gcsafe.} =
    MsQuicSetContextShim(ctx.connection, addr contextSlot)
  )

  var toggleValue = BOOLEAN(0)
  runBench("SetParam(接收数据报)", datagramIters, proc () {.gcsafe.} =
    toggleValue = (if (toggleValue == 0'u8): BOOLEAN(1) else: BOOLEAN(0))
    discard ctx.table.SetParam(ctx.connection, QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED,
      1'u32, addr toggleValue)
  )

  toggleValue = BOOLEAN(0)
  runBench("C Shim(接收数据报)", datagramIters, proc () {.gcsafe.} =
    toggleValue = (if (toggleValue == 0'u8): BOOLEAN(1) else: BOOLEAN(0))
    discard MsQuicEnableDatagramReceiveShim(ctx.connection, toggleValue)
  )

  let reason = "benchmark-close-reason"
  let reasonLen = uint32(reason.len)
  let reasonPtr = cast[pointer](reason.cstring)
  runBench("SetParam(关闭原因)", closeReasonIters, proc () {.gcsafe.} =
    discard ctx.table.SetParam(ctx.connection, QUIC_PARAM_CONN_CLOSE_REASON_PHRASE,
      reasonLen, reasonPtr)
  )
