import std/macros

const chronicles_enabled* {.booldefine.} = false

type
  LogLevel* = enum
    NONE,
    TRACE,
    DEBUG,
    INFO,
    NOTICE,
    WARN,
    ERROR,
    FATAL

type LogFormat* = enum
  json,
  textLines,
  textBlocks

const enabledLogLevel* =
  if chronicles_enabled:
    LogLevel.TRACE
  else:
    LogLevel.WARN

var globalLogLevel* = enabledLogLevel
template activeChroniclesStream*(): pointer = nil
when defined(android):
  proc android_log_write(prio: cint; tag: cstring; text: cstring): cint {.
      cdecl, importc: "__android_log_write", header: "<android/log.h>".}
  const
    AndroidLogVerbose = cint(2)
    AndroidLogDebug = cint(3)
    AndroidLogInfo = cint(4)
    AndroidLogWarn = cint(5)
    AndroidLogError = cint(6)
    AndroidLogFatal = cint(7)

when defined(ohos):
  when defined(nimlibp2p_no_hilog):
    proc OH_LOG_Print(
        logType: cint,
        level: cint,
        domain: cint,
        tag: cstring,
        fmt: cstring,
    ): cint {.cdecl, varargs.} =
      discard
  else:
    proc OH_LOG_Print(
        logType: cint,
        level: cint,
        domain: cint,
        tag: cstring,
        fmt: cstring,
    ): cint {.cdecl, importc: "OH_LOG_Print", header: "<hilog/log.h>", varargs.}
  const
    HILOG_TYPE_APP = cint(0)
    HILOG_LEVEL_DEBUG = cint(3)
    HILOG_LEVEL_INFO = cint(4)
    HILOG_LEVEL_WARN = cint(5)
    HILOG_LEVEL_ERROR = cint(6)
    HILOG_LEVEL_FATAL = cint(7)
    HILOG_DOMAIN = cint(0xD0B0)
  let HilogTag: cstring = "nimlibp2p"

proc levelEnabled(level: LogLevel): bool =
  result = ord(level) >= ord(globalLogLevel)

proc chroniclesPlatformLog(level: LogLevel, line: string) =
  when defined(android):
    let prio =
      case level
      of TRACE: AndroidLogDebug
      of DEBUG: AndroidLogDebug
      of INFO: AndroidLogInfo
      of NOTICE: AndroidLogInfo
      of WARN: AndroidLogWarn
      of ERROR: AndroidLogError
      of FATAL: AndroidLogFatal
      else: AndroidLogVerbose
    discard android_log_write(prio, "nim-libp2p", line.cstring)
    try:
      stderr.writeLine(line)
    except IOError:
      discard
  elif defined(ohos):
    let lvl =
      case level
      of TRACE, DEBUG: HILOG_LEVEL_DEBUG
      of INFO, NOTICE: HILOG_LEVEL_INFO
      of WARN: HILOG_LEVEL_WARN
      of ERROR: HILOG_LEVEL_ERROR
      of FATAL: HILOG_LEVEL_FATAL
      else: HILOG_LEVEL_DEBUG
    discard OH_LOG_Print(
      HILOG_TYPE_APP, lvl, HILOG_DOMAIN, HilogTag, "%{public}s", line.cstring
    )
    try:
      stderr.writeLine(line)
    except IOError:
      discard
  else:
    try:
      stderr.writeLine(line)
    except IOError:
      discard

template chroniclesToString(x: untyped): string =
  when compiles($x):
    $x
  else:
    repr(x)

template safeEvalToString(expr: untyped): string =
  ## Evaluate and stringify `expr` without leaking exceptions into callers that
  ## are annotated with `raises: []`.
  try:
    chroniclesToString(expr)
  except CatchableError:
    "<error>"

proc buildLogImpl(level: LogLevel, args: NimNode): NimNode =
  let levelLit = newLit(level)
  when defined(chroniclestub_debug_args):
    echo "[chronicles_stub] level=", level, " rawKind=", $args.kind, " rawLen=", $args.len, " rawRepr=", args.repr
    for i in 0 ..< args.len:
      echo "  raw[", i, "] kind=", $args[i].kind, " repr=", args[i].repr
  var argList = args
  # Some call paths (eg. templates forwarding varargs) can wrap the real
  # arguments in a single nnkArgList/nnkPar node. Flatten those wrappers.
  while argList.len == 1 and argList[0].kind in {nnkArgList, nnkPar}:
    argList = argList[0]

  # Some chronicles-style helpers can still nest the real args as the first
  # element; merge it into a single arg list.
  if argList.len > 0 and argList[0].kind in {nnkArgList, nnkPar}:
    var merged = newNimNode(nnkArgList)
    for j in 0 ..< argList[0].len:
      merged.add(argList[0][j])
    for i in 1 ..< argList.len:
      merged.add(argList[i])
    argList = merged

  when defined(chroniclestub_debug_args):
    echo "[chronicles_stub] normalized kind=", $argList.kind, " len=", $argList.len, " repr=", argList.repr
    for i in 0 ..< argList.len:
      echo "  norm[", i, "] kind=", $argList[i].kind, " repr=", argList[i].repr

  var msgIdx = -1
  for i in 0 ..< argList.len:
    if argList[i].kind != nnkExprEqExpr:
      msgIdx = i
      break

  when defined(chroniclestub_debug_args):
    echo "[chronicles_stub] msgIdx=", msgIdx

  var lineExpr: NimNode = quote do:
    "[" & $`levelLit` & "]"

  if msgIdx >= 0:
    let msgNode = argList[msgIdx]
    lineExpr = quote do:
      "[" & $`levelLit` & "] " & safeEvalToString(`msgNode`)

  when defined(chroniclestub_debug_args):
    echo "[chronicles_stub] lineExpr=", lineExpr.repr

  for i in 0 ..< argList.len:
    if i == msgIdx:
      continue
    let arg = argList[i]
    if arg.kind == nnkExprEqExpr and arg.len == 2:
      let keyStr = newLit(arg[0].repr)
      let valNode = arg[1]
      let valExpr = quote do: safeEvalToString(`valNode`)
      lineExpr = quote do: `lineExpr` & " " & `keyStr` & "=" & `valExpr`
    else:
      let extraNode = arg
      let extraExpr = quote do: safeEvalToString(`extraNode`)
      lineExpr = quote do: `lineExpr` & " " & `extraExpr`

  result = quote do:
    when nimvm:
      discard
    else:
      if levelEnabled(`levelLit`):
        chroniclesPlatformLog(`levelLit`, `lineExpr`)

macro log*(args: varargs[untyped]): untyped =
  buildLogImpl(INFO, args)

macro trace*(args: varargs[untyped]): untyped =
  buildLogImpl(TRACE, args)

macro debug*(args: varargs[untyped]): untyped =
  buildLogImpl(DEBUG, args)

macro info*(args: varargs[untyped]): untyped =
  buildLogImpl(INFO, args)

macro notice*(args: varargs[untyped]): untyped =
  buildLogImpl(NOTICE, args)

macro warn*(args: varargs[untyped]): untyped =
  buildLogImpl(WARN, args)

macro error*(args: varargs[untyped]): untyped =
  buildLogImpl(ERROR, args)

macro fatal*(args: varargs[untyped]): untyped =
  buildLogImpl(FATAL, args)

template logScope*(args: varargs[untyped]) =
  discard

template publicLogScope*(args: varargs[untyped]) =
  discard

template dynamicLogScope*(args: varargs[untyped]) =
  discard

template dynamicLogScope*(stream: typedesc, args: varargs[untyped]) =
  discard

proc setLogLevel*(name: string, level: LogLevel) =
  discard name
  if ord(level) < ord(enabledLogLevel):
    globalLogLevel = enabledLogLevel
  else:
    globalLogLevel = level

proc setLogEnabled*(name: string, enabled: bool) =
  discard name
  if enabled:
    globalLogLevel = enabledLogLevel
  else:
    globalLogLevel = FATAL

proc setTopicState*(name: string, state: bool) =
  discard name
  discard state

macro formatIt*(value, body: untyped): untyped =
  result = newStmtList()

macro expandIt*(args: varargs[untyped]): untyped =
  result = newStmtList()

proc isLogFormatUsed*(format: string): bool {.compileTime.} =
  discard format
  false

proc isLogFormatUsed*(format: LogFormat): bool {.compileTime.} =
  discard format
  false
