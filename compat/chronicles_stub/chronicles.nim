import std/macros

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

const enabledLogLevel* = LogLevel.TRACE

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

proc chroniclesReport(level: LogLevel) =
  if not levelEnabled(level):
    return
  chroniclesPlatformLog(level, "[" & $level & "]")

macro logImpl(level: static[LogLevel], args: varargs[untyped]): untyped =
  let levelLit = newLit(level)
  result = quote do:
    when nimvm:
      discard
    else:
      chroniclesReport(`levelLit`)

template log*(args: varargs[untyped]) =
  logImpl(INFO, args)

template trace*(args: varargs[untyped]) =
  logImpl(TRACE, args)

template debug*(args: varargs[untyped]) =
  logImpl(DEBUG, args)

template info*(args: varargs[untyped]) =
  logImpl(INFO, args)

template notice*(args: varargs[untyped]) =
  logImpl(NOTICE, args)

template warn*(args: varargs[untyped]) =
  logImpl(WARN, args)

template error*(args: varargs[untyped]) =
  logImpl(ERROR, args)

template fatal*(args: varargs[untyped]) =
  logImpl(FATAL, args)

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
  globalLogLevel = level

proc setLogLevel*(level: LogLevel) =
  globalLogLevel = level
  globalLogLevel = level

proc setLogEnabled*(name: string, enabled: bool) =
  discard name
  if enabled:
    globalLogLevel = TRACE
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
