## 诊断 Hook 蓝图，用于收集 MsQuic API 层事件。

import std/sequtils

type
  DiagnosticsEventKind* = enum
    diagRegistrationOpened
    diagRegistrationShutdown
    diagConfigurationLoaded
    diagConnectionParamSet
    diagConnectionStarted
    diagConnectionEvent

  DiagnosticsEvent* = object
    kind*: DiagnosticsEventKind
    handle*: pointer
    paramId*: uint32
    note*: string

  DiagnosticsHook* = proc (event: DiagnosticsEvent) {.gcsafe.}

var gDiagnosticsHooks*: seq[DiagnosticsHook] = @[]

proc registerDiagnosticsHook*(hook: DiagnosticsHook) =
  if hook != nil:
    gDiagnosticsHooks.add(hook)

proc clearDiagnosticsHooks*() =
  gDiagnosticsHooks.setLen(0)

proc emitDiagnostics*(event: DiagnosticsEvent) =
  for hook in gDiagnosticsHooks:
    hook(event)
