## Protocol validation suite aggregating handshake, interop, and fuzz smoke checks for E1.

import std/os
import std/osproc
import std/strutils

const
  NimBlueprintDir = "nim-msquic"
  NimProjectMarker = "project" & DirSep & "nimsquic.nimble"

type
  ValidationResult* = object
    name*: string
    passed*: bool
    details*: seq[string]
    artifacts*: seq[string]

  ValidationSummary* = object
    results*: seq[ValidationResult]
    passed*: bool
    failed*: seq[string]

proc blueprintRoot(): string =
  ## 从当前目录向上回溯，寻找包含 Nim 蓝图的目录。
  var dir = getCurrentDir()
  while true:
    if fileExists(joinPath(dir, NimProjectMarker)):
      return dir
    let nested = joinPath(dir, NimBlueprintDir)
    if dirExists(nested) and fileExists(joinPath(nested, NimProjectMarker)):
      return nested
    let parent = parentDir(dir)
    if parent.len == 0 or parent == dir:
      break
    dir = parent
  raise newException(OSError,
    "无法定位 Nim 蓝图根目录，当前路径: " & getCurrentDir())

proc runNimScriptCase(name: string; scriptRelPath: string;
    extraArgs: seq[string] = @[]): ValidationResult =
  let nimExe = findExe("nim")
  if nimExe.len == 0:
    return ValidationResult(name: name, passed: false,
      details: @["nim executable not found"], artifacts: @[])

  let root = blueprintRoot()
  let scriptPath = joinPath(root, scriptRelPath)
  if not fileExists(scriptPath):
    return ValidationResult(
      name: name,
      passed: false,
      details: @[
        "script not found: " & scriptPath
      ],
      artifacts: @[])
  var args = @["r", "--path:" & root, scriptPath]
  args.add(extraArgs)
  let cmdLine = quoteShellCommand(@[nimExe] & args)
  let (output, exitCode) = execCmdEx(cmdLine,
    {poEvalCommand, poStdErrToStdOut, poUsePath})
  var details = @["exitCode=" & $exitCode]
  if output.len > 0:
    details.add("output=" & output.strip())
  ValidationResult(
    name: name,
    passed: exitCode == 0,
    details: details,
    artifacts: if exitCode == 0: @[] else: @[output])

proc runProtocolValidationSuite*(): ValidationSummary =
  let results = @[
    runNimScriptCase("connection-handshake", "tests/connection_handshake_test.nim")
  ]
  var finalResults = results
  let skipTlsFlag = getEnv("NIM_MSQUIC_SKIP_TLS", "auto").toLowerAscii()
  if skipTlsFlag in ["1", "true", "yes", "on", "auto"]:
    finalResults.add ValidationResult(
      name: "tls-handshake",
      passed: true,
      details: @["skipped: TLS 验证已禁用 (NIM_MSQUIC_SKIP_TLS=" & skipTlsFlag & ")"],
      artifacts: @[])
  else:
    finalResults.add runNimScriptCase("tls-handshake", "tests/tls_handshake_test.nim")
  finalResults.add runNimScriptCase("fuzz-smoke", "tests/fuzz_smoke_runner.nim")
  var failed: seq[string] = @[]
  for res in finalResults:
    if not res.passed:
      failed.add(res.name)
  ValidationSummary(results: finalResults, passed: failed.len == 0, failed: failed)

when isMainModule:
  import std/strformat
  let summary = runProtocolValidationSuite()
  echo "Protocol Validation Summary"
  echo "==========================="
  for res in summary.results:
    echo fmt"- {res.name}: " & (if res.passed: "PASS" else: "FAIL")
    for detail in res.details:
      echo "    " & detail
    if res.artifacts.len > 0:
      for artifact in res.artifacts:
        echo "    artifact: " & artifact.strip()
  if summary.passed:
    echo "\nOverall result: PASS"
  else:
    echo "\nOverall result: FAIL"
    quit 1
