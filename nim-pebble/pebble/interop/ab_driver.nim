# Go ↔ Nim A/B test driver scaffold.
# Executes paired binaries with the same payload and captures divergences for
# downstream analysis.

import std/[json, os, osproc, sequtils, streams, strformat, strutils, times]

type
  Engine* = enum
    engineGo,
    engineNim

  ABInvocation* = object
    engine*: Engine
    command*: string
    args*: seq[string]

  ABOutcome* = object
    engine*: Engine
    exitCode*: int
    stdout*: string
    stderr*: string
    duration*: Duration

  ABMismatch* = object
    field*: string
    goValue*: string
    nimValue*: string

  ABReport* = object
    success*: bool
    goOutcome*: ABOutcome
    nimOutcome*: ABOutcome
    mismatches*: seq[ABMismatch]

  OutputComparator* = proc (goOutput, nimOutput: string): seq[ABMismatch] {.gcsafe.}

  ABDriver* = ref object
    goInvocation*: ABInvocation
    nimInvocation*: ABInvocation
    comparator*: OutputComparator

proc defaultComparator(goOutput, nimOutput: string): seq[ABMismatch] =
  if goOutput == nimOutput:
    @[]
  else:
    @[ABMismatch(field: "stdout", goValue: goOutput, nimValue: nimOutput)]

proc newABDriver*(goCmd, nimCmd: string;
                  goArgs: seq[string] = @[];
                  nimArgs: seq[string] = @[];
                  comparator: OutputComparator = defaultComparator): ABDriver =
  ABDriver(
    goInvocation: ABInvocation(engine: engineGo, command: goCmd, args: goArgs),
    nimInvocation: ABInvocation(engine: engineNim, command: nimCmd, args: nimArgs),
    comparator: comparator
  )

proc run(invocation: ABInvocation; payload: string): ABOutcome =
  let startTime = now()
  var process = startProcess(invocation.command, args = invocation.args,
                             options = {poUsePath})
  process.inputStream.writeData(payload.cstring, payload.len)
  process.inputStream.flush()
  process.inputStream.close()
  let stdoutData = process.outputStream.readAll()
  let stderrData = process.errorStream.readAll()
  let exitCode = process.waitForExit()
  process.close()
  ABOutcome(engine: invocation.engine,
            exitCode: exitCode,
            stdout: stdoutData,
            stderr: stderrData,
            duration: now() - startTime)

proc execute*(driver: ABDriver; payload: string): ABReport =
  let goOutcome = run(driver.goInvocation, payload)
  let nimOutcome = run(driver.nimInvocation, payload)
  var mismatches: seq[ABMismatch] = @[]
  if goOutcome.exitCode != nimOutcome.exitCode:
    mismatches.add(ABMismatch(field: "exitCode",
                              goValue: $goOutcome.exitCode,
                              nimValue: $nimOutcome.exitCode))
  if goOutcome.stderr != nimOutcome.stderr:
    mismatches.add(ABMismatch(field: "stderr",
                              goValue: goOutcome.stderr,
                              nimValue: nimOutcome.stderr))
  mismatches.add(driver.comparator(goOutcome.stdout, nimOutcome.stdout))
  ABReport(
    success: mismatches.len == 0,
    goOutcome: goOutcome,
    nimOutcome: nimOutcome,
    mismatches: mismatches
  )

proc toJson*(report: ABReport): JsonNode =
  result = %*{
    "success": report.success,
    "go": {
      "exitCode": report.goOutcome.exitCode,
      "durationMs": report.goOutcome.duration.inMilliseconds(),
      "stderr": report.goOutcome.stderr,
      "stdout": report.goOutcome.stdout
    },
    "nim": {
      "exitCode": report.nimOutcome.exitCode,
      "durationMs": report.nimOutcome.duration.inMilliseconds(),
      "stderr": report.nimOutcome.stderr,
      "stdout": report.nimOutcome.stdout
    },
    "mismatches": report.mismatches.mapIt(%*{
      "field": it.field,
      "go": it.goValue,
      "nim": it.nimValue
    })
  }

proc summary*(report: ABReport): string =
  if report.success:
    "A/B check passed"
  else:
    "A/B mismatch detected in fields: " &
      report.mismatches.mapIt(it.field).join(", ")

when isMainModule:
  if paramCount() < 2:
    quit("Usage: ab_driver <payloadFile> <goCmd> <nimCmd>", QuitFailure)
  let payload = readFile(paramStr(1))
  let goCmd = paramStr(2)
  let nimCmd = paramStr(3)
  let driver = newABDriver(goCmd, nimCmd)
  let report = driver.execute(payload)
  echo report.toJson().pretty()
  if not report.success:
    quit(1)
