{.used.}

import std/[json, os, parseopt, sequtils, strutils]
from std/times import epochTime

const
  harnessPath = "/Users/lbcheng/nim-libp2p/examples/mobile_ffi/game_mesh_host_harness.nim"

type
  AppSpec = object
    appId: string
    timeoutMs: int

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc suiteOutDir(baseDir: string): string =
  if baseDir.len > 0:
    createDir(baseDir)
    return baseDir
  result = getCurrentDir() / "build" / "game-mesh-host" / ("suite-" & $nowMillis())
  createDir(result)

proc runHarness(spec: AppSpec, baseOutDir: string): JsonNode =
  proc compileFlags(): seq[string] =
    if getEnv("HOST_HARNESS_TRANSPORT", "tcp").toLowerAscii() == "quic":
      @["-d:libp2p_msquic_experimental", "-d:libp2p_msquic_builtin"]
    else:
      @[]
  let appOutDir = baseOutDir / spec.appId
  createDir(appOutDir)
  let nimCacheDir = appOutDir / "nimcache"
  createDir(nimCacheDir)
  let logPath = appOutDir / (spec.appId & ".log")
  let transport = getEnv("HOST_HARNESS_TRANSPORT", "tcp")
  let cmd = (@[
    "nim",
    "c",
  ] & compileFlags() & @[
    "--nimcache:" & nimCacheDir,
    "-r",
    harnessPath,
    "--app:" & spec.appId,
    "--transport:" & transport,
    "--timeout-ms:" & $spec.timeoutMs,
    "--out-dir:" & appOutDir,
    ">",
    logPath,
    "2>&1"
  ]).join(" ")
  let exitCode = execShellCmd(cmd)
  let summaryPath = appOutDir / spec.appId / (spec.appId & ".summary.json")
  let logText = if fileExists(logPath): readFile(logPath) else: ""
  var summary =
    if fileExists(summaryPath):
      parseJson(readFile(summaryPath))
    else:
      %*{
        "ok": false,
        "appId": spec.appId,
        "error": "missing_summary"
      }
  if summary.kind != JObject:
    summary = %*{
      "ok": false,
      "appId": spec.appId,
      "error": "invalid_summary"
    }
  summary["commandExitCode"] = %exitCode
  summary["logPath"] = %logPath
  if exitCode != 0:
    summary["ok"] = %false
    if logText.len > 0:
      summary["failureLogTail"] = %logText.splitLines().filterIt(it.len > 0)[max(0, logText.splitLines().len - 40) .. ^1].join("\n")
  summary

proc summaryOk(summary: JsonNode): bool =
  summary.kind == JObject and summary.hasKey("ok") and summary["ok"].kind == JBool and summary["ok"].getBool()

proc writeSuiteSummary(outDir: string, payload: JsonNode) =
  writeFile(outDir / "suite.summary.json", pretty(payload))

when isMainModule:
  var outDir = ""
  var specs = @[
    AppSpec(appId: "chess", timeoutMs: 30_000),
    AppSpec(appId: "doudizhu", timeoutMs: 45_000)
  ]
  var transport = getEnv("HOST_HARNESS_TRANSPORT", "tcp").toLowerAscii()

  var parser = initOptParser(commandLineParams())
  while true:
    parser.next()
    case parser.kind
    of cmdEnd:
      break
    of cmdLongOption, cmdShortOption:
      case parser.key.normalize()
      of "outdir", "out-dir":
        outDir = parser.val
      of "timeoutchess", "timeout-chess":
        specs[0].timeoutMs = parseInt(parser.val)
      of "timeoutdoudizhu", "timeout-doudizhu":
        specs[1].timeoutMs = parseInt(parser.val)
      of "transport":
        transport = parser.val.toLowerAscii()
      else:
        raise newException(ValueError, "unknown option: " & parser.key)
    of cmdArgument:
      discard

  let finalOutDir = suiteOutDir(outDir)
  putEnv("HOST_HARNESS_TRANSPORT", transport)
  var results = newJArray()
  var overallOk = true
  for spec in specs:
    let summary = runHarness(spec, finalOutDir)
    results.add(summary)
    if not summaryOk(summary):
      overallOk = false

  let suiteSummary = %*{
    "ok": overallOk,
    "generatedAtMs": nowMillis(),
    "results": results
  }
  writeSuiteSummary(finalOutDir, suiteSummary)
  echo pretty(suiteSummary)
  quit(if overallOk: 0 else: 1)
