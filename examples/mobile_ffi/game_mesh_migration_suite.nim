{.used.}

import std/[json, os, osproc, strformat, strutils]
from std/times import epochTime

const
  hostSuitePath = "/Users/lbcheng/nim-libp2p/examples/mobile_ffi/game_mesh_host_suite.nim"
  hostRegressionPath = "/Users/lbcheng/nim-libp2p/tests/testmobile_ffi_game_mesh_host_e2e.nim"
  defaultUniMakerRoot = "/Users/lbcheng/UniMaker"

type
  MigrationConfig = object
    outDir: string
    androidSerial: string
    harmonyTarget: string
    uniMakerRoot: string
    hostTimeoutSec: int
    deviceTimeoutSec: int
    skipHost: bool
    skipDevice: bool

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc defaultConfig(): MigrationConfig =
  MigrationConfig(
    outDir: "",
    androidSerial: "",
    harmonyTarget: "",
    uniMakerRoot: defaultUniMakerRoot,
    hostTimeoutSec: 300,
    deviceTimeoutSec: 420,
    skipHost: false,
    skipDevice: false
  )

proc ensureOutDir(path: string): string =
  if path.len > 0:
    createDir(path)
    return path
  result = getCurrentDir() / "build" / "game-mesh-migration" / ("suite-" & $nowMillis())
  createDir(result)

proc parseArgs(): MigrationConfig =
  result = defaultConfig()
  let args = commandLineParams()
  var index = 0
  while index < args.len:
    let arg = args[index]
    proc nextValue(): string =
      if index + 1 >= args.len:
        raise newException(ValueError, "missing value for option: " & arg)
      inc index
      args[index]

    if arg == "--out-dir":
      result.outDir = nextValue()
    elif arg == "--android-serial":
      result.androidSerial = nextValue()
    elif arg == "--harmony-target":
      result.harmonyTarget = nextValue()
    elif arg == "--unimaker-root" or arg == "--project-root":
      result.uniMakerRoot = nextValue()
    elif arg == "--host-timeout-sec":
      result.hostTimeoutSec = parseInt(nextValue())
    elif arg == "--device-timeout-sec":
      result.deviceTimeoutSec = parseInt(nextValue())
    elif arg == "--skip-host":
      result.skipHost = true
    elif arg == "--skip-device":
      result.skipDevice = true
    elif arg.startsWith("--out-dir="):
      result.outDir = arg.split("=", 1)[1]
    elif arg.startsWith("--android-serial="):
      result.androidSerial = arg.split("=", 1)[1]
    elif arg.startsWith("--harmony-target="):
      result.harmonyTarget = arg.split("=", 1)[1]
    elif arg.startsWith("--unimaker-root="):
      result.uniMakerRoot = arg.split("=", 1)[1]
    elif arg.startsWith("--project-root="):
      result.uniMakerRoot = arg.split("=", 1)[1]
    elif arg.startsWith("--host-timeout-sec="):
      result.hostTimeoutSec = parseInt(arg.split("=", 1)[1])
    elif arg.startsWith("--device-timeout-sec="):
      result.deviceTimeoutSec = parseInt(arg.split("=", 1)[1])
    else:
      raise newException(ValueError, "unknown option: " & arg)
    inc index

proc readTextIfExists(path: string): string =
  if fileExists(path):
    readFile(path)
  else:
    ""

proc tailText(raw: string, maxLines: int = 160): string =
  let lines = raw.splitLines()
  if lines.len <= maxLines:
    return raw
  lines[(lines.len - maxLines) ..< lines.len].join("\n")

proc runCommandLogged(cmd: string, workdir, logPath: string, timeoutSec: int): tuple[code: int, output: string, timedOut: bool] =
  discard ensureOutDir(parentDir(logPath))
  if fileExists(logPath):
    removeFile(logPath)
  let wrapped = "exec " & cmd & " > " & logPath.quoteShell & " 2>&1"
  let process = startProcess(
    command = "/bin/sh",
    args = @["-lc", wrapped],
    workingDir = workdir,
    options = {poUsePath},
  )
  var closed = false
  defer:
    if not closed:
      close(process)
  let deadline =
    if timeoutSec > 0:
      nowMillis() + int64(timeoutSec) * 1000
    else:
      high(int64)
  while true:
    let exitCode = peekExitCode(process)
    if exitCode != -1:
      let output = readTextIfExists(logPath)
      close(process)
      closed = true
      return (exitCode, output, false)
    if timeoutSec > 0 and nowMillis() >= deadline:
      terminate(process)
      sleep(500)
      if peekExitCode(process) == -1:
        kill(process)
        discard waitForExit(process)
      let output = readTextIfExists(logPath)
      close(process)
      closed = true
      return (124, output, true)
    sleep(250)

proc parseJsonFile(path: string): JsonNode =
  if not fileExists(path):
    return %*{"ok": false, "error": "missing_summary", "path": path}
  try:
    parseJson(readFile(path))
  except CatchableError:
    %*{"ok": false, "error": "invalid_summary_json", "path": path}

proc jsonError(node: JsonNode): string =
  if node.kind == JObject and node.hasKey("error") and node["error"].kind == JString:
    node["error"].getStr()
  else:
    ""

proc detectAndroidSerial(explicitSerial: string): string =
  if explicitSerial.len > 0:
    return explicitSerial
  let res = execCmdEx("adb devices", {poUsePath, poStdErrToStdOut})
  for line in res.output.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0 or trimmed.startsWith("List of devices"):
      continue
    let parts = trimmed.splitWhitespace()
    if parts.len >= 2 and parts[1] == "device":
      return parts[0]
  ""

proc detectHarmonyTarget(explicitTarget: string): string =
  if explicitTarget.len > 0:
    return explicitTarget
  let res = execCmdEx("hdc list targets", {poUsePath, poStdErrToStdOut})
  for line in res.output.splitLines():
    let trimmed = line.strip()
    if trimmed.len > 0:
      return trimmed
  ""

proc mobileGamesCliPath(projectRoot: string): string =
  projectRoot / "build" / "bin" / "mobile_games_cli"

proc detectDeviceStage(outDir: string): string =
  for matchDir in walkDirs(outDir / "*"):
    if fileExists(matchDir / "summary.json"):
      return "summary_ready"
    if fileExists(matchDir / "guest.replay.json") or fileExists(matchDir / "host.replay.json"):
      return "replay_started"
    if fileExists(matchDir / "guest.finished.json") or fileExists(matchDir / "host.finished.json"):
      return "finished_wait"
    if fileExists(matchDir / "guest.apply.json") or fileExists(matchDir / "host.apply.json"):
      return "apply_started"
    if fileExists(matchDir / "guest.ready.json") or fileExists(matchDir / "host.ready.json"):
      return "ready_wait"
    if fileExists(matchDir / "join.json"):
      return "join_completed"
    if fileExists(matchDir / "create.json"):
      return "create_completed"
  "no_artifacts"

proc partialDeviceSummary(subcommand, outDir, logPath, output: string, code: int, timedOut: bool): JsonNode =
  var artifacts = newJArray()
  for filePath in walkDirRec(outDir):
    let name = extractFilename(filePath)
    if name.endsWith(".json") or name.endsWith(".jsonl") or name.endsWith(".log"):
      artifacts.add(%filePath)
  result = %*{
    "ok": false,
    "subcommand": subcommand,
    "error": (if timedOut: "timed_out" else: "missing_summary"),
    "timedOut": timedOut,
    "commandExitCode": code,
    "logPath": logPath,
    "stalledStage": detectDeviceStage(outDir),
    "artifactCount": artifacts.len,
    "artifacts": artifacts,
    "failureOutputTail": tailText(output),
  }
  for matchDir in walkDirs(outDir / "*"):
    if fileExists(matchDir / "create.json"):
      result["create"] = parseJsonFile(matchDir / "create.json")
    if fileExists(matchDir / "join.json"):
      result["join"] = parseJsonFile(matchDir / "join.json")
    if fileExists(matchDir / "host.ready.json"):
      result["hostReady"] = parseJsonFile(matchDir / "host.ready.json")
    if fileExists(matchDir / "guest.ready.json"):
      result["guestReady"] = parseJsonFile(matchDir / "guest.ready.json")
    let crashLog = matchDir / "crash-harmony-android_host__harmony_guest_join_attempt_0_poll_0-tail.log"
    if fileExists(crashLog):
      result["harmonyCrashTailPath"] = %crashLog
    let harmonyMonitor = matchDir / "monitor.harmony.jsonl"
    if fileExists(harmonyMonitor):
      result["harmonyMonitorPath"] = %harmonyMonitor
    let androidMonitor = matchDir / "monitor.android.jsonl"
    if fileExists(androidMonitor):
      result["androidMonitorPath"] = %androidMonitor
    break

proc runHostSuite(cfg: MigrationConfig, outDir: string): JsonNode =
  let logPath = outDir / "host-suite.log"
  let cmd = [
    "nim",
    "c",
    "-r",
    hostRegressionPath.quoteShell,
  ].join(" ")
  let (code, output, timedOut) = runCommandLogged(cmd, getCurrentDir(), logPath, cfg.hostTimeoutSec)
  result = %*{
    "ok": code == 0,
    "mode": "testmobile_ffi_game_mesh_host_e2e",
    "commandExitCode": code,
    "logPath": logPath,
    "timedOut": timedOut,
  }
  if code != 0:
    result["error"] = %"host_regression_failed"
    result["failureOutput"] = %tailText(output)

proc runDeviceCase(cfg: MigrationConfig, cliPath, projectRoot, androidSerial, harmonyTarget, subcommand, outDir: string): JsonNode =
  let logPath = outDir / (subcommand & ".log")
  createDir(outDir)
  let cmd = [
    cliPath.quoteShell,
    subcommand,
    "--project-root", projectRoot.quoteShell,
    "--android-serial", androidSerial.quoteShell,
    "--harmony-target", harmonyTarget.quoteShell,
    "--out-dir", outDir.quoteShell
  ].join(" ")
  let timeoutSec =
    if subcommand == "doudizhu-e2e": 600
    else: cfg.deviceTimeoutSec
  let (code, output, timedOut) = runCommandLogged(cmd, projectRoot, logPath, timeoutSec)
  var summary = parseJsonFile(outDir / "summary.json")
  if summary.kind != JObject or jsonError(summary) == "missing_summary":
    summary = partialDeviceSummary(subcommand, outDir, logPath, output, code, timedOut)
  summary["commandExitCode"] = %code
  summary["logPath"] = %logPath
  summary["timedOut"] = %timedOut
  if code != 0:
    summary["ok"] = %false
    summary["failureOutput"] = %tailText(output)
  summary

proc summaryOk(summary: JsonNode): bool =
  summary.kind == JObject and summary.hasKey("ok") and summary["ok"].kind == JBool and summary["ok"].getBool()

proc writeSummary(path: string, payload: JsonNode) =
  writeFile(path, pretty(payload) & "\n")

when isMainModule:
  let cfg = parseArgs()
  let finalOutDir = ensureOutDir(cfg.outDir)
  var root = newJObject()
  root["generatedAtMs"] = %nowMillis()
  root["ok"] = %true
  root["projectRoot"] = %getCurrentDir()
  root["uniMakerRoot"] = %cfg.uniMakerRoot

  if not cfg.skipHost:
    let hostOut = finalOutDir / "host"
    createDir(hostOut)
    let hostSummary = runHostSuite(cfg, hostOut)
    root["host"] = hostSummary
    if not summaryOk(hostSummary):
      root["ok"] = %false
  else:
    root["host"] = %*{"ok": false, "skipped": true, "reason": "skip_host"}

  if not cfg.skipDevice:
    let cliPath = mobileGamesCliPath(cfg.uniMakerRoot)
    let androidSerial = detectAndroidSerial(cfg.androidSerial)
    let harmonyTarget = detectHarmonyTarget(cfg.harmonyTarget)
    var deviceRoot = newJObject()
    deviceRoot["androidSerial"] = %androidSerial
    deviceRoot["harmonyTarget"] = %harmonyTarget
    deviceRoot["cliPath"] = %cliPath
    if cliPath.len == 0 or not fileExists(cliPath):
      deviceRoot["ok"] = %false
      deviceRoot["error"] = %"missing_mobile_games_cli"
      root["ok"] = %false
    elif androidSerial.len == 0 or harmonyTarget.len == 0:
      deviceRoot["ok"] = %false
      deviceRoot["error"] = %"missing_connected_devices"
      root["ok"] = %false
    else:
      let deviceOut = finalOutDir / "device"
      createDir(deviceOut)
      let chessSummary = runDeviceCase(cfg, cliPath, cfg.uniMakerRoot, androidSerial, harmonyTarget, "xiangqi-e2e", deviceOut / "chess")
      let ddzSummary = runDeviceCase(cfg, cliPath, cfg.uniMakerRoot, androidSerial, harmonyTarget, "doudizhu-e2e", deviceOut / "doudizhu")
      deviceRoot["chess"] = chessSummary
      deviceRoot["doudizhu"] = ddzSummary
      deviceRoot["ok"] = %(summaryOk(chessSummary) and summaryOk(ddzSummary))
      if not deviceRoot["ok"].getBool():
        root["ok"] = %false
    root["device"] = deviceRoot
  else:
    root["device"] = %*{"ok": false, "skipped": true, "reason": "skip_device"}

  writeSummary(finalOutDir / "migration.summary.json", root)
  echo pretty(root)
  quit(if summaryOk(root): 0 else: 1)
