import std/[base64, json, os, osproc, sequtils, streams, strformat, strutils, times]

const
  AndroidMirrorDir = "/data/local/tmp/unimaker-automation-results"
  HarmonyMirrorDir = "/data/local/tmp/unimaker-automation-results"
  DefaultAndroidPkg = "com.unimaker.native.debug"
  DefaultAndroidActivity = "com.unimaker.nativeapp.MainActivity"
  DefaultAndroidAutomationActivity = "com.unimaker.nativeapp.AutomationActivity"
  DefaultAndroidAutomationService = "com.unimaker.nativeapp.AutomationService"
  DefaultAndroidAction = "com.unimaker.nativeapp.AUTOMATION"
  DefaultHarmonyBundle = "com.example.unimaker"
  DefaultHarmonyAbility = "EntryAbility"
  AndroidCrashPattern = "Fatal signal|SIGSEGV|SIGABRT|am_crash|F libc|Process com\\.unimaker\\.native(\\.debug)? has died|AndroidRuntime"
  HarmonyCrashPattern = "Cpp Crash|SIGABRT|SIGSEGV|Unexpected call: exit\\(1\\)|PROCESS_KILL|FATAL|Fatal|com\\.example\\.unimaker|EntryAbility"

type
  ExecResult = object
    exitCode: int
    output: string
    timedOut: bool

  DeviceConfig = object
    adbBin: string
    hdcBin: string
    androidSerial: string
    harmonyTarget: string
    androidPkg: string
    androidActivity: string
    androidAutomationActivity: string
    androidAutomationService: string
    androidAction: string
    harmonyBundle: string
    harmonyAbility: string
    workDir: string

  CommandParam = object
    key: string
    value: string
    androidOnly: bool
    androidBase64: bool

proc shellQuote(value: string): string =
  "'" & value.replace("'", "'\"'\"'") & "'"

proc runCapture(cmd: string, timeoutMs = 15_000): ExecResult =
  let process = startProcess(
    command = "/bin/sh",
    args = @["-lc", cmd],
    options = {poUsePath, poStdErrToStdOut},
  )
  let deadline = epochTime() + float(timeoutMs) / 1000.0
  while true:
    let code = peekExitCode(process)
    if code != -1:
      result.exitCode = code
      break
    if epochTime() >= deadline:
      result.timedOut = true
      terminate(process)
      sleep(250)
      if peekExitCode(process) == -1:
        kill(process)
      discard waitForExit(process)
      result.exitCode = -1
      break
    sleep(100)
  result.output = process.outputStream.readAll()
  close(process)

proc nowStamp(): string =
  now().format("yyyyMMdd-HHmmss")

proc ensureDir(path: string) =
  if not dirExists(path):
    createDir(path)

proc writeJson(path: string, node: JsonNode) =
  ensureDir(path.parentDir())
  writeFile(path, node.pretty())

proc findFirstOnlineDevice(toolCmd: string): string =
  let res = runCapture(toolCmd, 5_000)
  if res.exitCode != 0:
    return ""
  for line in res.output.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0 or trimmed == "[Empty]":
      continue
    if '\t' in trimmed:
      let cols = trimmed.split('\t')
      if cols.len >= 2 and cols[1] == "device":
        return cols[0]
    elif trimmed != "List of devices attached" and not trimmed.startsWith("*"):
      return trimmed
  ""

proc requireOnlineDevices(cfg: var DeviceConfig) =
  if cfg.androidSerial.len == 0:
    cfg.androidSerial = findFirstOnlineDevice(shellQuote(cfg.adbBin) & " devices")
  if cfg.harmonyTarget.len == 0:
    cfg.harmonyTarget = findFirstOnlineDevice(shellQuote(cfg.hdcBin) & " list targets")
  if cfg.androidSerial.len == 0:
    quit("no Android device online")
  if cfg.harmonyTarget.len == 0:
    quit("no Harmony target online")

proc androidShell(cfg: DeviceConfig, command: string, timeoutMs = 15_000): ExecResult =
  runCapture(
    shellQuote(cfg.adbBin) & " -s " & shellQuote(cfg.androidSerial) & " shell " & command,
    timeoutMs,
  )

proc androidRunAsShell(cfg: DeviceConfig, command: string, timeoutMs = 15_000): ExecResult =
  androidShell(cfg, "run-as " & cfg.androidPkg & " " & command, timeoutMs)

proc harmonyShell(cfg: DeviceConfig, parts: seq[string], timeoutMs = 15_000): ExecResult =
  let remoteCmd = parts.map(shellQuote).join(" ")
  runCapture(
    shellQuote(cfg.hdcBin) & " -t " & shellQuote(cfg.harmonyTarget) & " shell " & shellQuote(remoteCmd),
    timeoutMs,
  )

proc captureAndroidCrashLog(cfg: DeviceConfig, outPath: string): JsonNode =
  let cmd =
    shellQuote(cfg.adbBin) & " -s " & shellQuote(cfg.androidSerial) &
    " logcat -d -v time | grep -E " & shellQuote(AndroidCrashPattern) & " || true"
  let res = runCapture(cmd, 5_000)
  ensureDir(outPath.parentDir())
  writeFile(outPath, res.output)
  %*{
    "path": outPath,
    "detected": res.output.strip().len > 0,
  }

proc captureHarmonyCrashLog(cfg: DeviceConfig, outPath: string): JsonNode =
  let cmd =
    shellQuote(cfg.hdcBin) & " -t " & shellQuote(cfg.harmonyTarget) &
    " shell " & shellQuote("hilog -x 2>/dev/null | grep -E " & shellQuote(HarmonyCrashPattern) & " || true")
  let res = runCapture(cmd, 5_000)
  ensureDir(outPath.parentDir())
  writeFile(outPath, res.output)
  %*{
    "path": outPath,
    "detected": res.output.strip().len > 0,
  }

proc wakeHarmony(cfg: DeviceConfig) =
  discard harmonyShell(cfg, @["power-shell", "wakeup"], 5_000)
  discard harmonyShell(cfg, @["power-shell", "timeout", "-o", "180000"], 5_000)

proc launchAndroidMain(cfg: DeviceConfig): ExecResult =
  let cmd =
    shellQuote(cfg.adbBin) & " -s " & shellQuote(cfg.androidSerial) &
    " shell am start -W --activity-single-top --activity-no-animation -n " &
    shellQuote(cfg.androidPkg & "/" & cfg.androidActivity)
  runCapture(cmd, 20_000)

proc launchHarmonyMain(cfg: DeviceConfig): ExecResult =
  wakeHarmony(cfg)
  harmonyShell(cfg, @["aa", "start", "-b", cfg.harmonyBundle, "-a", cfg.harmonyAbility], 20_000)

proc clearMirrorDirs(cfg: DeviceConfig) =
  discard androidShell(cfg, "rm -rf " & shellQuote(AndroidMirrorDir) & " && mkdir -p " & shellQuote(AndroidMirrorDir), 10_000)
  discard harmonyShell(cfg, @["rm", "-rf", HarmonyMirrorDir], 10_000)
  discard harmonyShell(cfg, @["mkdir", "-p", HarmonyMirrorDir], 10_000)

proc buildAndroidParamArgs(params: seq[CommandParam]): string =
  var parts: seq[string] = @[]
  for param in params:
    if param.androidBase64:
      var encodedKey = param.key
      let lowerKey = encodedKey.toLowerAscii()
      if not (lowerKey.endsWith("_b64") or lowerKey.endsWith("base64")):
        encodedKey.add("_b64")
      parts.add("--es")
      parts.add(encodedKey)
      parts.add(base64.encode(param.value))
    else:
      parts.add("--es")
      parts.add(param.key)
      parts.add(param.value)
  parts.map(shellQuote).join(" ")

proc buildHarmonyParamParts(params: seq[CommandParam]): seq[string] =
  for param in params:
    if param.androidOnly:
      continue
    result.add("--ps")
    result.add(param.key)
    result.add(param.value)

proc dispatchAndroid(cfg: DeviceConfig, token, cmd: string, params: seq[CommandParam], useActivity: bool): ExecResult =
  var command: string
  if useActivity:
    command =
      shellQuote(cfg.adbBin) & " -s " & shellQuote(cfg.androidSerial) &
      " shell am start -W --activity-single-top --activity-no-animation -n " &
      shellQuote(cfg.androidPkg & "/" & cfg.androidActivity) &
      " --es auto_cmd " & shellQuote(cmd) &
      " --es auto_token " & shellQuote(token)
  else:
    command =
      shellQuote(cfg.adbBin) & " -s " & shellQuote(cfg.androidSerial) &
      " shell am start -W -n " &
      shellQuote(cfg.androidPkg & "/" & cfg.androidAutomationActivity) &
      " --es auto_cmd " & shellQuote(cmd) &
      " --es auto_token " & shellQuote(token)
  let extraArgs = buildAndroidParamArgs(params)
  if extraArgs.len > 0:
    command.add(" ")
    command.add(extraArgs)
  runCapture(command, 20_000)

proc dispatchHarmony(cfg: DeviceConfig, token, cmd: string, params: seq[CommandParam]): ExecResult =
  wakeHarmony(cfg)
  var parts = @[
    "aa", "start",
    "-b", cfg.harmonyBundle,
    "-a", cfg.harmonyAbility,
    "--ps", "auto_cmd", cmd,
    "--ps", "auto_token", token,
  ]
  parts.add(buildHarmonyParamParts(params))
  harmonyShell(cfg, parts, 20_000)

proc pollJson(getter: proc(): string {.closure.}, timeoutMs: int): string =
  let deadline = epochTime() + float(timeoutMs) / 1000.0
  while epochTime() < deadline:
    let payload = getter().strip()
    if payload.startsWith("{") and payload.len > 2:
      return payload
    sleep(250)
  ""

proc lastJsonObject(payload: string): string =
  let lines = payload.splitLines()
  var idx = lines.len - 1
  while idx >= 0:
    let line = lines[idx]
    let trimmed = line.strip()
    let pos = trimmed.find('{')
    if pos >= 0:
      let candidate = trimmed[pos .. ^1].strip()
      if candidate.startsWith("{"):
        return candidate
    dec idx
  ""

proc waitAndroidResult(cfg: DeviceConfig, token: string, timeoutMs: int): JsonNode =
  let jsonPath = AndroidMirrorDir & "/" & token & ".json"
  let stagePath = AndroidMirrorDir & "/" & token & ".stage"
  let privateDir = "files/automation-results"
  let privateJsonPath = privateDir & "/" & token & ".json"
  let privateStagePath = privateDir & "/" & token & ".stage"
  var payload = pollJson(proc(): string =
    androidShell(cfg, "cat " & shellQuote(jsonPath) & " 2>/dev/null || true", 3_000).output
  , timeoutMs)
  if payload.len == 0:
    payload = pollJson(proc(): string =
      androidRunAsShell(cfg, "cat " & shellQuote(privateJsonPath), 3_000).output
    , timeoutMs)
  if payload.len > 0:
    return parseJson(payload)
  let stage = block:
    let mirrored = androidShell(cfg, "cat " & shellQuote(stagePath) & " 2>/dev/null || true", 3_000).output.strip()
    if mirrored.len > 0: mirrored
    else: androidRunAsShell(cfg, "cat " & shellQuote(privateStagePath), 3_000).output.strip()
  %*{
    "token": token,
    "ok": false,
    "error": "android_result_timeout",
    "stage": stage,
  }

proc waitHarmonyResult(cfg: DeviceConfig, token: string, timeoutMs: int): JsonNode =
  let jsonPath = HarmonyMirrorDir & "/" & token & ".json"
  var payload = pollJson(proc(): string =
    harmonyShell(cfg, @["cat", jsonPath], 3_000).output
  , timeoutMs)
  if payload.len == 0:
    let grepCmd =
      shellQuote(cfg.hdcBin) & " -t " & shellQuote(cfg.harmonyTarget) &
      " shell " & shellQuote("hilog -x 2>/dev/null | grep " & shellQuote(token) & " | tail -n 40")
    payload = pollJson(proc(): string =
      lastJsonObject(runCapture(grepCmd, 4_000).output)
    , timeoutMs)
  if payload.len > 0:
    return parseJson(payload)
  %*{
    "token": token,
    "ok": false,
    "error": "harmony_result_timeout",
  }

proc saveStep(workDir, name: string, payload: JsonNode) =
  writeJson(workDir / (name & ".json"), payload)

proc extractAddrs(node: JsonNode): seq[string] =
  for key in ["multiaddrs", "directMultiaddrs", "relayMultiaddrs", "listenAddrs", "dialableAddrs"]:
    if node.kind == JObject and node.hasKey(key) and node[key].kind == JArray:
      for item in node[key].items:
        if item.kind == JString:
          let address = item.getStr().strip()
          if address.len > 0 and address notin result:
            result.add(address)

proc extractPeerId(node: JsonNode): string =
  for key in ["peerId", "localPeerId"]:
    if node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
      let value = node[key].getStr().strip()
      if value.len > 0:
        return value
  ""

proc extractTailnetIp(node: JsonNode): string =
  if node.kind == JObject and node.hasKey("tailnetIPs") and node["tailnetIPs"].kind == JArray:
    for item in node["tailnetIPs"].items:
      if item.kind == JString:
        let ip = item.getStr().strip()
        if ip.len > 0:
          return ip
  if node.kind == JObject and node.hasKey("tailnetStatus") and node["tailnetStatus"].kind == JObject:
    return extractTailnetIp(node["tailnetStatus"])
  ""

proc runAndroidCmd(cfg: DeviceConfig, workDir, token, cmd: string, params: seq[CommandParam], useActivity = false, timeoutMs = 30_000): JsonNode =
  let start = dispatchAndroid(cfg, token, cmd, params, useActivity)
  let result = waitAndroidResult(cfg, token, timeoutMs)
  let payload = %*{
    "dispatch": {
      "exitCode": start.exitCode,
      "timedOut": start.timedOut,
      "output": start.output.strip(),
    },
    "result": result,
  }
  saveStep(workDir, "android-" & token, payload)
  payload

proc runHarmonyCmd(cfg: DeviceConfig, workDir, token, cmd: string, params: seq[CommandParam], timeoutMs = 30_000): JsonNode =
  let start = dispatchHarmony(cfg, token, cmd, params)
  let result = waitHarmonyResult(cfg, token, timeoutMs)
  let payload = %*{
    "dispatch": {
      "exitCode": start.exitCode,
      "timedOut": start.timedOut,
      "output": start.output.strip(),
    },
    "result": result,
  }
  saveStep(workDir, "harmony-" & token, payload)
  payload

proc boolField(node: JsonNode, key: string): bool =
  if node.kind == JObject and node.hasKey(key):
    case node[key].kind
    of JBool:
      return node[key].getBool()
    of JString:
      return node[key].getStr().toLowerAscii() == "true"
    else:
      discard
  false

proc main() =
  let stamp = nowStamp()
  var cfg = DeviceConfig(
    adbBin: getEnv("ADB_BIN", "adb"),
    hdcBin: getEnv("HDC_BIN", "hdc"),
    androidSerial: getEnv("ANDROID_SERIAL"),
    harmonyTarget: getEnv("HARMONY_TARGET"),
    androidPkg: getEnv("ANDROID_PKG", DefaultAndroidPkg),
    androidActivity: getEnv("ANDROID_ACTIVITY", DefaultAndroidActivity),
    androidAutomationActivity: getEnv("ANDROID_AUTOMATION_ACTIVITY", DefaultAndroidAutomationActivity),
    androidAutomationService: getEnv("ANDROID_AUTOMATION_SERVICE", DefaultAndroidAutomationService),
    androidAction: getEnv("ANDROID_ACTION", DefaultAndroidAction),
    harmonyBundle: getEnv("HARMONY_BUNDLE", DefaultHarmonyBundle),
    harmonyAbility: getEnv("HARMONY_ABILITY", DefaultHarmonyAbility),
    workDir: getEnv("WORK_DIR", getCurrentDir() / "build" / ("inprocess-automation-" & stamp)),
  )
  requireOnlineDevices(cfg)
  ensureDir(cfg.workDir)
  clearMirrorDirs(cfg)

  let androidLaunch = launchAndroidMain(cfg)
  let harmonyLaunch = launchHarmonyMain(cfg)
  writeFile(cfg.workDir / "android-launch.txt", androidLaunch.output)
  writeFile(cfg.workDir / "harmony-launch.txt", harmonyLaunch.output)

  let androidTransport = runAndroidCmd(cfg, cfg.workDir, "android-transport-" & stamp, "transport_addrs", @[], timeoutMs = 45_000)
  let harmonyTransport = runHarmonyCmd(cfg, cfg.workDir, "harmony-transport-" & stamp, "transport_addrs", @[], timeoutMs = 45_000)
  let androidStatus = runAndroidCmd(cfg, cfg.workDir, "android-tailnet-status-" & stamp, "tailnet_status", @[], timeoutMs = 45_000)
  let harmonyStatus = runHarmonyCmd(cfg, cfg.workDir, "harmony-tailnet-status-" & stamp, "tailnet_status", @[], timeoutMs = 45_000)

  let androidTransportResult = androidTransport["result"]
  let harmonyTransportResult = harmonyTransport["result"]
  let androidStatusResult = androidStatus["result"]
  let harmonyStatusResult = harmonyStatus["result"]

  let androidPeerId = extractPeerId(androidTransportResult)
  let harmonyPeerId = extractPeerId(harmonyTransportResult)
  let androidAddrs = extractAddrs(androidTransportResult)
  let harmonyAddrs = extractAddrs(harmonyTransportResult)
  let androidTailnetIp = extractTailnetIp(androidStatusResult)
  let harmonyTailnetIp = extractTailnetIp(harmonyStatusResult)

  var androidPing = %*{"result": {"ok": false, "error": "missing_peer_ip"}}
  if harmonyTailnetIp.len > 0:
    androidPing = runAndroidCmd(
      cfg,
      cfg.workDir,
      "android-tailnet-ping-" & stamp,
      "tailnet_ping",
      @[CommandParam(key: "peer_ip", value: harmonyTailnetIp)],
      timeoutMs = 30_000,
    )

  var harmonyPing = %*{"result": {"ok": false, "error": "missing_peer_ip"}}
  if androidTailnetIp.len > 0:
    harmonyPing = runHarmonyCmd(
      cfg,
      cfg.workDir,
      "harmony-tailnet-ping-" & stamp,
      "tailnet_ping",
      @[CommandParam(key: "peer_ip", value: androidTailnetIp)],
      timeoutMs = 30_000,
    )

  let dmBodyA2H = "automation-dm-a2h-" & stamp
  let dmBodyH2A = "automation-dm-h2a-" & stamp
  let msgA2H = "automation-msg-a2h-" & stamp
  let msgH2A = "automation-msg-h2a-" & stamp
  let androidSeedJson = $(%androidAddrs)
  let harmonySeedJson = $(%harmonyAddrs)

  var harmonyWaitA2H = %*{"result": {"ok": false, "error": "missing_peer_data"}}
  var androidSendA2H = %*{"result": {"ok": false, "error": "missing_peer_data"}}
  if androidPeerId.len > 0 and harmonyPeerId.len > 0:
    discard dispatchHarmony(
      cfg,
      "harmony-ui-dm-wait-a2h-" & stamp,
      "ui_dm_wait",
      @[
        CommandParam(key: "peer_id", value: androidPeerId),
        CommandParam(key: "body", value: dmBodyA2H),
        CommandParam(key: "message_id", value: msgA2H),
        CommandParam(key: "seed_peer_id", value: androidPeerId),
        CommandParam(key: "seed_multiaddrs", value: androidSeedJson),
      ],
    )
    sleep(1_000)
    androidSendA2H = runAndroidCmd(
      cfg,
      cfg.workDir,
      "android-ui-dm-send-a2h-" & stamp,
      "ui_dm_send",
      @[
        CommandParam(key: "peer_id", value: harmonyPeerId),
        CommandParam(key: "body", value: dmBodyA2H, androidBase64: true),
        CommandParam(key: "message_id", value: msgA2H),
        CommandParam(key: "seed_peer_id", value: harmonyPeerId),
        CommandParam(key: "seed_multiaddrs_b64", value: harmonySeedJson, androidOnly: true, androidBase64: true),
        CommandParam(key: "holdAfterMs", value: "1500"),
      ],
      useActivity = true,
      timeoutMs = 45_000,
    )
    harmonyWaitA2H = %*{
      "dispatch": {"exitCode": 0, "timedOut": false, "output": ""},
      "result": waitHarmonyResult(cfg, "harmony-ui-dm-wait-a2h-" & stamp, 45_000),
    }
    saveStep(cfg.workDir, "harmony-ui-dm-wait-a2h-" & stamp, harmonyWaitA2H)
  let androidWaitH2A = %*{"result": {"ok": false, "error": "missing_peer_data"}}
  var harmonySendH2A = %*{"result": {"ok": false, "error": "missing_peer_data"}}
  var androidWaitH2ANode = androidWaitH2A
  if androidPeerId.len > 0 and harmonyPeerId.len > 0:
    discard dispatchAndroid(
      cfg,
      "android-ui-dm-wait-h2a-" & stamp,
      "ui_dm_wait",
      @[
        CommandParam(key: "peer_id", value: harmonyPeerId),
        CommandParam(key: "body_b64", value: dmBodyH2A, androidBase64: true),
        CommandParam(key: "message_id", value: msgH2A),
        CommandParam(key: "seed_peer_id", value: harmonyPeerId),
        CommandParam(key: "seed_multiaddrs_b64", value: harmonySeedJson, androidOnly: true, androidBase64: true),
      ],
      useActivity = true,
    )
    sleep(1_000)
    harmonySendH2A = runHarmonyCmd(
      cfg,
      cfg.workDir,
      "harmony-ui-dm-send-h2a-" & stamp,
      "ui_dm_send",
      @[
        CommandParam(key: "peer_id", value: androidPeerId),
        CommandParam(key: "body", value: dmBodyH2A),
        CommandParam(key: "message_id", value: msgH2A),
        CommandParam(key: "seed_peer_id", value: androidPeerId),
        CommandParam(key: "seed_multiaddrs", value: androidSeedJson),
      ],
      timeoutMs = 45_000,
    )
    androidWaitH2ANode = %*{
      "dispatch": {"exitCode": 0, "timedOut": false, "output": ""},
      "result": waitAndroidResult(cfg, "android-ui-dm-wait-h2a-" & stamp, 45_000),
    }
    saveStep(cfg.workDir, "android-ui-dm-wait-h2a-" & stamp, androidWaitH2ANode)

  let androidCrash = captureAndroidCrashLog(cfg, cfg.workDir / "android-crash.log")
  let harmonyCrash = captureHarmonyCrashLog(cfg, cfg.workDir / "harmony-crash.log")

  let summary = %*{
    "ok":
      boolField(androidTransportResult, "ok") and
      boolField(harmonyTransportResult, "ok") and
      boolField(androidStatusResult, "ok") and
      boolField(harmonyStatusResult, "ok") and
      boolField(androidSendA2H["result"], "ok") and
      boolField(harmonyWaitA2H["result"], "ok") and
      boolField(harmonySendH2A["result"], "ok") and
      boolField(androidWaitH2ANode["result"], "ok"),
    "android": {
      "transport": androidTransportResult,
      "tailnetStatus": androidStatusResult,
      "tailnetPing": androidPing["result"],
    },
    "harmony": {
      "transport": harmonyTransportResult,
      "tailnetStatus": harmonyStatusResult,
      "tailnetPing": harmonyPing["result"],
    },
    "dm": {
      "androidToHarmony": {
        "send": androidSendA2H["result"],
        "wait": harmonyWaitA2H["result"],
      },
      "harmonyToAndroid": {
        "send": harmonySendH2A["result"],
        "wait": androidWaitH2ANode["result"],
      },
    },
    "crashMonitoring": {
      "android": androidCrash,
      "harmony": harmonyCrash,
    },
    "meta": {
      "workDir": cfg.workDir,
      "androidSerial": cfg.androidSerial,
      "harmonyTarget": cfg.harmonyTarget,
    },
  }
  saveStep(cfg.workDir, "summary", summary)
  echo summary.pretty()
  if androidCrash["detected"].getBool() or harmonyCrash["detected"].getBool():
    quit(2)
  if not summary["ok"].getBool():
    quit(1)

when isMainModule:
  main()
