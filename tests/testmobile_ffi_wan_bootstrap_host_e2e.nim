{.used.}

import std/[json, os, osproc, sequtils, strformat, strutils]
from std/times import epochTime

import unittest2

type
  HarnessProc = object
    label: string
    summaryPath: string
    logPath: string
    process: Process

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc boolField(node: JsonNode, key: string, defaultValue = false): bool =
  if node.kind == JObject and node.hasKey(key) and node[key].kind == JBool:
    return node[key].getBool()
  defaultValue

proc intField(node: JsonNode, key: string, defaultValue = 0): int =
  if node.kind != JObject or not node.hasKey(key):
    return defaultValue
  case node[key].kind
  of JInt:
    node[key].getInt()
  of JFloat:
    int(node[key].getFloat())
  else:
    defaultValue

proc strField(node: JsonNode, key: string, defaultValue = ""): string =
  if node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
    return node[key].getStr()
  defaultValue

proc harnessSourcePath(): string =
  currentSourcePath().parentDir() / ".." / "examples" / "mobile_ffi" / "wan_bootstrap_host_harness.nim"

proc harnessBinaryPath(): string =
  getTempDir() / "nim-libp2p-wan-bootstrap-host-test" / "wan_bootstrap_host_harness"

proc compileHarnessBinary() =
  let sourcePath = harnessSourcePath()
  let binaryPath = harnessBinaryPath()
  createDir(binaryPath.parentDir())
  let cmd = fmt"nim c --hints:off --warnings:off -o:{binaryPath.quoteShell} {sourcePath.quoteShell}"
  let res = execCmdEx(cmd, options = {poUsePath, poStdErrToStdOut})
  if res.exitCode != 0:
    raise newException(IOError, "failed to compile harness:\n" & res.output)
  if not fileExists(binaryPath):
    raise newException(IOError, "compiled harness missing: " & binaryPath)

proc readTextIfExists(path: string): string =
  if fileExists(path):
    readFile(path)
  else:
    ""

proc tailText(raw: string, maxLines = 120): string =
  let lines = raw.splitLines()
  if lines.len <= maxLines:
    return raw
  lines[(lines.len - maxLines) ..< lines.len].join("\n")

proc startHarness(
    baseDir, networkId, label: string, extraArgs: seq[string]
): HarnessProc =
  let binaryPath = harnessBinaryPath()
  let summaryPath = baseDir / (label & ".summary.json")
  let logPath = baseDir / (label & ".log")
  if fileExists(summaryPath):
    removeFile(summaryPath)
  if fileExists(logPath):
    removeFile(logPath)
  var args = @[
    fmt"--label={label}",
    fmt"--network-id={networkId}",
    fmt"--summary-out={summaryPath}",
    "--transport=tcp"
  ]
  args.add(extraArgs)
  let wrapped =
    "exec " & binaryPath.quoteShell & " " &
    args.mapIt(it.quoteShell).join(" ") &
    " > " & logPath.quoteShell & " 2>&1"
  let process = startProcess(
    command = "/bin/sh",
    args = @["-lc", wrapped],
    workingDir = baseDir,
    options = {poUsePath},
  )
  HarnessProc(
    label: label,
    summaryPath: summaryPath,
    logPath: logPath,
    process: process
  )

proc stopHarness(harness: var HarnessProc) =
  if harness.process == nil:
    return
  let exitCode = peekExitCode(harness.process)
  if exitCode == -1:
    terminate(harness.process)
    sleep(300)
    if peekExitCode(harness.process) == -1:
      kill(harness.process)
      discard waitForExit(harness.process)
  close(harness.process)
  harness.process = nil

proc waitForSummary(harness: HarnessProc, timeoutMs: int): JsonNode =
  let deadline = nowMillis() + int64(timeoutMs)
  while nowMillis() <= deadline:
    if fileExists(harness.summaryPath):
      try:
        return parseJson(readFile(harness.summaryPath))
      except CatchableError:
        discard
    let exitCode = peekExitCode(harness.process)
    if exitCode != -1 and not fileExists(harness.summaryPath):
      let logTail = tailText(readTextIfExists(harness.logPath))
      raise newException(
        IOError,
        "harness exited before summary: " & harness.label & " exit=" & $exitCode & "\n" & logTail
      )
    sleep(150)
  let logTail = tailText(readTextIfExists(harness.logPath))
  raise newException(IOError, "timed out waiting for summary: " & harness.label & "\n" & logTail)

proc waitForExit(harness: HarnessProc, timeoutMs: int): int =
  let deadline = nowMillis() + int64(timeoutMs)
  while nowMillis() <= deadline:
    let exitCode = peekExitCode(harness.process)
    if exitCode != -1:
      return exitCode
    sleep(150)
  raise newException(IOError, "timed out waiting for exit: " & harness.label)

proc candidatePeerIds(status: JsonNode): seq[string] =
  if status.kind != JObject or not status.hasKey("selectedCandidates"):
    return @[]
  let selected = status["selectedCandidates"]
  if selected.kind != JArray:
    return @[]
  for item in selected:
    if item.kind == JObject:
      let peerId = strField(item, "peerId")
      if peerId.len > 0:
        result.add(peerId)

proc connectedPeerIds(status: JsonNode): seq[string] =
  if status.kind != JObject or not status.hasKey("connectedPeers"):
    return @[]
  let connected = status["connectedPeers"]
  if connected.kind != JArray:
    return @[]
  for item in connected:
    if item.kind == JString:
      result.add(item.getStr())

proc attemptPeerId(joinResult: JsonNode, index: int): string =
  if joinResult.kind != JObject or not joinResult.hasKey("attempts"):
    return ""
  let attempts = joinResult["attempts"]
  if attempts.kind != JArray or index < 0 or index >= attempts.len:
    return ""
  let item = attempts[index]
  if item.kind != JObject or not item.hasKey("candidate"):
    return ""
  let candidate = item["candidate"]
  if candidate.kind != JObject:
    return ""
  strField(candidate, "peerId")

suite "Mobile FFI wan bootstrap host e2e":
  test "third node learns first peer from second snapshot without static fallback over real nodes":
    compileHarnessBinary()

    let networkId = "wan-host-e2e-" & $nowMillis()
    let baseDir = getTempDir() / ("wan-bootstrap-host-e2e-" & $nowMillis())
    createDir(baseDir)

    var node1 = startHarness(
      baseDir,
      networkId,
      "wan-node-1",
      @["--role=public", "--join=false", "--serve-ms=30000"]
    )
    defer:
      stopHarness(node1)

    let node1Summary = waitForSummary(node1, 10_000)
    let node1PeerId = strField(node1Summary, "peerId")
    let node1DialAddr = strField(node1Summary, "dialAddr")
    check boolField(node1Summary, "ok", false)
    check node1PeerId.len > 0
    check node1DialAddr.len > 0

    var node2 = startHarness(
      baseDir,
      networkId,
      "wan-node-2",
      @[
        "--role=mobile",
        fmt"--seed-peer-id={node1PeerId}",
        fmt"--seed-addrs={node1DialAddr}",
        "--serve-ms=30000"
      ]
    )
    defer:
      stopHarness(node2)

    let node2Summary = waitForSummary(node2, 12_000)
    let node2Join = node2Summary{"joinResult"}
    let node2PeerId = strField(node2Summary, "peerId")
    let node2DialAddr = strField(node2Summary, "dialAddr")
    check boolField(node2Summary, "ok", false)
    check boolField(node2Join, "ok", false)
    check attemptPeerId(node2Join, 0) == node1PeerId
    check node2PeerId.len > 0
    check node2DialAddr.len > 0

    var node3 = startHarness(
      baseDir,
      networkId,
      "wan-node-3",
      @[
        "--role=mobile",
        fmt"--seed-peer-id={node2PeerId}",
        fmt"--seed-addrs={node2DialAddr}",
        fmt"--expect-peer-id={node1PeerId}",
        "--expect-timeout-ms=12000"
      ]
    )
    defer:
      stopHarness(node3)

    let node3Summary = waitForSummary(node3, 15_000)
    let node3Exit = waitForExit(node3, 15_000)
    let node3Join = node3Summary{"joinResult"}
    let status3 = node3Summary{"bootstrap"}
    let metrics =
      if status3.kind == JObject and status3.hasKey("metrics") and status3["metrics"].kind == JObject:
        status3["metrics"]
      else:
        newJObject()
    let candidateIds = candidatePeerIds(status3)
    let connectedIds = connectedPeerIds(status3)
    check node3Exit == 0
    check boolField(node3Summary, "ok", false)
    check boolField(node3Summary, "learnedExpectedPeer", false)
    check boolField(node3Join, "ok", false)
    check attemptPeerId(node3Join, 0) == node2PeerId
    check intField(metrics, "snapshotImported", 0) >= 1
    check candidateIds.contains(node1PeerId)
    check connectedIds.contains(node2PeerId)
    check "bootstrap.libp2p.io" notin $status3
