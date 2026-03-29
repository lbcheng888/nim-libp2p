{.used.}

import std/[json, os, strutils]
from std/times import epochTime

import unittest2

const runnerPath = "/Users/lbcheng/nim-libp2p/examples/mobile_ffi/game_mesh_host_runner.nim"

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc hostE2eOutDir(appId: string): string =
  let path = getTempDir() / "nim-libp2p-host-e2e" / (appId & "-" & $nowMillis())
  createDir(path)
  path

proc compileFlagsForTransport(transport: string): seq[string] =
  case transport.toLowerAscii()
  of "quic", "quic-dual":
    @["-d:libp2p_msquic_experimental", "-d:libp2p_msquic_builtin"]
  else:
    @[]

proc runRunner(
    appId: string,
    timeoutMs: int,
    transport = "tcp",
    extraArgs: seq[string] = @[]
): JsonNode =
  let outDir = hostE2eOutDir(appId)
  let logPath = outDir / (appId & ".log")
  let nimCacheDir = outDir / "nimcache"
  createDir(nimCacheDir)
  let parts = @[
    "nim",
    "c"
  ] & compileFlagsForTransport(transport) & @[
    "--nimcache:" & nimCacheDir,
    "-r",
    runnerPath,
    "--app:" & appId,
    "--transport:" & transport,
    "--timeout-ms:" & $timeoutMs,
    "--out-dir:" & outDir
  ] & extraArgs
  let cmd = (parts & @[
    ">",
    logPath,
    "2>&1"
  ]).join(" ")
  let exitCode = execShellCmd(cmd)
  let summaryPath = outDir / appId / (appId & ".summary.json")
  let logText = if fileExists(logPath): readFile(logPath) else: ""
  if not fileExists(summaryPath):
    raise newException(IOError, "missing summary file for " & appId & "\n" & logText)
  let summary = parseJson(readFile(summaryPath))
  if exitCode != 0:
    raise newException(IOError, "host runner failed for " & appId & "\n" & logText & "\nsummary=" & $summary)
  summary

proc summaryOk(summary: JsonNode): bool =
  summary.kind == JObject and summary.hasKey("ok") and summary["ok"].getBool()

suite "Mobile FFI host game mesh e2e":
  test "xiangqi completes full localhost match over real nodes":
    let summary = runRunner("chess", 30_000)
    check summaryOk(summary)
    check summary["host"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["guest"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["host"]["finalState"]["stateHash"].getStr() ==
      summary["guest"]["finalState"]["stateHash"].getStr()
    check summary["host"]["replay"]["ok"].getBool()
    check summary["guest"]["replay"]["ok"].getBool()

  test "doudizhu completes localhost host+guest+bot match over real nodes":
    let summary = runRunner("doudizhu", 45_000)
    check summaryOk(summary)
    check summary["host"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["guest"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["host"]["finalState"]["stateHash"].getStr() ==
      summary["guest"]["finalState"]["stateHash"].getStr()
    check summary["host"]["replay"]["ok"].getBool()
    check summary["guest"]["replay"]["ok"].getBool()
    check summary["host"]["finalState"]["auditValid"].getBool()

  test "xiangqi completes full localhost match over QUIC-only real nodes":
    let summary = runRunner("chess", 30_000, transport = "quic")
    check summaryOk(summary)
    check summary["transport"].getStr() == "quic_explicit_seed"
    check summary["host"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["guest"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["host"]["finalState"]["stateHash"].getStr() ==
      summary["guest"]["finalState"]["stateHash"].getStr()
    check summary["host"]["replay"]["ok"].getBool()
    check summary["guest"]["replay"]["ok"].getBool()

  test "doudizhu completes localhost host+guest+bot match over QUIC-only real nodes":
    let summary = runRunner("doudizhu", 45_000, transport = "quic")
    check summaryOk(summary)
    check summary["transport"].getStr() == "quic_explicit_seed"
    check summary["host"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["guest"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["host"]["finalState"]["stateHash"].getStr() ==
      summary["guest"]["finalState"]["stateHash"].getStr()
    check summary["host"]["replay"]["ok"].getBool()
    check summary["guest"]["replay"]["ok"].getBool()
    check summary["host"]["finalState"]["auditValid"].getBool()

  test "xiangqi simulates LAN to mobile switch over dual-stack QUIC localhost nodes":
    let summary = runRunner(
      "chess",
      45_000,
      transport = "quic-dual",
      extraArgs = @["--network-switch:lan-mobile"]
    )
    check summaryOk(summary)
    check summary["transport"].getStr() == "quic_dual_stack_switch_simulated"
    check summary["switchVerified"].getBool()
    check not summary["mobilityValidation"]["realConnectionMigration"].getBool()
    check summary["addressSelection"]["initialConnectAddr"].getStr().contains("/ip4/")
    check summary["addressSelection"]["switchConnectAddr"].getStr().contains("/ip6/")
    check summary["host"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["guest"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["host"]["finalState"]["stateHash"].getStr() ==
      summary["guest"]["finalState"]["stateHash"].getStr()
    check summary["host"]["replay"]["ok"].getBool()
    check summary["guest"]["replay"]["ok"].getBool()

  test "doudizhu simulates LAN to mobile switch over dual-stack QUIC localhost nodes":
    let summary = runRunner(
      "doudizhu",
      60_000,
      transport = "quic-dual",
      extraArgs = @["--network-switch:lan-mobile"]
    )
    check summaryOk(summary)
    check summary["transport"].getStr() == "quic_dual_stack_switch_simulated"
    check summary["switchVerified"].getBool()
    check not summary["mobilityValidation"]["realConnectionMigration"].getBool()
    check summary["addressSelection"]["initialConnectAddr"].getStr().contains("/ip4/")
    check summary["addressSelection"]["switchConnectAddr"].getStr().contains("/ip6/")
    check summary["host"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["guest"]["finalState"]["phase"].getStr() == "FINISHED"
    check summary["host"]["finalState"]["stateHash"].getStr() ==
      summary["guest"]["finalState"]["stateHash"].getStr()
    check summary["host"]["replay"]["ok"].getBool()
    check summary["guest"]["replay"]["ok"].getBool()
    check summary["host"]["finalState"]["auditValid"].getBool()
