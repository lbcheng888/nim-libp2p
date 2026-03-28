{.used.}

import std/[json, os, unittest]
from std/times import epochTime

import ../examples/libnimlibp2p

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc takeCString(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

template waitUntilSatisfied(timeoutMs: int, intervalMs: int, body: untyped) =
  block:
    let deadline = nowMillis() + int64(timeoutMs)
    var satisfied = false
    while nowMillis() <= deadline:
      let ok = block:
        body
      if ok:
        satisfied = true
        break
      sleep(intervalMs)
    check satisfied

proc testDataDir(prefix: string): string =
  let path = getTempDir() / "nim-libp2p-libffi-routing-status" / (prefix & "-" & $nowMillis())
  createDir(path)
  path

proc routingConfig(prefix: string, extra: JsonNode): JsonNode =
  %*{
    "dataDir": testDataDir(prefix),
    "extra": extra,
  }

proc stopAndFreeNode(handle: pointer) =
  if handle.isNil:
    return
  discard libp2p_node_stop(handle)
  sleep(250)
  libp2p_node_free(handle)
  sleep(150)

suite "libnimlibp2p routing status":
  test "routing status getter reports configured routing planes":
    let cases = @[
      (
        "legacy",
        %*{},
        "legacyOnly",
        "legacy",
        false
      ),
      (
        "dual-legacy",
        %*{
          "routingMode": "dualStack",
          "primaryRoutingPlane": "legacy",
          "lsmrNetworkId": "libffi-routing-dual-legacy",
          "lsmrPath": [5]
        },
        "dualStack",
        "legacy",
        true
      ),
      (
        "dual-lsmr",
        %*{
          "routingMode": "dualStack",
          "primaryRoutingPlane": "lsmr",
          "lsmrNetworkId": "libffi-routing-dual-lsmr",
          "lsmrPath": [5]
        },
        "dualStack",
        "lsmr",
        true
      ),
      (
        "lsmr-only",
        %*{
          "routingMode": "lsmrOnly",
          "primaryRoutingPlane": "lsmr",
          "lsmrNetworkId": "libffi-routing-lsmr-only",
          "lsmrPath": [5]
        },
        "lsmrOnly",
        "lsmr",
        false
      ),
    ]

    for (label, extra, expectedMode, expectedPrimary, expectedShadow) in cases:
      let handle = libp2p_node_init(($routingConfig(label, extra)).cstring)
      check not handle.isNil
      if handle.isNil:
        continue
      defer:
        stopAndFreeNode(handle)

      let status = parseJson(takeCString(libp2p_get_routing_status_json(handle)))
      check status["mode"].getStr() == expectedMode
      check status["primary"].getStr() == expectedPrimary
      check status["shadowMode"].getBool() == expectedShadow
      check status.hasKey("legacyCandidates")
      check status.hasKey("legacyTrusted")
      check status.hasKey("lsmrActiveCertificates")
      check status.hasKey("lsmrMigrations")
      check status.hasKey("lsmrIsolations")

  test "diagnostics expose routing status after node start":
    let cfg = routingConfig(
      "diagnostics",
      %*{
        "routingMode": "dualStack",
        "primaryRoutingPlane": "lsmr",
        "lsmrNetworkId": "libffi-routing-diagnostics",
        "lsmrPath": [5]
      }
    )
    let handle = libp2p_node_init(($cfg).cstring)
    check not handle.isNil
    if handle.isNil:
      raiseAssert("libp2p_node_init failed for diagnostics test")
    defer:
      stopAndFreeNode(handle)

    check libp2p_node_start(handle) == 0
    waitUntilSatisfied(15_000, 150):
      libp2p_node_is_started(handle)

    let diagnostics = parseJson(takeCString(libp2p_get_diagnostics_json(handle)))
    check diagnostics.hasKey("routingStatus")
    check diagnostics["routingStatus"]["mode"].getStr() == "dualStack"
    check diagnostics["routingStatus"]["primary"].getStr() == "lsmr"
    check diagnostics["routingStatus"]["shadowMode"].getBool()
