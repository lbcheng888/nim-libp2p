{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, os]
from std/times import epochTime

import unittest2

import ./helpers
import ../libp2p/crypto/crypto
import ../libp2p/peerid
import ../examples/mobile_ffi/libnimlibp2p

proc takeCStringAndFree(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

template safeJsonExpr(valueExpr: untyped, fallback: string = "{}"): JsonNode =
  block:
    try:
      let value = valueExpr
      let text = takeCStringAndFree(value)
      if text.len == 0:
        parseJson(fallback)
      else:
        parseJson(text)
    except CatchableError:
      parseJson(fallback)

proc testDataDir(prefix: string): string =
  let path = getTempDir() / "nim-libp2p-mobile-ffi-tests" / prefix
  createDir(path)
  path

proc testIdentity(): JsonNode =
  let pair = KeyPair.random(PKScheme.Ed25519, rng[]).get()
  let privateKey = pair.seckey.getRawBytes().get()
  let publicKey = pair.pubkey.getRawBytes().get()
  let peerId = $PeerId.init(pair.seckey).get()
  %* {
    "privateKey": base64.encode(privateKey),
    "publicKey": base64.encode(publicKey),
    "peerId": peerId,
    "source": "test",
  }

proc seededSocialState(): JsonNode =
  %* {
    "version": 2,
    "conversations": {
      "dm:peer-seeded": {
        "conversationId": "dm:peer-seeded",
        "peerId": "peer-seeded",
        "messages": [
          {
            "id": "seeded-mid",
            "content": "seeded body",
            "timestampMs": 1,
          }
        ],
        "updatedAt": 1,
      }
    },
    "contacts": {},
    "groups": {},
    "moments": [],
    "notifications": [
      {
        "id": "notif-seeded",
        "kind": "dm_received",
        "read": false,
        "readState": "unread",
        "deliveryClass": "priority",
        "channel": "dm",
        "eventKind": "dm",
        "timestampMs": 1,
      }
    ],
    "subscriptions": {},
    "synccastRooms": {},
    "publishTasks": [],
    "playbackSessions": [],
    "reportTickets": [],
    "tombstones": [],
    "distributionLogs": [],
  }

proc newTestNodeWithDir(dataDir: string): pointer =
  let config = %* {
    "identity": testIdentity(),
    "dataDir": dataDir,
  }
  let handle = libp2p_node_init(($config).cstring)
  check not handle.isNil
  check libp2p_mdns_set_enabled(handle, false)
  handle

proc stopAndFreeNode(handle: pointer) =
  if handle.isNil:
    return
  discard libp2p_node_stop(handle)
  sleep(250)
  libp2p_node_free(handle)
  sleep(150)

proc startNode(handle: pointer) =
  check libp2p_node_start(handle) == 0
  let deadline = epochTime() + 15.0
  while epochTime() <= deadline:
    if libp2p_node_is_started(handle):
      return
    sleep(150)
  check false

proc waitUntil(timeoutMs: int, predicate: proc(): bool): bool =
  let deadline = epochTime() + (float(timeoutMs) / 1000.0)
  while epochTime() <= deadline:
    if predicate():
      return true
    sleep(100)
  false

suite "Mobile FFI social state cache":
  test "diagnostics keeps cached social snapshot after disk file becomes invalid":
    let dataDir = testDataDir("social-state-cache")
    let socialStatePath = dataDir / "social_state.json"
    writeFile(socialStatePath, $seededSocialState())

    let handle = newTestNodeWithDir(dataDir)
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let diagnostics1 = safeJsonExpr(libp2p_get_diagnostics_json(handle))
    check diagnostics1["social"]["notifications"].getInt() == 1
    check diagnostics1["social"]["conversations"].getInt() == 1

    writeFile(socialStatePath, "{broken-json")

    let diagnostics2 = safeJsonExpr(libp2p_get_diagnostics_json(handle))
    check diagnostics2["social"]["notifications"].getInt() == 1
    check diagnostics2["social"]["conversations"].getInt() == 1

  test "notification mark updates disk through owner async flush":
    let dataDir = testDataDir("social-state-async-flush")
    let socialStatePath = dataDir / "social_state.json"
    writeFile(socialStatePath, $seededSocialState())

    let handle = newTestNodeWithDir(dataDir)
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let markResponse = safeJsonExpr(
      social_notifications_mark(handle, """{"id":"notif-seeded","readState":"read"}""".cstring),
      """{"ok":false}"""
    )
    check markResponse["ok"].getBool()
    check markResponse["updated"].getInt() == 1
    check markResponse["summary"]["unreadTotal"].getInt() == 0

    let summary = safeJsonExpr(social_notifications_summary(handle))
    check summary["unreadTotal"].getInt() == 0

    check waitUntil(5000, proc(): bool =
      try:
        let disk = parseJson(readFile(socialStatePath))
        if disk.kind != JObject or not disk.hasKey("notifications") or disk["notifications"].kind != JArray:
          return false
        for item in disk["notifications"]:
          if item.kind == JObject and item.hasKey("id") and item["id"].kind == JString and item["id"].getStr() == "notif-seeded":
            let readValue =
              item.hasKey("read") and item["read"].kind == JBool and item["read"].getBool()
            let readState =
              if item.hasKey("readState") and item["readState"].kind == JString:
                item["readState"].getStr()
              else:
                ""
            return readValue and readState == "read"
        false
      except CatchableError:
        false
    )
