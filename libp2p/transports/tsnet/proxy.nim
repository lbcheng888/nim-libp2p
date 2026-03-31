{.push raises: [].}

import std/[locks, strutils, tables, times]

import ../../multiaddress
import ../../utility
import ../tsnetprovidertypes

type
  TsnetSharedKey = object
    text: string

  TsnetProxyKind* {.pure.} = enum
    Tcp
    Quic

  TsnetProxyRouteSnapshot* = object
    advertised*: MultiAddress
    raw*: MultiAddress
    kind*: TsnetProxyKind

  TsnetProxyRegistration* = object
    ownerId*: int
    advertisedKey*: TsnetSharedKey
    rawKey*: TsnetSharedKey

  TsnetResolvedRemoteRegistration* = object
    ownerId*: int
    rawKey*: TsnetSharedKey
    advertisedKey*: TsnetSharedKey

  TsnetDirectRouteRegistration* = object
    ownerId*: int
    advertisedKey*: TsnetSharedKey
    rawKey*: TsnetSharedKey
    pathKind*: TsnetSharedKey
    hitCount*: int
    lastSelectedUnixMilli*: int64
    failureCount*: int
    suspendedUntilUnixMilli*: int64

  TsnetDirectRouteHit* = object
    raw*: MultiAddress
    pathKind*: string

  TsnetDirectRouteSnapshot* = object
    advertised*: MultiAddress
    raw*: MultiAddress
    pathKind*: string
    hitCount*: int
    lastSelectedUnixMilli*: int64
    failureCount*: int
    suspendedUntilUnixMilli*: int64

const
  DirectRouteFailureBackoffMs* = 5_000'i64

var proxyRegistryLock {.global.}: Lock
var proxyRegistrations {.global.}: seq[TsnetProxyRegistration]
var proxyResolvedRemotes {.global.}: seq[TsnetResolvedRemoteRegistration]
var proxyDirectRoutes {.global.}: seq[TsnetDirectRouteRegistration]

initLock(proxyRegistryLock)

template proxySafe(body: untyped) =
  {.cast(gcsafe).}:
    body

proc parseKnownAddress(text: string): MultiAddress =
  MultiAddress.init(text).expect("tsnet proxy constructed a valid multiaddr")

proc initSharedKey(value: string): TsnetSharedKey =
  result.text = value

proc clearSharedKey(value: var TsnetSharedKey) =
  value.text = ""

proc sharedKeyToString(value: TsnetSharedKey): string =
  if value.text.len == 0:
    return ""
  value.text

proc sharedKeyEquals(value: TsnetSharedKey, expected: string): bool =
  sharedKeyToString(value) == expected

proc validSharedKey(value: TsnetSharedKey): bool =
  sharedKeyToString(value).len > 0

proc replaceSharedKey(value: var TsnetSharedKey, text: string) =
  if sharedKeyEquals(value, text):
    return
  value.text = text

proc freeProxyRegistration(registration: var TsnetProxyRegistration) =
  clearSharedKey(registration.advertisedKey)
  clearSharedKey(registration.rawKey)
  registration.ownerId = 0

proc freeResolvedRemoteRegistration(registration: var TsnetResolvedRemoteRegistration) =
  clearSharedKey(registration.rawKey)
  clearSharedKey(registration.advertisedKey)
  registration.ownerId = 0

proc freeDirectRouteRegistration(registration: var TsnetDirectRouteRegistration) =
  clearSharedKey(registration.advertisedKey)
  clearSharedKey(registration.rawKey)
  clearSharedKey(registration.pathKind)
  registration.ownerId = 0
  registration.hitCount = 0
  registration.lastSelectedUnixMilli = 0
  registration.failureCount = 0
  registration.suspendedUntilUnixMilli = 0

proc validProxyRegistration(registration: TsnetProxyRegistration): bool =
  registration.ownerId > 0 and
    validSharedKey(registration.advertisedKey) and
    validSharedKey(registration.rawKey)

proc validResolvedRemoteRegistration(
    registration: TsnetResolvedRemoteRegistration
): bool =
  registration.ownerId > 0 and
    validSharedKey(registration.rawKey) and
    validSharedKey(registration.advertisedKey)

proc validDirectRouteRegistration(
    registration: TsnetDirectRouteRegistration
): bool =
  registration.ownerId > 0 and
    validSharedKey(registration.advertisedKey) and
    validSharedKey(registration.rawKey) and
    validSharedKey(registration.pathKind)

proc sanitizeProxyRoutesUnsafe() =
  var keptRoutes: seq[TsnetProxyRegistration] = @[]
  for registration in proxyRegistrations.items():
    if validProxyRegistration(registration):
      keptRoutes.add(registration)
  proxyRegistrations = keptRoutes

proc sanitizeResolvedRemotesUnsafe() =
  var keptResolved: seq[TsnetResolvedRemoteRegistration] = @[]
  for registration in proxyResolvedRemotes.items():
    if validResolvedRemoteRegistration(registration):
      keptResolved.add(registration)
  proxyResolvedRemotes = keptResolved

proc sanitizeDirectRoutesUnsafe() =
  var keptDirect: seq[TsnetDirectRouteRegistration] = @[]
  for registration in proxyDirectRoutes.items():
    if validDirectRouteRegistration(registration):
      keptDirect.add(registration)
  proxyDirectRoutes = keptDirect

proc sanitizeProxyRegistryUnsafe() =
  sanitizeProxyRoutesUnsafe()
  sanitizeResolvedRemotesUnsafe()
  sanitizeDirectRoutesUnsafe()

proc splitAddressParts(address: MultiAddress): seq[string] =
  for part in ($address).split('/'):
    if part.len > 0:
      result.add(part)

proc ipv4ReachabilityScore(host: string): int =
  let parts = host.split('.')
  if parts.len != 4:
    return 0
  var octets: array[4, int]
  for idx, part in parts:
    if part.len == 0:
      return 0
    try:
      octets[idx] = parseInt(part)
    except ValueError:
      return 0
    if octets[idx] < 0 or octets[idx] > 255:
      return 0
  let a = octets[0]
  let b = octets[1]
  if a == 127 or a == 0 or a >= 224:
    return 0
  if a == 169 and b == 254:
    return 1
  if a == 100 and b >= 64 and b <= 127:
    return 1
  if a == 10 or (a == 172 and b >= 16 and b <= 31) or (a == 192 and b == 168):
    return 2
  4

proc ipv6ReachabilityScore(host: string): int =
  let lowered = host.strip().toLowerAscii()
  if lowered.len == 0 or lowered == "::" or lowered == "::1" or lowered == "0:0:0:0:0:0:0:1":
    return 0
  if lowered.startsWith("fe8") or lowered.startsWith("fe9") or
      lowered.startsWith("fea") or lowered.startsWith("feb"):
    return 1
  if lowered.startsWith("fc") or lowered.startsWith("fd"):
    return 2
  4

proc directRouteReachabilityScore(address: MultiAddress): int =
  let parts = splitAddressParts(address)
  if parts.len < 2:
    return 0
  case parts[0]
  of "ip4":
    ipv4ReachabilityScore(parts[1])
  of "ip6":
    ipv6ReachabilityScore(parts[1])
  else:
    0

proc familyFromAddress*(address: MultiAddress): string =
  let parts = splitAddressParts(address)
  if parts.len >= 2 and (parts[0] == "ip4" or parts[0] == "ip6"):
    return parts[0]
  ""

proc portFromAddress*(address: MultiAddress): int =
  let parts = splitAddressParts(address)
  var idx = 0
  while idx + 1 < parts.len:
    if parts[idx] == "tcp" or parts[idx] == "udp":
      try:
        return parseInt(parts[idx + 1])
      except ValueError:
        return 0
    inc idx
  0

proc chooseTailnetIp*(tailnetIps: openArray[string], family: string): string =
  for candidate in tailnetIps:
    let lowered = candidate.strip().toLowerAscii()
    if lowered.len == 0:
      continue
    if family == "ip6":
      if lowered.contains(":"):
        return lowered
    elif lowered.contains("."):
      return lowered
  ""

proc buildAdvertisedAddress*(
    family, ip: string,
    port: int,
    kind: TsnetProxyKind
): Result[MultiAddress, string] =
  if family notin ["ip4", "ip6"]:
    return err("unsupported tsnet proxy family: " & family)
  if ip.strip().len == 0:
    return err("missing tsnet proxy IP for family " & family)
  if port <= 0:
    return err("missing tsnet proxy port")
  let normalized = ip.strip().toLowerAscii()
  let text =
    case kind
    of TsnetProxyKind.Tcp:
      "/" & family & "/" & normalized & "/tcp/" & $port & "/tsnet"
    of TsnetProxyKind.Quic:
      "/" & family & "/" & normalized & "/udp/" & $port & "/quic-v1/tsnet"
  ok(parseKnownAddress(text))

proc registerProxyRoute*(
    ownerId: int,
    advertised: MultiAddress,
    raw: MultiAddress
): Result[void, string] =
  if ownerId <= 0:
    return err("invalid tsnet proxy owner id")
  let advertisedKey = $advertised
  let rawKey = $raw
  if advertisedKey.len == 0 or rawKey.len == 0:
    return err("invalid tsnet proxy registration")
  withLock(proxyRegistryLock):
    sanitizeProxyRoutesUnsafe()
    var found = false
    for registration in proxyRegistrations.mitems():
      if sharedKeyEquals(registration.advertisedKey, advertisedKey):
        if not sharedKeyEquals(registration.rawKey, rawKey):
          return err("duplicate tsnet proxy registration for " & advertisedKey)
        registration.ownerId = ownerId
        found = true
        break
    if not found:
      proxyRegistrations.add(TsnetProxyRegistration(
        ownerId: ownerId,
        advertisedKey: initSharedKey(advertisedKey),
        rawKey: initSharedKey(rawKey)
      ))
  ok()

proc unregisterProxyRoutes*(ownerId: int) =
  if ownerId <= 0:
    return
  proxySafe:
    withLock(proxyRegistryLock):
      sanitizeProxyRegistryUnsafe()
      var keptRoutes: seq[TsnetProxyRegistration] = @[]
      for registration in proxyRegistrations.items():
        if registration.ownerId != ownerId:
          keptRoutes.add(registration)
      proxyRegistrations = keptRoutes

      var keptResolved: seq[TsnetResolvedRemoteRegistration] = @[]
      for registration in proxyResolvedRemotes.items():
        if registration.ownerId != ownerId:
          keptResolved.add(registration)
      proxyResolvedRemotes = keptResolved

      var keptDirect: seq[TsnetDirectRouteRegistration] = @[]
      for registration in proxyDirectRoutes.items():
        if registration.ownerId != ownerId:
          keptDirect.add(registration)
      proxyDirectRoutes = keptDirect

proc proxyRouteSnapshots*(ownerId: int): seq[TsnetProxyRouteSnapshot] {.gcsafe.} =
  if ownerId <= 0:
    return @[]
  proxySafe:
    withLock(proxyRegistryLock):
      sanitizeProxyRoutesUnsafe()
      for registration in proxyRegistrations.items:
        if registration.ownerId != ownerId:
          continue
        let advertisedKey = sharedKeyToString(registration.advertisedKey)
        let rawKey = sharedKeyToString(registration.rawKey)
        let advertised = parseKnownAddress(advertisedKey)
        let raw = parseKnownAddress(rawKey)
        let kind =
          if "/udp/" in advertisedKey.toLowerAscii() and
              "/quic-v1" in advertisedKey.toLowerAscii():
            TsnetProxyKind.Quic
          else:
            TsnetProxyKind.Tcp
        result.add(TsnetProxyRouteSnapshot(
          advertised: advertised,
          raw: raw,
          kind: kind
        ))

proc lookupRawTarget*(
    family, ip: string,
    port: int,
    kind: TsnetProxyKind
): Result[MultiAddress, string] =
  let advertised = buildAdvertisedAddress(family, ip, port, kind).valueOr:
    return err(error)
  let advertisedKey = $advertised
  withLock(proxyRegistryLock):
    sanitizeProxyRoutesUnsafe()
    for registration in proxyRegistrations.items:
      if sharedKeyEquals(registration.advertisedKey, advertisedKey):
        return ok(parseKnownAddress(sharedKeyToString(registration.rawKey)))
  err("no tsnet proxy route is registered for " & advertisedKey)

proc resolveAdvertisedRemote*(raw: MultiAddress): Result[MultiAddress, string] =
  let rawKey = $raw
  withLock(proxyRegistryLock):
    sanitizeResolvedRemotesUnsafe()
    sanitizeProxyRoutesUnsafe()
    for registration in proxyResolvedRemotes.items:
      if sharedKeyEquals(registration.rawKey, rawKey):
        return ok(parseKnownAddress(sharedKeyToString(registration.advertisedKey)))
    for registration in proxyRegistrations.items:
      if sharedKeyEquals(registration.rawKey, rawKey):
        return ok(parseKnownAddress(sharedKeyToString(registration.advertisedKey)))
  err("no tsnet proxy remote is registered for " & rawKey)

proc registerResolvedRemote*(
    ownerId: int,
    raw: MultiAddress,
    advertised: MultiAddress
) {.gcsafe.} =
  if ownerId <= 0:
    return
  proxySafe:
    let rawKey = $raw
    let advertisedKey = $advertised
    if rawKey.len == 0 or advertisedKey.len == 0:
      return
    withLock(proxyRegistryLock):
      sanitizeResolvedRemotesUnsafe()
      var replaced = false
      for registration in proxyResolvedRemotes.mitems():
        if sharedKeyEquals(registration.rawKey, rawKey):
          registration.ownerId = ownerId
          replaceSharedKey(registration.advertisedKey, advertisedKey)
          replaced = true
          break
      if not replaced:
        proxyResolvedRemotes.add(TsnetResolvedRemoteRegistration(
          ownerId: ownerId,
          rawKey: initSharedKey(rawKey),
          advertisedKey: initSharedKey(advertisedKey)
        ))

proc registerDirectRoute*(
    ownerId: int,
    advertised: MultiAddress,
    raw: MultiAddress,
    pathKind: string
): Result[void, string] {.gcsafe.} =
  if ownerId <= 0:
    return err("invalid tsnet direct route owner id")
  let normalizedPath = normalizeTailnetPath(pathKind)
  if normalizedPath notin [TsnetPathDirect, TsnetPathPunchedDirect]:
    return err("unsupported tsnet direct route path " & pathKind)
  let advertisedKey = $advertised
  let rawKey = $raw
  if advertisedKey.len == 0 or rawKey.len == 0:
    return err("invalid tsnet direct route registration")
  if directRouteReachabilityScore(raw) <= 0:
    return err("tsnet direct route candidate is not externally reachable")
  proxySafe:
    withLock(proxyRegistryLock):
      sanitizeDirectRoutesUnsafe()
      var found = false
      for registration in proxyDirectRoutes.mitems():
        if sharedKeyEquals(registration.advertisedKey, advertisedKey) and
            sharedKeyEquals(registration.rawKey, rawKey):
          registration.ownerId = ownerId
          replaceSharedKey(registration.pathKind, normalizedPath)
          found = true
          break
      if not found:
        proxyDirectRoutes.add(TsnetDirectRouteRegistration(
          ownerId: ownerId,
          advertisedKey: initSharedKey(advertisedKey),
          rawKey: initSharedKey(rawKey),
          pathKind: initSharedKey(normalizedPath),
          hitCount: 0,
          lastSelectedUnixMilli: 0,
          failureCount: 0,
          suspendedUntilUnixMilli: 0
        ))
  ok()

proc directRouteSnapshots*(ownerId: int): seq[TsnetDirectRouteSnapshot] {.gcsafe.} =
  if ownerId <= 0:
    return @[]
  proxySafe:
    withLock(proxyRegistryLock):
      sanitizeDirectRoutesUnsafe()
      for registration in proxyDirectRoutes.items:
        if registration.ownerId != ownerId:
          continue
        result.add(TsnetDirectRouteSnapshot(
          advertised: parseKnownAddress(sharedKeyToString(registration.advertisedKey)),
          raw: parseKnownAddress(sharedKeyToString(registration.rawKey)),
          pathKind: sharedKeyToString(registration.pathKind),
          hitCount: registration.hitCount,
          lastSelectedUnixMilli: registration.lastSelectedUnixMilli,
          failureCount: registration.failureCount,
          suspendedUntilUnixMilli: registration.suspendedUntilUnixMilli
        ))

proc directRouteCount*(ownerId: int): int {.gcsafe.} =
  directRouteSnapshots(ownerId).len

proc directRoutePathCounts*(ownerId: int): tuple[directCount, punchedCount: int] {.gcsafe.} =
  for registration in directRouteSnapshots(ownerId):
    case normalizeTailnetPath(registration.pathKind)
    of TsnetPathDirect:
      inc result.directCount
    of TsnetPathPunchedDirect:
      inc result.punchedCount
    else:
      discard

proc compareDirectRoutePreference(
    candidate: TsnetDirectRouteRegistration,
    incumbent: TsnetDirectRouteRegistration
): bool =
  let candidatePath = normalizeTailnetPath(sharedKeyToString(candidate.pathKind))
  let incumbentPath = normalizeTailnetPath(sharedKeyToString(incumbent.pathKind))
  if candidatePath != incumbentPath:
    if candidatePath == TsnetPathDirect:
      return true
    if incumbentPath == TsnetPathDirect:
      return false
    if candidatePath == TsnetPathPunchedDirect:
      return true
    if incumbentPath == TsnetPathPunchedDirect:
      return false
  let candidateScore = directRouteReachabilityScore(parseKnownAddress(sharedKeyToString(candidate.rawKey)))
  let incumbentScore = directRouteReachabilityScore(parseKnownAddress(sharedKeyToString(incumbent.rawKey)))
  if candidateScore != incumbentScore:
    return candidateScore > incumbentScore
  if candidate.hitCount != incumbent.hitCount:
    return candidate.hitCount > incumbent.hitCount
  if candidate.lastSelectedUnixMilli != incumbent.lastSelectedUnixMilli:
    return candidate.lastSelectedUnixMilli > incumbent.lastSelectedUnixMilli
  sharedKeyToString(candidate.rawKey) < sharedKeyToString(incumbent.rawKey)

proc lookupDirectTarget*(
    ownerId: int,
    family, ip: string,
    port: int,
    kind: TsnetProxyKind
): Result[TsnetDirectRouteHit, string] {.gcsafe.} =
  if ownerId <= 0:
    return err("invalid tsnet direct route owner id")
  let advertised = buildAdvertisedAddress(family, ip, port, kind).valueOr:
    return err(error)
  let advertisedKey = $advertised
  proxySafe:
    withLock(proxyRegistryLock):
      sanitizeDirectRoutesUnsafe()
      let nowUnixMilli = getTime().toUnix().int64 * 1000
      var selected = -1
      for idx, registration in proxyDirectRoutes.mpairs():
        if registration.ownerId != ownerId:
          continue
        if not sharedKeyEquals(registration.advertisedKey, advertisedKey):
          continue
        if registration.suspendedUntilUnixMilli > nowUnixMilli:
          continue
        if selected < 0 or compareDirectRoutePreference(registration, proxyDirectRoutes[selected]):
          selected = idx
      if selected >= 0:
        let selectedPathKind = sharedKeyToString(proxyDirectRoutes[selected].pathKind)
        inc proxyDirectRoutes[selected].hitCount
        proxyDirectRoutes[selected].lastSelectedUnixMilli = nowUnixMilli
        proxyDirectRoutes[selected].failureCount = 0
        proxyDirectRoutes[selected].suspendedUntilUnixMilli = 0
        if sharedKeyEquals(proxyDirectRoutes[selected].pathKind, TsnetPathPunchedDirect):
          replaceSharedKey(proxyDirectRoutes[selected].pathKind, TsnetPathDirect)
        return ok(TsnetDirectRouteHit(
          raw: parseKnownAddress(sharedKeyToString(proxyDirectRoutes[selected].rawKey)),
          pathKind: selectedPathKind
        ))
  err("no tsnet direct route is registered for " & advertisedKey)

proc markDirectRouteFailure*(
    ownerId: int,
    advertised: MultiAddress,
    raw: MultiAddress,
    cooldownMs = DirectRouteFailureBackoffMs
): Result[void, string] {.gcsafe.} =
  if ownerId <= 0:
    return err("invalid tsnet direct route owner id")
  let advertisedKey = $advertised
  let rawKey = $raw
  if advertisedKey.len == 0 or rawKey.len == 0:
    return err("invalid tsnet direct route registration")
  proxySafe:
    withLock(proxyRegistryLock):
      sanitizeDirectRoutesUnsafe()
      let nowUnixMilli = getTime().toUnix().int64 * 1000
      for registration in proxyDirectRoutes.mitems():
        if registration.ownerId != ownerId:
          continue
        if sharedKeyEquals(registration.advertisedKey, advertisedKey) and
            sharedKeyEquals(registration.rawKey, rawKey):
          inc registration.failureCount
          registration.suspendedUntilUnixMilli = nowUnixMilli + max(0'i64, cooldownMs)
          return ok()
  err("no tsnet direct route matches " & advertisedKey & " -> " & rawKey)

proc injectInvalidProxyRegistrationForTest*(
    ownerId: int,
    rawText: string
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      proxyRegistrations.add(TsnetProxyRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText)
      ))

proc injectInvalidResolvedRemoteRegistrationForTest*(
    ownerId: int,
    rawText: string
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      proxyResolvedRemotes.add(TsnetResolvedRemoteRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText)
      ))

proc injectInvalidDirectRouteRegistrationForTest*(
    ownerId: int,
    rawText: string,
    pathKind = TsnetPathDirect
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      proxyDirectRoutes.add(TsnetDirectRouteRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText),
        pathKind: initSharedKey(pathKind)
      ))
