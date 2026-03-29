{.push raises: [].}

import std/[locks, strutils, tables, times]

import ../../multiaddress
import ../../utility
import ../tsnetprovidertypes

type
  TsnetProxyKind* {.pure.} = enum
    Tcp
    Quic

  TsnetProxyRouteSnapshot* = object
    advertised*: MultiAddress
    raw*: MultiAddress
    kind*: TsnetProxyKind

  TsnetProxyRegistration* = object
    ownerId*: int
    advertisedKey*: string
    rawKey*: string

  TsnetResolvedRemoteRegistration* = object
    ownerId*: int
    rawKey*: string
    advertisedKey*: string

  TsnetDirectRouteRegistration* = object
    ownerId*: int
    advertisedKey*: string
    rawKey*: string
    pathKind*: string
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
var proxyOwnerRegistrations {.global.}: Table[int, seq[TsnetProxyRegistration]] =
  initTable[int, seq[TsnetProxyRegistration]]()
var proxyResolvedRemotes {.global.}: seq[TsnetResolvedRemoteRegistration]
var proxyOwnerResolvedRemotes {.global.}: Table[int, seq[TsnetResolvedRemoteRegistration]] =
  initTable[int, seq[TsnetResolvedRemoteRegistration]]()
var proxyDirectRoutes {.global.}: seq[TsnetDirectRouteRegistration]
var proxyOwnerDirectRoutes {.global.}: Table[int, seq[TsnetDirectRouteRegistration]] =
  initTable[int, seq[TsnetDirectRouteRegistration]]()

initLock(proxyRegistryLock)

template proxySafe(body: untyped) =
  {.cast(gcsafe).}:
    body

proc parseKnownAddress(text: string): MultiAddress =
  MultiAddress.init(text).expect("tsnet proxy constructed a valid multiaddr")

proc syncOwnerDirectRouteUnsafe(
    ownerId: int,
    advertisedKey: string,
    rawKey: string,
    pathKind: string,
    hitCount = 0,
    lastSelectedUnixMilli = 0'i64,
    failureCount = 0,
    suspendedUntilUnixMilli = 0'i64
) =
  var registrations = proxyOwnerDirectRoutes.getOrDefault(ownerId)
  var replaced = false
  for registration in registrations.mitems():
    if registration.advertisedKey == advertisedKey and registration.rawKey == rawKey:
      registration.rawKey = rawKey
      registration.pathKind = pathKind
      registration.hitCount = hitCount
      registration.lastSelectedUnixMilli = lastSelectedUnixMilli
      registration.failureCount = failureCount
      registration.suspendedUntilUnixMilli = suspendedUntilUnixMilli
      replaced = true
      break
  if not replaced:
    registrations.add(TsnetDirectRouteRegistration(
      ownerId: ownerId,
      advertisedKey: advertisedKey,
      rawKey: rawKey,
      pathKind: pathKind,
      hitCount: hitCount,
      lastSelectedUnixMilli: lastSelectedUnixMilli,
      failureCount: failureCount,
      suspendedUntilUnixMilli: suspendedUntilUnixMilli
    ))
  proxyOwnerDirectRoutes[ownerId] = registrations

proc splitAddressParts(address: MultiAddress): seq[string] =
  for part in ($address).split('/'):
    if part.len > 0:
      result.add(part)

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
  withLock(proxyRegistryLock):
    var found = false
    for registration in proxyRegistrations.mitems():
      if registration.advertisedKey == advertisedKey:
        if registration.rawKey != rawKey:
          return err("duplicate tsnet proxy registration for " & advertisedKey)
        registration.ownerId = ownerId
        found = true
        break
    if not found:
      proxyRegistrations.add(TsnetProxyRegistration(
        ownerId: ownerId,
        advertisedKey: advertisedKey,
        rawKey: rawKey
      ))
    var registrations = proxyOwnerRegistrations.getOrDefault(ownerId)
    var replaced = false
    for idx, registration in registrations.mpairs():
      if registration.advertisedKey == advertisedKey:
        registration.rawKey = rawKey
        replaced = true
        break
    if not replaced:
      registrations.add(TsnetProxyRegistration(
        ownerId: ownerId,
        advertisedKey: advertisedKey,
        rawKey: rawKey
      ))
    proxyOwnerRegistrations[ownerId] = registrations
  ok()

proc unregisterProxyRoutes*(ownerId: int) =
  if ownerId <= 0:
    return
  proxySafe:
    withLock(proxyRegistryLock):
      var keptRoutes: seq[TsnetProxyRegistration] = @[]
      for registration in proxyRegistrations.items:
        if registration.ownerId != ownerId:
          keptRoutes.add(registration)
      proxyRegistrations = keptRoutes

      var rebuiltOwnerRegistrations = initTable[int, seq[TsnetProxyRegistration]]()
      for registration in proxyRegistrations.items:
        var registrations = rebuiltOwnerRegistrations.getOrDefault(registration.ownerId)
        registrations.add(registration)
        rebuiltOwnerRegistrations[registration.ownerId] = registrations
      proxyOwnerRegistrations = rebuiltOwnerRegistrations

      var keptResolved: seq[TsnetResolvedRemoteRegistration] = @[]
      for registration in proxyResolvedRemotes.items:
        if registration.ownerId != ownerId:
          keptResolved.add(registration)
      proxyResolvedRemotes = keptResolved

      var rebuiltResolvedRemotes = initTable[int, seq[TsnetResolvedRemoteRegistration]]()
      for registration in proxyResolvedRemotes.items:
        var registrations = rebuiltResolvedRemotes.getOrDefault(registration.ownerId)
        registrations.add(registration)
        rebuiltResolvedRemotes[registration.ownerId] = registrations
      proxyOwnerResolvedRemotes = rebuiltResolvedRemotes

      var keptDirect: seq[TsnetDirectRouteRegistration] = @[]
      for registration in proxyDirectRoutes.items:
        if registration.ownerId != ownerId:
          keptDirect.add(registration)
      proxyDirectRoutes = keptDirect

      var rebuiltDirectRoutes = initTable[int, seq[TsnetDirectRouteRegistration]]()
      for registration in proxyDirectRoutes.items:
        var registrations = rebuiltDirectRoutes.getOrDefault(registration.ownerId)
        registrations.add(registration)
        rebuiltDirectRoutes[registration.ownerId] = registrations
      proxyOwnerDirectRoutes = rebuiltDirectRoutes

proc proxyRouteSnapshots*(ownerId: int): seq[TsnetProxyRouteSnapshot] {.gcsafe.} =
  if ownerId <= 0:
    return @[]
  proxySafe:
    withLock(proxyRegistryLock):
      let registrations = proxyOwnerRegistrations.getOrDefault(ownerId)
      for registration in registrations.items:
        let advertised = parseKnownAddress(registration.advertisedKey)
        let raw = parseKnownAddress(registration.rawKey)
        let kind =
          if "/udp/" in registration.advertisedKey.toLowerAscii() and
              "/quic-v1" in registration.advertisedKey.toLowerAscii():
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
  withLock(proxyRegistryLock):
    for registration in proxyRegistrations.items:
      if registration.advertisedKey == $advertised:
        return ok(parseKnownAddress(registration.rawKey))
  err("no tsnet proxy route is registered for " & $advertised)

proc resolveAdvertisedRemote*(raw: MultiAddress): Result[MultiAddress, string] =
  withLock(proxyRegistryLock):
    for registration in proxyResolvedRemotes.items:
      if registration.rawKey == $raw:
        return ok(parseKnownAddress(registration.advertisedKey))
    for registration in proxyRegistrations.items:
      if registration.rawKey == $raw:
        return ok(parseKnownAddress(registration.advertisedKey))
  err("no tsnet proxy remote is registered for " & $raw)

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
    withLock(proxyRegistryLock):
      var replaced = false
      for registration in proxyResolvedRemotes.mitems():
        if registration.rawKey == rawKey:
          registration.ownerId = ownerId
          registration.advertisedKey = advertisedKey
          replaced = true
          break
      if not replaced:
        proxyResolvedRemotes.add(TsnetResolvedRemoteRegistration(
          ownerId: ownerId,
          rawKey: rawKey,
          advertisedKey: advertisedKey
        ))
      var registrations = proxyOwnerResolvedRemotes.getOrDefault(ownerId)
      var ownerReplaced = false
      for registration in registrations.mitems():
        if registration.rawKey == rawKey:
          registration.advertisedKey = advertisedKey
          ownerReplaced = true
          break
      if not ownerReplaced:
        registrations.add(TsnetResolvedRemoteRegistration(
          ownerId: ownerId,
          rawKey: rawKey,
          advertisedKey: advertisedKey
        ))
      proxyOwnerResolvedRemotes[ownerId] = registrations

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
  proxySafe:
    withLock(proxyRegistryLock):
      var found = false
      for registration in proxyDirectRoutes.mitems():
        if registration.advertisedKey == advertisedKey and registration.rawKey == rawKey:
          registration.ownerId = ownerId
          registration.rawKey = rawKey
          registration.pathKind = normalizedPath
          found = true
          break
      if not found:
        proxyDirectRoutes.add(TsnetDirectRouteRegistration(
          ownerId: ownerId,
          advertisedKey: advertisedKey,
          rawKey: rawKey,
          pathKind: normalizedPath,
          hitCount: 0,
          lastSelectedUnixMilli: 0,
          failureCount: 0,
          suspendedUntilUnixMilli: 0
        ))
      syncOwnerDirectRouteUnsafe(ownerId, advertisedKey, rawKey, normalizedPath)
  ok()

proc directRouteSnapshots*(ownerId: int): seq[TsnetDirectRouteSnapshot] {.gcsafe.} =
  if ownerId <= 0:
    return @[]
  proxySafe:
    withLock(proxyRegistryLock):
      for registration in proxyDirectRoutes.items:
        if registration.ownerId != ownerId:
          continue
        result.add(TsnetDirectRouteSnapshot(
          advertised: parseKnownAddress(registration.advertisedKey),
          raw: parseKnownAddress(registration.rawKey),
          pathKind: registration.pathKind,
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

  let candidatePath = normalizeTailnetPath(candidate.pathKind)
  let incumbentPath = normalizeTailnetPath(incumbent.pathKind)
  if candidatePath != incumbentPath:
    if candidatePath == TsnetPathDirect:
      return true
    if incumbentPath == TsnetPathDirect:
      return false
    if candidatePath == TsnetPathPunchedDirect:
      return true
    if incumbentPath == TsnetPathPunchedDirect:
      return false
  let candidateScore = directRouteReachabilityScore(parseKnownAddress(candidate.rawKey))
  let incumbentScore = directRouteReachabilityScore(parseKnownAddress(incumbent.rawKey))
  if candidateScore != incumbentScore:
    return candidateScore > incumbentScore
  if candidate.hitCount != incumbent.hitCount:
    return candidate.hitCount > incumbent.hitCount
  if candidate.lastSelectedUnixMilli != incumbent.lastSelectedUnixMilli:
    return candidate.lastSelectedUnixMilli > incumbent.lastSelectedUnixMilli
  candidate.rawKey < incumbent.rawKey

proc lookupDirectTarget*(
    family, ip: string,
    port: int,
    kind: TsnetProxyKind
): Result[TsnetDirectRouteHit, string] {.gcsafe.} =
  let advertised = buildAdvertisedAddress(family, ip, port, kind).valueOr:
    return err(error)
  proxySafe:
    withLock(proxyRegistryLock):
      let nowUnixMilli = getTime().toUnix().int64 * 1000
      var selected = -1
      for idx, registration in proxyDirectRoutes.mpairs():
        if registration.advertisedKey != $advertised:
          continue
        if registration.suspendedUntilUnixMilli > nowUnixMilli:
          continue
        if selected < 0 or compareDirectRoutePreference(registration, proxyDirectRoutes[selected]):
          selected = idx
      if selected >= 0:
        let selectedPathKind = proxyDirectRoutes[selected].pathKind
        inc proxyDirectRoutes[selected].hitCount
        proxyDirectRoutes[selected].lastSelectedUnixMilli = nowUnixMilli
        proxyDirectRoutes[selected].failureCount = 0
        proxyDirectRoutes[selected].suspendedUntilUnixMilli = 0
        if proxyDirectRoutes[selected].pathKind == TsnetPathPunchedDirect:
          proxyDirectRoutes[selected].pathKind = TsnetPathDirect
        syncOwnerDirectRouteUnsafe(
          proxyDirectRoutes[selected].ownerId,
          proxyDirectRoutes[selected].advertisedKey,
          proxyDirectRoutes[selected].rawKey,
          proxyDirectRoutes[selected].pathKind,
          proxyDirectRoutes[selected].hitCount,
          proxyDirectRoutes[selected].lastSelectedUnixMilli,
          proxyDirectRoutes[selected].failureCount,
          proxyDirectRoutes[selected].suspendedUntilUnixMilli
        )
        return ok(TsnetDirectRouteHit(
          raw: parseKnownAddress(proxyDirectRoutes[selected].rawKey),
          pathKind: selectedPathKind
        ))
  err("no tsnet direct route is registered for " & $advertised)

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
  proxySafe:
    withLock(proxyRegistryLock):
      let nowUnixMilli = getTime().toUnix().int64 * 1000
      for registration in proxyDirectRoutes.mitems():
        if registration.ownerId != ownerId:
          continue
        if registration.advertisedKey == advertisedKey and registration.rawKey == rawKey:
          inc registration.failureCount
          registration.suspendedUntilUnixMilli = nowUnixMilli + max(0'i64, cooldownMs)
          syncOwnerDirectRouteUnsafe(
            registration.ownerId,
            registration.advertisedKey,
            registration.rawKey,
            registration.pathKind,
            registration.hitCount,
            registration.lastSelectedUnixMilli,
            registration.failureCount,
            registration.suspendedUntilUnixMilli
          )
          return ok()
  err("no tsnet direct route matches " & advertisedKey & " -> " & rawKey)
