{.push raises: [].}

import std/[locks, strutils, tables, times]

import ../../multiaddress
import ../../utility
import ../tsnetprovidertypes

type
  TsnetSharedKey = object
    len: int
    data: array[1536, char]

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

  TsnetProxyRouteCacheEntry = object
    ownerId*: int
    advertisedKey*: TsnetSharedKey
    rawKey*: TsnetSharedKey
    kind*: TsnetProxyKind

  TsnetProxyRegistrationGroup = OrderedTable[string, TsnetProxyRegistration]
  TsnetProxyRegistrationTable = OrderedTable[string, TsnetProxyRegistrationGroup]
  TsnetDirectRouteGroup = OrderedTable[string, TsnetDirectRouteRegistration]
  TsnetDirectRouteTable = OrderedTable[string, TsnetDirectRouteGroup]

  TsnetResolvedRemoteRegistration* = object
    ownerId*: int
    rawKey*: TsnetSharedKey
    advertisedKey*: TsnetSharedKey
    rawText*: string
    advertisedText*: string

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

  TsnetDirectRouteCacheEntry = object
    ownerId*: int
    advertisedKey*: TsnetSharedKey
    rawKey*: TsnetSharedKey
    pathKind*: TsnetSharedKey
    hitCount*: int
    lastSelectedUnixMilli*: int64
    failureCount*: int
    suspendedUntilUnixMilli*: int64

const
  DirectRouteFailureBackoffMs* = 5_000'i64

type
  TsnetResolvedRemoteCacheEntry = object
    ownerId*: int
    rawKey*: TsnetSharedKey
    advertisedKey*: TsnetSharedKey

var proxyRegistryLock {.global.}: Lock
var proxyRegistrations {.global.}: TsnetProxyRegistrationTable =
  initOrderedTable[string, TsnetProxyRegistrationGroup]()
var proxyRegistrationSnapshots {.global.}: seq[TsnetProxyRouteCacheEntry]
var proxyRouteCounts {.global.}: Table[int, int] = initTable[int, int]()
var proxyResolvedRemotes {.global.}: Table[string, TsnetResolvedRemoteRegistration] =
  initTable[string, TsnetResolvedRemoteRegistration]()
var proxyResolvedRemoteSnapshots {.global.}: seq[TsnetResolvedRemoteCacheEntry]
var proxyDirectRoutes {.global.}: TsnetDirectRouteTable =
  initOrderedTable[string, TsnetDirectRouteGroup]()
var proxyDirectRouteSnapshots {.global.}: seq[TsnetDirectRouteCacheEntry]

initLock(proxyRegistryLock)

template proxySafe(body: untyped) =
  {.cast(gcsafe).}:
    body

proc parseKnownAddress(text: string): MultiAddress =
  MultiAddress.init(text).expect("tsnet proxy constructed a valid multiaddr")

proc initSharedKey(value: string): TsnetSharedKey =
  if value.len > result.data.len:
    raiseAssert(
      "tsnet shared key exceeds fixed storage len=" & $value.len &
      " cap=" & $result.data.len
    )
  result.len = value.len
  for idx, ch in value:
    result.data[idx] = ch

proc clearSharedKey(value: var TsnetSharedKey) =
  value.len = 0

proc sharedKeyLenInBounds(value: TsnetSharedKey): bool =
  value.len >= 0 and value.len <= value.data.len

proc sharedKeyToString(value: TsnetSharedKey): string =
  if value.len <= 0:
    return ""
  result = newString(value.len)
  for idx in 0 ..< value.len:
    result[idx] = value.data[idx]

proc sharedKeyEquals(value: TsnetSharedKey, expected: string): bool =
  if not sharedKeyLenInBounds(value):
    return false
  if value.len != expected.len:
    return false
  for idx in 0 ..< expected.len:
    if value.data[idx] != expected[idx]:
      return false
  true

proc validSharedKey(value: TsnetSharedKey): bool =
  sharedKeyLenInBounds(value) and value.len > 0

proc validSharedAddressKey(value: TsnetSharedKey): bool =
  if not validSharedKey(value):
    return false
  MultiAddress.init(sharedKeyToString(value)).isOk()

proc replaceSharedKey(value: var TsnetSharedKey, text: string) =
  if sharedKeyEquals(value, text):
    return
  if text.len > value.data.len:
    raiseAssert(
      "tsnet shared key exceeds fixed storage len=" & $text.len &
      " cap=" & $value.data.len
    )
  clearSharedKey(value)
  value.len = text.len
  for idx, ch in text:
    value.data[idx] = ch

proc freeProxyRegistration(registration: var TsnetProxyRegistration) =
  clearSharedKey(registration.advertisedKey)
  clearSharedKey(registration.rawKey)
  registration.ownerId = 0

proc freeResolvedRemoteRegistration(registration: var TsnetResolvedRemoteRegistration) =
  clearSharedKey(registration.rawKey)
  clearSharedKey(registration.advertisedKey)
  registration.ownerId = 0
  registration.rawText = ""
  registration.advertisedText = ""

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
    validSharedAddressKey(registration.advertisedKey) and
    validSharedAddressKey(registration.rawKey)

proc validResolvedRemoteRegistration(
    registration: TsnetResolvedRemoteRegistration
): bool =
  registration.ownerId > 0 and
    validSharedAddressKey(registration.rawKey) and
    validSharedAddressKey(registration.advertisedKey)

proc validDirectRouteRegistration(
    registration: TsnetDirectRouteRegistration
): bool =
  registration.ownerId > 0 and
    validSharedAddressKey(registration.advertisedKey) and
    validSharedAddressKey(registration.rawKey) and
    validSharedKey(registration.pathKind)

proc initDirectRouteGroup(): TsnetDirectRouteGroup =
  initOrderedTable[string, TsnetDirectRouteRegistration]()

proc initProxyRegistrationGroup(): TsnetProxyRegistrationGroup =
  initOrderedTable[string, TsnetProxyRegistration]()

proc compactProxyRegistrationGroup(
    source: TsnetProxyRegistrationGroup
): TsnetProxyRegistrationGroup =
  result = initProxyRegistrationGroup()
  for _, registration in source.pairs:
    if not validProxyRegistration(registration):
      continue
    let rawKey = sharedKeyToString(registration.rawKey)
    if rawKey.len == 0:
      continue
    result[rawKey] = registration

proc compactDirectRouteGroup(source: TsnetDirectRouteGroup): TsnetDirectRouteGroup =
  result = initDirectRouteGroup()
  for _, registration in source.pairs:
    if not validDirectRouteRegistration(registration):
      continue
    let rawKey = sharedKeyToString(registration.rawKey)
    if rawKey.len == 0:
      continue
    result[rawKey] = registration

proc kindFromAdvertisedKey(advertisedKey: string): TsnetProxyKind =
  if "/udp/" in advertisedKey.toLowerAscii() and
      "/quic-v1" in advertisedKey.toLowerAscii():
    TsnetProxyKind.Quic
  else:
    TsnetProxyKind.Tcp

proc compactProxyRegistrations(
    source: TsnetProxyRegistrationTable
): TsnetProxyRegistrationTable =
  result = initOrderedTable[string, TsnetProxyRegistrationGroup]()
  for _, registrations in source.pairs:
    for _, registration in registrations.pairs:
      if not validProxyRegistration(registration):
        continue
      let advertisedKey = sharedKeyToString(registration.advertisedKey)
      let rawKey = sharedKeyToString(registration.rawKey)
      if advertisedKey.len == 0 or rawKey.len == 0:
        continue
      var group = result.getOrDefault(advertisedKey, initProxyRegistrationGroup())
      group[rawKey] = registration
      result[advertisedKey] = move(group)

proc compactResolvedRemoteRegistrations(
    source: Table[string, TsnetResolvedRemoteRegistration]
): Table[string, TsnetResolvedRemoteRegistration] =
  result = initTable[string, TsnetResolvedRemoteRegistration]()
  for _, registration in source.pairs:
    if not validResolvedRemoteRegistration(registration):
      continue
    let rawKey = sharedKeyToString(registration.rawKey)
    if rawKey.len == 0:
      continue
    result[rawKey] = registration

proc refreshProxyRouteSnapshotsUnsafe() =
  var snapshots: seq[TsnetProxyRouteCacheEntry] = @[]
  var counts = initTable[int, int]()
  for _, registrations in proxyRegistrations.pairs:
    for _, registration in registrations.pairs:
      if not validProxyRegistration(registration):
        continue
      let advertisedKey = sharedKeyToString(registration.advertisedKey)
      counts[registration.ownerId] = counts.getOrDefault(registration.ownerId, 0) + 1
      snapshots.add(TsnetProxyRouteCacheEntry(
        ownerId: registration.ownerId,
        advertisedKey: registration.advertisedKey,
        rawKey: registration.rawKey,
        kind: kindFromAdvertisedKey(advertisedKey)
      ))
  proxyRegistrationSnapshots = move(snapshots)
  proxyRouteCounts = move(counts)

proc upsertProxyRouteSnapshotUnsafe(registration: TsnetProxyRegistration) =
  if not validProxyRegistration(registration):
    return
  let advertisedKey = sharedKeyToString(registration.advertisedKey)
  let rawKey = sharedKeyToString(registration.rawKey)
  if advertisedKey.len == 0 or rawKey.len == 0:
    return
  let next = TsnetProxyRouteCacheEntry(
    ownerId: registration.ownerId,
    advertisedKey: registration.advertisedKey,
    rawKey: registration.rawKey,
    kind: kindFromAdvertisedKey(advertisedKey)
  )
  for idx in 0 ..< proxyRegistrationSnapshots.len:
    if sharedKeyEquals(proxyRegistrationSnapshots[idx].advertisedKey, advertisedKey) and
        sharedKeyEquals(proxyRegistrationSnapshots[idx].rawKey, rawKey):
      proxyRegistrationSnapshots[idx] = next
      return
  proxyRegistrationSnapshots.add(next)
  proxyRouteCounts[registration.ownerId] = proxyRouteCounts.getOrDefault(registration.ownerId, 0) + 1

proc sanitizeProxyRoutesUnsafe() =
  proxyRegistrations = compactProxyRegistrations(proxyRegistrations)
  refreshProxyRouteSnapshotsUnsafe()

proc refreshResolvedRemoteSnapshotsUnsafe() =
  var snapshots = newSeqOfCap[TsnetResolvedRemoteCacheEntry](proxyResolvedRemotes.len)
  for _, registration in proxyResolvedRemotes.pairs:
    if validResolvedRemoteRegistration(registration):
      snapshots.add(TsnetResolvedRemoteCacheEntry(
        ownerId: registration.ownerId,
        rawKey: registration.rawKey,
        advertisedKey: registration.advertisedKey
      ))
  proxyResolvedRemoteSnapshots = move(snapshots)

proc refreshDirectRouteSnapshotsUnsafe() =
  var snapshots: seq[TsnetDirectRouteCacheEntry] = @[]
  for _, routes in proxyDirectRoutes.pairs:
    for _, registration in routes.pairs:
      if not validDirectRouteRegistration(registration):
        continue
      snapshots.add(TsnetDirectRouteCacheEntry(
        ownerId: registration.ownerId,
        advertisedKey: registration.advertisedKey,
        rawKey: registration.rawKey,
        pathKind: registration.pathKind,
        hitCount: registration.hitCount,
        lastSelectedUnixMilli: registration.lastSelectedUnixMilli,
        failureCount: registration.failureCount,
        suspendedUntilUnixMilli: registration.suspendedUntilUnixMilli
      ))
  proxyDirectRouteSnapshots = move(snapshots)

proc sanitizeResolvedRemotesUnsafe() =
  proxyResolvedRemotes = compactResolvedRemoteRegistrations(proxyResolvedRemotes)
  refreshResolvedRemoteSnapshotsUnsafe()

proc sanitizeDirectRoutesUnsafe() =
  var kept = initOrderedTable[string, TsnetDirectRouteGroup]()
  for _, routes in proxyDirectRoutes.pairs:
    for _, registration in routes.pairs:
      if not validDirectRouteRegistration(registration):
        continue
      let advertisedKey = sharedKeyToString(registration.advertisedKey)
      let rawKey = sharedKeyToString(registration.rawKey)
      if advertisedKey.len == 0 or rawKey.len == 0:
        continue
      var group = kept.getOrDefault(advertisedKey, initDirectRouteGroup())
      group[rawKey] = registration
      kept[advertisedKey] = move(group)
  proxyDirectRoutes = move(kept)
  refreshDirectRouteSnapshotsUnsafe()

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
    var finalRegistration = TsnetProxyRegistration()
    var registrations = compactProxyRegistrationGroup(
      proxyRegistrations.getOrDefault(advertisedKey, initProxyRegistrationGroup())
    )
    let existing = registrations.getOrDefault(rawKey)
    if validProxyRegistration(existing):
      var registration = existing
      registration.ownerId = ownerId
      registrations[rawKey] = registration
      finalRegistration = registration
    elif registrations.len > 0:
      for existingRawKey, existingRegistration in registrations.pairs:
        if validProxyRegistration(existingRegistration) and existingRawKey != rawKey:
          return err("duplicate tsnet proxy registration for " & advertisedKey)
    else:
      let registration = TsnetProxyRegistration(
        ownerId: ownerId,
        advertisedKey: initSharedKey(advertisedKey),
        rawKey: initSharedKey(rawKey)
      )
      registrations[rawKey] = registration
      finalRegistration = registration
    proxyRegistrations[advertisedKey] = move(registrations)
    upsertProxyRouteSnapshotUnsafe(finalRegistration)
  ok()

proc unregisterProxyRoutes*(ownerId: int) =
  if ownerId <= 0:
    return
  proxySafe:
    withLock(proxyRegistryLock):
      sanitizeProxyRegistryUnsafe()
      var keptRoutes = initOrderedTable[string, TsnetProxyRegistrationGroup]()
      for _, registrations in proxyRegistrations.pairs():
        var keptGroup = initProxyRegistrationGroup()
        var keptAdvertisedKey = ""
        for _, registration in registrations.pairs():
          if registration.ownerId != ownerId:
            let rawKey = sharedKeyToString(registration.rawKey)
            keptGroup[rawKey] = registration
            if keptAdvertisedKey.len == 0:
              keptAdvertisedKey = sharedKeyToString(registration.advertisedKey)
        if keptGroup.len > 0 and keptAdvertisedKey.len > 0:
          keptRoutes[keptAdvertisedKey] = move(keptGroup)
      proxyRegistrations = keptRoutes
      refreshProxyRouteSnapshotsUnsafe()

      var keptResolved = initTable[string, TsnetResolvedRemoteRegistration]()
      for _, registration in proxyResolvedRemotes.pairs():
        if registration.ownerId != ownerId:
          let rawKey = sharedKeyToString(registration.rawKey)
          if rawKey.len > 0:
            keptResolved[rawKey] = registration
      proxyResolvedRemotes = keptResolved
      refreshResolvedRemoteSnapshotsUnsafe()

      var keptDirect = initOrderedTable[string, TsnetDirectRouteGroup]()
      for _, routes in proxyDirectRoutes.pairs():
        var keptGroup = initDirectRouteGroup()
        var keptAdvertisedKey = ""
        for _, registration in routes.pairs():
          if registration.ownerId != ownerId:
            let rawKey = sharedKeyToString(registration.rawKey)
            keptGroup[rawKey] = registration
            if keptAdvertisedKey.len == 0:
              keptAdvertisedKey = sharedKeyToString(registration.advertisedKey)
        if keptGroup.len > 0 and keptAdvertisedKey.len > 0:
          keptDirect[keptAdvertisedKey] = move(keptGroup)
      proxyDirectRoutes = keptDirect
      refreshDirectRouteSnapshotsUnsafe()

proc proxyRouteSnapshots*(ownerId: int): seq[TsnetProxyRouteSnapshot] {.gcsafe.} =
  if ownerId <= 0:
    return @[]
  proxySafe:
    var snapshots: seq[TsnetProxyRouteCacheEntry] = @[]
    withLock(proxyRegistryLock):
      snapshots = newSeqOfCap[TsnetProxyRouteCacheEntry](proxyRegistrationSnapshots.len)
      for snapshot in proxyRegistrationSnapshots.items:
        if snapshot.ownerId == ownerId:
          snapshots.add(snapshot)
    for snapshot in snapshots:
      result.add(TsnetProxyRouteSnapshot(
        advertised: parseKnownAddress(sharedKeyToString(snapshot.advertisedKey)),
        raw: parseKnownAddress(sharedKeyToString(snapshot.rawKey)),
        kind: snapshot.kind
      ))

proc proxyRouteCount*(ownerId: int): int {.gcsafe.} =
  if ownerId <= 0:
    return 0
  proxySafe:
    withLock(proxyRegistryLock):
      result = proxyRouteCounts.getOrDefault(ownerId, 0)

proc lookupRawTarget*(
    family, ip: string,
    port: int,
    kind: TsnetProxyKind
): Result[MultiAddress, string] =
  let advertised = buildAdvertisedAddress(family, ip, port, kind).valueOr:
    return err(error)
  let advertisedKey = $advertised
  withLock(proxyRegistryLock):
    for snapshot in proxyRegistrationSnapshots.items:
      if sharedKeyEquals(snapshot.advertisedKey, advertisedKey):
        return ok(parseKnownAddress(sharedKeyToString(snapshot.rawKey)))
  err("no tsnet proxy route is registered for " & advertisedKey)

proc resolveAdvertisedRemote*(raw: MultiAddress): Result[MultiAddress, string] =
  let rawKey = $raw
  var advertisedKey = ""
  withLock(proxyRegistryLock):
    for registration in proxyResolvedRemoteSnapshots.items:
      if sharedKeyEquals(registration.rawKey, rawKey):
        advertisedKey = sharedKeyToString(registration.advertisedKey)
        break
    if advertisedKey.len == 0:
      for snapshot in proxyRegistrationSnapshots.items:
        if sharedKeyEquals(snapshot.rawKey, rawKey):
          advertisedKey = sharedKeyToString(snapshot.advertisedKey)
          break
  if advertisedKey.len > 0:
    return ok(parseKnownAddress(advertisedKey))
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
      let existing = proxyResolvedRemotes.getOrDefault(rawKey)
      if validResolvedRemoteRegistration(existing):
        var registration = existing
        registration.ownerId = ownerId
        replaceSharedKey(registration.rawKey, rawKey)
        replaceSharedKey(registration.advertisedKey, advertisedKey)
        registration.rawText = rawKey
        registration.advertisedText = advertisedKey
        proxyResolvedRemotes[rawKey] = registration
      else:
        proxyResolvedRemotes[rawKey] = TsnetResolvedRemoteRegistration(
          ownerId: ownerId,
          rawKey: initSharedKey(rawKey),
          advertisedKey: initSharedKey(advertisedKey),
          rawText: rawKey,
          advertisedText: advertisedKey
        )
      refreshResolvedRemoteSnapshotsUnsafe()

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
      var routes = compactDirectRouteGroup(
        proxyDirectRoutes.getOrDefault(advertisedKey, initDirectRouteGroup())
      )
      let existing = routes.getOrDefault(rawKey)
      if validDirectRouteRegistration(existing):
        var registration = existing
        registration.ownerId = ownerId
        replaceSharedKey(registration.pathKind, normalizedPath)
        routes[rawKey] = registration
      else:
        routes[rawKey] = TsnetDirectRouteRegistration(
          ownerId: ownerId,
          advertisedKey: initSharedKey(advertisedKey),
          rawKey: initSharedKey(rawKey),
          pathKind: initSharedKey(normalizedPath),
          hitCount: 0,
          lastSelectedUnixMilli: 0,
          failureCount: 0,
          suspendedUntilUnixMilli: 0
        )
      proxyDirectRoutes[advertisedKey] = move(routes)
      refreshDirectRouteSnapshotsUnsafe()
  ok()

proc directRouteSnapshots*(ownerId: int): seq[TsnetDirectRouteSnapshot] {.gcsafe.} =
  if ownerId <= 0:
    return @[]
  proxySafe:
    var snapshots: seq[TsnetDirectRouteCacheEntry] = @[]
    withLock(proxyRegistryLock):
      snapshots = newSeqOfCap[TsnetDirectRouteCacheEntry](proxyDirectRouteSnapshots.len)
      for snapshot in proxyDirectRouteSnapshots.items:
        if snapshot.ownerId == ownerId:
          snapshots.add(snapshot)
    for snapshot in snapshots:
      result.add(TsnetDirectRouteSnapshot(
        advertised: parseKnownAddress(sharedKeyToString(snapshot.advertisedKey)),
        raw: parseKnownAddress(sharedKeyToString(snapshot.rawKey)),
        pathKind: sharedKeyToString(snapshot.pathKind),
        hitCount: snapshot.hitCount,
        lastSelectedUnixMilli: snapshot.lastSelectedUnixMilli,
        failureCount: snapshot.failureCount,
        suspendedUntilUnixMilli: snapshot.suspendedUntilUnixMilli
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
      var routes = proxyDirectRoutes.getOrDefault(advertisedKey, initDirectRouteGroup())
      var selectedKey = ""
      var selected = TsnetDirectRouteRegistration()
      for rawRouteKey, registration in routes.pairs():
        if registration.ownerId != ownerId:
          continue
        if registration.suspendedUntilUnixMilli > nowUnixMilli:
          continue
        if selectedKey.len == 0 or compareDirectRoutePreference(registration, selected):
          selectedKey = rawRouteKey
          selected = registration
      if selectedKey.len > 0:
        let selectedPathKind = sharedKeyToString(selected.pathKind)
        inc selected.hitCount
        selected.lastSelectedUnixMilli = nowUnixMilli
        selected.failureCount = 0
        selected.suspendedUntilUnixMilli = 0
        if sharedKeyEquals(selected.pathKind, TsnetPathPunchedDirect):
          replaceSharedKey(selected.pathKind, TsnetPathDirect)
        routes[selectedKey] = selected
        proxyDirectRoutes[advertisedKey] = move(routes)
        refreshDirectRouteSnapshotsUnsafe()
        return ok(TsnetDirectRouteHit(
          raw: parseKnownAddress(sharedKeyToString(selected.rawKey)),
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
      var routes = proxyDirectRoutes.getOrDefault(advertisedKey, initDirectRouteGroup())
      let existing = routes.getOrDefault(rawKey)
      if validDirectRouteRegistration(existing) and existing.ownerId == ownerId:
        var registration = existing
        inc registration.failureCount
        registration.suspendedUntilUnixMilli = nowUnixMilli + max(0'i64, cooldownMs)
        routes[rawKey] = registration
        proxyDirectRoutes[advertisedKey] = move(routes)
        refreshDirectRouteSnapshotsUnsafe()
        return ok()
  err("no tsnet direct route matches " & advertisedKey & " -> " & rawKey)

proc injectInvalidProxyRegistrationForTest*(
    ownerId: int,
    rawText: string
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      var next = compactProxyRegistrations(proxyRegistrations)
      var registrations = next.getOrDefault(rawText, initProxyRegistrationGroup())
      registrations[rawText] = TsnetProxyRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText)
      )
      next[rawText] = move(registrations)
      proxyRegistrations = move(next)
      refreshProxyRouteSnapshotsUnsafe()

proc injectMalformedProxyRegistrationForTest*(
    ownerId: int,
    advertisedText: string,
    rawText: string
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      var next = compactProxyRegistrations(proxyRegistrations)
      var registrations = next.getOrDefault(advertisedText, initProxyRegistrationGroup())
      registrations[rawText] = TsnetProxyRegistration(
        ownerId: ownerId,
        advertisedKey: initSharedKey(advertisedText),
        rawKey: initSharedKey(rawText)
      )
      next[advertisedText] = move(registrations)
      proxyRegistrations = move(next)
      refreshProxyRouteSnapshotsUnsafe()

proc injectInvalidResolvedRemoteRegistrationForTest*(
    ownerId: int,
    rawText: string
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      proxyResolvedRemotes[rawText] = TsnetResolvedRemoteRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText),
        rawText: rawText
      )
      refreshResolvedRemoteSnapshotsUnsafe()

proc injectMalformedResolvedRemoteRegistrationForTest*(
    ownerId: int,
    rawText: string,
    advertisedText: string
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      proxyResolvedRemotes[rawText] = TsnetResolvedRemoteRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText),
        advertisedKey: initSharedKey(advertisedText),
        rawText: rawText,
        advertisedText: advertisedText
      )
      refreshResolvedRemoteSnapshotsUnsafe()

proc injectCorruptProxyRegistrationLenForTest*(
    ownerId: int,
    advertisedText: string,
    rawText: string,
    advertisedLen: int = -1,
    rawLen: int = -1
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      var registration = TsnetProxyRegistration(
        ownerId: ownerId,
        advertisedKey: initSharedKey(advertisedText),
        rawKey: initSharedKey(rawText)
      )
      if advertisedLen >= 0:
        registration.advertisedKey.len = advertisedLen
      if rawLen >= 0:
        registration.rawKey.len = rawLen
      var next = compactProxyRegistrations(proxyRegistrations)
      var registrations = next.getOrDefault(advertisedText, initProxyRegistrationGroup())
      registrations[rawText] = registration
      next[advertisedText] = move(registrations)
      proxyRegistrations = move(next)
      refreshProxyRouteSnapshotsUnsafe()

proc injectCorruptResolvedRemoteRegistrationLenForTest*(
    ownerId: int,
    rawText: string,
    advertisedText: string,
    rawLen: int = -1,
    advertisedLen: int = -1
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      var registration = TsnetResolvedRemoteRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText),
        advertisedKey: initSharedKey(advertisedText),
        rawText: rawText,
        advertisedText: advertisedText
      )
      if rawLen >= 0:
        registration.rawKey.len = rawLen
      if advertisedLen >= 0:
        registration.advertisedKey.len = advertisedLen
      proxyResolvedRemotes[rawText] = registration
      refreshResolvedRemoteSnapshotsUnsafe()

proc injectResolvedRemoteWithDetachedTextsForTest*(
    ownerId: int,
    rawKeyText: string,
    advertisedKeyText: string,
    rawText = "",
    advertisedText = ""
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      proxyResolvedRemotes[rawKeyText] = TsnetResolvedRemoteRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawKeyText),
        advertisedKey: initSharedKey(advertisedKeyText),
        rawText: rawText,
        advertisedText: advertisedText
      )
      refreshResolvedRemoteSnapshotsUnsafe()

proc injectInvalidDirectRouteRegistrationForTest*(
    ownerId: int,
    rawText: string,
    pathKind = TsnetPathDirect
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      var routes = proxyDirectRoutes.getOrDefault(rawText, initDirectRouteGroup())
      routes[rawText] = TsnetDirectRouteRegistration(
        ownerId: ownerId,
        rawKey: initSharedKey(rawText),
        pathKind: initSharedKey(pathKind)
      )
      proxyDirectRoutes[rawText] = move(routes)
      refreshDirectRouteSnapshotsUnsafe()

proc injectCorruptDirectRouteRegistrationLenForTest*(
    ownerId: int,
    advertisedText: string,
    rawText: string,
    pathKind = TsnetPathDirect,
    advertisedLen: int = -1,
    rawLen: int = -1,
    pathLen: int = -1
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      var registration = TsnetDirectRouteRegistration(
        ownerId: ownerId,
        advertisedKey: initSharedKey(advertisedText),
        rawKey: initSharedKey(rawText),
        pathKind: initSharedKey(pathKind)
      )
      if advertisedLen >= 0:
        registration.advertisedKey.len = advertisedLen
      if rawLen >= 0:
        registration.rawKey.len = rawLen
      if pathLen >= 0:
        registration.pathKind.len = pathLen
      var routes = proxyDirectRoutes.getOrDefault(advertisedText, initDirectRouteGroup())
      routes[rawText] = registration
      proxyDirectRoutes[advertisedText] = move(routes)
      refreshDirectRouteSnapshotsUnsafe()

proc injectMalformedDirectRouteRegistrationForTest*(
    ownerId: int,
    advertisedText: string,
    rawText: string,
    pathKind = TsnetPathDirect
) {.gcsafe.} =
  proxySafe:
    withLock(proxyRegistryLock):
      var routes = proxyDirectRoutes.getOrDefault(advertisedText, initDirectRouteGroup())
      routes[rawText] = TsnetDirectRouteRegistration(
        ownerId: ownerId,
        advertisedKey: initSharedKey(advertisedText),
        rawKey: initSharedKey(rawText),
        pathKind: initSharedKey(pathKind)
      )
      proxyDirectRoutes[advertisedText] = move(routes)
      refreshDirectRouteSnapshotsUnsafe()
