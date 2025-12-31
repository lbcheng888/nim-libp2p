# Nim-LibP2P
# Copyright (c) 2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [Exception].}

import std/[strutils, sequtils, tables, options, sets]
import results

when defined(windows):
  import winlean
  const IpProtoIp = IPPROTO_IP
else:
  import posix
  const IpProtoIp = 0'i32
  const SoBindToDevice = cint(25)
  const IfNameSize = 16

  when not declared(Ifaddrs):
    type
      Ifaddrs* {.importc: "struct ifaddrs", header: "<ifaddrs.h>", bycopy.} = object
        ifa_next*: ptr Ifaddrs
        ifa_name*: cstring
        ifa_flags*: cuint
        ifa_addr*: ptr SockAddr
        ifa_netmask*: ptr SockAddr
        ifa_dstaddr*: ptr SockAddr
        ifa_data*: pointer

  when not declared(getifaddrs):
    proc getifaddrs(ifap: ptr ptr Ifaddrs): cint {.importc, header: "<ifaddrs.h>".}
  when not declared(freeifaddrs):
    proc freeifaddrs(ifa: ptr Ifaddrs) {.importc, header: "<ifaddrs.h>".}
import chronos, chronos/transports/common, chronos/handles, chronicles,
       stew/[byteutils, endians2]
import ../peerinfo
import ../multiaddress
import ../peerid
import ../utility
import ../crypto/crypto
import ./discoverymngr

export discoverymngr

when defined(ohos):
  # Force no HiLog
  proc OH_LOG_Print(logType: cint, level: cint, domain: cint, tag: cstring, fmt: cstring): cint {.cdecl, varargs, raises: [].} =
    discard
  const
    HILOG_TYPE_APP = cint(0)
    HILOG_LEVEL_INFO = cint(4)
    MDNS_LOG_DOMAIN = cint(0xD0B0)
  let MdnsLogTag: cstring = "nimlibp2p"

when defined(ohos) or defined(android):
  {.emit: """
  #include <arpa/inet.h>
  #include <errno.h>
  #include <net/if.h>
  #include <netinet/in.h>
  #include <string.h>
  #include <sys/ioctl.h>
  #include <sys/socket.h>
  #include <unistd.h>

  static int libp2p_get_iface_ipv4(const char *iface, char *buf, size_t buflen) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
      return -errno;
    }
    struct ifreq req;
    memset(&req, 0, sizeof(req));
    strncpy(req.ifr_name, iface, IFNAMSIZ - 1);
    if (ioctl(fd, SIOCGIFADDR, &req) != 0) {
      int err = errno;
      close(fd);
      return -err;
    }
    close(fd);
    struct sockaddr_in *addr = (struct sockaddr_in *)&req.ifr_addr;
    if (inet_ntop(AF_INET, &addr->sin_addr, buf, buflen) == NULL) {
      return -errno;
    }
    return 0;
  }

  static int libp2p_get_iface_ipv6(const char *iface, char *buf, size_t buflen) {
    int fd = socket(AF_INET6, SOCK_DGRAM, 0);
    if (fd < 0) {
      return -errno;
    }
    struct ifreq req;
    memset(&req, 0, sizeof(req));
    strncpy(req.ifr_name, iface, IFNAMSIZ - 1);
    req.ifr_addr.sa_family = AF_INET6;
    if (ioctl(fd, SIOCGIFADDR, &req) != 0) {
      int err = errno;
      close(fd);
      return -err;
    }
    close(fd);
    struct sockaddr_in6 *addr = (struct sockaddr_in6 *)&req.ifr_addr;
    if (inet_ntop(AF_INET6, &addr->sin6_addr, buf, buflen) == NULL) {
      return -errno;
    }
    return 0;
  }
  """ .}
  proc libp2p_get_iface_ipv4(iface: cstring, outBuf: cstring, bufLen: csize_t): cint {.importc, nodecl, raises: [].}
  proc libp2p_get_iface_ipv6(iface: cstring, outBuf: cstring, bufLen: csize_t): cint {.importc, nodecl, raises: [].}
  const CandidateIfaces = ["wlan0", "ancowlan0", "eth0", "rmnet_data0", "rmnet0", "lan0", "p2p0"]

when defined(android):
  const
    ANDROID_LOG_INFO = cint(4)
  proc android_log_print(prio: cint, tag: cstring, fmt: cstring): cint {.cdecl, importc: "__android_log_print", header: "<android/log.h>", varargs, raises: [].}
  let MdnsLogTag: cstring = "nimlibp2p"

proc mdnsLog(msg: string) {.raises: [].} =
  when defined(ohos):
    let formatted = "[mdns] " & msg
    discard OH_LOG_Print(HILOG_TYPE_APP, HILOG_LEVEL_INFO, MDNS_LOG_DOMAIN, MdnsLogTag, "%{public}s", formatted.cstring)
    try:
      stderr.writeLine(formatted)
    except IOError:
      discard
  elif defined(android):
    let formatted = "[mdns] " & msg
    let fmt: cstring = "%s"
    let msgPtr = formatted.cstring
    discard android_log_print(ANDROID_LOG_INFO, MdnsLogTag, fmt, msgPtr)
    try:
      stderr.writeLine(formatted)
    except IOError:
      discard
  else:
    try:
      stderr.writeLine("[mdns] " & msg)
    except IOError:
      discard

when defined(ohos) or defined(android):
  proc getInterfaceIpv4(iface: string): string {.raises: [].} =
    var buf: array[16, char]
    zeroMem(addr buf[0], buf.len)
    let rc = libp2p_get_iface_ipv4(iface.cstring, cast[cstring](addr buf[0]), buf.len.csize_t)
    if rc == 0:
      result = $cast[cstring](addr buf[0])
    else:
      mdnsLog("queryInterfaceIpv4 failed iface=" & iface & " errno=" & $(-rc))

  proc getInterfaceIpv6*(iface: string): string {.raises: [].} =
    var buf: array[46, char]
    zeroMem(addr buf[0], buf.len)
    let rc = libp2p_get_iface_ipv6(iface.cstring, cast[cstring](addr buf[0]), buf.len.csize_t)
    if rc == 0:
      result = $cast[cstring](addr buf[0])
    else:
      mdnsLog("queryInterfaceIpv6 failed iface=" & iface & " errno=" & $(-rc))

proc isLanIpv4(ip: string): bool {.raises: [].} =
  if ip.len == 0:
    return false
  if ip.startsWith("10.") or ip.startsWith("192.168.") or ip.startsWith("169.254."):
    return true
  if ip.startsWith("172."):
    let octets = ip.split('.')
    if octets.len >= 2:
      try:
        let second = parseInt(octets[1])
        if second >= 16 and second <= 31:
          return true
      except ValueError:
        discard
  if ip.startsWith("100."):
    let octets = ip.split('.')
    if octets.len >= 2:
      try:
        let second = parseInt(octets[1])
        if second >= 64 and second <= 127:
          return true
      except ValueError:
        discard
  false



proc enumerateLanIpv4*(preferred: string = ""): seq[string] {.raises: [].} =
  var seen = initHashSet[string]()
  template addIp(value: string) =
    block:
      let ip = value
      if ip.len == 0:
        return
      let trimmed = ip.strip()
      if trimmed.len == 0:
        return
      if trimmed == "0.0.0.0" or trimmed == "127.0.0.1":
        return
      if not isLanIpv4(trimmed):
        return
      if seen.contains(trimmed):
        return
      try:
        let octets = trimmed.split('.')
        if octets.len == 4:
          let last = parseInt(octets[^1])
          if last == 0 or last == 255:
            mdnsLog("enumerateLanIpv4 skip broadcast=" & trimmed)
            return
      except ValueError:
        discard
      seen.incl(trimmed)
      result.add(trimmed)

  if preferred.len > 0:
    addIp(preferred)

  when declared(getifaddrs):
    var ifap: ptr Ifaddrs = nil
    try:
      if getifaddrs(addr ifap) == 0:
        defer:
          freeifaddrs(ifap)
        var cursor = ifap
        while cursor != nil:
          when declared(cursor.ifa_flags):
            when declared(IFF_LOOPBACK):
              if (cursor.ifa_flags and IFF_LOOPBACK) != 0:
                cursor = cursor.ifa_next
                continue
          let addrPtr = cursor.ifa_addr
          if addrPtr != nil and addrPtr.sa_family == TSa_Family(posix.AF_INET):
            let sin = cast[ptr Sockaddr_in](addrPtr)
            let raw = posix.inet_ntoa(sin.sin_addr)
            if raw != nil:
              mdnsLog("enumerateLanIpv4 candidate iface=" & $cursor.ifa_name & " ip=" & $raw)
              addIp($raw)
            else:
              mdnsLog("enumerateLanIpv4 candidate iface=" & $cursor.ifa_name & " ip=nil")
          else:
             mdnsLog("enumerateLanIpv4 skip iface=" & (if cursor.ifa_name != nil: $cursor.ifa_name else: "nil") & " family=" & (if addrPtr != nil: $addrPtr.sa_family else: "-1"))
          cursor = cursor.ifa_next
      else:
        mdnsLog("enumerateLanIpv4 getifaddrs failed errno=" & $errno)
    except Exception:
      discard

  when defined(ohos) or defined(android):
    for candidate in CandidateIfaces:
      let ip = getInterfaceIpv4(candidate)
      addIp(ip)

proc detectPrimaryIpv4*(): string {.raises: [].} =
  let candidates = enumerateLanIpv4()
  if candidates.len > 0:
    let ip = candidates[0]
    if ip.len > 0:
      return ip
  when defined(ohos) or defined(android):
    for candidate in CandidateIfaces:
      let ip = getInterfaceIpv4(candidate)
      if ip.len > 0 and ip != "0.0.0.0" and ip != "127.0.0.1":
        mdnsLog("selectInterfaceAddress fallback query iface=" & candidate & " ip=" & ip)
        return ip
  "0.0.0.0"

when defined(ohos) or defined(android):
  proc detectPrimaryIpv6*(): string {.raises: [].} =
    for candidate in CandidateIfaces:
      let ip = getInterfaceIpv6(candidate)
      if ip.len > 0 and ip != "::":
        mdnsLog("selectInterfaceAddress fallback ipv6 iface=" & candidate & " ip=" & ip)
        return ip
    ""

type
  DnsQuestion = object
    name: string
    qtype: uint16
    qclass: uint16

  DnsRecord = object
    name: string
    rtype: uint16
    rclass: uint16
    ttl: uint32
    rdata: seq[byte]

  MdnsInterface* = ref object of DiscoveryInterface
    peerInfo: PeerInfo
    rng: ref HmacDrbgContext
    peerName: string
    hostName: string
    serviceName: string
    serviceNameCanonical: string
    instanceName: string
    instanceNameLower: string
    queryInterval: Duration
    announceInterval: Duration
    ttlDuration: Duration
    multicastV4: TransportAddress
    sockV4: DatagramTransport
    running: bool
    pendingQueries: CountTable[string]
    seenAnnouncements: Table[PeerId, Moment]
    preferredIpv4: string
    boundIpv4*: string
    lastImmediateAnnounce: Moment

const
  MdnsPort* = Port(5353)
  MdnsIpv4Multicast* = "224.0.0.251"
  MdnsDefaultService* = "_p2p._udp.local"
  MdnsMetaQuery* = "_services._dns-sd._udp.local"
  PtrType = 12'u16
  TxtType = 16'u16
  SrvType = 33'u16
  AType = 1'u16
  AAAAType = 28'u16
  ClassIN = 1'u16
  CacheFlush = 0x8000'u16
  DefaultQueryInterval = 5.seconds
  DefaultAnnounceInterval = 30.seconds
  DefaultRecordTtl = 120.seconds
  MdnsResponseFlags = 0x8400'u16
  MaxAdvertisedAddrs = 16
  MdnsMaxTxtEntryLen = 220

const MdnsSuppressedSegments = [
  "/p2p-circuit",
  "/webrtc-direct",
  "/certhash",
  "/tls/",
  "/ws/",
  "/wss/"
]

when defined(windows):
  const
    IpAddMembership = IP_ADD_MEMBERSHIP
    IpDropMembership = IP_DROP_MEMBERSHIP
    IpMulticastIf = IP_MULTICAST_IF
    IpMulticastTtl = IP_MULTICAST_TTL
    IpMulticastLoop = IP_MULTICAST_LOOP
elif defined(macosx) or defined(freebsd) or defined(netbsd) or defined(openbsd):
  const
    IpAddMembership = 12
    IpDropMembership = 13
    IpMulticastIf = 9
    IpMulticastTtl = 10
    IpMulticastLoop = 11
else:
  const
    IpAddMembership = 35
    IpDropMembership = 36
    IpMulticastIf = 32
    IpMulticastTtl = 33
    IpMulticastLoop = 34

type
  IpMreq = object
    multiaddr: array[4, uint8]
    iface: array[4, uint8]

proc decodeName(data: openArray[byte], offset: var int): string
proc processDatagram(
    self: MdnsInterface, data: seq[byte], remote: TransportAddress
): Future[void] {.async.}

proc encodeName(name: string): seq[byte] =
  result = newSeq[byte]()
  for label in name.split('.'):
    if label.len == 0:
      continue
    if label.len > 63:
      raise newException(ValueError, "Label exceeds 63 characters in " & name)
    result.add(byte(label.len))
    result.add label.toBytes()
  result.add(0)

template addUint16(buf: var seq[byte], value: uint16) =
  buf.add(byte((value shr 8) and 0xFF))
  buf.add(byte(value and 0xFF))

template addUint32(buf: var seq[byte], value: uint32) =
  buf.add(byte((value shr 24) and 0xFF))
  buf.add(byte((value shr 16) and 0xFF))
  buf.add(byte((value shr 8) and 0xFF))
  buf.add(byte(value and 0xFF))

proc parseUint16(data: openArray[byte], offset: var int): uint16 =
  if offset + 2 > data.len:
    raise newException(ValueError, "Unexpected end of buffer")
  result = (uint16(data[offset]) shl 8) or uint16(data[offset + 1])
  offset += 2

proc parseUint32(data: openArray[byte], offset: var int): uint32 =
  if offset + 4 > data.len:
    raise newException(ValueError, "Unexpected end of buffer")
  result =
    (uint32(data[offset]) shl 24) or (uint32(data[offset + 1]) shl 16) or
    (uint32(data[offset + 2]) shl 8) or uint32(data[offset + 3])
  offset += 4

proc encodeIpv4(address: string): array[4, uint8] =
  let parts = address.split('.')
  if parts.len != 4:
    raise newException(ValueError, "invalid IPv4 address: " & address)
  for idx, part in parts:
    let value = parseInt(part)
    if value < 0 or value > 255:
      raise newException(ValueError, "invalid IPv4 segment")
    result[idx] = byte(value)

when defined(ohos) or defined(android):
  proc resolveInterfaceNameByIp*(address: string): string =
    for iface in CandidateIfaces:
      let ip = getInterfaceIpv4(iface)
      if ip == address:
        return iface
    ""
elif declared(getifaddrs):
  proc resolveInterfaceNameByIp(address: string): string =
    var ifap: ptr Ifaddrs = nil
    if getifaddrs(addr ifap) != 0:
      return ""
    defer:
      freeifaddrs(ifap)
    var cursor = ifap
    while cursor != nil:
      let addrPtr = cursor.ifa_addr
      if addrPtr != nil and addrPtr.sa_family == TSa_Family(posix.AF_INET):
        let sin = cast[ptr Sockaddr_in](addrPtr)
        var buf: array[16, char]
        let outBuf = cast[cstring](addr buf[0])
        if posix.inet_ntop(posix.AF_INET, cast[pointer](addr sin.sin_addr), outBuf, cint(buf.len)) != nil:
          let ip = $outBuf
          if ip == address:
            return $cursor.ifa_name
      cursor = cursor.ifa_next
    ""
else:
  proc resolveInterfaceNameByIp(address: string): string =
    discard address
    ""

proc randomPeerName(rng: ref HmacDrbgContext, chars: int = 32): string =
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
  if rng.isNil:
    return repeat('a', chars)
  var bytes = newSeqUninit[byte](chars)
  hmacDrbgGenerate(rng[], bytes)
  result = newString(chars)
  for i in 0 ..< chars:
    result[i] = alphabet[int(bytes[i]) mod alphabet.len]

proc ensureTransport(self: MdnsInterface)

proc buildQuery(serviceName: string): seq[byte] =
  var buf = newSeq[byte]()
  buf.add(0'u8)
  buf.add(0'u8) # ID
  addUint16(buf, 0)
  addUint16(buf, 1) # QDCOUNT
  addUint16(buf, 0) # ANCOUNT
  addUint16(buf, 0) # NSCOUNT
  addUint16(buf, 0) # ARCOUNT

  buf.add encodeName(serviceName)
  addUint16(buf, PtrType)
  addUint16(buf, ClassIN)
  buf

proc buildMetaQuery(): seq[byte] =
  var buf = newSeq[byte]()
  buf.add(0'u8)
  buf.add(0'u8)
  addUint16(buf, 0)
  addUint16(buf, 1)
  addUint16(buf, 0)
  addUint16(buf, 0)
  addUint16(buf, 0)
  buf.add encodeName(MdnsMetaQuery)
  addUint16(buf, PtrType)
  addUint16(buf, ClassIN)
  buf

proc targetName(self: MdnsInterface): string =
  self.instanceName

proc collectAdvertisedAddrs(
    self: MdnsInterface, service: var string
): tuple[peerId: Opt[PeerId], addrs: seq[MultiAddress]] =
  var peerId = Opt.some(self.peerInfo.peerId)
  var discovered: seq[MultiAddress]
  var seenAddrs = initHashSet[string]()
  var captureService = Opt.none(string)
  var capturePeer = Opt.none(PeerId)

  for attr in self.toAdvertise:
    if attr.ofType(DiscoveryService):
      let svc = string attr.to(DiscoveryService)
      service = svc
      captureService = Opt.some(svc)
    elif attr.ofType(PeerId):
      let pid = attr.to(PeerId)
      peerId = Opt.some(pid)
      capturePeer = Opt.some(pid)
    elif attr.ofType(MultiAddress):
      let ma = attr.to(MultiAddress)
      let key = $ma
      if not seenAddrs.contains(key):
        discovered.add(ma)
        seenAddrs.incl(key)

  if discovered.len == 0:
    var fallback: seq[MultiAddress]
    for ma in self.peerInfo.addrs:
      let key = $ma
      if not seenAddrs.contains(key):
        fallback.add(ma)
        seenAddrs.incl(key)
    discovered = fallback
  if discovered.len > MaxAdvertisedAddrs:
    mdnsLog(
      "collectAdvertisedAddrs trim addrCount=" & $discovered.len &
      " limit=" & $MaxAdvertisedAddrs
    )
    discovered.setLen(MaxAdvertisedAddrs)
  # 同步去重后的属性，避免 self.toAdvertise 持续膨胀
  var refreshed = PeerAttributes()
  if captureService.isSome:
    refreshed.add(DiscoveryService(captureService.get()))
  elif service.len > 0:
    refreshed.add(DiscoveryService(service))
  if capturePeer.isSome:
    refreshed.add(capturePeer.get())
  elif peerId.isSome:
    refreshed.add(peerId.get())
  for ma in discovered:
    refreshed.add(ma)
  self.toAdvertise = refreshed
  mdnsLog(
    "collectAdvertisedAddrs peer=" &
      (if peerId.isSome: $peerId.get() else: "<none>") &
      " addrCount=" & $discovered.len
  )

  (peerId, discovered)

proc shortenAddrForLog(value: string, limit: int = 96): string =
  if value.len <= limit:
    return value
  result = value[0 ..< limit] & "..."

proc shouldAnnounceInMdns(value: string): bool =
  if value.len > MdnsMaxTxtEntryLen:
    return false
  for marker in MdnsSuppressedSegments:
    if marker in value:
      return false
  true

proc makeDnsaddrStrings(
    peerId: Opt[PeerId], addrs: seq[MultiAddress]
): seq[string] =
  result = @[]
  let peerStr = if peerId.isSome: $peerId.get() else: ""
  let peerSuffix = if peerStr.len > 0: "/p2p/" & peerStr else: ""
  for ma in addrs:
    var addrStr = $ma
    if peerSuffix.len > 0 and addrStr.find("/p2p/") < 0:
      addrStr.add(peerSuffix)
    if not shouldAnnounceInMdns(addrStr):
      mdnsLog(
        "makeDnsaddrStrings skip addr len=" & $addrStr.len &
        " value=" & shortenAddrForLog(addrStr)
      )
      continue
    result.add("dnsaddr=" & addrStr)

proc buildTxtRecordPayload(entries: seq[string]): seq[byte] =
  var payload = newSeq[byte]()
  for entry in entries:
    let raw = entry.toBytes()
    if raw.len > 255:
      continue
    payload.add(byte(raw.len))
    payload.add(raw)
  payload

proc buildPtrAnswer(
    serviceName, instanceName: string, ttl: Duration
): seq[byte] =
  var buf = newSeq[byte]()
  buf.add encodeName(serviceName)
  addUint16(buf, PtrType)
  addUint16(buf, ClassIN)
  addUint32(buf, uint32(ttl.seconds))
  let targetBytes = encodeName(instanceName)
  addUint16(buf, uint16(targetBytes.len))
  buf.add targetBytes
  buf

proc buildTxtRecord(
    instanceName: string, ttl: Duration, entries: seq[string]
): seq[byte] =
  let payload = buildTxtRecordPayload(entries)
  if payload.len == 0:
    return @[]

  var buf = newSeq[byte]()
  buf.add encodeName(instanceName)
  addUint16(buf, TxtType)
  addUint16(buf, ClassIN or CacheFlush)
  addUint32(buf, uint32(ttl.seconds))
  addUint16(buf, uint16(payload.len))
  buf.add payload
  buf

proc buildAnnouncement(
    self: MdnsInterface, service: string, entries: seq[string]
): seq[byte] =
  mdnsLog(
    "buildAnnouncement service=" & service & " dnsaddrCount=" & $entries.len
  )
  let ptrAnswer = buildPtrAnswer(service, self.instanceName, self.ttlDuration)
  let txtRecord = buildTxtRecord(self.instanceName, self.ttlDuration, entries)
  mdnsLog("buildAnnouncement txtRecordLen=" & $txtRecord.len)
  let additionalCount = (if txtRecord.len > 0: 1 else: 0).uint16

  var buf = newSeq[byte]()
  buf.add(0'u8)
  buf.add(0'u8)
  addUint16(buf, MdnsResponseFlags)
  addUint16(buf, 0)             # QDCOUNT
  addUint16(buf, 1)             # ANCOUNT
  addUint16(buf, 0)             # NSCOUNT
  addUint16(buf, additionalCount)
  buf.add ptrAnswer
  if txtRecord.len > 0:
    buf.add txtRecord
  buf

proc ensureTransport(self: MdnsInterface) =
  let preferred = if self.preferredIpv4.len > 0: self.preferredIpv4 else: "0.0.0.0"
  mdnsLog(
    "ensureTransport enter hasSocket=" & $(not self.sockV4.isNil) &
    " preferred=" & preferred
  )
  info "mdns ensureTransport enter", hasSocket = not self.sockV4.isNil, preferred = preferred

  proc selectInterfaceAddress(): string =
    if self.preferredIpv4.len > 0 and self.preferredIpv4 != "0.0.0.0":
      mdnsLog("selectInterfaceAddress using preferred=" & self.preferredIpv4)
      return self.preferredIpv4
    for addr in self.peerInfo.addrs:
      let addrStr = $addr
      let tokens = addrStr.split('/')
      var idx = 0
      while idx < tokens.len:
        if tokens[idx] == "ip4" and idx + 1 < tokens.len:
          let candidate = tokens[idx + 1]
          if candidate != "0.0.0.0" and candidate != "127.0.0.1":
            mdnsLog("selectInterfaceAddress using peerInfo=" & candidate)
            return candidate
        idx += 1
    let detected = detectPrimaryIpv4()
    if detected != "0.0.0.0":
      mdnsLog("selectInterfaceAddress detectPrimaryIpv4=" & detected)
      return detected
    mdnsLog("selectInterfaceAddress fallback=0.0.0.0")
    "0.0.0.0"

  var ifaceAddr = selectInterfaceAddress()
  if ifaceAddr.len == 0:
    ifaceAddr = "0.0.0.0"
  info "mdns interface candidate", preferred = self.preferredIpv4, selected = ifaceAddr
  mdnsLog("selected interface=" & ifaceAddr)
  let local = initTAddress("0.0.0.0", MdnsPort)
  proc checkOpt(res: Result[void, OSErrorCode], name: string) =
    if res.isErr:
      let errVal = res.error()
      warn "mdns setsockopt failed", option = name, errno = int(errVal)

  proc dropMembership(handle: AsyncFD, targetIface: string, label: string) =
    if targetIface.len == 0:
      return
    var dropReq = IpMreq(
      multiaddr: encodeIpv4(MdnsIpv4Multicast),
      iface: encodeIpv4(targetIface)
    )
    let dropRes = setSockOpt2(
      handle, IpProtoIp, IpDropMembership, addr(dropReq), sizeof(dropReq)
    )
    if dropRes.isErr:
      let errVal = dropRes.error()
      let code = int(errVal)
      if code in [2, 19, 22]:
        mdnsLog("drop multicast membership skipped iface=" & targetIface & " label=" & label & " errno=" & $code)
      else:
        warn "mdns drop membership failed", iface = targetIface, label = label, errno = code
    else:
      mdnsLog("drop multicast membership iface=" & targetIface & " label=" & label)

  proc applyMembership(handle: AsyncFD, targetIface: string) =
    mdnsLog("applyMembership target=" & (if targetIface.len > 0: targetIface else: "<empty>"))
    when defined(ohos) or defined(linux):
      if targetIface.len > 0 and targetIface != "0.0.0.0":
        var bindName = targetIface
        when declared(resolveInterfaceNameByIp):
          if targetIface != "0.0.0.0":
            let name = resolveInterfaceNameByIp(targetIface)
            if name.len > 0:
              bindName = name
              mdnsLog("SO_BINDTODEVICE resolved ip=" & targetIface & " iface=" & bindName)
        var ifname: array[IfNameSize, char]
        zeroMem(addr ifname[0], sizeof(ifname))
        let copyLen = min(bindName.len, IfNameSize - 1)
        if copyLen > 0:
          copyMem(addr ifname[0], bindName.cstring, copyLen)
        let bindRes = setSockOpt2(handle, cint(SOL_SOCKET), SoBindToDevice, cast[pointer](addr ifname[0]), IfNameSize)
        if bindRes.isErr:
          let errVal = bindRes.error()
          warn "mdns bind to device failed", iface = bindName, errno = int(errVal)
          mdnsLog("SO_BINDTODEVICE failed iface=" & bindName & " errno=" & $int(errVal))
        else:
          mdnsLog("SO_BINDTODEVICE success iface=" & bindName)
    var ifaceBytes = encodeIpv4(targetIface)
    if targetIface != "0.0.0.0":
      let res = setSockOpt2(handle, IpProtoIp, IpMulticastIf, addr(ifaceBytes), sizeof(ifaceBytes))
      if res.isErr:
        let errVal = res.error()
        warn "mdns setsockopt failed", option = "IP_MULTICAST_IF", errno = int(errVal)
        info "mdns bind interface failed", iface = targetIface, errno = int(errVal)
        mdnsLog("IP_MULTICAST_IF failed iface=" & targetIface & " errno=" & $int(errVal))
      else:
        mdnsLog("multicast iface set=" & targetIface)
      info "mdns multicast interface", iface = targetIface, status = (not res.isErr)
      mdnsLog("IP_MULTICAST_IF status=" & $(not res.isErr))
    var mreq = IpMreq(
      multiaddr: encodeIpv4(MdnsIpv4Multicast),
      iface: ifaceBytes,
    )
    mdnsLog("attempt join multicast iface=" & targetIface)
    let joinLocal = setSockOpt2(
      handle, IpProtoIp, IpAddMembership, addr(mreq), sizeof(mreq)
    )
    if joinLocal.isErr:
      let errVal = joinLocal.error()
      warn "mdns setsockopt failed", option = "IP_ADD_MEMBERSHIP(local)", errno = int(errVal)
      info "mdns join local failed", iface = targetIface, errno = int(errVal)
      mdnsLog("join local failed iface=" & targetIface & " errno=" & $int(errVal))
    else:
      mdnsLog("multicast join iface=" & targetIface)
      info "mdns join local", iface = targetIface
      mdnsLog("join local iface=" & targetIface)
      mdnsLog("join local success iface=" & targetIface)
    info "mdns join local status", success = (not joinLocal.isErr), iface = targetIface
    mdnsLog("IP_ADD_MEMBERSHIP(local) status=" & $(not joinLocal.isErr))
    if targetIface != "0.0.0.0":
      var anyReq = IpMreq(
        multiaddr: encodeIpv4(MdnsIpv4Multicast),
        iface: encodeIpv4("0.0.0.0"),
      )
      mdnsLog("attempt join multicast iface=0.0.0.0")
      let joinAny = setSockOpt2(
        handle, IpProtoIp, IpAddMembership, addr(anyReq), sizeof(anyReq)
      )
      if joinAny.isErr:
        let errVal = joinAny.error()
        warn "mdns setsockopt failed", option = "IP_ADD_MEMBERSHIP(any)", errno = int(errVal)
        info "mdns join any failed", errno = int(errVal)
        mdnsLog("join any failed errno=" & $int(errVal))
      else:
        mdnsLog("multicast join iface=0.0.0.0")
        info "mdns join any", success = true
        mdnsLog("join any success")
        mdnsLog("join any success iface=0.0.0.0")
      info "mdns join any status", success = (not joinAny.isErr)
      mdnsLog("IP_ADD_MEMBERSHIP(any) status=" & $(not joinAny.isErr))

  if not self.sockV4.isNil:
    if self.boundIpv4 == ifaceAddr:
      mdnsLog("ensureTransport reuse existing socket selected=" & ifaceAddr & " preferred=" & preferred)
      info "mdns ensureTransport reuse", selected = ifaceAddr, preferred = preferred
      return
    mdnsLog("ensureTransport reconfigure socket old=" & self.boundIpv4 & " new=" & ifaceAddr)
    info "mdns ensureTransport reconfigure", old = self.boundIpv4, newIface = ifaceAddr
    let handle = self.sockV4.fd
    if self.boundIpv4.len > 0:
      dropMembership(handle, self.boundIpv4, "old")
    dropMembership(handle, "0.0.0.0", "any")
    applyMembership(handle, ifaceAddr)
    self.boundIpv4 = ifaceAddr
    return

  let callback = proc(
      transp: DatagramTransport, remote: TransportAddress
  ): Future[void] {.async: (raises: []).} =
    let iface = getUserData[MdnsInterface](transp)
    try:
      let payload = transp.getMessage()
      if payload.len == 0:
        mdnsLog("datagram received empty from=" & $remote)
        return
      mdnsLog("datagram received size=" & $payload.len & " remote=" & $remote)
      let previewLen = (if payload.len < 16: payload.len else: 16)
      var preview: string = ""
      for idx in 0 ..< previewLen:
        preview.add((if idx == 0: "" else: " ") & payload[idx].toHex(2))
      info "mdns datagram received", size = payload.len, remote = $remote, hex = preview
      await iface.processDatagram(payload, remote)
    except CatchableError as exc:
      trace "mdns: failed processing datagram", error = exc.msg

  proc reusePortUnsupported(code: int): bool =
    when not defined(windows):
      when declared(posix.ENOPROTOOPT):
        if code == int(posix.ENOPROTOOPT):
          return true
      when declared(posix.EOPNOTSUPP):
        if code == int(posix.EOPNOTSUPP):
          return true
      when declared(posix.ENOTSUP):
        if code == int(posix.ENOTSUP):
          return true
    else:
      when declared(WSAEINVAL):
        if code == WSAEINVAL.int:
          return true
    false

  var lastError: ref CatchableError = nil
  var reusePortActive = true
  let primaryFlags = {ServerFlags.ReuseAddr, ServerFlags.ReusePort}
  let fallbackFlags = {ServerFlags.ReuseAddr}

  for attempt in 0 .. 1:
    let useFlags = if attempt == 0: primaryFlags else: fallbackFlags
    let reusePortEnabled = ServerFlags.ReusePort in useFlags
    mdnsLog(
      "ensureTransport creating socket local=" & $local &
      " attempt=" & $(attempt + 1) &
      " reusePort=" & $reusePortEnabled
    )
    try:
      self.sockV4 = newDatagramTransport(
        callback,
        self,
        local = local,
        flags = useFlags,
      )
      reusePortActive = reusePortEnabled
      lastError = nil
      break
    except TransportOsError as exc:
      let errnoVal = int(exc.code)
      mdnsLog(
        "datagram transport create failed errno=" & $errnoVal &
        " reusePort=" & $reusePortEnabled &
        " msg=" & exc.msg
      )
      if attempt == 0 and reusePortUnsupported(errnoVal):
        mdnsLog(
          "ensureTransport retry without reusePort errno=" & $errnoVal &
          " msg=" & exc.msg
        )
        lastError = exc
        continue
      lastError = exc
      break
    except CatchableError as exc:
      mdnsLog("datagram transport create failed err=" & exc.msg)
      lastError = exc
      break

  if self.sockV4.isNil:
    let errMsg = if lastError.isNil: "unknown" else: lastError.msg
    mdnsLog("ensureTransport giving up err=" & errMsg)
    if lastError.isNil:
      raise newException(DiscoveryError, "failed creating mDNS socket: " & errMsg)
    raise newException(DiscoveryError, "failed creating mDNS socket: " & errMsg, lastError)

  mdnsLog(
    "datagram transport created local=" & $local &
    " reusePort=" & $reusePortActive
  )
  info "mdns socket created", local = $local, reusePort = reusePortActive
  try:
    let boundAddr = self.sockV4.localAddress()
    info "mdns socket bound", address = $boundAddr
  except CatchableError as exc:
    warn "mdns socket localAddress failed", err = exc.msg

  if not reusePortActive:
    info "mdns reusePort disabled fallback applied"
    mdnsLog("ensureTransport using socket without reusePort")

  let handle = self.sockV4.fd
  let ttlValue = 255
  checkOpt(setSockOpt2(handle, IpProtoIp, IpMulticastTtl, ttlValue), "IP_MULTICAST_TTL")
  var loopValue = 1
  checkOpt(setSockOpt2(handle, IpProtoIp, IpMulticastLoop, loopValue), "IP_MULTICAST_LOOP")

  mdnsLog("selected interface=" & ifaceAddr)
  info "mdns selected interface", iface = ifaceAddr
  debug "selected interface candidate", iface = ifaceAddr
  trace "mdns selected interface", iface = ifaceAddr
  mdnsLog("ensureTransport invoking applyMembership iface=" & ifaceAddr)
  applyMembership(handle, ifaceAddr)
  self.boundIpv4 = ifaceAddr
  info "mdns interface bound", preferred = self.preferredIpv4, bound = self.boundIpv4
  mdnsLog("interface bound preferred=" & self.preferredIpv4 & " bound=" & self.boundIpv4)

proc sendMulticast(self: MdnsInterface, payload: seq[byte]) {.async.} =
  if self.sockV4.isNil or payload.len == 0:
    mdnsLog("sendMulticast skipped socketNil=" & $(self.sockV4.isNil) & " payloadLen=" & $payload.len)
    info "mdns send multicast skipped", socketNil = (self.sockV4.isNil), payloadLen = payload.len
    return

  let target = self.multicastV4
  var localDesc = ""
  try:
    localDesc = $self.sockV4.localAddress()
  except CatchableError as exc:
    localDesc = "err:" & exc.msg
  mdnsLog("sendMulticast attempting bytes=" & $payload.len & " target=" & $target)
  info "mdns send multicast attempt", bytes = payload.len, target = $target, local = localDesc
  try:
    await self.sockV4.sendTo(target, unsafeAddr payload[0], payload.len)
    mdnsLog("sendMulticast bytes=" & $payload.len & " target=" & $target)
    info "mdns send multicast", bytes = payload.len, target = $target, local = localDesc
  except CatchableError as exc:
    trace "mdns: failed to send datagram", error = exc.msg
    warn "mdns multicast send failed", err = exc.msg

proc canonicalLower(name: string): string =
  var tmp = name.toLowerAscii()
  while tmp.len > 0 and tmp[^1] == '.':
    tmp.setLen(tmp.len - 1)
  tmp

proc canonicalServiceName(name: string): string =
  ## Normalize service labels so `_service._udp` 与 `_service._udp.local`
  ## 视为同一通道，避免不同平台附带 `.local` 导致匹配失败。
  var tmp = canonicalLower(name)
  const LocalSuffix = ".local"
  if tmp.len >= LocalSuffix.len and tmp.endsWith(LocalSuffix):
    tmp.setLen(tmp.len - LocalSuffix.len)
    while tmp.len > 0 and tmp[^1] == '.':
      tmp.setLen(tmp.len - 1)
  tmp

proc handleQuery(self: MdnsInterface, questions: seq[DnsQuestion]) {.async: (raises: [CancelledError, Exception]).} =
  var shouldAnnounce = false
  var announceServices: seq[string] = @[]
  let serviceCanonical = self.serviceNameCanonical
  let metaCanonical = canonicalServiceName(MdnsMetaQuery)

  for q in questions:
    let nameLower = q.name.toLowerAscii()
    let queryCanonical = canonicalServiceName(q.name)
    let queriedService =
      if q.name.len > 0:
        q.name
      else:
        self.serviceName
    case q.qtype
    of PtrType:
      if queryCanonical == serviceCanonical:
        shouldAnnounce = true
        announceServices.add(queriedService)
      elif queryCanonical == metaCanonical:
        var response: seq[byte] = @[]
        try:
          response = newSeq[byte]()
          response.add(0'u8)
          response.add(0'u8)
          addUint16(response, MdnsResponseFlags)
          addUint16(response, 0)
          addUint16(response, 1)
          addUint16(response, 0)
          addUint16(response, 0)
          response.add encodeName(MdnsMetaQuery)
          addUint16(response, PtrType)
          addUint16(response, ClassIN)
          addUint32(response, uint32(self.ttlDuration.seconds))
          let svcName = encodeName(self.serviceName)
          addUint16(response, uint16(svcName.len))
          response.add svcName
        except CatchableError as exc:
          trace "mdns: failed building meta response", error = exc.msg
          response.setLen(0)
        if response.len > 0:
          await self.sendMulticast(response)
    else:
      if nameLower == self.instanceNameLower:
        shouldAnnounce = true
        announceServices.add(self.serviceName)

  if shouldAnnounce:
    var service = self.serviceName
    let (peerId, addrs) = self.collectAdvertisedAddrs(service)
    let dnsAddrs = makeDnsaddrStrings(peerId, addrs)
    for svc in announceServices:
      let payload = buildAnnouncement(self, svc, dnsAddrs)
      await self.sendMulticast(payload)

proc decodeName(data: openArray[byte], offset: var int): string =
  var labels: seq[string]
  var jumped = false
  var pos = offset
  var loops = 0

  while pos < data.len:
    let length = data[pos]
    if length == 0:
      if not jumped:
        offset = pos + 1
      else:
        inc pos
      break
    if (length and 0xC0'u8) == 0xC0'u8:
      if pos + 1 >= data.len:
        raise newException(ValueError, "invalid name compression pointer")
      let pointer = ((uint16(length) and 0x3F) shl 8) or uint16(data[pos + 1])
      if not jumped:
        offset = pos + 2
      pos = pointer.int
      jumped = true
      inc loops
      if loops > data.len:
        raise newException(ValueError, "compression pointer loop")
      continue

    inc pos
    if pos + int(length) > data.len:
      raise newException(ValueError, "label exceeds buffer")
    labels.add string.fromBytes(data[pos ..< pos + int(length)])
    pos += int(length)
    if not jumped:
      offset = pos

  result = labels.join(".")

proc parseQuestions(
    data: openArray[byte], offset: var int, count: int
): seq[DnsQuestion] =
  result = @[]
  for _ in 0 ..< count:
    var nameOffset = offset
    let name = decodeName(data, nameOffset)
    offset = nameOffset
    let qtype = parseUint16(data, offset)
    let qclass = parseUint16(data, offset)
    result.add DnsQuestion(name: name, qtype: qtype, qclass: qclass)

proc parseRecords(
    data: openArray[byte], offset: var int, count: int
): seq[DnsRecord] =
  result = @[]
  for _ in 0 ..< count:
    var nameOffset = offset
    let name = decodeName(data, nameOffset)
    offset = nameOffset
    let rtype = parseUint16(data, offset)
    let rclass = parseUint16(data, offset)
    let ttl = parseUint32(data, offset)
    let rdLength = parseUint16(data, offset)
    if offset + int(rdLength) > data.len:
      raise newException(ValueError, "resource record exceeds buffer")
    let rdata = data[offset ..< offset + int(rdLength)]
    offset += int(rdLength)
    result.add(
      DnsRecord(name: name, rtype: rtype, rclass: rclass, ttl: ttl, rdata: rdata)
    )

proc decodeDnsaddrEntry(entry: string): Opt[string] =
  var trimmed = entry.strip()
  if trimmed.len == 0:
    return Opt.none(string)
  let lower = trimmed.toLowerAscii()
  for prefix in ["dnsaddr", "addr", "addrs"]:
    if lower.startsWith(prefix):
      let idx = prefix.len
      if idx < trimmed.len and (trimmed[idx] == '=' or trimmed[idx] == ':'):
        var candidate = trimmed[(idx + 1) .. ^1].strip()
        if candidate.len == 0:
          return Opt.none(string)
        var innerLower = candidate.toLowerAscii()
        for innerPrefix in ["addr:", "addrs:"]:
          if innerLower.startsWith(innerPrefix):
            candidate = candidate[innerPrefix.len .. ^1].strip()
            innerLower = candidate.toLowerAscii()
            break
        if candidate.len == 0:
          return Opt.none(string)
        return Opt.some(candidate)
  if (lower.find("/ip4/") >= 0) or (lower.find("/ip6/") >= 0):
    return Opt.some(trimmed)
  Opt.none(string)

proc collectAnnouncement(
    self: MdnsInterface, answers, additionals: seq[DnsRecord]
) =
  trace "mdns collect announcement", answers = answers.len, additionals = additionals.len
  mdnsLog("collectAnnouncement answers=" & $answers.len & " additionals=" & $additionals.len)
  let serviceNameCanonical = canonicalServiceName(self.serviceName)
  var targets = initTable[string, tuple[service: string, instance: string]]()
  for answer in answers:
    mdnsLog("collectAnnouncement answer name=" & answer.name & " type=" & $answer.rtype)
    if answer.rtype != PtrType:
      continue
    let nameLower = canonicalServiceName(answer.name)
    if nameLower != serviceNameCanonical:
      continue
    var offset = 0
    let target = decodeName(answer.rdata, offset)
    let targetLower = canonicalServiceName(target)
    targets[targetLower] = (service: answer.name, instance: target)

  if targets.len == 0:
    mdnsLog("collectAnnouncement no matching PTR answers")
    return

  var dnsRecords = initTable[string, tuple[dnsaddrs: seq[string], ttl: uint32]]()

  proc accumulateTxt(records: seq[DnsRecord], label: string) =
    for record in records:
      mdnsLog(
        "collectAnnouncement " & label & " name=" & record.name & " type=" &
          $record.rtype & " rdataLen=" & $record.rdata.len
      )
      if record.rtype != TxtType:
        continue
      var entries: seq[string] = @[]
      var idx = 0
      while idx < record.rdata.len:
        let length = int(record.rdata[idx])
        inc idx
        if idx + length > record.rdata.len:
          break
        let entry = string.fromBytes(record.rdata[idx ..< idx + length])
        let decoded = decodeDnsaddrEntry(entry)
        if decoded.isSome:
          let value = decoded.get()
          entries.add(value)
          mdnsLog(
            "collectAnnouncement txt entry raw=" & entry & " decoded=" & value &
            " source=" & label
          )
        else:
          mdnsLog("collectAnnouncement skip txt entry=" & entry & " source=" & label)
        idx += length
      if entries.len > 0:
        let key = canonicalServiceName(record.name)
        var bucket = dnsRecords.getOrDefault(key)
        bucket.dnsaddrs.add(entries)
        bucket.ttl = record.ttl
        dnsRecords[key] = bucket

  accumulateTxt(additionals, "additional")
  accumulateTxt(answers, "answer")

  if dnsRecords.len == 0:
    mdnsLog("collectAnnouncement no dnsRecords")
    return

  var grouped = initTable[PeerId, tuple[svc: string, addrs: seq[MultiAddress], ttl: uint32]]()

  for targetLower, meta in targets:
    if targetLower notin dnsRecords:
      mdnsLog(
        "collectAnnouncement missing txt record instance=" & meta.instance &
        " service=" & meta.service &
        " canonKey=" & targetLower
      )
      continue
    let data = dnsRecords[targetLower]
    for entry in data.dnsaddrs:
      if entry.find("/tcp/0") >= 0 or entry.find("/udp/0") >= 0:
        mdnsLog("collectAnnouncement skip zero port=" & entry)
        continue
      var cleanedEntry = entry
      let marker = "/p2p/" & $self.peerInfo.peerId
      while cleanedEntry.endsWith(marker & marker):
        cleanedEntry = cleanedEntry[0 ..< cleanedEntry.len - marker.len]
      let ma = MultiAddress.init(cleanedEntry).valueOr:
        mdnsLog("collectAnnouncement invalid dnsaddr entry=" & entry & " err=" & error)
        continue
      let parsed = parseFullAddress(ma).valueOr:
        mdnsLog("collectAnnouncement parseFullAddress failed entry=" & cleanedEntry & " err=" & error)
        continue
      let peerId = parsed[0]
      mdnsLog("collectAnnouncement parsed peer=" & $peerId & " addr=" & $parsed[1])
      if peerId == self.peerInfo.peerId:
        continue
      var bucket = grouped.getOrDefault(peerId)
      bucket.svc = meta.service
      bucket.addrs.add(parsed[1])
      bucket.ttl = data.ttl
      grouped[peerId] = bucket

  if grouped.len == 0:
    mdnsLog("collectAnnouncement grouped empty after parsing txt records")
    return

  mdnsLog("collectAnnouncement cache size=" & $self.seenAnnouncements.len)
  let now = Moment.now()
  for key, value in grouped.mpairs:
    mdnsLog(
      "collectAnnouncement grouped peer=" & $key &
      " addrCount=" & $value.addrs.len &
      " ttl=" & $value.ttl
    )
    let expiry = now + int64(value.ttl).seconds
    if key in self.seenAnnouncements and self.seenAnnouncements[key] > now:
      mdnsLog(
        "collectAnnouncement skip cached peer=" & $key &
        " expiry=" & $self.seenAnnouncements[key] &
        " now=" & $now
      )
      continue
    var attrs: PeerAttributes
    attrs.add(key)
    attrs.add(DiscoveryService(value.svc))
    for addr in value.addrs:
      attrs.add(addr)
      mdnsLog("collectAnnouncement add addr=" & $addr)
    mdnsLog("collectAnnouncement emitting peer=" & $key)
    self.onPeerFound(attrs)
    self.seenAnnouncements[key] = expiry

proc processResponse(self: MdnsInterface, data: seq[byte]) {.raises: [Exception].} =
  var offset = 0
  discard parseUint16(data, offset) # ID
  let flags = parseUint16(data, offset)
  let qdCount = parseUint16(data, offset).int
  let anCount = parseUint16(data, offset).int
  let nsCount = parseUint16(data, offset).int
  let arCount = parseUint16(data, offset).int

  let questions = parseQuestions(data, offset, qdCount)
  discard parseRecords(data, offset, nsCount)
  let answers = parseRecords(data, offset, anCount)
  let additionals = parseRecords(data, offset, arCount)

  if (flags and 0x8000'u16) == 0:
    asyncSpawn self.handleQuery(questions)
  else:
    self.collectAnnouncement(answers, additionals)

proc processDatagram(
    self: MdnsInterface, data: seq[byte], remote: TransportAddress
) {.async.} =
  mdnsLog("processDatagram bytes=" & $data.len & " from=" & $remote)
  try:
    if data.len == 0:
      mdnsLog("processDatagram empty payload from=" & $remote)
    else:
      let previewLen = min(data.len, 32)
      var preview: string = ""
      for idx in 0 ..< previewLen:
        preview.add((if idx == 0: "" else: " ") & data[idx].toHex(2))
      mdnsLog("processDatagram preview=" & preview & " size=" & $data.len & " from=" & $remote)
    self.processResponse(data)
  except Exception as exc:
    trace "mdns: failed decoding message", error = exc.msg
    mdnsLog("processDatagram decode failed err=" & exc.msg)

proc closeTransport*(self: MdnsInterface) {.async.} =
  if self.sockV4.isNil:
    return
  try:
    await self.sockV4.closeWait()
    mdnsLog("socket closed")
  except CatchableError as exc:
    mdnsLog("socket close failed: " & exc.msg)
  self.sockV4 = nil
  self.boundIpv4 = ""

method request*(
    self: MdnsInterface, pa: PeerAttributes
) {.async: (raises: [DiscoveryError, CancelledError]).} =
  mdnsLog("request start service=" & self.serviceName & " preferred=" & self.preferredIpv4)
  try:
    ensureTransport(self)
    mdnsLog("request ensureTransport ok bound=" & self.boundIpv4)
  except Exception as exc:
    mdnsLog("request ensureTransport failed err=" & exc.msg)
    raise newException(DiscoveryError, "mdns ensureTransport failed: " & exc.msg)
  var service = self.serviceName
  for attr in pa:
    if attr.ofType(DiscoveryService):
      service = string attr.to(DiscoveryService)
  let firstRequest = service notin self.pendingQueries
  self.pendingQueries.inc(service)

  if firstRequest:
    let meta = block:
      try:
        buildMetaQuery()
      except Exception as exc:
        trace "mdns: failed to build meta query", error = exc.msg
        newSeq[byte]()
    if meta.len > 0:
      try:
        await self.sendMulticast(meta)
      except Exception as exc:
        trace "mdns: failed to send meta query", error = exc.msg

  try:
    while true:
      let query = block:
        try:
          buildQuery(service)
        except Exception as exc:
          trace "mdns: failed to build query", error = exc.msg
          mdnsLog("buildQuery failed service=" & service & " err=" & exc.msg)
          newSeq[byte]()
      if query.len > 0:
        mdnsLog("send query bytes=" & $query.len & " service=" & service & " iface=" & self.boundIpv4)
        try:
          await self.sendMulticast(query)
        except Exception as exc:
          trace "mdns: failed to send query", error = exc.msg
          mdnsLog("send query failed err=" & exc.msg & " service=" & service)
      else:
        mdnsLog("skip query send (empty payload) service=" & service)
      await sleepAsync(self.queryInterval)
  finally:
    if service in self.pendingQueries:
      let current = self.pendingQueries[service]
      if current <= 1:
        self.pendingQueries.del(service)
      else:
        self.pendingQueries[service] = current - 1

method advertise*(self: MdnsInterface) {.async: (raises: [CancelledError, AdvertiseError]).} =
  mdnsLog(
    "advertise loop enter socketNil=" & $(self.sockV4.isNil) &
    " boundIpv4=" & (if self.boundIpv4.len == 0: "<unset>" else: self.boundIpv4)
  )
  try:
    ensureTransport(self)
    mdnsLog(
      "advertise ensureTransport ok socketNil=" & $(self.sockV4.isNil) &
      " boundIpv4=" & (if self.boundIpv4.len == 0: "<unset>" else: self.boundIpv4)
    )
  except Exception as exc:
    trace "mdns: ensureTransport failed", error = exc.msg
    mdnsLog("advertise ensureTransport failed err=" & exc.msg)
    return
  while true:
    var service = self.serviceName
    let collected = block:
      try:
        self.collectAdvertisedAddrs(service)
      except Exception as exc:
        trace "mdns: collectAdvertisedAddrs failed", error = exc.msg
        (Opt.some(self.peerInfo.peerId), @[])
    let peerIdOpt = collected.peerId
    var addrs = collected.addrs
    var dnsAddrs = block:
      try:
        makeDnsaddrStrings(peerIdOpt, addrs)
      except Exception as exc:
        trace "mdns: failed to build dnsaddr strings", error = exc.msg
        @[]
    if dnsAddrs.len == 0:
      let fullAddrs = self.peerInfo.fullAddrs().valueOr: @[]
      dnsAddrs = fullAddrs.mapIt("dnsaddr=" & $it)

    if dnsAddrs.len > 0:
      let payload = block:
        try:
          buildAnnouncement(self, service, dnsAddrs)
        except Exception as exc:
          trace "mdns: failed to build announcement", error = exc.msg
          newSeq[byte]()
      if payload.len > 0:
        try:
          await self.sendMulticast(payload)
        except Exception as exc:
          trace "mdns: failed to send announcement", error = exc.msg
    await sleepAsync(self.announceInterval) or self.advertisementUpdated.wait()
    self.advertisementUpdated.clear()

proc new*(
    T: typedesc[MdnsInterface],
    peerInfo: PeerInfo,
    rng: ref HmacDrbgContext = nil,
    serviceName = MdnsDefaultService,
    queryInterval = DefaultQueryInterval,
    announceInterval = DefaultAnnounceInterval,
    ttl = DefaultRecordTtl,
    preferredIpv4 = "",
): MdnsInterface =
  var rngRef = rng
  if rngRef.isNil:
    rngRef = newRng()
  let peerLabel = randomPeerName(rngRef)
  let instance = peerLabel & "." & serviceName

  result = T(
    peerInfo: peerInfo,
    rng: rngRef,
    peerName: peerLabel,
    hostName: peerLabel & ".p2p.local",
    serviceName: serviceName,
    serviceNameCanonical: canonicalServiceName(serviceName),
    instanceName: instance,
    instanceNameLower: instance.toLowerAscii(),
    queryInterval: queryInterval,
    announceInterval: announceInterval,
    ttlDuration: ttl,
    multicastV4: initTAddress(MdnsIpv4Multicast, MdnsPort),
    pendingQueries: initCountTable[string](),
    seenAnnouncements: initTable[PeerId, Moment](),
    preferredIpv4: preferredIpv4,
    boundIpv4: "",
  )

proc setPreferredIpv4*(self: MdnsInterface, ipv4: string) {.raises: [].} =
  if self.isNil:
    return
  self.preferredIpv4 = ipv4

proc getPreferredIpv4*(self: MdnsInterface): string {.raises: [].} =
  if self.isNil:
    return ""
  self.preferredIpv4
