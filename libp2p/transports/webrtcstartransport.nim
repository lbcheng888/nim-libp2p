# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[options]
import pkg/[chronicles, chronos, results]

import
  ./transport,
  ./webrtcdirecttransport,
  ../multiaddress,
  ../multicodec,
  ../peerid,
  ../upgrademngrs/upgrade

logScope:
  topics = "libp2p webrtcstar"

const
  starCodec = multiCodec("p2p-webrtc-star")
  wsCodec = multiCodec("ws")
  wssCodec = multiCodec("wss")

type
  WebRtcStarTransport* = ref object of Transport
    direct: WebRtcDirectTransport
    listenAddrs: seq[MultiAddress]

  WebRtcStarError* = object of transport.TransportError

proc convertToDirect(address: MultiAddress): Result[MultiAddress, string] =
  var builder = ""
  var hasStar = false
  for partRes in address.items():
    if partRes.isErr():
      return err("invalid multiaddress component: " & partRes.error())
    let part = partRes.get()
    let nameRes = part.protoName()
    if nameRes.isErr():
      return err("failed to read multiaddress component name: " & nameRes.error())
    let name = nameRes.get()
    case name
    of "p2p-webrtc-star":
      hasStar = true
      continue
    of "ws":
      builder.add("/http")
    of "wss":
      builder.add("/https")
    else:
      let strRes = part.toString()
      if strRes.isErr():
        return err("failed to render multiaddress component: " & strRes.error())
      builder.add(strRes.get())
  if not hasStar:
    return err("address missing /p2p-webrtc-star: " & $address)
  if builder.len == 0:
    return err("converted multiaddress is empty")
  let directRes = MultiAddress.init(builder)
  if directRes.isErr():
    return err("failed to parse converted multiaddress: " & directRes.error())
  ok(directRes.get())

proc convertToStar(address: MultiAddress): Result[MultiAddress, string] =
  var builder = "/p2p-webrtc-star"
  var sawHttp = false
  for partRes in address.items():
    if partRes.isErr():
      return err("invalid multiaddress component: " & partRes.error())
    let part = partRes.get()
    let nameRes = part.protoName()
    if nameRes.isErr():
      return err("failed to read multiaddress component name: " & nameRes.error())
    let name = nameRes.get()
    case name
    of "http":
      builder.add("/ws")
      sawHttp = true
    of "https":
      builder.add("/wss")
      sawHttp = true
    of "p2p-webrtc-star":
      return err("address already contains /p2p-webrtc-star: " & $address)
    else:
      let strRes = part.toString()
      if strRes.isErr():
        return err("failed to render multiaddress component: " & strRes.error())
      builder.add(strRes.get())
  if not sawHttp:
    builder.add("/ws")
  let starRes = MultiAddress.init(builder)
  if starRes.isErr():
    return err("failed to parse star multiaddress: " & starRes.error())
  ok(starRes.get())

proc decorateConnection(self: WebRtcStarTransport, conn: Connection) =
  proc mapAddr(addrOpt: Opt[MultiAddress]): Opt[MultiAddress] =
    if addrOpt.isSome():
      let converted = convertToStar(addrOpt.get()).valueOr:
        trace "failed to convert direct address back to star format",
          error = error, address = $addrOpt.get()
        return addrOpt
      Opt.some(converted)
    else:
      Opt.none(MultiAddress)

  conn.observedAddr = mapAddr(conn.observedAddr)
  conn.localAddr = mapAddr(conn.localAddr)

proc new*(
    _: type WebRtcStarTransport,
    upgrade: Upgrade,
    privateKey: PrivateKey,
    cfg: WebRtcDirectConfig = defaultConfig(),
): WebRtcStarTransport =
  let transport = WebRtcStarTransport(
    direct: WebRtcDirectTransport.new(upgrade, privateKey, cfg),
    listenAddrs: @[],
  )
  procCall Transport(transport).initialize()
  transport.upgrader = upgrade
  transport.networkReachability = NetworkReachability.Reachable
  transport

method handles*(
    self: WebRtcStarTransport, address: MultiAddress
): bool {.raises: [].} =
  if not procCall Transport(self).handles(address):
    return false
  address.contains(starCodec).valueOr(false)

method start*(
    self: WebRtcStarTransport, addrs: seq[MultiAddress]
): Future[void] {.
    async: (raises: [LPError, transport.TransportError, CancelledError])
.} =
  if self.direct.isNil():
    raise (ref WebRtcStarError)(msg: "direct transport missing", parent: nil)
  var converted: seq[MultiAddress] = @[]
  converted.setLen(0)
  for addr in addrs:
    let directAddr = convertToDirect(addr).valueOr:
      raise (ref WebRtcStarError)(msg: error, parent: nil)
    converted.add(directAddr)
  await self.direct.start(converted)
  self.listenAddrs = addrs
  await procCall Transport(self).start(addrs)

method stop*(self: WebRtcStarTransport) {.async: (raises: []).} =
  if not self.direct.isNil():
    try:
      await self.direct.stop()
    except CatchableError as exc:
      trace "failed stopping direct transport for webrtc-star", error = exc.msg
  await procCall Transport(self).stop()

method accept*(
    self: WebRtcStarTransport
): Future[Connection] {.
    async: (raises: [transport.TransportError, CancelledError])
.} =
  if self.direct.isNil():
    raise (ref WebRtcStarError)(msg: "direct transport missing", parent: nil)
  let conn = await self.direct.accept()
  self.decorateConnection(conn)
  conn

method dial*(
    self: WebRtcStarTransport,
    hostname: string,
    address: MultiAddress,
    peerId: Opt[PeerId] = Opt.none(PeerId),
): Future[Connection] {.
    async: (raises: [transport.TransportError, CancelledError])
.} =
  if self.direct.isNil():
    raise (ref WebRtcStarError)(msg: "direct transport missing", parent: nil)
  let converted = convertToDirect(address).valueOr:
    raise (ref WebRtcStarError)(msg: error, parent: nil)
  let conn = await self.direct.dial(hostname, converted, peerId)
  self.decorateConnection(conn)
  conn
