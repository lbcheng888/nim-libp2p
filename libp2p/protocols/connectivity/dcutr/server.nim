# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[sets, sequtils]
import stew/objects
import results, chronos, chronicles

import core
import
  ../../protocol, ../../../stream/connection, ../../../switch, ../../../utils/future

export chronicles

type Dcutr* = ref object of LPProtocol

logScope:
  topics = "libp2p dcutr"

proc new*(
    T: typedesc[Dcutr],
    switch: Switch,
    connectTimeout = 15.seconds,
    maxDialableAddrs = 8,
): T =
  proc handleStream(
      stream: Connection, proto: string
  ) {.async: (raises: [CancelledError]).} =
    var peerDialableAddrs: seq[MultiAddress]
    try:
      let connectMsg = DcutrMsg.decode(await stream.readLp(DcutrMsgMaxSize))
      connectMsg.expectMsgType(MsgType.Connect)
      info "Dcutr receiver received Connect", peerId = stream.peerId,
        remoteAddrCount = connectMsg.addrs.len,
        remoteAddrs = addrsForLog(connectMsg.addrs)

      var ourAddrs = switch.peerStore.getMostObservedProtosAndPorts()
        # likely empty when the peer is reachable
      if ourAddrs.len == 0:
        # this list should be the same as the peer's public addrs when it is reachable
        ourAddrs =
          switch.peerInfo.listenAddrs.mapIt(switch.peerStore.guessDialableAddr(it))
      let ourDialableAddrs = getHolePunchableAddrs(ourAddrs, maxDialableAddrs)
      if ourDialableAddrs.len == 0:
        info "Dcutr receiver has no supported dialable addresses. Aborting Dcutr.",
          ourAddrs = addrsForLog(ourAddrs)
        return

      await stream.send(MsgType.Connect, ourDialableAddrs)
      info "Dcutr receiver sent Connect", peerId = stream.peerId,
        localAddrCount = ourDialableAddrs.len,
        localAddrs = addrsForLog(ourDialableAddrs)
      let syncMsg = DcutrMsg.decode(await stream.readLp(DcutrMsgMaxSize))
      syncMsg.expectMsgType(MsgType.Sync)
      info "Dcutr receiver received Sync", peerId = stream.peerId

      peerDialableAddrs = getHolePunchableAddrs(connectMsg.addrs, maxDialableAddrs)
      if peerDialableAddrs.len == 0:
        info "Dcutr initiator has no supported dialable addresses to connect to. Aborting Dcutr.",
          addrs = addrsForLog(connectMsg.addrs)
        return
      var futs = peerDialableAddrs.mapIt(
        switch.connect(
          stream.peerId,
          @[it],
          forceDial = true,
          reuseConnection = false,
          dir = Direction.Out,
        )
      )
      try:
        discard await anyCompleted(futs).wait(connectTimeout)
        info "Dcutr receiver simultaneous dial succeeded", peerId = stream.peerId,
          remoteAddrCount = peerDialableAddrs.len,
          remoteAddrs = addrsForLog(peerDialableAddrs)
      finally:
        for fut in futs:
          fut.cancel()
    except CancelledError as err:
      trace "cancelled Dcutr receiver"
      raise err
    except AllFuturesFailedError as err:
      warn "Dcutr receiver could not connect to the remote peer, " &
        "all connect attempts failed",
        peerDialableAddrs = addrsForLog(peerDialableAddrs),
        description = err.msg
    except AsyncTimeoutError as err:
      warn "Dcutr receiver could not connect to the remote peer, " &
        "all connect attempts timed out",
        peerDialableAddrs = addrsForLog(peerDialableAddrs),
        description = err.msg
    except CatchableError as err:
      warn "Unexpected error when Dcutr receiver tried to connect " &
        "to the remote peer", description = err.msg

  let self = T()
  self.handler = handleStream
  self.codec = DcutrCodec
  self
