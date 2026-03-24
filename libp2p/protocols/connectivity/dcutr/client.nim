# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/sequtils

import results
import chronos, chronicles

import core
import
  ../../protocol, ../../../stream/connection, ../../../switch, ../../../utils/future

export DcutrError, DcutrAttemptOutcome

type DcutrClient* = ref object
  connectTimeout: Duration
  maxDialableAddrs: int

logScope:
  topics = "libp2p dcutrclient"

proc new*(
    T: typedesc[DcutrClient], connectTimeout = 15.seconds, maxDialableAddrs = 8
): T =
  return T(connectTimeout: connectTimeout, maxDialableAddrs: maxDialableAddrs)

proc startSync*(
    self: DcutrClient, switch: Switch, remotePeerId: PeerId, addrs: seq[MultiAddress]
): Future[DcutrAttemptOutcome] {.async: (raises: [DcutrError, CancelledError]).} =
  logScope:
    peerId = switch.peerInfo.peerId

  var
    peerDialableAddrs: seq[MultiAddress]
    stream: Connection
  try:
    let ourDialableAddrs = getHolePunchableAddrs(addrs, self.maxDialableAddrs)
    if ourDialableAddrs.len == 0:
      info "Dcutr initiator has no supported dialable addresses. Aborting Dcutr.",
        addrs = addrsForLog(addrs)
      return DcutrLocalUnsupportedAddrs

    info "Dcutr initiator starting sync", remotePeerId,
      localAddrCount = ourDialableAddrs.len,
      localAddrs = addrsForLog(ourDialableAddrs)
    stream = await switch.dial(remotePeerId, DcutrCodec)
    await stream.send(MsgType.Connect, ourDialableAddrs)
    info "Dcutr initiator sent Connect", remotePeerId,
      localAddrCount = ourDialableAddrs.len,
      localAddrs = addrsForLog(ourDialableAddrs)
    let rttStart = Moment.now()
    let connectAnswer = DcutrMsg.decode(await stream.readLp(DcutrMsgMaxSize))
    connectAnswer.expectMsgType(MsgType.Connect)

    peerDialableAddrs = getHolePunchableAddrs(connectAnswer.addrs, self.maxDialableAddrs)
    if peerDialableAddrs.len == 0:
      info "Dcutr receiver has no supported dialable addresses to connect to. Aborting Dcutr.",
        addrs = addrsForLog(connectAnswer.addrs)
      return DcutrRemoteUnsupportedAddrs

    let rttEnd = Moment.now()
    info "Dcutr initiator received Connect", remotePeerId,
      remoteAddrCount = peerDialableAddrs.len,
      remoteAddrs = addrsForLog(peerDialableAddrs)
    let halfRtt = (rttEnd - rttStart) div 2'i64
    await stream.send(MsgType.Sync, @[])
    info "Dcutr initiator sent Sync", remotePeerId, halfRttMs = halfRtt.milliseconds
    await sleepAsync(halfRtt)

    var futs = peerDialableAddrs.mapIt(
      switch.connect(
        stream.peerId,
        @[it],
        forceDial = true,
        reuseConnection = false,
        dir = Direction.In,
      )
    )
    try:
      discard await anyCompleted(futs).wait(self.connectTimeout)
      info "Dcutr initiator simultaneous dial succeeded", remotePeerId,
        remoteAddrCount = peerDialableAddrs.len,
        remoteAddrs = addrsForLog(peerDialableAddrs)
      return DcutrConnected
    finally:
      for fut in futs:
        fut.cancel()
  except CancelledError as err:
    raise err
  except AllFuturesFailedError as err:
    warn "Dcutr initiator could not connect to the remote peer, all connect attempts failed",
      peerDialableAddrs = addrsForLog(peerDialableAddrs), description = err.msg
    raise newException(
      DcutrError,
      "Dcutr initiator could not connect to the remote peer, all connect attempts failed",
      err,
    )
  except AsyncTimeoutError as err:
    warn "Dcutr initiator could not connect to the remote peer, all connect attempts timed out",
      peerDialableAddrs = addrsForLog(peerDialableAddrs), description = err.msg
    raise newException(
      DcutrError,
      "Dcutr initiator could not connect to the remote peer, all connect attempts timed out",
      err,
    )
  except CatchableError as err:
    warn "Unexpected error when Dcutr initiator tried to connect to the remote peer",
      description = err.msg
    raise newException(
      DcutrError,
      "Unexpected error when Dcutr initiator tried to connect to the remote peer: " &
        err.msg,
      err,
    )
  finally:
    if stream != nil:
      await stream.close()
