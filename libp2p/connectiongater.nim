# Nim-LibP2P
# Copyright (c) 2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/sequtils
import pkg/results
import multiaddress, peerid

type
  PeerResult = Result[PeerId, void]
  AddrResult = Result[MultiAddress, void]

  ConnectionGater* = ref object of RootObj
    blockedPeers*: seq[PeerId]
    blockedAddrs*: seq[string]

proc new*(T: type ConnectionGater): T =
  T(blockedPeers: @[], blockedAddrs: @[])

proc blockPeer*(g: ConnectionGater, peer: PeerId) =
  if g.isNil:
    return
  if peer notin g.blockedPeers:
    g.blockedPeers.add(peer)

proc unblockPeer*(g: ConnectionGater, peer: PeerId) =
  if g.isNil:
    return
  g.blockedPeers.keepItIf(it != peer)

proc blockAddress*(g: ConnectionGater, address: MultiAddress) =
  if g.isNil:
    return
  let repr = $address
  if repr notin g.blockedAddrs:
    g.blockedAddrs.add(repr)

proc unblockAddress*(g: ConnectionGater, address: MultiAddress) =
  if g.isNil:
    return
  let repr = $address
  g.blockedAddrs.keepItIf(it != repr)

proc allowDial*(g: ConnectionGater, peer: PeerResult, address: MultiAddress): bool {.inline.} =
  if g.isNil:
    return true
  if peer.isOk and peer.get() in g.blockedPeers:
    return false
  if $address in g.blockedAddrs:
    return false
  true

proc allowAccept*(g: ConnectionGater, localAddr: AddrResult, remoteAddr: AddrResult): bool {.inline.} =
  if g.isNil:
    return true
  if localAddr.isOk:
    let repr = $(localAddr.get())
    if repr in g.blockedAddrs:
      return false
  if remoteAddr.isOk:
    let repr = $(remoteAddr.get())
    if repr in g.blockedAddrs:
      return false
  true

proc allowSecured*(g: ConnectionGater, peer: PeerId, remoteAddr: Opt[MultiAddress]): bool {.inline.} =
  if g.isNil:
    return true
  if peer in g.blockedPeers:
    return false
  remoteAddr.withValue(addr):
    if $addr in g.blockedAddrs:
      return false
  true
