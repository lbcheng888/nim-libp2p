{.used.}

# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

import std/options

import chronos
import unittest2
import pkg/results

import ../libp2p/[cid, peerid]
import ../libp2p/errors
import ../libp2p/stream/connection
import ../libp2p/protocols/datatransfer/[datatransfer, channelmanager, protobuf, persistence]

proc stubDialer(peerId: PeerId, protos: seq[string]): Future[Connection] {.async.} =
  discard peerId
  discard protos
  raise (ref CancelledError)(msg: "stub dialer should not be invoked")

type
  TestPersistence = ref object of DataTransferChannelPersistence
    state: DataTransferChannelState

method load(
    persistence: TestPersistence
): Result[DataTransferChannelState, string] =
  ok(persistence.state)

method save(
    persistence: TestPersistence, state: DataTransferChannelState
): Result[void, string] =
  persistence.state = state
  ok()

suite "Data transfer helpers":
  test "retry policy clamps attempts to at least one":
    let policy = DataTransferRetryPolicy.init(maxAttempts = 0, retryDelay = 200.milliseconds)
    check policy.maxAttempts == 1
    check policy.retryDelay == 200.milliseconds

suite "Data transfer manager stats":
  test "stats aggregates counts and pause flags":
    var peer: PeerId
    require peer.init("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC")
    let persistence = TestPersistence(
      state: DataTransferChannelState(
        nextTransferId: 3,
        channels: @[
          DataTransferChannelRecord(
            peerId: $peer,
            transferId: 1,
            direction: "dtcdInbound",
            status: "dtcsRequested",
            paused: true,
            remotePaused: false,
            retryMaxAttempts: 1,
            retryDelayNs: 0,
            lastStatus: none(string),
            lastError: none(string),
            createdAtNs: 0,
            updatedAtNs: 0,
            request: some(
              DataTransferRequestRecord(
                isPull: true,
                voucherType: "",
                voucher: "",
                selector: "",
                baseCid: none(string)
              )
            ),
            extensions: @[],
          ),
          DataTransferChannelRecord(
            peerId: $peer,
            transferId: 2,
            direction: "dtcdOutbound",
            status: "dtcsPaused",
            paused: false,
            remotePaused: true,
            retryMaxAttempts: 2,
            retryDelayNs: 0,
            lastStatus: some("paused-by-peer"),
            lastError: none(string),
            createdAtNs: 0,
            updatedAtNs: 0,
            request: some(
              DataTransferRequestRecord(
                isPull: false,
                voucherType: "",
                voucher: "",
                selector: "",
                baseCid: none(string)
              )
            ),
            extensions: @[],
          )
        ],
      )
    )
    let manager = DataTransferChannelManager.new(stubDialer, persistence)

    let stats = waitFor manager.stats()
    check stats.total == 2
    check stats.counts[dtcdInbound][dtcsRequested] == 1
    check stats.counts[dtcdOutbound][dtcsPaused] == 1
    check stats.pausedLocal[dtcdInbound] == 1
    check stats.pausedRemote[dtcdOutbound] == 1
