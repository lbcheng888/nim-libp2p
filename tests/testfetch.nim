{.used.}

# Nim-LibP2P

import unittest2
import chronos
import std/options

import ../libp2p/protocols/fetch/[fetch, protobuf]

suite "fetch protocol":
  test "request encode decode":
    let req = FetchRequest(identifier: "key-123")
    let encoded = req.encode()
    let decoded = decodeFetchRequest(encoded)
    check decoded.isSome()
    check decoded.get().identifier == req.identifier

  test "response encode decode":
    let resp = FetchResponse(status: FetchStatus.fsOk, data: @[byte 1, 2, 3])
    let encoded = resp.encode()
    let decoded = decodeFetchResponse(encoded)
    check decoded.isSome()
    let resp2 = decoded.get()
    check resp2.status == resp.status
    check resp2.data == resp.data

  test "invalid payload handling":
    check decodeFetchRequest(@[]) == none(FetchRequest)
    check decodeFetchResponse(@[]) == none(FetchResponse)

  test "extended status encode decode":
    let resp = FetchResponse(status: FetchStatus.fsTooLarge, data: @[])
    let encoded = resp.encode()
    let decoded = decodeFetchResponse(encoded)
    check decoded.isSome()
    check decoded.get().status == FetchStatus.fsTooLarge

  test "fetch config defaults":
    let cfg = FetchConfig.init()
    check cfg.maxRequestBytes == DefaultFetchMessageSize
    check cfg.maxResponseBytes == DefaultFetchMessageSize
    check cfg.handlerTimeout == 10.seconds
