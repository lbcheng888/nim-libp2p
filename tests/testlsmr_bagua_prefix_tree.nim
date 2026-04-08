{.used.}

import std/[options, sequtils]

import unittest2

import ../libp2p/lsmr

suite "LSMR Bagua Prefix Tree":
  test "root digest is order independent":
    let keyA = LsmrBaguaKey.init(@[5'u8, 1'u8], 1'u8, LsmrBagua.lbgZhen, "a1")
    let keyB = LsmrBaguaKey.init(@[5'u8, 9'u8], 2'u8, LsmrBagua.lbgKan, "b2")
    let keyC = LsmrBaguaKey.init(@[5'u8, 1'u8], 1'u8, LsmrBagua.lbgZhen, "a2")

    var left = LsmrBaguaPrefixTree.init()
    left.put(keyA, [byte 0x01, byte 0x02], version = 1'u64)
    left.put(keyB, [byte 0x03], version = 2'u64)
    left.put(keyC, [byte 0x04], version = 3'u64)

    var right = LsmrBaguaPrefixTree.init()
    right.put(keyC, [byte 0x04], version = 3'u64)
    right.put(keyB, [byte 0x03], version = 2'u64)
    right.put(keyA, [byte 0x01, byte 0x02], version = 1'u64)

    check left.len == 3
    check left.rootDigest() == right.rootDigest()
    check left.get(keyA).isSome()
    check left.get(keyA).get().payload == @[byte 0x01, byte 0x02]

  test "prefix lookup follows topology resolution bagua and content":
    let keyA = LsmrBaguaKey.init(@[5'u8, 1'u8], 1'u8, LsmrBagua.lbgZhen, "abc1")
    let keyB = LsmrBaguaKey.init(@[5'u8, 1'u8], 1'u8, LsmrBagua.lbgZhen, "abc2")
    let keyC = LsmrBaguaKey.init(@[5'u8, 1'u8], 1'u8, LsmrBagua.lbgKan, "abc3")
    let keyD = LsmrBaguaKey.init(@[5'u8, 9'u8], 2'u8, LsmrBagua.lbgKun, "zzz0")

    var tree = LsmrBaguaPrefixTree.init()
    tree.put(keyA, [byte 0x0a], version = 1'u64)
    tree.put(keyB, [byte 0x0b], version = 1'u64)
    tree.put(keyC, [byte 0x0c], version = 1'u64)
    tree.put(keyD, [byte 0x0d], version = 1'u64)

    let topoCursor = LsmrBaguaCursor.init(topologyPrefix = @[5'u8, 1'u8])
    let zhenCursor =
      LsmrBaguaCursor.init(
        topologyPrefix = @[5'u8, 1'u8],
        resolution = some(1'u8),
        bagua = some(LsmrBagua.lbgZhen),
      )
    let contentCursor =
      LsmrBaguaCursor.init(
        topologyPrefix = @[5'u8, 1'u8],
        resolution = some(1'u8),
        bagua = some(LsmrBagua.lbgZhen),
        contentPrefix = "abc",
      )

    check tree.prefixLeaves(topoCursor).len == 3
    check tree.prefixLeaves(zhenCursor).len == 2
    check tree.prefixLeaves(contentCursor).mapIt(it.key.contentLsh) == @["abc1", "abc2"]

    let oldZhenDigest = tree.prefixDigest(zhenCursor)
    let oldKunDigest =
      tree.prefixDigest(
        LsmrBaguaCursor.init(
          topologyPrefix = @[5'u8, 9'u8],
          resolution = some(2'u8),
          bagua = some(LsmrBagua.lbgKun),
        )
      )
    tree.put(
      LsmrBaguaKey.init(@[5'u8, 1'u8], 1'u8, LsmrBagua.lbgZhen, "abc9"),
      [byte 0x0e],
      version = 2'u64,
    )
    check tree.prefixDigest(zhenCursor) != oldZhenDigest
    check tree.prefixDigest(
      LsmrBaguaCursor.init(
        topologyPrefix = @[5'u8, 9'u8],
        resolution = some(2'u8),
        bagua = some(LsmrBagua.lbgKun),
      )
    ) == oldKunDigest
    check tree.delete(keyD)
    check tree.prefixLeaves(
      LsmrBaguaCursor.init(
        topologyPrefix = @[5'u8, 9'u8],
        resolution = some(2'u8),
        bagua = some(LsmrBagua.lbgKun),
      )
    ).len == 0

  test "object coordinate maps into bagua tier replicas":
    let coord = objectCoordinate("cid:test", @[byte 0x01, byte 0x7f, byte 0xa4], 3)
    check coord.objectId == "cid:test"
    check coord.depth == 3'u8
    check coord.contentLsh == "017FA4"

    let edgeKeys = coord.baguaKeys(LsmrStorageTier.lstEdge)
    check edgeKeys.len == 2
    check edgeKeys[0].topologyPath == coord.path
    check edgeKeys[0].resolution == 3'u8
    check edgeKeys[0].bagua == LsmrBagua.lbgZhen
    check edgeKeys[1].bagua == LsmrBagua.lbgXun

    let globalKeys = coord.baguaKeys(LsmrStorageTier.lstGlobal, resolution = 1'u8)
    check globalKeys.mapIt(it.bagua) == @[LsmrBagua.lbgKun, LsmrBagua.lbgGen]
    check globalKeys.allIt(it.resolution == 1'u8)

    var tree = LsmrBaguaPrefixTree.init()
    let inserted = tree.putObject(coord, LsmrStorageTier.lstFog, [byte 0xaa, byte 0xbb], version = 9'u64)
    check inserted.mapIt(it.bagua) == @[LsmrBagua.lbgKan, LsmrBagua.lbgLi]
    let fogCursor =
      LsmrBaguaCursor.init(
        topologyPrefix = coord.path,
        resolution = some(coord.depth),
      )
    check tree.prefixLeaves(fogCursor).len == 2

  test "diff reports missing and payload mismatch":
    let sharedKey = LsmrBaguaKey.init(@[5'u8], 1'u8, LsmrBagua.lbgQian, "aa")
    let localOnlyKey = LsmrBaguaKey.init(@[5'u8, 1'u8], 1'u8, LsmrBagua.lbgZhen, "bb")
    let remoteOnlyKey = LsmrBaguaKey.init(@[5'u8, 9'u8], 2'u8, LsmrBagua.lbgKun, "cc")
    let mismatchKey = LsmrBaguaKey.init(@[5'u8, 3'u8], 1'u8, LsmrBagua.lbgLi, "dd")

    var localTree = LsmrBaguaPrefixTree.init()
    localTree.put(sharedKey, [byte 0x01], version = 1'u64)
    localTree.put(localOnlyKey, [byte 0x02], version = 1'u64)
    localTree.put(mismatchKey, [byte 0x03], version = 1'u64)

    var remoteTree = LsmrBaguaPrefixTree.init()
    remoteTree.put(sharedKey, [byte 0x01], version = 1'u64)
    remoteTree.put(remoteOnlyKey, [byte 0x04], version = 1'u64)
    remoteTree.put(mismatchKey, [byte 0x05], version = 1'u64)

    let deltas = localTree.diff(remoteTree)
    check deltas.len == 3
    check deltas[0].key == localOnlyKey
    check deltas[0].kind == LsmrBaguaDiffKind.lbdMissingRemote
    check deltas[1].key == mismatchKey
    check deltas[1].kind == LsmrBaguaDiffKind.lbdPayloadMismatch
    check deltas[2].key == remoteOnlyKey
    check deltas[2].kind == LsmrBaguaDiffKind.lbdMissingLocal
