## Nim Fuzz smoke runner：验证少量预设样本不会触发错误。

import std/unittest

import ../tooling/fuzz_harness

when isMainModule:
  suite "fuzz smoke runner":
    test "initial flight samples succeed":
      let samples = @[
        @[0x01'u8, 0x02, 0x03, 0x04],
        @[0xAA'u8, 0xBB, 0xCC],
        @[0x10'u8, 0x10, 0x10, 0x10, 0x10]
      ]
      let results = fuzzPayloads(samples)
      check results.len == samples.len
      for res in results:
        check res.status == frSuccess
