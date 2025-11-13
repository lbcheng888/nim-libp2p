import std/unittest

import ../validation/protocol_validation

suite "Protocol validation suite":
  test "default suite passes":
    let summary = runProtocolValidationSuite()
    check summary.passed
    check summary.failed.len == 0
    check summary.results.len >= 3
