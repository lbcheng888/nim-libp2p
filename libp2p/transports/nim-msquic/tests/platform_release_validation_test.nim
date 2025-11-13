## 验证 E4 阶段平台发布矩阵的一致性。

import std/unittest
import std/sets
import std/strutils

import ../platform/[release_validation, platform_matrix]

suite "platform release validation matrix (E4)":
  test "覆盖核心平台并通过一致性检查":
    let matrix = defaultPlatformReleaseMatrix()
    var covered = initHashSet[PlatformOs]()
    for scenario in matrix.scenarios:
      covered.incl scenario.capability.os
      check scenarioIsConsistent(scenario)
      check scenario.packaging.name.len > 0
      for note in scenario.packaging.installNotes:
        check note.len > 0
    check covered.contains(posWindowsUser)
    check covered.contains(posLinux)
    check covered.contains(posFreeBsd)
    check covered.contains(posMacos)
    check matrix.pipelineFile.endsWith(".yml")
    let checklist = releaseChecklist(matrix)
    check checklist.len == matrix.scenarios.len
    for line in checklist:
      check line.contains("->")
