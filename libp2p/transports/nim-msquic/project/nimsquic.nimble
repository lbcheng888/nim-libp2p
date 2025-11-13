# Package definition for Nim MsQuic blueprint (B6 产出)

version       = "0.1.0"
author        = "MsQuic Nim Porting WG"
description   = "Nim 2.2.6 blueprint modules for porting MsQuic"
license       = "MIT"
srcDir        = "../"
skipDirs      = @["project"]
binDir        = "../build"

requires "nim >= 2.2.6"

const NimSrcPath = "../nim"
const TestPath = "../tests"
const TestFiles = @[
  "test_newreno.nim",
  "test_flow_control.nim",
  "test_connection_migration.nim",
  "test_frame_catalog.nim",
  "congestion_pacing_test.nim",
  "protocol_validation_test.nim"
]

task build, "编译检查核心模块与平台/工具蓝图" :
  exec "nim check --path:" & NimSrcPath & " " & NimSrcPath & "/core/mod.nim"
  exec "nim check --path:" & NimSrcPath & " " & NimSrcPath & "/platform/mod.nim"
  exec "nim check --path:" & NimSrcPath & " " & NimSrcPath & "/tooling/mod.nim"

task test, "运行 Nim 单元测试（拥塞、流控、迁移等场景）" :
  for file in TestFiles:
    exec "nim r --path:" & NimSrcPath & " " & TestPath & "/" & file

task lint, "执行 Nim 静态分析（style check）" :
  exec "nim check --styleCheck:hint --path:" & NimSrcPath & " " &
       NimSrcPath & "/congestion/newreno_model.nim"
