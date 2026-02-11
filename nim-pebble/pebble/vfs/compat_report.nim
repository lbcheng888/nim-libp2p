# 生成 Nim VFS 与 Go Pebble VFS 之间的语义差异清单，支持输出 Markdown 报告。

import std/[os, strformat, strutils, times]

type
  GapStatus* = enum
    statusAligned,
    statusPartial,
    statusMissing

  VfsSemanticGap* = object
    area*: string
    goBehavior*: string
    nimBehavior*: string
    mitigation*: string
    status*: GapStatus

proc statusLabel(status: GapStatus): string =
  case status
  of statusAligned: "已对齐"
  of statusPartial: "部分对齐"
  of statusMissing: "缺失"

proc defaultGaps*(): seq[VfsSemanticGap] =
  @[
    VfsSemanticGap(
      area: "内存文件系统 (MemFS)",
      goBehavior: "通过 vfs.NewMem() 提供具备 rename/link 语义的全内存后端，配合 vfstest 在单机测试中模拟崩溃恢复。",
      nimBehavior: "当前仅提供 POSIX/本地磁盘栈，尚未移植内存后端。",
      mitigation: "使用 MountFileSystem 将逻辑路径挂载到 tmpfs/ramdisk，或在测试前复制数据到临时目录。",
      status: statusMissing
    ),
    VfsSemanticGap(
      area: "磁盘健康与容量模拟",
      goBehavior: "Go 版包含 disk_health/disk_full 装饰器，可按水位注入 EIO/ENOSPC 并记录指标。",
      nimBehavior: "依赖 FaultInjector 对单次操作注入失败，尚未提供水位触发或指标输出。",
      mitigation: "结合 FaultInjector 与 MountFileSystem，将目标目录挂载到受控卷并由外部脚本制造磁盘压力。",
      status: statusPartial
    ),
    VfsSemanticGap(
      area: "OpenOption 扩展",
      goBehavior: "Open 时可注入 SyncingFile/LoggingFS 等选项以调整刷盘策略与观测点。",
      nimBehavior: "OpenOption 抽象已就绪，但暂无具体实现，默认行为等价于基础 POSIX。",
      mitigation: "后续按需移植 Syncing/Logging 等选项；短期通过 FaultInjector + RateLimitedFileSystem 组合落地观测。",
      status: statusPartial
    ),
    VfsSemanticGap(
      area: "测试数据复用",
      goBehavior: "测试直接挂载 vfs 默认实现，依赖真实 Pebble testdata 与 vfstest 共享基准。",
      nimBehavior: "新增 MountFileSystem，可将 Nim 逻辑前缀映射到任意物理路径，支持 read-only/读写模式。",
      mitigation: "通过 newMountFileSystem/newMountedPebbleVfs 与默认栈组合，复用 Go 侧 testdata 或自定义基准目录。",
      status: statusAligned
    )
  ]

proc toMarkdown*(gaps: seq[VfsSemanticGap]; timestamp: DateTime = now()): string =
  var lines: seq[string] = @[]
  lines.add("# Nim VFS 语义对齐清单")
  lines.add("")
  lines.add("| 区域 | Go 语义 | Nim 语义 | 缓解策略 | 状态 |")
  lines.add("| --- | --- | --- | --- | --- |")
  for gap in gaps:
    lines.add(&"| {gap.area} | {gap.goBehavior} | {gap.nimBehavior} | {gap.mitigation} | {statusLabel(gap.status)} |")
  lines.add("")
  lines.add(&"最后更新：{timestamp.format(\"yyyy-MM-dd HH:mm:ss\")}")
  lines.join("\n")

proc writeCompatibilityReport*(target: string; gaps: seq[VfsSemanticGap] = defaultGaps()) =
  let content = gaps.toMarkdown()
  createDir parentDir(target)
  writeFile(target, content & "\n")

when isMainModule:
  let defaultOut = currentSourcePath().parentDir().parentDir().parentDir().parentDir().parentDir() /
                  "docs" / "nim_vfs_semantic_diff.md"
  writeCompatibilityReport(defaultOut)
  echo "Nim VFS 语义差异报告已生成: ", defaultOut
