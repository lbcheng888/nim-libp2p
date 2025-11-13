# Generates Markdown documentation describing Go↔Nim interoperability
# constraints for foundational data types and module naming.

import std/[os, sequtils, strformat, strutils, times]

import pebble/core/module_map

proc repoRoot(): string =
  var path = currentSourcePath()
  for _ in 0 ..< 5:
    path = parentDir(path)
  path

proc buildReport(): string =
  let timestamp = now()
  result.add("# Pebble Go↔Nim 数据格式约束\n\n")
  result.add(&"生成时间：{timestamp.format(\"yyyy-MM-dd HH:mm:ss\")}\n\n")
  result.add("## 模块命名映射\n")
  for goPkg, nimModule in packageMap.pairs():
    result.add(&"- `{goPkg}` → `{nimModule}`\n")
  result.add("\n")
  result.add("## 基础类型约束\n")
  result.add("- `Key`：按字节序列比较，禁止依赖文本编码，长度上限由上层存储策略约束。\n")
  result.add("- `SequenceNumber`：无符号 64 位整型，按降序编码在内部键后缀，保持与 Go 版完全一致。\n")
  result.add("- `Comparator`：必须满足自反、对称与传递性；默认实现为字节序字典序。\n\n")
  result.add("## 环境覆盖规范\n")
  result.add("- 环境变量前缀固定为 `PEBBLE_`，双下划线 `__` 映射为层级分隔符。\n")
  result.add("- 配置键全部归一化为小写并使用点号路径，例如 `PEBBLE_STORAGE__PATH` → `storage.path`。\n\n")
  result.add("## 资源配额语义\n")
  result.add("- `softLimit` 触发节流，`hardLimit` 拒绝新请求；单位由资源类型定义。\n")
  result.add("- 资源类型覆盖内存、磁盘、文件句柄与压实令牌，可扩展。\n")
  result

proc writeReport(target: string) =
  createDir parentDir(target)
  writeFile(target, buildReport())

when isMainModule:
  let defaultOut = repoRoot() / "docs" / "nim_interop_formats.md"
  let outPath = if paramCount() >= 1: paramStr(1) else: defaultOut
  writeReport(outPath)
  echo "interop format report generated at ", outPath
