# QA DSL：定义批处理测试的场景描述语法并提供解析器。

import std/[options, strformat, strutils, sequtils]

type
  QAActionKind* = enum
    qaPut,
    qaDelete,
    qaMerge

  QAAction* = object
    kind*: QAActionKind
    key*: string
    value*: Option[string]

proc parseActionLine(line: string): QAAction =
  let trimmed = line.strip()
  if trimmed.len == 0 or trimmed[0] == '#':
    return
  let parts = trimmed.splitWhitespace()
  if parts.len == 0:
    return
  let opcode = parts[0].toLowerAscii()
  case opcode
  of "put":
    if parts.len < 3:
      raise newException(ValueError, "put 语句需要 key 与 value")
    let key = parts[1]
    let value = parts[2 ..< parts.len].join(" ")
    QAAction(kind: qaPut, key: key, value: some(value))
  of "delete", "del":
    if parts.len < 2:
      raise newException(ValueError, "delete 语句需要 key")
    QAAction(kind: qaDelete, key: parts[1], value: none(string))
  of "merge":
    if parts.len < 3:
      raise newException(ValueError, "merge 语句需要 key 与 value")
    let key = parts[1]
    let value = parts[2 ..< parts.len].join(" ")
    QAAction(kind: qaMerge, key: key, value: some(value))
  else:
    raise newException(ValueError, "未知 QA DSL 操作: " & opcode)

proc parseScenario*(text: string): seq[QAAction] =
  for line in text.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0 or trimmed[0] == '#':
      continue
    result.add(parseActionLine(trimmed))

proc `$`*(action: QAAction): string =
  let base = case action.kind
    of qaPut: "put"
    of qaDelete: "delete"
    of qaMerge: "merge"
  if action.value.isSome():
    fmt"{base} {action.key} {action.value.get()}"
  else:
    fmt"{base} {action.key}"
